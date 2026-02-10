use std::io::{Cursor, Write};
use std::sync::Arc;
use std::time::Duration;

use oxide_core::{
    ArchiveOptions, ArchivePipeline, ArchiveProgressSnapshot, ArchiveReader, BufferPool,
    CompressionAlgo, PreProcessingStrategy, ProgressSink, StatValue,
};
use tempfile::{NamedTempFile, TempDir};

fn write_fixture(data: &[u8]) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(data)?;
    file.flush()?;
    Ok(file)
}

fn build_text_fixture(bytes: usize) -> Vec<u8> {
    let line = b"phase1 pipeline integration line\n";
    let mut data = Vec::with_capacity(bytes);
    while data.len() < bytes {
        let remaining = bytes - data.len();
        let take = remaining.min(line.len());
        data.extend_from_slice(&line[..take]);
    }
    data
}

fn write_directory_file(
    root: &TempDir,
    rel_path: &str,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let path = root.path().join(rel_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, data)?;
    Ok(())
}

#[derive(Default)]
struct CollectProgress {
    snapshots: Vec<ArchiveProgressSnapshot>,
}

impl ProgressSink for CollectProgress {
    fn on_progress(&mut self, snapshot: ArchiveProgressSnapshot) {
        self.snapshots.push(snapshot);
    }
}

#[test]
fn pipeline_roundtrip_reconstructs_original_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(512 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(64 * 1024, 128));
    let pipeline =
        ArchivePipeline::new(32 * 1024, 4, Arc::clone(&buffer_pool), CompressionAlgo::Lz4);

    let archive = pipeline.archive_file(file.path(), Vec::new())?;
    let restored = pipeline.extract_archive(Cursor::new(archive))?;

    assert_eq!(restored, data);

    let metrics = buffer_pool.metrics();
    assert!(metrics.created > 0);
    Ok(())
}

#[test]
fn pipeline_writes_blocks_in_strict_id_order() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(256 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(32 * 1024, 64));
    let pipeline = ArchivePipeline::new(8 * 1024, 4, buffer_pool, CompressionAlgo::Deflate);
    let archive = pipeline.archive_file(file.path(), Vec::new())?;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    for (expected, block) in reader.iter_blocks().enumerate() {
        let (header, payload) = block?;
        assert_eq!(header.block_id, expected as u64);
        assert_eq!(payload.len(), header.compressed_size as usize);
    }

    Ok(())
}

#[test]
fn pipeline_records_preprocessing_strategy_in_block_headers()
-> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);
    let archive = pipeline.archive_file(file.path(), Vec::new())?;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    let (header, payload) = reader.read_block(0)?;

    assert_eq!(header.strategy()?, PreProcessingStrategy::None);
    assert_eq!(header.compression()?, CompressionAlgo::Lz4);
    assert_eq!(payload.len(), header.compressed_size as usize);

    Ok(())
}

#[test]
fn pipeline_reaches_buffer_pool_steady_state() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(64 * 1024, 128));
    let pipeline = ArchivePipeline::new(
        512 * 1024,
        1,
        Arc::clone(&buffer_pool),
        CompressionAlgo::Lzma,
    );

    // Single worker and single block keeps demand deterministic after warm-up.
    let _archive = pipeline.archive_file(file.path(), Vec::new())?;
    let created_after_warmup = buffer_pool.metrics().created;

    for _ in 0..5 {
        let _archive = pipeline.archive_file(file.path(), Vec::new())?;
    }

    let created_after_steady_state = buffer_pool.metrics().created;
    assert_eq!(created_after_steady_state, created_after_warmup);

    Ok(())
}

#[test]
fn directory_archive_roundtrip_restores_tree() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "top.txt", b"top-level")?;
    write_directory_file(&source, "nested/a.bin", &[1, 2, 3, 4, 5])?;
    write_directory_file(&source, "nested/deeper/b.txt", b"deep-content")?;
    std::fs::create_dir_all(source.path().join("empty/leaf"))?;

    let buffer_pool = Arc::new(BufferPool::new(32 * 1024, 128));
    let pipeline = ArchivePipeline::new(
        16 * 1024,
        4,
        Arc::clone(&buffer_pool),
        CompressionAlgo::Deflate,
    );

    let archive = pipeline.archive_directory(source.path(), Vec::new())?;

    let out = tempfile::tempdir()?;
    pipeline.extract_directory_archive(Cursor::new(archive), out.path())?;

    assert_eq!(std::fs::read(out.path().join("top.txt"))?, b"top-level");
    assert_eq!(
        std::fs::read(out.path().join("nested/a.bin"))?,
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        std::fs::read(out.path().join("nested/deeper/b.txt"))?,
        b"deep-content"
    );
    assert!(out.path().join("empty/leaf").is_dir());

    Ok(())
}

#[test]
fn archive_path_supports_directory_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "sample.txt", b"directory mode")?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline.archive_path(source.path(), Vec::new())?;
    let out = tempfile::tempdir()?;
    pipeline.extract_directory_archive(Cursor::new(archive), out.path())?;

    assert_eq!(
        std::fs::read(out.path().join("sample.txt"))?,
        b"directory mode"
    );
    Ok(())
}

#[test]
fn archive_sets_directory_source_flag() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "sample.txt", b"directory mode")?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline.archive_path(source.path(), Vec::new())?;
    let reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.global_header().flags & 1, 1);
    Ok(())
}

#[test]
fn directory_archive_marks_blocks_without_preprocessing_in_fast_mode()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(
        &source,
        "notes.txt",
        b"this directory contains text content\nline two\n",
    )?;
    write_directory_file(
        &source,
        "artifact.bin",
        &[0x55, 0x48, 0x89, 0xE5, 0x90, 0x90, 0x90, 0xC3],
    )?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(32, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline.archive_path(source.path(), Vec::new())?;
    let mut reader = ArchiveReader::new(Cursor::new(archive))?;

    for block in reader.iter_blocks() {
        let (header, _payload) = block?;
        assert_eq!(header.strategy()?, PreProcessingStrategy::None);
    }

    Ok(())
}

#[test]
fn extract_path_restores_file_payload() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(8 * 1024, 2, buffer_pool, CompressionAlgo::Deflate);

    let archive = pipeline.archive_path(file.path(), Vec::new())?;
    let out_root = tempfile::tempdir()?;
    let out_file = out_root.path().join("restored.txt");
    let kind = pipeline.extract_path(Cursor::new(archive), &out_file)?;

    assert_eq!(kind, oxide_core::ArchiveSourceKind::File);
    assert_eq!(std::fs::read(out_file)?, data);
    Ok(())
}

#[test]
fn extract_path_restores_directory_payload() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "nested/data.bin", &[9, 8, 7])?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline.archive_path(source.path(), Vec::new())?;
    let out_root = tempfile::tempdir()?;
    let out_dir = out_root.path().join("restored-tree");
    let kind = pipeline.extract_path(Cursor::new(archive), &out_dir)?;

    assert_eq!(kind, oxide_core::ArchiveSourceKind::Directory);
    assert_eq!(
        std::fs::read(out_dir.join("nested/data.bin"))?,
        vec![9, 8, 7]
    );
    Ok(())
}

#[test]
fn archive_path_with_reports_progress_and_extensible_stats()
-> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(256 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(32 * 1024, 64));
    let pipeline = ArchivePipeline::new(16 * 1024, 4, buffer_pool, CompressionAlgo::Deflate);
    let mut sink = CollectProgress::default();
    let options = ArchiveOptions {
        progress_interval: Duration::from_millis(1),
        emit_final_progress: true,
    };

    let outcome = pipeline.archive_path_with(file.path(), Vec::new(), options, &mut sink)?;
    assert!(!sink.snapshots.is_empty());

    let final_snapshot = sink.snapshots.last().expect("missing final snapshot");
    assert_eq!(final_snapshot.blocks_completed, final_snapshot.blocks_total);

    let stats = &outcome.stats;
    assert_eq!(stats.blocks_total, final_snapshot.blocks_total);
    assert_eq!(stats.blocks_completed, final_snapshot.blocks_completed);
    assert_eq!(
        stats.extension_u64("runtime.completed"),
        Some(stats.blocks_completed as u64)
    );
    assert_eq!(
        stats.extension_u64("runtime.worker_count"),
        Some(stats.workers.len() as u64)
    );
    assert!(matches!(
        stats.extension("runtime.effective_cores"),
        Some(StatValue::F64(_))
    ));
    assert!(matches!(
        stats.extension("pipeline.max_inflight_blocks"),
        Some(StatValue::U64(_))
    ));
    assert!(matches!(
        stats.extension("pipeline.max_inflight_bytes"),
        Some(StatValue::U64(_))
    ));
    assert!(matches!(
        stats.extension("stage.writer_us"),
        Some(StatValue::U64(_))
    ));
    assert!(matches!(
        stats.extension("archive.output_input_ratio"),
        Some(StatValue::F64(_))
    ));

    let restored = pipeline.extract_archive(Cursor::new(outcome.writer))?;
    assert_eq!(restored, data);
    Ok(())
}

#[test]
fn archive_options_can_disable_final_progress_emit() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 32));
    let pipeline = ArchivePipeline::new(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);
    let mut sink = CollectProgress::default();
    let options = ArchiveOptions {
        progress_interval: Duration::from_secs(3600),
        emit_final_progress: false,
    };

    let outcome = pipeline.archive_path_with(file.path(), Vec::new(), options, &mut sink)?;
    assert!(sink.snapshots.is_empty());
    assert!(outcome.stats.blocks_total > 0);

    Ok(())
}

#[test]
fn directory_progress_reports_stable_block_total() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "a.txt", b"alpha\nbeta\ngamma\n")?;
    write_directory_file(&source, "b.bin", &[0x55, 0x48, 0x89, 0xE5, 0xC3])?;
    write_directory_file(&source, "nested/c.txt", b"nested text file\n")?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = ArchivePipeline::new(64, 2, buffer_pool, CompressionAlgo::Lz4);
    let mut sink = CollectProgress::default();
    let options = ArchiveOptions {
        progress_interval: Duration::from_millis(1),
        emit_final_progress: true,
    };

    let outcome = pipeline.archive_path_with(source.path(), Vec::new(), options, &mut sink)?;
    assert!(!sink.snapshots.is_empty());

    let expected_total = sink.snapshots[0].blocks_total;
    assert!(expected_total > 0);
    assert!(
        sink.snapshots
            .iter()
            .all(|snapshot| snapshot.blocks_total == expected_total)
    );

    let final_snapshot = sink.snapshots.last().expect("missing final snapshot");
    assert_eq!(final_snapshot.blocks_completed, final_snapshot.blocks_total);
    assert_eq!(outcome.stats.blocks_total, expected_total);

    let reader = ArchiveReader::new(Cursor::new(outcome.writer))?;
    assert_eq!(reader.block_count(), expected_total);

    Ok(())
}
