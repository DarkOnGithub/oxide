use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;

use oxide_core::{
    ArchiveEntryKind, ArchivePipeline, ArchivePipelineConfig, ArchiveProgressEvent, ArchiveReader,
    BufferPool, CompressionAlgo, ExtractProgressEvent, FOOTER_SIZE, GLOBAL_HEADER_SIZE,
    PreProcessingStrategy, ReportValue, RunTelemetryOptions, SECTION_TABLE_ENTRY_SIZE,
    TelemetryEvent, TelemetrySink,
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

fn build_incompressible_fixture(bytes: usize) -> Vec<u8> {
    let mut state = 0xCAFEBABE_DEADBEEFu64;
    let mut data = Vec::with_capacity(bytes);
    for _ in 0..bytes {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        data.push((state >> 56) as u8);
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

fn build_pipeline(
    block_size: usize,
    workers: usize,
    pool: Arc<BufferPool>,
    compression: CompressionAlgo,
) -> ArchivePipeline {
    let config = ArchivePipelineConfig::new(block_size, workers, pool, compression);
    ArchivePipeline::new(config)
}

#[derive(Default)]
struct CollectProgress {
    snapshots: Vec<ArchiveProgressEvent>,
}

impl TelemetrySink for CollectProgress {
    fn on_event(&mut self, event: TelemetryEvent) {
        if let TelemetryEvent::ArchiveProgress(snapshot) = event {
            self.snapshots.push(snapshot);
        }
    }
}

#[derive(Default)]
struct CollectExtractProgress {
    snapshots: Vec<ExtractProgressEvent>,
}

impl TelemetrySink for CollectExtractProgress {
    fn on_event(&mut self, event: TelemetryEvent) {
        if let TelemetryEvent::ExtractProgress(snapshot) = event {
            self.snapshots.push(snapshot);
        }
    }
}

#[test]
fn pipeline_roundtrip_reconstructs_original_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(512 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(64 * 1024, 128));
    let pipeline = build_pipeline(32 * 1024, 4, Arc::clone(&buffer_pool), CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_file(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let (restored, _report) =
        pipeline.extract_archive(Cursor::new(archive), RunTelemetryOptions::default(), None)?;

    assert_eq!(restored, data);

    let metrics = buffer_pool.metrics();
    assert!(metrics.created > 0);
    Ok(())
}

#[test]
fn pipeline_seekable_archive_path_roundtrips_file_output() -> Result<(), Box<dyn std::error::Error>>
{
    let data = build_text_fixture(384 * 1024);
    let input = write_fixture(&data)?;
    let output = NamedTempFile::new()?;

    let buffer_pool = Arc::new(BufferPool::new(64 * 1024, 128));
    let pipeline = build_pipeline(32 * 1024, 4, Arc::clone(&buffer_pool), CompressionAlgo::Lz4);

    let output_file = output.reopen()?;
    pipeline.archive_path_seekable(
        input.path(),
        output_file,
        RunTelemetryOptions::default(),
        None,
    )?;

    let mut archive = output.reopen()?;
    archive.seek(SeekFrom::Start(0))?;
    let (restored, _report) =
        pipeline.extract_archive(archive, RunTelemetryOptions::default(), None)?;

    assert_eq!(restored, data);
    Ok(())
}

#[test]
fn pipeline_marks_raw_passthrough_blocks_when_compression_is_not_smaller()
-> Result<(), Box<dyn std::error::Error>> {
    let data = build_incompressible_fixture(256 * 1024);
    let file = write_fixture(&data)?;

    let mut config = ArchivePipelineConfig::new(
        8 * 1024,
        3,
        Arc::new(BufferPool::new(16 * 1024, 64)),
        CompressionAlgo::Lz4,
    );
    config.performance.raw_fallback_enabled = true;
    let pipeline = ArchivePipeline::new(config);

    let run = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions::default(),
        None,
    )?;
    assert!(matches!(
        run.report.extensions.get("compression.raw_passthrough_blocks"),
        Some(ReportValue::U64(count)) if *count > 0
    ));

    let archive = run.writer;
    let mut reader = ArchiveReader::new(Cursor::new(archive.clone()))?;
    let mut raw_blocks = 0usize;
    for block in reader.iter_blocks() {
        let (header, payload) = block?;
        let compression = header.compression_meta()?;
        if compression.raw_passthrough {
            raw_blocks += 1;
            assert_eq!(header.encoded_len, header.raw_len);
            assert_eq!(payload.len(), header.raw_len as usize);
        }
    }
    assert!(raw_blocks > 0);

    let (restored, _report) =
        pipeline.extract_archive(Cursor::new(archive), RunTelemetryOptions::default(), None)?;
    assert_eq!(restored, data);
    Ok(())
}

#[test]
fn pipeline_writes_blocks_in_strict_id_order() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(256 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(32 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 4, buffer_pool, CompressionAlgo::Lz4);
    let archive = pipeline
        .archive_file(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    for (expected, block) in reader.iter_blocks().enumerate() {
        let (header, payload) = block?;
        assert_eq!(header.chunk_id, expected as u64);
        assert_eq!(payload.len(), header.encoded_len as usize);
    }

    Ok(())
}

#[test]
fn pipeline_records_preprocessing_strategy_in_block_headers()
-> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);
    let archive = pipeline
        .archive_file(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    let (header, payload) = reader.read_block(0)?;

    assert_eq!(header.strategy()?, PreProcessingStrategy::None);
    assert_eq!(header.compression()?, CompressionAlgo::Lz4);
    assert_eq!(payload.len(), header.encoded_len as usize);

    Ok(())
}

#[test]
fn pipeline_reaches_buffer_pool_steady_state() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(64 * 1024, 128));
    let pipeline = build_pipeline(
        512 * 1024,
        1,
        Arc::clone(&buffer_pool),
        CompressionAlgo::Lz4,
    );

    let _archive = pipeline.archive_file(
        file.path(),
        Vec::new(),
        RunTelemetryOptions::default(),
        None,
    )?;
    let created_after_warmup = buffer_pool.metrics().created;

    for _ in 0..5 {
        let _archive = pipeline.archive_file(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?;
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
    write_directory_file(&source, "nested/empty.bin", b"")?;
    std::fs::create_dir_all(source.path().join("empty/leaf"))?;

    let buffer_pool = Arc::new(BufferPool::new(32 * 1024, 128));
    let pipeline = build_pipeline(16 * 1024, 4, Arc::clone(&buffer_pool), CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_directory(
            source.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;

    let out = tempfile::tempdir()?;
    let report = pipeline.extract_directory_archive(
        Cursor::new(archive),
        out.path(),
        RunTelemetryOptions::default(),
        None,
    )?;

    assert_eq!(report.source_kind, oxide_core::ArchiveSourceKind::Directory);
    assert_eq!(std::fs::read(out.path().join("top.txt"))?, b"top-level");
    assert_eq!(
        std::fs::read(out.path().join("nested/a.bin"))?,
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        std::fs::read(out.path().join("nested/deeper/b.txt"))?,
        b"deep-content"
    );
    assert_eq!(
        std::fs::read(out.path().join("nested/empty.bin"))?,
        Vec::<u8>::new()
    );
    assert!(out.path().join("empty/leaf").is_dir());

    Ok(())
}

#[test]
fn archive_path_supports_directory_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "sample.txt", b"directory mode")?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            source.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let out = tempfile::tempdir()?;
    let report = pipeline.extract_directory_archive(
        Cursor::new(archive),
        out.path(),
        RunTelemetryOptions::default(),
        None,
    )?;

    assert_eq!(report.source_kind, oxide_core::ArchiveSourceKind::Directory);
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
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            source.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(u32::from(reader.global_header().feature_bits) & 1, 1);
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
    let pipeline = build_pipeline(32, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            source.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
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
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let out_root = tempfile::tempdir()?;
    let out_file = out_root.path().join("restored.txt");
    let report = pipeline.extract_path(
        Cursor::new(archive),
        &out_file,
        RunTelemetryOptions::default(),
        None,
    )?;

    assert_eq!(report.source_kind, oxide_core::ArchiveSourceKind::File);
    assert_eq!(std::fs::read(out_file)?, data);
    assert!(matches!(
        report.extensions.get("pipeline.decode_task_queue_capacity"),
        Some(ReportValue::U64(value)) if *value > 0
    ));
    assert!(matches!(
        report.extensions.get("pipeline.decode_result_queue_peak"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        report.extensions.get("pipeline.reorder_pending_limit"),
        Some(ReportValue::U64(value)) if *value > 0
    ));
    assert!(report.main_thread.stage_us.contains_key("ordered_write"));
    Ok(())
}

#[test]
fn extract_path_restores_directory_payload() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "nested/data.bin", &[9, 8, 7])?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            source.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let out_root = tempfile::tempdir()?;
    let out_dir = out_root.path().join("restored-tree");
    let report = pipeline.extract_path(
        Cursor::new(archive),
        &out_dir,
        RunTelemetryOptions::default(),
        None,
    )?;

    assert_eq!(report.source_kind, oxide_core::ArchiveSourceKind::Directory);
    assert_eq!(
        std::fs::read(out_dir.join("nested/data.bin"))?,
        vec![9, 8, 7]
    );
    assert!(matches!(
        report.extensions.get("extract.directory_entries"),
        Some(ReportValue::U64(2))
    ));
    assert!(report.main_thread.stage_us.contains_key("directory_decode"));
    assert!(report.main_thread.stage_us.contains_key("output_write"));
    Ok(())
}

#[test]
fn archive_reader_exposes_directory_manifest() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "nested/data.bin", &[9, 8, 7])?;
    write_directory_file(&source, "nested/empty.bin", b"")?;
    std::fs::create_dir_all(source.path().join("empty/leaf"))?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            source.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let reader = ArchiveReader::new(Cursor::new(archive))?;
    let manifest = reader.manifest().expect("missing archive manifest");
    assert_eq!(
        manifest.entries(),
        vec![
            oxide_core::ArchiveListingEntry {
                path: "empty".to_string(),
                kind: ArchiveEntryKind::Directory,
                size: 0,
            },
            oxide_core::ArchiveListingEntry {
                path: "empty/leaf".to_string(),
                kind: ArchiveEntryKind::Directory,
                size: 0,
            },
            oxide_core::ArchiveListingEntry {
                path: "nested".to_string(),
                kind: ArchiveEntryKind::Directory,
                size: 0,
            },
            oxide_core::ArchiveListingEntry {
                path: "nested/data.bin".to_string(),
                kind: ArchiveEntryKind::File,
                size: 3,
            },
            oxide_core::ArchiveListingEntry {
                path: "nested/empty.bin".to_string(),
                kind: ArchiveEntryKind::File,
                size: 0,
            },
        ]
    );
    Ok(())
}

#[test]
fn archive_reader_exposes_file_manifest() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let reader = ArchiveReader::new(Cursor::new(archive))?;
    let manifest = reader.manifest().expect("missing archive manifest");
    assert_eq!(
        manifest.entries(),
        vec![oxide_core::ArchiveListingEntry {
            path: file
                .path()
                .file_name()
                .and_then(|value| value.to_str())
                .expect("utf8 file name")
                .to_string(),
            kind: ArchiveEntryKind::File,
            size: data.len() as u64,
        }]
    );
    Ok(())
}

#[test]
fn extract_archive_ignores_footer_crc_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(96 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let mut archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let footer_crc_offset = archive.len() - FOOTER_SIZE + 4;
    archive[footer_crc_offset] ^= 0x5A;

    let (restored, _report) =
        pipeline.extract_archive(Cursor::new(archive), RunTelemetryOptions::default(), None)?;
    assert_eq!(restored, data);

    Ok(())
}

#[test]
fn extract_archive_ignores_payload_checksum_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(96 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);

    let mut archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;
    let reader = ArchiveReader::new(Cursor::new(archive.clone()))?;
    let payload_entry_index = reader
        .section_table()
        .iter()
        .position(|entry| entry.section_type == oxide_core::SectionType::PayloadRegion)
        .expect("payload section");
    let payload_checksum_offset =
        GLOBAL_HEADER_SIZE + (payload_entry_index * SECTION_TABLE_ENTRY_SIZE) + 20;
    archive[payload_checksum_offset] ^= 0xA5;

    let (restored, _report) =
        pipeline.extract_archive(Cursor::new(archive), RunTelemetryOptions::default(), None)?;
    assert_eq!(restored, data);

    Ok(())
}

#[test]
fn archive_path_reports_progress_and_extensible_stats() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(256 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(32 * 1024, 64));
    let pipeline = build_pipeline(16 * 1024, 4, buffer_pool, CompressionAlgo::Lz4);
    let mut sink = CollectProgress::default();

    let run = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions {
            progress_interval: Duration::from_millis(1),
            emit_final_progress: true,
            include_telemetry_snapshot: true,
        },
        Some(&mut sink),
    )?;
    assert!(!sink.snapshots.is_empty());

    let final_snapshot = sink.snapshots.last().expect("missing final snapshot");
    assert_eq!(final_snapshot.blocks_completed, final_snapshot.blocks_total);
    assert!(final_snapshot.preprocessing_avg_bps >= 0.0);
    assert!(final_snapshot.compression_avg_bps >= 0.0);
    assert!(final_snapshot.preprocessing_compression_avg_bps >= 0.0);
    assert!(final_snapshot.preprocessing_wall_avg_bps >= 0.0);
    assert!(final_snapshot.compression_wall_avg_bps >= 0.0);
    assert!(final_snapshot.preprocessing_compression_wall_avg_bps >= 0.0);

    let report = &run.report;
    assert_eq!(report.blocks_total, final_snapshot.blocks_total);
    assert_eq!(report.blocks_completed, final_snapshot.blocks_completed);
    assert!(matches!(
        report.extensions.get("runtime.completed"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        report.extensions.get("runtime.worker_count"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        report.extensions.get("runtime.effective_cores"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report.extensions.get("pipeline.max_inflight_blocks"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        report.extensions.get("pipeline.max_inflight_bytes"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        report.extensions.get("stage.writer_us"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        report.extensions.get("throughput.preprocessing_avg_bps"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report.extensions.get("throughput.compression_avg_bps"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report
            .extensions
            .get("throughput.preprocessing_compression_avg_bps"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report
            .extensions
            .get("throughput.preprocessing_wall_avg_bps"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report.extensions.get("throughput.compression_wall_avg_bps"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report
            .extensions
            .get("throughput.preprocessing_compression_wall_avg_bps"),
        Some(ReportValue::F64(_))
    ));
    assert!(matches!(
        report.extensions.get("archive.output_input_ratio"),
        Some(ReportValue::F64(_))
    ));

    let (restored, _) = pipeline.extract_archive(
        Cursor::new(run.writer),
        RunTelemetryOptions::default(),
        None,
    )?;
    assert_eq!(restored, data);
    Ok(())
}

#[test]
fn archive_options_can_disable_final_progress_emit() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(64 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 32));
    let pipeline = build_pipeline(8 * 1024, 2, buffer_pool, CompressionAlgo::Lz4);
    let mut sink = CollectProgress::default();

    let run = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions {
            progress_interval: Duration::from_secs(3600),
            emit_final_progress: false,
            include_telemetry_snapshot: true,
        },
        Some(&mut sink),
    )?;
    assert!(sink.snapshots.is_empty());
    assert!(run.report.blocks_total > 0);

    Ok(())
}

#[test]
fn directory_progress_reports_stable_block_total() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    write_directory_file(&source, "a.txt", b"alpha\nbeta\ngamma\n")?;
    write_directory_file(&source, "b.bin", &[0x55, 0x48, 0x89, 0xE5, 0xC3])?;
    write_directory_file(&source, "nested/c.txt", b"nested text file\n")?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(64, 2, buffer_pool, CompressionAlgo::Lz4);
    let mut sink = CollectProgress::default();

    let run = pipeline.archive_path(
        source.path(),
        Vec::new(),
        RunTelemetryOptions {
            progress_interval: Duration::from_millis(1),
            emit_final_progress: true,
            include_telemetry_snapshot: true,
        },
        Some(&mut sink),
    )?;
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
    assert_eq!(run.report.blocks_total, expected_total);

    let reader = ArchiveReader::new(Cursor::new(run.writer))?;
    assert_eq!(reader.block_count(), expected_total);

    Ok(())
}

#[test]
fn extract_progress_reports_runtime_worker_snapshots() -> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(192 * 1024);
    let file = write_fixture(&data)?;

    let buffer_pool = Arc::new(BufferPool::new(16 * 1024, 64));
    let pipeline = build_pipeline(8 * 1024, 3, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;

    let mut sink = CollectExtractProgress::default();
    let (restored, report) = pipeline.extract_archive(
        Cursor::new(archive),
        RunTelemetryOptions {
            progress_interval: Duration::from_millis(1),
            emit_final_progress: true,
            include_telemetry_snapshot: true,
        },
        Some(&mut sink),
    )?;

    assert_eq!(restored, data);
    assert!(report.blocks_total > 0);
    assert!(!sink.snapshots.is_empty());

    let final_snapshot = sink.snapshots.last().expect("missing extract progress");
    assert_eq!(final_snapshot.blocks_completed, final_snapshot.blocks_total);
    assert!(final_snapshot.runtime.workers.len() >= 3);
    assert!(final_snapshot.read_avg_bps >= 0.0);
    assert!(final_snapshot.decode_avg_bps >= 0.0);
    assert!(final_snapshot.decode_archive_ratio.is_finite());

    Ok(())
}

#[test]
fn extract_archive_handles_queue_pressure_without_deadlock()
-> Result<(), Box<dyn std::error::Error>> {
    let data = build_text_fixture(192 * 1024);
    let file = write_fixture(&data)?;

    let workers = 1usize;
    let buffer_pool = Arc::new(BufferPool::new(4 * 1024, 128));
    let pipeline = build_pipeline(1024, workers, buffer_pool, CompressionAlgo::Lz4);

    let archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;

    let mut sink = CollectExtractProgress::default();
    let (restored, report) = pipeline.extract_archive(
        Cursor::new(archive),
        RunTelemetryOptions {
            progress_interval: Duration::from_millis(1),
            emit_final_progress: true,
            include_telemetry_snapshot: true,
        },
        Some(&mut sink),
    )?;

    assert_eq!(restored, data);
    assert!(report.blocks_total > (workers as u32 * 9));
    assert!(!sink.snapshots.is_empty());

    let mut last_completed = 0u32;
    for snapshot in &sink.snapshots {
        assert!(snapshot.blocks_completed >= last_completed);
        last_completed = snapshot.blocks_completed;
    }

    let final_snapshot = sink.snapshots.last().expect("missing extract progress");
    assert_eq!(final_snapshot.blocks_completed, final_snapshot.blocks_total);

    Ok(())
}
