use std::io::{Cursor, Write};
use std::sync::Arc;

use oxide_core::{ArchivePipeline, ArchiveReader, BufferPool, CompressionAlgo};
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
