use std::io::{Cursor, Write};
use std::sync::Arc;
use std::time::Duration;

use oxide_core::{
    ArchivePipeline, ArchivePipelineConfig, ArchiveSourceKind, BufferPool, CompressionAlgo,
    ReportExport, ReportValue, RunTelemetryOptions,
};
use tempfile::{NamedTempFile, tempdir};

fn write_fixture(data: &[u8]) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(data)?;
    file.flush()?;
    Ok(file)
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

#[test]
fn archive_run_exports_report_and_flat_map() -> Result<(), Box<dyn std::error::Error>> {
    let data = vec![b'a'; 128 * 1024];
    let file = write_fixture(&data)?;

    let pool = Arc::new(BufferPool::new(32 * 1024, 64));
    let pipeline = build_pipeline(16 * 1024, 2, pool, CompressionAlgo::Lz4);
    let run = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions {
            progress_interval: Duration::from_millis(1),
            emit_final_progress: true,
            include_telemetry_snapshot: true,
        },
        None,
    )?;

    let report = run.report;
    assert!(report.blocks_total > 0);
    assert!(!report.workers.is_empty());

    let map = report.to_flat_map();
    assert!(matches!(
        map.get("archive.input_bytes_total"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        map.get("archive.worker_count"),
        Some(ReportValue::U64(_))
    ));
    Ok(())
}

#[test]
fn extract_path_report_includes_worker_and_main_thread_metrics()
-> Result<(), Box<dyn std::error::Error>> {
    let data = b"oxide extract report payload".repeat(8192);
    let file = write_fixture(&data)?;

    let pool = Arc::new(BufferPool::new(32 * 1024, 64));
    let pipeline = build_pipeline(16 * 1024, 3, pool, CompressionAlgo::Deflate);
    let archive = pipeline
        .archive_path(
            file.path(),
            Vec::new(),
            RunTelemetryOptions::default(),
            None,
        )?
        .writer;

    let output_dir = tempdir()?;
    let output_path = output_dir.path().join("restored.bin");
    let report = pipeline.extract_path(
        Cursor::new(archive),
        &output_path,
        RunTelemetryOptions::default(),
        None,
    )?;

    assert_eq!(report.source_kind, ArchiveSourceKind::File);
    assert_eq!(std::fs::read(&output_path)?, data);
    assert!(report.blocks_total > 0);
    assert!(!report.workers.is_empty());
    assert!(report.main_thread.stage_us.contains_key("archive_read"));
    assert!(report.main_thread.stage_us.contains_key("decode_wait"));

    let map = report.to_flat_map();
    assert!(matches!(
        map.get("extract.output_bytes_total"),
        Some(ReportValue::U64(_))
    ));
    assert!(matches!(
        map.get("thread.main.stage.archive_read_us"),
        Some(ReportValue::U64(_))
    ));
    Ok(())
}

#[test]
fn archive_report_can_include_telemetry_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let data = vec![7u8; 64 * 1024];
    let file = write_fixture(&data)?;

    let pool = Arc::new(BufferPool::new(16 * 1024, 32));
    let pipeline = build_pipeline(8 * 1024, 2, pool, CompressionAlgo::Lz4);
    let run = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions::default(),
        None,
    )?;

    assert!(run.report.telemetry.is_some());
    Ok(())
}
