use std::io::{Cursor, Write};
use std::sync::Arc;
use std::time::Duration;

use oxide_core::{
    ArchivePipeline, ArchivePipelineConfig, ArchiveSourceKind, BufferPool, CompressionAlgo,
    RunTelemetryOptions, telemetry,
};
use tempfile::{NamedTempFile, tempdir};

use crate::shared::TELEMETRY_TEST_MUTEX;

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
fn archive_report_can_include_telemetry_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = TELEMETRY_TEST_MUTEX
        .lock()
        .expect("telemetry test lock poisoned");

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

#[test]
fn telemetry_snapshot_is_scoped_to_each_run() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = TELEMETRY_TEST_MUTEX
        .lock()
        .expect("telemetry test lock poisoned");

    telemetry::reset();

    let data = vec![3u8; 32 * 1024];
    let file = write_fixture(&data)?;

    let pool = Arc::new(BufferPool::new(8 * 1024, 16));
    let pipeline = build_pipeline(4 * 1024, 2, pool, CompressionAlgo::Lz4);

    let first = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions::default(),
        None,
    )?;
    let second = pipeline.archive_path(
        file.path(),
        Vec::new(),
        RunTelemetryOptions::default(),
        None,
    )?;

    let first_snapshot = first
        .report
        .telemetry
        .as_ref()
        .expect("first telemetry snapshot missing");
    let second_snapshot = second
        .report
        .telemetry
        .as_ref()
        .expect("second telemetry snapshot missing");

    assert_eq!(
        first_snapshot.counter("oxide.pipeline.archive.run.count"),
        Some(1)
    );
    assert_eq!(
        second_snapshot.counter("oxide.pipeline.archive.run.count"),
        Some(1)
    );

    let first_hist = first_snapshot
        .histogram("oxide.pipeline.archive.run.latency_us")
        .expect("first archive latency histogram missing");
    let second_hist = second_snapshot
        .histogram("oxide.pipeline.archive.run.latency_us")
        .expect("second archive latency histogram missing");
    assert_eq!(first_hist.count, 1);
    assert_eq!(second_hist.count, 1);

    Ok(())
}
