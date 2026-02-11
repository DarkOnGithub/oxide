use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Context;
use crossbeam_channel::Sender;
use oxide_core::telemetry::events::{self, GlobalTelemetrySink};
use oxide_core::{
    ArchivePipeline, ArchivePipelineConfig, ArchiveReport, BufferPool, CompressionAlgo,
    ExtractReport, PipelinePerformanceOptions, RunTelemetryOptions, TelemetryEvent, TelemetrySink,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobMode {
    Archive,
    Extract,
}

#[derive(Debug, Clone)]
pub struct JobConfig {
    pub mode: JobMode,
    pub input_path: PathBuf,
    pub output_path: PathBuf,
    pub workers: usize,
    pub block_size: usize,
    pub compression: CompressionAlgo,
    pub stats_interval: Duration,
}

#[derive(Debug, Clone)]
pub enum JobResult {
    Archive(ArchiveReport),
    Extract(ExtractReport),
}

#[derive(Debug, Clone)]
pub enum JobEvent {
    Telemetry(TelemetryEvent),
    JobStarted {
        mode: JobMode,
        input_path: PathBuf,
        output_path: PathBuf,
    },
    JobFinished {
        result: Result<JobResult, String>,
    },
}

pub fn spawn_job(config: JobConfig, tx: Sender<JobEvent>) {
    thread::spawn(move || {
        let _ = tx.send(JobEvent::JobStarted {
            mode: config.mode,
            input_path: config.input_path.clone(),
            output_path: config.output_path.clone(),
        });

        let mut sink = ChannelTelemetrySink { tx: tx.clone() };
        events::set_global_sink(Some(Box::new(ChannelGlobalSink { tx: tx.clone() })));

        let result = match run_job(config, &mut sink) {
            Ok(report) => Ok(report),
            Err(error) => Err(format!("{error:#}")),
        };

        events::set_global_sink(None);
        let _ = tx.send(JobEvent::JobFinished { result });
    });
}

fn run_job(config: JobConfig, sink: &mut dyn TelemetrySink) -> anyhow::Result<JobResult> {
    match config.mode {
        JobMode::Archive => run_archive(config, sink),
        JobMode::Extract => run_extract(config, sink),
    }
}

fn run_archive(config: JobConfig, sink: &mut dyn TelemetrySink) -> anyhow::Result<JobResult> {
    if let Some(parent) = config
        .output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    let buffer_pool = Arc::new(BufferPool::new(
        1024 * 1024,
        config.workers.saturating_mul(16),
    ));
    let mut pipeline_config = ArchivePipelineConfig::new(
        config.block_size.max(1),
        config.workers.max(1),
        buffer_pool,
        config.compression,
    );
    pipeline_config.performance = PipelinePerformanceOptions::default();
    let pipeline = ArchivePipeline::new(pipeline_config);

    let output = File::create(&config.output_path)
        .with_context(|| format!("create output file {}", config.output_path.display()))?;

    let run_options = RunTelemetryOptions {
        progress_interval: config.stats_interval,
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };

    let run = pipeline
        .archive_path(&config.input_path, output, run_options, Some(sink))
        .with_context(|| {
            format!(
                "archive {} -> {}",
                config.input_path.display(),
                config.output_path.display()
            )
        })?;

    Ok(JobResult::Archive(run.report))
}

fn run_extract(config: JobConfig, sink: &mut dyn TelemetrySink) -> anyhow::Result<JobResult> {
    if let Some(parent) = config
        .output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    let buffer_pool = Arc::new(BufferPool::new(
        1024 * 1024,
        config.workers.saturating_mul(8).max(8),
    ));
    let mut pipeline_config = ArchivePipelineConfig::new(
        1024 * 1024,
        config.workers.max(1),
        buffer_pool,
        CompressionAlgo::Lz4,
    );
    pipeline_config.performance = PipelinePerformanceOptions::default();
    let pipeline = ArchivePipeline::new(pipeline_config);

    let input = File::open(&config.input_path)
        .with_context(|| format!("open archive {}", config.input_path.display()))?;

    let run_options = RunTelemetryOptions {
        progress_interval: config.stats_interval,
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };

    let report = pipeline
        .extract_path(input, &config.output_path, run_options, Some(sink))
        .with_context(|| {
            format!(
                "extract {} -> {}",
                config.input_path.display(),
                config.output_path.display()
            )
        })?;

    Ok(JobResult::Extract(report))
}

#[derive(Clone)]
struct ChannelTelemetrySink {
    tx: Sender<JobEvent>,
}

impl TelemetrySink for ChannelTelemetrySink {
    fn on_event(&mut self, event: TelemetryEvent) {
        let _ = self.tx.send(JobEvent::Telemetry(event));
    }
}

#[derive(Clone)]
struct ChannelGlobalSink {
    tx: Sender<JobEvent>,
}

impl GlobalTelemetrySink for ChannelGlobalSink {
    fn on_event(&mut self, event: TelemetryEvent) {
        let _ = self.tx.send(JobEvent::Telemetry(event));
    }
}
