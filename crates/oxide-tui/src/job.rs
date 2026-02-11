use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    pub output_display: String,
    pub ephemeral_output: bool,
    pub null_output: bool,
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
        output_path: String,
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
            output_path: config.output_display.clone(),
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
    let run_options = RunTelemetryOptions {
        progress_interval: config.stats_interval,
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };

    let output_path = resolve_output_path(&config);

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

    if config.null_output {
        return pipeline
            .archive_path(&config.input_path, io::sink(), run_options, Some(sink))
            .with_context(|| format!("archive {} -> null sink", config.input_path.display()))
            .map(|run| JobResult::Archive(run.report));
    }

    if let Some(parent) = output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    let output = File::create(&output_path)
        .with_context(|| format!("create output file {}", output_path.display()))?;

    let result = pipeline
        .archive_path(&config.input_path, output, run_options, Some(sink))
        .with_context(|| {
            format!(
                "archive {} -> {}",
                config.input_path.display(),
                output_path.display()
            )
        })
        .map(|run| JobResult::Archive(run.report));

    cleanup_ephemeral_output_if_needed(config.ephemeral_output, &output_path);
    result
}

fn run_extract(config: JobConfig, sink: &mut dyn TelemetrySink) -> anyhow::Result<JobResult> {
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

    if config.null_output {
        return pipeline
            .extract_archive(input, run_options, Some(sink))
            .with_context(|| format!("extract {} -> null sink", config.input_path.display()))
            .map(|(_, report)| JobResult::Extract(report));
    }

    let output_path = resolve_output_path(&config);
    if let Some(parent) = output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    let result = pipeline
        .extract_path(input, &output_path, run_options, Some(sink))
        .with_context(|| {
            format!(
                "extract {} -> {}",
                config.input_path.display(),
                output_path.display()
            )
        })
        .map(JobResult::Extract);

    cleanup_ephemeral_output_if_needed(config.ephemeral_output, &output_path);
    result
}

fn resolve_output_path(config: &JobConfig) -> PathBuf {
    if config.ephemeral_output {
        build_ephemeral_output_path(config.mode, &config.input_path)
    } else {
        config.output_path.clone()
    }
}

fn build_ephemeral_output_path(mode: JobMode, input_path: &Path) -> PathBuf {
    let label = input_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            name.chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "output".to_string());
    let suffix = match mode {
        JobMode::Archive => ".oxz",
        JobMode::Extract => ".out",
    };
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    std::env::temp_dir().join(format!(
        "oxide-ephemeral-{label}-{}-{timestamp}{suffix}",
        std::process::id()
    ))
}

fn cleanup_ephemeral_output_if_needed(ephemeral_output: bool, output_path: &Path) {
    if !ephemeral_output {
        return;
    }

    if output_path.is_file() {
        let _ = std::fs::remove_file(output_path);
        return;
    }

    if output_path.is_dir() {
        let _ = std::fs::remove_dir_all(output_path);
        return;
    }

    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_dir_all(output_path);
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
