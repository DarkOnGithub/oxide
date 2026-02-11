use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::CompressionPreset;
use crate::core::{PoolRuntimeSnapshot, WorkerRuntimeSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveSourceKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub struct ArchiveProgressSnapshot {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub input_bytes_total: u64,
    pub input_bytes_completed: u64,
    pub output_bytes_completed: u64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub blocks_pending: u32,
    pub runtime: PoolRuntimeSnapshot,
}

/// Extensible metric value attached to archive run stats.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    U64(u64),
    F64(f64),
    Duration(Duration),
    Text(String),
}

/// Runtime options for archive operations.
#[derive(Debug, Clone)]
pub struct ArchiveOptions {
    pub progress_interval: Duration,
    pub emit_final_progress: bool,
}

impl Default for ArchiveOptions {
    fn default() -> Self {
        Self {
            progress_interval: Duration::from_millis(250),
            emit_final_progress: true,
        }
    }
}

/// Throughput-oriented knobs for archive/extract behavior.
#[derive(Debug, Clone)]
pub struct PipelinePerformanceOptions {
    /// Enables block-size autotuning before archive work starts.
    pub autotune_enabled: bool,
    /// Minimum total input bytes required before autotune is considered.
    pub autotune_min_input_bytes: u64,
    /// Maximum bytes sampled for autotune scoring.
    pub autotune_sample_bytes: usize,
    /// Enables per-block raw passthrough when compression does not reduce size.
    pub raw_fallback_enabled: bool,
    /// Compression preset metadata stored in each block.
    pub compression_preset: CompressionPreset,
    /// Maximum in-flight block payload bytes pending worker completion.
    pub max_inflight_bytes: usize,
    /// Maximum in-flight blocks scaled by worker count.
    pub max_inflight_blocks_per_worker: usize,
    /// Streaming read buffer size used by directory producer path.
    pub directory_stream_read_buffer_size: usize,
    /// Preserves file format boundaries when building directory batches.
    pub preserve_directory_format_boundaries: bool,
    /// Timeout used when waiting for worker results.
    pub result_wait_timeout: Duration,
}

impl Default for PipelinePerformanceOptions {
    fn default() -> Self {
        Self {
            autotune_enabled: false,
            autotune_min_input_bytes: 256 * 1024 * 1024,
            autotune_sample_bytes: 128 * 1024 * 1024,
            raw_fallback_enabled: true,
            compression_preset: CompressionPreset::Fast,
            max_inflight_bytes: 512 * 1024 * 1024,
            max_inflight_blocks_per_worker: 32,
            directory_stream_read_buffer_size: 16 * 1024 * 1024,
            preserve_directory_format_boundaries: false,
            result_wait_timeout: Duration::from_millis(5),
        }
    }
}

/// Full output from archive operations that track run metadata.
#[derive(Debug)]
pub struct ArchiveOutcome<W> {
    pub writer: W,
    pub stats: ArchiveRunStats,
}

#[derive(Debug, Clone)]
pub struct ArchiveRunStats {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub input_bytes_total: u64,
    pub output_bytes_total: u64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub workers: Vec<WorkerRuntimeSnapshot>,
    pub extensions: BTreeMap<String, StatValue>,
}

impl ArchiveRunStats {
    pub fn extension(&self, key: &str) -> Option<&StatValue> {
        self.extensions.get(key)
    }

    pub fn extension_u64(&self, key: &str) -> Option<u64> {
        match self.extension(key) {
            Some(StatValue::U64(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn extension_f64(&self, key: &str) -> Option<f64> {
        match self.extension(key) {
            Some(StatValue::F64(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn extension_duration(&self, key: &str) -> Option<Duration> {
        match self.extension(key) {
            Some(StatValue::Duration(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn extension_text(&self, key: &str) -> Option<&str> {
        match self.extension(key) {
            Some(StatValue::Text(value)) => Some(value),
            _ => None,
        }
    }
}

/// Progress hook for archive operations.
pub trait ProgressSink {
    fn on_progress(&mut self, snapshot: ArchiveProgressSnapshot);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopProgress;

impl ProgressSink for NoopProgress {
    fn on_progress(&mut self, _snapshot: ArchiveProgressSnapshot) {}
}

pub(crate) struct FnProgressSink<F> {
    pub(crate) callback: F,
}

impl<F> ProgressSink for FnProgressSink<F>
where
    F: FnMut(ArchiveProgressSnapshot),
{
    fn on_progress(&mut self, snapshot: ArchiveProgressSnapshot) {
        (self.callback)(snapshot);
    }
}
