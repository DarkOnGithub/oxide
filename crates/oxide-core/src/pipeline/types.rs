use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::CompressionPreset;
use crate::buffer::BufferPool;
use crate::types::CompressionAlgo;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveSourceKind {
    File,
    Directory,
}

/// Extensible metric value used by pipeline internals before report export.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    U64(u64),
    F64(f64),
    Text(String),
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

/// Construction config for the archive pipeline.
#[derive(Debug, Clone)]
pub struct ArchivePipelineConfig {
    pub target_block_size: usize,
    pub workers: usize,
    pub buffer_pool: Arc<BufferPool>,
    pub compression_algo: CompressionAlgo,
    pub performance: PipelinePerformanceOptions,
}

impl ArchivePipelineConfig {
    pub fn new(
        target_block_size: usize,
        workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        Self {
            target_block_size,
            workers,
            buffer_pool,
            compression_algo,
            performance: PipelinePerformanceOptions::default(),
        }
    }
}
