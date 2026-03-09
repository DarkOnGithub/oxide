use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::buffer::BufferPool;
use crate::types::CompressionAlgo;
use crate::CompressionPreset;

/// Indicates whether the archive source is a single file or a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveSourceKind {
    /// Source is a single file.
    File,
    /// Source is a directory tree.
    Directory,
}

/// Indicates whether an archive listing entry is a file or directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveEntryKind {
    /// Entry is a regular file.
    File,
    /// Entry is a directory.
    Directory,
}

/// Metadata for a single entry discovered while inspecting an archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveListingEntry {
    /// Path relative to the archive root.
    pub path: String,
    /// Entry kind.
    pub kind: ArchiveEntryKind,
    /// File size in bytes. Directory entries report `0`.
    pub size: u64,
}

/// Extensible metric value used by pipeline internals before report export.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    /// Unsigned 64-bit integer.
    U64(u64),
    /// 64-bit floating point number.
    F64(f64),
    /// UTF-8 string.
    Text(String),
}

/// Throughput-oriented knobs for archive/extract behavior.
///
/// These options allow fine-tuning the performance of the pipeline.
#[derive(Debug, Clone)]
pub struct PipelinePerformanceOptions {
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
    /// Total number of directory producer threads, including prefetch helpers.
    pub producer_threads: usize,
    /// File-size threshold above which directory input uses mmap fast-path.
    pub directory_mmap_threshold_bytes: usize,
    /// Capacity of the writer result queue (in blocks).
    pub writer_result_queue_blocks: usize,
}

impl Default for PipelinePerformanceOptions {
    fn default() -> Self {
        Self {
            raw_fallback_enabled: true,
            compression_preset: CompressionPreset::Fast,
            max_inflight_bytes: 512 * 1024 * 1024,
            max_inflight_blocks_per_worker: 32,
            directory_stream_read_buffer_size: 16 * 1024 * 1024,
            preserve_directory_format_boundaries: false,
            result_wait_timeout: Duration::from_millis(5),
            producer_threads: 1,
            directory_mmap_threshold_bytes: 8 * 1024 * 1024,
            writer_result_queue_blocks: 1024,
        }
    }
}

/// Construction config for the archive pipeline.
#[derive(Debug, Clone)]
pub struct ArchivePipelineConfig {
    /// Target size for data blocks.
    pub target_block_size: usize,
    /// Number of parallel workers.
    pub workers: usize,
    /// Shared buffer pool for memory management.
    pub buffer_pool: Arc<BufferPool>,
    /// Compression algorithm to use.
    pub compression_algo: CompressionAlgo,
    /// Performance tuning options.
    pub performance: PipelinePerformanceOptions,
}

impl ArchivePipelineConfig {
    /// Creates a new pipeline configuration with default performance options.
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
