use std::fs;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::CompressionPreset;
use crate::buffer::BufferPool;
use crate::types::CompressionAlgo;

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

impl ArchiveEntryKind {
    pub(crate) const FLAG_KIND_MASK: u8 = 0b0000_0011;
    pub(crate) const FLAG_DIRECTORY: u8 = 0;
    pub(crate) const FLAG_FILE: u8 = 1;
    pub(crate) const FLAG_SYMLINK: u8 = 2;
    pub(crate) const FLAG_HARDLINK: u8 = 3;

    pub(crate) fn to_flags(self) -> u8 {
        match self {
            Self::Directory => Self::FLAG_DIRECTORY,
            Self::File => Self::FLAG_FILE,
        }
    }

    pub(crate) fn from_flags(flags: u8) -> Option<Self> {
        match flags & Self::FLAG_KIND_MASK {
            Self::FLAG_DIRECTORY => Some(Self::Directory),
            Self::FLAG_FILE => Some(Self::File),
            Self::FLAG_SYMLINK | Self::FLAG_HARDLINK => None,
            _ => None,
        }
    }
}

/// Cross-platform timestamp metadata stored for archive entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ArchiveTimestamp {
    /// Whole seconds relative to the Unix epoch.
    pub seconds: i64,
    /// Additional nanoseconds within the second.
    pub nanoseconds: u32,
}

impl ArchiveTimestamp {
    pub fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => Self {
                seconds: duration.as_secs() as i64,
                nanoseconds: duration.subsec_nanos(),
            },
            Err(error) => {
                let duration = error.duration();
                if duration.subsec_nanos() == 0 {
                    Self {
                        seconds: -(duration.as_secs() as i64),
                        nanoseconds: 0,
                    }
                } else {
                    Self {
                        seconds: -(duration.as_secs() as i64) - 1,
                        nanoseconds: 1_000_000_000 - duration.subsec_nanos(),
                    }
                }
            }
        }
    }

    pub fn to_system_time(self) -> SystemTime {
        if self.seconds >= 0 {
            UNIX_EPOCH
                + Duration::from_secs(self.seconds as u64)
                + Duration::from_nanos(self.nanoseconds as u64)
        } else {
            let seconds = self.seconds.unsigned_abs();
            let duration = if self.nanoseconds == 0 {
                Duration::from_secs(seconds)
            } else {
                Duration::from_secs(seconds.saturating_sub(1))
                    + Duration::from_nanos((1_000_000_000 - self.nanoseconds) as u64)
            };
            UNIX_EPOCH - duration
        }
    }
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
    /// Unix file mode bits. Directory and file entries both preserve this.
    pub mode: u32,
    /// Last modification time recorded for the entry.
    pub mtime: ArchiveTimestamp,
    /// Unix owner identifier captured during archive creation.
    pub uid: u32,
    /// Unix group identifier captured during archive creation.
    pub gid: u32,
    /// Logical byte offset of the entry payload in the uncompressed content stream.
    pub content_offset: u64,
}

impl ArchiveListingEntry {
    pub fn directory(path: String, mode: u32, mtime: ArchiveTimestamp, uid: u32, gid: u32) -> Self {
        Self {
            path,
            kind: ArchiveEntryKind::Directory,
            size: 0,
            mode,
            mtime,
            uid,
            gid,
            content_offset: 0,
        }
    }

    pub fn file(
        path: String,
        size: u64,
        mode: u32,
        mtime: ArchiveTimestamp,
        uid: u32,
        gid: u32,
        content_offset: u64,
    ) -> Self {
        Self {
            path,
            kind: ArchiveEntryKind::File,
            size,
            mode,
            mtime,
            uid,
            gid,
            content_offset,
        }
    }

    pub fn from_metadata(
        path: String,
        kind: ArchiveEntryKind,
        size: u64,
        metadata: &fs::Metadata,
        content_offset: u64,
    ) -> crate::types::Result<Self> {
        let mtime = ArchiveTimestamp::from_system_time(metadata.modified()?);
        let mode = metadata_mode(metadata);
        let (uid, gid) = metadata_owner_ids(metadata);
        Ok(match kind {
            ArchiveEntryKind::Directory => Self::directory(path, mode, mtime, uid, gid),
            ArchiveEntryKind::File => Self::file(path, size, mode, mtime, uid, gid, content_offset),
        })
    }

    pub fn content_range(&self) -> Range<u64> {
        self.content_offset..self.content_offset.saturating_add(self.size)
    }
}

#[cfg(unix)]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt;

    metadata.mode()
}

#[cfg(not(unix))]
fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    if metadata.permissions().readonly() {
        0o444
    } else {
        0o666
    }
}

#[cfg(unix)]
fn metadata_owner_ids(metadata: &fs::Metadata) -> (u32, u32) {
    use std::os::unix::fs::MetadataExt;

    (metadata.uid(), metadata.gid())
}

#[cfg(not(unix))]
fn metadata_owner_ids(_: &fs::Metadata) -> (u32, u32) {
    (0, 0)
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
    /// Optional explicit zstd compression level used only during encoding.
    pub zstd_level: Option<i32>,
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
            zstd_level: None,
            max_inflight_bytes: 512 * 1024 * 1024,
            max_inflight_blocks_per_worker: 256,
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
    /// Whether preprocessing should be skipped entirely.
    pub skip_preprocessing: bool,
    /// Whether compression should be skipped entirely.
    pub skip_compression: bool,
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
            skip_preprocessing: false,
            skip_compression: false,
            performance: PipelinePerformanceOptions::default(),
        }
    }
}
