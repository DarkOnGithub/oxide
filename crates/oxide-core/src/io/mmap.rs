use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};

use crate::telemetry;
use crate::telemetry::profile;
use crate::telemetry::tags;
use crate::types::{BatchData, Result};
use crate::OxideError;

const PROFILE_TAG_STACK_MMAP: [&str; 2] = [tags::TAG_SYSTEM, tags::TAG_MMAP];

/// Memory-mapped file input for efficient large file access.
///
/// Uses the operating system's virtual memory manager to map a file
/// directly into the process address space, allowing efficient
/// random access without loading the entire file into RAM.
///
/// # Safety
///
/// Memory mapping is generally safe but has platform-specific
/// considerations. See the `memmap2` crate documentation for details.
///
/// # Example
/// ```no_run
/// use oxide_core::MmapInput;
/// use std::path::Path;
///
/// let input = MmapInput::open(Path::new("data.bin"))?;
/// let data = input.as_bytes()?;
/// # Ok::<(), oxide_core::OxideError>(())
/// ```
#[derive(Debug, Clone)]
pub struct MmapInput {
    mmap: Option<Arc<Mmap>>,
    path: PathBuf,
    len: u64,
}

impl MmapInput {
    /// Opens a file for memory-mapped access.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or mapped.
    pub fn open(path: &Path) -> Result<Self> {
        let started_at = Instant::now();
        let result = (|| {
            let file = File::open(path)?;
            let len = file.metadata()?.len();

            let mmap = if len == 0 {
                None
            } else {
                Some(Arc::new(unsafe { MmapOptions::new().map(&file)? }))
            };

            Ok(Self {
                mmap,
                path: path.to_path_buf(),
                len,
            })
        })();

        let elapsed_us = profile::elapsed_us(started_at);
        telemetry::increment_counter(
            tags::METRIC_MMAP_OPEN_COUNT,
            1,
            &[("subsystem", "mmap"), ("op", "open")],
        );
        telemetry::record_histogram(
            tags::METRIC_MMAP_OPEN_LATENCY_US,
            elapsed_us,
            &[("subsystem", "mmap"), ("op", "open")],
        );
        telemetry::sample_process_memory();

        match &result {
            Ok(_input) => {
                profile::event(
                    tags::PROFILE_MMAP,
                    &PROFILE_TAG_STACK_MMAP,
                    "open",
                    "ok",
                    elapsed_us,
                    "mmap open completed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_MMAP) {
                    tracing::debug!(
                        target: tags::PROFILE_MMAP,
                        op = "open",
                        result = "ok",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_MMAP,
                        file_len = _input.len_u64(),
                        empty_file = _input.is_empty(),
                        path = %path.display(),
                        "mmap open context"
                    );
                }
            }
            Err(_error) => {
                profile::event(
                    tags::PROFILE_MMAP,
                    &PROFILE_TAG_STACK_MMAP,
                    "open",
                    "error",
                    elapsed_us,
                    "mmap open failed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_MMAP) {
                    tracing::debug!(
                        target: tags::PROFILE_MMAP,
                        op = "open",
                        result = "error",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_MMAP,
                        path = %path.display(),
                        error = %_error,
                        "mmap open context"
                    );
                }
            }
        }

        result
    }

    /// Returns the path of the opened file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the file length as a u64.
    pub fn len_u64(&self) -> u64 {
        self.len
    }

    /// Returns the file length as a usize, clamped to usize::MAX.
    pub fn len(&self) -> usize {
        self.len.min(usize::MAX as u64) as usize
    }

    /// Returns true if the file is empty (zero bytes).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a slice of the file as a BatchData object (zero-copy if possible).
    ///
    /// # Arguments
    /// * `start` - Start byte position (inclusive)
    /// * `end` - End byte position (exclusive)
    ///
    /// # Errors
    /// Returns an error if the range is invalid.
    pub fn mapped_slice(&self, start: usize, end: usize) -> Result<BatchData> {
        self.mapped_slice_u64(start as u64, end as u64)
    }

    /// Returns a slice of the file as a BatchData object using u64 indices.
    ///
    /// # Arguments
    /// * `start` - Start byte position (inclusive)
    /// * `end` - End byte position (exclusive)
    ///
    /// # Errors
    /// Returns an error if the range is invalid or overflows usize.
    pub fn mapped_slice_u64(&self, start: u64, end: u64) -> Result<BatchData> {
        let (start, end) = self.validate_range(start, end)?;

        match &self.mmap {
            Some(map) => Ok(BatchData::Mapped {
                map: Arc::clone(map),
                start,
                end,
            }),
            None => Ok(BatchData::Owned(Bytes::new())),
        }
    }

    /// Returns a slice of the file as a Bytes object.
    ///
    /// # Arguments
    /// * `start` - Start byte position (inclusive)
    /// * `end` - End byte position (exclusive)
    ///
    /// # Errors
    /// Returns an error if the range is invalid.
    pub fn slice(&self, start: usize, end: usize) -> Result<Bytes> {
        self.slice_u64(start as u64, end as u64)
    }

    /// Returns a slice of the file as a Bytes object using u64 indices.
    ///
    /// # Arguments
    /// * `start` - Start byte position (inclusive)
    /// * `end` - End byte position (exclusive)
    ///
    /// # Errors
    /// Returns an error if the range is invalid or overflows usize.
    pub fn slice_u64(&self, start: u64, end: u64) -> Result<Bytes> {
        let started_at = Instant::now();
        let result = (|| {
            let (start, end) = self.validate_range(start, end)?;

            match &self.mmap {
                Some(mmap) => Ok(Bytes::copy_from_slice(&mmap[start..end])),
                None => Ok(Bytes::new()),
            }
        })();

        let elapsed_us = profile::elapsed_us(started_at);
        telemetry::increment_counter(
            tags::METRIC_MMAP_SLICE_COUNT,
            1,
            &[("subsystem", "mmap"), ("op", "slice")],
        );
        telemetry::record_histogram(
            tags::METRIC_MMAP_SLICE_LATENCY_US,
            elapsed_us,
            &[("subsystem", "mmap"), ("op", "slice")],
        );

        match &result {
            Ok(_bytes) => {
                profile::event(
                    tags::PROFILE_MMAP,
                    &PROFILE_TAG_STACK_MMAP,
                    "slice",
                    "ok",
                    elapsed_us,
                    "mmap slice completed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_MMAP) {
                    tracing::debug!(
                        target: tags::PROFILE_MMAP,
                        op = "slice",
                        result = "ok",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_MMAP,
                        start,
                        end,
                        slice_len = _bytes.len(),
                        "mmap slice context"
                    );
                }
            }
            Err(_error) => {
                profile::event(
                    tags::PROFILE_MMAP,
                    &PROFILE_TAG_STACK_MMAP,
                    "slice",
                    "error",
                    elapsed_us,
                    "mmap slice failed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_MMAP) {
                    tracing::debug!(
                        target: tags::PROFILE_MMAP,
                        op = "slice",
                        result = "error",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_MMAP,
                        start,
                        end,
                        error = %_error,
                        "mmap slice context"
                    );
                }
            }
        }

        result
    }

    /// Returns the entire file contents as a Bytes object.
    ///
    /// # Errors
    /// Returns an error if the file is too large for memory.
    pub fn as_bytes(&self) -> Result<Bytes> {
        let started_at = Instant::now();
        let result = self.slice_u64(0, self.len);
        let elapsed_us = profile::elapsed_us(started_at);

        match &result {
            Ok(_bytes) => {
                profile::event(
                    tags::PROFILE_MMAP,
                    &PROFILE_TAG_STACK_MMAP,
                    "as_bytes",
                    "ok",
                    elapsed_us,
                    "mmap as_bytes completed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_MMAP) {
                    tracing::debug!(
                        target: tags::PROFILE_MMAP,
                        op = "as_bytes",
                        result = "ok",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_MMAP,
                        len = _bytes.len(),
                        "mmap as_bytes context"
                    );
                }
            }
            Err(_error) => {
                profile::event(
                    tags::PROFILE_MMAP,
                    &PROFILE_TAG_STACK_MMAP,
                    "as_bytes",
                    "error",
                    elapsed_us,
                    "mmap as_bytes failed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_MMAP) {
                    tracing::debug!(
                        target: tags::PROFILE_MMAP,
                        op = "as_bytes",
                        result = "error",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_MMAP,
                        error = %_error,
                        "mmap as_bytes context"
                    );
                }
            }
        }

        result
    }

    fn validate_range(&self, start: u64, end: u64) -> Result<(usize, usize)> {
        if start > end || end > self.len {
            return Err(OxideError::InvalidFormat("invalid mmap slice range"));
        }

        let start = usize::try_from(start)
            .map_err(|_| OxideError::InvalidFormat("range start overflow"))?;
        let end =
            usize::try_from(end).map_err(|_| OxideError::InvalidFormat("range end overflow"))?;

        Ok((start, end))
    }
}
