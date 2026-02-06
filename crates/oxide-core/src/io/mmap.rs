use std::fs::File;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};

use crate::types::Result;
use crate::OxideError;

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
#[derive(Debug)]
pub struct MmapInput {
    mmap: Option<Mmap>,
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
        let file = File::open(path)?;
        let len = file.metadata()?.len();

        let mmap = if len == 0 {
            None
        } else {
            Some(unsafe { MmapOptions::new().map(&file)? })
        };

        Ok(Self {
            mmap,
            path: path.to_path_buf(),
            len,
        })
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
        let (start, end) = self.validate_range(start, end)?;

        match &self.mmap {
            Some(mmap) => Ok(Bytes::copy_from_slice(&mmap[start..end])),
            None => Ok(Bytes::new()),
        }
    }

    /// Returns the entire file contents as a Bytes object.
    ///
    /// # Errors
    /// Returns an error if the file is too large for memory.
    pub fn as_bytes(&self) -> Result<Bytes> {
        self.slice_u64(0, self.len)
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
