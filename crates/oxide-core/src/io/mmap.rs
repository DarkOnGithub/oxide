use std::fs::File;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};

use crate::types::Result;
use crate::OxideError;

#[derive(Debug)]
pub struct MmapInput {
    mmap: Option<Mmap>,
    path: PathBuf,
    len: u64,
}

impl MmapInput {
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len_u64(&self) -> u64 {
        self.len
    }

    pub fn len(&self) -> usize {
        self.len.min(usize::MAX as u64) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn slice(&self, start: usize, end: usize) -> Result<Bytes> {
        self.slice_u64(start as u64, end as u64)
    }

    pub fn slice_u64(&self, start: u64, end: u64) -> Result<Bytes> {
        let (start, end) = self.validate_range(start, end)?;

        match &self.mmap {
            Some(mmap) => {
                Ok(Bytes::copy_from_slice(&mmap[start..end]))
            }
            None => Ok(Bytes::new()),
        }
    }

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
