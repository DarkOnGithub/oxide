use std::fmt;

use super::lz4::Lz4Scratch;
use crate::{OxideError, Result};

pub(crate) struct ZstdScratch {
    compressor: Option<zstd::bulk::Compressor<'static>>,
    compressor_level: Option<i32>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

impl Default for ZstdScratch {
    fn default() -> Self {
        Self {
            compressor: None,
            compressor_level: None,
            decompressor: None,
        }
    }
}

impl fmt::Debug for ZstdScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdScratch")
            .field("compressor_initialized", &self.compressor.is_some())
            .field("compressor_level", &self.compressor_level)
            .field("decompressor_initialized", &self.decompressor.is_some())
            .finish()
    }
}

impl ZstdScratch {
    pub(crate) fn compressor(
        &mut self,
        level: i32,
    ) -> Result<&mut zstd::bulk::Compressor<'static>> {
        if self.compressor.is_none() {
            let compressor = zstd::bulk::Compressor::new(level).map_err(|err| {
                OxideError::CompressionError(format!("zstd compressor init failed: {err}"))
            })?;
            self.compressor = Some(compressor);
            self.compressor_level = Some(level);
        } else if self.compressor_level != Some(level) {
            self.compressor
                .as_mut()
                .expect("checked is_some above")
                .set_compression_level(level)
                .map_err(|err| {
                    OxideError::CompressionError(format!(
                        "zstd compressor reconfigure failed: {err}"
                    ))
                })?;
            self.compressor_level = Some(level);
        }

        Ok(self
            .compressor
            .as_mut()
            .expect("compressor initialized before return"))
    }

    pub(crate) fn decompressor(&mut self) -> Result<&mut zstd::bulk::Decompressor<'static>> {
        if self.decompressor.is_none() {
            let decompressor = zstd::bulk::Decompressor::new().map_err(|err| {
                OxideError::DecompressionError(format!("zstd decompressor init failed: {err}"))
            })?;
            self.decompressor = Some(decompressor);
        }

        Ok(self
            .decompressor
            .as_mut()
            .expect("decompressor initialized before return"))
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        0
    }
}

#[derive(Default)]
pub(crate) struct CompressionScratchArena {
    lz4: Lz4Scratch,
    zstd: ZstdScratch,
}

impl fmt::Debug for CompressionScratchArena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompressionScratchArena")
            .field("lz4", &self.lz4)
            .field("zstd", &self.zstd)
            .finish()
    }
}

impl CompressionScratchArena {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn lz4(&mut self) -> &mut Lz4Scratch {
        &mut self.lz4
    }

    pub(crate) fn zstd(&mut self) -> &mut ZstdScratch {
        &mut self.zstd
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.lz4.allocated_bytes() + self.zstd.allocated_bytes()
    }
}
