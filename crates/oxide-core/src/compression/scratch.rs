use std::fmt;

use super::lz4::Lz4Scratch;
use crate::{OxideError, Result};

const MIN_RETAINED_OUTPUT_CAPACITY: usize = 256 * 1024;
const LZMA_MAX_RETAINED_OUTPUT_CAPACITY: usize = 8 * 1024 * 1024;
const ZSTD_MAX_RETAINED_OUTPUT_CAPACITY: usize = 8 * 1024 * 1024;

#[inline]
fn capped_retained_capacity(capacity: usize, max_retained: usize) -> usize {
    capacity
        .min(max_retained)
        .max(MIN_RETAINED_OUTPUT_CAPACITY.min(max_retained))
}

#[inline]
fn recycle_output_with_cap(mut output: Vec<u8>, max_retained: usize) -> Vec<u8> {
    output.clear();
    let retained_capacity = capped_retained_capacity(output.capacity(), max_retained);
    if output.capacity() > retained_capacity {
        output.shrink_to(retained_capacity);
    }
    output
}

#[derive(Default)]
pub(crate) struct LzmaScratch {
    output: Vec<u8>,
}

impl fmt::Debug for LzmaScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LzmaScratch")
            .field("output_capacity", &self.output.capacity())
            .finish()
    }
}

impl LzmaScratch {
    pub(crate) fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    pub(crate) fn recycle_output(&mut self, output: Vec<u8>) {
        self.output = recycle_output_with_cap(output, LZMA_MAX_RETAINED_OUTPUT_CAPACITY);
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.output.capacity()
    }
}

struct CachedZstdCompressor {
    level: i32,
    dictionary_id: u8,
    compressor: zstd::bulk::Compressor<'static>,
}

struct CachedZstdDecompressor {
    dictionary_id: u8,
    decompressor: zstd::bulk::Decompressor<'static>,
}

#[derive(Default)]
pub(crate) struct ZstdScratch {
    compressors: Vec<CachedZstdCompressor>,
    decompressors: Vec<CachedZstdDecompressor>,
    output: Vec<u8>,
}


impl fmt::Debug for ZstdScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdScratch")
            .field("compressors_cached", &self.compressors.len())
            .field("decompressors_cached", &self.decompressors.len())
            .field("output_capacity", &self.output.capacity())
            .finish()
    }
}

impl ZstdScratch {
    pub(crate) fn compressor(
        &mut self,
        level: i32,
        dictionary_id: u8,
        dictionary: Option<&[u8]>,
    ) -> Result<&mut zstd::bulk::Compressor<'static>> {
        if let Some(index) = self
            .compressors
            .iter()
            .position(|cached| cached.level == level && cached.dictionary_id == dictionary_id)
        {
            return Ok(&mut self.compressors[index].compressor);
        }

        {
            let compressor =
                zstd::bulk::Compressor::with_dictionary(level, dictionary.unwrap_or(&[])).map_err(
                    |err| {
                        OxideError::CompressionError(format!("zstd compressor init failed: {err}"))
                    },
                )?;
            self.compressors.push(CachedZstdCompressor {
                level,
                dictionary_id,
                compressor,
            });
        }

        Ok(&mut self
            .compressors
            .last_mut()
            .expect("compressor initialized before return")
            .compressor)
    }

    pub(crate) fn decompressor(
        &mut self,
        dictionary_id: u8,
        dictionary: Option<&[u8]>,
    ) -> Result<&mut zstd::bulk::Decompressor<'static>> {
        if let Some(index) = self
            .decompressors
            .iter()
            .position(|cached| cached.dictionary_id == dictionary_id)
        {
            return Ok(&mut self.decompressors[index].decompressor);
        }

        {
            let decompressor = zstd::bulk::Decompressor::with_dictionary(dictionary.unwrap_or(&[]))
                .map_err(|err| {
                    OxideError::DecompressionError(format!("zstd decompressor init failed: {err}"))
                })?;
            self.decompressors.push(CachedZstdDecompressor {
                dictionary_id,
                decompressor,
            });
        }

        Ok(&mut self
            .decompressors
            .last_mut()
            .expect("decompressor initialized before return")
            .decompressor)
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.output.capacity()
    }

    pub(crate) fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    pub(crate) fn recycle_output(&mut self, output: Vec<u8>) {
        self.output = recycle_output_with_cap(output, ZSTD_MAX_RETAINED_OUTPUT_CAPACITY);
    }
}

#[derive(Default)]
pub(crate) struct CompressionScratchArena {
    lz4: Lz4Scratch,
    lzma: LzmaScratch,
    zstd: ZstdScratch,
}

impl fmt::Debug for CompressionScratchArena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompressionScratchArena")
            .field("lz4", &self.lz4)
            .field("lzma", &self.lzma)
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

    pub(crate) fn lzma(&mut self) -> &mut LzmaScratch {
        &mut self.lzma
    }

    pub(crate) fn zstd(&mut self) -> &mut ZstdScratch {
        &mut self.zstd
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.lz4.allocated_bytes() + self.lzma.allocated_bytes() + self.zstd.allocated_bytes()
    }
}
