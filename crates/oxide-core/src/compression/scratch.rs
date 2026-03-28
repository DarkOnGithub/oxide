use std::fmt;

use super::lz4::Lz4Scratch;
use crate::{OxideError, Result, ZstdCompressionParameters};

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

pub(crate) struct ZstdScratch {
    compressor: Option<zstd::bulk::Compressor<'static>>,
    compressor_level: Option<i32>,
    compressor_dictionary_id: u8,
    compressor_parameters: ZstdCompressionParameters,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    decompressor_dictionary_id: u8,
    output: Vec<u8>,
}

impl Default for ZstdScratch {
    fn default() -> Self {
        Self {
            compressor: None,
            compressor_level: None,
            compressor_dictionary_id: 0,
            compressor_parameters: ZstdCompressionParameters::default(),
            decompressor: None,
            decompressor_dictionary_id: 0,
            output: Vec::new(),
        }
    }
}

impl fmt::Debug for ZstdScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdScratch")
            .field("compressor_initialized", &self.compressor.is_some())
            .field("compressor_level", &self.compressor_level)
            .field("compressor_dictionary_id", &self.compressor_dictionary_id)
            .field("compressor_parameters", &self.compressor_parameters)
            .field("decompressor_initialized", &self.decompressor.is_some())
            .field(
                "decompressor_dictionary_id",
                &self.decompressor_dictionary_id,
            )
            .field("output_capacity", &self.output.capacity())
            .finish()
    }
}

impl ZstdScratch {
    pub(crate) fn compressor(
        &mut self,
        level: i32,
        parameters: ZstdCompressionParameters,
        dictionary_id: u8,
        dictionary: Option<&[u8]>,
    ) -> Result<&mut zstd::bulk::Compressor<'static>> {
        if self.compressor.is_none() {
            let compressor =
                super::zstd::build_compressor(level, parameters, dictionary.unwrap_or(&[]))?;
            self.compressor = Some(compressor);
            self.compressor_level = Some(level);
            self.compressor_dictionary_id = dictionary_id;
            self.compressor_parameters = parameters;
        } else if self.compressor_level != Some(level)
            || self.compressor_dictionary_id != dictionary_id
            || self.compressor_parameters != parameters
        {
            self.compressor = Some(super::zstd::build_compressor(
                level,
                parameters,
                dictionary.unwrap_or(&[]),
            )?);
            self.compressor_level = Some(level);
            self.compressor_dictionary_id = dictionary_id;
            self.compressor_parameters = parameters;
        }

        Ok(self
            .compressor
            .as_mut()
            .expect("compressor initialized before return"))
    }

    pub(crate) fn decompressor(
        &mut self,
        dictionary_id: u8,
        dictionary: Option<&[u8]>,
    ) -> Result<&mut zstd::bulk::Decompressor<'static>> {
        if self.decompressor.is_none() {
            let decompressor = zstd::bulk::Decompressor::with_dictionary(dictionary.unwrap_or(&[]))
                .map_err(|err| {
                    OxideError::DecompressionError(format!("zstd decompressor init failed: {err}"))
                })?;
            self.decompressor = Some(decompressor);
            self.decompressor_dictionary_id = dictionary_id;
        } else if self.decompressor_dictionary_id != dictionary_id {
            self.decompressor
                .as_mut()
                .expect("checked is_some above")
                .set_dictionary(dictionary.unwrap_or(&[]))
                .map_err(|err| {
                    OxideError::DecompressionError(format!(
                        "zstd decompressor reconfigure failed: {err}"
                    ))
                })?;
            self.decompressor_dictionary_id = dictionary_id;
        }

        Ok(self
            .decompressor
            .as_mut()
            .expect("decompressor initialized before return"))
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

#[cfg(test)]
#[path = "../../tests/compression/scratch.rs"]
mod tests;
