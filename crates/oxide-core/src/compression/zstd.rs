use std::io::Cursor;

use super::scratch::ZstdScratch;
use crate::{CompressionPreset, OxideError, Result};

const ZSTD_FAST_LEVEL: i32 = 1;
const ZSTD_BALANCED_LEVEL: i32 = 3;
const ZSTD_ULTRA_LEVEL: i32 = 19;

#[inline]
fn level_for_preset(preset: CompressionPreset) -> i32 {
    match preset {
        CompressionPreset::Fast => ZSTD_FAST_LEVEL,
        CompressionPreset::Default => ZSTD_BALANCED_LEVEL,
        CompressionPreset::High => ZSTD_ULTRA_LEVEL,
    }
}

#[inline]
fn resolve_level(preset: CompressionPreset, zstd_level: Option<i32>) -> i32 {
    zstd_level.unwrap_or_else(|| level_for_preset(preset))
}

pub fn apply(data: &[u8], preset: CompressionPreset, zstd_level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = ZstdScratch::default();
    apply_with_scratch(data, preset, zstd_level, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    preset: CompressionPreset,
    zstd_level: Option<i32>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    scratch
        .compressor(resolve_level(preset, zstd_level))?
        .compress(data)
        .map_err(|err| OxideError::CompressionError(format!("zstd encode failed: {err}")))
}

pub fn reverse(data: &[u8], raw_len: Option<usize>) -> Result<Vec<u8>> {
    let mut scratch = ZstdScratch::default();
    reverse_with_scratch(data, raw_len, &mut scratch)
}

pub(crate) fn reverse_with_scratch(
    data: &[u8],
    raw_len: Option<usize>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    let result = match raw_len {
        Some(raw_len) => scratch.decompressor()?.decompress(data, raw_len),
        None => zstd::stream::decode_all(Cursor::new(data)),
    };

    result.map_err(|err| OxideError::DecompressionError(format!("zstd decode failed: {err}")))
}
