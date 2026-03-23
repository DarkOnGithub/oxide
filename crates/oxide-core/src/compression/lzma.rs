use liblzma::{decode_all, encode_all, stream::PRESET_EXTREME};

use crate::{CompressionPreset, OxideError, Result};

const LZMA_FAST_LEVEL: u32 = 1;
const LZMA_BALANCED_LEVEL: u32 = 6;
const LZMA_HIGH_LEVEL: u32 = 9 | PRESET_EXTREME;

#[inline]
fn level_for_preset(preset: CompressionPreset) -> u32 {
    match preset {
        CompressionPreset::Fast => LZMA_FAST_LEVEL,
        CompressionPreset::Default => LZMA_BALANCED_LEVEL,
        CompressionPreset::High => LZMA_HIGH_LEVEL,
    }
}

pub fn apply(data: &[u8], preset: CompressionPreset) -> Result<Vec<u8>> {
    encode_all(data, level_for_preset(preset))
        .map_err(|err| OxideError::CompressionError(format!("lzma encode failed: {err}")))
}

pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    decode_all(data)
        .map_err(|err| OxideError::DecompressionError(format!("lzma decode failed: {err}")))
}
