use std::io::Read;

use liblzma::{
    read::{XzDecoder, XzEncoder},
    stream::PRESET_EXTREME,
};

use super::scratch::LzmaScratch;
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
    let mut scratch = LzmaScratch::default();
    apply_with_scratch(data, preset, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    preset: CompressionPreset,
    scratch: &mut LzmaScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, preset, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    preset: CompressionPreset,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let mut encoder = XzEncoder::new(data, level_for_preset(preset));
    encoder
        .read_to_end(output)
        .map_err(|err| OxideError::CompressionError(format!("lzma encode failed: {err}")))?;
    Ok(())
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut LzmaScratch) {
    scratch.recycle_output(output);
}

pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let mut scratch = LzmaScratch::default();
    reverse_with_scratch(data, &mut scratch)
}

pub(crate) fn reverse_with_scratch(data: &[u8], scratch: &mut LzmaScratch) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    reverse_into_vec(data, &mut output)?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(data: &[u8], output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    let mut decoder = XzDecoder::new(data);
    decoder
        .read_to_end(output)
        .map_err(|err| OxideError::DecompressionError(format!("lzma decode failed: {err}")))?;
    Ok(())
}
