use std::io::Cursor;

use zpaq_rs::{compress_stream, decompress_stream};

use super::scratch::ZpaqScratch;
use crate::{CompressionPreset, OxideError, Result};

const ZPAQ_FAST_METHOD: &str = "1";
const ZPAQ_BALANCED_METHOD: &str = "2";
const ZPAQ_HIGH_METHOD: &str = "5";

#[inline]
fn method_for_preset(preset: CompressionPreset) -> &'static str {
    match preset {
        CompressionPreset::Fast => ZPAQ_FAST_METHOD,
        CompressionPreset::Default => ZPAQ_BALANCED_METHOD,
        CompressionPreset::High => ZPAQ_HIGH_METHOD,
    }
}

pub fn apply(data: &[u8], preset: CompressionPreset) -> Result<Vec<u8>> {
    let mut scratch = ZpaqScratch::default();
    apply_with_scratch(data, preset, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    preset: CompressionPreset,
    scratch: &mut ZpaqScratch,
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
    compress_stream(
        Cursor::new(data),
        output,
        method_for_preset(preset),
        None,
        None,
    )
    .map_err(|err| OxideError::CompressionError(format!("zpaq encode failed: {err}")))?;
    Ok(())
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut ZpaqScratch) {
    scratch.recycle_output(output);
}

pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let mut scratch = ZpaqScratch::default();
    reverse_with_scratch(data, &mut scratch)
}

pub(crate) fn reverse_with_scratch(data: &[u8], scratch: &mut ZpaqScratch) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    reverse_into_vec(data, &mut output)?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(data: &[u8], output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    decompress_stream(Cursor::new(data), output)
        .map_err(|err| OxideError::DecompressionError(format!("zpaq decode failed: {err}")))?;
    Ok(())
}
