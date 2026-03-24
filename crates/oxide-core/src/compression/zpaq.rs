use std::io::Cursor;

use zpaq_rs::{compress_stream, decompress_stream};

use super::scratch::ZpaqScratch;
use crate::{OxideError, Result};

pub(crate) const ZPAQ_DEFAULT_LEVEL: i32 = 2;
const ZPAQ_MIN_LEVEL: i32 = 1;
const ZPAQ_MAX_LEVEL: i32 = 5;

#[inline]
fn resolve_method(level: Option<i32>) -> Result<String> {
    let level = level.unwrap_or(ZPAQ_DEFAULT_LEVEL);
    if !(ZPAQ_MIN_LEVEL..=ZPAQ_MAX_LEVEL).contains(&level) {
        return Err(OxideError::CompressionError(format!(
            "invalid zpaq level {level}: expected {ZPAQ_MIN_LEVEL}..={ZPAQ_MAX_LEVEL}"
        )));
    }

    Ok(level.to_string())
}

pub fn apply(data: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = ZpaqScratch::default();
    apply_with_scratch(data, level, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    scratch: &mut ZpaqScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, level, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(data: &[u8], level: Option<i32>, output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    let method = resolve_method(level)?;
    compress_stream(Cursor::new(data), output, &method, None, None)
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
