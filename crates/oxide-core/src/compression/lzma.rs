use std::io::Read;

use liblzma::{
    read::{XzDecoder, XzEncoder},
    stream::PRESET_EXTREME,
};

use super::scratch::LzmaScratch;
use crate::{OxideError, Result};

pub(crate) const LZMA_DEFAULT_LEVEL: i32 = 6;
const LZMA_MIN_LEVEL: i32 = 1;
const LZMA_MAX_LEVEL: i32 = 9;

#[inline]
fn resolve_level(level: Option<i32>, extreme: bool) -> Result<u32> {
    let level = level.unwrap_or(LZMA_DEFAULT_LEVEL);
    if !(LZMA_MIN_LEVEL..=LZMA_MAX_LEVEL).contains(&level) {
        return Err(OxideError::CompressionError(format!(
            "invalid lzma level {level}: expected {LZMA_MIN_LEVEL}..={LZMA_MAX_LEVEL}"
        )));
    }

    let level = if extreme {
        (level as u32) | PRESET_EXTREME
    } else {
        level as u32
    };
    Ok(level)
}

pub fn apply(data: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = LzmaScratch::default();
    apply_with_scratch(data, level, false, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    extreme: bool,
    scratch: &mut LzmaScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, level, extreme, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    level: Option<i32>,
    extreme: bool,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let mut encoder = XzEncoder::new(data, resolve_level(level, extreme)?);
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

#[cfg(test)]
mod tests {
    use super::{PRESET_EXTREME, resolve_level};

    #[test]
    fn plain_level_nine_does_not_enable_extreme() {
        assert_eq!(resolve_level(Some(9), false).expect("resolve level"), 9);
    }

    #[test]
    fn explicit_extreme_sets_extreme_bit() {
        assert_eq!(
            resolve_level(Some(9), true).expect("resolve level"),
            9 | PRESET_EXTREME
        );
    }
}
