use std::io::Cursor;

use super::scratch::ZstdScratch;
use crate::{OxideError, Result};

pub(crate) const ZSTD_DEFAULT_LEVEL: i32 = 3;
const ZSTD_MIN_LEVEL: i32 = 1;
const ZSTD_MAX_LEVEL: i32 = 22;

#[inline]
fn resolve_level(level: Option<i32>) -> Result<i32> {
    let level = level.unwrap_or(ZSTD_DEFAULT_LEVEL);
    if !(ZSTD_MIN_LEVEL..=ZSTD_MAX_LEVEL).contains(&level) {
        return Err(OxideError::CompressionError(format!(
            "invalid zstd level {level}: expected {ZSTD_MIN_LEVEL}..={ZSTD_MAX_LEVEL}"
        )));
    }

    Ok(level)
}

pub fn apply(data: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = ZstdScratch::default();
    apply_with_scratch(data, level, 0, None, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, level, dictionary_id, dictionary, scratch, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    level: Option<i32>,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let required_capacity = zstd::zstd_safe::compress_bound(data.len());
    if output.capacity() < required_capacity {
        output.reserve(required_capacity - output.len());
    }

    scratch
        .compressor(resolve_level(level)?, dictionary_id, dictionary)?
        .compress_to_buffer(data, output)
        .map_err(|err| OxideError::CompressionError(format!("zstd encode failed: {err}")))?;

    Ok(())
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut ZstdScratch) {
    scratch.recycle_output(output);
}

pub fn reverse(data: &[u8], raw_len: Option<usize>) -> Result<Vec<u8>> {
    let mut scratch = ZstdScratch::default();
    reverse_with_scratch(data, raw_len, 0, None, &mut scratch)
}

pub(crate) fn reverse_with_scratch(
    data: &[u8],
    raw_len: Option<usize>,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    reverse_into_vec(
        data,
        raw_len,
        dictionary_id,
        dictionary,
        scratch,
        &mut output,
    )?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(
    data: &[u8],
    raw_len: Option<usize>,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();

    match raw_len {
        Some(raw_len) => {
            if output.capacity() < raw_len {
                output.reserve(raw_len - output.len());
            }

            scratch
                .decompressor(dictionary_id, dictionary)?
                .decompress_to_buffer(data, output)
                .map_err(|err| {
                    OxideError::DecompressionError(format!("zstd decode failed: {err}"))
                })?;
            Ok(())
        }
        None => {
            let decoded = zstd::stream::decode_all(Cursor::new(data)).map_err(|err| {
                OxideError::DecompressionError(format!("zstd decode failed: {err}"))
            })?;
            output.extend_from_slice(&decoded);
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "../../tests/compression/zstd.rs"]
mod tests;
