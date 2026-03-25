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
    apply_with_scratch(data, level, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, level, scratch, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    level: Option<i32>,
    scratch: &mut ZstdScratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let required_capacity = zstd::zstd_safe::compress_bound(data.len());
    if output.capacity() < required_capacity {
        output.reserve(required_capacity - output.len());
    }

    scratch
        .compressor(resolve_level(level)?)?
        .compress_to_buffer(data, output)
        .map_err(|err| OxideError::CompressionError(format!("zstd encode failed: {err}")))?;

    Ok(())
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut ZstdScratch) {
    scratch.recycle_output(output);
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
    let mut output = scratch.take_output();
    reverse_into_vec(data, raw_len, scratch, &mut output)?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(
    data: &[u8],
    raw_len: Option<usize>,
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
                .decompressor()?
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
mod tests {
    use super::{
        apply_into_vec, apply_with_scratch, resolve_level, reverse_into_vec, reverse_with_scratch,
        ZSTD_DEFAULT_LEVEL,
    };
    use crate::compression::scratch::ZstdScratch;

    #[test]
    fn default_level_is_used_when_omitted() {
        assert_eq!(resolve_level(None).unwrap(), ZSTD_DEFAULT_LEVEL);
    }

    #[test]
    fn explicit_level_is_respected() {
        assert_eq!(resolve_level(Some(19)).unwrap(), 19);
    }

    #[test]
    fn reusable_scratch_supports_multiple_levels_and_round_trips() {
        let payload = b"banana bandana banana";
        let mut scratch = ZstdScratch::default();

        let balanced = apply_with_scratch(payload, Some(3), &mut scratch)
            .expect("zstd compression should succeed");
        let ultra = apply_with_scratch(payload, Some(19), &mut scratch)
            .expect("zstd compression should succeed");

        let balanced_decoded = reverse_with_scratch(&balanced, Some(payload.len()), &mut scratch)
            .expect("zstd decompression should succeed");
        let ultra_decoded = reverse_with_scratch(&ultra, Some(payload.len()), &mut scratch)
            .expect("zstd decompression should succeed");

        assert_eq!(balanced_decoded, payload);
        assert_eq!(ultra_decoded, payload);
    }

    #[test]
    fn direct_buffer_round_trip_reuses_output_vec() {
        let payload = b"banana bandana banana";
        let mut scratch = ZstdScratch::default();

        let mut compressed = Vec::with_capacity(64);
        let compressed_ptr = compressed.as_ptr();
        apply_into_vec(payload, Some(3), &mut scratch, &mut compressed)
            .expect("zstd compression should succeed");
        assert_eq!(compressed.as_ptr(), compressed_ptr);

        let mut decoded = Vec::with_capacity(payload.len());
        let decoded_ptr = decoded.as_ptr();
        reverse_into_vec(&compressed, Some(payload.len()), &mut scratch, &mut decoded)
            .expect("zstd decompression should succeed");

        assert_eq!(decoded.as_ptr(), decoded_ptr);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn direct_buffer_round_trip_grows_undersized_vecs() {
        let payload: Vec<u8> = (0..16_384).map(|idx| ((idx * 73) % 251) as u8).collect();
        let mut scratch = ZstdScratch::default();

        let mut compressed = Vec::with_capacity(512);
        apply_into_vec(&payload, Some(3), &mut scratch, &mut compressed)
            .expect("zstd compression should grow undersized output");

        let mut decoded = Vec::with_capacity(1024);
        reverse_into_vec(&compressed, Some(payload.len()), &mut scratch, &mut decoded)
            .expect("zstd decompression should grow undersized output");

        assert_eq!(decoded, payload);
    }
}
