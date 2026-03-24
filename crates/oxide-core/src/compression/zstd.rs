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
    scratch
        .compressor(resolve_level(level)?)?
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

#[cfg(test)]
mod tests {
    use super::{apply_with_scratch, resolve_level, reverse_with_scratch, ZSTD_DEFAULT_LEVEL};
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
}
