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

#[cfg(test)]
mod tests {
    use super::{
        ZSTD_BALANCED_LEVEL, ZSTD_ULTRA_LEVEL, apply_with_scratch, level_for_preset, resolve_level,
        reverse_with_scratch,
    };
    use crate::CompressionPreset;
    use crate::compression::scratch::ZstdScratch;

    #[test]
    fn balanced_and_ultra_presets_use_requested_levels() {
        assert_eq!(
            level_for_preset(CompressionPreset::Default),
            ZSTD_BALANCED_LEVEL
        );
        assert_eq!(level_for_preset(CompressionPreset::High), ZSTD_ULTRA_LEVEL);
    }

    #[test]
    fn explicit_level_override_wins_over_preset_mapping() {
        assert_eq!(resolve_level(CompressionPreset::Fast, Some(19)), 19);
    }

    #[test]
    fn reusable_scratch_supports_multiple_levels_and_round_trips() {
        let payload = b"banana bandana banana";
        let mut scratch = ZstdScratch::default();

        let balanced = apply_with_scratch(payload, CompressionPreset::Default, None, &mut scratch)
            .expect("balanced zstd compression should succeed");
        let ultra = apply_with_scratch(payload, CompressionPreset::High, None, &mut scratch)
            .expect("ultra zstd compression should succeed");

        let balanced_decoded = reverse_with_scratch(&balanced, Some(payload.len()), &mut scratch)
            .expect("balanced zstd decompression should succeed");
        let ultra_decoded = reverse_with_scratch(&ultra, Some(payload.len()), &mut scratch)
            .expect("ultra zstd decompression should succeed");

        assert_eq!(balanced_decoded, payload);
        assert_eq!(ultra_decoded, payload);
    }
}
