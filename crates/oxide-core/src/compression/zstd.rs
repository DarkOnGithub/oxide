use std::io::Cursor;

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
    zstd::bulk::compress(data, resolve_level(preset, zstd_level))
        .map_err(|err| OxideError::CompressionError(format!("zstd encode failed: {err}")))
}

pub fn reverse(data: &[u8], raw_len: Option<usize>) -> Result<Vec<u8>> {
    let result = match raw_len {
        Some(raw_len) => zstd::bulk::decompress(data, raw_len),
        None => zstd::stream::decode_all(Cursor::new(data)),
    };

    result.map_err(|err| OxideError::DecompressionError(format!("zstd decode failed: {err}")))
}

#[cfg(test)]
mod tests {
    use super::{ZSTD_BALANCED_LEVEL, ZSTD_ULTRA_LEVEL, level_for_preset, resolve_level};
    use crate::CompressionPreset;

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
}
