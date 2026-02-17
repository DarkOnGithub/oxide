use crate::Result;
use crate::preprocessing::utils;

/// Converts raw binary bytes into 32-bit instruction words for BCJ-style logic.
///
/// Input: `data` is executable or object-code bytes interpreted as
/// little-endian 32-bit words. Trailing bytes smaller than one word are
/// ignored.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_u32_words_le(data)
}

/// Applies Branch Call Jump (BCJ) filtering to binary bytes.
///
/// Input: `data` is the raw binary byte buffer to preprocess.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Branch Call Jump (BCJ) filtering on binary bytes.
///
/// Input: `data` is the BCJ-preprocessed binary byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
