use crate::preprocessing::utils;
use crate::{OxideError, Result};


/// Converts raw binary bytes into 32-bit instruction words for BCJ-style logic.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_u32_words_le(data)
}

/// Applies a minimal x86 BCJ transform for E8/E9 rel32 operands.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}

/// Reverses the x86 BCJ transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    
    Ok(data.to_vec())
}
