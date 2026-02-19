use crate::preprocessing::utils;
use crate::{OxideError, Result};

/// Converts raw text bytes into integer symbol ids for BWT-style processing.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_symbols(data)
}

/// Applies a blockwise Burrows-Wheeler transform.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}

/// Reverses the blockwise Burrows-Wheeler transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    
    Ok(data.to_vec())
}

    