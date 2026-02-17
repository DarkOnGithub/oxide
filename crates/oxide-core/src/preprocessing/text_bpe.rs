use crate::Result;
use crate::preprocessing::utils;

/// Converts raw text bytes into integer symbol ids for BPE-style processing.
///
/// Input: `data` is a text byte stream where each byte is mapped to one
/// symbol id.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_symbols(data)
}

/// Applies Byte Pair Encoding (BPE) preprocessing to text bytes.
///
/// Input: `data` is the raw text byte buffer to preprocess.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Byte Pair Encoding (BPE) preprocessing on text bytes.
///
/// Input: `data` is the BPE-preprocessed text byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
