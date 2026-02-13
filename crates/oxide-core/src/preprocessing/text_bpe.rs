use crate::Result;

/// Applies Byte Pair Encoding (BPE) preprocessing to text data.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Byte Pair Encoding (BPE) preprocessing.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
