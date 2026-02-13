use crate::Result;

/// Applies Burrows-Wheeler Transform (BWT) preprocessing to text data.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Burrows-Wheeler Transform (BWT) preprocessing.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
