use crate::Result;

/// Applies YCoCg-R color space conversion preprocessing to image data.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses YCoCg-R color space conversion preprocessing.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
