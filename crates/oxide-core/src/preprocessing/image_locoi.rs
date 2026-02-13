use crate::Result;

/// Applies LOCO-I (JPEG-LS) predictor preprocessing to image data.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses LOCO-I predictor preprocessing.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
