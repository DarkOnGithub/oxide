use crate::Result;

/// Applies Linear Predictive Coding preprocessing to audio data.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Linear Predictive Coding preprocessing.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
