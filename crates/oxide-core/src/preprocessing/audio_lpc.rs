use crate::Result;
use crate::preprocessing::utils;

/// Converts raw audio bytes into a sequence of LPC-ready sample values.
///
/// Input: `data` is expected to contain little-endian 16-bit PCM-style audio
/// samples. A trailing odd byte is ignored.
pub fn bytes_to_data(data: &[u8]) -> Vec<i16> {
    utils::bytes_to_i16_samples_le(data)
}

/// Applies Linear Predictive Coding preprocessing to audio bytes.
///
/// Input: `data` is the raw audio byte buffer to preprocess.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Linear Predictive Coding preprocessing on audio bytes.
///
/// Input: `data` is the LPC-preprocessed audio byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
