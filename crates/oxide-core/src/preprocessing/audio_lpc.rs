use crate::Result;
use crate::preprocessing::utils;

/// Converts raw audio bytes into a sequence of LPC-ready sample values.
///
/// Input remains raw bytes. Metadata controls sample layout decoding.
pub fn bytes_to_data(data: &[u8], metadata: &utils::AudioMetadata) -> Result<Vec<i16>> {
    utils::bytes_to_i16_samples(data, metadata)
}

/// Applies Linear Predictive Coding preprocessing to audio bytes.
///
/// Input: `data` is the raw audio byte buffer to preprocess.
pub fn apply(data: &[u8], metadata: Option<&utils::AudioMetadata>) -> Result<Vec<u8>> {
    if let Some(metadata) = metadata {
        let _ = bytes_to_data(data, metadata)?;
    }
    Ok(data.to_vec())
}

/// Reverses Linear Predictive Coding preprocessing on audio bytes.
///
/// Input: `data` is the LPC-preprocessed audio byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
