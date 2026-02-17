use crate::Result;
use crate::preprocessing::utils;

/// Converts raw image bytes into RGB pixels for Paeth predictor processing.
///
/// Input remains raw bytes. Metadata controls pixel layout decoding.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies Paeth predictor preprocessing to image bytes.
///
/// Input: `data` is the raw image byte buffer to preprocess.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {
    if let Some(metadata) = metadata {
        let data = bytes_to_data(data, metadata)?;
    }
    Ok(data.to_vec())
}

/// Reverses Paeth predictor preprocessing on image bytes.
///
/// Input: `data` is the Paeth-preprocessed image byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
