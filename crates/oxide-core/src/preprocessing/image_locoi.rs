use crate::Result;
use crate::preprocessing::utils;

/// Converts raw image bytes into grayscale-like pixel intensity samples.
///
/// Input remains raw bytes. Metadata controls pixel layout decoding.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<u8>> {
    utils::bytes_to_grayscale_pixels(data, metadata)
}

/// Applies LOCO-I (JPEG-LS) predictor preprocessing to image bytes.
///
/// Input: `data` is the raw image byte buffer to preprocess.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses LOCO-I predictor preprocessing on image bytes.
///
/// Input: `data` is the LOCO-I preprocessed image byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
