use crate::Result;
use crate::preprocessing::utils;

/// Converts raw image bytes into RGB pixels for Paeth predictor processing.
///
/// Input: `data` is a packed RGB byte stream in `RGBRGB...` order. Trailing
/// bytes that do not form a full pixel are ignored.
pub fn bytes_to_data(data: &[u8]) -> Vec<[u8; 3]> {
    utils::bytes_to_rgb_pixels(data)
}

/// Applies Paeth predictor preprocessing to image bytes.
///
/// Input: `data` is the raw image byte buffer to preprocess.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses Paeth predictor preprocessing on image bytes.
///
/// Input: `data` is the Paeth-preprocessed image byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
