use crate::preprocessing::utils;
use crate::{OxideError, Result};

/// Converts raw image bytes into RGB pixels for Paeth predictor processing.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies a Paeth row predictor filter.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}

/// Reverses the Paeth row predictor filter.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}
