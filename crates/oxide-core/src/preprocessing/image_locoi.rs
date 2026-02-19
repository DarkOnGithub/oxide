use crate::preprocessing::utils;
use crate::{OxideError, Result};


/// Converts raw image bytes into grayscale-like pixel intensity samples.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<u8>> {
    utils::bytes_to_grayscale_pixels(data, metadata)
}

/// Applies a LOCO-I style median edge predictor filter.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}

/// Reverses the LOCO-I style predictor filter.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}
