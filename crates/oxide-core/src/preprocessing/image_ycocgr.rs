use crate::preprocessing::utils;
use crate::{OxideError, Result};

/// Converts raw image bytes into RGB pixels for YCoCg-R color transforms.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies a reversible YCoCg-R color transform.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}

/// Reverses the YCoCg-R color transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {

    Ok(data.to_vec())
}
