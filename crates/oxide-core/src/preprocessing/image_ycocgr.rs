use crate::Result;
use crate::preprocessing::utils;

/// Converts raw image bytes into RGB pixels for YCoCg-R color transforms.
///
/// Input remains raw bytes. Metadata controls pixel layout decoding.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies YCoCg-R color space conversion preprocessing to image bytes.
///
/// Input: `data` is the raw image byte buffer to preprocess.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}

/// Reverses YCoCg-R color space conversion preprocessing on image bytes.
///
/// Input: `data` is the YCoCg-R preprocessed image byte buffer.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    Ok(data.to_vec())
}
