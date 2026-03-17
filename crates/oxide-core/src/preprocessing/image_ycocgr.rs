use crate::Result;
use crate::error::OxideError;
use crate::preprocessing::utils;

const TRANSFORM_MARKER: &[u8; 4] = b"YCGR";
const CHANNELS_PER_PIXEL: usize = 3;
const ENCODED_COMPONENT_BYTES: usize = std::mem::size_of::<i16>();
const ENCODED_PIXEL_BYTES: usize = CHANNELS_PER_PIXEL * ENCODED_COMPONENT_BYTES;

/// Converts raw image bytes into RGB pixels for YCoCg-R color transforms.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies a reversible YCoCg-R color transform.
///
/// Only packed RGB input is transformed. Other image layouts are returned
/// unchanged because reverse preprocessing cannot reconstruct dropped alpha
/// channels or original component ordering without additional metadata.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {
    let Some(meta) = metadata else {
        return Ok(data.to_vec());
    };
    if !supports_transform(data, meta) {
        return Ok(data.to_vec());
    }

    let pixels = bytes_to_data(data, meta)?;
    let mut output =
        Vec::with_capacity(TRANSFORM_MARKER.len() + pixels.len() * ENCODED_PIXEL_BYTES);
    output.extend_from_slice(TRANSFORM_MARKER);

    for pixel in pixels {
        let r = pixel[0] as i16;
        let g = pixel[1] as i16;
        let b = pixel[2] as i16;

        let co = r - b;
        let tmp = b + (co >> 1);
        let cg = g - tmp;
        let y = tmp + (cg >> 1);

        output.extend_from_slice(&y.to_le_bytes());
        output.extend_from_slice(&co.to_le_bytes());
        output.extend_from_slice(&cg.to_le_bytes());
    }
    Ok(output)
}

/// Reverses the YCoCg-R color transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let Some(payload) = data.strip_prefix(TRANSFORM_MARKER) else {
        return Ok(data.to_vec());
    };
    if payload.len() % ENCODED_PIXEL_BYTES != 0 {
        return Err(OxideError::InvalidFormat("invalid YCoCg-R payload length"));
    }
    let mut output = Vec::with_capacity((payload.len() / ENCODED_PIXEL_BYTES) * CHANNELS_PER_PIXEL);

    for chunk in payload.chunks_exact(ENCODED_PIXEL_BYTES) {
        let y = i16::from_le_bytes([chunk[0], chunk[1]]) as i32;
        let co = i16::from_le_bytes([chunk[2], chunk[3]]) as i32;
        let cg = i16::from_le_bytes([chunk[4], chunk[5]]) as i32;

        let t = y - (cg >> 1);
        let g = cg + t;
        let b = t - (co >> 1);
        let r = b + co;

        output.push(decode_channel(r)?);
        output.push(decode_channel(g)?);
        output.push(decode_channel(b)?);
    }

    Ok(output)
}

fn supports_transform(data: &[u8], metadata: &utils::ImageMetadata) -> bool {
    if !matches!(metadata.pixel_format, utils::ImagePixelFormat::Rgb8) {
        return false;
    }

    match (metadata.width, metadata.height) {
        (None, None) => data.len() % CHANNELS_PER_PIXEL == 0,
        (Some(width), None) => {
            let Some(row_bytes) = width.checked_mul(CHANNELS_PER_PIXEL) else {
                return false;
            };
            metadata.row_stride.unwrap_or(row_bytes) == row_bytes && data.len() % row_bytes == 0
        }
        (Some(width), Some(height)) => {
            let Some(row_bytes) = width.checked_mul(CHANNELS_PER_PIXEL) else {
                return false;
            };
            let Some(required_bytes) = row_bytes.checked_mul(height) else {
                return false;
            };

            metadata.row_stride.unwrap_or(row_bytes) == row_bytes && data.len() == required_bytes
        }
        _ => false,
    }
}

fn decode_channel(value: i32) -> Result<u8> {
    u8::try_from(value).map_err(|_| OxideError::InvalidFormat("invalid YCoCg-R channel value"))
}
