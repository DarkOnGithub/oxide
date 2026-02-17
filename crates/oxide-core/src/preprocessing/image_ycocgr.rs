use crate::preprocessing::utils;
use crate::{OxideError, Result};

const YCOCGR_MAGIC: &[u8; 4] = b"OYC1";
const HEADER_SIZE: usize = 4 + 4 + 1 + 1;
const MIN_INPUT_SIZE: usize = 64;

/// Converts raw image bytes into RGB pixels for YCoCg-R color transforms.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies a reversible YCoCg-R color transform.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {
    let Some(metadata) = metadata else {
        return Ok(data.to_vec());
    };

    let _ = bytes_to_data(data, metadata)?;
    if data.len() < MIN_INPUT_SIZE {
        return Ok(data.to_vec());
    }

    let (format_code, bpp, r_idx, g_idx, b_idx) = match metadata.pixel_format {
        utils::ImagePixelFormat::Rgb8 => (1u8, 3usize, 0usize, 1usize, 2usize),
        utils::ImagePixelFormat::Bgr8 => (2u8, 3usize, 2usize, 1usize, 0usize),
        utils::ImagePixelFormat::Rgba8 => (3u8, 4usize, 0usize, 1usize, 2usize),
        utils::ImagePixelFormat::Bgra8 => (4u8, 4usize, 2usize, 1usize, 0usize),
        utils::ImagePixelFormat::Gray8 | utils::ImagePixelFormat::GrayAlpha8 => {
            return Ok(data.to_vec());
        }
    };

    let original_len = u32::try_from(data.len())
        .map_err(|_| OxideError::InvalidFormat("image payload too large for YCoCg-R header"))?;

    let mut transformed = data.to_vec();
    for pixel in transformed.chunks_exact_mut(bpp) {
        let r = pixel[r_idx];
        let g = pixel[g_idx];
        let b = pixel[b_idx];

        let co = (r as i16 - b as i16) as i8;
        let t = (b as i16 + ((co as i16) >> 1)) as u8;
        let cg = (g as i16 - t as i16) as i8;
        let y = (t as i16 + ((cg as i16) >> 1)) as u8;

        pixel[r_idx] = y;
        pixel[g_idx] = co as u8;
        pixel[b_idx] = cg as u8;
    }

    let mut output = Vec::with_capacity(HEADER_SIZE + transformed.len());
    output.extend_from_slice(YCOCGR_MAGIC);
    output.extend_from_slice(&original_len.to_le_bytes());
    output.push(format_code);
    output.push(bpp as u8);
    output.extend_from_slice(&transformed);
    Ok(output)
}

/// Reverses the YCoCg-R color transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(YCOCGR_MAGIC) {
        return Ok(data.to_vec());
    }

    if data.len() < HEADER_SIZE {
        return Err(OxideError::InvalidFormat(
            "YCoCg-R data too short for header",
        ));
    }

    let original_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let format_code = data[8];
    let bpp = data[9] as usize;

    let (expected_bpp, r_idx, g_idx, b_idx) = match format_code {
        1 => (3usize, 0usize, 1usize, 2usize),
        2 => (3usize, 2usize, 1usize, 0usize),
        3 => (4usize, 0usize, 1usize, 2usize),
        4 => (4usize, 2usize, 1usize, 0usize),
        _ => {
            return Err(OxideError::InvalidFormat(
                "YCoCg-R header contains unknown pixel format",
            ));
        }
    };

    if bpp != expected_bpp {
        return Err(OxideError::InvalidFormat("YCoCg-R header bpp mismatch"));
    }

    let mut output = data[HEADER_SIZE..].to_vec();
    if output.len() != original_len {
        return Err(OxideError::InvalidFormat("YCoCg-R payload size mismatch"));
    }

    for pixel in output.chunks_exact_mut(bpp) {
        let y = pixel[r_idx];
        let co = pixel[g_idx] as i8;
        let cg = pixel[b_idx] as i8;

        let t = (y as i16 - ((cg as i16) >> 1)) as u8;
        let g = (cg as i16 + t as i16) as u8;
        let b = (t as i16 - ((co as i16) >> 1)) as u8;
        let r = (b as i16 + co as i16) as u8;

        pixel[r_idx] = r;
        pixel[g_idx] = g;
        pixel[b_idx] = b;
    }

    Ok(output)
}
