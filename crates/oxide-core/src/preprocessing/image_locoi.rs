use crate::preprocessing::utils;
use crate::{OxideError, Result};

const LOCO_MAGIC: &[u8; 4] = b"OLC1";
const HEADER_SIZE: usize = 4 + 4 + 1 + 3 + 4 + 4 + 4;
const MIN_INPUT_SIZE: usize = 64;

#[derive(Clone, Copy)]
struct ImageLayout {
    bpp: usize,
    row_bytes: usize,
    row_stride: usize,
    rows: usize,
}

/// Converts raw image bytes into grayscale-like pixel intensity samples.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<u8>> {
    utils::bytes_to_grayscale_pixels(data, metadata)
}

/// Applies a LOCO-I style median edge predictor filter.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {
    let Some(metadata) = metadata else {
        return Ok(data.to_vec());
    };

    let _ = bytes_to_data(data, metadata)?;
    if data.len() < MIN_INPUT_SIZE {
        return Ok(data.to_vec());
    }

    let Some(layout) = layout_from_metadata(data.len(), metadata)? else {
        return Ok(data.to_vec());
    };

    let original_len = u32::try_from(data.len())
        .map_err(|_| OxideError::InvalidFormat("image payload too large for LOCO-I header"))?;
    let row_bytes_u32 = u32::try_from(layout.row_bytes)
        .map_err(|_| OxideError::InvalidFormat("LOCO-I row bytes too large for header"))?;
    let row_stride_u32 = u32::try_from(layout.row_stride)
        .map_err(|_| OxideError::InvalidFormat("LOCO-I row stride too large for header"))?;
    let rows_u32 = u32::try_from(layout.rows)
        .map_err(|_| OxideError::InvalidFormat("LOCO-I row count too large for header"))?;

    let mut filtered = data.to_vec();
    for row in 0..layout.rows {
        let row_start = row * layout.row_stride;
        for col in 0..layout.row_bytes {
            let idx = row_start + col;
            let left = if col >= layout.bpp {
                data[idx - layout.bpp]
            } else {
                0
            };
            let up = if row > 0 {
                data[idx - layout.row_stride]
            } else {
                0
            };
            let up_left = if row > 0 && col >= layout.bpp {
                data[idx - layout.row_stride - layout.bpp]
            } else {
                0
            };

            let predictor = loco_predict(left, up, up_left);
            filtered[idx] = data[idx].wrapping_sub(predictor);
        }
    }

    let mut output = Vec::with_capacity(HEADER_SIZE + filtered.len());
    output.extend_from_slice(LOCO_MAGIC);
    output.extend_from_slice(&original_len.to_le_bytes());
    output.push(layout.bpp as u8);
    output.extend_from_slice(&[0, 0, 0]);
    output.extend_from_slice(&row_bytes_u32.to_le_bytes());
    output.extend_from_slice(&row_stride_u32.to_le_bytes());
    output.extend_from_slice(&rows_u32.to_le_bytes());
    output.extend_from_slice(&filtered);
    Ok(output)
}

/// Reverses the LOCO-I style predictor filter.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(LOCO_MAGIC) {
        return Ok(data.to_vec());
    }

    if data.len() < HEADER_SIZE {
        return Err(OxideError::InvalidFormat(
            "LOCO-I data too short for header",
        ));
    }

    let original_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let bpp = data[8] as usize;
    let row_bytes = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;
    let row_stride = u32::from_le_bytes([data[16], data[17], data[18], data[19]]) as usize;
    let rows = u32::from_le_bytes([data[20], data[21], data[22], data[23]]) as usize;

    if bpp == 0 {
        return Err(OxideError::InvalidFormat(
            "LOCO-I header has zero bytes-per-pixel",
        ));
    }
    if row_stride < row_bytes {
        return Err(OxideError::InvalidFormat(
            "LOCO-I row stride smaller than row bytes",
        ));
    }
    let required = rows
        .checked_mul(row_stride)
        .ok_or(OxideError::InvalidFormat("LOCO-I image size overflow"))?;

    let mut output = data[HEADER_SIZE..].to_vec();
    if output.len() != original_len {
        return Err(OxideError::InvalidFormat("LOCO-I payload size mismatch"));
    }
    if required > output.len() {
        return Err(OxideError::InvalidFormat(
            "LOCO-I payload shorter than declared layout",
        ));
    }

    for row in 0..rows {
        let row_start = row * row_stride;
        for col in 0..row_bytes {
            let idx = row_start + col;
            let left = if col >= bpp { output[idx - bpp] } else { 0 };
            let up = if row > 0 { output[idx - row_stride] } else { 0 };
            let up_left = if row > 0 && col >= bpp {
                output[idx - row_stride - bpp]
            } else {
                0
            };

            let predictor = loco_predict(left, up, up_left);
            output[idx] = output[idx].wrapping_add(predictor);
        }
    }

    Ok(output)
}

fn layout_from_metadata(
    data_len: usize,
    metadata: &utils::ImageMetadata,
) -> Result<Option<ImageLayout>> {
    let Some(width) = metadata.width else {
        return Ok(None);
    };

    let bpp = metadata.pixel_format.bytes_per_pixel();
    let row_bytes = width
        .checked_mul(bpp)
        .ok_or(OxideError::InvalidFormat("image row size overflow"))?;
    if row_bytes == 0 {
        return Ok(None);
    }

    let row_stride = metadata.row_stride.unwrap_or(row_bytes);
    if row_stride < row_bytes {
        return Err(OxideError::InvalidFormat(
            "image row stride smaller than packed row size",
        ));
    }

    let rows = match metadata.height {
        Some(height) => {
            let required = row_stride
                .checked_mul(height)
                .ok_or(OxideError::InvalidFormat("image size overflow"))?;
            if data_len < required {
                return Err(OxideError::InvalidFormat(
                    "image data shorter than metadata dimensions",
                ));
            }
            height
        }
        None => data_len / row_stride,
    };

    if rows == 0 {
        return Ok(None);
    }

    Ok(Some(ImageLayout {
        bpp,
        row_bytes,
        row_stride,
        rows,
    }))
}

#[inline]
fn loco_predict(left: u8, up: u8, up_left: u8) -> u8 {
    let left_i = left as i16;
    let up_i = up as i16;
    let up_left_i = up_left as i16;

    if up_left_i >= left_i.max(up_i) {
        left_i.min(up_i) as u8
    } else if up_left_i <= left_i.min(up_i) {
        left_i.max(up_i) as u8
    } else {
        (left_i + up_i - up_left_i) as u8
    }
}
