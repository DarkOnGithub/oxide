use crate::Result;
use crate::preprocessing::utils;
use crate::error::OxideError;

/// Converts raw image bytes into RGB pixels for YCoCg-R color transforms.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<[u8; 3]>> {
    utils::bytes_to_rgb_pixels(data, metadata)
}

/// Applies a reversible YCoCg-R color transform.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {
    let meta = match metadata {
        Some(m) => m,
        None => return Ok(data.to_vec()),
    };
    let pixels = bytes_to_data(data, meta)?;
    let mut output = Vec::with_capacity(pixels.len() * 6);
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
    if data.len() % 6 != 0 {
        return Ok(data.to_vec());
    }
    let mut output = Vec::with_capacity(data.len() / 2);

    for chunk in data.chunks_exact(6) {
        
        let y = i16::from_le_bytes([chunk[0], chunk[1]]) as i32;
        let co = i16::from_le_bytes([chunk[2], chunk[3]]) as i32;
        let cg = i16::from_le_bytes([chunk[4], chunk[5]]) as i32;
        
        let t = y - (cg >> 1);
        let g = cg + t;
        let b = t - (co >> 1);
        let r = b + co;

        output.push(r.clamp(0, 255) as u8);
        output.push(g.clamp(0, 255) as u8);
        output.push(b.clamp(0, 255) as u8);
    }
    Ok(output)
}
