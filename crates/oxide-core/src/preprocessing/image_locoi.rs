use crate::Result;
use crate::preprocessing::utils;


const MARKER: u8 = 0xAB;
const HEADER_LEN: usize = 9;

#[inline(always)]
fn med(a: u8, b: u8, c: u8) -> u8 {
    let (a, b, c) = (a as i16, b as i16, c as i16);
    let min_ab = a.min(b);
    let max_ab = a.max(b);
    if c >= max_ab {
        min_ab as u8 
    } else if c <= min_ab {
        max_ab as u8
    } else {
        (a + b - c) as u8 
    }
}

#[inline(always)]
fn write_header(out: &mut Vec<u8>, width: usize, height: usize) {
    out.push(MARKER);
    out.extend_from_slice(&(width as u32).to_le_bytes());
    out.extend_from_slice(&(height as u32).to_le_bytes());
}

#[inline(always)]
fn read_header(data: &[u8]) -> Option<(usize, usize)> {
    if data.len() < HEADER_LEN || data[0] != MARKER {
        return None;
    }
    let w = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
    let h = u32::from_le_bytes(data[5..9].try_into().ok()?) as usize;
    Some((w, h))
}

/// Converts raw image bytes into grayscale-like pixel intensity samples.
pub fn bytes_to_data(data: &[u8], metadata: &utils::ImageMetadata) -> Result<Vec<u8>> {
    utils::bytes_to_grayscale_pixels(data, metadata)
}

/// Applies a LOCO-I style median edge predictor filter.
pub fn apply(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {

    let Some(meta) = metadata else {
        return Ok(data.to_vec());
    };

    let (w, h) = match (meta.width, meta.height) {
    (Some(w), Some(h)) if w > 0 && h > 0 => (w, h),
    _ => return Ok(data.to_vec()),
    };
    
    let n = w * h;

    if data.len() < n {
        return Ok(data.to_vec());
    }

    let mut out = Vec::with_capacity(HEADER_LEN + n);
    write_header(&mut out, w, h);
    out.resize(HEADER_LEN + n, 0u8);

    let (_, res) = out.split_at_mut(HEADER_LEN);

    res[0] = data[0];
    for x in 1..w {
        res[x] = data[x].wrapping_sub(data[x - 1]);
    }

    for y in 1..h {
        let row = y * w;

        res[row] = data[row].wrapping_sub(data[row - w]);

        unsafe {
            for x in 1..w {
                let idx = row + x;
                let a = *data.get_unchecked(idx - 1);
                let b = *data.get_unchecked(idx - w);
                let c = *data.get_unchecked(idx - w - 1);
                *res.get_unchecked_mut(idx) =
                    data.get_unchecked(idx).wrapping_sub(med(a, b, c));
            }
        }
    }

    Ok(out)
}

/// Reverses the LOCO-I style predictor filter.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let Some((w, h)) = read_header(data) else {
        return Ok(data.to_vec());
    };

    let n = w * h;
    if data.len() < HEADER_LEN + n {
        return Ok(data.to_vec());
    }

    let res = &data[HEADER_LEN..HEADER_LEN + n];
    let mut out = vec![0u8; n];

    out[0] = res[0];
    for x in 1..w {
        out[x] = res[x].wrapping_add(out[x - 1]);
    }

    for y in 1..h {
        let row = y * w;

        out[row] = res[row].wrapping_add(out[row - w]);

        unsafe {
            for x in 1..w {
                let idx = row + x;
                let a = *out.get_unchecked(idx - 1);
                let b = *out.get_unchecked(idx - w);
                let c = *out.get_unchecked(idx - w - 1);
                *out.get_unchecked_mut(idx) =
                    res.get_unchecked(idx).wrapping_add(med(a, b, c));
            }
        }
    }

    Ok(out)
}