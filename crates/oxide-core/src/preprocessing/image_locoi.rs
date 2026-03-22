use crate::Result;
use crate::preprocessing::utils;



#[inline(always)]
fn loco_predict(w: u8, n: u8, nw: u8) -> u8 {
    let (lo, hi) = if w < n { (w, n) } else { (n, w) };

    if nw >= hi {
        lo
    } else if nw <= lo {
        hi
    } else {
        ((w as i16) + (n as i16) - (nw as i16)).clamp(0, 255) as u8
    }
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

    let w = meta.width as usize;
    let h = meta.height as usize;
    let total = w * h;

    if data.len() < total || total == 0 {
        return Ok(data.to_vec());
    }

    let mut out: Vec<u8> = Vec::with_capacity(total);
    unsafe { out.set_len(total) }

    out[0] = data[0];

    let first_row_in  = &data[1..w];
    let first_row_out = &mut out[1..w];
    let mut prev = data[0];
    for (&px, dst) in first_row_in.iter().zip(first_row_out.iter_mut()) {
        *dst = px.wrapping_sub(prev);
        prev = px;
    }

    for y in 1..h {
        let cur  = y * w;
        let prev_row = cur - w;

        out[cur] = data[cur].wrapping_sub(data[prev_row]);

        let cur_in   = &data[cur + 1  ..cur  + w];
        let w_slice  = &data[cur      ..cur  + w - 1];
        let n_slice  = &data[prev_row + 1..prev_row + w];
        let nw_slice = &data[prev_row    ..prev_row + w - 1];
        let cur_out  = &mut out[cur + 1..cur + w];

        for ((((&px, &w_val), &n_val), &nw_val), dst) in cur_in
            .iter()
            .zip(w_slice)
            .zip(n_slice)
            .zip(nw_slice)
            .zip(cur_out.iter_mut())
        {
            *dst = px.wrapping_sub(loco_predict(w_val, n_val, nw_val));
        }
    }

    Ok(out)
}

pub fn reverse(data: &[u8], metadata: Option<&utils::ImageMetadata>) -> Result<Vec<u8>> {
    let Some(meta) = metadata else {
        return Ok(data.to_vec());
    };

    let w = meta.width as usize;
    let h = meta.height as usize;
    let total = w * h;

    if data.len() < total || total == 0 {
        return Ok(data.to_vec());
    }

    let mut out = vec![0u8; total];

    out[0] = data[0];

    for x in 1..w {
        out[x] = data[x].wrapping_add(out[x - 1]);
    }

    for y in 1..h {
        let cur      = y * w;
        let prev_row = cur - w;

        out[cur] = data[cur].wrapping_add(out[prev_row]);

        for x in 1..w {
            let w_val  = out[cur      + x - 1];
            let n_val  = out[prev_row + x];
            let nw_val = out[prev_row + x - 1];
            out[cur + x] = data[cur + x].wrapping_add(loco_predict(w_val, n_val, nw_val));
        }
    }

    Ok(out)
}