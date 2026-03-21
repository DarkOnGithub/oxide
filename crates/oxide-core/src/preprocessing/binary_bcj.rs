use crate::Result;
use crate::preprocessing::utils;

/// Converts raw binary bytes into 32-bit instruction words for BCJ-style logic.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    let mut result = Vec::new();

    let mut i = 0;
    while i + 4 <= data.len() {
        let val = (data[i] as u32)
            | ((data[i + 1] as u32) << 8)
            | ((data[i + 2] as u32) << 16)
            | ((data[i + 3] as u32) << 24);

        result.push(val);
        i += 4;
    }

    result
}

/// Applies a minimal x86 BCJ transform for E8/E9 rel32 operands.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = data.to_vec();
    let len = out.len();

    let mut i = 0;
    while i + 4 < len {
        let opcode = out[i];

        if opcode == 0xE8 || opcode == 0xE9 {
            // Lire rel32 (little endian)
            let rel = (out[i + 1] as u32)
                | ((out[i + 2] as u32) << 8)
                | ((out[i + 3] as u32) << 16)
                | ((out[i + 4] as u32) << 24);

            let pos = i as u32;

            // Transform: rel → absolute
            let new = rel.wrapping_add(pos + 5);

            // Réécrire
            out[i + 1] = (new & 0xFF) as u8;
            out[i + 2] = ((new >> 8) & 0xFF) as u8;
            out[i + 3] = ((new >> 16) & 0xFF) as u8;
            out[i + 4] = ((new >> 24) & 0xFF) as u8;

            i += 5;
        } else {
            i += 1;
        }
    }

    Ok(out)
}

/// Reverses the x86 BCJ transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = data.to_vec();
    let len = out.len();

    let mut i = 0;
    while i + 4 < len {
        let opcode = out[i];

        if opcode == 0xE8 || opcode == 0xE9 {
            let val = (out[i + 1] as u32)
                | ((out[i + 2] as u32) << 8)
                | ((out[i + 3] as u32) << 16)
                | ((out[i + 4] as u32) << 24);

            let pos = i as u32;

            // Inverse: absolute → rel
            let new = val.wrapping_sub(pos + 5);

            out[i + 1] = (new & 0xFF) as u8;
            out[i + 2] = ((new >> 8) & 0xFF) as u8;
            out[i + 3] = ((new >> 16) & 0xFF) as u8;
            out[i + 4] = ((new >> 24) & 0xFF) as u8;

            i += 5;
        } else {
            i += 1;
        }
    }

    Ok(out)
}