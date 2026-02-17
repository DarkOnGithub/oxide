use crate::preprocessing::utils;
use crate::{OxideError, Result};

const BCJ_MAGIC: &[u8; 4] = b"OBC1";
const HEADER_SIZE: usize = 4 + 4;
const MIN_INPUT_SIZE: usize = 32;
const MIN_CONVERSIONS: usize = 2;

/// Converts raw binary bytes into 32-bit instruction words for BCJ-style logic.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_u32_words_le(data)
}

/// Applies a minimal x86 BCJ transform for E8/E9 rel32 operands.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < MIN_INPUT_SIZE {
        return Ok(data.to_vec());
    }

    let original_len = u32::try_from(data.len())
        .map_err(|_| OxideError::InvalidFormat("input too large for BCJ header"))?;

    let mut output = data.to_vec();
    let mut conversions = 0usize;
    let mut i = 0usize;

    while i + 4 < output.len() {
        let opcode = output[i];
        if opcode == 0xE8 || opcode == 0xE9 {
            let relative =
                i32::from_le_bytes([output[i + 1], output[i + 2], output[i + 3], output[i + 4]]);
            let source = (i as i32).wrapping_add(5);
            let absolute = relative.wrapping_add(source);
            output[i + 1..i + 5].copy_from_slice(&absolute.to_le_bytes());
            conversions += 1;
            i += 5;
        } else {
            i += 1;
        }
    }

    if conversions < MIN_CONVERSIONS {
        return Ok(data.to_vec());
    }

    let mut encoded = Vec::with_capacity(HEADER_SIZE + output.len());
    encoded.extend_from_slice(BCJ_MAGIC);
    encoded.extend_from_slice(&original_len.to_le_bytes());
    encoded.extend_from_slice(&output);
    Ok(encoded)
}

/// Reverses the x86 BCJ transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(BCJ_MAGIC) {
        return Ok(data.to_vec());
    }

    if data.len() < HEADER_SIZE {
        return Err(OxideError::InvalidFormat("BCJ data too short for header"));
    }

    let original_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut output = data[HEADER_SIZE..].to_vec();
    if output.len() != original_len {
        return Err(OxideError::InvalidFormat("BCJ payload size mismatch"));
    }

    let mut i = 0usize;
    while i + 4 < output.len() {
        let opcode = output[i];
        if opcode == 0xE8 || opcode == 0xE9 {
            let absolute =
                i32::from_le_bytes([output[i + 1], output[i + 2], output[i + 3], output[i + 4]]);
            let source = (i as i32).wrapping_add(5);
            let relative = absolute.wrapping_sub(source);
            output[i + 1..i + 5].copy_from_slice(&relative.to_le_bytes());
            i += 5;
        } else {
            i += 1;
        }
    }

    Ok(output)
}
