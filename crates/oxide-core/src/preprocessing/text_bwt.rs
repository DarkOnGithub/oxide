use crate::preprocessing::utils;
use crate::{OxideError, Result};

const BWT_MAGIC: &[u8; 4] = b"OBW1";
const MIN_INPUT_SIZE: usize = 64;
const BLOCK_SIZE: usize = 2048;
const HEADER_FIXED_SIZE: usize = 4 + 4 + 4 + 4;

/// Converts raw text bytes into integer symbol ids for BWT-style processing.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_symbols(data)
}

/// Applies a blockwise Burrows-Wheeler transform.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < MIN_INPUT_SIZE {
        return Ok(data.to_vec());
    }

    let original_len = u32::try_from(data.len())
        .map_err(|_| OxideError::InvalidFormat("input too large for BWT header"))?;

    let block_count = data.len().div_ceil(BLOCK_SIZE);
    let block_count_u32 =
        u32::try_from(block_count).map_err(|_| OxideError::InvalidFormat("too many BWT blocks"))?;
    let block_size_u32 = u32::try_from(BLOCK_SIZE)
        .map_err(|_| OxideError::InvalidFormat("invalid BWT block size"))?;

    let mut primary_indices = Vec::with_capacity(block_count);
    let mut payload = Vec::with_capacity(data.len());

    for block in data.chunks(BLOCK_SIZE) {
        let (encoded, primary) = encode_block(block);
        primary_indices.push(primary as u32);
        payload.extend_from_slice(&encoded);
    }

    let mut output = Vec::with_capacity(HEADER_FIXED_SIZE + block_count * 4 + payload.len());
    output.extend_from_slice(BWT_MAGIC);
    output.extend_from_slice(&original_len.to_le_bytes());
    output.extend_from_slice(&block_size_u32.to_le_bytes());
    output.extend_from_slice(&block_count_u32.to_le_bytes());
    for &index in &primary_indices {
        output.extend_from_slice(&index.to_le_bytes());
    }
    output.extend_from_slice(&payload);
    Ok(output)
}

/// Reverses the blockwise Burrows-Wheeler transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(BWT_MAGIC) {
        return Ok(data.to_vec());
    }

    if data.len() < HEADER_FIXED_SIZE {
        return Err(OxideError::InvalidFormat("BWT data too short for header"));
    }

    let original_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let block_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let block_count = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;

    if block_size == 0 {
        return Err(OxideError::InvalidFormat("BWT block size must be non-zero"));
    }
    if block_count == 0 && original_len != 0 {
        return Err(OxideError::InvalidFormat("BWT block count is zero"));
    }

    let index_table_len = block_count
        .checked_mul(4)
        .ok_or(OxideError::InvalidFormat("BWT index table size overflow"))?;
    let payload_start = HEADER_FIXED_SIZE
        .checked_add(index_table_len)
        .ok_or(OxideError::InvalidFormat("BWT header size overflow"))?;
    if payload_start > data.len() {
        return Err(OxideError::InvalidFormat("BWT index table is truncated"));
    }

    let payload = &data[payload_start..];
    if payload.len() != original_len {
        return Err(OxideError::InvalidFormat("BWT payload size mismatch"));
    }

    let expected_block_count = original_len.div_ceil(block_size);
    if block_count != expected_block_count {
        return Err(OxideError::InvalidFormat("BWT block count mismatch"));
    }

    let mut indices = Vec::with_capacity(block_count);
    let mut cursor = HEADER_FIXED_SIZE;
    for _ in 0..block_count {
        let index = u32::from_le_bytes([
            data[cursor],
            data[cursor + 1],
            data[cursor + 2],
            data[cursor + 3],
        ]);
        indices.push(index as usize);
        cursor += 4;
    }

    let mut output = Vec::with_capacity(original_len);
    for (block_id, block) in payload.chunks(block_size).enumerate() {
        let primary = indices[block_id];
        let decoded = decode_block(block, primary)?;
        output.extend_from_slice(&decoded);
    }

    if output.len() != original_len {
        return Err(OxideError::InvalidFormat("BWT reconstructed size mismatch"));
    }

    Ok(output)
}

fn encode_block(block: &[u8]) -> (Vec<u8>, usize) {
    let n = block.len();
    if n == 0 {
        return (Vec::new(), 0);
    }

    let mut rotations: Vec<usize> = (0..n).collect();
    rotations.sort_unstable_by(|&left, &right| compare_rotations(block, left, right));

    let primary = rotations.iter().position(|&idx| idx == 0).unwrap_or(0);
    let mut last_column = Vec::with_capacity(n);
    for &start in &rotations {
        let last = if start == 0 {
            block[n - 1]
        } else {
            block[start - 1]
        };
        last_column.push(last);
    }

    (last_column, primary)
}

fn decode_block(last_column: &[u8], primary: usize) -> Result<Vec<u8>> {
    let n = last_column.len();
    if n == 0 {
        if primary == 0 {
            return Ok(Vec::new());
        }
        return Err(OxideError::InvalidFormat(
            "BWT primary index out of range for empty block",
        ));
    }
    if primary >= n {
        return Err(OxideError::InvalidFormat("BWT primary index out of range"));
    }

    let mut counts = [0usize; 256];
    for &byte in last_column {
        counts[byte as usize] += 1;
    }

    let mut first_offsets = [0usize; 256];
    let mut total = 0usize;
    for byte in 0..256 {
        first_offsets[byte] = total;
        total += counts[byte];
    }

    let mut seen = [0usize; 256];
    let mut next = vec![0usize; n];
    for (idx, &byte) in last_column.iter().enumerate() {
        let byte_index = byte as usize;
        let rank = seen[byte_index];
        next[idx] = first_offsets[byte_index] + rank;
        seen[byte_index] += 1;
    }

    let mut output = vec![0u8; n];
    let mut pos = primary;
    for out_idx in (0..n).rev() {
        output[out_idx] = last_column[pos];
        pos = next[pos];
    }

    Ok(output)
}

fn compare_rotations(block: &[u8], left: usize, right: usize) -> std::cmp::Ordering {
    if left == right {
        return std::cmp::Ordering::Equal;
    }

    let n = block.len();
    for offset in 0..n {
        let left_idx = {
            let idx = left + offset;
            if idx >= n { idx - n } else { idx }
        };
        let right_idx = {
            let idx = right + offset;
            if idx >= n { idx - n } else { idx }
        };

        let cmp = block[left_idx].cmp(&block[right_idx]);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }

    std::cmp::Ordering::Equal
}
