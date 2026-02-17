use crate::preprocessing::utils;
use crate::{OxideError, Result};

const BPE_MAGIC: &[u8; 4] = b"OBP1";
const MIN_INPUT_SIZE: usize = 32;
const MIN_PAIR_FREQUENCY: u32 = 3;
const MAX_RULES: usize = 32;
const HEADER_FIXED_SIZE: usize = 4 + 4 + 1;
const RULE_SIZE: usize = 3;

/// Converts raw text bytes into integer symbol ids for BPE-style processing.
pub fn bytes_to_data(data: &[u8]) -> Vec<u32> {
    utils::bytes_to_symbols(data)
}

/// Applies a minimal byte-pair substitution transform.
///
/// The transform is only emitted when the transformed payload + header is
/// smaller than the input payload.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < MIN_INPUT_SIZE {
        return Ok(data.to_vec());
    }

    let original_len = u32::try_from(data.len())
        .map_err(|_| OxideError::InvalidFormat("input too large for BPE header"))?;

    let mut byte_counts = [0u32; 256];
    for &byte in data {
        byte_counts[byte as usize] += 1;
    }

    let unused_tokens: Vec<u8> = (0..=255)
        .filter(|&byte| byte_counts[byte] == 0)
        .map(|byte| byte as u8)
        .collect();
    if unused_tokens.is_empty() {
        return Ok(data.to_vec());
    }

    let mut pair_counts = [0u32; 65_536];
    for pair in data.windows(2) {
        let idx = pair_index(pair[0], pair[1]);
        pair_counts[idx] = pair_counts[idx].saturating_add(1);
    }

    let mut candidates = Vec::new();
    for (idx, &count) in pair_counts.iter().enumerate() {
        if count >= MIN_PAIR_FREQUENCY {
            candidates.push((count, idx as u16));
        }
    }
    if candidates.is_empty() {
        return Ok(data.to_vec());
    }

    candidates.sort_unstable_by(|(left_count, left_pair), (right_count, right_pair)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_pair.cmp(right_pair))
    });

    let max_rules = candidates.len().min(unused_tokens.len()).min(MAX_RULES);
    if max_rules == 0 {
        return Ok(data.to_vec());
    }

    let candidate_pairs: Vec<u16> = candidates
        .into_iter()
        .take(max_rules)
        .map(|(_, pair)| pair)
        .collect();

    let mut best_total_len = data.len();
    let mut best_rule_count = 0usize;
    let mut best_payload_len = data.len();

    let mut lookup = [-1i16; 65_536];
    for rule_count in 1..=max_rules {
        let pair = candidate_pairs[rule_count - 1] as usize;
        lookup[pair] = i16::from(unused_tokens[rule_count - 1]);

        let payload_len = encoded_length(data, &lookup);
        let total_len = HEADER_FIXED_SIZE
            .saturating_add(rule_count.saturating_mul(RULE_SIZE))
            .saturating_add(payload_len);

        if total_len < best_total_len {
            best_total_len = total_len;
            best_rule_count = rule_count;
            best_payload_len = payload_len;
        }
    }

    if best_rule_count == 0 {
        return Ok(data.to_vec());
    }

    let mut best_lookup = [-1i16; 65_536];
    for rule_index in 0..best_rule_count {
        let pair = candidate_pairs[rule_index] as usize;
        best_lookup[pair] = i16::from(unused_tokens[rule_index]);
    }

    let mut payload = Vec::with_capacity(best_payload_len);
    encode_payload(data, &best_lookup, &mut payload);

    let mut output = Vec::with_capacity(best_total_len);
    output.extend_from_slice(BPE_MAGIC);
    output.extend_from_slice(&original_len.to_le_bytes());
    output.push(best_rule_count as u8);

    for rule_index in 0..best_rule_count {
        let token = unused_tokens[rule_index];
        let pair = candidate_pairs[rule_index];
        output.push(token);
        output.push((pair >> 8) as u8);
        output.push((pair & 0x00FF) as u8);
    }

    output.extend_from_slice(&payload);
    Ok(output)
}

/// Reverses the BPE transform.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(BPE_MAGIC) {
        return Ok(data.to_vec());
    }

    if data.len() < HEADER_FIXED_SIZE {
        return Err(OxideError::InvalidFormat("BPE data too short for header"));
    }

    let original_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let rule_count = data[8] as usize;
    if rule_count == 0 {
        return Err(OxideError::InvalidFormat("BPE rule table is empty"));
    }

    let table_len = rule_count
        .checked_mul(RULE_SIZE)
        .ok_or(OxideError::InvalidFormat("BPE rule table size overflow"))?;
    let payload_start = HEADER_FIXED_SIZE
        .checked_add(table_len)
        .ok_or(OxideError::InvalidFormat("BPE header size overflow"))?;
    if payload_start > data.len() {
        return Err(OxideError::InvalidFormat("BPE header is truncated"));
    }

    let mut used_tokens = [false; 256];
    let mut token_pairs = [(0u8, 0u8); 256];

    let mut cursor = HEADER_FIXED_SIZE;
    for _ in 0..rule_count {
        let token = data[cursor];
        let first = data[cursor + 1];
        let second = data[cursor + 2];
        cursor += RULE_SIZE;

        if used_tokens[token as usize] {
            return Err(OxideError::InvalidFormat(
                "BPE header contains duplicate token",
            ));
        }
        used_tokens[token as usize] = true;
        token_pairs[token as usize] = (first, second);
    }

    let mut output = Vec::with_capacity(original_len);
    for &byte in &data[payload_start..] {
        if used_tokens[byte as usize] {
            let (first, second) = token_pairs[byte as usize];
            output.push(first);
            output.push(second);
        } else {
            output.push(byte);
        }
    }

    if output.len() != original_len {
        return Err(OxideError::InvalidFormat("BPE reconstructed size mismatch"));
    }

    Ok(output)
}

#[inline]
fn pair_index(first: u8, second: u8) -> usize {
    ((first as usize) << 8) | second as usize
}

fn encoded_length(data: &[u8], lookup: &[i16; 65_536]) -> usize {
    let mut i = 0usize;
    let mut len = 0usize;
    while i < data.len() {
        if i + 1 < data.len() {
            let idx = pair_index(data[i], data[i + 1]);
            if lookup[idx] >= 0 {
                len += 1;
                i += 2;
                continue;
            }
        }
        len += 1;
        i += 1;
    }
    len
}

fn encode_payload(data: &[u8], lookup: &[i16; 65_536], output: &mut Vec<u8>) {
    let mut i = 0usize;
    while i < data.len() {
        if i + 1 < data.len() {
            let idx = pair_index(data[i], data[i + 1]);
            let token = lookup[idx];
            if token >= 0 {
                output.push(token as u8);
                i += 2;
                continue;
            }
        }

        output.push(data[i]);
        i += 1;
    }
}
