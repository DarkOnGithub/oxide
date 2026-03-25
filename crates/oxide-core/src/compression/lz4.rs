use core::mem::size_of;
use core::ptr;

use crate::{OxideError, Result};

#[path = "lz4/copy.rs"]
mod copy;
#[path = "lz4/decode.rs"]
mod decode;

const MIN_MATCH: usize = 4;
const LAST_LITERALS: usize = 5;
const MFLIMIT: usize = 12;
const HASH_LOG: u32 = 12;
const HASH_SIZE: usize = 1 << HASH_LOG;
const HASH_SEED: u32 = 2_654_435_761;
const MAX_OFFSET: usize = u16::MAX as usize;

#[derive(Debug, Default)]
pub(crate) struct Lz4Scratch {
    table: Vec<u32>,
    history: Vec<u8>,
    output: Vec<u8>,
}

impl Lz4Scratch {
    fn prepare_table(table: &mut Vec<u32>) {
        if table.len() != HASH_SIZE {
            table.resize(HASH_SIZE, 0);
        }
        table.fill(0);
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.table.capacity().saturating_mul(size_of::<u32>())
            + self.history.capacity()
            + self.output.capacity()
    }

    pub(crate) fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    pub(crate) fn recycle_output(&mut self, mut output: Vec<u8>) {
        output.clear();
        self.output = output;
    }
}

#[derive(Debug, Clone, Copy)]
struct CompressionTuning {
    skip_strength: usize,
}

const DEFAULT_TUNING: CompressionTuning = CompressionTuning { skip_strength: 6 };

/// Compresses data using the LZ4 algorithm.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    let mut scratch = Lz4Scratch::default();
    apply_with_scratch(data, &mut scratch)
}

pub(crate) fn apply_with_scratch(data: &[u8], scratch: &mut Lz4Scratch) -> Result<Vec<u8>> {
    if data.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "lz4 input exceeds 32-bit size prefix".to_string(),
        ));
    }

    let mut output = scratch.take_output();
    apply_into_vec_with_table(data, &mut output, &mut scratch.table)?;

    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    scratch: &mut Lz4Scratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    apply_into_vec_with_table(data, output, &mut scratch.table)
}

fn apply_into_vec_with_table(
    data: &[u8],
    output: &mut Vec<u8>,
    table: &mut Vec<u32>,
) -> Result<()> {
    if data.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "lz4 input exceeds 32-bit size prefix".to_string(),
        ));
    }

    output.clear();
    let required_capacity = 4 + max_compressed_size(data.len());
    if output.capacity() < required_capacity {
        output.reserve(required_capacity - output.capacity());
    }
    output.extend_from_slice(&(data.len() as u32).to_le_bytes());
    compress_block(data, 0, output, table, DEFAULT_TUNING);
    Ok(())
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut Lz4Scratch) {
    scratch.recycle_output(output);
}

/// Decompresses data using the LZ4 algorithm.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let mut scratch = Lz4Scratch::default();
    reverse_with_scratch(data, &mut scratch)
}

pub(crate) fn reverse_with_scratch(data: &[u8], scratch: &mut Lz4Scratch) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    reverse_into_vec(data, &mut output)?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(data: &[u8], output: &mut Vec<u8>) -> Result<()> {
    if data.len() < 4 {
        return Err(OxideError::DecompressionError(
            "lz4 decode failed: missing 4-byte size prefix".to_string(),
        ));
    }

    let expected_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    decode::decompress_block_into(&data[4..], expected_size, output)
        .map_err(|err| OxideError::DecompressionError(format!("lz4 decode failed: {err}")))
}

#[inline]
fn max_compressed_size(input_len: usize) -> usize {
    input_len + (input_len / 255) + 16
}

fn compress_block(
    history: &[u8],
    input_start: usize,
    output: &mut Vec<u8>,
    table: &mut Vec<u32>,
    tuning: CompressionTuning,
) {
    let input_len = history.len().saturating_sub(input_start);
    if input_len < MFLIMIT + 1 {
        emit_last_literals(output, history, input_start);
        return;
    }

    Lz4Scratch::prepare_table(table);
    let mut anchor = input_start;
    let mut search_pos = input_start + 1;
    let mflimit = history.len() - MFLIMIT;
    let match_limit = history.len() - LAST_LITERALS;

    'main: while search_pos <= mflimit {
        let mut current = search_pos;
        let mut search_match_nb = 1usize << tuning.skip_strength;

        loop {
            if current > mflimit {
                break 'main;
            }

            let current_ptr = unsafe {
                // SAFETY: current <= mflimit means current + 4 <= history.len().
                history.as_ptr().add(current)
            };
            let sequence = unsafe {
                // SAFETY: current_ptr points to at least 4 readable bytes.
                load_u32(current_ptr)
            };

            let hash = hash_sequence(sequence);
            let entry = table[hash];
            table[hash] = (current as u32) + 1;

            if entry != 0 {
                let mut candidate = (entry - 1) as usize;
                let offset = current - candidate;

                if offset <= MAX_OFFSET {
                    let candidate_ptr = unsafe {
                        // SAFETY: candidate came from a previously inserted position.
                        history.as_ptr().add(candidate)
                    };
                    let candidate_seq = unsafe {
                        // SAFETY: candidate_ptr points to at least 4 readable bytes.
                        load_u32(candidate_ptr)
                    };

                    if sequence == candidate_seq {
                        while current > anchor
                            && candidate > 0
                            && history[current - 1] == history[candidate - 1]
                        {
                            current -= 1;
                            candidate -= 1;
                        }

                        let literal_len = current - anchor;
                        let mut match_end = current + MIN_MATCH;
                        let candidate_end = candidate + MIN_MATCH;
                        match_end +=
                            count_match_bytes(history, match_end, candidate_end, match_limit);

                        let match_len = match_end - current;
                        emit_sequence(output, history, anchor, literal_len, offset, match_len);

                        anchor = match_end;
                        if anchor > mflimit {
                            break 'main;
                        }

                        let insert_pos = anchor.saturating_sub(2);
                        if insert_pos <= mflimit {
                            let hash = hash_sequence(unsafe {
                                // SAFETY: insert_pos <= mflimit means insert_pos + 4 <= history.len().
                                load_u32(history.as_ptr().add(insert_pos))
                            });
                            table[hash] = (insert_pos as u32) + 1;
                        }

                        search_pos = anchor;
                        continue 'main;
                    }
                }
            }

            current += search_match_nb >> tuning.skip_strength;
            search_match_nb += 1;
        }
    }

    emit_last_literals(output, history, anchor);
}

#[inline]
fn hash_sequence(sequence: u32) -> usize {
    (sequence.wrapping_mul(HASH_SEED) >> (32 - HASH_LOG)) as usize
}

#[inline]
fn emit_sequence(
    output: &mut Vec<u8>,
    input: &[u8],
    literal_start: usize,
    literal_len: usize,
    offset: usize,
    match_len: usize,
) {
    debug_assert!(match_len >= MIN_MATCH);
    debug_assert!(offset > 0 && offset <= MAX_OFFSET);

    let token_pos = output.len();
    output.push(0);

    let literal_token = literal_len.min(15) as u8;
    let match_token = (match_len - MIN_MATCH).min(15) as u8;
    output[token_pos] = (literal_token << 4) | match_token;

    if literal_len >= 15 {
        write_len(output, literal_len - 15);
    }

    output.extend_from_slice(&input[literal_start..literal_start + literal_len]);
    output.extend_from_slice(&(offset as u16).to_le_bytes());

    let match_extra = match_len - MIN_MATCH;
    if match_extra >= 15 {
        write_len(output, match_extra - 15);
    }
}

#[inline]
fn emit_last_literals(output: &mut Vec<u8>, input: &[u8], literal_start: usize) {
    let literal_len = input.len() - literal_start;
    output.push((literal_len.min(15) as u8) << 4);

    if literal_len >= 15 {
        write_len(output, literal_len - 15);
    }

    output.extend_from_slice(&input[literal_start..]);
}

#[inline]
fn write_len(output: &mut Vec<u8>, mut len: usize) {
    while len >= 255 {
        output.push(255);
        len -= 255;
    }
    output.push(len as u8);
}

#[inline]
fn count_match_bytes(input: &[u8], left: usize, right: usize, end: usize) -> usize {
    debug_assert!(left <= end && right <= end && end <= input.len());

    if left >= end {
        return 0;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if avx2_enabled() {
            return unsafe {
                // SAFETY: guarded by runtime feature detection.
                count_match_bytes_avx2(input, left, right, end)
            };
        }
    }

    unsafe {
        // SAFETY: caller guarantees [left, end) and [right, right + (end-left)) are valid ranges.
        count_match_bytes_scalar(input, left, right, end)
    }
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn count_match_bytes_scalar(
    input: &[u8],
    mut left: usize,
    mut right: usize,
    end: usize,
) -> usize {
    let start = left;
    let base = input.as_ptr();
    let word = size_of::<usize>();

    while left + word <= end {
        let lhs = load_usize(base.add(left));
        let rhs = load_usize(base.add(right));

        if lhs == rhs {
            left += word;
            right += word;
        } else {
            let diff = lhs ^ rhs;
            left += (diff.to_le().trailing_zeros() as usize) >> 3;
            return left - start;
        }
    }

    while left < end && *base.add(left) == *base.add(right) {
        left += 1;
        right += 1;
    }

    left - start
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn avx2_enabled() -> bool {
    use std::sync::OnceLock;

    static HAS_AVX2: OnceLock<bool> = OnceLock::new();
    *HAS_AVX2.get_or_init(|| std::arch::is_x86_feature_detected!("avx2"))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn count_match_bytes_avx2(
    input: &[u8],
    mut left: usize,
    mut right: usize,
    end: usize,
) -> usize {
    use core::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8,
    };

    let start = left;
    let base = input.as_ptr();

    while left + 32 <= end {
        let lhs = _mm256_loadu_si256(base.add(left) as *const __m256i);
        let rhs = _mm256_loadu_si256(base.add(right) as *const __m256i);
        let mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lhs, rhs)) as u32;

        if mask == u32::MAX {
            left += 32;
            right += 32;
        } else {
            return (left - start) + (!mask).trailing_zeros() as usize;
        }
    }

    (left - start) + count_match_bytes_scalar(input, left, right, end)
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn load_u32(ptr: *const u8) -> u32 {
    ptr::read_unaligned(ptr as *const u32)
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn load_usize(ptr: *const u8) -> usize {
    ptr::read_unaligned(ptr as *const usize)
}

#[cfg(test)]
mod tests {
    use super::{Lz4Scratch, apply_into_vec, apply_with_scratch, recycle_output, reverse_into_vec};

    #[test]
    fn direct_buffer_round_trip_reuses_output_vec() {
        let payload = b"banana bandana banana";
        let mut scratch = Lz4Scratch::default();
        let mut compressed = Vec::with_capacity(64);
        let original_ptr = compressed.as_ptr();

        apply_into_vec(payload, &mut scratch, &mut compressed)
            .expect("lz4 compression should succeed");

        assert_eq!(compressed.as_ptr(), original_ptr);

        let mut decoded = Vec::with_capacity(payload.len());
        let decoded_ptr = decoded.as_ptr();
        reverse_into_vec(&compressed, &mut decoded).expect("lz4 decompression should succeed");

        assert_eq!(decoded.as_ptr(), decoded_ptr);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn scratch_output_can_be_recycled_for_reuse() {
        let payload = b"banana bandana banana";
        let mut scratch = Lz4Scratch::default();

        let compressed =
            apply_with_scratch(payload, &mut scratch).expect("lz4 compression should succeed");
        recycle_output(compressed.clone(), &mut scratch);
        let decoded = super::reverse_with_scratch(&compressed, &mut scratch)
            .expect("lz4 decompression should succeed");

        assert_eq!(decoded, payload);
    }
}
