use core::fmt;
use core::mem::size_of;
use core::ptr;

use crate::{OxideError, Result};

const MIN_MATCH: usize = 4;
const LAST_LITERALS: usize = 5;
const MFLIMIT: usize = 12;
const HASH_LOG: u32 = 12;
const HASH_SIZE: usize = 1 << HASH_LOG;
const HASH_SEED: u32 = 2_654_435_761;
const MAX_OFFSET: usize = u16::MAX as usize;
const SKIP_STRENGTH: usize = 6;

/// Compresses data using the LZ4 algorithm.
///
/// Returns a byte vector starting with a 4-byte little-endian original size
/// followed by the LZ4 block.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "lz4 input exceeds 32-bit size prefix".to_string(),
        ));
    }

    let mut output = Vec::with_capacity(4 + max_compressed_size(data.len()));
    output.extend_from_slice(&(data.len() as u32).to_le_bytes());
    compress_block(data, &mut output);
    Ok(output)
}

/// Decompresses data using the LZ4 algorithm.
///
/// Expects a 4-byte little-endian size prefix followed by the LZ4 block.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 4 {
        return Err(OxideError::DecompressionError(
            "lz4 decode failed: missing 4-byte size prefix".to_string(),
        ));
    }

    let expected_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    decompress_block(&data[4..], expected_size)
        .map_err(|err| OxideError::DecompressionError(format!("lz4 decode failed: {err}")))
}

#[inline]
fn max_compressed_size(input_len: usize) -> usize {
    input_len + (input_len / 255) + 16
}

fn compress_block(input: &[u8], output: &mut Vec<u8>) {
    if input.len() < MFLIMIT + 1 {
        emit_last_literals(output, input, 0);
        return;
    }

    let mut table = [0u32; HASH_SIZE];
    let mut anchor = 0usize;
    let mut search_pos = 1usize;
    let mflimit = input.len() - MFLIMIT;
    let match_limit = input.len() - LAST_LITERALS;

    'main: while search_pos <= mflimit {
        let mut current = search_pos;
        let mut search_match_nb = 1usize << SKIP_STRENGTH;

        loop {
            if current > mflimit {
                break 'main;
            }

            let current_ptr = unsafe {
                // SAFETY: current <= mflimit means current + 4 <= input.len().
                input.as_ptr().add(current)
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
                        // SAFETY: candidate came from earlier valid positions hashed with >=4 bytes.
                        input.as_ptr().add(candidate)
                    };
                    let candidate_seq = unsafe {
                        // SAFETY: candidate_ptr points to at least 4 readable bytes.
                        load_u32(candidate_ptr)
                    };

                    if sequence == candidate_seq {
                        while current > anchor
                            && candidate > 0
                            && input[current - 1] == input[candidate - 1]
                        {
                            current -= 1;
                            candidate -= 1;
                        }

                        let literal_len = current - anchor;
                        let mut match_end = current + MIN_MATCH;
                        let candidate_end = candidate + MIN_MATCH;
                        match_end +=
                            count_match_bytes(input, match_end, candidate_end, match_limit);

                        let match_len = match_end - current;
                        emit_sequence(output, input, anchor, literal_len, offset, match_len);

                        anchor = match_end;
                        if anchor > mflimit {
                            break 'main;
                        }

                        let insert_pos = anchor.saturating_sub(2);
                        if insert_pos <= mflimit {
                            let hash = hash_sequence(unsafe {
                                // SAFETY: insert_pos <= mflimit means insert_pos + 4 <= input.len().
                                load_u32(input.as_ptr().add(insert_pos))
                            });
                            table[hash] = (insert_pos as u32) + 1;
                        }

                        search_pos = anchor;
                        continue 'main;
                    }
                }
            }

            current += search_match_nb >> SKIP_STRENGTH;
            search_match_nb += 1;
        }
    }

    emit_last_literals(output, input, anchor);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeError {
    ExpectedAnotherByte,
    LiteralOutOfBounds,
    OffsetOutOfBounds,
    OutputTooSmall { expected: usize, actual: usize },
    LengthOverflow,
    DecodedSizeMismatch { expected: usize, actual: usize },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedAnotherByte => f.write_str("expected another byte, found none"),
            Self::LiteralOutOfBounds => f.write_str("literal is out of bounds of the input"),
            Self::OffsetOutOfBounds => {
                f.write_str("the offset to copy is not contained in the decompressed buffer")
            }
            Self::OutputTooSmall { expected, actual } => {
                write!(
                    f,
                    "provided output is too small for the decompressed data, actual {actual}, expected {expected}"
                )
            }
            Self::LengthOverflow => f.write_str("decoded length overflows platform usize"),
            Self::DecodedSizeMismatch { expected, actual } => {
                write!(
                    f,
                    "decoded size mismatch, expected {expected} bytes, got {actual}"
                )
            }
        }
    }
}

fn decompress_block(
    input: &[u8],
    expected_size: usize,
) -> core::result::Result<Vec<u8>, DecodeError> {
    if input.is_empty() {
        return Err(DecodeError::ExpectedAnotherByte);
    }

    let mut output: Vec<u8> = Vec::with_capacity(expected_size);
    unsafe {
        // SAFETY: we only read from already-decoded regions and fully validate writes before copying.
        output.set_len(expected_size);
    }

    let mut input_pos = 0usize;
    let mut output_pos = 0usize;
    let input_ptr = input.as_ptr();
    let output_ptr = output.as_mut_ptr();

    while input_pos < input.len() {
        let token = input[input_pos];
        input_pos += 1;

        let mut literal_len = (token >> 4) as usize;
        if literal_len == 15 {
            literal_len = literal_len
                .checked_add(read_length(input, &mut input_pos)?)
                .ok_or(DecodeError::LengthOverflow)?;
        }

        if input_pos
            .checked_add(literal_len)
            .is_none_or(|end| end > input.len())
        {
            return Err(DecodeError::LiteralOutOfBounds);
        }

        if output_pos
            .checked_add(literal_len)
            .is_none_or(|end| end > expected_size)
        {
            return Err(DecodeError::OutputTooSmall {
                expected: output_pos.saturating_add(literal_len),
                actual: expected_size,
            });
        }

        unsafe {
            // SAFETY: both ranges were bounds-checked above and do not overlap.
            ptr::copy_nonoverlapping(
                input_ptr.add(input_pos),
                output_ptr.add(output_pos),
                literal_len,
            );
        }

        input_pos += literal_len;
        output_pos += literal_len;

        if input_pos == input.len() {
            return if output_pos == expected_size {
                Ok(output)
            } else {
                Err(DecodeError::DecodedSizeMismatch {
                    expected: expected_size,
                    actual: output_pos,
                })
            };
        }

        if input_pos + 2 > input.len() {
            return Err(DecodeError::ExpectedAnotherByte);
        }

        let offset = u16::from_le_bytes([input[input_pos], input[input_pos + 1]]) as usize;
        input_pos += 2;

        if offset == 0 || offset > output_pos {
            return Err(DecodeError::OffsetOutOfBounds);
        }

        let mut match_len = (token as usize & 0x0F) + MIN_MATCH;
        if (token & 0x0F) == 0x0F {
            match_len = match_len
                .checked_add(read_length(input, &mut input_pos)?)
                .ok_or(DecodeError::LengthOverflow)?;
        }

        if output_pos
            .checked_add(match_len)
            .is_none_or(|end| end > expected_size)
        {
            return Err(DecodeError::OutputTooSmall {
                expected: output_pos.saturating_add(match_len),
                actual: expected_size,
            });
        }

        unsafe {
            // SAFETY: offset is validated against output_pos, and destination is within output.
            copy_match(output_ptr.add(output_pos), offset, match_len);
        }

        output_pos += match_len;
    }

    if output_pos != expected_size {
        return Err(DecodeError::DecodedSizeMismatch {
            expected: expected_size,
            actual: output_pos,
        });
    }

    Ok(output)
}

#[inline]
fn read_length(input: &[u8], input_pos: &mut usize) -> core::result::Result<usize, DecodeError> {
    let mut len = 0usize;

    loop {
        let byte = *input
            .get(*input_pos)
            .ok_or(DecodeError::ExpectedAnotherByte)?;
        *input_pos += 1;

        len = len
            .checked_add(byte as usize)
            .ok_or(DecodeError::LengthOverflow)?;

        if byte != 255 {
            return Ok(len);
        }
    }
}

#[inline]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn copy_match(dst: *mut u8, offset: usize, len: usize) {
    let src = dst.sub(offset);

    if offset == 1 {
        // RLE fast path, common for long runs.
        let value = src.read();
        ptr::write_bytes(dst, value, len);
        return;
    }

    if offset >= len {
        ptr::copy_nonoverlapping(src, dst, len);
        return;
    }

    let mut copied = 0usize;
    while copied < len {
        let chunk = (len - copied).min(offset);
        let from = dst.add(copied).sub(offset);
        ptr::copy_nonoverlapping(from, dst.add(copied), chunk);
        copied += chunk;
    }
}

