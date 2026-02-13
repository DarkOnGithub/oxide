use core::cmp::min;
use core::fmt;
use core::mem::size_of;
use core::ptr;

use crate::{OxideError, Result};

const MAGIC: &[u8; 4] = b"OLZ1";
const VERSION: u8 = 1;
const PROPS: u8 = 0x5D; // lc=3, lp=0, pb=2 marker for this internal profile

const WINDOW_SIZE: usize = 256 * 1024;
const DICT_SIZE: usize = WINDOW_SIZE;
const MIN_MATCH: usize = 2;
const MAX_MATCH: usize = 273;
const HASH_LOG: u32 = 16;
const HASH_SIZE: usize = 1 << HASH_LOG;
const MAX_CHAIN: usize = 48;
const LITERAL_CONTEXT_BITS: usize = 3;
const LITERAL_CONTEXTS: usize = 1 << LITERAL_CONTEXT_BITS;
const LOOKAHEAD_THRESHOLD: usize = 24;
const GOOD_MATCH_BREAK: usize = 96;

const BIT_MODEL_TOTAL: u16 = 1 << 11;
const BIT_MODEL_INIT: u16 = BIT_MODEL_TOTAL / 2;
const MOVE_BITS: u16 = 5;
const TOP_VALUE: u32 = 1 << 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Literal(u8),
    Match {
        len: usize,
        dist: usize,
        rep_index: Option<u8>,
    },
    End,
}

/// Compresses data using an LZMA-like range encoding algorithm.
///
/// The output includes a 4-byte size prefix, a magic header "OLZ1",
/// properties byte, dictionary size, and the compressed payload.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "lzma-like encode failed: input exceeds 32-bit size prefix".to_string(),
        ));
    }

    let tokens = tokenize(data);
    let mut models = Models::new();
    let mut encoder = RangeEncoder::new();
    let mut prev_byte = 0u8;
    let mut reps = [1usize; 4];
    let mut input_pos = 0usize;

    for token in tokens {
        match token {
            Token::End => {
                encoder.encode_bit(&mut models.is_end, true);
            }
            Token::Literal(byte) => {
                encoder.encode_bit(&mut models.is_end, false);
                encoder.encode_bit(&mut models.is_match, false);
                encode_literal(&mut encoder, &mut models, prev_byte, byte);
                prev_byte = byte;
                input_pos += 1;
            }
            Token::Match {
                len,
                dist,
                rep_index,
            } => {
                encoder.encode_bit(&mut models.is_end, false);
                encoder.encode_bit(&mut models.is_match, true);
                if let Some(rep_idx) = rep_index {
                    encoder.encode_bit(&mut models.is_rep, true);
                    encode_rep_index(&mut encoder, &mut models, rep_idx);
                } else {
                    encoder.encode_bit(&mut models.is_rep, false);
                    encode_dist(&mut encoder, &mut models, dist);
                }
                encode_len(&mut encoder, &mut models, len);
                update_reps(&mut reps, dist);
                let end = input_pos + len;
                if end > data.len() {
                    return Err(OxideError::CompressionError(
                        "lzma-like encode failed: match advanced beyond input".to_string(),
                    ));
                }
                prev_byte = data[end - 1];
                input_pos = end;
            }
        }
    }

    if input_pos != data.len() {
        return Err(OxideError::CompressionError(format!(
            "lzma-like encode failed: token stream consumed {input_pos} bytes for {}-byte input",
            data.len()
        )));
    }

    let payload = encoder.finish();
    if payload.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "lzma-like encode failed: payload exceeds 32-bit size".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(4 + 4 + 1 + 1 + 4 + 4 + payload.len());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.push(PROPS);
    out.extend_from_slice(&(DICT_SIZE as u32).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decompresses data using the LZMA-like range encoding algorithm.
///
/// Expects the format produced by [`apply`].
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 4 {
        return Err(OxideError::DecompressionError(
            "lzma-like decode failed: missing 4-byte size prefix".to_string(),
        ));
    }

    let expected = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    decode_stream(&data[4..], expected).map_err(|err| {
        OxideError::DecompressionError(format!("lzma-like decode failed: {err}"))
    })
}

fn decode_stream(input: &[u8], expected_size: usize) -> core::result::Result<Vec<u8>, DecodeError> {
    const HEADER: usize = 4 + 1 + 1 + 4 + 4;
    if input.len() < HEADER {
        return Err(DecodeError::UnexpectedEof);
    }
    if &input[..4] != MAGIC {
        return Err(DecodeError::InvalidMagic);
    }
    if input[4] != VERSION {
        return Err(DecodeError::UnsupportedVersion(input[4]));
    }
    if input[5] != PROPS {
        return Err(DecodeError::InvalidProperties(input[5]));
    }

    let dict_size = u32::from_le_bytes([input[6], input[7], input[8], input[9]]) as usize;
    if dict_size != DICT_SIZE {
        return Err(DecodeError::InvalidDictionarySize(dict_size));
    }

    let payload_len = u32::from_le_bytes([input[10], input[11], input[12], input[13]]) as usize;
    if input.len() != HEADER + payload_len {
        return Err(DecodeError::PayloadSizeMismatch {
            expected: payload_len,
            actual: input.len().saturating_sub(HEADER),
        });
    }
    let payload = &input[HEADER..];

    let mut models = Models::new();
    let mut decoder = RangeDecoder::new(payload)?;
    let mut output = Vec::with_capacity(expected_size);
    let mut prev_byte = 0u8;
    let mut reps = [1usize; 4];

    loop {
        let is_end = decoder.decode_bit(&mut models.is_end)?;
        if is_end {
            break;
        }

        let is_match = decoder.decode_bit(&mut models.is_match)?;
        if !is_match {
            let byte = decode_literal(&mut decoder, &mut models, prev_byte)?;
            output.push(byte);
            prev_byte = byte;
            if output.len() > expected_size {
                return Err(DecodeError::OutputTooLarge {
                    expected: expected_size,
                    actual: output.len(),
                });
            }
            continue;
        }

        let is_rep = decoder.decode_bit(&mut models.is_rep)?;
        let dist = if is_rep {
            let rep_index = decode_rep_index(&mut decoder, &mut models)?;
            reps[rep_index as usize]
        } else {
            decode_dist(&mut decoder, &mut models)?
        };

        let len = decode_len(&mut decoder, &mut models)?;
        if dist == 0 || dist > min(DICT_SIZE, output.len()) {
            return Err(DecodeError::InvalidMatchDistance {
                distance: dist,
                produced: output.len(),
            });
        }
        if output.len() + len > expected_size {
            return Err(DecodeError::OutputTooLarge {
                expected: expected_size,
                actual: output.len() + len,
            });
        }
        for _ in 0..len {
            let idx = output.len() - dist;
            let b = output[idx];
            output.push(b);
        }
        prev_byte = *output.last().expect("match produced at least one byte");
        update_reps(&mut reps, dist);
    }

    if output.len() != expected_size {
        return Err(DecodeError::OutputSizeMismatch {
            expected: expected_size,
            actual: output.len(),
        });
    }

    decoder.finish()?;
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
struct Match {
    len: usize,
    dist: usize,
}

#[derive(Debug, Clone, Copy)]
struct Choice {
    len: usize,
    dist: usize,
    rep_index: Option<u8>,
    score: isize,
}

fn tokenize(input: &[u8]) -> Vec<Token> {
    if input.is_empty() {
        return vec![Token::End];
    }

    let mut tokens = Vec::with_capacity(input.len() / 2 + 4);
    let mut head = vec![-1_i32; HASH_SIZE];
    let mut prev = vec![-1_i32; input.len()];
    let mut reps = [1usize; 4];
    let mut pos = 0usize;

    while pos < input.len() {
        let best = best_choice(input, pos, &head, &prev, &reps);

        if let Some(choice) = best {
            let mut inserted_pos = false;
            if choice.len <= LOOKAHEAD_THRESHOLD && pos + 1 < input.len() {
                insert_position(input, pos, &mut head, &mut prev);
                inserted_pos = true;
                let next = best_choice(input, pos + 1, &head, &prev, &reps);
                if next.map(|cand| cand.score).unwrap_or(isize::MIN) > choice.score + 8 {
                    tokens.push(Token::Literal(input[pos]));
                    pos += 1;
                    continue;
                }
            }

            let end = pos + choice.len;
            if inserted_pos {
                for p in pos + 1..end {
                    insert_position(input, p, &mut head, &mut prev);
                }
            } else {
                for p in pos..end {
                    insert_position(input, p, &mut head, &mut prev);
                }
            }

            tokens.push(Token::Match {
                len: choice.len,
                dist: choice.dist,
                rep_index: choice.rep_index,
            });
            update_reps(&mut reps, choice.dist);
            pos = end;
        } else {
            insert_position(input, pos, &mut head, &mut prev);
            tokens.push(Token::Literal(input[pos]));
            pos += 1;
        }
    }

    tokens.push(Token::End);
    tokens
}

fn best_choice(
    input: &[u8],
    pos: usize,
    head: &[i32],
    prev: &[i32],
    reps: &[usize; 4],
) -> Option<Choice> {
    let max_len = min(MAX_MATCH, input.len().saturating_sub(pos));
    if max_len < MIN_MATCH {
        return None;
    }

    let mut best: Option<Choice> = None;

    for (idx, &dist) in reps.iter().enumerate() {
        if dist == 0 || dist > pos || dist > WINDOW_SIZE {
            continue;
        }
        let cand = pos - dist;
        let len = match_len(input, pos, cand, max_len);
        if len < MIN_MATCH {
            continue;
        }
        let score = len as isize * 8 - (14 + (idx as isize * 2));
        if score <= 0 {
            continue;
        }
        let choice = Choice {
            len,
            dist,
            rep_index: Some(idx as u8),
            score,
        };
        if better_choice(choice, best) {
            best = Some(choice);
        }
    }

    if let Some(m) = find_best_new_match(input, pos, head, prev, max_len) {
        let score = m.len as isize * 8 - 30;
        if score > 0 {
            let choice = Choice {
                len: m.len,
                dist: m.dist,
                rep_index: None,
                score,
            };
            if better_choice(choice, best) {
                best = Some(choice);
            }
        }
    }

    best
}

#[inline]
fn better_choice(candidate: Choice, current: Option<Choice>) -> bool {
    match current {
        None => true,
        Some(existing) => {
            candidate.score > existing.score
                || (candidate.score == existing.score
                    && (candidate.len > existing.len
                        || (candidate.len == existing.len && candidate.dist < existing.dist)))
        }
    }
}

fn find_best_new_match(
    input: &[u8],
    pos: usize,
    head: &[i32],
    prev: &[i32],
    max_len: usize,
) -> Option<Match> {
    let hash = hash_at(input, pos);
    let mut candidate = head[hash];
    let mut best = Match { len: 0, dist: 0 };
    let mut depth = 0usize;

    while candidate >= 0 && depth < MAX_CHAIN {
        let cand = candidate as usize;
        if cand >= pos {
            break;
        }
        let dist = pos - cand;
        if dist > WINDOW_SIZE {
            break;
        }
        if input[cand] != input[pos] || input[cand + 1] != input[pos + 1] {
            candidate = prev[cand];
            depth += 1;
            continue;
        }
        if best.len >= MIN_MATCH {
            let probe = best.len.min(max_len - 1);
            if input[cand + probe] != input[pos + probe] {
                candidate = prev[cand];
                depth += 1;
                continue;
            }
        }

        let len = match_len(input, pos, cand, max_len);
        if len > best.len || (len == best.len && dist < best.dist) {
            best = Match { len, dist };
            if len == max_len {
                break;
            }
            if len >= GOOD_MATCH_BREAK {
                break;
            }
        }

        candidate = prev[cand];
        depth += 1;
    }

    if best.len >= MIN_MATCH {
        Some(best)
    } else {
        None
    }
}

#[inline]
fn match_len(input: &[u8], left: usize, right: usize, max_len: usize) -> usize {
    let mut i = 0usize;
    let word = size_of::<usize>();
    let left_ptr = unsafe {
        // SAFETY: callers guarantee `left` is valid for `max_len`.
        input.as_ptr().add(left)
    };
    let right_ptr = unsafe {
        // SAFETY: callers guarantee `right` is valid for `max_len`.
        input.as_ptr().add(right)
    };

    while i + word <= max_len {
        let lhs = unsafe {
            // SAFETY: both ranges have at least `word` bytes remaining.
            ptr::read_unaligned(left_ptr.add(i) as *const usize)
        };
        let rhs = unsafe {
            // SAFETY: both ranges have at least `word` bytes remaining.
            ptr::read_unaligned(right_ptr.add(i) as *const usize)
        };
        if lhs == rhs {
            i += word;
            continue;
        }
        for j in 0..word {
            if input[left + i + j] != input[right + i + j] {
                return i + j;
            }
        }
    }
    while i < max_len && input[left + i] == input[right + i] {
        i += 1;
    }
    i
}

#[inline]
fn insert_position(input: &[u8], pos: usize, head: &mut [i32], prev: &mut [i32]) {
    if pos >= input.len() {
        return;
    }
    let hash = hash_at(input, pos);
    prev[pos] = head[hash];
    head[hash] = pos as i32;
}

#[inline]
fn hash_at(input: &[u8], pos: usize) -> usize {
    let b0 = input[pos] as u32;
    let b1 = input.get(pos + 1).copied().unwrap_or(0) as u32;
    let b2 = input.get(pos + 2).copied().unwrap_or(0) as u32;
    let b3 = input.get(pos + 3).copied().unwrap_or(0) as u32;
    let seq = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    ((seq.wrapping_mul(0x7FEB_352D) >> (32 - HASH_LOG)) as usize) & (HASH_SIZE - 1)
}

#[inline]
fn update_reps(reps: &mut [usize; 4], dist: usize) {
    if let Some(pos) = reps.iter().position(|&d| d == dist) {
        for i in (1..=pos).rev() {
            reps[i] = reps[i - 1];
        }
        reps[0] = dist;
    } else {
        for i in (1..4).rev() {
            reps[i] = reps[i - 1];
        }
        reps[0] = dist;
    }
}

#[derive(Debug, Clone)]
struct Models {
    is_end: u16,
    is_match: u16,
    is_rep: u16,
    rep_index: [u16; 3],
    len_bits: [u16; 9],
    dist_bits: [u16; 18],
    literal: [[u16; 8]; LITERAL_CONTEXTS],
}

impl Models {
    fn new() -> Self {
        Self {
            is_end: BIT_MODEL_INIT,
            is_match: BIT_MODEL_INIT,
            is_rep: BIT_MODEL_INIT,
            rep_index: [BIT_MODEL_INIT; 3],
            len_bits: [BIT_MODEL_INIT; 9],
            dist_bits: [BIT_MODEL_INIT; 18],
            literal: [[BIT_MODEL_INIT; 8]; LITERAL_CONTEXTS],
        }
    }
}

fn encode_literal(encoder: &mut RangeEncoder, models: &mut Models, prev_byte: u8, byte: u8) {
    let ctx = (prev_byte >> (8 - LITERAL_CONTEXT_BITS as u8)) as usize;
    for i in 0..8 {
        let bit = (byte >> (7 - i)) & 1 != 0;
        encoder.encode_bit(&mut models.literal[ctx][i], bit);
    }
}

fn decode_literal(
    decoder: &mut RangeDecoder<'_>,
    models: &mut Models,
    prev_byte: u8,
) -> core::result::Result<u8, DecodeError> {
    let ctx = (prev_byte >> (8 - LITERAL_CONTEXT_BITS as u8)) as usize;
    let mut byte = 0u8;
    for i in 0..8 {
        let bit = decoder.decode_bit(&mut models.literal[ctx][i])?;
        byte = (byte << 1) | bit as u8;
    }
    Ok(byte)
}

fn encode_rep_index(encoder: &mut RangeEncoder, models: &mut Models, index: u8) {
    debug_assert!(index < 4);
    let high = (index >> 1) & 1 != 0;
    let low = index & 1 != 0;
    encoder.encode_bit(&mut models.rep_index[0], high);
    if high {
        encoder.encode_bit(&mut models.rep_index[2], low);
    } else {
        encoder.encode_bit(&mut models.rep_index[1], low);
    }
}

fn decode_rep_index(
    decoder: &mut RangeDecoder<'_>,
    models: &mut Models,
) -> core::result::Result<u8, DecodeError> {
    let high = decoder.decode_bit(&mut models.rep_index[0])?;
    let low = if high {
        decoder.decode_bit(&mut models.rep_index[2])?
    } else {
        decoder.decode_bit(&mut models.rep_index[1])?
    };
    Ok(((high as u8) << 1) | low as u8)
}

fn encode_len(encoder: &mut RangeEncoder, models: &mut Models, len: usize) {
    debug_assert!((MIN_MATCH..=MAX_MATCH).contains(&len));
    let value = (len - MIN_MATCH) as u32;
    for i in 0..9 {
        let shift = 8 - i;
        let bit = ((value >> shift) & 1) != 0;
        encoder.encode_bit(&mut models.len_bits[i], bit);
    }
}

fn decode_len(
    decoder: &mut RangeDecoder<'_>,
    models: &mut Models,
) -> core::result::Result<usize, DecodeError> {
    let mut value = 0u32;
    for i in 0..9 {
        let bit = decoder.decode_bit(&mut models.len_bits[i])?;
        value = (value << 1) | bit as u32;
    }
    let len = value as usize + MIN_MATCH;
    if len > MAX_MATCH {
        return Err(DecodeError::InvalidMatchLength(len));
    }
    Ok(len)
}

fn encode_dist(encoder: &mut RangeEncoder, models: &mut Models, dist: usize) {
    debug_assert!((1..=DICT_SIZE).contains(&dist));
    let value = (dist - 1) as u32;
    for i in 0..18 {
        let shift = 17 - i;
        let bit = ((value >> shift) & 1) != 0;
        encoder.encode_bit(&mut models.dist_bits[i], bit);
    }
}

fn decode_dist(
    decoder: &mut RangeDecoder<'_>,
    models: &mut Models,
) -> core::result::Result<usize, DecodeError> {
    let mut value = 0u32;
    for i in 0..18 {
        let bit = decoder.decode_bit(&mut models.dist_bits[i])?;
        value = (value << 1) | bit as u32;
    }
    Ok(value as usize + 1)
}

#[derive(Debug, Clone)]
struct RangeEncoder {
    low: u64,
    range: u32,
    cache_size: u64,
    cache: u8,
    out: Vec<u8>,
}

impl RangeEncoder {
    fn new() -> Self {
        Self {
            low: 0,
            range: u32::MAX,
            cache_size: 1,
            cache: 0,
            out: Vec::new(),
        }
    }

    fn encode_bit(&mut self, prob: &mut u16, bit: bool) {
        let bound = (self.range >> 11) * (*prob as u32);
        if !bit {
            self.range = bound;
            *prob = (*prob as u32 + ((BIT_MODEL_TOTAL as u32 - *prob as u32) >> MOVE_BITS)) as u16;
        } else {
            self.low += bound as u64;
            self.range -= bound;
            *prob = (*prob as u32 - ((*prob as u32) >> MOVE_BITS)) as u16;
        }

        while self.range < TOP_VALUE {
            self.range <<= 8;
            self.shift_low();
        }
    }

    fn shift_low(&mut self) {
        let low32 = self.low as u32;
        if low32 < 0xFF00_0000 || (self.low >> 32) != 0 {
            let carry = (self.low >> 32) as u8;
            let mut temp = self.cache;
            loop {
                self.out.push(temp.wrapping_add(carry));
                if self.cache_size == 1 {
                    break;
                }
                self.cache_size -= 1;
                temp = 0xFF;
            }
            self.cache = ((self.low >> 24) & 0xFF) as u8;
            self.cache_size = 0;
        }
        self.cache_size += 1;
        self.low = (self.low & 0x00FF_FFFF) << 8;
    }

    fn finish(mut self) -> Vec<u8> {
        for _ in 0..5 {
            self.shift_low();
        }
        self.out
    }
}

#[derive(Debug, Clone, Copy)]
struct RangeDecoder<'a> {
    code: u32,
    range: u32,
    input: &'a [u8],
    pos: usize,
}

impl<'a> RangeDecoder<'a> {
    fn new(input: &'a [u8]) -> core::result::Result<Self, DecodeError> {
        let mut decoder = Self {
            code: 0,
            range: u32::MAX,
            input,
            pos: 0,
        };
        for _ in 0..5 {
            decoder.code = (decoder.code << 8) | decoder.read_byte()? as u32;
        }
        Ok(decoder)
    }

    fn decode_bit(&mut self, prob: &mut u16) -> core::result::Result<bool, DecodeError> {
        let bound = (self.range >> 11) * (*prob as u32);
        let bit;
        if self.code < bound {
            self.range = bound;
            *prob = (*prob as u32 + ((BIT_MODEL_TOTAL as u32 - *prob as u32) >> MOVE_BITS)) as u16;
            bit = false;
        } else {
            self.code -= bound;
            self.range -= bound;
            *prob = (*prob as u32 - ((*prob as u32) >> MOVE_BITS)) as u16;
            bit = true;
        }

        while self.range < TOP_VALUE {
            self.range <<= 8;
            self.code = (self.code << 8) | self.read_byte()? as u32;
        }
        Ok(bit)
    }

    fn read_byte(&mut self) -> core::result::Result<u8, DecodeError> {
        let byte = *self.input.get(self.pos).ok_or(DecodeError::UnexpectedEof)?;
        self.pos += 1;
        Ok(byte)
    }

    fn finish(&self) -> core::result::Result<(), DecodeError> {
        if self.pos < self.input.len() && self.input.len() - self.pos > 5 {
            return Err(DecodeError::TrailingData);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodeError {
    UnexpectedEof,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidProperties(u8),
    InvalidDictionarySize(usize),
    PayloadSizeMismatch { expected: usize, actual: usize },
    InvalidMatchLength(usize),
    InvalidMatchDistance { distance: usize, produced: usize },
    OutputTooLarge { expected: usize, actual: usize },
    OutputSizeMismatch { expected: usize, actual: usize },
    TrailingData,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected end of input"),
            Self::InvalidMagic => f.write_str("invalid stream magic"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported stream version {version}"),
            Self::InvalidProperties(props) => {
                write!(f, "invalid properties byte {props:#04x}")
            }
            Self::InvalidDictionarySize(size) => write!(f, "invalid dictionary size {size}"),
            Self::PayloadSizeMismatch { expected, actual } => write!(
                f,
                "payload size mismatch (expected {expected}, actual {actual})"
            ),
            Self::InvalidMatchLength(len) => write!(f, "invalid match length {len}"),
            Self::InvalidMatchDistance { distance, produced } => write!(
                f,
                "invalid match distance {distance} for produced output {produced}"
            ),
            Self::OutputTooLarge { expected, actual } => write!(
                f,
                "decoded output exceeded expected size (expected {expected}, actual {actual})"
            ),
            Self::OutputSizeMismatch { expected, actual } => write!(
                f,
                "decoded size mismatch (expected {expected}, actual {actual})"
            ),
            Self::TrailingData => f.write_str("trailing payload data after decode"),
        }
    }
}

