use core::cmp::min;
use core::fmt;
use core::mem::size_of;
use core::ptr;

use crate::{OxideError, Result};

const WINDOW_SIZE: usize = 32 * 1024;
const MIN_MATCH: usize = 3;
const MAX_MATCH: usize = 258;
const HASH_LOG: u32 = 15;
const HASH_SIZE: usize = 1 << HASH_LOG;
const MAX_CHAIN: usize = 16;
const LAZY_MATCH_THRESHOLD: usize = 12;

const MATCH_SYMBOL: usize = 256;
const END_SYMBOL: usize = 257;
const SYMBOLS: usize = 258;

const MAGIC: &[u8; 4] = b"ODF1";
const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Literal(u8),
    Match { len: usize, dist: usize },
    End,
}

/// Compresses data using a custom Deflate-like algorithm.
///
/// The output stream includes a 4-byte little-endian size prefix,
/// a magic header "ODF1", and a version byte before the bitstream.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "deflate encode failed: input exceeds 32-bit size prefix".to_string(),
        ));
    }

    let tokens = tokenize_lz77(data);
    let mut frequencies = [0u32; SYMBOLS];
    for token in &tokens {
        match *token {
            Token::Literal(byte) => frequencies[byte as usize] += 1,
            Token::Match { .. } => frequencies[MATCH_SYMBOL] += 1,
            Token::End => frequencies[END_SYMBOL] += 1,
        }
    }
    let lengths = build_code_lengths(&frequencies, 15);
    let codes = build_canonical_codes(&lengths);

    let mut out = Vec::with_capacity(data.len() / 2 + 320);
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend(lengths.iter().copied());

    let mut writer = BitWriter::new();
    for token in tokens {
        match token {
            Token::Literal(byte) => {
                write_symbol(&mut writer, byte as usize, &codes, &lengths);
            }
            Token::Match { len, dist } => {
                write_symbol(&mut writer, MATCH_SYMBOL, &codes, &lengths);
                writer.write_bits((len - MIN_MATCH) as u32, 8);
                writer.write_bits((dist - 1) as u32, 15);
            }
            Token::End => {
                write_symbol(&mut writer, END_SYMBOL, &codes, &lengths);
            }
        }
    }

    out.extend_from_slice(&writer.finish());
    Ok(out)
}

/// Decompresses data using the custom Deflate-like algorithm.
///
/// Expects the format produced by [`apply`].
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 4 {
        return Err(OxideError::DecompressionError(
            "deflate decode failed: missing 4-byte size prefix".to_string(),
        ));
    }

    let expected = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    decode_stream(&data[4..], expected)
        .map_err(|err| OxideError::DecompressionError(format!("deflate decode failed: {err}")))
}

fn decode_stream(input: &[u8], expected_size: usize) -> core::result::Result<Vec<u8>, DecodeError> {
    let table_bytes = 4 + 1 + SYMBOLS;
    if input.len() < table_bytes {
        return Err(DecodeError::UnexpectedEof);
    }
    if &input[..4] != MAGIC {
        return Err(DecodeError::InvalidMagic);
    }
    if input[4] != VERSION {
        return Err(DecodeError::UnsupportedVersion(input[4]));
    }

    let mut lengths = [0u8; SYMBOLS];
    lengths.copy_from_slice(&input[5..5 + SYMBOLS]);
    if lengths[END_SYMBOL] == 0 {
        return Err(DecodeError::InvalidHuffmanTable(
            "missing end-of-stream symbol".to_string(),
        ));
    }

    let decode_tree = DecodeTree::from_code_lengths(&lengths)?;
    let mut reader = BitReader::new(&input[table_bytes..]);
    let mut output = Vec::with_capacity(expected_size);

    loop {
        let symbol = decode_tree.decode_symbol(&mut reader)?;
        if symbol <= 255 {
            output.push(symbol as u8);
            if output.len() > expected_size {
                return Err(DecodeError::OutputTooLarge {
                    expected: expected_size,
                    actual: output.len(),
                });
            }
            continue;
        }

        if symbol == MATCH_SYMBOL {
            let len = reader.read_bits(8)? as usize + MIN_MATCH;
            if len > MAX_MATCH {
                return Err(DecodeError::InvalidMatchLength(len));
            }
            let dist = reader.read_bits(15)? as usize + 1;
            if dist == 0 || dist > min(WINDOW_SIZE, output.len()) {
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
                let byte = output[idx];
                output.push(byte);
            }
            continue;
        }

        if symbol == END_SYMBOL {
            break;
        }

        return Err(DecodeError::InvalidSymbol(symbol));
    }

    if output.len() != expected_size {
        return Err(DecodeError::SizeMismatch {
            expected: expected_size,
            actual: output.len(),
        });
    }

    reader.finish()?;
    Ok(output)
}

fn tokenize_lz77(input: &[u8]) -> Vec<Token> {
    if input.is_empty() {
        return vec![Token::End];
    }

    let mut tokens = Vec::with_capacity(input.len() / 2 + 4);
    let mut head = vec![-1_i32; HASH_SIZE];
    let mut prev = vec![-1_i32; input.len()];
    let mut pos = 0usize;

    while pos < input.len() {
        let best = if pos + MIN_MATCH <= input.len() {
            find_best_match(input, pos, &head, &prev)
        } else {
            None
        };

        if let Some(mat) = best {
            let next_better =
                if mat.len < LAZY_MATCH_THRESHOLD && pos + 1 + MIN_MATCH <= input.len() {
                    find_best_match(input, pos + 1, &head, &prev)
                        .map(|next| next.len > mat.len + 1)
                        .unwrap_or(false)
                } else {
                    false
                };

            if next_better {
                insert_position(input, pos, &mut head, &mut prev);
                tokens.push(Token::Literal(input[pos]));
                pos += 1;
                continue;
            }

            let end = pos + mat.len;
            for p in pos..end {
                insert_position(input, p, &mut head, &mut prev);
            }
            tokens.push(Token::Match {
                len: mat.len,
                dist: mat.dist,
            });
            pos = end;
            continue;
        }

        insert_position(input, pos, &mut head, &mut prev);
        tokens.push(Token::Literal(input[pos]));
        pos += 1;
    }

    tokens.push(Token::End);
    tokens
}

#[derive(Debug, Clone, Copy)]
struct Match {
    len: usize,
    dist: usize,
}

fn find_best_match(input: &[u8], pos: usize, head: &[i32], prev: &[i32]) -> Option<Match> {
    if pos + MIN_MATCH > input.len() {
        return None;
    }

    let hash = hash_at(input, pos);
    let mut candidate = head[hash];
    let mut best = Match { len: 0, dist: 0 };
    let max_len = min(MAX_MATCH, input.len() - pos);
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

        let len = match_len_fast(input, pos, cand, max_len);

        if len >= MIN_MATCH && (len > best.len || (len == best.len && dist < best.dist)) {
            best = Match { len, dist };
            if len == max_len {
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
fn match_len_fast(input: &[u8], left: usize, right: usize, max_len: usize) -> usize {
    let mut len = 0usize;
    let word = size_of::<usize>();
    let left_ptr = unsafe {
        // SAFETY: `left` is validated by callers to be in-bounds.
        input.as_ptr().add(left)
    };
    let right_ptr = unsafe {
        // SAFETY: `right` is validated by callers to be in-bounds.
        input.as_ptr().add(right)
    };

    while len + word <= max_len {
        let lhs = unsafe {
            // SAFETY: both pointers have at least `word` readable bytes here.
            ptr::read_unaligned(left_ptr.add(len) as *const usize)
        };
        let rhs = unsafe {
            // SAFETY: both pointers have at least `word` readable bytes here.
            ptr::read_unaligned(right_ptr.add(len) as *const usize)
        };
        if lhs == rhs {
            len += word;
            continue;
        }
        for i in 0..word {
            if input[left + len + i] != input[right + len + i] {
                return len + i;
            }
        }
    }

    while len < max_len && input[left + len] == input[right + len] {
        len += 1;
    }
    len
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
    ((seq.wrapping_mul(0x9E37_79B1) >> (32 - HASH_LOG)) as usize) & (HASH_SIZE - 1)
}

fn write_symbol(writer: &mut BitWriter, symbol: usize, codes: &[u32], lengths: &[u8]) {
    let len = lengths[symbol];
    debug_assert!(len > 0);
    writer.write_bits(codes[symbol], len);
}

fn build_code_lengths(frequencies: &[u32], max_bits: u8) -> [u8; SYMBOLS] {
    let mut lengths = [0u8; SYMBOLS];
    let mut active = Vec::new();
    for (symbol, &freq) in frequencies.iter().enumerate() {
        if freq > 0 {
            active.push(symbol);
        }
    }

    if active.is_empty() {
        lengths[END_SYMBOL] = 1;
        return lengths;
    }
    if active.len() == 1 {
        lengths[active[0]] = 1;
        if active[0] != END_SYMBOL {
            lengths[END_SYMBOL] = 1;
        }
        return lengths;
    }

    #[derive(Clone, Copy)]
    struct HeapEntry {
        freq: u64,
        order: usize,
        node: usize,
    }

    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.freq == other.freq && self.order == other.order && self.node == other.node
        }
    }
    impl Eq for HeapEntry {}
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> core::cmp::Ordering {
            other
                .freq
                .cmp(&self.freq)
                .then_with(|| other.order.cmp(&self.order))
        }
    }

    #[derive(Clone, Copy)]
    struct Node {
        left: Option<usize>,
        right: Option<usize>,
        symbol: Option<usize>,
    }

    use std::collections::BinaryHeap;
    let mut heap = BinaryHeap::new();
    let mut nodes = Vec::with_capacity(active.len() * 2);
    let mut next_order = 0usize;

    active.sort_unstable();
    for &symbol in &active {
        let node_idx = nodes.len();
        nodes.push(Node {
            left: None,
            right: None,
            symbol: Some(symbol),
        });
        heap.push(HeapEntry {
            freq: frequencies[symbol] as u64,
            order: next_order,
            node: node_idx,
        });
        next_order += 1;
    }

    while heap.len() > 1 {
        let a = heap.pop().expect("heap has item");
        let b = heap.pop().expect("heap has item");
        let node_idx = nodes.len();
        nodes.push(Node {
            left: Some(a.node),
            right: Some(b.node),
            symbol: None,
        });
        heap.push(HeapEntry {
            freq: a.freq + b.freq,
            order: next_order,
            node: node_idx,
        });
        next_order += 1;
    }

    let root = heap.pop().expect("non-empty heap").node;
    let mut stack = Vec::new();
    stack.push((root, 0u8));
    while let Some((idx, depth)) = stack.pop() {
        let node = nodes[idx];
        if let Some(symbol) = node.symbol {
            lengths[symbol] = depth.max(1);
        } else {
            if let Some(left) = node.left {
                stack.push((left, depth.saturating_add(1)));
            }
            if let Some(right) = node.right {
                stack.push((right, depth.saturating_add(1)));
            }
        }
    }

    if lengths.iter().any(|&len| len > max_bits) {
        let active_count = active.len().max(1);
        let uniform = ((usize::BITS - (active_count - 1).leading_zeros()) as u8).max(1);
        lengths.fill(0);
        for symbol in active {
            lengths[symbol] = uniform;
        }
    }

    if lengths[END_SYMBOL] == 0 {
        lengths[END_SYMBOL] = 1;
    }
    lengths
}

fn build_canonical_codes(lengths: &[u8]) -> [u32; SYMBOLS] {
    let mut max_bits = 0usize;
    for &len in lengths {
        max_bits = max_bits.max(len as usize);
    }

    let mut bl_count = vec![0u32; max_bits + 1];
    for &len in lengths {
        if len > 0 {
            bl_count[len as usize] += 1;
        }
    }

    let mut next_code = vec![0u32; max_bits + 1];
    let mut code = 0u32;
    for bits in 1..=max_bits {
        code = (code + bl_count[bits - 1]) << 1;
        next_code[bits] = code;
    }

    let mut codes = [0u32; SYMBOLS];
    for symbol in 0..SYMBOLS {
        let len = lengths[symbol] as usize;
        if len > 0 {
            codes[symbol] = next_code[len];
            next_code[len] += 1;
        }
    }
    codes
}

#[derive(Debug)]
struct DecodeTree {
    nodes: Vec<DecodeNode>,
}

#[derive(Debug, Clone, Copy)]
struct DecodeNode {
    left: Option<usize>,
    right: Option<usize>,
    symbol: Option<usize>,
}

impl DecodeTree {
    fn new() -> Self {
        Self {
            nodes: vec![DecodeNode {
                left: None,
                right: None,
                symbol: None,
            }],
        }
    }

    fn from_codes(codes: &[u32], lengths: &[u8]) -> core::result::Result<Self, DecodeError> {
        let mut tree = DecodeTree::new();
        for symbol in 0..SYMBOLS {
            let len = lengths[symbol];
            if len == 0 {
                continue;
            }
            tree.insert(symbol, codes[symbol], len)?;
        }
        Ok(tree)
    }

    fn from_code_lengths(lengths: &[u8]) -> core::result::Result<Self, DecodeError> {
        if lengths.iter().all(|&len| len == 9) {
            return Ok(Self::fixed_9bit());
        }
        let codes = build_canonical_codes(lengths);
        Self::from_codes(&codes, lengths)
    }

    fn fixed_9bit() -> Self {
        let mut tree = DecodeTree::new();
        for symbol in 0..SYMBOLS {
            tree.insert(symbol, symbol as u32, 9)
                .expect("fixed table is valid");
        }
        tree
    }

    fn insert(
        &mut self,
        symbol: usize,
        code: u32,
        bit_len: u8,
    ) -> core::result::Result<(), DecodeError> {
        let mut node_idx = 0usize;
        for i in (0..bit_len).rev() {
            let bit = (code >> i) & 1;
            let child = if bit == 0 {
                self.nodes[node_idx].left
            } else {
                self.nodes[node_idx].right
            };
            let next_idx = if let Some(existing) = child {
                existing
            } else {
                let created = self.nodes.len();
                self.nodes.push(DecodeNode {
                    left: None,
                    right: None,
                    symbol: None,
                });
                if bit == 0 {
                    self.nodes[node_idx].left = Some(created);
                } else {
                    self.nodes[node_idx].right = Some(created);
                }
                created
            };
            node_idx = next_idx;
        }
        if self.nodes[node_idx].symbol.is_some() {
            return Err(DecodeError::InvalidHuffmanTable(
                "symbol collision in decode tree".to_string(),
            ));
        }
        self.nodes[node_idx].symbol = Some(symbol);
        Ok(())
    }

    fn decode_symbol(
        &self,
        reader: &mut BitReader<'_>,
    ) -> core::result::Result<usize, DecodeError> {
        let mut idx = 0usize;
        loop {
            let node = self.nodes[idx];
            if let Some(symbol) = node.symbol {
                return Ok(symbol);
            }
            let bit = reader.read_bit()?;
            idx = if !bit {
                node.left.ok_or(DecodeError::InvalidCode)?
            } else {
                node.right.ok_or(DecodeError::InvalidCode)?
            };
        }
    }
}

#[derive(Debug, Clone)]
struct BitWriter {
    out: Vec<u8>,
    current: u8,
    used: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            out: Vec::new(),
            current: 0,
            used: 0,
        }
    }

    fn write_bits(&mut self, value: u32, bits: u8) {
        for i in (0..bits).rev() {
            let bit = ((value >> i) & 1) as u8;
            self.current = (self.current << 1) | bit;
            self.used += 1;
            if self.used == 8 {
                self.out.push(self.current);
                self.current = 0;
                self.used = 0;
            }
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            self.current <<= 8 - self.used;
            self.out.push(self.current);
        }
        self.out
    }
}

#[derive(Debug, Clone, Copy)]
struct BitReader<'a> {
    input: &'a [u8],
    pos: usize,
    current: u8,
    left: u8,
}

impl<'a> BitReader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            pos: 0,
            current: 0,
            left: 0,
        }
    }

    fn read_bit(&mut self) -> core::result::Result<bool, DecodeError> {
        if self.left == 0 {
            self.current = *self.input.get(self.pos).ok_or(DecodeError::UnexpectedEof)?;
            self.pos += 1;
            self.left = 8;
        }
        let bit = (self.current & 0x80) != 0;
        self.current <<= 1;
        self.left -= 1;
        Ok(bit)
    }

    fn read_bits(&mut self, bits: u8) -> core::result::Result<u32, DecodeError> {
        let mut value = 0u32;
        for _ in 0..bits {
            value = (value << 1) | self.read_bit()? as u32;
        }
        Ok(value)
    }

    fn finish(&self) -> core::result::Result<(), DecodeError> {
        if self.left > 0 && self.current != 0 {
            return Err(DecodeError::TrailingBits);
        }
        if self.pos != self.input.len() {
            return Err(DecodeError::TrailingBits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodeError {
    UnexpectedEof,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidHuffmanTable(String),
    InvalidCode,
    InvalidSymbol(usize),
    InvalidMatchLength(usize),
    InvalidMatchDistance { distance: usize, produced: usize },
    OutputTooLarge { expected: usize, actual: usize },
    SizeMismatch { expected: usize, actual: usize },
    TrailingBits,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => f.write_str("unexpected end of input"),
            Self::InvalidMagic => f.write_str("invalid stream magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported stream version {version}")
            }
            Self::InvalidHuffmanTable(reason) => {
                write!(f, "invalid huffman table: {reason}")
            }
            Self::InvalidCode => f.write_str("invalid huffman code in payload"),
            Self::InvalidSymbol(symbol) => write!(f, "invalid symbol {symbol}"),
            Self::InvalidMatchLength(len) => write!(f, "invalid match length {len}"),
            Self::InvalidMatchDistance { distance, produced } => write!(
                f,
                "invalid match distance {distance} for produced output {produced} bytes"
            ),
            Self::OutputTooLarge { expected, actual } => write!(
                f,
                "decoded output exceeded expected size (expected {expected}, actual {actual})"
            ),
            Self::SizeMismatch { expected, actual } => write!(
                f,
                "decoded size mismatch (expected {expected}, actual {actual})"
            ),
            Self::TrailingBits => f.write_str("trailing bits after end-of-stream marker"),
        }
    }
}
