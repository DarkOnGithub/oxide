use core::fmt;
use core::ptr;

use super::MIN_MATCH;
use super::copy::{self, CopyKernel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecodeError {
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

#[derive(Debug, Clone, Copy)]
struct TokenEntry {
    literal_base: u8,
    match_base: u8,
    literal_extends: bool,
    match_extends: bool,
}

impl TokenEntry {
    const fn new(
        literal_base: u8,
        match_base: u8,
        literal_extends: bool,
        match_extends: bool,
    ) -> Self {
        Self {
            literal_base,
            match_base,
            literal_extends,
            match_extends,
        }
    }
}

const TOKEN_TABLE: [TokenEntry; 256] = build_token_table();

const fn build_token_table() -> [TokenEntry; 256] {
    let mut table = [TokenEntry::new(0, 0, false, false); 256];
    let mut token = 0usize;

    while token < table.len() {
        let literal = ((token >> 4) & 0x0F) as u8;
        let match_len = ((token & 0x0F) as u8) + (MIN_MATCH as u8);
        let match_extends = (token & 0x0F) == 0x0F;
        table[token] = TokenEntry::new(literal, match_len, literal == 0x0F, match_extends);
        token += 1;
    }

    table
}

#[derive(Debug, Default)]
struct DecodeStats;

impl DecodeStats {
    #[inline]
    fn record_token(&mut self) {}

    #[inline]
    fn record_length_extension(&mut self) {}

    #[inline]
    fn record_literals(&mut self, _len: usize) {}

    #[inline]
    fn record_match(&mut self, _len: usize, _kernel: CopyKernel) {}
}

pub(super) fn decompress_block(
    input: &[u8],
    expected_size: usize,
) -> core::result::Result<Vec<u8>, DecodeError> {
    let mut output = Vec::new();
    decompress_block_into(input, expected_size, &mut output)?;
    Ok(output)
}

pub(super) fn decompress_block_into(
    input: &[u8],
    expected_size: usize,
    output: &mut Vec<u8>,
) -> core::result::Result<(), DecodeError> {
    if input.is_empty() {
        return Err(DecodeError::ExpectedAnotherByte);
    }

    output.clear();
    output.resize(expected_size, 0);

    let result = {
        let mut decoder = BlockDecoder::new(input, output.as_mut_slice());
        decoder
            .decode()
            .and_then(|()| decoder.ensure_output_complete())
    };

    if result.is_err() {
        output.clear();
    }

    result
}

struct BlockDecoder<'a> {
    input: &'a [u8],
    output: &'a mut [u8],
    input_pos: usize,
    output_pos: usize,
    stats: DecodeStats,
}

impl<'a> BlockDecoder<'a> {
    fn new(input: &'a [u8], output: &'a mut [u8]) -> Self {
        Self {
            input,
            output,
            input_pos: 0,
            output_pos: 0,
            stats: DecodeStats,
        }
    }

    fn decode(&mut self) -> core::result::Result<(), DecodeError> {
        while self.input_pos < self.input.len() {
            let token = self.read_byte()?;
            let entry = TOKEN_TABLE[token as usize];
            self.stats.record_token();

            let literal_len =
                self.resolve_length(entry.literal_base as usize, entry.literal_extends)?;
            self.copy_literals(literal_len)?;

            if self.input_pos == self.input.len() {
                break;
            }

            let offset = self.read_offset()?;
            let match_len = self.resolve_length(entry.match_base as usize, entry.match_extends)?;
            self.copy_match(offset, match_len)?;
        }

        self.ensure_output_complete()
    }

    fn ensure_output_complete(&self) -> core::result::Result<(), DecodeError> {
        if self.output_pos == self.output.len() {
            Ok(())
        } else {
            Err(DecodeError::DecodedSizeMismatch {
                expected: self.output.len(),
                actual: self.output_pos,
            })
        }
    }

    #[inline]
    fn read_byte(&mut self) -> core::result::Result<u8, DecodeError> {
        let byte = *self
            .input
            .get(self.input_pos)
            .ok_or(DecodeError::ExpectedAnotherByte)?;
        self.input_pos += 1;
        Ok(byte)
    }

    #[inline]
    fn read_offset(&mut self) -> core::result::Result<usize, DecodeError> {
        let end = self
            .input_pos
            .checked_add(2)
            .ok_or(DecodeError::LengthOverflow)?;
        let bytes = self
            .input
            .get(self.input_pos..end)
            .ok_or(DecodeError::ExpectedAnotherByte)?;
        self.input_pos += 2;

        let offset = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        if offset == 0 || offset > self.output_pos {
            return Err(DecodeError::OffsetOutOfBounds);
        }

        Ok(offset)
    }

    #[inline]
    fn resolve_length(
        &mut self,
        base: usize,
        extends: bool,
    ) -> core::result::Result<usize, DecodeError> {
        if !extends {
            return Ok(base);
        }

        self.stats.record_length_extension();
        base.checked_add(self.read_length()?)
            .ok_or(DecodeError::LengthOverflow)
    }

    fn read_length(&mut self) -> core::result::Result<usize, DecodeError> {
        let mut len = 0usize;

        loop {
            let byte = self.read_byte()?;
            len = len
                .checked_add(byte as usize)
                .ok_or(DecodeError::LengthOverflow)?;

            if byte != u8::MAX {
                return Ok(len);
            }
        }
    }

    fn copy_literals(&mut self, literal_len: usize) -> core::result::Result<(), DecodeError> {
        let input_end = self
            .input_pos
            .checked_add(literal_len)
            .ok_or(DecodeError::LengthOverflow)?;
        if input_end > self.input.len() {
            return Err(DecodeError::LiteralOutOfBounds);
        }

        let output_end = self
            .output_pos
            .checked_add(literal_len)
            .ok_or(DecodeError::LengthOverflow)?;
        if output_end > self.output.len() {
            return Err(DecodeError::OutputTooSmall {
                expected: output_end,
                actual: self.output.len(),
            });
        }

        if literal_len > 0 {
            unsafe {
                // SAFETY: source and destination were bounds-checked and do not overlap.
                ptr::copy_nonoverlapping(
                    self.input.as_ptr().add(self.input_pos),
                    self.output.as_mut_ptr().add(self.output_pos),
                    literal_len,
                );
            }
            self.stats.record_literals(literal_len);
        }

        self.input_pos = input_end;
        self.output_pos = output_end;
        Ok(())
    }

    fn copy_match(
        &mut self,
        offset: usize,
        match_len: usize,
    ) -> core::result::Result<(), DecodeError> {
        let output_end = self
            .output_pos
            .checked_add(match_len)
            .ok_or(DecodeError::LengthOverflow)?;
        if output_end > self.output.len() {
            return Err(DecodeError::OutputTooSmall {
                expected: output_end,
                actual: self.output.len(),
            });
        }

        let kernel = unsafe {
            // SAFETY: offset was validated against output_pos and output_end is in bounds.
            copy::copy_match(
                self.output.as_mut_ptr().add(self.output_pos),
                offset,
                match_len,
            )
        };
        self.output_pos = output_end;
        self.stats.record_match(match_len, kernel);
        Ok(())
    }
}
