use lz4_flex::block::{
    CompressError, compress_into, compress_prepend_size, get_maximum_output_size,
};

use crate::{OxideError, Result};

#[path = "lz4/copy.rs"]
mod copy;
#[path = "lz4/decode.rs"]
mod decode;

const MIN_MATCH: usize = 4;

#[derive(Debug, Default)]
pub(crate) struct Lz4Scratch {
    output: Vec<u8>,
}

impl Lz4Scratch {
    pub(crate) fn allocated_bytes(&self) -> usize {
        self.output.capacity()
    }

    pub(crate) fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    pub(crate) fn recycle_output(&mut self, mut output: Vec<u8>) {
        output.clear();
        self.output = output;
    }
}

/// Compresses data using the LZ4 algorithm.
pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(compress_prepend_size(data))
}

pub(crate) fn apply_with_scratch(data: &[u8], scratch: &mut Lz4Scratch) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, scratch, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    _scratch: &mut Lz4Scratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    if data.len() > u32::MAX as usize {
        return Err(OxideError::CompressionError(
            "lz4 input exceeds 32-bit size prefix".to_string(),
        ));
    }

    let required_capacity = 4 + get_maximum_output_size(data.len());
    output.clear();
    output.reserve(required_capacity);
    output.resize(required_capacity, 0);
    output[..4].copy_from_slice(&(data.len() as u32).to_le_bytes());

    match compress_into(data, &mut output[4..]) {
        Ok(written) => {
            output.truncate(4 + written);
            Ok(())
        }
        Err(err) => {
            output.clear();
            Err(map_compress_error(err))
        }
    }
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

#[cfg(test)]
#[path = "../../tests/compression/lz4.rs"]
mod tests;

fn map_compress_error(err: CompressError) -> OxideError {
    OxideError::CompressionError(format!("lz4 encode failed: {err}"))
}
