use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

use crate::{OxideError, Result};

pub fn apply(data: &[u8]) -> Result<Vec<u8>> {
    Ok(compress_prepend_size(data))
}

pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    decompress_size_prepended(data)
        .map_err(|err| OxideError::DecompressionError(format!("lz4 decode failed: {err}")))
}
