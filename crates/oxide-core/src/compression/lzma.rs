use std::io::Read;

use liblzma::{
    read::XzDecoder,
    stream::{Action, Check, PRESET_EXTREME, Status, Stream},
};

use super::scratch::LzmaScratch;
use crate::{OxideError, Result};

pub(crate) const LZMA_DEFAULT_LEVEL: i32 = 6;
const LZMA_MIN_LEVEL: i32 = 1;
const LZMA_MAX_LEVEL: i32 = 9;

#[inline]
fn resolve_level(level: Option<i32>, extreme: bool) -> Result<u32> {
    let level = level.unwrap_or(LZMA_DEFAULT_LEVEL);
    if !(LZMA_MIN_LEVEL..=LZMA_MAX_LEVEL).contains(&level) {
        return Err(OxideError::CompressionError(format!(
            "invalid lzma level {level}: expected {LZMA_MIN_LEVEL}..={LZMA_MAX_LEVEL}"
        )));
    }

    let level = if extreme {
        (level as u32) | PRESET_EXTREME
    } else {
        level as u32
    };
    Ok(level)
}

pub fn apply(data: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = LzmaScratch::default();
    apply_with_scratch(data, level, false, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    extreme: bool,
    scratch: &mut LzmaScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, level, extreme, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    level: Option<i32>,
    extreme: bool,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    ensure_output_spare_capacity(output, data.len().min(32 * 1024).max(32 * 1024));
    let mut stream = Stream::new_easy_encoder(resolve_level(level, extreme)?, Check::Crc64)
        .map_err(|err| OxideError::CompressionError(format!("lzma encoder init failed: {err}")))?;

    let mut remaining = data;
    while !remaining.is_empty() {
        let total_in = stream.total_in();
        stream
            .process_vec(remaining, output, Action::Run)
            .map_err(|err| OxideError::CompressionError(format!("lzma encode failed: {err}")))?;

        let consumed = (stream.total_in() - total_in) as usize;
        if consumed == 0 {
            ensure_output_spare_capacity(output, output.capacity().max(32 * 1024));
            continue;
        }

        remaining = &remaining[consumed..];
        if !remaining.is_empty() {
            ensure_output_spare_capacity(output, remaining.len().min(32 * 1024).max(32 * 1024));
        }
    }

    loop {
        let total_out = stream.total_out();
        let status = stream
            .process_vec(&[], output, Action::Finish)
            .map_err(|err| OxideError::CompressionError(format!("lzma encode failed: {err}")))?;
        if status == Status::StreamEnd {
            break;
        }
        if stream.total_out() == total_out {
            ensure_output_spare_capacity(output, output.capacity().max(32 * 1024));
        }
    }

    Ok(())
}

#[inline]
fn ensure_output_spare_capacity(output: &mut Vec<u8>, additional: usize) {
    let spare = output.spare_capacity_mut().len();
    if spare < additional {
        output.reserve(additional - spare);
    }
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut LzmaScratch) {
    scratch.recycle_output(output);
}

pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    let mut scratch = LzmaScratch::default();
    reverse_with_scratch(data, &mut scratch)
}

pub(crate) fn reverse_with_scratch(data: &[u8], scratch: &mut LzmaScratch) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    reverse_into_vec(data, &mut output)?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(data: &[u8], output: &mut Vec<u8>) -> Result<()> {
    output.clear();
    let mut decoder = XzDecoder::new(data);
    decoder
        .read_to_end(output)
        .map_err(|err| OxideError::DecompressionError(format!("lzma decode failed: {err}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PRESET_EXTREME, resolve_level};

    #[test]
    fn plain_level_nine_does_not_enable_extreme() {
        assert_eq!(resolve_level(Some(9), false).expect("resolve level"), 9);
    }

    #[test]
    fn explicit_extreme_sets_extreme_bit() {
        assert_eq!(
            resolve_level(Some(9), true).expect("resolve level"),
            9 | PRESET_EXTREME
        );
    }
}
