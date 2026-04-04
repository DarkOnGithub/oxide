use std::io::Read;

use liblzma::{
    read::XzDecoder,
    stream::{Action, Check, Filters, LzmaOptions, PRESET_EXTREME, Status, Stream},
};

use super::scratch::LzmaScratch;
use crate::{OxideError, Result};

pub(crate) const LZMA_DEFAULT_LEVEL: i32 = 6;
const LZMA_MIN_LEVEL: i32 = 1;
const LZMA_MAX_LEVEL: i32 = 9;
const LZMA_MIN_DICT_SIZE: usize = 4 * 1024;
const LZMA_LEVEL_1_TO_6_DICT_SIZE: usize = 1024 * 1024;
const LZMA_LEVEL_7_TO_8_DICT_SIZE: usize = 2 * 1024 * 1024;
const LZMA_LEVEL_9_DICT_SIZE: usize = 4 * 1024 * 1024;

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

#[inline]
fn resolve_dictionary_size(level: Option<i32>, dictionary_size: Option<usize>) -> Result<u32> {
    let resolved = dictionary_size.unwrap_or_else(|| match level.unwrap_or(LZMA_DEFAULT_LEVEL) {
        1..=6 => LZMA_LEVEL_1_TO_6_DICT_SIZE,
        7..=8 => LZMA_LEVEL_7_TO_8_DICT_SIZE,
        9 => LZMA_LEVEL_9_DICT_SIZE,
        _ => LZMA_LEVEL_7_TO_8_DICT_SIZE,
    });

    if resolved < LZMA_MIN_DICT_SIZE {
        return Err(OxideError::CompressionError(format!(
            "invalid lzma dictionary size {resolved}: expected at least {LZMA_MIN_DICT_SIZE}"
        )));
    }

    u32::try_from(resolved).map_err(|_| {
        OxideError::CompressionError(format!(
            "invalid lzma dictionary size {resolved}: exceeds 32-bit limit"
        ))
    })
}

fn build_encoder_stream(
    level: Option<i32>,
    extreme: bool,
    dictionary_size: Option<usize>,
) -> Result<Stream> {
    let mut options = LzmaOptions::new_preset(resolve_level(level, extreme)?)
        .map_err(|err| OxideError::CompressionError(format!("lzma options init failed: {err}")))?;
    options.dict_size(resolve_dictionary_size(level, dictionary_size)?);

    let mut filters = Filters::new();
    filters.lzma2(&options);
    Stream::new_stream_encoder(&filters, Check::None)
        .map_err(|err| OxideError::CompressionError(format!("lzma encoder init failed: {err}")))
}

pub fn apply(data: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = LzmaScratch::default();
    apply_with_scratch(data, level, false, None, &mut scratch)
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    extreme: bool,
    dictionary_size: Option<usize>,
    scratch: &mut LzmaScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(data, level, extreme, dictionary_size, &mut output)?;
    Ok(output)
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    level: Option<i32>,
    extreme: bool,
    dictionary_size: Option<usize>,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    ensure_output_spare_capacity(output, 32 * 1024);
    let mut stream = build_encoder_stream(level, extreme, dictionary_size)?;

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
            ensure_output_spare_capacity(output, 32 * 1024);
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
