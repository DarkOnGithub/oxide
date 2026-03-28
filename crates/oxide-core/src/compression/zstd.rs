use std::io::Cursor;

use super::scratch::ZstdScratch;
use crate::{OxideError, Result, ZstdCompressionParameters, ZstdStrategy};

pub(crate) const ZSTD_DEFAULT_LEVEL: i32 = 3;
const ZSTD_MIN_LEVEL: i32 = 1;
const ZSTD_MAX_LEVEL: i32 = 22;

#[inline]
fn resolve_level(level: Option<i32>) -> Result<i32> {
    let level = level.unwrap_or(ZSTD_DEFAULT_LEVEL);
    if !(ZSTD_MIN_LEVEL..=ZSTD_MAX_LEVEL).contains(&level) {
        return Err(OxideError::CompressionError(format!(
            "invalid zstd level {level}: expected {ZSTD_MIN_LEVEL}..={ZSTD_MAX_LEVEL}"
        )));
    }

    Ok(level)
}

pub fn apply(data: &[u8], level: Option<i32>) -> Result<Vec<u8>> {
    let mut scratch = ZstdScratch::default();
    apply_with_scratch(
        data,
        level,
        ZstdCompressionParameters::default(),
        0,
        None,
        &mut scratch,
    )
}

pub(crate) fn apply_with_scratch(
    data: &[u8],
    level: Option<i32>,
    parameters: ZstdCompressionParameters,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    apply_into_vec(
        data,
        level,
        parameters,
        dictionary_id,
        dictionary,
        scratch,
        &mut output,
    )?;
    Ok(output)
}

pub(crate) fn build_compressor(
    level: i32,
    parameters: ZstdCompressionParameters,
    dictionary: &[u8],
) -> Result<zstd::bulk::Compressor<'static>> {
    let mut compressor =
        zstd::bulk::Compressor::with_dictionary(level, dictionary).map_err(|err| {
            OxideError::CompressionError(format!("zstd compressor init failed: {err}"))
        })?;
    apply_parameters(&mut compressor, parameters)?;
    Ok(compressor)
}

fn apply_parameters(
    compressor: &mut zstd::bulk::Compressor<'static>,
    parameters: ZstdCompressionParameters,
) -> Result<()> {
    use zstd::zstd_safe::{CParameter, Strategy};

    if let Some(window_log) = parameters.window_log {
        compressor
            .set_parameter(CParameter::WindowLog(window_log))
            .map_err(|err| OxideError::CompressionError(format!("zstd WindowLog failed: {err}")))?;
    }
    if let Some(strategy) = parameters.strategy {
        compressor
            .set_parameter(CParameter::Strategy(match strategy {
                ZstdStrategy::Fast => Strategy::ZSTD_fast,
                ZstdStrategy::Dfast => Strategy::ZSTD_dfast,
                ZstdStrategy::Greedy => Strategy::ZSTD_greedy,
                ZstdStrategy::Lazy => Strategy::ZSTD_lazy,
                ZstdStrategy::Lazy2 => Strategy::ZSTD_lazy2,
                ZstdStrategy::Btlazy2 => Strategy::ZSTD_btlazy2,
                ZstdStrategy::Btopt => Strategy::ZSTD_btopt,
                ZstdStrategy::Btultra => Strategy::ZSTD_btultra,
                ZstdStrategy::Btultra2 => Strategy::ZSTD_btultra2,
            }))
            .map_err(|err| OxideError::CompressionError(format!("zstd Strategy failed: {err}")))?;
    }
    if let Some(enabled) = parameters.enable_long_distance_matching {
        compressor
            .set_parameter(CParameter::EnableLongDistanceMatching(enabled))
            .map_err(|err| {
                OxideError::CompressionError(format!(
                    "zstd EnableLongDistanceMatching failed: {err}"
                ))
            })?;
    }
    if let Some(value) = parameters.ldm_hash_log {
        compressor
            .set_parameter(CParameter::LdmHashLog(value))
            .map_err(|err| {
                OxideError::CompressionError(format!("zstd LdmHashLog failed: {err}"))
            })?;
    }
    if let Some(value) = parameters.ldm_min_match {
        compressor
            .set_parameter(CParameter::LdmMinMatch(value))
            .map_err(|err| {
                OxideError::CompressionError(format!("zstd LdmMinMatch failed: {err}"))
            })?;
    }
    if let Some(value) = parameters.ldm_bucket_size_log {
        compressor
            .set_parameter(CParameter::LdmBucketSizeLog(value))
            .map_err(|err| {
                OxideError::CompressionError(format!("zstd LdmBucketSizeLog failed: {err}"))
            })?;
    }
    if let Some(value) = parameters.ldm_hash_rate_log {
        compressor
            .set_parameter(CParameter::LdmHashRateLog(value))
            .map_err(|err| {
                OxideError::CompressionError(format!("zstd LdmHashRateLog failed: {err}"))
            })?;
    }
    if let Some(value) = parameters.job_size {
        compressor
            .set_parameter(CParameter::JobSize(value))
            .map_err(|err| OxideError::CompressionError(format!("zstd JobSize failed: {err}")))?;
    }
    if let Some(value) = parameters.overlap_log {
        compressor
            .set_parameter(CParameter::OverlapSizeLog(value))
            .map_err(|err| {
                OxideError::CompressionError(format!("zstd OverlapSizeLog failed: {err}"))
            })?;
    }

    Ok(())
}

pub(crate) fn apply_into_vec(
    data: &[u8],
    level: Option<i32>,
    parameters: ZstdCompressionParameters,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let required_capacity = zstd::zstd_safe::compress_bound(data.len());
    if output.capacity() < required_capacity {
        output.reserve(required_capacity - output.len());
    }

    scratch
        .compressor(resolve_level(level)?, parameters, dictionary_id, dictionary)?
        .compress_to_buffer(data, output)
        .map_err(|err| OxideError::CompressionError(format!("zstd encode failed: {err}")))?;

    Ok(())
}

pub(crate) fn recycle_output(output: Vec<u8>, scratch: &mut ZstdScratch) {
    scratch.recycle_output(output);
}

pub fn reverse(data: &[u8], raw_len: Option<usize>) -> Result<Vec<u8>> {
    let mut scratch = ZstdScratch::default();
    reverse_with_scratch(data, raw_len, 0, None, &mut scratch)
}

pub(crate) fn reverse_with_scratch(
    data: &[u8],
    raw_len: Option<usize>,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
) -> Result<Vec<u8>> {
    let mut output = scratch.take_output();
    reverse_into_vec(
        data,
        raw_len,
        dictionary_id,
        dictionary,
        scratch,
        &mut output,
    )?;
    Ok(output)
}

pub(crate) fn reverse_into_vec(
    data: &[u8],
    raw_len: Option<usize>,
    dictionary_id: u8,
    dictionary: Option<&[u8]>,
    scratch: &mut ZstdScratch,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();

    match raw_len {
        Some(raw_len) => {
            if output.capacity() < raw_len {
                output.reserve(raw_len - output.len());
            }

            scratch
                .decompressor(dictionary_id, dictionary)?
                .decompress_to_buffer(data, output)
                .map_err(|err| {
                    OxideError::DecompressionError(format!("zstd decode failed: {err}"))
                })?;
            Ok(())
        }
        None => {
            let decoded = zstd::stream::decode_all(Cursor::new(data)).map_err(|err| {
                OxideError::DecompressionError(format!("zstd decode failed: {err}"))
            })?;
            output.extend_from_slice(&decoded);
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "../../tests/compression/zstd.rs"]
mod tests;
