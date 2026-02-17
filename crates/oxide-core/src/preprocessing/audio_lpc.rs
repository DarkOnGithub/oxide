use crate::preprocessing::utils;
use crate::{OxideError, Result};

const LPC_MAGIC: &[u8; 4] = b"OLP1";
const HEADER_SIZE: usize = 4 + 4 + 2 + 1 + 1 + 1 + 1;
const MIN_INPUT_SIZE: usize = 64;

/// Converts raw audio bytes into a sequence of LPC-ready sample values.
pub fn bytes_to_data(data: &[u8], metadata: &utils::AudioMetadata) -> Result<Vec<i16>> {
    utils::bytes_to_i16_samples(data, metadata)
}

/// Applies a minimal per-channel delta predictor on sample words.
pub fn apply(data: &[u8], metadata: Option<&utils::AudioMetadata>) -> Result<Vec<u8>> {
    let Some(metadata) = metadata else {
        return Ok(data.to_vec());
    };

    let channels = metadata.channels;
    let bytes_per_sample = metadata.bytes_per_sample;
    if channels == 0 {
        return Err(OxideError::InvalidFormat("audio channels must be non-zero"));
    }
    if bytes_per_sample == 0 {
        return Err(OxideError::InvalidFormat(
            "audio bytes_per_sample must be non-zero",
        ));
    }
    if bytes_per_sample > 8 || data.len() < MIN_INPUT_SIZE {
        return Ok(data.to_vec());
    }

    let original_len = u32::try_from(data.len())
        .map_err(|_| OxideError::InvalidFormat("audio payload too large for LPC header"))?;
    let channels_u16 = u16::try_from(channels)
        .map_err(|_| OxideError::InvalidFormat("audio channel count too large for LPC header"))?;

    let full_samples = data.len() / bytes_per_sample;
    if full_samples == 0 {
        return Ok(data.to_vec());
    }

    let mut transformed = data.to_vec();
    let mut previous = vec![0u64; channels];
    let mask = sample_mask(bytes_per_sample);
    let mut zero_before = 0usize;
    let mut zero_after = 0usize;

    for sample_idx in 0..full_samples {
        let start = sample_idx * bytes_per_sample;
        let end = start + bytes_per_sample;
        let channel = sample_idx % channels;

        let sample = read_sample_word(&data[start..end], metadata.endian);
        let residual = sample.wrapping_sub(previous[channel]) & mask;
        previous[channel] = sample;
        write_sample_word(&mut transformed[start..end], residual, metadata.endian);

        if sample == 0 {
            zero_before += 1;
        }
        if residual == 0 {
            zero_after += 1;
        }
    }

    if zero_after <= zero_before.saturating_add(channels) {
        return Ok(data.to_vec());
    }

    let mut output = Vec::with_capacity(HEADER_SIZE + transformed.len());
    output.extend_from_slice(LPC_MAGIC);
    output.extend_from_slice(&original_len.to_le_bytes());
    output.extend_from_slice(&channels_u16.to_le_bytes());
    output.push(bytes_per_sample as u8);
    output.push(match metadata.endian {
        utils::AudioEndian::Little => 0,
        utils::AudioEndian::Big => 1,
    });
    output.push(match metadata.encoding {
        utils::AudioSampleEncoding::SignedPcm => 0,
        utils::AudioSampleEncoding::UnsignedPcm => 1,
        utils::AudioSampleEncoding::Float => 2,
    });
    output.push(0);
    output.extend_from_slice(&transformed);
    Ok(output)
}

/// Reverses the per-channel delta predictor.
///
/// Payloads without the transform marker are returned unchanged.
pub fn reverse(data: &[u8]) -> Result<Vec<u8>> {
    if !data.starts_with(LPC_MAGIC) {
        return Ok(data.to_vec());
    }

    if data.len() < HEADER_SIZE {
        return Err(OxideError::InvalidFormat("LPC data too short for header"));
    }

    let original_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let channels = u16::from_le_bytes([data[8], data[9]]) as usize;
    let bytes_per_sample = data[10] as usize;
    let endian = match data[11] {
        0 => utils::AudioEndian::Little,
        1 => utils::AudioEndian::Big,
        _ => {
            return Err(OxideError::InvalidFormat(
                "LPC header contains invalid endian tag",
            ));
        }
    };

    if channels == 0 {
        return Err(OxideError::InvalidFormat("LPC header has zero channels"));
    }
    if bytes_per_sample == 0 || bytes_per_sample > 8 {
        return Err(OxideError::InvalidFormat(
            "LPC header has unsupported bytes_per_sample",
        ));
    }

    let mut output = data[HEADER_SIZE..].to_vec();
    if output.len() != original_len {
        return Err(OxideError::InvalidFormat("LPC payload size mismatch"));
    }

    let full_samples = output.len() / bytes_per_sample;
    let mut previous = vec![0u64; channels];
    let mask = sample_mask(bytes_per_sample);

    for sample_idx in 0..full_samples {
        let start = sample_idx * bytes_per_sample;
        let end = start + bytes_per_sample;
        let channel = sample_idx % channels;

        let residual = read_sample_word(&output[start..end], endian);
        let sample = residual.wrapping_add(previous[channel]) & mask;
        previous[channel] = sample;
        write_sample_word(&mut output[start..end], sample, endian);
    }

    Ok(output)
}

#[inline]
fn sample_mask(bytes_per_sample: usize) -> u64 {
    if bytes_per_sample >= 8 {
        u64::MAX
    } else {
        (1u64 << (bytes_per_sample * 8)) - 1
    }
}

fn read_sample_word(sample: &[u8], endian: utils::AudioEndian) -> u64 {
    let mut value = 0u64;
    match endian {
        utils::AudioEndian::Little => {
            for (shift, byte) in sample.iter().enumerate() {
                value |= u64::from(*byte) << (shift * 8);
            }
        }
        utils::AudioEndian::Big => {
            for &byte in sample {
                value = (value << 8) | u64::from(byte);
            }
        }
    }
    value
}

fn write_sample_word(out: &mut [u8], value: u64, endian: utils::AudioEndian) {
    match endian {
        utils::AudioEndian::Little => {
            for (shift, byte) in out.iter_mut().enumerate() {
                *byte = (value >> (shift * 8)) as u8;
            }
        }
        utils::AudioEndian::Big => {
            let len = out.len();
            for (idx, byte) in out.iter_mut().enumerate() {
                let shift = (len - 1 - idx) * 8;
                *byte = (value >> shift) as u8;
            }
        }
    }
}
