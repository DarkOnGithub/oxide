use std::time::Instant;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{CompressionAlgo, CompressionPreset, Result};

pub mod lz4;
pub(crate) mod scratch;

pub(crate) use scratch::CompressionScratchArena;

/// Compression parameters for a single encode request.
#[derive(Debug, Clone, Copy)]
pub struct CompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub preset: CompressionPreset,
    pub dictionary: Option<&'a [u8]>,
}

impl<'a> CompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            preset: CompressionPreset::Default,
            dictionary: None,
        }
    }
}

/// Decompression parameters for a single decode request.
#[derive(Debug, Clone, Copy)]
pub struct DecompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub dictionary: Option<&'a [u8]>,
}

impl<'a> DecompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            dictionary: None,
        }
    }
}

/// Dispatches compression to the specified algorithm.
pub fn apply_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    apply_compression_request_with_scratch(CompressionRequest::new(data, algo), &mut scratch)
}

pub(crate) fn apply_compression_request_with_scratch(
    request: CompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
) -> Result<Vec<u8>> {
    let start = Instant::now();
    let input_bytes = request.data.len() as u64;

    let result = match request.algo {
        CompressionAlgo::Lz4 => lz4::apply_with_scratch(
            request.data,
            request.preset,
            request.dictionary,
            scratch.lz4(),
        ),
    };

    if let Ok(ref compressed) = result {
        let elapsed_us = duration_to_us(start.elapsed());
        let output_bytes = compressed.len() as u64;
        let algo_str = algo_label(request.algo);
        let preset_str = preset_label(request.preset);
        let dictionary = if request.dictionary.is_some() {
            "present"
        } else {
            "none"
        };

        let labels = [
            ("algo", algo_str),
            ("preset", preset_str),
            ("dictionary", dictionary),
        ];
        telemetry::increment_counter(tags::METRIC_COMPRESSION_APPLY_COUNT, 1, &labels);
        telemetry::record_histogram(
            tags::METRIC_COMPRESSION_APPLY_LATENCY_US,
            elapsed_us,
            &labels,
        );
        telemetry::record_histogram(tags::METRIC_COMPRESSION_INPUT_BYTES, input_bytes, &labels);
        telemetry::record_histogram(tags::METRIC_COMPRESSION_OUTPUT_BYTES, output_bytes, &labels);

        profile::event(
            tags::PROFILE_COMPRESSION,
            &[tags::TAG_COMPRESSION, algo_str, preset_str],
            "apply",
            "ok",
            elapsed_us,
            "compression applied",
        );
    }

    result
}

/// Dispatches decompression to the specified algorithm.
pub fn reverse_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    reverse_compression_request(DecompressionRequest::new(data, algo))
}

pub(crate) fn reverse_compression_request(request: DecompressionRequest<'_>) -> Result<Vec<u8>> {
    let start = Instant::now();
    let input_bytes = request.data.len() as u64;

    let result = match request.algo {
        CompressionAlgo::Lz4 => lz4::reverse_with_dictionary(request.data, request.dictionary),
    };

    if let Ok(ref decompressed) = result {
        let elapsed_us = duration_to_us(start.elapsed());
        let output_bytes = decompressed.len() as u64;
        let algo_str = algo_label(request.algo);
        let dictionary = if request.dictionary.is_some() {
            "present"
        } else {
            "none"
        };

        let labels = [("algo", algo_str), ("dictionary", dictionary)];
        telemetry::increment_counter(tags::METRIC_COMPRESSION_REVERSE_COUNT, 1, &labels);
        telemetry::record_histogram(
            tags::METRIC_COMPRESSION_REVERSE_LATENCY_US,
            elapsed_us,
            &labels,
        );
        telemetry::record_histogram(tags::METRIC_COMPRESSION_INPUT_BYTES, input_bytes, &labels);
        telemetry::record_histogram(tags::METRIC_COMPRESSION_OUTPUT_BYTES, output_bytes, &labels);

        profile::event(
            tags::PROFILE_COMPRESSION,
            &[tags::TAG_COMPRESSION, algo_str],
            "reverse",
            "ok",
            elapsed_us,
            "compression reversed",
        );
    }

    result
}

fn algo_label(algo: CompressionAlgo) -> &'static str {
    match algo {
        CompressionAlgo::Lz4 => "lz4",
    }
}

fn preset_label(preset: CompressionPreset) -> &'static str {
    match preset {
        CompressionPreset::Fast => "fast",
        CompressionPreset::Default => "default",
        CompressionPreset::High => "high",
    }
}
