use std::time::Instant;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{CompressionAlgo, Result};

pub mod lz4;
pub(crate) mod scratch;

pub(crate) use scratch::CompressionScratchArena;

/// Dispatches compression to the specified algorithm.
pub fn apply_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    apply_compression_with_scratch(data, algo, &mut scratch)
}

pub(crate) fn apply_compression_with_scratch(
    data: &[u8],
    _algo: CompressionAlgo,
    scratch: &mut CompressionScratchArena,
) -> Result<Vec<u8>> {
    let start = Instant::now();
    let input_bytes = data.len() as u64;

    let result = lz4::apply_with_scratch(data, scratch.lz4());

    if let Ok(ref compressed) = result {
        let elapsed_us = duration_to_us(start.elapsed());
        let output_bytes = compressed.len() as u64;
        let algo_str = "lz4";

        let labels = [("algo", algo_str)];
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
            &[tags::TAG_COMPRESSION, algo_str],
            "apply",
            "ok",
            elapsed_us,
            "compression applied",
        );
    }

    result
}

/// Dispatches decompression to the specified algorithm.
pub fn reverse_compression(data: &[u8], _algo: CompressionAlgo) -> Result<Vec<u8>> {
    let start = Instant::now();
    let input_bytes = data.len() as u64;

    let result = lz4::reverse(data);

    if let Ok(ref decompressed) = result {
        let elapsed_us = duration_to_us(start.elapsed());
        let output_bytes = decompressed.len() as u64;
        let algo_str = "lz4";

        let labels = [("algo", algo_str)];
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
