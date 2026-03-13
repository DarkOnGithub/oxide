use std::time::{Duration, Instant};

use crate::buffer::BufferPool;
use crate::compression::{CompressionRequest, apply_compression_request_with_scratch};
use crate::core::WorkerScratchArena;
use crate::pipeline::archive::types::ProcessingThroughputTotals;
use crate::types::{Batch, CompressedBlock, CompressionAlgo, Result};

pub fn process_batch(
    batch: Batch,
    _pool: &BufferPool,
    _compression: CompressionAlgo,
    raw_fallback_enabled: bool,
    processing_totals: &ProcessingThroughputTotals,
    scratch: &mut WorkerScratchArena,
) -> Result<CompressedBlock> {
    let source = batch.data();
    let preprocessing_elapsed = Duration::ZERO;
    let compression_input = source;
    let plan = batch.compression_plan;

    let compression_started = Instant::now();
    let compressed = apply_compression_request_with_scratch(
        CompressionRequest {
            data: compression_input,
            algo: plan.algo,
            preset: plan.preset,
            zstd_level: plan.zstd_level,
        },
        scratch.compression(),
    )?;
    let compression_elapsed = compression_started.elapsed();
    processing_totals.record(
        source.len() as u64,
        preprocessing_elapsed,
        compression_input.len() as u64,
        compression_elapsed,
    );
    let raw_passthrough = raw_fallback_enabled && compressed.len() >= compression_input.len();
    let data = if raw_passthrough {
        compression_input.to_vec()
    } else {
        compressed
    };

    Ok(CompressedBlock::with_chunk_encoding(
        batch.id,
        batch.stream_id,
        data,
        crate::PreProcessingStrategy::None,
        plan,
        raw_passthrough,
        batch.len() as u64,
    ))
}

#[inline]
pub fn select_stored_payload<'a>(
    source: &'a [u8],
    compressed: &'a [u8],
    raw_fallback_enabled: bool,
) -> (&'a [u8], bool) {
    let raw_passthrough = raw_fallback_enabled && compressed.len() >= source.len();
    let payload = if raw_passthrough { source } else { compressed };
    (payload, raw_passthrough)
}
