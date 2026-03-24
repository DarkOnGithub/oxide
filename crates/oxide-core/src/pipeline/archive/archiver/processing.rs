use std::time::Instant;

use crate::buffer::BufferPool;
use crate::compression::{
    apply_compression_request_with_scratch, apply_compression_request_with_scratch_into,
    recycle_compression_buffer, supports_direct_buffer_output, CompressionRequest,
};
use crate::core::WorkerScratchArena;
use crate::pipeline::archive::types::ProcessingThroughputTotals;
use crate::types::{Batch, CompressedBlock, CompressedPayload, CompressionAlgo, Result};

const INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN: usize = 32 * 1024;
const INCOMPRESSIBLE_PROBE_SAMPLE_LEN: usize = 16 * 1024;

#[inline]
fn should_skip_full_compression_probe(source_len: usize) -> bool {
    source_len >= INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN
}

fn is_likely_incompressible_sample(
    source: &[u8],
    plan: crate::types::ChunkEncodingPlan,
    scratch: &mut WorkerScratchArena,
) -> Result<bool> {
    let sample_len = source.len().min(INCOMPRESSIBLE_PROBE_SAMPLE_LEN);
    if sample_len == 0 {
        return Ok(false);
    }

    let probe = apply_compression_request_with_scratch(
        CompressionRequest {
            data: &source[..sample_len],
            algo: plan.algo,
            level: plan.level,
        },
        scratch.compression(),
    )?;

    let likely_incompressible = probe.len() >= sample_len;
    recycle_compression_buffer(plan.algo, probe, scratch.compression());

    Ok(likely_incompressible)
}

pub fn process_batch(
    batch: Batch,
    pool: &BufferPool,
    _compression: CompressionAlgo,
    skip_compression: bool,
    raw_fallback_enabled: bool,
    processing_totals: &ProcessingThroughputTotals,
    scratch: &mut WorkerScratchArena,
) -> Result<CompressedBlock> {
    let Batch {
        id,
        data,
        stream_id,
        compression_plan: plan,
        force_raw_storage,
        ..
    } = batch;
    let source_len = data.len();
    let source = data.as_slice();
    let request = CompressionRequest {
        data: source,
        algo: plan.algo,
        level: plan.level,
    };

    if force_raw_storage {
        processing_totals.record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data_in_pool(data, pool),
            plan,
            true,
            source_len as u64,
        ));
    }

    if raw_fallback_enabled
        && !skip_compression
        && should_skip_full_compression_probe(source_len)
        && is_likely_incompressible_sample(source, plan, scratch)?
    {
        processing_totals.record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data_in_pool(data, pool),
            plan,
            true,
            source_len as u64,
        ));
    }

    if !skip_compression && supports_direct_buffer_output(plan.algo) {
        let compression_started = Instant::now();
        let mut compressed = pool.acquire();
        apply_compression_request_with_scratch_into(
            request,
            scratch.compression(),
            compressed.as_mut_vec(),
        )?;
        let compression_elapsed = compression_started.elapsed();
        processing_totals.record(source_len as u64, compression_elapsed);

        let raw_passthrough = raw_fallback_enabled && compressed.len() >= source_len;
        let data = if raw_passthrough {
            CompressedPayload::from_batch_data_in_pool(data, pool)
        } else {
            CompressedPayload::from(compressed)
        };

        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            data,
            plan,
            raw_passthrough,
            source_len as u64,
        ));
    }

    let (compressed, compression_elapsed) = if skip_compression {
        (source.to_vec(), std::time::Duration::ZERO)
    } else {
        let compression_started = Instant::now();
        let compressed = apply_compression_request_with_scratch(request, scratch.compression())?;
        (compressed, compression_started.elapsed())
    };
    processing_totals.record(source_len as u64, compression_elapsed);

    let raw_passthrough =
        skip_compression || (raw_fallback_enabled && compressed.len() >= source_len);
    let data = if raw_passthrough {
        CompressedPayload::from_batch_data_in_pool(data, pool)
    } else {
        CompressedPayload::from_vec_in_pool(compressed, pool)
    };

    Ok(CompressedBlock::with_chunk_encoding(
        id,
        stream_id,
        data,
        plan,
        raw_passthrough,
        source_len as u64,
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        is_likely_incompressible_sample, process_batch, INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN,
    };
    use crate::buffer::BufferPool;
    use crate::core::WorkerScratchArena;
    use crate::pipeline::archive::types::ProcessingThroughputTotals;
    use crate::types::{Batch, ChunkEncodingPlan, CompressionAlgo};

    #[test]
    fn skip_compression_stores_source_bytes_raw() {
        let mut batch = Batch::new(0, "demo.txt", Bytes::from_static(b"banana bandana banana"));
        batch.compression_plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);

        let pool = BufferPool::new(1024, 4);
        let totals = ProcessingThroughputTotals::default();
        let mut scratch = WorkerScratchArena::new();
        let block = process_batch(
            batch,
            &pool,
            CompressionAlgo::Lz4,
            true,
            false,
            &totals,
            &mut scratch,
        )
        .expect("batch should process");

        assert!(block.raw_passthrough);
        assert_eq!(block.data.as_slice(), b"banana bandana banana");
    }

    #[test]
    fn force_raw_storage_bypasses_compression() {
        let mut batch = Batch::new(0, "photo.jpg", Bytes::from_static(b"banana bandana banana"));
        batch.compression_plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);
        batch.force_raw_storage = true;

        let pool = BufferPool::new(1024, 4);
        let totals = ProcessingThroughputTotals::default();
        let mut scratch = WorkerScratchArena::new();
        let block = process_batch(
            batch,
            &pool,
            CompressionAlgo::Lz4,
            false,
            true,
            &totals,
            &mut scratch,
        )
        .expect("batch should process");

        assert!(block.raw_passthrough);
        assert_eq!(block.data.as_slice(), b"banana bandana banana");
    }

    #[test]
    fn incompressible_probe_identifies_random_like_data() {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let sample = (0..INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN)
            .map(|_| {
                state ^= state << 7;
                state ^= state >> 9;
                state ^= state << 8;
                state as u8
            })
            .collect::<Vec<_>>();
        let plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);
        let mut scratch = WorkerScratchArena::new();

        let probe = is_likely_incompressible_sample(&sample, plan, &mut scratch)
            .expect("probe should succeed");

        assert!(probe);
    }

    #[test]
    fn incompressible_probe_keeps_repetitive_data_compressible() {
        let sample = vec![b'a'; INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN];
        let plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);
        let mut scratch = WorkerScratchArena::new();

        let probe = is_likely_incompressible_sample(&sample, plan, &mut scratch)
            .expect("probe should succeed");

        assert!(!probe);
    }
}
