use std::time::Instant;

use crate::buffer::BufferPool;
use crate::compression::{CompressionRequest, apply_compression_request_with_scratch};
use crate::core::WorkerScratchArena;
use crate::pipeline::archive::types::ProcessingThroughputTotals;
use crate::types::{Batch, CompressedBlock, CompressedPayload, CompressionAlgo, Result};

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

    let (compressed, compression_elapsed) = if skip_compression {
        (source.to_vec(), std::time::Duration::ZERO)
    } else {
        let compression_started = Instant::now();
        let compressed = apply_compression_request_with_scratch(
            CompressionRequest {
                data: source,
                algo: plan.algo,
                preset: plan.preset,
                zstd_level: plan.zstd_level,
            },
            scratch.compression(),
        )?;
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

    use super::process_batch;
    use crate::buffer::BufferPool;
    use crate::core::WorkerScratchArena;
    use crate::pipeline::archive::types::ProcessingThroughputTotals;
    use crate::types::{Batch, ChunkEncodingPlan, CompressionAlgo, CompressionPreset};

    #[test]
    fn skip_compression_stores_source_bytes_raw() {
        let mut batch = Batch::new(0, "demo.txt", Bytes::from_static(b"banana bandana banana"));
        batch.compression_plan =
            ChunkEncodingPlan::new(CompressionAlgo::Lz4, CompressionPreset::Default);

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
        batch.compression_plan =
            ChunkEncodingPlan::new(CompressionAlgo::Lz4, CompressionPreset::Default);
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
}
