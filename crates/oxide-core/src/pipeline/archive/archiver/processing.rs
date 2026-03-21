use std::time::{Duration, Instant};

use crate::buffer::BufferPool;
use crate::compression::{CompressionRequest, apply_compression_request_with_scratch};
use crate::core::WorkerScratchArena;
use crate::pipeline::archive::types::ProcessingThroughputTotals;
use crate::preprocessing::{apply_preprocessing_with_metadata, get_preprocessing_strategy};
use crate::types::{Batch, CompressedBlock, CompressedPayload, CompressionAlgo, Result};

pub fn process_batch(
    batch: Batch,
    pool: &BufferPool,
    _compression: CompressionAlgo,
    skip_preprocessing: bool,
    skip_compression: bool,
    raw_fallback_enabled: bool,
    processing_totals: &ProcessingThroughputTotals,
    scratch: &mut WorkerScratchArena,
) -> Result<CompressedBlock> {
    let Batch {
        id,
        data,
        file_type_hint,
        preprocessing_metadata,
        stream_id,
        compression_plan: plan,
        force_raw_storage,
        ..
    } = batch;
    let source_len = data.len();
    let source = data.as_slice();
    if force_raw_storage {
        processing_totals.record(
            source.len() as u64,
            Duration::ZERO,
            source.len() as u64,
            Duration::ZERO,
        );

        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data_in_pool(data, pool),
            crate::PreProcessingStrategy::None,
            plan,
            true,
            source_len as u64,
        ));
    }

    let strategy = if skip_preprocessing {
        crate::PreProcessingStrategy::None
    } else {
        get_preprocessing_strategy(
            file_type_hint,
            &plan.preprocessing_profile,
            preprocessing_metadata.as_ref(),
        )
    };

    let (preprocessed, preprocessing_elapsed) = if strategy == crate::PreProcessingStrategy::None {
        (None, Duration::ZERO)
    } else {
        let preprocessing_started = Instant::now();
        let p =
            apply_preprocessing_with_metadata(source, &strategy, preprocessing_metadata.as_ref())?;
        (Some(p), preprocessing_started.elapsed())
    };
    let compression_input = preprocessed.as_deref().unwrap_or(source);

    let (compressed, compression_elapsed) = if skip_compression {
        (compression_input.to_vec(), Duration::ZERO)
    } else {
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
        (compressed, compression_started.elapsed())
    };
    processing_totals.record(
        source_len as u64,
        preprocessing_elapsed,
        compression_input.len() as u64,
        compression_elapsed,
    );
    let raw_passthrough =
        skip_compression || (raw_fallback_enabled && compressed.len() >= source.len());
    let stored_strategy = if raw_passthrough && !skip_compression {
        crate::PreProcessingStrategy::None
    } else {
        strategy
    };
    let data = if skip_compression {
        match preprocessed {
            Some(preprocessed) => CompressedPayload::from_vec_in_pool(preprocessed, pool),
            None => CompressedPayload::from_batch_data_in_pool(data, pool),
        }
    } else if raw_passthrough {
        CompressedPayload::from_batch_data_in_pool(data, pool)
    } else {
        CompressedPayload::from_vec_in_pool(compressed, pool)
    };

    Ok(CompressedBlock::with_chunk_encoding(
        id,
        stream_id,
        data,
        stored_strategy,
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
    use crate::preprocessing::{apply_preprocessing_with_metadata, reverse_preprocessing};
    use crate::types::{Batch, ChunkEncodingPlan, CompressionAlgo, CompressionPreset, FileFormat};
    use crate::{PreProcessingStrategy, TextStrategy};

    #[test]
    fn skip_preprocessing_disables_strategy_selection() {
        let mut batch = Batch::with_hint(
            0,
            "demo.txt",
            Bytes::from_static(b"hello world"),
            FileFormat::Text,
        );
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
            false,
            &totals,
            &mut scratch,
        )
        .expect("batch should process");

        assert_eq!(block.pre_proc, PreProcessingStrategy::None);
    }

    #[test]
    fn skip_compression_stores_preprocessed_bytes_raw() {
        let mut batch = Batch::with_hint(
            0,
            "demo.txt",
            Bytes::from_static(b"banana bandana banana"),
            FileFormat::Text,
        );
        batch.compression_plan =
            ChunkEncodingPlan::new(CompressionAlgo::Lz4, CompressionPreset::Default);
        let expected = apply_preprocessing_with_metadata(
            batch.data(),
            &PreProcessingStrategy::Text(TextStrategy::Bpe),
            batch.preprocessing_metadata.as_ref(),
        )
        .expect("preprocessing should succeed");

        let pool = BufferPool::new(1024, 4);
        let totals = ProcessingThroughputTotals::default();
        let mut scratch = WorkerScratchArena::new();
        let block = process_batch(
            batch,
            &pool,
            CompressionAlgo::Lz4,
            false,
            true,
            false,
            &totals,
            &mut scratch,
        )
        .expect("batch should process");

        assert!(block.raw_passthrough);
        assert_eq!(
            block.pre_proc,
            PreProcessingStrategy::Text(TextStrategy::Bpe)
        );
        assert_eq!(block.data.as_slice(), expected.as_slice());
        assert_eq!(
            reverse_preprocessing(block.data.as_slice(), &block.pre_proc)
                .expect("reverse should succeed"),
            b"banana bandana banana"
        );
    }

    #[test]
    fn force_raw_storage_bypasses_preprocessing_and_compression() {
        let mut batch = Batch::with_hint(
            0,
            "photo.jpg",
            Bytes::from_static(b"banana bandana banana"),
            FileFormat::Text,
        );
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
            false,
            true,
            &totals,
            &mut scratch,
        )
        .expect("batch should process");

        assert!(block.raw_passthrough);
        assert_eq!(block.pre_proc, PreProcessingStrategy::None);
        assert_eq!(block.data.as_slice(), b"banana bandana banana");
    }
}
