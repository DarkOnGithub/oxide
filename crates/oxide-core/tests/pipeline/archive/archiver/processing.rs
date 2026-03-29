use bytes::Bytes;

use super::{
    INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN, INCOMPRESSIBLE_PROBE_SAMPLE_LEN,
    LZMA_EXTREME_PROBE_MIN_SOURCE_LEN, LZMA_PROBE_MIN_SOURCE_LEN, compression_probe_config,
    is_likely_incompressible_sample, process_batch, should_skip_full_compression_probe,
};
use crate::buffer::BufferPool;
use crate::core::WorkerScratchArena;
use crate::dictionary::ArchiveDictionaryBank;
use crate::pipeline::archive::types::ProcessingThroughputTotals;
use crate::types::{Batch, ChunkEncodingPlan, CompressedPayload, CompressionAlgo};

#[test]
fn skip_compression_stores_source_bytes_raw() {
    let source = Bytes::from_static(b"banana bandana banana");
    let source_ptr = source.as_ptr();
    let mut batch = Batch::new(0, "demo.txt", source);
    batch.compression_plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);

    let pool = BufferPool::new(1024, 4);
    let totals = ProcessingThroughputTotals::default();
    let mut scratch = WorkerScratchArena::new();
    let dictionary_bank = ArchiveDictionaryBank::default();
    let block = process_batch(
        batch,
        &pool,
        CompressionAlgo::Lz4,
        true,
        false,
        &dictionary_bank,
        &totals,
        &mut scratch,
    )
    .expect("batch should process");

    assert!(block.raw_passthrough);
    assert_eq!(block.data.as_slice(), b"banana bandana banana");
    match block.data {
        CompressedPayload::Bytes(bytes) => assert_eq!(bytes.as_ptr(), source_ptr),
        other => panic!("expected bytes payload, got {other:?}"),
    }
}

#[test]
fn force_raw_storage_bypasses_compression() {
    let source = Bytes::from_static(b"banana bandana banana");
    let source_ptr = source.as_ptr();
    let mut batch = Batch::new(0, "photo.jpg", source);
    batch.compression_plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);
    batch.force_raw_storage = true;

    let pool = BufferPool::new(1024, 4);
    let totals = ProcessingThroughputTotals::default();
    let mut scratch = WorkerScratchArena::new();
    let dictionary_bank = ArchiveDictionaryBank::default();
    let block = process_batch(
        batch,
        &pool,
        CompressionAlgo::Lz4,
        false,
        true,
        &dictionary_bank,
        &totals,
        &mut scratch,
    )
    .expect("batch should process");

    assert!(block.raw_passthrough);
    assert_eq!(block.data.as_slice(), b"banana bandana banana");
    match block.data {
        CompressedPayload::Bytes(bytes) => assert_eq!(bytes.as_ptr(), source_ptr),
        other => panic!("expected bytes payload, got {other:?}"),
    }
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
    let dictionary_bank = ArchiveDictionaryBank::default();

    let probe = is_likely_incompressible_sample(&sample, plan, &dictionary_bank, &mut scratch)
        .expect("probe should succeed");

    assert!(probe);
}

#[test]
fn incompressible_probe_keeps_repetitive_data_compressible() {
    let sample = vec![b'a'; INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN];
    let plan = ChunkEncodingPlan::new(CompressionAlgo::Lz4);
    let mut scratch = WorkerScratchArena::new();
    let dictionary_bank = ArchiveDictionaryBank::default();

    let probe = is_likely_incompressible_sample(&sample, plan, &dictionary_bank, &mut scratch)
        .expect("probe should succeed");

    assert!(!probe);
}

#[test]
fn lzma_probe_is_now_enabled() {
    let regular = crate::types::ChunkEncodingPlan::new(CompressionAlgo::Lzma)
        .with_level(Some(7))
        .with_lzma_extreme(false);
    let extreme = crate::types::ChunkEncodingPlan::new(CompressionAlgo::Lzma)
        .with_level(Some(9))
        .with_lzma_extreme(true);

    let regular_config = compression_probe_config(regular);
    let extreme_config = compression_probe_config(extreme);

    assert_eq!(regular_config.min_source_len, INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN);
    assert_eq!(extreme_config.min_source_len, INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN);
    assert!(regular_config.sample_len < INCOMPRESSIBLE_PROBE_SAMPLE_LEN);
    assert_eq!(regular_config.sample_len, extreme_config.sample_len);
    assert!(should_skip_full_compression_probe(
        LZMA_PROBE_MIN_SOURCE_LEN,
        regular
    ));
    assert!(should_skip_full_compression_probe(
        LZMA_EXTREME_PROBE_MIN_SOURCE_LEN,
        extreme,
    ));
}
