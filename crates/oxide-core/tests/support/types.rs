use bytes::Bytes;
use oxide_core::{
    Batch, BatchData, CompressedBlock, CompressedPayload, CompressionAlgo, MmapInput,
    compute_checksum,
};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn batch_constructor_defaults() {
    let batch = Batch::new(7, "sample.bin", Bytes::from_static(b"abc"));

    assert_eq!(batch.id, 7);
    assert_eq!(batch.source_path, std::path::PathBuf::from("sample.bin"));
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());
    assert_eq!(batch.stream_id, 0);
    assert!(!batch.force_raw_storage);
}

#[test]
fn compressed_block_constructor_sets_crc() {
    let payload = vec![1, 2, 3, 4];
    let block = CompressedBlock::new(2, payload.clone(), CompressionAlgo::Lz4, 2048);

    assert_eq!(block.crc32, compute_checksum(&payload));
    assert!(block.verify_crc32());
}

#[test]
fn compressed_block_crc_detects_payload_mutation() {
    let block = CompressedBlock::new(2, vec![1, 2, 3, 4], CompressionAlgo::Lz4, 4);

    let mutated = CompressedBlock {
        data: CompressedPayload::from(vec![1, 2, 3, 4, 5]),
        ..block
    };

    assert!(!mutated.verify_crc32());
}

#[test]
fn mapped_batch_data_reports_len_and_slice() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(b"abcdefghij")?;
    file.flush()?;

    let mmap = MmapInput::open(file.path())?;
    let mapped = mmap.mapped_slice(2, 6)?;

    assert_eq!(mapped.len(), 4);
    assert!(!mapped.is_empty());
    assert_eq!(mapped.as_slice(), b"cdef");

    let (map, start, end) = match mapped {
        BatchData::Mapped { map, start, end } => (map, start, end),
        BatchData::Owned(_) => panic!("expected mapped slice"),
    };

    let batch = Batch::from_mapped(3, "mapped.bin", map, start, end);
    assert_eq!(batch.data(), b"cdef");

    Ok(())
}
