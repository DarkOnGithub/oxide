use bytes::Bytes;
use oxide_core::{
    Batch, BatchData, BinaryStrategy, CompressedBlock, CompressionAlgo, FileFormat, MmapInput,
    PreProcessingStrategy,
};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn batch_constructor_defaults_to_unknown() {
    let batch = Batch::new(7, "sample.bin", Bytes::from_static(b"abc"));

    assert_eq!(batch.id, 7);
    assert_eq!(batch.source_path, std::path::PathBuf::from("sample.bin"));
    assert_eq!(batch.file_type_hint, FileFormat::Common);
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());
}

#[test]
fn batch_constructor_with_hint() {
    let batch = Batch::with_hint(1, "sound.wav", Bytes::new(), FileFormat::Audio);

    assert_eq!(batch.file_type_hint, FileFormat::Audio);
    assert!(batch.is_empty());
}

#[test]
fn compressed_block_constructor_sets_crc() {
    let block = CompressedBlock::new(
        2,
        vec![1, 2, 3, 4],
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj),
        CompressionAlgo::Lz4,
        2048,
    );

    assert!(block.verify_crc32());
}

#[test]
fn compressed_block_crc_detects_mutation() {
    let mut block = CompressedBlock::new(
        2,
        vec![1, 2, 3, 4],
        PreProcessingStrategy::None,
        CompressionAlgo::Deflate,
        4,
    );
    block.data.push(5);

    assert!(!block.verify_crc32());
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

    let batch = Batch::from_mapped(3, "mapped.bin", map, start, end, FileFormat::Binary);
    assert_eq!(batch.data(), b"cdef");
    assert_eq!(batch.file_type_hint, FileFormat::Binary);

    Ok(())
}
