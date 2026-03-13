use std::io::Cursor;
use std::sync::Arc;

use oxide_core::{
    ARCHIVE_METADATA_SIZE, ArchiveReader, ArchiveWriter, BlockHeader, BufferPool,
    CHUNK_DESCRIPTOR_SIZE, CompressionAlgo, CompressionMeta, CompressionPreset, Footer,
    GLOBAL_HEADER_SIZE, GlobalHeader, ImageStrategy, OxideError, PreProcessingStrategy,
    ReorderBuffer, SeekableArchiveWriter, TextStrategy,
};

fn block(
    id: usize,
    payload: &[u8],
    pre_proc: PreProcessingStrategy,
    compression: CompressionAlgo,
) -> oxide_core::CompressedBlock {
    oxide_core::CompressedBlock::new(
        id,
        payload.to_vec(),
        pre_proc,
        compression,
        payload.len() as u64,
    )
}

#[test]
fn strategy_flags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let strategies = [
        PreProcessingStrategy::None,
        PreProcessingStrategy::Text(TextStrategy::Bpe),
        PreProcessingStrategy::Text(TextStrategy::Bwt),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR),
        PreProcessingStrategy::Image(ImageStrategy::Paeth),
        PreProcessingStrategy::Image(ImageStrategy::LocoI),
    ];

    for strategy in strategies {
        let flags = strategy.to_flags();
        let decoded = PreProcessingStrategy::from_flags(flags)?;
        assert_eq!(decoded, strategy);
    }

    assert!(PreProcessingStrategy::from_flags(0b1100_0001).is_err());
    assert!(PreProcessingStrategy::from_flags(0x07).is_err());
    Ok(())
}

#[test]
fn compression_flags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let algorithms = [CompressionAlgo::Lz4];

    for algorithm in algorithms {
        let flags = algorithm.to_flags();
        let decoded = CompressionAlgo::from_flags(flags)?;
        assert_eq!(decoded, algorithm);
    }

    assert!(CompressionAlgo::from_flags(0x10).is_err());
    Ok(())
}

#[test]
fn header_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let header = GlobalHeader::new(48, 56, 88, 120, 184);
    let mut encoded = Vec::new();
    header.write(&mut encoded)?;
    assert_eq!(encoded.len(), GLOBAL_HEADER_SIZE);

    let decoded = GlobalHeader::read(&mut Cursor::new(encoded))?;
    assert_eq!(decoded, header);
    Ok(())
}

#[test]
fn block_header_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let header = BlockHeader::new(
        128,
        1024,
        384,
        PreProcessingStrategy::Text(TextStrategy::Bwt),
        CompressionAlgo::Lz4,
        0xAABB_CCDD,
    );

    let mut encoded = Vec::new();
    header.write(&mut encoded)?;
    assert_eq!(encoded.len(), CHUNK_DESCRIPTOR_SIZE);

    let decoded = BlockHeader::read(&mut Cursor::new(encoded))?;
    assert_eq!(decoded, header);
    assert_eq!(
        decoded.strategy()?,
        PreProcessingStrategy::Text(TextStrategy::Bwt)
    );
    assert_eq!(decoded.compression()?, CompressionAlgo::Lz4);
    Ok(())
}

#[test]
fn block_header_round_trip_preserves_raw_passthrough() -> Result<(), Box<dyn std::error::Error>> {
    let header = BlockHeader::new_with_compression_meta(
        512,
        4096,
        4096,
        PreProcessingStrategy::None,
        CompressionMeta::new(CompressionAlgo::Lz4, CompressionPreset::Fast, true),
        0x0102_0304,
    );

    let mut encoded = Vec::new();
    header.write(&mut encoded)?;
    assert_eq!(encoded.len(), CHUNK_DESCRIPTOR_SIZE);

    let decoded = BlockHeader::read(&mut Cursor::new(encoded))?;
    let meta = decoded.compression_meta()?;
    assert_eq!(decoded, header);
    assert_eq!(meta.algo, CompressionAlgo::Lz4);
    assert_eq!(meta.preset, CompressionPreset::Fast);
    assert!(meta.raw_passthrough);
    Ok(())
}

#[test]
fn compression_meta_flags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let meta = CompressionMeta::new(CompressionAlgo::Lz4, CompressionPreset::Fast, true);
    let encoded = meta.to_flags();
    let decoded = CompressionMeta::from_flags(encoded)?;

    assert_eq!(decoded.algo, CompressionAlgo::Lz4);
    assert_eq!(decoded.preset, CompressionPreset::Fast);
    assert!(decoded.raw_passthrough);

    assert!(CompressionMeta::from_flags(0b1110_0001).is_err());
    assert!(CompressionMeta::from_flags(0b0001_0000).is_err());
    Ok(())
}

#[test]
fn footer_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let footer = Footer::new(0xDEAD_BEEF);
    let mut encoded = Vec::new();
    footer.write(&mut encoded)?;
    let decoded = Footer::read(&mut Cursor::new(encoded))?;
    assert_eq!(decoded, footer);
    Ok(())
}

#[test]
fn archive_writer_and_reader_support_random_and_sequential_access()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let mut writer = ArchiveWriter::new(Vec::new(), Arc::clone(&pool));
    writer.write_global_header(3)?;
    writer.write_block(&block(
        0,
        b"alpha",
        PreProcessingStrategy::None,
        CompressionAlgo::Lz4,
    ))?;
    writer.write_block(&block(
        1,
        b"beta",
        PreProcessingStrategy::Text(TextStrategy::Bwt),
        CompressionAlgo::Lz4,
    ))?;
    writer.write_block(&block(
        2,
        b"gamma",
        PreProcessingStrategy::Image(ImageStrategy::Paeth),
        CompressionAlgo::Lz4,
    ))?;
    let archive = writer.write_footer()?;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.block_count(), 3);

    let (header, payload) = reader.read_block(1)?;
    assert_eq!(payload, b"beta");
    assert!(header.payload_offset > 0);

    let mut seen_offsets = Vec::new();
    for block_entry in reader.iter_blocks() {
        let (block_header, data) = block_entry?;
        seen_offsets.push(block_header.payload_offset);
        assert_eq!(data.len(), block_header.encoded_len as usize);
    }
    assert_eq!(seen_offsets.len(), 3);
    assert!(seen_offsets.windows(2).all(|pair| pair[0] < pair[1]));

    Ok(())
}

#[test]
fn archive_writer_reorders_out_of_order_blocks() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let mut writer = ArchiveWriter::new(Vec::new(), Arc::clone(&pool));
    writer.write_global_header(3)?;

    assert_eq!(
        writer.push_block(block(
            2,
            b"third",
            PreProcessingStrategy::None,
            CompressionAlgo::Lz4
        ))?,
        0
    );
    assert_eq!(
        writer.push_block(block(
            0,
            b"first",
            PreProcessingStrategy::None,
            CompressionAlgo::Lz4
        ))?,
        1
    );
    assert_eq!(
        writer.push_block(block(
            1,
            b"second",
            PreProcessingStrategy::None,
            CompressionAlgo::Lz4
        ))?,
        2
    );

    let archive = writer.write_footer()?;
    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    let payloads: Vec<Vec<u8>> = reader
        .iter_blocks()
        .map(|entry| entry.map(|(_, payload)| payload))
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        payloads,
        vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
    );
    Ok(())
}

#[test]
fn seekable_archive_writer_streams_payload_and_round_trips()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let cursor = Cursor::new(Vec::new());
    let mut writer = SeekableArchiveWriter::new(cursor, Arc::clone(&pool));
    writer.write_global_header(3)?;
    writer.write_block(&block(
        0,
        b"alpha",
        PreProcessingStrategy::None,
        CompressionAlgo::Lz4,
    ))?;
    writer.write_block(&block(
        1,
        b"beta",
        PreProcessingStrategy::Text(TextStrategy::Bwt),
        CompressionAlgo::Lz4,
    ))?;
    writer.write_block(&block(
        2,
        b"gamma",
        PreProcessingStrategy::Image(ImageStrategy::Paeth),
        CompressionAlgo::Lz4,
    ))?;

    let archive = writer.write_footer()?.into_inner();
    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.block_count(), 3);
    let (header, payload) = reader.read_block(1)?;
    assert_eq!(payload, b"beta");
    assert!(header.payload_offset > 0);
    Ok(())
}

#[test]
fn reader_ignores_global_crc_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let mut writer = ArchiveWriter::new(Vec::new(), pool);
    writer.write_global_header(1)?;
    writer.write_block(&block(
        0,
        b"payload",
        PreProcessingStrategy::None,
        CompressionAlgo::Lz4,
    ))?;
    let mut archive = writer.write_footer()?;

    let payload_offset = ArchiveReader::new(Cursor::new(archive.clone()))?
        .global_header()
        .payload_offset as usize;
    archive[payload_offset] ^= 0xFF;

    let reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.block_count(), 1);
    Ok(())
}

#[test]
fn reader_ignores_block_crc_mismatch_on_read() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let mut writer = ArchiveWriter::new(Vec::new(), pool);
    writer.write_global_header(1)?;

    let mut corrupted_crc = block(
        0,
        b"payload",
        PreProcessingStrategy::None,
        CompressionAlgo::Lz4,
    );
    corrupted_crc.crc32 ^= 0x0101_0101;
    writer.write_block(&corrupted_crc)?;
    let archive = writer.write_footer()?;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    let (header, payload) = reader.read_block(0)?;
    assert_eq!(payload, b"payload");
    assert!(header.payload_offset >= (GLOBAL_HEADER_SIZE + ARCHIVE_METADATA_SIZE) as u64);
    Ok(())
}

#[test]
fn read_block_rejects_out_of_bounds_index() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let mut writer = ArchiveWriter::new(Vec::new(), pool);
    writer.write_global_header(1)?;
    writer.write_block(&block(
        0,
        b"payload",
        PreProcessingStrategy::None,
        CompressionAlgo::Lz4,
    ))?;
    let archive = writer.write_footer()?;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    let err = reader.read_block(9).unwrap_err();
    assert!(matches!(
        err,
        OxideError::InvalidFormat("block index out of range")
    ));
    Ok(())
}

#[test]
fn reorder_buffer_enforces_capacity_and_duplicates() {
    let mut reorder = ReorderBuffer::with_limit(1);
    assert!(reorder.push(1usize, "late").unwrap().is_empty());

    let err = reorder.push(3usize, "too-late").unwrap_err();
    assert!(matches!(
        err,
        OxideError::InvalidFormat("reorder buffer capacity exceeded")
    ));

    let mut reorder = ReorderBuffer::with_limit(4);
    assert!(reorder.push(2usize, "block2").unwrap().is_empty());
    let err = reorder.push(2usize, "duplicate").unwrap_err();
    assert!(matches!(
        err,
        OxideError::InvalidFormat("duplicate block id in reorder buffer")
    ));
}
