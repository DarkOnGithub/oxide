use oxide_core::{
    ArchiveReader, ArchiveWriter, CHUNK_DESCRIPTOR_SIZE, ChunkDescriptor, CompressionAlgo,
    CompressionMeta, Footer, GLOBAL_HEADER_SIZE, GlobalHeader, OxideError, ReorderBuffer,
    SeekableArchiveWriter,
};
use std::io::Cursor;

fn block(id: usize, payload: &[u8], compression: CompressionAlgo) -> oxide_core::CompressedBlock {
    oxide_core::CompressedBlock::new(id, payload.to_vec(), compression, payload.len() as u64)
}

#[test]
fn compression_flags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let algorithms = [
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd,
        CompressionAlgo::Lzma,
    ];

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
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(1)?;
    writer.write_block(&block(0, b"payload", CompressionAlgo::Lz4))?;
    let archive = writer.write_footer()?;

    let decoded = GlobalHeader::read(&mut Cursor::new(archive))?;
    assert_eq!(decoded.version, oxide_core::OXZ_VERSION);
    assert_eq!(decoded.payload_offset, GLOBAL_HEADER_SIZE as u64);
    assert_eq!(decoded.block_count, 1);
    assert!(decoded.entry_table_offset >= decoded.payload_offset);
    assert!(decoded.chunk_table_offset >= decoded.entry_table_offset);
    Ok(())
}

#[test]
fn block_header_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let header = ChunkDescriptor::new(128, 1024, 384, CompressionAlgo::Lz4, 0xAABB_CCDD);

    let mut encoded = Vec::new();
    header.write(&mut encoded)?;
    assert_eq!(encoded.len(), CHUNK_DESCRIPTOR_SIZE);

    let decoded = ChunkDescriptor::read(&mut Cursor::new(encoded), 128)?;
    assert_eq!(decoded, header);
    assert_eq!(decoded.compression()?, CompressionAlgo::Lz4);
    Ok(())
}

#[test]
fn reference_block_header_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let header = ChunkDescriptor::new_reference(128, 1024, 3);

    let mut encoded = Vec::new();
    header.write(&mut encoded)?;
    assert_eq!(encoded.len(), CHUNK_DESCRIPTOR_SIZE);

    let decoded = ChunkDescriptor::read(&mut Cursor::new(encoded), 128)?;
    assert_eq!(decoded.reference_target, Some(3));
    assert_eq!(decoded.encoded_len, 0);
    assert_eq!(decoded.raw_len, 1024);
    Ok(())
}

#[test]
fn block_header_round_trip_preserves_raw_passthrough() -> Result<(), Box<dyn std::error::Error>> {
    let header = ChunkDescriptor::new_with_compression_meta(
        512,
        4096,
        4096,
        CompressionMeta::new(CompressionAlgo::Lz4, true),
        0x0102_0304,
    );

    let mut encoded = Vec::new();
    header.write(&mut encoded)?;
    assert_eq!(encoded.len(), CHUNK_DESCRIPTOR_SIZE);

    let decoded = ChunkDescriptor::read(&mut Cursor::new(encoded), 512)?;
    let meta = decoded.compression_meta()?;
    assert_eq!(decoded, header);
    assert_eq!(meta.algo, CompressionAlgo::Lz4);
    assert!(meta.raw_passthrough);
    Ok(())
}

#[test]
fn compression_meta_flags_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    for algo in [
        CompressionAlgo::Lz4,
        CompressionAlgo::Zstd,
        CompressionAlgo::Lzma,
    ] {
        let meta = CompressionMeta::new(algo, true);
        let encoded = meta.to_flags();
        let decoded = CompressionMeta::from_flags(encoded)?;

        assert_eq!(decoded.algo, algo);
        assert!(decoded.raw_passthrough);
    }

    assert!(CompressionMeta::from_flags(0b0000_0000).is_err());
    assert!(CompressionMeta::from_flags(0b0001_0000).is_err());
    Ok(())
}

#[test]
fn footer_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let footer = Footer::new(3, 100, 20, 120, 42, 0, 0xDEAD_BEEF);
    let mut encoded = Vec::new();
    footer.write(&mut encoded)?;
    let decoded = Footer::read(&mut Cursor::new(encoded))?;
    assert_eq!(decoded, footer);
    Ok(())
}

#[test]
fn archive_writer_and_reader_support_random_and_sequential_access()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(3)?;
    writer.write_block(&block(0, b"alpha", CompressionAlgo::Lz4))?;
    writer.write_block(&block(1, b"beta", CompressionAlgo::Lz4))?;
    writer.write_block(&block(2, b"gamma", CompressionAlgo::Lz4))?;
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
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(3)?;

    assert_eq!(
        writer.push_block(block(2, b"third", CompressionAlgo::Lz4))?,
        0
    );
    assert_eq!(
        writer.push_block(block(0, b"first", CompressionAlgo::Lz4))?,
        1
    );
    assert_eq!(
        writer.push_block(block(1, b"second", CompressionAlgo::Lz4))?,
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
fn archive_writer_deduplicates_duplicate_blocks() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(2)?;
    writer.write_block(&block(0, b"repeat", CompressionAlgo::Lz4))?;
    writer.write_block(&block(1, b"repeat", CompressionAlgo::Lz4))?;
    let archive = writer.write_footer()?;

    let reader = ArchiveReader::new(Cursor::new(archive.clone()))?;
    let header = reader.global_header();
    let chunk_table = &archive[header.chunk_table_offset as usize
        ..(header.chunk_table_offset + u64::from(header.chunk_table_len)) as usize];
    let chunk_table = if reader.footer().chunk_table_compressed() {
        oxide_core::compression::zstd::reverse(
            chunk_table,
            Some(header.block_count as usize * CHUNK_DESCRIPTOR_SIZE),
        )?
    } else {
        chunk_table.to_vec()
    };
    let descriptors = oxide_core::format::oxz::decode_chunk_table(
        &chunk_table,
        header.payload_offset,
        header.block_count,
    )?;

    assert_eq!(descriptors[0].reference_target, None);
    assert_eq!(descriptors[1].reference_target, Some(0));
    assert_eq!(descriptors[1].encoded_len, 0);

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.read_block(0)?.1, b"repeat");
    assert_eq!(reader.read_block(1)?.1, b"repeat");
    Ok(())
}

#[test]
fn seekable_archive_writer_streams_payload_and_round_trips()
-> Result<(), Box<dyn std::error::Error>> {
    let cursor = Cursor::new(Vec::new());
    let mut writer = SeekableArchiveWriter::new(cursor);
    writer.write_global_header(3)?;
    writer.write_block(&block(0, b"alpha", CompressionAlgo::Lz4))?;
    writer.write_block(&block(1, b"beta", CompressionAlgo::Lz4))?;
    writer.write_block(&block(2, b"gamma", CompressionAlgo::Lz4))?;

    let archive = writer.write_footer()?.into_inner();
    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.block_count(), 3);
    let (header, payload) = reader.read_block(1)?;
    assert_eq!(payload, b"beta");
    assert!(header.payload_offset > 0);
    Ok(())
}

#[test]
fn seekable_archive_writer_deduplicates_duplicate_blocks() -> Result<(), Box<dyn std::error::Error>>
{
    let cursor = Cursor::new(Vec::new());
    let mut writer = SeekableArchiveWriter::new(cursor);
    writer.write_global_header(2)?;
    writer.write_block(&block(0, b"repeat", CompressionAlgo::Lz4))?;
    writer.write_block(&block(1, b"repeat", CompressionAlgo::Lz4))?;

    let archive = writer.write_footer()?.into_inner();
    let reader = ArchiveReader::new(Cursor::new(archive.clone()))?;
    let header = reader.global_header();
    let chunk_table = &archive[header.chunk_table_offset as usize
        ..(header.chunk_table_offset + u64::from(header.chunk_table_len)) as usize];
    let chunk_table = if reader.footer().chunk_table_compressed() {
        oxide_core::compression::zstd::reverse(
            chunk_table,
            Some(header.block_count as usize * CHUNK_DESCRIPTOR_SIZE),
        )?
    } else {
        chunk_table.to_vec()
    };
    let descriptors = oxide_core::format::oxz::decode_chunk_table(
        &chunk_table,
        header.payload_offset,
        header.block_count,
    )?;

    assert_eq!(descriptors[0].reference_target, None);
    assert_eq!(descriptors[1].reference_target, Some(0));
    assert_eq!(descriptors[1].encoded_len, 0);

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    assert_eq!(reader.read_block(0)?.1, b"repeat");
    assert_eq!(reader.read_block(1)?.1, b"repeat");
    Ok(())
}

#[test]
fn archive_writer_can_disable_block_deduplication() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = ArchiveWriter::with_limits_and_manifest(
        Vec::new(),
        oxide_core::DEFAULT_REORDER_PENDING_LIMIT,
        0,
        None,
    );
    writer.write_global_header(2)?;
    writer.write_block(&block(0, b"repeat", CompressionAlgo::Lz4))?;
    writer.write_block(&block(1, b"repeat", CompressionAlgo::Lz4))?;
    let archive = writer.write_footer()?;

    let reader = ArchiveReader::new(Cursor::new(archive.clone()))?;
    let header = reader.global_header();
    let chunk_table = &archive[header.chunk_table_offset as usize
        ..(header.chunk_table_offset + u64::from(header.chunk_table_len)) as usize];
    let chunk_table = if reader.footer().chunk_table_compressed() {
        oxide_core::compression::zstd::reverse(
            chunk_table,
            Some(header.block_count as usize * CHUNK_DESCRIPTOR_SIZE),
        )?
    } else {
        chunk_table.to_vec()
    };
    let descriptors = oxide_core::format::oxz::decode_chunk_table(
        &chunk_table,
        header.payload_offset,
        header.block_count,
    )?;

    assert_eq!(descriptors[0].reference_target, None);
    assert_eq!(descriptors[1].reference_target, None);
    assert!(descriptors[1].encoded_len > 0);
    Ok(())
}

#[test]
fn reader_ignores_global_crc_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(1)?;
    writer.write_block(&block(0, b"payload", CompressionAlgo::Lz4))?;
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
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(1)?;

    let mut corrupted_crc = block(0, b"payload", CompressionAlgo::Lz4);
    corrupted_crc.crc32 ^= 0x0101_0101;
    writer.write_block(&corrupted_crc)?;
    let archive = writer.write_footer()?;

    let mut reader = ArchiveReader::new(Cursor::new(archive))?;
    let (header, payload) = reader.read_block(0)?;
    assert_eq!(payload, b"payload");
    assert!(header.payload_offset >= GLOBAL_HEADER_SIZE as u64);
    Ok(())
}

#[test]
fn read_block_rejects_out_of_bounds_index() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = ArchiveWriter::new(Vec::new());
    writer.write_global_header(1)?;
    writer.write_block(&block(0, b"payload", CompressionAlgo::Lz4))?;
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

#[test]
fn archive_writer_compresses_manifest_and_chunk_table_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let entries = (0..256)
        .map(|index| {
            oxide_core::ArchiveListingEntry::file(
                format!("nested/path/file-{index:04}.txt"),
                6,
                0o644,
                oxide_core::ArchiveTimestamp::default(),
                0,
                0,
                index as u64 * 6,
            )
        })
        .collect::<Vec<_>>();
    let manifest = oxide_core::ArchiveManifest::new(entries);
    let mut writer = ArchiveWriter::with_manifest(Vec::new(), Some(manifest));
    writer.write_global_header(256)?;
    for index in 0..256 {
        writer.write_block(&block(index, b"repeat", CompressionAlgo::Lz4))?;
    }

    let archive = writer.write_footer()?;
    let reader = ArchiveReader::new(Cursor::new(archive))?;

    assert!(reader.footer().manifest_compressed());
    assert!(reader.footer().chunk_table_compressed());
    Ok(())
}
