use std::cell::Cell;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::rc::Rc;

use crate::{
    ArchiveManifest, ArchiveReader, ArchiveWriter, ChunkDescriptor, CompressedBlock,
    CompressionAlgo, CompressionMeta,
};

#[derive(Debug, Clone)]
struct SeekCountingCursor {
    inner: Cursor<Vec<u8>>,
    seek_count: Rc<Cell<usize>>,
}

impl SeekCountingCursor {
    fn new(bytes: Vec<u8>) -> (Self, Rc<Cell<usize>>) {
        let seek_count = Rc::new(Cell::new(0));
        (
            Self {
                inner: Cursor::new(bytes),
                seek_count: Rc::clone(&seek_count),
            },
            seek_count,
        )
    }
}

impl Read for SeekCountingCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for SeekCountingCursor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.seek_count.set(self.seek_count.get().saturating_add(1));
        self.inner.seek(pos)
    }
}

fn build_test_archive() -> Vec<u8> {
    let mut writer = ArchiveWriter::with_manifest(Vec::new(), Some(ArchiveManifest::default()));
    writer
        .write_global_header_with_flags(2, 0)
        .expect("test archive header should write");

    let meta = CompressionMeta::new(CompressionAlgo::Lz4, true);
    writer
        .write_owned_block(CompressedBlock::with_compression_meta(
            0,
            b"alpha".to_vec(),
            meta,
            5,
        ))
        .expect("first test block should write");
    writer
        .write_owned_block(CompressedBlock::with_compression_meta(
            1,
            b"beta".to_vec(),
            meta,
            4,
        ))
        .expect("second test block should write");

    writer
        .write_footer()
        .expect("test archive footer should write")
}

fn build_reference_test_archive() -> Vec<u8> {
    let mut writer = ArchiveWriter::with_manifest(Vec::new(), Some(ArchiveManifest::default()));
    writer
        .write_global_header_with_flags(4, 0)
        .expect("test archive header should write");

    let meta = CompressionMeta::new(CompressionAlgo::Lz4, true);
    writer
        .write_owned_block(CompressedBlock::with_compression_meta(
            0,
            b"alpha".to_vec(),
            meta,
            5,
        ))
        .expect("first test block should write");
    writer
        .write_owned_block(CompressedBlock::with_compression_meta(
            1,
            b"beta".to_vec(),
            meta,
            4,
        ))
        .expect("second test block should write");
    writer
        .write_owned_block(CompressedBlock::reference(2, 0, 0, 5))
        .expect("reference test block should write");
    writer
        .write_owned_block(CompressedBlock::with_compression_meta(
            3,
            b"gamma".to_vec(),
            meta,
            5,
        ))
        .expect("fourth test block should write");

    writer
        .write_footer()
        .expect("test archive footer should write")
}

#[test]
fn sequential_block_reads_reuse_buffer_without_extra_seeks() {
    let archive_bytes = build_test_archive();

    let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
    let mut archive =
        ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");
    let seek_count_after_open = seek_count.get();

    let mut first_buffer = Vec::new();
    let first: ChunkDescriptor = archive
        .read_block_into(0, &mut first_buffer)
        .expect("first block should read");
    assert_eq!(seek_count.get(), seek_count_after_open);
    assert_eq!(first.encoded_len, 5);
    assert_eq!(&first_buffer, b"alpha");

    let mut buffer = Vec::with_capacity(16);
    let second = archive
        .read_block_into(1, &mut buffer)
        .expect("second block should read");
    assert_eq!(seek_count.get(), seek_count_after_open);
    assert_eq!(&buffer, b"beta");
    assert_eq!(second.encoded_len, 4);
    assert!(buffer.capacity() >= 16);
}

#[test]
fn sequential_reader_seeks_when_blocks_are_requested_out_of_order() {
    let archive_bytes = build_test_archive();

    let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
    let mut archive =
        ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");
    let seek_count_after_open = seek_count.get();

    let mut buffer = Vec::new();
    archive
        .read_block_into(1, &mut buffer)
        .expect("out-of-order block should read");

    assert!(seek_count.get() > seek_count_after_open);
    assert_eq!(&buffer, b"beta");
}

#[test]
fn sequential_reference_blocks_keep_cursor_aligned() {
    let archive_bytes = build_reference_test_archive();

    let (reader, _seek_count) = SeekCountingCursor::new(archive_bytes);
    let mut archive =
        ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");

    let mut buffer = Vec::new();
    archive
        .read_block_into(0, &mut buffer)
        .expect("first block should read");
    assert_eq!(&buffer, b"alpha");

    archive
        .read_block_into(1, &mut buffer)
        .expect("second block should read");
    assert_eq!(&buffer, b"beta");

    archive
        .read_block_into(2, &mut buffer)
        .expect("reference block should read");
    assert_eq!(&buffer, b"alpha");

    archive
        .read_block_into(3, &mut buffer)
        .expect("block after reference should read");
    assert_eq!(&buffer, b"gamma");
}
