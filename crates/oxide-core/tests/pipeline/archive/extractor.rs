use super::{
    decode_queue_capacity, join_io_reader, ordered_write_queue_capacity, reorder_pending_limit,
    spawn_io_reader, ReadRequest, MIN_DECODE_QUEUE_CAPACITY, MIN_ORDERED_WRITE_QUEUE_CAPACITY,
};
use crate::{
    ArchiveManifest, ArchiveReader, ArchiveWriter, BufferPool, CompressedBlock, CompressionAlgo,
    CompressionMeta,
};
use std::io::Cursor;
use std::sync::Arc;

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

#[test]
fn extract_queue_capacity_respects_minimums_and_block_count() {
    assert_eq!(decode_queue_capacity(1, 1, 1), 1);
    assert_eq!(decode_queue_capacity(1, 4, 4), 4);
    assert_eq!(decode_queue_capacity(1, 64, 64), MIN_DECODE_QUEUE_CAPACITY);
}

#[test]
fn ordered_write_queue_is_at_least_decode_queue_and_bounded() {
    let decode = decode_queue_capacity(2, 64, 64);
    let ordered = ordered_write_queue_capacity(2, decode, 64, 64);

    assert!(ordered >= decode);
    assert!(ordered >= MIN_ORDERED_WRITE_QUEUE_CAPACITY);
    assert!(ordered <= 64);
}

#[test]
fn reorder_limit_is_derived_from_ordered_queue_and_capped_by_blocks() {
    assert_eq!(reorder_pending_limit(16, 128, 128), 128);
    assert_eq!(reorder_pending_limit(64, 96, 96), 96);
}

#[test]
fn io_reader_moves_block_reads_off_the_orchestrator_path() {
    let archive = ArchiveReader::new_for_sequential_extract(Cursor::new(build_test_archive()))
        .expect("archive should open");
    let buffer_pool = Arc::new(BufferPool::new(16, 4));
    let (read_request_tx, read_request_rx) = crossbeam_channel::bounded(1);
    let (task_tx, task_rx) = crossbeam_channel::bounded(1);
    let (result_tx, _result_rx) = crossbeam_channel::bounded(1);

    let reader_handle = std::thread::spawn(move || {
        spawn_io_reader(archive, read_request_rx, task_tx, result_tx, buffer_pool)
    });

    read_request_tx
        .send(ReadRequest {
            index: 0,
            block_index: 0,
            encoded_len: 5,
        })
        .expect("read request should send");
    drop(read_request_tx);

    let task = task_rx.recv().expect("decode task should arrive");
    assert_eq!(task.index, 0);
    assert_eq!(task.block_data.as_slice(), b"alpha");

    join_io_reader(reader_handle).expect("reader should join cleanly");
}
