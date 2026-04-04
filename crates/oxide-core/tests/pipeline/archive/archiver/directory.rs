use super::*;
use crate::CompressionAlgo;
use std::sync::{Arc, Mutex};

fn block(id: usize) -> CompressedBlock {
    CompressedBlock::new(id, vec![id as u8], CompressionAlgo::Lz4, 1)
}

#[test]
fn worker_results_are_forwarded_in_order() {
    let (writer_tx, writer_rx) = bounded::<CompressedBlock>(4);
    let writer_failure = Arc::new(Mutex::new(None::<String>));
    let mut pending_results = ReorderBuffer::with_limit(4);
    let mut completed_bytes = 0u64;
    let mut first_error = None;
    let mut raw_passthrough_blocks = 0u64;
    let mut writer_queue_peak = 0usize;
    let mut writer_enqueue_blocked = Duration::ZERO;
    let mut retired_count = 0usize;
    let mut received_count = 0usize;

    let mut state = DirectoryWriterState {
        writer_tx: &writer_tx,
        writer_failure: &writer_failure,
        pending_results: &mut pending_results,
        completed_bytes: &mut completed_bytes,
        first_error: &mut first_error,
        raw_passthrough_blocks: &mut raw_passthrough_blocks,
        writer_queue_peak: &mut writer_queue_peak,
        writer_enqueue_blocked: &mut writer_enqueue_blocked,
        retired_count: &mut retired_count,
        received_count: &mut received_count,
    };
    record_result_to_writer_queue(Ok(block(2)), &mut state);
    record_result_to_writer_queue(Ok(block(1)), &mut state);
    record_result_to_writer_queue(Ok(block(0)), &mut state);

    assert!(first_error.is_none());
    assert_eq!(pending_results.pending_len(), 0);
    assert_eq!(retired_count, 3);
    assert_eq!(completed_bytes, 3);
    assert_eq!(writer_rx.recv().unwrap().id, 0);
    assert_eq!(writer_rx.recv().unwrap().id, 1);
    assert_eq!(writer_rx.recv().unwrap().id, 2);
}

#[test]
fn writer_failure_releases_pending_results_and_surfaces_cause() {
    let (writer_tx, writer_rx) = bounded::<CompressedBlock>(1);
    let writer_failure = Arc::new(Mutex::new(Some("I/O error: disk full".to_string())));
    let mut pending_results = ReorderBuffer::with_limit(4);
    let mut completed_bytes = 0u64;
    let mut first_error = None;
    let mut raw_passthrough_blocks = 0u64;
    let mut writer_queue_peak = 0usize;
    let mut writer_enqueue_blocked = Duration::ZERO;
    let mut retired_count = 0usize;
    let mut received_count = 0usize;

    let mut state = DirectoryWriterState {
        writer_tx: &writer_tx,
        writer_failure: &writer_failure,
        pending_results: &mut pending_results,
        completed_bytes: &mut completed_bytes,
        first_error: &mut first_error,
        raw_passthrough_blocks: &mut raw_passthrough_blocks,
        writer_queue_peak: &mut writer_queue_peak,
        writer_enqueue_blocked: &mut writer_enqueue_blocked,
        retired_count: &mut retired_count,
        received_count: &mut received_count,
    };
    record_result_to_writer_queue(Ok(block(2)), &mut state);

    drop(writer_rx);

    record_result_to_writer_queue(Ok(block(0)), &mut state);

    assert_eq!(pending_results.pending_len(), 0);
    assert_eq!(retired_count, 2);
    assert!(matches!(
        first_error,
        Some(crate::OxideError::CompressionError(message)) if message == "I/O error: disk full"
    ));
}

#[test]
fn submission_gate_allows_worker_refill_while_writer_catches_up() {
    assert!(can_submit_more_work(8, 6, 4, 8, 4));
}

#[test]
fn submission_gate_stops_when_post_worker_backlog_is_full() {
    assert!(!can_submit_more_work(8, 6, 2, 8, 4));
}

#[test]
fn submission_drain_budget_scales_with_pipeline_but_stays_bounded() {
    assert_eq!(submission_drain_budget(32, 8), 32);
    assert_eq!(submission_drain_budget(512, 64), 128);
    assert_eq!(submission_drain_budget(512, 512), 256);
}
