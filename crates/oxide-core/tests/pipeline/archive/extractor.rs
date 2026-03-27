use super::{
    MIN_DECODE_QUEUE_CAPACITY, MIN_ORDERED_WRITE_QUEUE_CAPACITY, decode_queue_capacity,
    ordered_write_queue_capacity, reorder_pending_limit,
};

#[test]
fn extract_queue_capacity_respects_minimums_and_block_count() {
    assert_eq!(decode_queue_capacity(1, 1), 1);
    assert_eq!(decode_queue_capacity(1, 4), 4);
    assert_eq!(decode_queue_capacity(1, 64), MIN_DECODE_QUEUE_CAPACITY);
}

#[test]
fn ordered_write_queue_is_at_least_decode_queue_and_bounded() {
    let decode = decode_queue_capacity(2, 64);
    let ordered = ordered_write_queue_capacity(2, decode, 64);

    assert!(ordered >= decode);
    assert!(ordered >= MIN_ORDERED_WRITE_QUEUE_CAPACITY);
    assert!(ordered <= 64);
}

#[test]
fn reorder_limit_is_derived_from_ordered_queue_and_capped_by_blocks() {
    assert_eq!(reorder_pending_limit(16, 128), 128);
    assert_eq!(reorder_pending_limit(64, 96), 96);
}
