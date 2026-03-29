use super::{BlockDedupIndex, DedupDecision};
use crate::Batch;
use bytes::Bytes;

#[test]
fn block_dedup_index_references_prior_matching_blocks() {
    let mut dedup = BlockDedupIndex::new();
    let first = Batch::new(0, "first.bin", Bytes::from_static(b"abcd"));
    let duplicate = Batch::new(1, "second.bin", Bytes::from_static(b"abcd"));
    let unique = Batch::new(2, "third.bin", Bytes::from_static(b"abce"));

    assert_eq!(
        dedup.classify(&first).expect("first classify"),
        DedupDecision::Unique
    );
    assert_eq!(
        dedup.classify(&duplicate).expect("duplicate classify"),
        DedupDecision::Reference(0)
    );
    assert_eq!(
        dedup.classify(&unique).expect("unique classify"),
        DedupDecision::Unique
    );
}
