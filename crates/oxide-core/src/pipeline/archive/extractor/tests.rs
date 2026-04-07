use super::*;
use crate::{CompressionAlgo, PipelinePerformanceOptions};

#[test]
fn extract_block_profile_uses_selected_blocks_only() {
    let headers = vec![
        ChunkDescriptor::new(0, 1024, 128, CompressionAlgo::Lz4, 0),
        ChunkDescriptor::new(128, 4096, 512, CompressionAlgo::Lz4, 0),
        ChunkDescriptor::new(640, 2048, 256, CompressionAlgo::Lz4, 0),
    ];
    let plan = DecodePlan {
        selected_blocks: vec![true, false, true],
        block_count: 2,
    };

    let profile = ExtractBlockProfile::from_plan(&headers, &plan);

    assert_eq!(
        profile,
        ExtractBlockProfile {
            max_encoded_len: 256,
            max_raw_len: 2048,
            queue_block_bytes_p95: 2048,
        }
    );
    assert_eq!(profile.max_queue_block_bytes(), 2048);
    assert_eq!(profile.budgeted_queue_block_bytes(), 2048);
}

#[test]
fn extract_queue_budget_uses_p95_instead_of_max_outlier() {
    let mut headers = Vec::new();
    for index in 0..19u64 {
        headers.push(ChunkDescriptor::new(
            index * 1024,
            1024,
            1024,
            CompressionAlgo::Lz4,
            0,
        ));
    }
    headers.push(ChunkDescriptor::new(
        19 * 1024,
        8 * 1024 * 1024,
        8 * 1024 * 1024,
        CompressionAlgo::Lz4,
        0,
    ));

    let plan = DecodePlan::all(headers.len());
    let profile = ExtractBlockProfile::from_plan(&headers, &plan);

    assert_eq!(profile.max_queue_block_bytes(), 8 * 1024 * 1024);
    assert_eq!(profile.budgeted_queue_block_bytes(), 1024);
}

#[test]
fn extract_inflight_limit_respects_byte_budget() {
    let mut performance = PipelinePerformanceOptions::default();
    performance.max_inflight_bytes = 16 * 1024 * 1024;

    let inflight = extract_inflight_block_limit(10_000, 1024 * 1024, &performance);

    assert_eq!(inflight, 16);
}

#[test]
fn ordered_write_queue_expands_into_inflight_headroom_for_uniform_blocks() {
    let profile = ExtractBlockProfile {
        max_encoded_len: 1024 * 1024,
        max_raw_len: 1024 * 1024,
        queue_block_bytes_p95: 1024 * 1024,
    };
    let queue = decode_queue_capacity(16, 10_000, 256);
    let ordered = ordered_write_queue_capacity(16, queue, 10_000, 256, profile);

    assert_eq!(queue, 64);
    assert_eq!(ordered, 208);
}

#[test]
fn ordered_write_queue_stays_worker_bounded_for_skewed_blocks() {
    let profile = ExtractBlockProfile {
        max_encoded_len: 8 * 1024 * 1024,
        max_raw_len: 8 * 1024 * 1024,
        queue_block_bytes_p95: 1024 * 1024,
    };
    let queue = decode_queue_capacity(16, 10_000, 256);
    let ordered = ordered_write_queue_capacity(16, queue, 10_000, 256, profile);

    assert_eq!(queue, 64);
    assert_eq!(ordered, 128);
}

#[test]
fn ordered_write_headroom_share_steps_down_at_skew_thresholds() {
    let low_skew = ExtractBlockProfile {
        max_encoded_len: 2 * 1024,
        max_raw_len: 2 * 1024,
        queue_block_bytes_p95: 1024,
    };
    let medium_skew = ExtractBlockProfile {
        max_encoded_len: 4 * 1024,
        max_raw_len: 4 * 1024,
        queue_block_bytes_p95: 1024,
    };
    let high_skew = ExtractBlockProfile {
        max_encoded_len: 5 * 1024,
        max_raw_len: 5 * 1024,
        queue_block_bytes_p95: 1024,
    };

    assert_eq!(ordered_write_headroom_share_numerator(low_skew), 3);
    assert_eq!(ordered_write_headroom_share_numerator(medium_skew), 2);
    assert_eq!(ordered_write_headroom_share_numerator(high_skew), 1);
}

#[test]
fn extract_queue_capacity_is_limited_by_byte_budget() {
    let queue = decode_queue_capacity(16, 10_000, 12);
    let ordered = ordered_write_queue_capacity(
        16,
        queue,
        10_000,
        12,
        ExtractBlockProfile {
            max_encoded_len: 1024,
            max_raw_len: 1024,
            queue_block_bytes_p95: 1024,
        },
    );
    let reorder = reorder_pending_limit(ordered, 10_000, 12);

    assert_eq!(queue, 12);
    assert_eq!(ordered, 12);
    assert_eq!(reorder, 12);
}

#[test]
fn extract_buffer_pool_reuses_base_pool_when_capacity_is_sufficient() {
    let base = Arc::new(BufferPool::new(1024, 8));
    let selected = select_extract_buffer_pool(&base, 512);

    assert!(Arc::ptr_eq(&base, &selected));
}

#[test]
fn extract_buffer_pool_grows_to_match_archive_profile() {
    let base = Arc::new(BufferPool::new(1024, 8));
    let selected = select_extract_buffer_pool(&base, 4096);

    assert!(!Arc::ptr_eq(&base, &selected));
    assert_eq!(selected.default_capacity(), 4096);
    assert_eq!(selected.max_buffers(), base.max_buffers());
}
