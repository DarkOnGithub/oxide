use std::path::PathBuf;
use std::sync::Arc;

use oxide_core::BufferPool;
use oxide_core::pipeline::{
    ArchivePipelineConfig, PipelinePerformanceOptions,
    archive::ArchivePipeline,
    directory::{BlockCountPlanner, DirectoryBatchSubmitter},
};
use oxide_core::types::{CompressionAlgo, FileFormat};

mod archive_tests {
    use super::*;

    fn pseudo_random_bytes(len: usize) -> Vec<u8> {
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            out.push((state >> 56) as u8);
        }
        out
    }

    fn build_pipeline_for_score(raw_fallback_enabled: bool) -> ArchivePipeline {
        let mut config = ArchivePipelineConfig::new(
            8 * 1024,
            1,
            Arc::new(BufferPool::new(16 * 1024, 16)),
            CompressionAlgo::Lz4,
        );
        config.performance.raw_fallback_enabled = raw_fallback_enabled;
        ArchivePipeline::new(config)
    }

    #[test]
    fn inflight_window_is_limited_by_byte_budget() {
        let mut performance = PipelinePerformanceOptions::default();
        performance.max_inflight_blocks_per_worker = 32;
        performance.max_inflight_bytes = 16 * 1024 * 1024;

        let inflight = ArchivePipeline::max_inflight_blocks(10_000, 16, 1024 * 1024, &performance);

        assert_eq!(inflight, 16);
    }

    #[test]
    fn inflight_window_never_exceeds_total_blocks() {
        let mut performance = PipelinePerformanceOptions::default();
        performance.max_inflight_blocks_per_worker = 64;
        performance.max_inflight_bytes = 2 * 1024 * 1024 * 1024;

        let inflight = ArchivePipeline::max_inflight_blocks(7, 16, 256 * 1024, &performance);

        assert_eq!(inflight, 7);
    }

    #[test]
    fn select_stored_payload_uses_raw_when_compression_is_not_smaller() {
        let source = [1u8, 2, 3, 4];
        let compressed_equal = [9u8, 9, 9, 9];
        let compressed_larger = [9u8, 9, 9, 9, 9];

        let (stored_equal, raw_equal) =
            ArchivePipeline::select_stored_payload(&source, &compressed_equal, true);
        assert!(raw_equal);
        assert_eq!(stored_equal, source.as_slice());

        let (stored_larger, raw_larger) =
            ArchivePipeline::select_stored_payload(&source, &compressed_larger, true);
        assert!(raw_larger);
        assert_eq!(stored_larger, source.as_slice());
    }

    #[test]
    fn score_block_size_respects_raw_fallback_setting() {
        let sample = pseudo_random_bytes(64 * 1024);

        let score_with_fallback = build_pipeline_for_score(true)
            .score_block_size(&sample, 8 * 1024)
            .expect("score with fallback");
        let score_without_fallback = build_pipeline_for_score(false)
            .score_block_size(&sample, 8 * 1024)
            .expect("score without fallback");

        assert!(score_without_fallback.output_bytes > sample.len());
        assert!(score_with_fallback.output_bytes <= sample.len());
        assert!(score_with_fallback.output_bytes <= score_without_fallback.output_bytes);
    }
}

mod directory_tests {
    use super::*;

    #[test]
    fn submitter_flushes_on_format_change_when_preserve_boundaries_enabled() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8, true);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Binary, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("binary push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].file_type_hint, FileFormat::Text);
        assert_eq!(batches[0].len(), 6);
        assert_eq!(batches[1].file_type_hint, FileFormat::Binary);
        assert_eq!(batches[1].len(), 6);
    }

    #[test]
    fn submitter_merges_formats_when_preserve_boundaries_disabled() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8, false);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Binary, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("binary push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].file_type_hint, FileFormat::Common);
        assert_eq!(batches[0].len(), 8);
        assert_eq!(batches[1].file_type_hint, FileFormat::Binary);
        assert_eq!(batches[1].len(), 4);
    }

    #[test]
    fn block_count_planner_respects_boundary_toggle() {
        let mut preserving = BlockCountPlanner::new(8, true);
        preserving.push_len(4, FileFormat::Text);
        preserving.push_len(4, FileFormat::Binary);
        preserving.push_len(4, FileFormat::Text);
        assert_eq!(preserving.finish(), 3);

        let mut merging = BlockCountPlanner::new(8, false);
        merging.push_len(4, FileFormat::Text);
        merging.push_len(4, FileFormat::Binary);
        merging.push_len(4, FileFormat::Text);
        assert_eq!(merging.finish(), 2);
    }
}
