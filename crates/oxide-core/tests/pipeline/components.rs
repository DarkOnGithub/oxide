use std::path::PathBuf;

use oxide_core::pipeline::{
    PipelinePerformanceOptions,
    archive::ArchivePipeline,
    directory::{BlockCountPlanner, DirectoryBatchSubmitter},
};
use oxide_core::types::FileFormat;

mod archive_tests {
    use super::*;

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
    fn worker_scaling_does_not_clamp_cli_sized_byte_budget() {
        let mut performance = PipelinePerformanceOptions::default();
        performance.max_inflight_bytes = 2 * 1024 * 1024 * 1024;

        let inflight = ArchivePipeline::max_inflight_blocks(10_000, 16, 1024 * 1024, &performance);

        assert_eq!(inflight, 2048);
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
}

mod directory_tests {
    use super::*;

    #[test]
    fn submitter_flushes_on_format_change_when_preserve_boundaries_enabled() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8, true);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Binary, false, |batch| {
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
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Binary, false, |batch| {
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
        preserving.push_len(4, FileFormat::Text, false);
        preserving.push_len(4, FileFormat::Binary, false);
        preserving.push_len(4, FileFormat::Text, false);
        assert_eq!(preserving.finish(), 3);

        let mut merging = BlockCountPlanner::new(8, false);
        merging.push_len(4, FileFormat::Text, false);
        merging.push_len(4, FileFormat::Binary, false);
        merging.push_len(4, FileFormat::Text, false);
        assert_eq!(merging.finish(), 2);
    }

    #[test]
    fn submitter_flushes_on_raw_storage_policy_change() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8, false);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"aaaaaa", FileFormat::Text, false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("text push should succeed");
        submitter
            .push_bytes_with_hint(b"bbbbbb", FileFormat::Common, true, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("raw push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert!(!batches[0].force_raw_storage);
        assert_eq!(batches[0].len(), 6);
        assert!(batches[1].force_raw_storage);
        assert_eq!(batches[1].len(), 6);
    }

    #[test]
    fn block_count_planner_flushes_on_raw_storage_policy_change() {
        let mut planner = BlockCountPlanner::new(8, false);
        planner.push_len(6, FileFormat::Text, false);
        planner.push_len(6, FileFormat::Common, true);

        assert_eq!(planner.finish(), 2);
    }
}
