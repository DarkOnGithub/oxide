use std::path::PathBuf;

use oxide_core::MmapInput;
use oxide_core::pipeline::{
    PipelinePerformanceOptions,
    archive::ArchivePipeline,
    directory::{BlockCountPlanner, DirectoryBatchSubmitter},
};
use oxide_core::types::{BatchData, FileFormat};
use tempfile::tempdir;

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

    #[test]
    fn submitter_uses_mapped_batches_for_full_mmap_sized_blocks() {
        let temp = tempdir().expect("tempdir");
        let file_path = temp.path().join("payload.bin");
        std::fs::write(&file_path, b"abcdefghij").expect("write payload");

        let mmap = MmapInput::open(&file_path).expect("open mmap");
        let map = mmap.mapping().expect("mapped file");
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 4, false);
        let mut batches = Vec::new();

        submitter
            .push_mapped_with_hint(map, 0, mmap.len(), FileFormat::Binary, false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("mapped push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 3);
        assert!(matches!(batches[0].data, BatchData::Mapped { .. }));
        assert!(matches!(batches[1].data, BatchData::Mapped { .. }));
        assert!(matches!(batches[2].data, BatchData::Owned(_)));
        assert_eq!(batches[0].data.as_slice(), b"abcd");
        assert_eq!(batches[1].data.as_slice(), b"efgh");
        assert_eq!(batches[2].data.as_slice(), b"ij");
    }

    #[test]
    fn submitter_splices_pending_owned_bytes_before_switching_to_mapped_blocks() {
        let temp = tempdir().expect("tempdir");
        let file_path = temp.path().join("payload.bin");
        std::fs::write(&file_path, b"cdefgh").expect("write payload");

        let mmap = MmapInput::open(&file_path).expect("open mmap");
        let map = mmap.mapping().expect("mapped file");
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 4, false);
        let mut batches = Vec::new();

        submitter
            .push_bytes_with_hint(b"ab", FileFormat::Binary, false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("owned push should succeed");
        submitter
            .push_mapped_with_hint(map, 0, mmap.len(), FileFormat::Binary, false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("mapped push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert!(matches!(batches[0].data, BatchData::Owned(_)));
        assert!(matches!(batches[1].data, BatchData::Mapped { .. }));
        assert_eq!(batches[0].data.as_slice(), b"abcd");
        assert_eq!(batches[1].data.as_slice(), b"efgh");
    }
}
