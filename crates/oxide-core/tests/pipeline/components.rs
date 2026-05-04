use oxide_core::ChunkingPolicy;
use oxide_core::MmapInput;
use oxide_core::pipeline::{
    PipelinePerformanceOptions,
    archive::ArchivePipeline,
    directory::{BlockCountPlanner, DirectoryBatchSubmitter},
};
use oxide_core::types::{BatchData, ChunkEncodingPlan, CompressionAlgo};
use std::path::PathBuf;
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
}

mod directory_tests {
    use super::*;

    #[test]
    fn submitter_batches_fixed_size_chunks() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8);
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/data.txt", b"aaaaaabbbbbb", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 8);
        assert_eq!(batches[1].len(), 4);
        assert!(!batches[0].force_raw_storage);
        assert!(!batches[1].force_raw_storage);
    }

    #[test]
    fn submitter_flushes_on_raw_storage_policy_change() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8);
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/a.txt", b"aaaaaa", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("first push should succeed");
        submitter
            .push_bytes("root/b.txt", b"bbbbbb", true, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("second push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 6);
        assert!(!batches[0].force_raw_storage);
        assert_eq!(batches[1].len(), 6);
        assert!(batches[1].force_raw_storage);
    }

    #[test]
    fn block_count_planner_flushes_on_raw_storage_policy_change() {
        let mut planner = BlockCountPlanner::new(8);
        planner.push_len("root/a.txt", 6, false);
        planner.push_len("root/b.jpg", 6, true);

        assert_eq!(planner.finish(), 2);
    }

    #[test]
    fn block_count_planner_keeps_zstd_batches_merged_across_extensions() {
        let mut planner =
            BlockCountPlanner::new_with_plan(8, ChunkEncodingPlan::new(CompressionAlgo::Zstd));
        planner.push_len("root/a.json", 4, false);
        planner.push_len("root/b.html", 4, false);

        assert_eq!(planner.finish(), 1);
    }

    #[test]
    fn submitter_uses_mapped_batches_for_full_mmap_sized_blocks() {
        let temp = tempdir().expect("tempdir");
        let file_path = temp.path().join("payload.bin");
        std::fs::write(&file_path, b"abcdefghij").expect("write payload");

        let mmap = MmapInput::open(&file_path).expect("open mmap");
        let map = mmap.mapping().expect("mapped file");
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 4);
        let mut batches = Vec::new();

        submitter
            .push_mapped(&file_path, map, 0, mmap.len(), false, |batch| {
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
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 4);
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/prefix.bin", b"ab", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("owned push should succeed");
        submitter
            .push_mapped(&file_path, map, 0, mmap.len(), false, |batch| {
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

    #[test]
    fn submitter_merges_same_policy_bytes_even_when_paths_change_for_lz4() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 8);
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/a.json", b"aaaa", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("first push should succeed");
        submitter
            .push_bytes("root/b.html", b"bbbb", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("second push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].source_path, PathBuf::from("root/a.json"));
        assert_eq!(batches[0].data.as_slice(), b"aaaabbbb");
        assert_eq!(batches[0].stream_id, 1);
    }

    #[test]
    fn submitter_keeps_zstd_batches_merged_and_falls_back_to_root_source_path() {
        let mut submitter = DirectoryBatchSubmitter::new_with_plan(
            PathBuf::from("root"),
            8,
            ChunkEncodingPlan::new(CompressionAlgo::Zstd),
        );
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/a.json", b"aaaa", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("first push should succeed");
        submitter
            .push_bytes("root/b.html", b"bbbb", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("second push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].source_path, PathBuf::from("root"));
        assert_eq!(batches[0].data.as_slice(), b"aaaabbbb");
        assert_eq!(batches[0].stream_id, 1);
    }

    #[test]
    fn submitter_assigns_distinct_stream_ids_per_source_group() {
        let mut submitter = DirectoryBatchSubmitter::new(PathBuf::from("root"), 4);
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/a.txt", b"aaaa", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("first push should succeed");
        submitter
            .push_bytes("root/b.txt", b"bbbb", true, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("second push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(batches.len(), 2);
        assert_ne!(batches[0].stream_id, 0);
        assert_ne!(batches[1].stream_id, 0);
        assert_ne!(batches[0].stream_id, batches[1].stream_id);
    }

    #[test]
    fn submitter_applies_cdc_across_file_boundaries() {
        let policy = ChunkingPolicy::cdc(16, 8, 32);
        let mut submitter = DirectoryBatchSubmitter::new_with_policy(PathBuf::from("root"), policy);
        let mut batches = Vec::new();

        submitter
            .push_bytes("root/a.bin", b"abcdefgh", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("first push should succeed");
        submitter
            .push_bytes("root/b.bin", b"ijklmnopqrstuvwx", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("second push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert!(batches.len() >= 1);
        assert_eq!(batches.iter().map(|batch| batch.len()).sum::<usize>(), 24);
        assert_eq!(batches[0].data.as_slice()[..8], *b"abcdefgh");
    }

    #[test]
    fn block_count_planner_matches_cdc_submitter_boundaries() {
        let policy = ChunkingPolicy::cdc(16, 8, 32);
        let mut planner = BlockCountPlanner::new_with_policy(policy);
        planner.push_bytes("root/a.bin", b"abcdefgh", false);
        planner.push_bytes("root/b.bin", b"ijklmnopqrstuvwx", false);

        let mut submitter = DirectoryBatchSubmitter::new_with_policy(PathBuf::from("root"), policy);
        let mut batches = Vec::new();
        submitter
            .push_bytes("root/a.bin", b"abcdefgh", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("first push should succeed");
        submitter
            .push_bytes("root/b.bin", b"ijklmnopqrstuvwx", false, |batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("second push should succeed");
        submitter
            .finish(|batch| {
                batches.push(batch);
                Ok(())
            })
            .expect("finish should succeed");

        assert_eq!(planner.finish(), batches.len());
    }
}
