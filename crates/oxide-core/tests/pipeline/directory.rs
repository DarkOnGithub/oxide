use super::{
    detect_file_probe_plans, discover_directory_tree, estimate_directory_block_count, FileProbePlan,
};
use crate::{ChunkEncodingPlan, CompressionAlgo};
use std::fs;
use tempfile::tempdir;

#[cfg(unix)]
use std::os::unix::fs::symlink;

#[test]
fn discovery_collects_sizes_in_one_pass() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let nested = root.join("nested");
    fs::create_dir(&nested).expect("create nested dir");
    fs::write(root.join("notes.txt"), b"hello\nworld\n").expect("write text");
    fs::write(nested.join("payload.bin"), [0, 159, 146, 150]).expect("write binary");

    let discovery = discover_directory_tree(root).expect("discover directory");

    assert_eq!(discovery.directories.len(), 1);
    assert_eq!(discovery.directories[0].rel_path, "nested");
    assert_eq!(discovery.symlinks.len(), 0);
    assert_eq!(discovery.files.len(), 2);
    assert_eq!(discovery.files[0].entry.rel_path, "nested/payload.bin");
    assert_eq!(discovery.files[1].entry.rel_path, "notes.txt");
    assert_eq!(discovery.input_bytes_total, 16);
}

#[test]
fn discovery_includes_hidden_files_and_directories() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let hidden_dir = root.join(".hidden");
    let nested_hidden_dir = hidden_dir.join("nested");

    fs::create_dir(&hidden_dir).expect("create hidden dir");
    fs::create_dir(&nested_hidden_dir).expect("create nested hidden dir");
    fs::write(root.join(".root-hidden.txt"), b"root").expect("write root hidden file");
    fs::write(hidden_dir.join("visible.txt"), b"hidden-dir").expect("write hidden dir file");
    fs::write(nested_hidden_dir.join(".nested-hidden.txt"), b"nested")
        .expect("write nested hidden file");

    let discovery = discover_directory_tree(root).expect("discover directory");

    assert_eq!(
        discovery
            .directories
            .iter()
            .map(|entry| entry.rel_path.as_str())
            .collect::<Vec<_>>(),
        vec![".hidden", ".hidden/nested"]
    );
    assert_eq!(
        discovery
            .files
            .iter()
            .map(|entry| entry.entry.rel_path.as_str())
            .collect::<Vec<_>>(),
        vec![
            ".hidden/nested/.nested-hidden.txt",
            ".hidden/visible.txt",
            ".root-hidden.txt",
        ]
    );
    assert_eq!(discovery.input_bytes_total, 4 + 10 + 6);
}

#[test]
fn discovery_groups_compressible_files_before_raw_candidates() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();

    fs::write(
        root.join("01-photo.jpg"),
        [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10],
    )
    .expect("write jpeg");
    fs::write(root.join("02-notes.txt"), b"alpha alpha alpha\n").expect("write text");
    fs::write(root.join("03-script.js"), b"console.log('hello');\n").expect("write script");

    let discovery = discover_directory_tree(root).expect("discover directory");

    assert_eq!(
        discovery
            .files
            .iter()
            .map(|entry| entry.entry.rel_path.as_str())
            .collect::<Vec<_>>(),
        vec!["03-script.js", "02-notes.txt", "01-photo.jpg"]
    );
}

#[cfg(unix)]
#[test]
fn discovery_preserves_symlinks_without_following_them() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let nested = root.join("nested");
    fs::create_dir(&nested).expect("create nested dir");
    symlink(&nested, root.join("nested-link")).expect("create symlink");

    let discovery = discover_directory_tree(root).expect("discover directory");

    assert_eq!(discovery.directories.len(), 1);
    assert_eq!(discovery.symlinks.len(), 1);
    assert_eq!(discovery.symlinks[0].entry.rel_path, "nested-link");
    assert_eq!(
        discovery.symlinks[0].target,
        nested.to_string_lossy().as_ref()
    );
    assert_eq!(discovery.files.len(), 0);
}

#[test]
fn probe_plan_uses_extension_based_raw_storage() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    fs::write(root.join("photo.jpg"), [0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]).expect("write jpeg");

    let discovery = discover_directory_tree(root).expect("discover directory");
    let plans = detect_file_probe_plans(
        &discovery,
        16,
        ChunkEncodingPlan::new(CompressionAlgo::Zstd),
        1,
    )
    .expect("probe plans");

    assert_eq!(plans.len(), 1);
    assert!(plans[0].force_raw_storage);
}

#[test]
fn probe_plan_marks_compressible_files_as_non_raw_storage() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    fs::write(root.join("alpha.txt"), b"alpha alpha alpha alpha\n").expect("write alpha");

    let discovery = discover_directory_tree(root).expect("discover directory");
    let plans = detect_file_probe_plans(
        &discovery,
        128,
        ChunkEncodingPlan::new(CompressionAlgo::Zstd),
        1,
    )
    .expect("probe plans");

    assert_eq!(plans.len(), 1);
    assert!(!plans[0].force_raw_storage);
}

#[test]
fn block_count_uses_fixed_size_blocks_without_extension_boundaries() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    fs::write(root.join("alpha.txt"), b"alpha\n").expect("write alpha");
    fs::write(root.join("beta.bin"), [0, 159, 146, 150, 42]).expect("write beta");

    let discovery = discover_directory_tree(root).expect("discover directory");
    let block_count = estimate_directory_block_count(
        &discovery,
        &[
            FileProbePlan {
                force_raw_storage: false,
            },
            FileProbePlan {
                force_raw_storage: false,
            },
        ],
        4,
        ChunkEncodingPlan::new(CompressionAlgo::Lz4),
    )
    .expect("plan");

    assert_eq!(block_count, 3);
}

#[test]
fn block_count_accounts_for_raw_storage_policy_changes() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    fs::write(root.join("first.txt"), b"aaaaaa").expect("write first");
    fs::write(root.join("second.jpg"), b"bbbbbb").expect("write second");

    let discovery = discover_directory_tree(root).expect("discover directory");
    let block_count = estimate_directory_block_count(
        &discovery,
        &[
            FileProbePlan {
                force_raw_storage: false,
            },
            FileProbePlan {
                force_raw_storage: true,
            },
        ],
        8,
        ChunkEncodingPlan::new(CompressionAlgo::Lz4),
    )
    .expect("plan");

    assert_eq!(block_count, 2);
}

#[test]
fn block_count_does_not_merge_past_fixed_target_when_limit_is_larger() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    fs::write(root.join("01.txt"), b"aaaa").expect("write first");
    fs::write(root.join("02.txt"), b"bbbb").expect("write second");
    fs::write(root.join("03.txt"), b"cccc").expect("write third");

    let discovery = discover_directory_tree(root).expect("discover directory");
    let block_count = estimate_directory_block_count(
        &discovery,
        &[
            FileProbePlan {
                force_raw_storage: false,
            },
            FileProbePlan {
                force_raw_storage: false,
            },
            FileProbePlan {
                force_raw_storage: false,
            },
        ],
        8,
        ChunkEncodingPlan::new(CompressionAlgo::Lz4),
    )
    .expect("plan");

    assert_eq!(block_count, 2);
}

#[test]
fn block_count_keeps_zstd_batches_merged_across_extension_changes() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    fs::write(root.join("a.json"), b"aaaa").expect("write first");
    fs::write(root.join("b.html"), b"bbbb").expect("write second");

    let discovery = discover_directory_tree(root).expect("discover directory");
    let block_count = estimate_directory_block_count(
        &discovery,
        &[
            FileProbePlan {
                force_raw_storage: false,
            },
            FileProbePlan {
                force_raw_storage: false,
            },
        ],
        8,
        ChunkEncodingPlan::new(CompressionAlgo::Zstd),
    )
    .expect("plan");

    assert_eq!(block_count, 1);
}
