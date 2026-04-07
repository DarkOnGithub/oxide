use std::path::{Path, PathBuf};

use super::*;
use crate::{ArchiveListingEntry, ArchiveTimestamp, OxideError};

fn file_entry(path: impl Into<String>, size: u64, content_offset: u64) -> ArchiveListingEntry {
    ArchiveListingEntry::file(
        path.into(),
        size,
        0o644,
        ArchiveTimestamp::default(),
        0,
        0,
        content_offset,
    )
}

fn file_work_item(index: usize, path: PathBuf) -> PreparedRestoreWorkItem {
    PreparedRestoreWorkItem {
        index,
        restore_entry: RestoreEntry {
            path,
            entry: ArchiveListingEntry::file(
                format!("file-{index}.bin"),
                8,
                0o644,
                ArchiveTimestamp::default(),
                0,
                0,
                0,
            ),
        },
        reserved_open_permit: None,
        permit_wait_started: None,
        active_file_counted: false,
    }
}

#[test]
fn prepared_entry_windows_scale_with_parallelism() {
    let config = prepared_entry_window_config_for_counts(10_000, 10_000, 12);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 288,
            ready_entry_window_capacity: 72,
        }
    );
}

#[test]
fn prepared_entry_windows_adapt_for_file_heavy_workloads() {
    let config = prepared_entry_window_config_for_counts(10_000, 5_000, 16);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 288,
            ready_entry_window_capacity: 96,
        }
    );
}

#[test]
fn prepared_entry_windows_do_not_boost_sparse_file_workloads() {
    let config = prepared_entry_window_config_for_counts(10_000, 1_000, 16);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 192,
            ready_entry_window_capacity: 96,
        }
    );
}

#[test]
fn prepared_entry_windows_only_boost_when_density_and_backlog_align() {
    let config = prepared_entry_window_config_for_counts(1_000, 240, 16);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 192,
            ready_entry_window_capacity: 96,
        }
    );
}

#[test]
fn prepared_entry_windows_respect_safe_upper_clamp() {
    let config = prepared_entry_window_config_for_counts(10_000, 10_000, 64);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 384,
            ready_entry_window_capacity: 96,
        }
    );
}

#[test]
fn prepared_entry_windows_are_capped_by_workload() {
    let config = prepared_entry_window_config_for_counts(12, 5, 32);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 5,
            ready_entry_window_capacity: 12,
        }
    );
}

#[test]
fn prepared_entry_windows_respect_minimums_for_large_workloads() {
    let config = prepared_entry_window_config_for_counts(10_000, 10_000, 1);

    assert_eq!(
        config,
        PreparedEntryWindowConfig {
            preopened_file_window_capacity: 32,
            ready_entry_window_capacity: MIN_READY_ENTRY_WINDOW_CAPACITY,
        }
    );
}

#[test]
fn restore_multiple_files_across_chunk_boundaries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manifest = ArchiveManifest::new(vec![
        file_entry("nested/first.txt", 4, 0),
        file_entry("nested/second.txt", 4, 4),
    ]);

    let mut writer = DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
    writer.write_chunk(b"te").expect("write first chunk");
    writer.write_chunk(b"stda").expect("write second chunk");
    writer.write_chunk(b"ta").expect("write third chunk");
    writer.finish().expect("finish restore");

    assert_eq!(
        std::fs::read(temp.path().join("nested/first.txt")).expect("read first"),
        b"test"
    );
    assert_eq!(
        std::fs::read(temp.path().join("nested/second.txt")).expect("read second"),
        b"data"
    );
}

#[test]
fn restore_finishes_manifests_with_only_empty_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manifest = ArchiveManifest::new(vec![
        ArchiveListingEntry::directory(
            "nested".to_string(),
            0o755,
            ArchiveTimestamp::default(),
            0,
            0,
        ),
        file_entry("nested/empty.txt", 0, 0),
    ]);

    let mut writer = DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
    let stats = writer.finish().expect("finish restore");

    assert!(temp.path().join("nested").is_dir());
    assert_eq!(
        std::fs::metadata(temp.path().join("nested/empty.txt"))
            .expect("empty file metadata")
            .len(),
        0
    );
    assert_eq!(stats.entry_count, 2);
}

#[cfg(unix)]
#[test]
fn restore_finishes_manifests_with_symlink_entries_only() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manifest = ArchiveManifest::new(vec![ArchiveListingEntry {
        path: "link".to_string(),
        kind: crate::ArchiveEntryKind::Symlink,
        target: Some("target.txt".to_string()),
        size: 0,
        mode: 0o777,
        mtime: ArchiveTimestamp::default(),
        uid: 0,
        gid: 0,
        content_offset: 0,
    }]);

    let mut writer = DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
    let stats = writer.finish().expect("finish restore");

    assert_eq!(
        std::fs::read_link(temp.path().join("link")).expect("read link"),
        Path::new("target.txt")
    );
    assert_eq!(stats.entry_count, 1);
}

#[test]
fn prepare_restore_entry_with_open_returns_file_ready() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("data.bin");
    let restore_entry = RestoreEntry {
        path: path.clone(),
        entry: ArchiveListingEntry::file(
            "data.bin".to_string(),
            8,
            0o644,
            ArchiveTimestamp::default(),
            0,
            0,
            0,
        ),
    };
    let (permit_tx, permit_rx) = bounded(1);
    permit_tx.send(()).expect("seed permit");

    let prepared = prepare_restore_entry_with_open(restore_entry, &permit_rx, &permit_tx)
        .expect("file should prepare");

    match prepared {
        PreparedRestoreEntry::FileReady {
            path: prepared_path,
            writer,
            permit,
            ..
        } => {
            assert_eq!(prepared_path, path);
            assert!(path.exists());
            let _file = writer.into_inner().expect("writer should flush");
            drop(permit);
        }
        other => panic!("expected FileReady, got {other:?}"),
    }
}

#[test]
fn work_items_reserve_open_permits_in_order() {
    let temp = tempfile::tempdir().expect("tempdir");
    let (permit_tx, permit_rx) = bounded(1);
    permit_tx.send(()).expect("seed permit");

    let mut first = file_work_item(0, temp.path().join("first.bin"));
    let mut second = file_work_item(1, temp.path().join("second.bin"));

    assert!(
        try_reserve_open_file_permit_for_work_item(&mut first, &permit_rx, &permit_tx)
            .expect("first reserve")
    );
    assert!(first.reserved_open_permit.is_some());
    assert!(
        !try_reserve_open_file_permit_for_work_item(&mut second, &permit_rx, &permit_tx)
            .expect("second should wait")
    );

    drop(first.reserved_open_permit.take());

    assert!(
        try_reserve_open_file_permit_for_work_item(&mut second, &permit_rx, &permit_tx)
            .expect("second reserve after release")
    );
    assert!(second.reserved_open_permit.is_some());
}

#[test]
fn restore_finish_rejects_truncated_payload_mid_file() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manifest = ArchiveManifest::new(vec![file_entry("nested/file.txt", 4, 0)]);

    let mut writer = DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
    writer.write_chunk(b"te").expect("write partial payload");

    let error = writer
        .finish()
        .expect_err("finish should reject truncation");
    assert!(matches!(
        error,
        OxideError::InvalidFormat("truncated file payload during directory restore")
    ));
}

#[test]
fn restore_finish_rejects_missing_later_file_payload() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manifest = ArchiveManifest::new(vec![
        file_entry("nested/first.txt", 4, 0),
        file_entry("nested/second.txt", 4, 4),
    ]);

    let mut writer = DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
    writer.write_chunk(b"test").expect("write first file");

    let error = writer
        .finish()
        .expect_err("finish should reject missing later payload");
    assert!(matches!(
        error,
        OxideError::InvalidFormat("directory restore ended before all entries completed")
    ));
}

#[test]
fn restore_write_chunk_rejects_payload_beyond_manifest() {
    let temp = tempfile::tempdir().expect("tempdir");
    let manifest = ArchiveManifest::new(vec![file_entry("nested/file.txt", 4, 0)]);

    let mut writer = DirectoryRestoreWriter::create(temp.path(), manifest).expect("create writer");
    let error = writer
        .write_chunk(b"test!")
        .expect_err("write should reject extra payload");

    assert!(matches!(
        error,
        OxideError::InvalidFormat("directory archive contains file data beyond manifest entries")
    ));
}
