use super::super::reorder_writer::OrderedChunkWriter;
use super::DirectoryRestoreWriter;
use crate::{ArchiveEntryKind, ArchiveListingEntry, ArchiveManifest, ArchiveTimestamp};
use std::fs;
use std::io::Read;
use std::path::Path;
use tempfile::tempdir;

#[cfg(unix)]
#[test]
fn restore_creates_symlink_entries() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let manifest = ArchiveManifest::new(vec![ArchiveListingEntry {
        path: "link".to_string(),
        kind: ArchiveEntryKind::Symlink,
        target: Some("target.txt".to_string()),
        size: 0,
        mode: 0o777,
        mtime: ArchiveTimestamp::default(),
        uid: 0,
        gid: 0,
        content_offset: 0,
    }]);

    let mut writer = DirectoryRestoreWriter::create(root, manifest).expect("create writer");
    writer.finish().expect("finish restore");

    let link_path = root.join("link");
    let metadata = fs::symlink_metadata(&link_path).expect("symlink metadata");
    assert!(metadata.file_type().is_symlink());
    assert_eq!(
        fs::read_link(&link_path).expect("read link"),
        Path::new("target.txt")
    );
}

#[test]
fn restore_creates_implicit_parent_directories() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let manifest = ArchiveManifest::new(vec![ArchiveListingEntry::file(
        "nested/deeper/file.txt".to_string(),
        4,
        0o644,
        ArchiveTimestamp::default(),
        0,
        0,
        0,
    )]);

    let mut writer = DirectoryRestoreWriter::create(root, manifest).expect("create writer");
    writer.write_chunk(b"test").expect("write chunk");
    writer.finish().expect("finish restore");

    let mut restored = String::new();
    fs::File::open(root.join("nested/deeper/file.txt"))
        .expect("open restored file")
        .read_to_string(&mut restored)
        .expect("read restored file");
    assert_eq!(restored, "test");
}

#[test]
fn restore_multiple_files_completes_metadata_finalization() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path();
    let mut entries = Vec::new();
    let mut payload = Vec::new();
    for index in 0..8 {
        let name = format!("dir/file-{index}.txt");
        entries.push(ArchiveListingEntry::file(
            name,
            1,
            0o644,
            ArchiveTimestamp::default(),
            0,
            0,
            index as u64,
        ));
        payload.push(b'a' + index as u8);
    }

    let manifest = ArchiveManifest::new(entries);
    let mut writer = DirectoryRestoreWriter::create(root, manifest).expect("create writer");
    writer.write_chunk(&payload).expect("write chunk");
    writer.finish().expect("finish restore");

    for index in 0..8 {
        let path = root.join(format!("dir/file-{index}.txt"));
        assert_eq!(
            fs::read(&path).expect("read restored file"),
            vec![b'a' + index as u8]
        );
    }
}
