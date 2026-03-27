use super::ArchiveManifest;
use crate::{ArchiveEntryKind, ArchiveListingEntry, ArchiveTimestamp};

#[test]
fn manifest_round_trips_extended_entry_metadata() {
    let manifest = ArchiveManifest::new(vec![
        ArchiveListingEntry::directory(
            "nested".to_string(),
            0o755,
            ArchiveTimestamp {
                seconds: 1_710_000_000,
                nanoseconds: 42,
            },
            1000,
            100,
        ),
        ArchiveListingEntry::file(
            "nested/data.bin".to_string(),
            7,
            0o640,
            ArchiveTimestamp {
                seconds: 1_710_000_123,
                nanoseconds: 99,
            },
            1000,
            100,
            0,
        ),
        ArchiveListingEntry {
            path: "nested/link".to_string(),
            kind: ArchiveEntryKind::Symlink,
            target: Some("../shared.txt".to_string()),
            size: 0,
            mode: 0o777,
            mtime: ArchiveTimestamp {
                seconds: 1_710_000_456,
                nanoseconds: 7,
            },
            uid: 1000,
            gid: 100,
            content_offset: 0,
        },
    ]);

    let bytes = manifest.encode().expect("encode manifest");
    let decoded = ArchiveManifest::decode(&bytes).expect("decode manifest");

    assert_eq!(decoded, manifest);
    assert_eq!(decoded.entries()[0].kind, ArchiveEntryKind::Directory);
    assert_eq!(decoded.entries()[1].kind, ArchiveEntryKind::File);
    assert_eq!(decoded.entries()[2].kind, ArchiveEntryKind::Symlink);
    assert_eq!(
        decoded.entries()[2].target.as_deref(),
        Some("../shared.txt")
    );
}
