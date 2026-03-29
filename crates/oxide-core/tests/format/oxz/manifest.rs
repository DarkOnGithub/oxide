use super::ArchiveManifest;
use crate::{
    ArchiveDictionary, ArchiveDictionaryBank, ArchiveEntryKind, ArchiveListingEntry,
    ArchiveTimestamp, CompressionAlgo, DictionaryClass,
};

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

#[test]
fn manifest_round_trips_extension_dictionaries() {
    let manifest = ArchiveManifest::new(vec![ArchiveListingEntry::file(
        "nested/data.json".to_string(),
        7,
        0o640,
        ArchiveTimestamp::default(),
        1000,
        100,
        0,
    )])
    .with_dictionary_bank(
        ArchiveDictionaryBank::new(vec![ArchiveDictionary {
            id: 7,
            algo: CompressionAlgo::Zstd,
            class: DictionaryClass::Extension("json".to_string()),
            bytes: vec![1, 2, 3, 4],
        }])
        .expect("dictionary bank"),
    );

    let bytes = manifest.encode().expect("encode manifest");
    let decoded = ArchiveManifest::decode(&bytes).expect("decode manifest");

    assert_eq!(decoded, manifest);
}

#[test]
fn manifest_decodes_legacy_dictionary_entries() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"OXM2");
    bytes.push(1);
    bytes.push(1);
    bytes.push(3);
    bytes.push(CompressionAlgo::Zstd.to_flags());
    bytes.push(2);
    bytes.push(4);
    bytes.extend_from_slice(&[9, 8, 7, 6]);
    bytes.push(0);

    let decoded = ArchiveManifest::decode(&bytes).expect("decode legacy manifest");

    let dictionaries = decoded.dictionary_bank().dictionaries();
    assert_eq!(dictionaries.len(), 1);
    assert_eq!(dictionaries[0].id, 3);
    assert_eq!(dictionaries[0].algo, CompressionAlgo::Zstd);
    assert_eq!(dictionaries[0].class, DictionaryClass::StructuredText);
    assert_eq!(dictionaries[0].bytes, vec![9, 8, 7, 6]);
}
