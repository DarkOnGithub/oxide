use std::path::Path;

use super::{should_force_raw_storage, should_force_raw_storage_by_extension};

#[test]
fn raw_storage_extension_policy_matches_known_compressed_types() {
    assert!(should_force_raw_storage_by_extension(Path::new(
        "photo.jpg"
    )));
    assert!(should_force_raw_storage_by_extension(Path::new(
        "bundle.ZIP"
    )));
    assert!(should_force_raw_storage_by_extension(Path::new(
        "archive.tar.zst"
    )));
    assert!(!should_force_raw_storage_by_extension(Path::new(
        "notes.txt"
    )));
    assert!(!should_force_raw_storage_by_extension(Path::new(
        "bitmap.bmp"
    )));
    assert!(!should_force_raw_storage_by_extension(Path::new("README")));
}

#[test]
fn raw_storage_policy_uses_path_only() {
    assert!(should_force_raw_storage(Path::new("photo.jpg")));
    assert!(!should_force_raw_storage(Path::new("payload.bin")));
}
