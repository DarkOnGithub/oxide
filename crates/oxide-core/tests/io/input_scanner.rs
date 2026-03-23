use std::io::Write;

use oxide_core::{ChunkingPolicy, InputScanner};
use tempfile::NamedTempFile;

#[test]
fn scanner_splits_file_into_fixed_size_batches() -> Result<(), Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(b"abcdefghij")?;
    file.flush()?;

    let scanner = InputScanner::new(4);
    let batches = scanner.scan_file(file.path())?;

    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].data(), b"abcd");
    assert_eq!(batches[1].data(), b"efgh");
    assert_eq!(batches[2].data(), b"ij");
    assert!(batches.iter().all(|batch| !batch.force_raw_storage));

    Ok(())
}

#[test]
fn scanner_returns_empty_for_empty_files() -> Result<(), Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;

    let scanner = InputScanner::new(4);
    let batches = scanner.scan_file(file.path())?;

    assert!(batches.is_empty());
    Ok(())
}

#[test]
fn block_boundary_respects_chunking_policy() {
    let scanner = InputScanner::with_chunking_policy(ChunkingPolicy::fixed(4));

    assert_eq!(scanner.find_block_boundary(0, 10), 4);
    assert_eq!(scanner.find_block_boundary(4, 10), 8);
    assert_eq!(scanner.find_block_boundary(8, 10), 10);
    assert_eq!(scanner.find_block_boundary(10, 10), 10);
}
