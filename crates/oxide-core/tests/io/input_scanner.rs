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
    let data = b"abcdefghij";

    assert_eq!(scanner.find_block_boundary(data, 0), 4);
    assert_eq!(scanner.find_block_boundary(data, 4), 8);
    assert_eq!(scanner.find_block_boundary(data, 8), 10);
    assert_eq!(scanner.find_block_boundary(data, 10), 10);
}

#[test]
fn cdc_scanner_respects_bounds() {
    let policy = ChunkingPolicy::content_defined_for_target(16 * 1024);
    let scanner = InputScanner::with_chunking_policy(policy);
    let data = vec![b'a'; 128 * 1024];

    let end = scanner.find_block_boundary(&data, 0);

    assert!(end >= policy.min_size);
    assert!(end <= policy.max_size);
}
