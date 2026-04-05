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
    assert!(batches.iter().all(|batch| batch.stream_id == 1));

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

#[test]
fn cdc_boundaries_are_deterministic() {
    let scanner = InputScanner::with_chunking_policy(ChunkingPolicy::cdc(32, 8, 64));
    let data = b"alpha alpha alpha alpha beta beta beta beta gamma gamma gamma gamma";

    let mut first_pass = Vec::new();
    let mut start = 0usize;
    while start < data.len() {
        let end = scanner.find_block_boundary_in_slice(data, start);
        first_pass.push((start, end));
        start = end;
    }

    let mut second_pass = Vec::new();
    let mut start = 0usize;
    while start < data.len() {
        let end = scanner.find_block_boundary_in_slice(data, start);
        second_pass.push((start, end));
        start = end;
    }

    assert_eq!(first_pass, second_pass);
}

#[test]
fn cdc_boundaries_respect_min_and_max_block_sizes() {
    let scanner = InputScanner::with_chunking_policy(ChunkingPolicy::cdc(32, 8, 48));
    let data = vec![b'a'; 160];

    let mut start = 0usize;
    while start < data.len() {
        let end = scanner.find_block_boundary_in_slice(&data, start);
        let len = end - start;
        if end < data.len() {
            assert!(len >= 8);
        }
        assert!(len <= 48);
        start = end;
    }
}
