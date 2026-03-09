use std::fs;
use std::io::Read;
use std::path::Path;

use crate::format::{
    CHUNK_DESCRIPTOR_SIZE, CORE_SECTION_COUNT, GLOBAL_HEADER_SIZE,
    SECTION_TABLE_ENTRY_SIZE,
};
use crate::pipeline::types::PipelinePerformanceOptions;
use crate::format::ArchiveManifest;
use crate::types::Result;

pub const SUBMISSION_DRAIN_BUDGET: usize = 128;
pub const DIRECTORY_FORMAT_PROBE_LIMIT: usize = 64 * 1024;
pub const DIRECTORY_PREFETCH_WINDOW: usize = 8;
pub const MIN_INFLIGHT_BLOCKS: usize = 64;
pub const MAX_INFLIGHT_BLOCKS: usize = 4096;

#[inline]
pub fn container_prefix_bytes(
    block_count: u32,
    dictionary_bytes: usize,
    manifest_bytes: usize,
) -> u64 {
    let section_count = CORE_SECTION_COUNT as u64 + if manifest_bytes > 0 { 1 } else { 0 };
    GLOBAL_HEADER_SIZE as u64
        + section_count * SECTION_TABLE_ENTRY_SIZE as u64
        + block_count as u64 * CHUNK_DESCRIPTOR_SIZE as u64
        + dictionary_bytes as u64
        + manifest_bytes as u64
}

pub fn max_inflight_blocks(
    total_blocks: usize,
    num_workers: usize,
    block_size: usize,
    performance: &PipelinePerformanceOptions,
) -> usize {
    let scaled_by_workers = num_workers.saturating_mul(performance.max_inflight_blocks_per_worker);
    let bounded_workers = scaled_by_workers.clamp(MIN_INFLIGHT_BLOCKS, MAX_INFLIGHT_BLOCKS);

    let block_bytes = block_size.max(1);
    let inflight_bytes = performance.max_inflight_bytes.max(block_bytes);
    let bounded_by_bytes = inflight_bytes.div_ceil(block_bytes).max(1);

    let bounded = bounded_workers
        .min(bounded_by_bytes)
        .clamp(1, MAX_INFLIGHT_BLOCKS);
    bounded.min(total_blocks.max(1))
}

pub fn file_manifest(path: &Path, size: u64) -> Result<ArchiveManifest> {
    let name = path.file_name().and_then(|value| value.to_str()).ok_or(
        crate::OxideError::InvalidFormat(
            "file archive requires a utf8 file name for manifest metadata",
        ),
    )?;
    Ok(ArchiveManifest::new(vec![crate::ArchiveListingEntry {
        path: name.to_string(),
        kind: crate::ArchiveEntryKind::File,
        size,
    }]))
}

pub fn collect_file_sample(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let limit = max_bytes.max(1);
    let mut file = fs::File::open(path)?;
    let mut sample = Vec::with_capacity(limit);
    let mut scratch = vec![0u8; 64 * 1024];
    while sample.len() < limit {
        let to_read = (limit - sample.len()).min(scratch.len());
        let read = file.read(&mut scratch[..to_read])?;
        if read == 0 {
            break;
        }
        sample.extend_from_slice(&scratch[..read]);
    }
    Ok(sample)
}
