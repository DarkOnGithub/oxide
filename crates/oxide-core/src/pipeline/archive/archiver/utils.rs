use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::{collections::HashMap, collections::hash_map::Entry};

use crate::format::ArchiveManifest;
use crate::format::{
    CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE,
};
use crate::pipeline::types::PipelinePerformanceOptions;
use crate::types::Batch;
use crate::types::Result;

pub const SUBMISSION_DRAIN_BUDGET: usize = 128;
pub const DIRECTORY_FORMAT_PROBE_LIMIT: usize = 64 * 1024;
pub const DIRECTORY_PREFETCH_WINDOW: usize = 8;
pub const MIN_INFLIGHT_BLOCKS: usize = 64;
pub const MAX_INFLIGHT_BLOCKS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupKey {
    pub raw_len: usize,
    pub hash_prefix: [u8; 16],
}

impl Hash for DedupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.raw_len.hash(state);
        self.hash_prefix.hash(state);
    }
}

#[derive(Debug, Default)]
pub struct BlockDedupIndex {
    blocks: HashMap<DedupKey, u32>,
}

impl BlockDedupIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn classify(&mut self, batch: &Batch) -> Result<DedupDecision> {
        let key = dedup_key(batch.data());
        let block_index = u32::try_from(batch.id)
            .map_err(|_| crate::OxideError::InvalidFormat("block index exceeds u32 range"))?;

        Ok(match self.blocks.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(block_index);
                DedupDecision::Unique
            }
            Entry::Occupied(entry) => DedupDecision::Reference(*entry.get()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupDecision {
    Unique,
    Reference(u32),
}

pub fn dedup_key(data: &[u8]) -> DedupKey {
    let hash = blake3::hash(data);
    let mut hash_prefix = [0u8; 16];
    hash_prefix.copy_from_slice(&hash.as_bytes()[..16]);
    DedupKey {
        raw_len: data.len(),
        hash_prefix,
    }
}

#[inline]
pub fn container_prefix_bytes(block_count: u32, manifest_bytes: usize) -> u64 {
    let _ = (block_count, manifest_bytes);
    GLOBAL_HEADER_SIZE as u64
}

#[inline]
pub fn container_trailer_bytes(block_count: u32, manifest_bytes: usize) -> u64 {
    manifest_bytes as u64
        + CHUNK_TABLE_HEADER_SIZE as u64
        + block_count as u64 * CHUNK_DESCRIPTOR_SIZE as u64
        + FOOTER_SIZE as u64
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
    let metadata = fs::metadata(path)?;
    let entry = crate::ArchiveListingEntry::from_metadata(
        name.to_string(),
        crate::ArchiveEntryKind::File,
        size,
        None,
        &metadata,
        0,
    )?;
    Ok(ArchiveManifest::new(vec![entry]))
}

#[cfg(test)]
#[path = "../../../../tests/pipeline/archive/archiver/utils.rs"]
mod tests;
