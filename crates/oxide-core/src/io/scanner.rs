use std::path::Path;
use std::time::Instant;

use crate::io::MmapInput;
use crate::io::chunking::ChunkingPolicy;
use crate::telemetry;
use crate::telemetry::profile;
use crate::telemetry::tags;
use crate::types::{Batch, ChunkEncodingPlan, Result};

const PROFILE_TAG_STACK_SCANNER: &[&str] = &[tags::TAG_SCANNER];

/// Scanner configuration and fixed-size batch selection entrypoint.
#[derive(Debug, Clone)]
pub struct InputScanner {
    chunking_policy: ChunkingPolicy,
    compression_plan: ChunkEncodingPlan,
}

impl InputScanner {
    /// Creates a new scanner with a target block size.
    pub fn new(target_block_size: usize) -> Self {
        Self::with_chunking_policy(ChunkingPolicy::fixed(target_block_size))
    }

    /// Creates a new scanner with an explicit chunking policy.
    pub fn with_chunking_policy(chunking_policy: ChunkingPolicy) -> Self {
        Self::with_chunking_policy_and_plan(chunking_policy, ChunkEncodingPlan::default())
    }

    /// Creates a new scanner with an explicit chunking policy and compression plan.
    pub fn with_chunking_policy_and_plan(
        chunking_policy: ChunkingPolicy,
        compression_plan: ChunkEncodingPlan,
    ) -> Self {
        Self {
            chunking_policy,
            compression_plan,
        }
    }

    /// Returns configured target block size.
    pub fn target_block_size(&self) -> usize {
        self.chunking_policy.target_size
    }

    /// Returns the configured chunking policy.
    pub fn chunking_policy(&self) -> ChunkingPolicy {
        self.chunking_policy
    }

    /// Scans a file into ordered fixed-size batches.
    pub fn scan_file(&self, path: &Path) -> Result<Vec<Batch>> {
        let started_at = Instant::now();
        let result = self.scan_file_inner(path);

        let elapsed_us = profile::elapsed_us(started_at);
        telemetry::record_histogram(tags::METRIC_SCANNER_SCAN_LATENCY_US, elapsed_us);

        match &result {
            Ok(_batches) => {
                telemetry::increment_counter(tags::METRIC_SCANNER_SCAN_COUNT, 1);
                profile::event(
                    tags::PROFILE_SCANNER,
                    PROFILE_TAG_STACK_SCANNER,
                    "scan_file",
                    "ok",
                    elapsed_us,
                    "scanner completed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(PROFILE_TAG_STACK_SCANNER) {
                    tracing::debug!(
                        target: tags::PROFILE_SCANNER,
                        op = "scan_file",
                        result = "ok",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_SCANNER,
                        batch_count = _batches.len(),
                        path = %path.display(),
                        "scanner context"
                    );
                }
            }
            Err(_error) => {
                profile::event(
                    tags::PROFILE_SCANNER,
                    PROFILE_TAG_STACK_SCANNER,
                    "scan_file",
                    "error",
                    elapsed_us,
                    "scanner failed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(PROFILE_TAG_STACK_SCANNER) {
                    tracing::debug!(
                        target: tags::PROFILE_SCANNER,
                        op = "scan_file",
                        result = "error",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_SCANNER,
                        path = %path.display(),
                        error = %_error,
                        "scanner context"
                    );
                }
            }
        }

        result
    }

    fn scan_file_inner(&self, path: &Path) -> Result<Vec<Batch>> {
        let mmap = MmapInput::open(path)?;
        if mmap.is_empty() {
            return Ok(Vec::new());
        }

        let len = mmap.len();
        let estimated_batches = len.saturating_add(self.chunking_policy.target_size - 1)
            / self.chunking_policy.target_size;
        let mut batches = Vec::with_capacity(estimated_batches.max(1));
        let mut start = 0usize;
        let source_path = path.to_path_buf();
        let mut id = 0usize;
        let mapping = mmap.mapping();
        let full_slice = mapping.as_deref();

        while start < len {
            let end = match full_slice {
                Some(bytes) => self.find_block_boundary(bytes, start),
                None => len,
            };
            let batch_data = mmap.mapped_slice(start, end)?;
            batches.push(Batch {
                id,
                source_path: source_path.clone(),
                data: batch_data,
                stream_id: 0,
                compression_plan: self.compression_plan,
                force_raw_storage: false,
            });
            start = end;
            id += 1;
        }

        Ok(batches)
    }

    /// Finds the next fixed-size boundary.
    pub fn find_block_boundary(&self, data: &[u8], start: usize) -> usize {
        if start >= data.len() {
            return data.len();
        }

        self.chunking_policy.find_boundary(data, start)
    }
}
