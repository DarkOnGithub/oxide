use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use blake3::Hash as Blake3Hash;

use crate::buffer::BufferPool;
use crate::compression::{
    CompressionRequest, apply_compression_request_with_scratch,
    apply_compression_request_with_scratch_into, recycle_compression_buffer,
    supports_direct_buffer_output,
};
use crate::core::WorkerScratchArena;
use crate::dictionary::ArchiveDictionaryBank;
use crate::pipeline::archive::types::ProcessingThroughputTotals;
use crate::types::{Batch, CompressedBlock, CompressedPayload, CompressionAlgo, Result};

const INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN: usize = 32 * 1024;
const INCOMPRESSIBLE_PROBE_SAMPLE_LEN: usize = 16 * 1024;
const LZMA_PROBE_SAMPLE_LEN: usize = 8 * 1024;

/// Minimum sample size for the entropy probe to be meaningful.
const ENTROPY_PROBE_MIN_SAMPLE_LEN: usize = 512;
/// Shannon entropy threshold above which data is considered incompressible.
/// 8.0 = perfectly random; 7.5 is conservative enough to avoid false positives
/// on highly compressible data while catching compressed/encrypted content.
const ENTROPY_INCOMPRESSIBLE_THRESHOLD: f64 = 7.5;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RawChunkDedupeKey {
    original_len: u64,
    payload_hash: Blake3Hash,
}

impl RawChunkDedupeKey {
    fn from_source(source: &[u8]) -> Self {
        Self {
            original_len: source.len() as u64,
            payload_hash: blake3::hash(source),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RawChunkDeduper {
    max_entries: usize,
    entries: HashMap<RawChunkDedupeKey, usize>,
    insertion_order: VecDeque<(RawChunkDedupeKey, usize)>,
}

impl RawChunkDeduper {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    fn find_reference(&mut self, block_id: usize, key: RawChunkDedupeKey) -> Result<Option<u32>> {
        if self.max_entries == 0 {
            return Ok(None);
        }

        if let Some(&canonical_block_id) = self.entries.get(&key) {
            if canonical_block_id < block_id {
                return u32::try_from(canonical_block_id)
                    .map(Some)
                    .map_err(|_| crate::OxideError::InvalidFormat("block id exceeds u32 range"));
            }

            if canonical_block_id == block_id {
                return Ok(None);
            }
        }

        self.entries.insert(key.clone(), block_id);
        self.insertion_order.push_back((key, block_id));

        while self.insertion_order.len() > self.max_entries {
            if let Some((evicted_key, evicted_block_id)) = self.insertion_order.pop_front()
                && self.entries.get(&evicted_key) == Some(&evicted_block_id)
            {
                self.entries.remove(&evicted_key);
            }
        }

        Ok(None)
    }
}

pub(crate) type SharedRawChunkDeduper = Arc<Mutex<RawChunkDeduper>>;

pub(crate) fn shared_raw_chunk_deduper(max_entries: usize) -> SharedRawChunkDeduper {
    Arc::new(Mutex::new(RawChunkDeduper::new(max_entries)))
}

#[derive(Debug, Clone, Copy)]
struct CompressionProbeConfig {
    min_source_len: usize,
    sample_len: usize,
}

#[inline]
fn compression_probe_config(plan: crate::types::ChunkEncodingPlan) -> CompressionProbeConfig {
    match plan.algo {
        CompressionAlgo::Lz4 => CompressionProbeConfig {
            min_source_len: INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN,
            sample_len: INCOMPRESSIBLE_PROBE_SAMPLE_LEN,
        },
        CompressionAlgo::Zstd => CompressionProbeConfig {
            min_source_len: INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN,
            sample_len: INCOMPRESSIBLE_PROBE_SAMPLE_LEN,
        },
        CompressionAlgo::Lzma if plan.lzma_extreme => CompressionProbeConfig {
            min_source_len: INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN,
            sample_len: LZMA_PROBE_SAMPLE_LEN,
        },
        CompressionAlgo::Lzma => CompressionProbeConfig {
            min_source_len: INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN,
            sample_len: LZMA_PROBE_SAMPLE_LEN,
        },
    }
}

#[inline]
fn should_skip_full_compression_probe(
    source_len: usize,
    plan: crate::types::ChunkEncodingPlan,
) -> bool {
    source_len >= compression_probe_config(plan).min_source_len
}

/// Fast entropy estimation using byte-frequency counting.
/// Returns true if the sample has Shannon entropy above the incompressible
/// threshold, indicating the data is likely already compressed or random.
#[inline]
fn is_likely_incompressible_entropy(source: &[u8], sample_len: usize) -> bool {
    if sample_len < ENTROPY_PROBE_MIN_SAMPLE_LEN {
        return false;
    }

    let sample = &source[..sample_len];
    let mut freq = [0u32; 256];
    for &byte in sample {
        freq[byte as usize] += 1;
    }

    let len = sample_len as f64;
    let mut entropy = 0.0f64;
    for &count in &freq {
        if count > 0 {
            let p = count as f64 / len;
            entropy -= p * p.log2();
        }
    }

    entropy > ENTROPY_INCOMPRESSIBLE_THRESHOLD
}

#[inline]
fn incompressible_probe_offsets_storage(
    source_len: usize,
    sample_len: usize,
) -> ([usize; 3], usize) {
    let sample_len = sample_len.min(source_len);
    if sample_len == 0 {
        return ([0; 3], 0);
    }

    let max_start = source_len.saturating_sub(sample_len);
    let mut offsets = [0usize; 3];
    let mut len = 1usize;
    if max_start >= sample_len {
        let middle = max_start / 2;
        if middle != 0 {
            offsets[len] = middle;
            len += 1;
        }
        if max_start != 0 && max_start != middle {
            offsets[len] = max_start;
            len += 1;
        }
    }

    (offsets, len)
}

fn is_likely_incompressible_sample(
    source: &[u8],
    source_path: &Path,
    plan: crate::types::ChunkEncodingPlan,
    dictionary_bank: &ArchiveDictionaryBank,
    scratch: &mut WorkerScratchArena,
) -> Result<bool> {
    let sample_len = source.len().min(compression_probe_config(plan).sample_len);
    if sample_len == 0 {
        return Ok(false);
    }

    if plan.algo != CompressionAlgo::Zstd {
        if is_likely_incompressible_entropy(source, sample_len) {
            return Ok(true);
        }

        let probe = apply_compression_request_with_scratch(
            CompressionRequest {
                data: &source[..sample_len],
                algo: plan.algo,
                level: plan.level,
                lzma_extreme: plan.lzma_extreme,
                lzma_dictionary_size: plan.lzma_dictionary_size,
                stream_id: 0,
                dictionary_id: 0,
                dictionary: None,
            },
            scratch.compression(),
        )?;

        let likely_incompressible = probe.len() >= sample_len;
        recycle_compression_buffer(plan.algo, probe, scratch.compression());
        return Ok(likely_incompressible);
    }

    let (offsets, offset_count) = incompressible_probe_offsets_storage(source.len(), sample_len);
    for &offset in &offsets[..offset_count] {
        let sample = &source[offset..offset + sample_len];
        let selected_dictionary =
            dictionary_bank.select_for_chunk(plan.algo, sample, Some(source_path));
        let probe = apply_compression_request_with_scratch(
            CompressionRequest {
                data: sample,
                algo: plan.algo,
                level: plan.level,
                lzma_extreme: plan.lzma_extreme,
                lzma_dictionary_size: plan.lzma_dictionary_size,
                stream_id: 0,
                dictionary_id: selected_dictionary
                    .map(|dictionary| dictionary.id)
                    .unwrap_or(0),
                dictionary: selected_dictionary.map(|dictionary| dictionary.bytes.as_slice()),
            },
            scratch.compression(),
        )?;

        let sample_likely_incompressible = probe.len() >= sample_len;
        recycle_compression_buffer(plan.algo, probe, scratch.compression());
        if !sample_likely_incompressible {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(crate) struct ProcessBatchConfig<'a> {
    pub skip_compression: bool,
    pub raw_fallback_enabled: bool,
    pub dictionary_bank: &'a ArchiveDictionaryBank,
    pub processing_totals: &'a ProcessingThroughputTotals,
    pub raw_chunk_deduper: Option<&'a SharedRawChunkDeduper>,
}

pub(crate) fn process_batch(
    batch: Batch,
    pool: &BufferPool,
    _compression: CompressionAlgo,
    config: &ProcessBatchConfig<'_>,
    scratch: &mut WorkerScratchArena,
) -> Result<CompressedBlock> {
    let Batch {
        id,
        source_path,
        data,
        stream_id,
        compression_plan: plan,
        force_raw_storage,
        ..
    } = batch;
    let source_len = data.len();
    let source = data.as_slice();

    if let Some(raw_chunk_deduper) = config.raw_chunk_deduper {
        let raw_chunk_key = RawChunkDedupeKey::from_source(source);
        let reference_target = raw_chunk_deduper
            .lock()
            .map_err(|_| {
                crate::OxideError::CompressionError("raw chunk dedupe state poisoned".to_string())
            })?
            .find_reference(id, raw_chunk_key)?;
        if let Some(reference_target) = reference_target {
            config
                .processing_totals
                .record(source_len as u64, std::time::Duration::ZERO);
            return Ok(CompressedBlock::reference(
                id,
                stream_id,
                reference_target,
                source_len as u64,
            ));
        }
    }

    if force_raw_storage {
        config
            .processing_totals
            .record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data(data),
            plan,
            true,
            source_len as u64,
        ));
    }

    if config.skip_compression {
        config
            .processing_totals
            .record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data(data),
            plan,
            true,
            source_len as u64,
        ));
    }

    if config.raw_fallback_enabled
        && should_skip_full_compression_probe(source_len, plan)
        && is_likely_incompressible_sample(
            source,
            &source_path,
            plan,
            config.dictionary_bank,
            scratch,
        )?
    {
        config
            .processing_totals
            .record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data(data),
            plan,
            true,
            source_len as u64,
        ));
    }

    let selected_dictionary =
        config
            .dictionary_bank
            .select_for_chunk(plan.algo, source, Some(&source_path));
    let dictionary_id = selected_dictionary
        .map(|dictionary| dictionary.id)
        .unwrap_or(0);
    let request = CompressionRequest {
        data: source,
        algo: plan.algo,
        level: plan.level,
        lzma_extreme: plan.lzma_extreme,
        lzma_dictionary_size: plan.lzma_dictionary_size,
        stream_id,
        dictionary_id,
        dictionary: selected_dictionary.map(|dictionary| dictionary.bytes.as_slice()),
    };

    if supports_direct_buffer_output(plan.algo) {
        let compression_started = Instant::now();
        let mut compressed = pool.acquire_with_capacity(source_len);
        apply_compression_request_with_scratch_into(
            request,
            scratch.compression(),
            compressed.as_mut_vec(),
        )?;
        let compression_elapsed = compression_started.elapsed();
        config
            .processing_totals
            .record(source_len as u64, compression_elapsed);

        let raw_passthrough = config.raw_fallback_enabled && compressed.len() >= source_len;
        let data = if raw_passthrough {
            CompressedPayload::from_batch_data(data)
        } else {
            CompressedPayload::from(compressed)
        };

        return Ok(CompressedBlock::with_chunk_encoding_and_dictionary(
            id,
            stream_id,
            data,
            plan,
            raw_passthrough,
            source_len as u64,
            if raw_passthrough { 0 } else { dictionary_id },
        ));
    }

    let compression_started = Instant::now();
    let compressed = apply_compression_request_with_scratch(request, scratch.compression())?;
    let compression_elapsed = compression_started.elapsed();
    config
        .processing_totals
        .record(source_len as u64, compression_elapsed);

    let raw_passthrough = config.raw_fallback_enabled && compressed.len() >= source_len;
    let data = if raw_passthrough {
        CompressedPayload::from_batch_data(data)
    } else {
        CompressedPayload::from_vec_in_pool(compressed, pool)
    };

    Ok(CompressedBlock::with_chunk_encoding_and_dictionary(
        id,
        stream_id,
        data,
        plan,
        raw_passthrough,
        source_len as u64,
        if raw_passthrough { 0 } else { dictionary_id },
    ))
}

#[inline]
pub fn select_stored_payload<'a>(
    source: &'a [u8],
    compressed: &'a [u8],
    raw_fallback_enabled: bool,
) -> (&'a [u8], bool) {
    let raw_passthrough = raw_fallback_enabled && compressed.len() >= source.len();
    let payload = if raw_passthrough { source } else { compressed };
    (payload, raw_passthrough)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_chunk_deduper_references_earlier_duplicate_blocks() {
        let mut deduper = RawChunkDeduper::new(8);

        assert_eq!(
            deduper
                .find_reference(0, RawChunkDedupeKey::from_source(b"abcd"))
                .expect("first block"),
            None
        );
        assert_eq!(
            deduper
                .find_reference(1, RawChunkDedupeKey::from_source(b"abcd"))
                .expect("duplicate block"),
            Some(0)
        );
    }

    #[test]
    fn raw_chunk_deduper_replaces_later_canonical_ids_with_earlier_ones() {
        let mut deduper = RawChunkDeduper::new(8);

        assert_eq!(
            deduper
                .find_reference(5, RawChunkDedupeKey::from_source(b"abcd"))
                .expect("late block"),
            None
        );
        assert_eq!(
            deduper
                .find_reference(2, RawChunkDedupeKey::from_source(b"abcd"))
                .expect("earlier block"),
            None
        );
        assert_eq!(
            deduper
                .find_reference(7, RawChunkDedupeKey::from_source(b"abcd"))
                .expect("later duplicate"),
            Some(2)
        );
    }
}
