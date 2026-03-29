use std::time::Instant;

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
#[cfg(test)]
const LZMA_PROBE_MIN_SOURCE_LEN: usize = 256 * 1024;
#[cfg(test)]
const LZMA_EXTREME_PROBE_MIN_SOURCE_LEN: usize = 512 * 1024;
const LZMA_PROBE_SAMPLE_LEN: usize = 8 * 1024;

/// Minimum sample size for the entropy probe to be meaningful.
const ENTROPY_PROBE_MIN_SAMPLE_LEN: usize = 512;
/// Shannon entropy threshold above which data is considered incompressible.
/// 8.0 = perfectly random; 7.5 is conservative enough to avoid false positives
/// on highly compressible data while catching compressed/encrypted content.
const ENTROPY_INCOMPRESSIBLE_THRESHOLD: f64 = 7.5;

#[derive(Debug, Clone, Copy)]
struct CompressionProbeConfig {
    min_source_len: usize,
    sample_len: usize,
}

#[inline]
fn compression_probe_config(plan: crate::types::ChunkEncodingPlan) -> CompressionProbeConfig {
    match plan.algo {
        CompressionAlgo::Lz4 => CompressionProbeConfig {
            min_source_len: usize::MAX,
            sample_len: INCOMPRESSIBLE_PROBE_SAMPLE_LEN,
        },
        CompressionAlgo::Zstd => CompressionProbeConfig {
            min_source_len: INCOMPRESSIBLE_PROBE_MIN_SOURCE_LEN,
            sample_len: INCOMPRESSIBLE_PROBE_SAMPLE_LEN,
        },
        CompressionAlgo::Lzma if plan.lzma_extreme => CompressionProbeConfig {
            min_source_len: usize::MAX,
            sample_len: LZMA_PROBE_SAMPLE_LEN,
        },
        CompressionAlgo::Lzma => CompressionProbeConfig {
            min_source_len: usize::MAX,
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

fn is_likely_incompressible_sample(
    source: &[u8],
    plan: crate::types::ChunkEncodingPlan,
    dictionary_bank: &ArchiveDictionaryBank,
    scratch: &mut WorkerScratchArena,
) -> Result<bool> {
    let sample_len = source.len().min(compression_probe_config(plan).sample_len);
    if sample_len == 0 {
        return Ok(false);
    }

    // Fast path: use entropy estimation (~100x faster than trial compression).
    // Only fires for very high entropy data (>7.5 bits/byte), so borderline
    // cases still get the full compression probe.
    if is_likely_incompressible_entropy(source, sample_len) {
        return Ok(true);
    }

    let selected_dictionary = dictionary_bank.select_for_chunk(plan.algo, &source[..sample_len]);
    let probe = apply_compression_request_with_scratch(
        CompressionRequest {
            data: &source[..sample_len],
            algo: plan.algo,
            level: plan.level,
            lzma_extreme: plan.lzma_extreme,
            lzma_dictionary_size: plan.lzma_dictionary_size,
            dictionary_id: selected_dictionary
                .map(|dictionary| dictionary.id)
                .unwrap_or(0),
            dictionary: selected_dictionary.map(|dictionary| dictionary.bytes.as_slice()),
        },
        scratch.compression(),
    )?;

    let likely_incompressible = probe.len() >= sample_len;
    recycle_compression_buffer(plan.algo, probe, scratch.compression());

    Ok(likely_incompressible)
}

pub fn process_batch(
    batch: Batch,
    pool: &BufferPool,
    _compression: CompressionAlgo,
    skip_compression: bool,
    raw_fallback_enabled: bool,
    dictionary_bank: &ArchiveDictionaryBank,
    processing_totals: &ProcessingThroughputTotals,
    scratch: &mut WorkerScratchArena,
) -> Result<CompressedBlock> {
    let Batch {
        id,
        data,
        stream_id,
        compression_plan: plan,
        force_raw_storage,
        ..
    } = batch;
    let source_len = data.len();
    let source = data.as_slice();

    if force_raw_storage {
        processing_totals.record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data_in_pool(data, pool),
            plan,
            true,
            source_len as u64,
        ));
    }

    if skip_compression {
        processing_totals.record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data_in_pool(data, pool),
            plan,
            true,
            source_len as u64,
        ));
    }

    let selected_dictionary = dictionary_bank.select_for_chunk(plan.algo, source);
    let dictionary_id = selected_dictionary
        .map(|dictionary| dictionary.id)
        .unwrap_or(0);
    let request = CompressionRequest {
        data: source,
        algo: plan.algo,
        level: plan.level,
        lzma_extreme: plan.lzma_extreme,
        lzma_dictionary_size: plan.lzma_dictionary_size,
        dictionary_id,
        dictionary: selected_dictionary.map(|dictionary| dictionary.bytes.as_slice()),
    };

    if raw_fallback_enabled
        && !skip_compression
        && should_skip_full_compression_probe(source_len, plan)
        && is_likely_incompressible_sample(source, plan, dictionary_bank, scratch)?
    {
        processing_totals.record(source_len as u64, std::time::Duration::ZERO);
        return Ok(CompressedBlock::with_chunk_encoding(
            id,
            stream_id,
            CompressedPayload::from_batch_data_in_pool(data, pool),
            plan,
            true,
            source_len as u64,
        ));
    }

    if !skip_compression && supports_direct_buffer_output(plan.algo) {
        let compression_started = Instant::now();
        let mut compressed = pool.acquire();
        apply_compression_request_with_scratch_into(
            request,
            scratch.compression(),
            compressed.as_mut_vec(),
        )?;
        let compression_elapsed = compression_started.elapsed();
        processing_totals.record(source_len as u64, compression_elapsed);

        let raw_passthrough = raw_fallback_enabled && compressed.len() >= source_len;
        let data = if raw_passthrough {
            CompressedPayload::from_batch_data_in_pool(data, pool)
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
    processing_totals.record(source_len as u64, compression_elapsed);

    let raw_passthrough = raw_fallback_enabled && compressed.len() >= source_len;
    let data = if raw_passthrough {
        CompressedPayload::from_batch_data_in_pool(data, pool)
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
#[path = "../../../../tests/pipeline/archive/archiver/processing.rs"]
mod tests;
