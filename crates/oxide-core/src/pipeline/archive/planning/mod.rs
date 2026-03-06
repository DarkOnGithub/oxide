use std::cmp::Ordering;
use std::time::{Duration, Instant};

use crate::compression::{CompressionRequest, apply_compression_request_with_scratch};
use crate::io::ChunkingPolicy;
use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{
    Batch, ChunkEncodingPlan, CompressionAlgo, CompressionPreset, Result, WorkerScratchArena,
};

pub mod dictionary;
pub mod objective;

pub use dictionary::DictionaryCatalog;
pub use objective::{ObjectiveScore, ObjectiveWeights, score_candidate};

pub const MAX_DICTIONARY_BYTES: usize = 16 * 1024;
const MAX_TRIAL_BYTES: usize = 64 * 1024;
const PRESET_CANDIDATES: [CompressionPreset; 3] = [
    CompressionPreset::Fast,
    CompressionPreset::Default,
    CompressionPreset::High,
];

#[derive(Debug, Clone)]
pub struct PlannerSummary {
    pub mode: CompressionPreset,
    pub chunking_policy: ChunkingPolicy,
    pub chunk_count: usize,
    pub total_chunk_bytes: u64,
    pub min_chunk_bytes: usize,
    pub max_chunk_bytes: usize,
    pub dictionary_count: usize,
    pub dictionary_bytes: usize,
    pub chunks_with_dictionaries: u64,
    pub preset_chunk_counts: [u64; 3],
}

impl PlannerSummary {
    pub fn new(
        mode: CompressionPreset,
        chunking_policy: ChunkingPolicy,
        dictionaries: &DictionaryCatalog,
    ) -> Self {
        Self {
            mode,
            chunking_policy,
            chunk_count: 0,
            total_chunk_bytes: 0,
            min_chunk_bytes: usize::MAX,
            max_chunk_bytes: 0,
            dictionary_count: dictionaries.count(),
            dictionary_bytes: dictionaries.total_bytes(),
            chunks_with_dictionaries: 0,
            preset_chunk_counts: [0; 3],
        }
    }

    pub fn avg_chunk_bytes(&self) -> f64 {
        if self.chunk_count == 0 {
            0.0
        } else {
            self.total_chunk_bytes as f64 / self.chunk_count as f64
        }
    }

    pub fn min_chunk_bytes(&self) -> usize {
        if self.chunk_count == 0 {
            0
        } else {
            self.min_chunk_bytes
        }
    }

    fn record_batch(&mut self, batch: &Batch) {
        self.chunk_count += 1;
        self.total_chunk_bytes = self.total_chunk_bytes.saturating_add(batch.len() as u64);
        self.min_chunk_bytes = self.min_chunk_bytes.min(batch.len());
        self.max_chunk_bytes = self.max_chunk_bytes.max(batch.len());
        if batch.compression_plan.dict_id != 0 {
            self.chunks_with_dictionaries = self.chunks_with_dictionaries.saturating_add(1);
        }
        let index = match batch.compression_plan.preset {
            CompressionPreset::Fast => 0,
            CompressionPreset::Default => 1,
            CompressionPreset::High => 2,
        };
        self.preset_chunk_counts[index] = self.preset_chunk_counts[index].saturating_add(1);
    }
}

#[derive(Debug, Clone, Copy)]
struct CandidatePlan {
    plan: ChunkEncodingPlan,
    score: ObjectiveScore,
    stored_bytes: usize,
}

pub fn apply_chunk_plans(
    batches: &mut [Batch],
    default_algo: CompressionAlgo,
    mode: CompressionPreset,
    chunking_policy: ChunkingPolicy,
    dictionaries: &DictionaryCatalog,
) -> Result<PlannerSummary> {
    let mut scratch = WorkerScratchArena::new();
    let mut summary = PlannerSummary::new(mode, chunking_policy, dictionaries);

    for batch in batches {
        batch.compression_plan =
            plan_batch_encoding(batch, default_algo, mode, dictionaries, &mut scratch)?;
        summary.record_batch(batch);
    }

    Ok(summary)
}

pub fn plan_batch_encoding(
    batch: &Batch,
    default_algo: CompressionAlgo,
    mode: CompressionPreset,
    dictionaries: &DictionaryCatalog,
    scratch: &mut WorkerScratchArena,
) -> Result<ChunkEncodingPlan> {
    let weights = ObjectiveWeights::for_preset(mode);
    let source = batch.data();
    if source.is_empty() {
        return Ok(ChunkEncodingPlan::new(default_algo, mode, 0));
    }

    let sample_len = source.len().min(MAX_TRIAL_BYTES);
    let sample = &source[..sample_len];
    let mut best: Option<CandidatePlan> = None;

    for preset in PRESET_CANDIDATES {
        for dict_id in dictionaries.candidate_dict_ids(batch.file_type_hint) {
            let dictionary = dictionaries.get(dict_id);
            let started = Instant::now();
            let compressed = apply_compression_request_with_scratch(
                CompressionRequest {
                    data: sample,
                    algo: default_algo,
                    preset,
                    dictionary,
                },
                scratch.compression(),
            )?;
            let elapsed = started.elapsed();
            let stored_bytes = compressed.len().min(sample.len());
            let memory_bytes = dictionary.map_or(0, |dictionary| dictionary.len());
            let candidate = CandidatePlan {
                plan: ChunkEncodingPlan::new(default_algo, preset, dict_id),
                score: score_candidate(sample.len(), stored_bytes, elapsed, memory_bytes, weights),
                stored_bytes,
            };

            if better_candidate(candidate, best) {
                best = Some(candidate);
            }
        }
    }

    Ok(best
        .map(|candidate| candidate.plan)
        .unwrap_or_else(|| ChunkEncodingPlan::new(default_algo, mode, 0)))
}

pub fn record_planner_telemetry(summary: &PlannerSummary, elapsed: Duration) {
    let elapsed_us = duration_to_us(elapsed);
    let mode = preset_label(summary.mode);
    let chunking_mode = match summary.chunking_policy.mode {
        crate::io::ChunkingMode::Fixed => "fixed",
        crate::io::ChunkingMode::Adaptive => "adaptive",
    };
    let labels = [("mode", mode), ("chunking", chunking_mode)];

    telemetry::increment_counter(tags::METRIC_PLANNER_RUN_COUNT, 1, &labels);
    telemetry::record_histogram(tags::METRIC_PLANNER_RUN_LATENCY_US, elapsed_us, &labels);
    telemetry::record_histogram(
        tags::METRIC_PLANNER_CHUNK_COUNT,
        summary.chunk_count as u64,
        &labels,
    );
    telemetry::record_histogram(
        tags::METRIC_PLANNER_CHUNK_BYTES,
        summary.avg_chunk_bytes().round() as u64,
        &labels,
    );
    telemetry::record_histogram(
        tags::METRIC_PLANNER_DICTIONARY_COUNT,
        summary.dictionary_count as u64,
        &labels,
    );
    telemetry::record_histogram(
        tags::METRIC_PLANNER_DICTIONARY_BYTES,
        summary.dictionary_bytes as u64,
        &labels,
    );
    profile::event(
        tags::PROFILE_PLANNER,
        &[tags::TAG_SYSTEM, tags::TAG_PLANNER],
        "plan",
        "ok",
        elapsed_us,
        "planner completed",
    );
}

fn better_candidate(candidate: CandidatePlan, current: Option<CandidatePlan>) -> bool {
    match current {
        None => true,
        Some(current) => {
            candidate
                .score
                .total
                .partial_cmp(&current.score.total)
                .unwrap_or(Ordering::Equal)
                .then_with(|| current.stored_bytes.cmp(&candidate.stored_bytes))
                .then_with(|| current.plan.dict_id.cmp(&candidate.plan.dict_id))
                .then_with(|| {
                    preset_rank(current.plan.preset).cmp(&preset_rank(candidate.plan.preset))
                })
                == Ordering::Greater
        }
    }
}

fn preset_rank(preset: CompressionPreset) -> u8 {
    match preset {
        CompressionPreset::Fast => 0,
        CompressionPreset::Default => 1,
        CompressionPreset::High => 2,
    }
}

fn preset_label(preset: CompressionPreset) -> &'static str {
    match preset {
        CompressionPreset::Fast => "fast",
        CompressionPreset::Default => "balanced",
        CompressionPreset::High => "max_ratio",
    }
}
