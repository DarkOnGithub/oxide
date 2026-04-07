use super::super::directory;
use super::super::types::ArchiveSourceKind;
use crate::buffer::PooledBuffer;
use crate::core::{PoolRuntimeSnapshot, WorkerRuntimeSnapshot};
use crate::format::{ArchiveManifest, ChunkDescriptor};
use crate::io::MmapInput;
use crate::types::{Batch, duration_to_us};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct PreparedInput {
    pub source_kind: ArchiveSourceKind,
    pub manifest: ArchiveManifest,
    pub batches: Vec<Batch>,
    pub input_bytes_total: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StageTimings {
    pub discovery: Duration,
    pub producer_read: Duration,
    pub producer_submit_blocked: Duration,
    pub submit_wait: Duration,
    pub result_wait: Duration,
    pub writer_enqueue_blocked: Duration,
    pub writer: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessingThroughputSnapshot {
    pub compression_input_bytes: u64,
    pub compression_elapsed: Duration,
}

impl ProcessingThroughputSnapshot {
    pub fn compression_avg_bps(self) -> f64 {
        throughput_bps(self.compression_input_bytes, self.compression_elapsed)
    }

    pub fn compression_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.compression_input_bytes, elapsed)
    }
}

#[derive(Debug, Default)]
pub struct ProcessingThroughputTotals {
    pub compression_input_bytes: AtomicU64,
    pub compression_elapsed_us: AtomicU64,
}

impl ProcessingThroughputTotals {
    pub fn record(&self, compression_input_bytes: u64, compression_elapsed: Duration) {
        self.compression_input_bytes
            .fetch_add(compression_input_bytes, AtomicOrdering::AcqRel);
        self.compression_elapsed_us
            .fetch_add(duration_to_us(compression_elapsed), AtomicOrdering::AcqRel);
    }

    pub fn snapshot(&self) -> ProcessingThroughputSnapshot {
        ProcessingThroughputSnapshot {
            compression_input_bytes: self.compression_input_bytes.load(AtomicOrdering::Acquire),
            compression_elapsed: Duration::from_micros(
                self.compression_elapsed_us.load(AtomicOrdering::Acquire),
            ),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExtractStageTimings {
    pub archive_read: Duration,
    pub decode_submit: Duration,
    pub decode_wait: Duration,
    pub writer_enqueue_blocked: Duration,
    pub merge: Duration,
    pub ordered_write: Duration,
    pub directory_decode: Duration,
    pub prepared_file_open: Duration,
    pub prepared_file_permit_wait: Duration,
    pub prepared_file_wait: Duration,
    pub output_prepare_directories: Duration,
    pub output_write: Duration,
    pub output_create: Duration,
    pub output_create_directories: Duration,
    pub output_create_files: Duration,
    pub output_data: Duration,
    pub output_flush: Duration,
    pub output_metadata: Duration,
    pub output_metadata_files: Duration,
    pub output_metadata_directories: Duration,
    pub file_transition_wait: Duration,
    pub write_shard_blocked: Vec<Duration>,
    pub write_shard_output_data: Vec<Duration>,
}

impl ExtractStageTimings {
    pub fn record_write_shard_blocked(&mut self, shard: usize, elapsed: Duration) {
        record_duration_by_shard(&mut self.write_shard_blocked, shard, elapsed);
    }

    pub fn record_write_shard_output_data(&mut self, shard: usize, elapsed: Duration) {
        record_duration_by_shard(&mut self.write_shard_output_data, shard, elapsed);
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExtractPipelineStats {
    pub decode_task_queue_capacity: usize,
    pub decode_task_queue_peak: usize,
    pub decode_result_queue_capacity: usize,
    pub decode_result_queue_peak: usize,
    pub ordered_write_queue_capacity: usize,
    pub ordered_write_queue_peak: usize,
    pub reorder_pending_limit: usize,
    pub reorder_pending_peak: usize,
    pub reorder_pending_bytes_peak: u64,
    pub write_shard_count: usize,
    pub write_shard_queue_peak: Vec<usize>,
    pub ready_file_frontier: usize,
    pub planner_ready_queue_peak: usize,
    pub active_files_peak: usize,
}

impl ExtractPipelineStats {
    pub fn set_write_shard_count(&mut self, shard_count: usize) {
        self.write_shard_count = self.write_shard_count.max(shard_count);
        if self.write_shard_queue_peak.len() < self.write_shard_count {
            self.write_shard_queue_peak
                .resize(self.write_shard_count, 0);
        }
    }

    pub fn record_write_shard_queue_peak(&mut self, shard: usize, peak: usize) {
        self.set_write_shard_count(shard.saturating_add(1));
        if let Some(slot) = self.write_shard_queue_peak.get_mut(shard) {
            *slot = (*slot).max(peak);
        }
    }

    pub fn record_ready_file_frontier(&mut self, frontier: usize) {
        self.ready_file_frontier = self.ready_file_frontier.max(frontier);
    }

    pub fn record_planner_ready_queue_peak(&mut self, peak: usize) {
        self.planner_ready_queue_peak = self.planner_ready_queue_peak.max(peak);
    }

    pub fn record_active_files_peak(&mut self, peak: usize) {
        self.active_files_peak = self.active_files_peak.max(peak);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DirectoryProducerOutcome {
    pub producer_read: Duration,
    pub producer_submit_blocked: Duration,
}

#[derive(Debug)]
pub struct DirectoryWriterOutcome<W> {
    pub writer: W,
    pub output_bytes_written: u64,
    pub pending_write_peak: usize,
    pub writer_time: Duration,
}

#[derive(Debug, Clone)]
pub(super) struct PrefetchRequest {
    pub index: usize,
    pub(super) file: directory::DirectoryFileSpec,
}

#[derive(Debug)]
pub enum PrefetchPayload {
    Owned(Vec<u8>),
    Mapped(MmapInput),
}

#[derive(Debug)]
pub struct PrefetchResult {
    pub index: usize,
    pub payload: PrefetchPayload,
    pub read_elapsed: Duration,
}

/// Compression-related knobs passed to stats/report builders.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompressionTuning {
    pub block_size: usize,
    pub raw_passthrough_blocks: u64,
    pub level: Option<i32>,
    pub lzma_extreme: bool,
    pub lzma_dictionary_size: Option<usize>,
}

/// Queue sizing and peak usage metrics for the archive pipeline.
#[derive(Debug, Clone, Copy, Default)]
pub struct PipelineQueueStats {
    pub max_inflight_blocks: usize,
    pub max_inflight_bytes: usize,
    pub configured_inflight_bytes: usize,
    pub max_inflight_blocks_per_worker: usize,
    pub writer_queue_capacity: usize,
    pub reorder_pending_limit: usize,
    pub pending_write_peak: usize,
    pub writer_queue_peak: usize,
}

#[derive(Debug)]
pub struct DecodedArchivePayload {
    pub flags: u32,
    pub payload: Vec<u8>,
    pub decoded_bytes_total: u64,
    pub archive_bytes_total: u64,
    pub blocks_total: u32,
    pub workers: Vec<WorkerRuntimeSnapshot>,
    pub stage_timings: ExtractStageTimings,
    pub pipeline_stats: ExtractPipelineStats,
}

#[derive(Debug)]
pub struct DecodeTask {
    pub index: usize,
    pub header: ChunkDescriptor,
    pub block_data: PooledBuffer,
    pub read_elapsed: Duration,
}

#[derive(Debug)]
pub struct DecodeWorkerOutcome {
    pub worker_id: usize,
    pub tasks_completed: usize,
    pub busy: Duration,
    pub uptime: Duration,
}

#[derive(Debug)]
pub struct DecodeRuntimeState {
    pub started_at: Instant,
    pub submitted: AtomicUsize,
    pub completed: AtomicUsize,
    pub worker_started_offsets_us: Vec<AtomicU64>,
    pub worker_stopped_offsets_us: Vec<AtomicU64>,
    pub worker_busy_us: Vec<AtomicU64>,
    pub worker_task_counts: Vec<AtomicUsize>,
}

impl DecodeRuntimeState {
    pub fn new(worker_count: usize, started_at: Instant) -> Self {
        Self {
            started_at,
            submitted: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            worker_started_offsets_us: (0..worker_count).map(|_| AtomicU64::new(0)).collect(),
            worker_stopped_offsets_us: (0..worker_count).map(|_| AtomicU64::new(0)).collect(),
            worker_busy_us: (0..worker_count).map(|_| AtomicU64::new(0)).collect(),
            worker_task_counts: (0..worker_count).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    pub fn mark_worker_started(&self, worker_id: usize) {
        let started_offset = duration_to_us(self.started_at.elapsed()).saturating_add(1);
        let _ = self.worker_started_offsets_us[worker_id].compare_exchange(
            0,
            started_offset,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        );
    }

    pub fn mark_worker_stopped(&self, worker_id: usize) {
        let stopped_offset = duration_to_us(self.started_at.elapsed()).saturating_add(1);
        self.worker_stopped_offsets_us[worker_id].store(stopped_offset, AtomicOrdering::Release);
    }

    pub fn record_submission(&self) {
        self.submitted.fetch_add(1, AtomicOrdering::AcqRel);
    }

    pub fn record_completion(&self) {
        self.completed.fetch_add(1, AtomicOrdering::AcqRel);
    }

    pub fn record_worker_task(&self, worker_id: usize, busy: Duration) {
        let busy_us = duration_to_us(busy);
        self.worker_busy_us[worker_id].fetch_add(busy_us, AtomicOrdering::AcqRel);
        self.worker_task_counts[worker_id].fetch_add(1, AtomicOrdering::AcqRel);
    }

    pub fn snapshot(&self) -> PoolRuntimeSnapshot {
        let elapsed = self.started_at.elapsed();
        let elapsed_us = duration_to_us(elapsed);
        let submitted = self.submitted.load(AtomicOrdering::Acquire);
        let completed = self.completed.load(AtomicOrdering::Acquire);
        let pending = submitted.saturating_sub(completed);

        let mut workers = Vec::with_capacity(self.worker_task_counts.len());
        for worker_id in 0..self.worker_task_counts.len() {
            let started_raw =
                self.worker_started_offsets_us[worker_id].load(AtomicOrdering::Acquire);
            let stopped_raw =
                self.worker_stopped_offsets_us[worker_id].load(AtomicOrdering::Acquire);
            let busy_us_raw = self.worker_busy_us[worker_id].load(AtomicOrdering::Acquire);

            let start_us = started_raw.saturating_sub(1);
            let stop_us = if stopped_raw == 0 {
                elapsed_us
            } else {
                stopped_raw.saturating_sub(1)
            };
            let uptime_us = if started_raw == 0 {
                0
            } else {
                stop_us.saturating_sub(start_us)
            };
            let busy_us = busy_us_raw.min(uptime_us);
            let idle_us = uptime_us.saturating_sub(busy_us);
            let utilization = if uptime_us == 0 {
                0.0
            } else {
                busy_us as f64 / uptime_us as f64
            };

            workers.push(WorkerRuntimeSnapshot {
                worker_id,
                tasks_completed: self.worker_task_counts[worker_id].load(AtomicOrdering::Acquire),
                uptime: Duration::from_micros(uptime_us),
                busy: Duration::from_micros(busy_us),
                idle: Duration::from_micros(idle_us),
                utilization,
            });
        }

        PoolRuntimeSnapshot {
            elapsed,
            submitted,
            completed,
            pending,
            workers,
        }
    }
}

#[inline]
pub fn throughput_bps(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if bytes == 0 || secs <= 0.0 || !secs.is_finite() {
        0.0
    } else {
        bytes as f64 / secs
    }
}

fn record_duration_by_shard(slots: &mut Vec<Duration>, shard: usize, elapsed: Duration) {
    if slots.len() <= shard {
        slots.resize(shard + 1, Duration::ZERO);
    }
    if let Some(slot) = slots.get_mut(shard) {
        *slot += elapsed;
    }
}
