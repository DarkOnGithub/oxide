use super::super::directory;
use super::super::types::ArchiveSourceKind;
use crate::core::{PoolRuntimeSnapshot, WorkerRuntimeSnapshot};
use crate::format::BlockHeader;
use crate::types::{Batch, duration_to_us};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct PreparedInput {
    pub source_kind: ArchiveSourceKind,
    pub batches: Vec<Batch>,
    pub input_bytes_total: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct BlockSizeDecision {
    pub selected_block_size: usize,
    pub autotune_requested: bool,
    pub autotune_ran: bool,
    pub sampled_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct BlockSizeScore {
    pub block_size: usize,
    pub throughput_bps: f64,
    pub output_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StageTimings {
    pub discovery: Duration,
    pub format_probe: Duration,
    pub producer_read: Duration,
    pub submit_wait: Duration,
    pub result_wait: Duration,
    pub writer: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessingThroughputSnapshot {
    pub preprocessing_input_bytes: u64,
    pub compression_input_bytes: u64,
    pub preprocessing_elapsed: Duration,
    pub compression_elapsed: Duration,
}

impl ProcessingThroughputSnapshot {
    pub fn preprocessing_avg_bps(self) -> f64 {
        throughput_bps(self.preprocessing_input_bytes, self.preprocessing_elapsed)
    }

    pub fn compression_avg_bps(self) -> f64 {
        throughput_bps(self.compression_input_bytes, self.compression_elapsed)
    }

    pub fn preprocessing_compression_avg_bps(self) -> f64 {
        throughput_bps(
            self.preprocessing_input_bytes,
            self.preprocessing_elapsed + self.compression_elapsed,
        )
    }

    pub fn preprocessing_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.preprocessing_input_bytes, elapsed)
    }

    pub fn compression_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.compression_input_bytes, elapsed)
    }

    pub fn preprocessing_compression_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.preprocessing_input_bytes, elapsed)
    }
}

#[derive(Debug, Default)]
pub struct ProcessingThroughputTotals {
    pub preprocessing_input_bytes: AtomicU64,
    pub compression_input_bytes: AtomicU64,
    pub preprocessing_elapsed_us: AtomicU64,
    pub compression_elapsed_us: AtomicU64,
}

impl ProcessingThroughputTotals {
    pub fn record(
        &self,
        preprocessing_input_bytes: u64,
        preprocessing_elapsed: Duration,
        compression_input_bytes: u64,
        compression_elapsed: Duration,
    ) {
        self.preprocessing_input_bytes
            .fetch_add(preprocessing_input_bytes, AtomicOrdering::AcqRel);
        self.compression_input_bytes
            .fetch_add(compression_input_bytes, AtomicOrdering::AcqRel);
        self.preprocessing_elapsed_us.fetch_add(
            duration_to_us(preprocessing_elapsed),
            AtomicOrdering::AcqRel,
        );
        self.compression_elapsed_us
            .fetch_add(duration_to_us(compression_elapsed), AtomicOrdering::AcqRel);
    }

    pub fn snapshot(&self) -> ProcessingThroughputSnapshot {
        ProcessingThroughputSnapshot {
            preprocessing_input_bytes: self.preprocessing_input_bytes.load(AtomicOrdering::Acquire),
            compression_input_bytes: self.compression_input_bytes.load(AtomicOrdering::Acquire),
            preprocessing_elapsed: Duration::from_micros(
                self.preprocessing_elapsed_us.load(AtomicOrdering::Acquire),
            ),
            compression_elapsed: Duration::from_micros(
                self.compression_elapsed_us.load(AtomicOrdering::Acquire),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExtractStageTimings {
    pub archive_read: Duration,
    pub decode_submit: Duration,
    pub decode_wait: Duration,
    pub merge: Duration,
    pub ordered_write: Duration,
    pub directory_decode: Duration,
    pub output_write: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExtractPipelineStats {
    pub decode_task_queue_capacity: usize,
    pub decode_task_queue_peak: usize,
    pub decode_result_queue_capacity: usize,
    pub decode_result_queue_peak: usize,
    pub reorder_pending_limit: usize,
    pub reorder_pending_peak: usize,
    pub reorder_pending_bytes_peak: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DirectoryProducerOutcome {
    pub producer_read: Duration,
}

#[derive(Debug)]
pub struct DirectoryWriterOutcome<W> {
    pub writer: W,
    pub output_bytes_written: u64,
    pub pending_write_peak: usize,
    pub writer_time: Duration,
}

#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    pub index: usize,
    pub file: directory::DirectoryFileSpec,
}

#[derive(Debug)]
pub struct PrefetchResult {
    pub index: usize,
    pub data: Vec<u8>,
    pub read_elapsed: Duration,
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
    pub header: BlockHeader,
    pub block_data: Vec<u8>,
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
