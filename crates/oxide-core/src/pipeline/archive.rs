use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError, bounded};

use crate::buffer::BufferPool;
use crate::core::{PoolRuntimeSnapshot, WorkerPool, WorkerPoolHandle, WorkerRuntimeSnapshot};
use crate::format::{
    ArchiveReader, ArchiveWriter, BLOCK_HEADER_SIZE, BlockHeader, FOOTER_SIZE, GLOBAL_HEADER_SIZE,
};
use crate::io::{InputScanner, MmapInput};
use crate::telemetry::{
    self, ArchiveProgressEvent, ArchiveReport, ArchiveRun, ExtractProgressEvent, ExtractReport,
    ReportValue, RunTelemetryOptions, TelemetryEvent, TelemetrySink, ThreadReport, WorkerReport,
    profile, tags,
};
use crate::types::{
    Batch, CompressedBlock, CompressionAlgo, CompressionMeta, CompressionPreset, FileFormat,
    PreProcessingStrategy, Result, duration_to_us,
};

use super::directory::{self, DirectoryBatchSubmitter};
use super::types::{
    ArchivePipelineConfig, ArchiveSourceKind, PipelinePerformanceOptions, StatValue,
};

const SUBMISSION_DRAIN_BUDGET: usize = 128;
const DIRECTORY_FORMAT_PROBE_LIMIT: usize = 64 * 1024;
const DIRECTORY_PREFETCH_WINDOW: usize = 8;
const MIN_INFLIGHT_BLOCKS: usize = 64;
const MAX_INFLIGHT_BLOCKS: usize = 4096;
const PROFILE_TAG_STACK_PIPELINE: [&str; 2] = [tags::TAG_SYSTEM, tags::TAG_PIPELINE];
const AUTOTUNE_CANDIDATE_BLOCK_SIZES: [usize; 5] = [
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    4 * 1024 * 1024,
];

#[derive(Debug)]
struct PreparedInput {
    source_kind: ArchiveSourceKind,
    batches: Vec<Batch>,
    input_bytes_total: u64,
}

#[derive(Debug, Clone, Copy)]
struct BlockSizeDecision {
    selected_block_size: usize,
    autotune_requested: bool,
    autotune_ran: bool,
    sampled_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct BlockSizeScore {
    pub block_size: usize,
    pub throughput_bps: f64,
    pub output_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct StageTimings {
    discovery: Duration,
    format_probe: Duration,
    producer_read: Duration,
    submit_wait: Duration,
    result_wait: Duration,
    writer: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct ProcessingThroughputSnapshot {
    preprocessing_input_bytes: u64,
    compression_input_bytes: u64,
    preprocessing_elapsed: Duration,
    compression_elapsed: Duration,
}

impl ProcessingThroughputSnapshot {
    fn preprocessing_avg_bps(self) -> f64 {
        throughput_bps(self.preprocessing_input_bytes, self.preprocessing_elapsed)
    }

    fn compression_avg_bps(self) -> f64 {
        throughput_bps(self.compression_input_bytes, self.compression_elapsed)
    }

    fn preprocessing_compression_avg_bps(self) -> f64 {
        throughput_bps(
            self.preprocessing_input_bytes,
            self.preprocessing_elapsed + self.compression_elapsed,
        )
    }

    fn preprocessing_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.preprocessing_input_bytes, elapsed)
    }

    fn compression_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.compression_input_bytes, elapsed)
    }

    fn preprocessing_compression_wall_avg_bps(self, elapsed: Duration) -> f64 {
        throughput_bps(self.preprocessing_input_bytes, elapsed)
    }
}

#[derive(Debug, Default)]
struct ProcessingThroughputTotals {
    preprocessing_input_bytes: AtomicU64,
    compression_input_bytes: AtomicU64,
    preprocessing_elapsed_us: AtomicU64,
    compression_elapsed_us: AtomicU64,
}

impl ProcessingThroughputTotals {
    fn record(
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

    fn snapshot(&self) -> ProcessingThroughputSnapshot {
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
struct ExtractStageTimings {
    archive_read: Duration,
    decode_submit: Duration,
    decode_wait: Duration,
    merge: Duration,
    directory_decode: Duration,
    output_write: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct DirectoryProducerOutcome {
    producer_read: Duration,
}

#[derive(Debug)]
struct DirectoryWriterOutcome<W> {
    writer: W,
    output_bytes_written: u64,
    pending_write_peak: usize,
    writer_time: Duration,
}

#[derive(Debug, Clone)]
struct PrefetchRequest {
    index: usize,
    file: directory::DirectoryFileSpec,
}

#[derive(Debug)]
struct PrefetchResult {
    index: usize,
    data: Vec<u8>,
    read_elapsed: Duration,
}

#[derive(Debug)]
struct DecodedArchivePayload {
    flags: u32,
    payload: Vec<u8>,
    archive_bytes_total: u64,
    blocks_total: u32,
    workers: Vec<WorkerRuntimeSnapshot>,
    stage_timings: ExtractStageTimings,
}

#[derive(Debug)]
struct DecodeTask {
    index: usize,
    header: BlockHeader,
    block_data: Vec<u8>,
}

#[derive(Debug)]
struct DecodeWorkerOutcome {
    worker_id: usize,
    tasks_completed: usize,
    busy: Duration,
    uptime: Duration,
}

#[derive(Debug)]
struct DecodeRuntimeState {
    started_at: Instant,
    submitted: AtomicUsize,
    completed: AtomicUsize,
    worker_started_offsets_us: Vec<AtomicU64>,
    worker_stopped_offsets_us: Vec<AtomicU64>,
    worker_busy_us: Vec<AtomicU64>,
    worker_task_counts: Vec<AtomicUsize>,
}

impl DecodeRuntimeState {
    fn new(worker_count: usize, started_at: Instant) -> Self {
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

    fn mark_worker_started(&self, worker_id: usize) {
        let started_offset = duration_to_us(self.started_at.elapsed()).saturating_add(1);
        let _ = self.worker_started_offsets_us[worker_id].compare_exchange(
            0,
            started_offset,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        );
    }

    fn mark_worker_stopped(&self, worker_id: usize) {
        let stopped_offset = duration_to_us(self.started_at.elapsed()).saturating_add(1);
        self.worker_stopped_offsets_us[worker_id].store(stopped_offset, AtomicOrdering::Release);
    }

    fn record_submission(&self) {
        self.submitted.fetch_add(1, AtomicOrdering::AcqRel);
    }

    fn record_completion(&self) {
        self.completed.fetch_add(1, AtomicOrdering::AcqRel);
    }

    fn record_worker_task(&self, worker_id: usize, busy: Duration) {
        let busy_us = duration_to_us(busy);
        self.worker_busy_us[worker_id].fetch_add(busy_us, AtomicOrdering::AcqRel);
        self.worker_task_counts[worker_id].fetch_add(1, AtomicOrdering::AcqRel);
    }

    fn snapshot(&self) -> PoolRuntimeSnapshot {
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
fn throughput_bps(bytes: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if bytes == 0 || secs <= 0.0 || !secs.is_finite() {
        0.0
    } else {
        bytes as f64 / secs
    }
}

struct NoopTelemetrySink;

impl TelemetrySink for NoopTelemetrySink {
    fn on_event(&mut self, _event: TelemetryEvent) {}
}

/// This pipeline performs pass-through processing for block payloads while preserving
/// the metadata and ordering guarantees needed by the archive format.
pub struct ArchivePipeline {
    scanner: InputScanner,
    num_workers: usize,
    compression_algo: CompressionAlgo,
    buffer_pool: Arc<BufferPool>,
    performance: PipelinePerformanceOptions,
}
impl ArchivePipeline {
    /// Creates a new archive pipeline.
    pub fn new(config: ArchivePipelineConfig) -> Self {
        Self {
            scanner: InputScanner::new(config.target_block_size),
            num_workers: config.workers.max(1),
            compression_algo: config.compression_algo,
            buffer_pool: config.buffer_pool,
            performance: config.performance,
        }
    }

    /// Reads an input file, processes blocks in parallel, and writes an OXZ archive.
    pub fn archive_file<P, W>(
        &self,
        path: P,
        writer: W,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ArchiveRun<W>>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let path = path.as_ref();
        tracing::info!(path = %path.display(), "starting file archival");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        self.archive_file_path_with(path, writer, &options, sink)
    }

    /// Archives either a single file or a directory tree.
    pub fn archive_path<P, W>(
        &self,
        path: P,
        writer: W,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ArchiveRun<W>>
    where
        P: AsRef<Path>,
        W: Write + Send + 'static,
    {
        let path = path.as_ref();
        tracing::info!(path = %path.display(), "starting path archival");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            self.archive_file_path_with(path, writer, &options, sink)
        } else if metadata.is_dir() {
            self.archive_directory_path_with(path, writer, &options, sink)
        } else {
            Err(crate::OxideError::InvalidFormat(
                "path must be a file or directory",
            ))
        }
    }

    /// Archives a directory tree as a single OXZ payload.
    pub fn archive_directory<P, W>(
        &self,
        dir_path: P,
        writer: W,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ArchiveRun<W>>
    where
        P: AsRef<Path>,
        W: Write + Send + 'static,
    {
        let path = dir_path.as_ref();
        tracing::info!(path = %path.display(), "starting directory archival");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        self.archive_directory_path_with(path, writer, &options, sink)
    }

    fn archive_file_path_with<W>(
        &self,
        path: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>>
    where
        W: Write,
    {
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(crate::OxideError::InvalidFormat(
                "archive_file expects a file path",
            ));
        }
        let block_size = self.choose_block_size_for_file(path, metadata.len())?;
        let prepared = self.prepare_file(path, block_size.selected_block_size)?;
        self.archive_prepared_with(prepared, writer, options, sink, block_size)
    }

    fn archive_directory_path_with<W>(
        &self,
        dir_path: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>>
    where
        W: Write + Send + 'static,
    {
        self.archive_directory_streaming_with(dir_path, writer, options, sink)
    }

    /// Extracts all block payload bytes from an OXZ archive in block order and
    /// returns a detailed report.
    pub fn extract_archive<R: Read + std::io::Seek>(
        &self,
        reader: R,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<(Vec<u8>, ExtractReport)> {
        tracing::info!("starting archive extraction");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let started_at = Instant::now();
        let mut decoded = Self::read_archive_payload_with_metrics(
            reader,
            self.num_workers,
            started_at,
            &options,
            sink,
        )?;
        let source_kind = directory::source_kind_from_flags(decoded.flags);
        let extensions = Self::extract_extensions_from_flags(decoded.flags);
        let decoded_bytes_total = decoded.payload.len() as u64;
        let stage_timings = decoded.stage_timings;
        let payload = std::mem::take(&mut decoded.payload);
        let report = Self::build_extract_report_from_decoded(
            started_at,
            decoded,
            source_kind,
            decoded_bytes_total,
            decoded_bytes_total,
            extensions,
            options,
        );
        Self::record_extract_run_telemetry(report.elapsed, stage_timings);

        sink.on_event(TelemetryEvent::ExtractCompleted(report.clone()));

        Ok((payload, report))
    }

    /// Extracts a directory tree archive produced by [`archive_directory`].
    pub fn extract_directory_archive<R, P>(
        &self,
        reader: R,
        output_dir: P,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ExtractReport>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        tracing::info!(output_dir = %output_dir.as_ref().display(), "starting directory archive extraction");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let started_at = Instant::now();
        let mut decoded = Self::read_archive_payload_with_metrics(
            reader,
            self.num_workers,
            started_at,
            &options,
            sink,
        )?;
        let mut extensions = Self::extract_extensions_from_flags(decoded.flags);
        let decoded_bytes_total = decoded.payload.len() as u64;

        let directory_decode_started = Instant::now();
        let entries = directory::decode_directory_entries(&decoded.payload, decoded.flags)?.ok_or(
            crate::OxideError::InvalidFormat("archive does not contain a directory bundle"),
        )?;
        decoded.stage_timings.directory_decode += directory_decode_started.elapsed();

        let output_bytes_total = entries
            .iter()
            .filter_map(|entry| match entry {
                directory::DirectoryBundleEntry::File { data, .. } => Some(data.len() as u64),
                directory::DirectoryBundleEntry::Directory { .. } => None,
            })
            .sum();
        extensions.insert(
            "extract.directory_entries".to_string(),
            ReportValue::U64(entries.len() as u64),
        );

        let write_started = Instant::now();
        directory::write_directory_entries(output_dir.as_ref(), entries)?;
        decoded.stage_timings.output_write += write_started.elapsed();
        let stage_timings = decoded.stage_timings;

        let report = Self::build_extract_report_from_decoded(
            started_at,
            decoded,
            ArchiveSourceKind::Directory,
            decoded_bytes_total,
            output_bytes_total,
            extensions,
            options,
        );
        Self::record_extract_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ExtractCompleted(report.clone()));
        Ok(report)
    }

    /// Extracts an archive to `output_path` and returns the extract report.
    ///
    /// File payloads are written to `output_path` as a single file.
    /// Directory payloads are restored under `output_path` as a root directory.
    pub fn extract_path<R, P>(
        &self,
        reader: R,
        output_path: P,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ExtractReport>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        tracing::info!(output_path = %output_path.as_ref().display(), "starting path extraction");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let started_at = Instant::now();
        let mut decoded = Self::read_archive_payload_with_metrics(
            reader,
            self.num_workers,
            started_at,
            &options,
            sink,
        )?;
        let mut extensions = Self::extract_extensions_from_flags(decoded.flags);
        let decoded_bytes_total = decoded.payload.len() as u64;
        let (source_kind, output_bytes_total) =
            Self::restore_decoded_payload(output_path.as_ref(), &mut decoded, &mut extensions)?;
        let stage_timings = decoded.stage_timings;
        let report = Self::build_extract_report_from_decoded(
            started_at,
            decoded,
            source_kind,
            decoded_bytes_total,
            output_bytes_total,
            extensions,
            options,
        );
        Self::record_extract_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ExtractCompleted(report.clone()));
        Ok(report)
    }

    fn extract_extensions_from_flags(flags: u32) -> BTreeMap<String, ReportValue> {
        let mut extensions = BTreeMap::new();
        extensions.insert("extract.flags".to_string(), ReportValue::U64(flags as u64));
        extensions
    }

    fn restore_decoded_payload(
        output_path: &Path,
        decoded: &mut DecodedArchivePayload,
        extensions: &mut BTreeMap<String, ReportValue>,
    ) -> Result<(ArchiveSourceKind, u64)> {
        let directory_decode_started = Instant::now();
        if let Some(entries) = directory::decode_directory_entries(&decoded.payload, decoded.flags)?
        {
            decoded.stage_timings.directory_decode += directory_decode_started.elapsed();
            let output_bytes_total = entries
                .iter()
                .filter_map(|entry| match entry {
                    directory::DirectoryBundleEntry::File { data, .. } => Some(data.len() as u64),
                    directory::DirectoryBundleEntry::Directory { .. } => None,
                })
                .sum();
            extensions.insert(
                "extract.directory_entries".to_string(),
                ReportValue::U64(entries.len() as u64),
            );

            let write_started = Instant::now();
            directory::write_directory_entries(output_path, entries)?;
            decoded.stage_timings.output_write += write_started.elapsed();
            Ok((ArchiveSourceKind::Directory, output_bytes_total))
        } else {
            decoded.stage_timings.directory_decode += directory_decode_started.elapsed();
            if let Some(parent) = output_path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let write_started = Instant::now();
            fs::write(output_path, &decoded.payload)?;
            decoded.stage_timings.output_write += write_started.elapsed();
            Ok((ArchiveSourceKind::File, decoded.payload.len() as u64))
        }
    }

    fn build_extract_report_from_decoded(
        started_at: Instant,
        decoded: DecodedArchivePayload,
        source_kind: ArchiveSourceKind,
        decoded_bytes_total: u64,
        output_bytes_total: u64,
        extensions: BTreeMap<String, ReportValue>,
        options: RunTelemetryOptions,
    ) -> ExtractReport {
        Self::build_extract_report(
            source_kind,
            started_at.elapsed(),
            decoded.archive_bytes_total,
            decoded_bytes_total,
            output_bytes_total,
            decoded.blocks_total,
            decoded.workers,
            decoded.stage_timings,
            extensions,
            options,
        )
    }

    fn read_archive_payload_with_metrics<R: Read + std::io::Seek>(
        reader: R,
        workers: usize,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let mut archive = ArchiveReader::new(reader)?;
        let flags = archive.global_header().flags;
        let source_kind = directory::source_kind_from_flags(flags);
        let block_capacity = archive.block_count() as usize;
        let mut expected_total = 0usize;
        let worker_count = workers.max(1);
        let queue_capacity = worker_count.saturating_mul(4).max(1);
        let (task_tx, task_rx) = bounded::<DecodeTask>(queue_capacity);
        let (result_tx, result_rx) = bounded::<(usize, Result<Vec<u8>>)>(queue_capacity);
        let runtime_state = Arc::new(DecodeRuntimeState::new(worker_count, started_at));
        let mut worker_handles = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let local_task_rx = task_rx.clone();
            let local_result_tx = result_tx.clone();
            let local_runtime = Arc::clone(&runtime_state);
            let handle = thread::spawn(move || -> DecodeWorkerOutcome {
                let started = Instant::now();
                let mut tasks_completed = 0usize;
                let mut busy = Duration::ZERO;
                local_runtime.mark_worker_started(worker_id);

                while let Ok(task) = local_task_rx.recv() {
                    let decode_started = Instant::now();
                    let decoded = Self::decode_block_payload(task.header, task.block_data);
                    let busy_elapsed = decode_started.elapsed();
                    busy += busy_elapsed;
                    local_runtime.record_worker_task(worker_id, busy_elapsed);
                    tasks_completed += 1;
                    if local_result_tx.send((task.index, decoded)).is_err() {
                        break;
                    }
                }
                local_runtime.mark_worker_stopped(worker_id);

                DecodeWorkerOutcome {
                    worker_id,
                    tasks_completed,
                    busy,
                    uptime: started.elapsed(),
                }
            });
            worker_handles.push(handle);
        }
        drop(result_tx);

        let mut stage_timings = ExtractStageTimings::default();
        let mut archive_bytes_total = GLOBAL_HEADER_SIZE as u64 + FOOTER_SIZE as u64;
        let mut submitted = 0usize;
        let mut received = 0usize;
        let mut first_error: Option<crate::OxideError> = None;
        let mut decoded_bytes_completed = 0u64;
        let mut decoded_blocks = vec![None; block_capacity];
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        for entry in archive.iter_blocks() {
            let read_started = Instant::now();
            let (header, block_data) = entry?;
            stage_timings.archive_read += read_started.elapsed();
            expected_total = expected_total.saturating_add(header.original_size as usize);
            archive_bytes_total = archive_bytes_total
                .saturating_add(BLOCK_HEADER_SIZE as u64)
                .saturating_add(block_data.len() as u64);

            while submitted.saturating_sub(received) >= queue_capacity {
                Self::receive_decode_result(
                    &result_rx,
                    &mut stage_timings,
                    &mut decoded_blocks,
                    &runtime_state,
                    &mut decoded_bytes_completed,
                    &mut received,
                    &mut first_error,
                )?;
                Self::emit_extract_progress_if_due(
                    source_kind,
                    started_at,
                    archive_bytes_total,
                    decoded_bytes_completed,
                    block_capacity as u32,
                    received as u32,
                    runtime_state.snapshot(),
                    emit_every,
                    &mut last_emit_at,
                    false,
                    sink,
                );
            }

            let submit_started = Instant::now();
            task_tx
                .send(DecodeTask {
                    index: submitted,
                    header,
                    block_data,
                })
                .map_err(|_| {
                    crate::OxideError::CompressionError(
                        "decode queue closed before submission completed".to_string(),
                    )
                })?;
            stage_timings.decode_submit += submit_started.elapsed();
            submitted += 1;
            runtime_state.record_submission();
            Self::emit_extract_progress_if_due(
                source_kind,
                started_at,
                archive_bytes_total,
                decoded_bytes_completed,
                block_capacity as u32,
                received as u32,
                runtime_state.snapshot(),
                emit_every,
                &mut last_emit_at,
                false,
                sink,
            );
        }
        drop(task_tx);

        if submitted != block_capacity {
            return Err(crate::OxideError::InvalidFormat(
                "archive block count mismatch during decode",
            ));
        }

        while received < submitted {
            Self::receive_decode_result(
                &result_rx,
                &mut stage_timings,
                &mut decoded_blocks,
                &runtime_state,
                &mut decoded_bytes_completed,
                &mut received,
                &mut first_error,
            )?;
            Self::emit_extract_progress_if_due(
                source_kind,
                started_at,
                archive_bytes_total,
                decoded_bytes_completed,
                block_capacity as u32,
                received as u32,
                runtime_state.snapshot(),
                emit_every,
                &mut last_emit_at,
                false,
                sink,
            );
        }

        let workers = Self::join_decode_workers(worker_handles)?;
        if let Some(error) = first_error {
            return Err(error);
        }

        let merge_started = Instant::now();
        let mut output = Vec::with_capacity(expected_total);
        for block in decoded_blocks {
            let block = block.ok_or(crate::OxideError::InvalidFormat(
                "missing decoded block payload",
            ))?;
            output.extend_from_slice(&block);
        }
        stage_timings.merge += merge_started.elapsed();

        if options.emit_final_progress {
            Self::emit_extract_progress(
                source_kind,
                started_at,
                archive_bytes_total,
                decoded_bytes_completed,
                block_capacity as u32,
                received as u32,
                runtime_state.snapshot(),
                sink,
            );
        }

        Ok(DecodedArchivePayload {
            flags,
            payload: output,
            archive_bytes_total,
            blocks_total: submitted as u32,
            workers,
            stage_timings,
        })
    }

    fn receive_decode_result(
        result_rx: &Receiver<(usize, Result<Vec<u8>>)>,
        stage_timings: &mut ExtractStageTimings,
        decoded_blocks: &mut [Option<Vec<u8>>],
        runtime_state: &DecodeRuntimeState,
        decoded_bytes_completed: &mut u64,
        received: &mut usize,
        first_error: &mut Option<crate::OxideError>,
    ) -> Result<()> {
        let wait_started = Instant::now();
        let (index, block) = result_rx.recv().map_err(|_| {
            crate::OxideError::CompressionError(
                "decode result channel closed before completion".to_string(),
            )
        })?;
        stage_timings.decode_wait += wait_started.elapsed();
        if index >= decoded_blocks.len() {
            return Err(crate::OxideError::InvalidFormat(
                "decode result index out of bounds",
            ));
        }
        match block {
            Ok(bytes) => {
                if decoded_blocks[index].is_some() {
                    return Err(crate::OxideError::InvalidFormat(
                        "duplicate decode result index",
                    ));
                }
                *decoded_bytes_completed =
                    decoded_bytes_completed.saturating_add(bytes.len() as u64);
                decoded_blocks[index] = Some(bytes);
            }
            Err(error) => {
                if first_error.is_none() {
                    *first_error = Some(error);
                }
            }
        }
        *received += 1;
        runtime_state.record_completion();
        Ok(())
    }

    fn join_decode_workers(
        handles: Vec<thread::JoinHandle<DecodeWorkerOutcome>>,
    ) -> Result<Vec<WorkerRuntimeSnapshot>> {
        let mut workers = Vec::with_capacity(handles.len());
        for handle in handles {
            let outcome = handle.join().map_err(|payload| {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                crate::OxideError::CompressionError(format!(
                    "decode worker thread panicked: {details}"
                ))
            })?;
            let busy = outcome.busy.min(outcome.uptime);
            let idle = outcome.uptime.saturating_sub(busy);
            let utilization = if outcome.uptime == Duration::ZERO {
                0.0
            } else {
                busy.as_secs_f64() / outcome.uptime.as_secs_f64()
            };
            workers.push(WorkerRuntimeSnapshot {
                worker_id: outcome.worker_id,
                tasks_completed: outcome.tasks_completed,
                uptime: outcome.uptime,
                busy,
                idle,
                utilization,
            });
        }
        workers.sort_by_key(|worker| worker.worker_id);
        Ok(workers)
    }

    fn decode_block_payload(header: BlockHeader, block_data: Vec<u8>) -> Result<Vec<u8>> {
        let compression_meta = header.compression_meta()?;
        let decoded = if compression_meta.raw_passthrough {
            block_data
        } else {
            crate::compression::reverse_compression(&block_data, compression_meta.algo)?
        };
        let strategy = header.strategy()?;
        let restored = crate::preprocessing::reverse_preprocessing(&decoded, &strategy)?;
        if restored.len() != header.original_size as usize {
            return Err(crate::OxideError::InvalidFormat(
                "decoded block size mismatch",
            ));
        }
        Ok(restored)
    }

    #[allow(clippy::too_many_arguments)]
    fn build_extract_report(
        source_kind: ArchiveSourceKind,
        elapsed: Duration,
        archive_bytes_total: u64,
        decoded_bytes_total: u64,
        output_bytes_total: u64,
        blocks_total: u32,
        worker_runtime: Vec<WorkerRuntimeSnapshot>,
        stage_timings: ExtractStageTimings,
        mut extensions: BTreeMap<String, ReportValue>,
        options: RunTelemetryOptions,
    ) -> ExtractReport {
        let decode_busy_us = worker_runtime
            .iter()
            .map(|worker| worker.busy.as_micros())
            .sum::<u128>()
            .min(u64::MAX as u128) as u64;
        let elapsed_us = elapsed.as_micros().max(1).min(u64::MAX as u128) as u64;
        let effective_cores = decode_busy_us as f64 / elapsed_us as f64;

        extensions.insert(
            "runtime.decode_busy_us".to_string(),
            ReportValue::U64(decode_busy_us),
        );
        extensions.insert(
            "runtime.effective_cores".to_string(),
            ReportValue::F64(effective_cores),
        );
        extensions.insert(
            "runtime.worker_count".to_string(),
            ReportValue::U64(worker_runtime.len() as u64),
        );

        let workers = worker_runtime
            .iter()
            .map(WorkerReport::from_runtime)
            .collect::<Vec<_>>();
        let mut main_thread = ThreadReport::new("main");
        main_thread.stage_us.insert(
            "archive_read".to_string(),
            stage_timings.archive_read.as_micros().min(u64::MAX as u128) as u64,
        );
        main_thread.stage_us.insert(
            "decode_submit".to_string(),
            stage_timings
                .decode_submit
                .as_micros()
                .min(u64::MAX as u128) as u64,
        );
        main_thread.stage_us.insert(
            "decode_wait".to_string(),
            stage_timings.decode_wait.as_micros().min(u64::MAX as u128) as u64,
        );
        main_thread.stage_us.insert(
            "merge".to_string(),
            stage_timings.merge.as_micros().min(u64::MAX as u128) as u64,
        );
        main_thread.stage_us.insert(
            "directory_decode".to_string(),
            stage_timings
                .directory_decode
                .as_micros()
                .min(u64::MAX as u128) as u64,
        );
        main_thread.stage_us.insert(
            "output_write".to_string(),
            stage_timings.output_write.as_micros().min(u64::MAX as u128) as u64,
        );

        ExtractReport::new(
            source_kind,
            elapsed,
            archive_bytes_total,
            decoded_bytes_total,
            output_bytes_total,
            blocks_total,
            workers,
            main_thread,
            extensions,
            options,
        )
    }

    fn archive_directory_streaming_with<W: Write>(
        &self,
        root: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>>
    where
        W: Write + Send + 'static,
    {
        let mut stage_timings = StageTimings::default();

        let discovery_started = Instant::now();
        let discovery = directory::discover_directory_tree(root)?;
        stage_timings.discovery += discovery_started.elapsed();

        let block_size = self.choose_block_size_for_directory(&discovery)?;
        let format_probe_started = Instant::now();
        let file_formats =
            directory::detect_file_formats(&discovery, DIRECTORY_FORMAT_PROBE_LIMIT)?;
        stage_timings.format_probe += format_probe_started.elapsed();
        let block_count = directory::estimate_directory_block_count(
            &discovery,
            &file_formats,
            block_size.selected_block_size,
            self.performance.preserve_directory_format_boundaries,
        )?;
        let input_bytes_total = discovery.input_bytes_total;
        let total_blocks = usize::try_from(block_count)
            .map_err(|_| crate::OxideError::InvalidFormat("block count exceeds usize range"))?;
        let max_inflight_blocks = Self::max_inflight_blocks(
            total_blocks,
            self.num_workers,
            block_size.selected_block_size,
            &self.performance,
        );
        let max_inflight_bytes = max_inflight_blocks.saturating_mul(block_size.selected_block_size);

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );
        let processing_totals = Arc::new(ProcessingThroughputTotals::default());
        let compression_preset = self.performance.compression_preset;
        let raw_fallback_enabled = self.performance.raw_fallback_enabled;
        let worker_processing_totals = Arc::clone(&processing_totals);
        let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression| {
            Self::process_batch(
                batch,
                pool,
                compression,
                compression_preset,
                raw_fallback_enabled,
                worker_processing_totals.as_ref(),
            )
        });

        let writer_queue_capacity = self
            .performance
            .writer_result_queue_blocks
            .max(1)
            .min(max_inflight_blocks.max(1));
        let (writer_tx, writer_rx) = bounded::<CompressedBlock>(writer_queue_capacity);
        let writer_output_bytes = Arc::new(AtomicU64::new(GLOBAL_HEADER_SIZE as u64));
        let writer_output_bytes_shared = Arc::clone(&writer_output_bytes);
        let writer_buffer_pool = Arc::clone(&self.buffer_pool);
        let writer_handle = thread::spawn(move || -> Result<DirectoryWriterOutcome<W>> {
            let mut archive_writer = ArchiveWriter::new(writer, writer_buffer_pool);
            archive_writer.write_global_header_with_flags(
                block_count,
                directory::source_kind_flags(ArchiveSourceKind::Directory),
            )?;

            let mut output_bytes_written = GLOBAL_HEADER_SIZE as u64;
            let mut pending_sizes = BTreeMap::<usize, usize>::new();
            let mut next_written_id = 0usize;
            let mut pending_write_peak = 0usize;
            let mut writer_time = Duration::ZERO;

            while let Ok(block) = writer_rx.recv() {
                let block_id = block.id;
                let block_len = block.data.len();
                if pending_sizes.insert(block_id, block_len).is_some() {
                    return Err(crate::OxideError::InvalidFormat(
                        "duplicate block id received by directory writer",
                    ));
                }

                let write_started = Instant::now();
                let written = archive_writer.push_block(block)?;
                writer_time += write_started.elapsed();

                for _ in 0..written {
                    let len = pending_sizes.remove(&next_written_id).ok_or(
                        crate::OxideError::InvalidFormat(
                            "directory writer pending state drift detected",
                        ),
                    )?;
                    output_bytes_written = output_bytes_written
                        .saturating_add(BLOCK_HEADER_SIZE as u64)
                        .saturating_add(len as u64);
                    next_written_id += 1;
                }
                pending_write_peak = pending_write_peak.max(archive_writer.pending_blocks());
                writer_output_bytes_shared.store(output_bytes_written, AtomicOrdering::Release);
            }

            if !pending_sizes.is_empty() {
                return Err(crate::OxideError::InvalidFormat(
                    "directory writer closed with pending blocks",
                ));
            }

            let writer = archive_writer.write_footer()?;
            output_bytes_written = output_bytes_written.saturating_add(FOOTER_SIZE as u64);
            writer_output_bytes_shared.store(output_bytes_written, AtomicOrdering::Release);

            Ok(DirectoryWriterOutcome {
                writer,
                output_bytes_written,
                pending_write_peak,
                writer_time,
            })
        });

        let started_at = Instant::now();
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut completed_bytes = 0u64;
        let mut received_count = 0usize;
        let mut submitted_count = 0usize;
        let mut first_error: Option<crate::OxideError> = None;
        let mut output_bytes_written = GLOBAL_HEADER_SIZE as u64;
        let mut raw_passthrough_blocks = 0u64;
        let mut writer_queue_peak = 0usize;
        let result_wait_timeout = self
            .performance
            .result_wait_timeout
            .max(Duration::from_millis(1));

        let (batch_tx, batch_rx) = bounded::<Batch>(max_inflight_blocks.max(1));
        let producer_root = discovery.root.clone();
        let producer_directories = discovery.directories.clone();
        let producer_files = discovery.files.clone();
        let producer_file_formats = file_formats.clone();
        let producer_block_size = block_size.selected_block_size;
        let preserve_boundaries = self.performance.preserve_directory_format_boundaries;
        let stream_read_buffer_size = self.performance.directory_stream_read_buffer_size.max(1);
        let producer_threads = self.performance.producer_threads.clamp(1, 2);
        let mmap_threshold = self.performance.directory_mmap_threshold_bytes.max(1);

        let producer_handle = thread::spawn(move || -> Result<DirectoryProducerOutcome> {
            let mut submitter = DirectoryBatchSubmitter::new(
                producer_root,
                producer_block_size,
                preserve_boundaries,
            );
            let mut producer_read = Duration::ZERO;

            let submit_batch = |batch: Batch| -> Result<()> {
                batch_tx.send(batch).map_err(|_| {
                    crate::OxideError::CompressionError(
                        "directory producer channel closed before completion".to_string(),
                    )
                })
            };

            let entry_count = producer_directories
                .len()
                .checked_add(producer_files.len())
                .ok_or(crate::OxideError::InvalidFormat(
                    "directory entry count overflow",
                ))?;
            let entry_count = u32::try_from(entry_count)
                .map_err(|_| crate::OxideError::InvalidFormat("directory entry count overflow"))?;

            let mut bundle_header = Vec::with_capacity(10);
            bundle_header.extend_from_slice(&directory::DIRECTORY_BUNDLE_MAGIC);
            bundle_header.extend_from_slice(&directory::DIRECTORY_BUNDLE_VERSION.to_le_bytes());
            bundle_header.extend_from_slice(&entry_count.to_le_bytes());
            submitter.push_bytes_with_hint(&bundle_header, FileFormat::Common, |batch| {
                submit_batch(batch)
            })?;

            for rel_path in &producer_directories {
                let mut encoded = Vec::with_capacity(1 + 4 + rel_path.len());
                encoded.push(0);
                directory::encode_path(&mut encoded, rel_path)?;
                submitter.push_bytes_with_hint(&encoded, FileFormat::Common, |batch| {
                    submit_batch(batch)
                })?;
            }

            let mut prefetch_request_tx = None;
            let mut prefetch_result_rx = None;
            let mut prefetch_handle = None;
            if producer_threads > 1 {
                let (prefetch_tx, prefetch_rx) =
                    bounded::<PrefetchRequest>(DIRECTORY_PREFETCH_WINDOW);
                let (result_tx, result_rx) = bounded::<PrefetchResult>(DIRECTORY_PREFETCH_WINDOW);
                let handle = thread::spawn(move || -> Result<()> {
                    while let Ok(request) = prefetch_rx.recv() {
                        let read_started = Instant::now();
                        let data = fs::read(&request.file.full_path)?;
                        let read_elapsed = read_started.elapsed();
                        result_tx
                            .send(PrefetchResult {
                                index: request.index,
                                data,
                                read_elapsed,
                            })
                            .map_err(|_| {
                                crate::OxideError::CompressionError(
                                    "directory prefetch result queue closed".to_string(),
                                )
                            })?;
                    }
                    Ok(())
                });
                prefetch_request_tx = Some(prefetch_tx);
                prefetch_result_rx = Some(result_rx);
                prefetch_handle = Some(handle);
            }

            let mut prefetched = BTreeMap::<usize, PrefetchResult>::new();
            let mut next_prefetch = 0usize;
            let mut read_buffer = vec![0u8; stream_read_buffer_size];
            for (file_index, (file, file_format)) in producer_files
                .iter()
                .zip(producer_file_formats.iter().copied())
                .enumerate()
            {
                if let Some(prefetch_tx) = prefetch_request_tx.as_ref() {
                    while next_prefetch < producer_files.len()
                        && next_prefetch <= file_index.saturating_add(DIRECTORY_PREFETCH_WINDOW)
                    {
                        let candidate = &producer_files[next_prefetch];
                        let candidate_size = usize::try_from(candidate.size).unwrap_or(usize::MAX);
                        if candidate_size > 0 && candidate_size <= mmap_threshold {
                            prefetch_tx
                                .send(PrefetchRequest {
                                    index: next_prefetch,
                                    file: candidate.clone(),
                                })
                                .map_err(|_| {
                                    crate::OxideError::CompressionError(
                                        "directory prefetch queue closed before completion"
                                            .to_string(),
                                    )
                                })?;
                        }
                        next_prefetch += 1;
                    }
                }

                let mut encoded = Vec::with_capacity(1 + 4 + file.rel_path.len() + 8);
                encoded.push(1);
                directory::encode_path(&mut encoded, &file.rel_path)?;
                encoded.extend_from_slice(&file.size.to_le_bytes());
                submitter.push_bytes_with_hint(&encoded, FileFormat::Common, |batch| {
                    submit_batch(batch)
                })?;

                let file_size = usize::try_from(file.size).unwrap_or(usize::MAX);
                if prefetch_request_tx.is_some() && file_size > 0 && file_size <= mmap_threshold {
                    let prefetched_file = if let Some(result) = prefetched.remove(&file_index) {
                        result
                    } else {
                        let result_rx = prefetch_result_rx
                            .as_ref()
                            .expect("prefetch result receiver missing");
                        loop {
                            let result = result_rx.recv().map_err(|_| {
                                crate::OxideError::CompressionError(
                                    "directory prefetch worker stopped before completion"
                                        .to_string(),
                                )
                            })?;
                            if result.index == file_index {
                                break result;
                            }
                            prefetched.insert(result.index, result);
                        }
                    };
                    producer_read += prefetched_file.read_elapsed;
                    submitter.push_bytes_with_hint(
                        &prefetched_file.data,
                        file_format,
                        |batch| submit_batch(batch),
                    )?;
                } else if file_size >= mmap_threshold {
                    let read_started = Instant::now();
                    let mmap = MmapInput::open(&file.full_path)?;
                    let mut offset = 0usize;
                    let len = mmap.len();
                    while offset < len {
                        let end = offset.saturating_add(stream_read_buffer_size).min(len);
                        let bytes = mmap.mapped_slice(offset, end)?;
                        submitter.push_bytes_with_hint(bytes.as_slice(), file_format, |batch| {
                            submit_batch(batch)
                        })?;
                        offset = end;
                    }
                    producer_read += read_started.elapsed();
                } else {
                    let mut file_reader = fs::File::open(&file.full_path)?;
                    loop {
                        let read_started = Instant::now();
                        let read = file_reader.read(&mut read_buffer)?;
                        producer_read += read_started.elapsed();
                        if read == 0 {
                            break;
                        }

                        submitter.push_bytes_with_hint(
                            &read_buffer[..read],
                            file_format,
                            |batch| submit_batch(batch),
                        )?;
                    }
                }

                if let Some(result_rx) = prefetch_result_rx.as_ref() {
                    loop {
                        match result_rx.try_recv() {
                            Ok(result) => {
                                prefetched.insert(result.index, result);
                            }
                            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                        }
                    }
                }
            }

            drop(prefetch_request_tx);
            if let Some(prefetch_handle) = prefetch_handle {
                match prefetch_handle.join() {
                    Ok(outcome) => outcome?,
                    Err(payload) => {
                        let details = if let Some(message) = payload.downcast_ref::<&str>() {
                            (*message).to_string()
                        } else if let Some(message) = payload.downcast_ref::<String>() {
                            message.clone()
                        } else {
                            "unknown panic payload".to_string()
                        };
                        return Err(crate::OxideError::CompressionError(format!(
                            "directory prefetch thread panicked: {details}"
                        )));
                    }
                }
            }

            submitter.finish(|batch| submit_batch(batch))?;
            Ok(DirectoryProducerOutcome { producer_read })
        });

        let mut producer_done = false;
        let mut shutdown_called = false;
        loop {
            let mut progressed = false;

            while !producer_done
                && submitted_count.saturating_sub(received_count) < max_inflight_blocks
            {
                match batch_rx.try_recv() {
                    Ok(batch) => {
                        handle.submit(batch)?;
                        submitted_count += 1;
                        progressed = true;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        producer_done = true;
                        break;
                    }
                }
            }

            let mut drained = 0usize;
            while drained < SUBMISSION_DRAIN_BUDGET {
                let Some(result) = handle.recv_timeout(Duration::from_millis(0)) else {
                    break;
                };
                Self::record_result_to_writer_queue(
                    result,
                    &writer_tx,
                    &mut completed_bytes,
                    &mut first_error,
                    &mut raw_passthrough_blocks,
                    &mut writer_queue_peak,
                );
                received_count += 1;
                drained += 1;
            }
            progressed |= drained > 0;

            if !producer_done
                && submitted_count.saturating_sub(received_count) < max_inflight_blocks
            {
                let wait_started = Instant::now();
                match batch_rx.recv_timeout(result_wait_timeout) {
                    Ok(batch) => {
                        stage_timings.submit_wait += wait_started.elapsed();
                        handle.submit(batch)?;
                        submitted_count += 1;
                        progressed = true;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        stage_timings.submit_wait += wait_started.elapsed();
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        stage_timings.submit_wait += wait_started.elapsed();
                        producer_done = true;
                    }
                }
            }

            if !shutdown_called && producer_done {
                handle.shutdown();
                shutdown_called = true;
            }

            if shutdown_called && received_count == submitted_count {
                if options.emit_final_progress {
                    Self::emit_archive_progress_if_due(
                        &handle,
                        ArchiveSourceKind::Directory,
                        started_at,
                        input_bytes_total,
                        completed_bytes,
                        output_bytes_written,
                        block_count,
                        emit_every,
                        &mut last_emit_at,
                        true,
                        processing_totals.as_ref(),
                        sink,
                    );
                }
                break;
            }

            if !progressed {
                let inflight_full =
                    submitted_count.saturating_sub(received_count) >= max_inflight_blocks;
                let wait_started = Instant::now();
                if let Some(result) = handle.recv_timeout(result_wait_timeout) {
                    Self::record_result_to_writer_queue(
                        result,
                        &writer_tx,
                        &mut completed_bytes,
                        &mut first_error,
                        &mut raw_passthrough_blocks,
                        &mut writer_queue_peak,
                    );
                    received_count += 1;
                }
                let waited = wait_started.elapsed();
                stage_timings.result_wait += waited;
                if inflight_full {
                    stage_timings.submit_wait += waited;
                }
            }

            output_bytes_written = writer_output_bytes.load(AtomicOrdering::Acquire);
            Self::emit_archive_progress_if_due(
                &handle,
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                false,
                processing_totals.as_ref(),
                sink,
            );
        }

        let producer_outcome = match producer_handle.join() {
            Ok(outcome) => outcome,
            Err(payload) => {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                return Err(crate::OxideError::CompressionError(format!(
                    "directory producer thread panicked: {details}"
                )));
            }
        };
        match producer_outcome {
            Ok(outcome) => {
                stage_timings.producer_read += outcome.producer_read;
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }

        if submitted_count != total_blocks {
            first_error.get_or_insert(crate::OxideError::InvalidFormat(
                "directory block count mismatch",
            ));
        }
        let expected = handle.submitted_count();
        if expected != submitted_count {
            first_error.get_or_insert(crate::OxideError::InvalidFormat(
                "submitted block count drift detected",
            ));
        }
        if !shutdown_called {
            handle.shutdown();
        }

        while received_count < submitted_count {
            let wait_started = Instant::now();
            if let Some(result) = handle.recv_timeout(result_wait_timeout) {
                Self::record_result_to_writer_queue(
                    result,
                    &writer_tx,
                    &mut completed_bytes,
                    &mut first_error,
                    &mut raw_passthrough_blocks,
                    &mut writer_queue_peak,
                );
                received_count += 1;
            }
            stage_timings.result_wait += wait_started.elapsed();
            output_bytes_written = writer_output_bytes.load(AtomicOrdering::Acquire);
            Self::emit_archive_progress_if_due(
                &handle,
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                options.emit_final_progress && received_count == submitted_count,
                processing_totals.as_ref(),
                sink,
            );
        }

        let final_runtime = handle.runtime_snapshot();
        if let Err(join_error) = handle.join() {
            return Err(join_error);
        }

        drop(writer_tx);
        let writer_outcome = match writer_handle.join() {
            Ok(outcome) => outcome,
            Err(payload) => {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };
                Err(crate::OxideError::CompressionError(format!(
                    "directory writer thread panicked: {details}"
                )))
            }
        };

        let writer_outcome = match writer_outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                first_error.get_or_insert(error);
                return Err(first_error.expect("first error must be set"));
            }
        };
        stage_timings.writer += writer_outcome.writer_time;
        output_bytes_written = writer_outcome.output_bytes_written;
        let pending_write_peak = writer_outcome.pending_write_peak;

        if let Some(error) = first_error {
            return Err(error);
        }
        let processing_snapshot = processing_totals.snapshot();
        let extensions = Self::build_stats_extensions(
            input_bytes_total,
            output_bytes_written,
            &final_runtime,
            block_size,
            raw_passthrough_blocks,
            self.performance.compression_preset,
            max_inflight_blocks,
            max_inflight_bytes,
            pending_write_peak,
            writer_queue_peak,
            stage_timings,
            processing_snapshot,
        );

        let report = Self::build_archive_report(
            ArchiveSourceKind::Directory,
            started_at.elapsed(),
            input_bytes_total,
            output_bytes_written,
            block_count,
            final_runtime.completed as u32,
            final_runtime.workers,
            extensions,
            *options,
        );
        Self::record_archive_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
        Ok(ArchiveRun {
            writer: writer_outcome.writer,
            report,
        })
    }

    fn emit_archive_progress_if_due(
        handle: &WorkerPoolHandle,
        source_kind: ArchiveSourceKind,
        started_at: Instant,
        input_bytes_total: u64,
        input_bytes_completed: u64,
        output_bytes_completed: u64,
        blocks_total: u32,
        emit_every: Duration,
        last_emit_at: &mut Instant,
        force: bool,
        processing_totals: &ProcessingThroughputTotals,
        sink: &mut dyn TelemetrySink,
    ) {
        if force || last_emit_at.elapsed() >= emit_every {
            let runtime = handle.runtime_snapshot();
            let processing = processing_totals.snapshot();
            let elapsed = started_at.elapsed();
            let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
            let input_done = input_bytes_completed.min(input_bytes_total);
            let read_avg_bps = input_done as f64 / elapsed_secs;
            let write_avg_bps = output_bytes_completed as f64 / elapsed_secs;
            let output_input_ratio = if input_done == 0 {
                1.0
            } else {
                output_bytes_completed as f64 / input_done as f64
            };
            let compression_ratio = if output_bytes_completed == 0 {
                0.0
            } else {
                input_done as f64 / output_bytes_completed as f64
            };
            sink.on_event(TelemetryEvent::ArchiveProgress(ArchiveProgressEvent {
                source_kind,
                elapsed,
                input_bytes_total,
                input_bytes_completed: input_done,
                output_bytes_completed,
                read_avg_bps,
                write_avg_bps,
                preprocessing_avg_bps: processing.preprocessing_avg_bps(),
                compression_avg_bps: processing.compression_avg_bps(),
                preprocessing_compression_avg_bps: processing.preprocessing_compression_avg_bps(),
                preprocessing_wall_avg_bps: processing.preprocessing_wall_avg_bps(elapsed),
                compression_wall_avg_bps: processing.compression_wall_avg_bps(elapsed),
                preprocessing_compression_wall_avg_bps: processing
                    .preprocessing_compression_wall_avg_bps(elapsed),
                output_input_ratio,
                compression_ratio,
                blocks_total,
                blocks_completed: runtime.completed as u32,
                blocks_pending: runtime.pending as u32,
                runtime,
            }));
            *last_emit_at = Instant::now();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_extract_progress_if_due(
        source_kind: ArchiveSourceKind,
        started_at: Instant,
        archive_bytes_completed: u64,
        decoded_bytes_completed: u64,
        blocks_total: u32,
        blocks_completed: u32,
        runtime: PoolRuntimeSnapshot,
        emit_every: Duration,
        last_emit_at: &mut Instant,
        force: bool,
        sink: &mut dyn TelemetrySink,
    ) {
        if force || last_emit_at.elapsed() >= emit_every {
            Self::emit_extract_progress(
                source_kind,
                started_at,
                archive_bytes_completed,
                decoded_bytes_completed,
                blocks_total,
                blocks_completed,
                runtime,
                sink,
            );
            *last_emit_at = Instant::now();
        }
    }

    fn emit_extract_progress(
        source_kind: ArchiveSourceKind,
        started_at: Instant,
        archive_bytes_completed: u64,
        decoded_bytes_completed: u64,
        blocks_total: u32,
        blocks_completed: u32,
        runtime: PoolRuntimeSnapshot,
        sink: &mut dyn TelemetrySink,
    ) {
        let elapsed = started_at.elapsed();
        let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = archive_bytes_completed as f64 / elapsed_secs;
        let decode_avg_bps = decoded_bytes_completed as f64 / elapsed_secs;
        let decode_archive_ratio = if archive_bytes_completed == 0 {
            1.0
        } else {
            decoded_bytes_completed as f64 / archive_bytes_completed as f64
        };
        sink.on_event(TelemetryEvent::ExtractProgress(ExtractProgressEvent {
            source_kind,
            elapsed,
            archive_bytes_completed,
            decoded_bytes_completed,
            read_avg_bps,
            decode_avg_bps,
            decode_archive_ratio,
            blocks_total,
            blocks_completed,
            runtime,
        }));
    }

    fn record_pipeline_stage(metric: &'static str, op: &'static str, elapsed: Duration) {
        let elapsed_us = duration_to_us(elapsed);
        telemetry::record_histogram(metric, elapsed_us, &[("subsystem", "pipeline"), ("op", op)]);
        profile::event(
            tags::PROFILE_PIPELINE,
            &PROFILE_TAG_STACK_PIPELINE,
            op,
            "ok",
            elapsed_us,
            "pipeline stage measured",
        );
    }

    fn record_archive_run_telemetry(elapsed: Duration, stage_timings: StageTimings) {
        let elapsed_us = duration_to_us(elapsed);
        telemetry::increment_counter(
            tags::METRIC_PIPELINE_ARCHIVE_RUN_COUNT,
            1,
            &[("subsystem", "pipeline"), ("op", "archive_run")],
        );
        telemetry::record_histogram(
            tags::METRIC_PIPELINE_ARCHIVE_RUN_LATENCY_US,
            elapsed_us,
            &[("subsystem", "pipeline"), ("op", "archive_run")],
        );
        profile::event(
            tags::PROFILE_PIPELINE,
            &PROFILE_TAG_STACK_PIPELINE,
            "archive_run",
            "ok",
            elapsed_us,
            "archive run completed",
        );

        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_DISCOVERY_US,
            "stage_discovery",
            stage_timings.discovery,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_FORMAT_PROBE_US,
            "stage_format_probe",
            stage_timings.format_probe,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_PRODUCER_READ_US,
            "stage_producer_read",
            stage_timings.producer_read,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_SUBMIT_WAIT_US,
            "stage_submit_wait",
            stage_timings.submit_wait,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_RESULT_WAIT_US,
            "stage_result_wait",
            stage_timings.result_wait,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_WRITER_US,
            "stage_writer",
            stage_timings.writer,
        );
    }

    fn record_extract_run_telemetry(elapsed: Duration, stage_timings: ExtractStageTimings) {
        let elapsed_us = duration_to_us(elapsed);
        telemetry::increment_counter(
            tags::METRIC_PIPELINE_EXTRACT_RUN_COUNT,
            1,
            &[("subsystem", "pipeline"), ("op", "extract_run")],
        );
        telemetry::record_histogram(
            tags::METRIC_PIPELINE_EXTRACT_RUN_LATENCY_US,
            elapsed_us,
            &[("subsystem", "pipeline"), ("op", "extract_run")],
        );
        profile::event(
            tags::PROFILE_PIPELINE,
            &PROFILE_TAG_STACK_PIPELINE,
            "extract_run",
            "ok",
            elapsed_us,
            "extract run completed",
        );

        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_ARCHIVE_READ_US,
            "stage_archive_read",
            stage_timings.archive_read,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_DECODE_SUBMIT_US,
            "stage_decode_submit",
            stage_timings.decode_submit,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_DECODE_WAIT_US,
            "stage_decode_wait",
            stage_timings.decode_wait,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_MERGE_US,
            "stage_merge",
            stage_timings.merge,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_DIRECTORY_DECODE_US,
            "stage_directory_decode",
            stage_timings.directory_decode,
        );
        Self::record_pipeline_stage(
            tags::METRIC_PIPELINE_STAGE_OUTPUT_WRITE_US,
            "stage_output_write",
            stage_timings.output_write,
        );
    }

    pub fn max_inflight_blocks(
        total_blocks: usize,
        num_workers: usize,
        block_size: usize,
        performance: &PipelinePerformanceOptions,
    ) -> usize {
        let scaled_by_workers =
            num_workers.saturating_mul(performance.max_inflight_blocks_per_worker);
        let bounded_workers = scaled_by_workers.clamp(MIN_INFLIGHT_BLOCKS, MAX_INFLIGHT_BLOCKS);

        let block_bytes = block_size.max(1);
        let inflight_bytes = performance.max_inflight_bytes.max(block_bytes);
        let bounded_by_bytes = inflight_bytes.div_ceil(block_bytes).max(1);

        let bounded = bounded_workers
            .min(bounded_by_bytes)
            .clamp(1, MAX_INFLIGHT_BLOCKS);
        bounded.min(total_blocks.max(1))
    }

    fn record_result_to_writer_queue(
        result: Result<CompressedBlock>,
        writer_tx: &crossbeam_channel::Sender<CompressedBlock>,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
        writer_queue_peak: &mut usize,
    ) {
        match result {
            Ok(block) => {
                *completed_bytes = (*completed_bytes).saturating_add(block.original_len);
                if block.raw_passthrough {
                    *raw_passthrough_blocks = raw_passthrough_blocks.saturating_add(1);
                }
                if first_error.is_some() {
                    return;
                }
                if writer_tx.send(block).is_err() {
                    first_error.get_or_insert(crate::OxideError::CompressionError(
                        "directory writer queue closed before completion".to_string(),
                    ));
                    return;
                }
                *writer_queue_peak = (*writer_queue_peak).max(writer_tx.len());
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }

    fn drain_results_to_writer<W: Write>(
        handle: &WorkerPoolHandle,
        archive_writer: &mut ArchiveWriter<W>,
        pending_write: &mut BTreeMap<usize, CompressedBlock>,
        next_write_id: &mut usize,
        output_bytes_written: &mut u64,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
        received_count: &mut usize,
        pending_write_peak: &mut usize,
        writer_time: &mut Duration,
        max_results: usize,
    ) -> usize {
        let mut drained = 0usize;
        while drained < max_results {
            if let Some(result) = handle.recv_timeout(Duration::from_millis(0)) {
                Self::record_result_to_writer(
                    result,
                    archive_writer,
                    pending_write,
                    next_write_id,
                    output_bytes_written,
                    completed_bytes,
                    first_error,
                    raw_passthrough_blocks,
                    pending_write_peak,
                    writer_time,
                );
                *received_count += 1;
                drained += 1;
            } else {
                break;
            }
        }
        drained
    }

    fn recv_result_to_writer<W: Write>(
        handle: &WorkerPoolHandle,
        timeout: Duration,
        archive_writer: &mut ArchiveWriter<W>,
        pending_write: &mut BTreeMap<usize, CompressedBlock>,
        next_write_id: &mut usize,
        output_bytes_written: &mut u64,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
        received_count: &mut usize,
        pending_write_peak: &mut usize,
        writer_time: &mut Duration,
    ) -> bool {
        if let Some(result) = handle.recv_timeout(timeout) {
            Self::record_result_to_writer(
                result,
                archive_writer,
                pending_write,
                next_write_id,
                output_bytes_written,
                completed_bytes,
                first_error,
                raw_passthrough_blocks,
                pending_write_peak,
                writer_time,
            );
            *received_count += 1;
            true
        } else {
            false
        }
    }

    fn record_result_to_writer<W: Write>(
        result: Result<CompressedBlock>,
        archive_writer: &mut ArchiveWriter<W>,
        pending_write: &mut BTreeMap<usize, CompressedBlock>,
        next_write_id: &mut usize,
        output_bytes_written: &mut u64,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
        pending_write_peak: &mut usize,
        writer_time: &mut Duration,
    ) {
        match result {
            Ok(block) => {
                *completed_bytes = (*completed_bytes).saturating_add(block.original_len);
                if block.raw_passthrough {
                    *raw_passthrough_blocks = raw_passthrough_blocks.saturating_add(1);
                }
                if first_error.is_some() {
                    return;
                }

                let block_id = block.id;
                if pending_write.insert(block_id, block).is_some() {
                    first_error.get_or_insert(crate::OxideError::InvalidFormat(
                        "duplicate block id received from worker",
                    ));
                    return;
                }
                *pending_write_peak = (*pending_write_peak).max(pending_write.len());

                while let Some(ready) = pending_write.remove(next_write_id) {
                    *output_bytes_written = (*output_bytes_written)
                        .saturating_add(BLOCK_HEADER_SIZE as u64)
                        .saturating_add(ready.data.len() as u64);
                    let write_started = Instant::now();
                    if let Err(error) = archive_writer.write_owned_block(ready) {
                        first_error.get_or_insert(error);
                        *writer_time += write_started.elapsed();
                        break;
                    }
                    *writer_time += write_started.elapsed();
                    *next_write_id += 1;
                }
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }

    fn build_stats_extensions(
        input_bytes_total: u64,
        output_bytes_total: u64,
        runtime: &PoolRuntimeSnapshot,
        block_size: BlockSizeDecision,
        raw_passthrough_blocks: u64,
        compression_preset: CompressionPreset,
        max_inflight_blocks: usize,
        max_inflight_bytes: usize,
        pending_write_peak: usize,
        writer_queue_peak: usize,
        stage_timings: StageTimings,
        processing_snapshot: ProcessingThroughputSnapshot,
    ) -> BTreeMap<String, StatValue> {
        let mut extensions = BTreeMap::new();
        let compress_busy_us = runtime
            .workers
            .iter()
            .map(|worker| worker.busy.as_micros())
            .sum::<u128>()
            .min(u64::MAX as u128) as u64;
        let preprocessing_busy_us = duration_to_us(processing_snapshot.preprocessing_elapsed);
        let compression_busy_us = duration_to_us(processing_snapshot.compression_elapsed);
        let preprocessing_avg_bps = processing_snapshot.preprocessing_avg_bps();
        let compression_avg_bps = processing_snapshot.compression_avg_bps();
        let preprocessing_compression_avg_bps =
            processing_snapshot.preprocessing_compression_avg_bps();
        let preprocessing_wall_avg_bps =
            processing_snapshot.preprocessing_wall_avg_bps(runtime.elapsed);
        let compression_wall_avg_bps =
            processing_snapshot.compression_wall_avg_bps(runtime.elapsed);
        let preprocessing_compression_wall_avg_bps =
            processing_snapshot.preprocessing_compression_wall_avg_bps(runtime.elapsed);
        let elapsed_us = runtime.elapsed.as_micros().max(1).min(u64::MAX as u128) as u64;
        let effective_cores = compress_busy_us as f64 / elapsed_us as f64;

        extensions.insert(
            "runtime.submitted".to_string(),
            StatValue::U64(runtime.submitted as u64),
        );
        extensions.insert(
            "runtime.completed".to_string(),
            StatValue::U64(runtime.completed as u64),
        );
        extensions.insert(
            "runtime.pending".to_string(),
            StatValue::U64(runtime.pending as u64),
        );
        extensions.insert("runtime.elapsed_us".to_string(), StatValue::U64(elapsed_us));
        extensions.insert(
            "runtime.worker_count".to_string(),
            StatValue::U64(runtime.workers.len() as u64),
        );
        extensions.insert(
            "runtime.compress_busy_us".to_string(),
            StatValue::U64(compress_busy_us),
        );
        extensions.insert(
            "runtime.preprocessing_busy_us".to_string(),
            StatValue::U64(preprocessing_busy_us),
        );
        extensions.insert(
            "runtime.compression_busy_us".to_string(),
            StatValue::U64(compression_busy_us),
        );
        extensions.insert(
            "runtime.preprocessing_input_bytes".to_string(),
            StatValue::U64(processing_snapshot.preprocessing_input_bytes),
        );
        extensions.insert(
            "runtime.compression_input_bytes".to_string(),
            StatValue::U64(processing_snapshot.compression_input_bytes),
        );
        extensions.insert(
            "runtime.effective_cores".to_string(),
            StatValue::F64(effective_cores),
        );
        extensions.insert(
            "throughput.preprocessing_avg_bps".to_string(),
            StatValue::F64(preprocessing_avg_bps),
        );
        extensions.insert(
            "throughput.compression_avg_bps".to_string(),
            StatValue::F64(compression_avg_bps),
        );
        extensions.insert(
            "throughput.preprocessing_compression_avg_bps".to_string(),
            StatValue::F64(preprocessing_compression_avg_bps),
        );
        extensions.insert(
            "throughput.preprocessing_wall_avg_bps".to_string(),
            StatValue::F64(preprocessing_wall_avg_bps),
        );
        extensions.insert(
            "throughput.compression_wall_avg_bps".to_string(),
            StatValue::F64(compression_wall_avg_bps),
        );
        extensions.insert(
            "throughput.preprocessing_compression_wall_avg_bps".to_string(),
            StatValue::F64(preprocessing_compression_wall_avg_bps),
        );
        extensions.insert(
            "tuning.block_size".to_string(),
            StatValue::U64(block_size.selected_block_size as u64),
        );
        extensions.insert(
            "tuning.autotune_requested".to_string(),
            StatValue::U64(block_size.autotune_requested as u64),
        );
        extensions.insert(
            "tuning.autotune_ran".to_string(),
            StatValue::U64(block_size.autotune_ran as u64),
        );
        extensions.insert(
            "tuning.autotune_sampled_bytes".to_string(),
            StatValue::U64(block_size.sampled_bytes as u64),
        );
        extensions.insert(
            "compression.raw_passthrough_blocks".to_string(),
            StatValue::U64(raw_passthrough_blocks),
        );
        extensions.insert(
            "compression.preset".to_string(),
            StatValue::Text(format!("{compression_preset:?}")),
        );
        extensions.insert(
            "pipeline.max_inflight_blocks".to_string(),
            StatValue::U64(max_inflight_blocks as u64),
        );
        extensions.insert(
            "pipeline.max_inflight_bytes".to_string(),
            StatValue::U64(max_inflight_bytes as u64),
        );
        extensions.insert(
            "pipeline.pending_write_peak".to_string(),
            StatValue::U64(pending_write_peak as u64),
        );
        extensions.insert(
            "pipeline.writer_queue_peak".to_string(),
            StatValue::U64(writer_queue_peak as u64),
        );
        extensions.insert(
            "stage.discovery_us".to_string(),
            StatValue::U64(stage_timings.discovery.as_micros().min(u64::MAX as u128) as u64),
        );
        extensions.insert(
            "stage.format_probe_us".to_string(),
            StatValue::U64(stage_timings.format_probe.as_micros().min(u64::MAX as u128) as u64),
        );
        extensions.insert(
            "stage.producer_read_us".to_string(),
            StatValue::U64(
                stage_timings
                    .producer_read
                    .as_micros()
                    .min(u64::MAX as u128) as u64,
            ),
        );
        extensions.insert(
            "stage.submit_wait_us".to_string(),
            StatValue::U64(stage_timings.submit_wait.as_micros().min(u64::MAX as u128) as u64),
        );
        extensions.insert(
            "stage.result_wait_us".to_string(),
            StatValue::U64(stage_timings.result_wait.as_micros().min(u64::MAX as u128) as u64),
        );
        extensions.insert(
            "stage.writer_us".to_string(),
            StatValue::U64(stage_timings.writer.as_micros().min(u64::MAX as u128) as u64),
        );
        if input_bytes_total > 0 {
            extensions.insert(
                "archive.output_input_ratio".to_string(),
                StatValue::F64(output_bytes_total as f64 / input_bytes_total as f64),
            );
        }
        extensions
    }

    #[allow(clippy::too_many_arguments)]
    fn build_archive_report(
        source_kind: ArchiveSourceKind,
        elapsed: Duration,
        input_bytes_total: u64,
        output_bytes_total: u64,
        blocks_total: u32,
        blocks_completed: u32,
        worker_runtime: Vec<WorkerRuntimeSnapshot>,
        extensions: BTreeMap<String, StatValue>,
        options: RunTelemetryOptions,
    ) -> ArchiveReport {
        let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = input_bytes_total as f64 / elapsed_secs;
        let write_avg_bps = output_bytes_total as f64 / elapsed_secs;
        let output_input_ratio = if input_bytes_total == 0 {
            1.0
        } else {
            output_bytes_total as f64 / input_bytes_total as f64
        };

        let workers = worker_runtime
            .iter()
            .map(WorkerReport::from_runtime)
            .collect::<Vec<_>>();
        let mut main_thread = ThreadReport::new("main");
        let mut report_extensions = BTreeMap::new();

        for (key, value) in extensions {
            if let Some(stage) = key
                .strip_prefix("stage.")
                .and_then(|stage| stage.strip_suffix("_us"))
            {
                if let StatValue::U64(value_us) = &value {
                    main_thread.stage_us.insert(stage.to_string(), *value_us);
                }
            }
            report_extensions.insert(key, Self::report_value_from_stat(value));
        }

        let telemetry = if options.include_telemetry_snapshot {
            Some(crate::telemetry::snapshot())
        } else {
            None
        };

        ArchiveReport {
            source_kind,
            elapsed,
            input_bytes_total,
            output_bytes_total,
            blocks_total,
            blocks_completed,
            read_avg_bps,
            write_avg_bps,
            output_input_ratio,
            workers,
            main_thread,
            extensions: report_extensions,
            telemetry,
        }
    }

    fn report_value_from_stat(value: StatValue) -> ReportValue {
        match value {
            StatValue::U64(value) => ReportValue::U64(value),
            StatValue::F64(value) => ReportValue::F64(value),
            StatValue::Text(value) => ReportValue::Text(value),
        }
    }

    fn archive_prepared_with<W: Write>(
        &self,
        prepared: PreparedInput,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
        block_size: BlockSizeDecision,
    ) -> Result<ArchiveRun<W>> {
        let PreparedInput {
            source_kind,
            batches,
            input_bytes_total,
        } = prepared;
        let total_blocks = batches.len();
        let block_count = u32::try_from(total_blocks)
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;
        let max_inflight_blocks = Self::max_inflight_blocks(
            total_blocks,
            self.num_workers,
            block_size.selected_block_size,
            &self.performance,
        );
        let max_inflight_bytes = max_inflight_blocks.saturating_mul(block_size.selected_block_size);

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );
        let processing_totals = Arc::new(ProcessingThroughputTotals::default());

        let compression_preset = self.performance.compression_preset;
        let raw_fallback_enabled = self.performance.raw_fallback_enabled;
        let worker_processing_totals = Arc::clone(&processing_totals);
        let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression| {
            Self::process_batch(
                batch,
                pool,
                compression,
                compression_preset,
                raw_fallback_enabled,
                worker_processing_totals.as_ref(),
            )
        });

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.buffer_pool));
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(source_kind),
        )?;
        let mut output_bytes_written = GLOBAL_HEADER_SIZE as u64;
        let mut pending_write = BTreeMap::<usize, CompressedBlock>::new();
        let mut next_write_id = 0usize;

        let started_at = Instant::now();
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut first_error: Option<crate::OxideError> = None;
        let mut submitted_count = 0usize;
        let mut completed_bytes = 0u64;
        let mut received_count = 0usize;
        let mut raw_passthrough_blocks = 0u64;
        let mut pending_write_peak = 0usize;
        let mut shutdown_called = false;
        let mut stage_timings = StageTimings::default();
        let result_wait_timeout = self
            .performance
            .result_wait_timeout
            .max(Duration::from_millis(1));
        let mut batches = batches.into_iter();

        loop {
            while submitted_count.saturating_sub(received_count) < max_inflight_blocks {
                let Some(batch) = batches.next() else {
                    break;
                };
                handle.submit(batch)?;
                submitted_count += 1;
            }

            let drained = Self::drain_results_to_writer(
                &handle,
                &mut archive_writer,
                &mut pending_write,
                &mut next_write_id,
                &mut output_bytes_written,
                &mut completed_bytes,
                &mut first_error,
                &mut raw_passthrough_blocks,
                &mut received_count,
                &mut pending_write_peak,
                &mut stage_timings.writer,
                SUBMISSION_DRAIN_BUDGET,
            );

            if submitted_count == total_blocks && !shutdown_called {
                handle.shutdown();
                shutdown_called = true;
            }

            if shutdown_called && received_count == submitted_count {
                if options.emit_final_progress {
                    Self::emit_archive_progress_if_due(
                        &handle,
                        source_kind,
                        started_at,
                        input_bytes_total,
                        completed_bytes,
                        output_bytes_written,
                        block_count,
                        emit_every,
                        &mut last_emit_at,
                        true,
                        processing_totals.as_ref(),
                        sink,
                    );
                }
                break;
            }

            if drained == 0 {
                let wait_started = Instant::now();
                Self::recv_result_to_writer(
                    &handle,
                    result_wait_timeout,
                    &mut archive_writer,
                    &mut pending_write,
                    &mut next_write_id,
                    &mut output_bytes_written,
                    &mut completed_bytes,
                    &mut first_error,
                    &mut raw_passthrough_blocks,
                    &mut received_count,
                    &mut pending_write_peak,
                    &mut stage_timings.writer,
                );
                stage_timings.result_wait += wait_started.elapsed();
            }
            Self::emit_archive_progress_if_due(
                &handle,
                source_kind,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                false,
                processing_totals.as_ref(),
                sink,
            );
        }

        let final_runtime = handle.runtime_snapshot();
        if let Err(join_error) = handle.join() {
            return Err(join_error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if next_write_id != submitted_count || !pending_write.is_empty() {
            return Err(crate::OxideError::InvalidFormat(
                "writer has pending blocks after completion",
            ));
        }

        let writer = archive_writer.write_footer()?;
        output_bytes_written = output_bytes_written.saturating_add(FOOTER_SIZE as u64);
        let processing_snapshot = processing_totals.snapshot();
        let extensions = Self::build_stats_extensions(
            input_bytes_total,
            output_bytes_written,
            &final_runtime,
            block_size,
            raw_passthrough_blocks,
            self.performance.compression_preset,
            max_inflight_blocks,
            max_inflight_bytes,
            pending_write_peak,
            0,
            stage_timings,
            processing_snapshot,
        );
        let report = Self::build_archive_report(
            source_kind,
            started_at.elapsed(),
            input_bytes_total,
            output_bytes_written,
            block_count,
            final_runtime.completed as u32,
            final_runtime.workers,
            extensions,
            *options,
        );
        Self::record_archive_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
        Ok(ArchiveRun { writer, report })
    }

    fn process_batch(
        batch: Batch,
        pool: &BufferPool,
        compression: CompressionAlgo,
        compression_preset: CompressionPreset,
        raw_fallback_enabled: bool,
        processing_totals: &ProcessingThroughputTotals,
    ) -> Result<CompressedBlock> {
        let pre_proc =
            crate::preprocessing::get_preprocessing_strategy(batch.file_type_hint, compression);
        let source = batch.data();
        let metadata = batch.preprocessing_metadata.as_ref();
        let (preprocessed, preprocessing_elapsed) =
            if matches!(pre_proc, PreProcessingStrategy::None) {
                (None, Duration::ZERO)
            } else {
                let preprocessing_started = Instant::now();
                (
                    Some(crate::preprocessing::apply_preprocessing_with_metadata(
                        source, &pre_proc, metadata,
                    )?),
                    preprocessing_started.elapsed(),
                )
            };
        let compression_input = preprocessed.as_deref().unwrap_or(source);

        let compression_started = Instant::now();
        let compressed = crate::compression::apply_compression(compression_input, compression)?;
        let compression_elapsed = compression_started.elapsed();
        processing_totals.record(
            source.len() as u64,
            preprocessing_elapsed,
            compression_input.len() as u64,
            compression_elapsed,
        );
        let (output, raw_passthrough) = Self::select_stored_payload(
            compression_input,
            compressed.as_slice(),
            raw_fallback_enabled,
        );

        let mut scratch = pool.acquire();
        scratch.extend_from_slice(output);

        // Move the pooled Vec out without allocating.
        let mut data = Vec::new();
        std::mem::swap(scratch.as_mut_vec(), &mut data);

        Ok(CompressedBlock::with_compression_meta(
            batch.id,
            data,
            pre_proc,
            CompressionMeta::new(compression, compression_preset, raw_passthrough),
            batch.len() as u64,
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

    fn choose_block_size_for_file(
        &self,
        path: &Path,
        input_bytes: u64,
    ) -> Result<BlockSizeDecision> {
        if !self.performance.autotune_enabled
            || input_bytes < self.performance.autotune_min_input_bytes
        {
            return Ok(BlockSizeDecision {
                selected_block_size: self.scanner.target_block_size(),
                autotune_requested: self.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let sample = Self::collect_file_sample(path, self.performance.autotune_sample_bytes)?;
        self.pick_tuned_block_size(&sample)
    }

    fn choose_block_size_for_directory(
        &self,
        discovery: &directory::DirectoryDiscovery,
    ) -> Result<BlockSizeDecision> {
        if !self.performance.autotune_enabled
            || discovery.input_bytes_total < self.performance.autotune_min_input_bytes
        {
            return Ok(BlockSizeDecision {
                selected_block_size: self.scanner.target_block_size(),
                autotune_requested: self.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let sample =
            Self::collect_directory_sample(discovery, self.performance.autotune_sample_bytes)?;
        self.pick_tuned_block_size(&sample)
    }

    fn pick_tuned_block_size(&self, sample: &[u8]) -> Result<BlockSizeDecision> {
        if sample.is_empty() {
            return Ok(BlockSizeDecision {
                selected_block_size: self.scanner.target_block_size(),
                autotune_requested: self.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let mut candidates = AUTOTUNE_CANDIDATE_BLOCK_SIZES.to_vec();
        let default_block = self.scanner.target_block_size().max(1);
        if !candidates.contains(&default_block) {
            candidates.push(default_block);
        }
        candidates.sort_unstable();
        candidates.dedup();

        let mut scores = Vec::with_capacity(candidates.len());
        for block_size in candidates {
            scores.push(self.score_block_size(sample, block_size)?);
        }

        scores.sort_by(|left, right| {
            right
                .throughput_bps
                .partial_cmp(&left.throughput_bps)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.output_bytes.cmp(&right.output_bytes))
                .then_with(|| left.block_size.cmp(&right.block_size))
        });

        let selected = scores
            .first()
            .map(|score| score.block_size)
            .unwrap_or(default_block);
        Ok(BlockSizeDecision {
            selected_block_size: selected.max(1),
            autotune_requested: self.performance.autotune_enabled,
            autotune_ran: true,
            sampled_bytes: sample.len(),
        })
    }

    pub fn score_block_size(&self, sample: &[u8], block_size: usize) -> Result<BlockSizeScore> {
        let started = Instant::now();
        let mut output_bytes = 0usize;
        let chunk_size = block_size.max(1);
        for chunk in sample.chunks(chunk_size) {
            let compressed = crate::compression::apply_compression(chunk, self.compression_algo)?;
            let (stored_payload, _raw_passthrough) = Self::select_stored_payload(
                chunk,
                compressed.as_slice(),
                self.performance.raw_fallback_enabled,
            );
            output_bytes = output_bytes.saturating_add(stored_payload.len());
        }
        let elapsed = started.elapsed().as_secs_f64().max(1e-9);
        Ok(BlockSizeScore {
            block_size: chunk_size,
            throughput_bps: sample.len() as f64 / elapsed,
            output_bytes,
        })
    }

    fn collect_file_sample(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
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

    fn collect_directory_sample(
        discovery: &directory::DirectoryDiscovery,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        let limit = max_bytes.max(1);
        let mut sample = Vec::with_capacity(limit);
        let mut scratch = vec![0u8; 64 * 1024];
        for file in &discovery.files {
            if sample.len() >= limit {
                break;
            }
            let mut reader = fs::File::open(&file.full_path)?;
            while sample.len() < limit {
                let to_read = (limit - sample.len()).min(scratch.len());
                let read = reader.read(&mut scratch[..to_read])?;
                if read == 0 {
                    break;
                }
                sample.extend_from_slice(&scratch[..read]);
            }
        }
        Ok(sample)
    }

    fn prepare_file(&self, path: &Path, block_size: usize) -> Result<PreparedInput> {
        let scanner = InputScanner::new(block_size);
        let batches = scanner.scan_file(path)?;
        let input_bytes_total = batches.iter().map(|batch| batch.len() as u64).sum();
        Ok(PreparedInput {
            source_kind: ArchiveSourceKind::File,
            batches,
            input_bytes_total,
        })
    }
}
