use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};

use crate::buffer::{BufferPool, PooledBuffer};
use crate::compression::CompressionScratchArena;
use crate::core::WorkerRuntimeSnapshot;
use crate::dictionary::ArchiveDictionaryBank;
use crate::format::{ArchiveReader, ChunkDescriptor, GlobalHeader};
use crate::telemetry::{ReportValue, RunTelemetryOptions, TelemetrySink};
use crate::types::Result;

use super::super::directory;
use super::super::types::ArchiveSourceKind;
use super::directory_restore::{
    DirectoryExtractSelection, DirectoryRestoreWriter, FilteredDirectoryRestoreWriter,
    OUTPUT_BUFFER_CAPACITY, apply_entry_metadata,
};
use super::reorder_writer::{BoundedReorderWriter, OrderedChunkWriter, ReorderWriterStats};
use super::telemetry::*;
use super::types::*;

const DECODE_QUEUE_MULTIPLIER: usize = 4;
const ORDERED_WRITE_QUEUE_MULTIPLIER: usize = 8;
// Large archives can have substantial decode skew; keep enough headroom for
// late blocks without failing the extraction outright.
const REORDER_PENDING_MULTIPLIER: usize = 8;
const MIN_DECODE_QUEUE_CAPACITY: usize = 8;
const MIN_ORDERED_WRITE_QUEUE_CAPACITY: usize = 16;
const RESULT_DRAIN_BUDGET: usize = 64;

#[derive(Default)]
struct VecChunkWriter {
    output: Vec<u8>,
}

impl VecChunkWriter {
    fn into_inner(self) -> Vec<u8> {
        self.output
    }
}

impl OrderedChunkWriter for VecChunkWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

struct FileChunkWriter {
    writer: BufWriter<fs::File>,
    path: PathBuf,
    entry: crate::ArchiveListingEntry,
    stats: FileRestoreStats,
}

#[derive(Debug, Default, Clone, Copy)]
struct FileRestoreStats {
    ordered_write_time: Duration,
    output_write: Duration,
    output_create: Duration,
    output_create_directories: Duration,
    output_create_files: Duration,
    output_data: Duration,
    output_flush: Duration,
    output_metadata: Duration,
    output_metadata_files: Duration,
}

impl FileRestoreStats {
    fn record_output_create_directories(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
        self.output_create_directories += elapsed;
    }

    fn record_output_create_file(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_create += elapsed;
        self.output_create_files += elapsed;
    }

    fn record_output_data(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_data += elapsed;
    }

    fn record_output_flush(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_flush += elapsed;
    }

    fn record_output_metadata_file(&mut self, elapsed: Duration) {
        self.output_write += elapsed;
        self.output_metadata += elapsed;
        self.output_metadata_files += elapsed;
    }
}

impl FileChunkWriter {
    fn create(path: &Path, entry: crate::ArchiveListingEntry) -> Result<Self> {
        let mut stats = FileRestoreStats::default();
        if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
            let output_started = Instant::now();
            fs::create_dir_all(parent)?;
            stats.record_output_create_directories(output_started.elapsed());
        }

        let output_started = Instant::now();
        let file = fs::File::create(path)?;
        stats.record_output_create_file(output_started.elapsed());
        Ok(Self {
            writer: BufWriter::with_capacity(OUTPUT_BUFFER_CAPACITY, file),
            path: path.to_path_buf(),
            entry,
            stats,
        })
    }

    fn flush_and_apply_metadata(&mut self) -> Result<()> {
        let flush_started = Instant::now();
        self.writer.flush()?;
        self.stats.record_output_flush(flush_started.elapsed());

        let metadata_started = Instant::now();
        apply_entry_metadata(&self.path, &self.entry)?;
        self.stats
            .record_output_metadata_file(metadata_started.elapsed());
        Ok(())
    }

    fn stats(&self) -> FileRestoreStats {
        self.stats
    }
}

impl OrderedChunkWriter for FileChunkWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        let write_started = Instant::now();
        self.writer.write_all(bytes)?;
        let elapsed = write_started.elapsed();
        self.stats.ordered_write_time += elapsed;
        self.stats.record_output_data(elapsed);
        Ok(())
    }
}

#[derive(Debug)]
struct DecodeStreamOutcome {
    flags: u32,
    decoded_bytes_total: u64,
    archive_bytes_total: u64,
    blocks_total: u32,
    workers: Vec<WorkerRuntimeSnapshot>,
    stage_timings: ExtractStageTimings,
    pipeline_stats: ExtractPipelineStats,
}

#[derive(Debug)]
enum DecodedBlock {
    Owned(Vec<u8>),
    Pooled(PooledBuffer),
}

impl DecodedBlock {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
}

impl AsRef<[u8]> for DecodedBlock {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes.as_slice(),
            Self::Pooled(bytes) => bytes.as_slice(),
        }
    }
}

enum OrderedWriteTask {
    Block { index: usize, bytes: DecodedBlock },
    Abort,
}

#[derive(Debug, Clone, Copy)]
struct ReadRequest {
    index: usize,
    block_index: u32,
}

struct OrderedWriterOutcome<W> {
    writer: W,
    stats: ReorderWriterStats,
}

enum ProcessedDecodeResult {
    Block { index: usize, bytes: DecodedBlock },
    NewError,
    IgnoredAfterError,
}

#[derive(Debug)]
struct DecodePlan {
    selected_blocks: Vec<bool>,
    block_count: usize,
}

impl DecodePlan {
    fn all(block_count: usize) -> Self {
        Self {
            selected_blocks: vec![true; block_count],
            block_count,
        }
    }

    fn from_ranges(headers: &[ChunkDescriptor], ranges: &[Range<u64>]) -> Self {
        let mut selected_blocks = vec![false; headers.len()];
        let mut block_count = 0usize;
        let mut range_index = 0usize;
        let mut decoded_offset = 0u64;

        for (block_index, header) in headers.iter().enumerate() {
            let block_start = decoded_offset;
            let block_end = block_start.saturating_add(header.raw_len as u64);
            decoded_offset = block_end;

            while range_index < ranges.len() && ranges[range_index].end <= block_start {
                range_index += 1;
            }

            let Some(range) = ranges.get(range_index) else {
                break;
            };

            if range.start < block_end && block_start < range.end {
                selected_blocks[block_index] = true;
                block_count += 1;
            }
        }

        Self {
            selected_blocks,
            block_count,
        }
    }

    fn block_count(&self) -> usize {
        self.block_count
    }

    fn includes(&self, block_index: usize) -> bool {
        self.selected_blocks[block_index]
    }

    fn project_ranges(
        &self,
        headers: &[ChunkDescriptor],
        ranges: &[Range<u64>],
    ) -> Vec<Range<u64>> {
        let mut projected: Vec<Range<u64>> = Vec::new();
        let mut range_index = 0usize;
        let mut decoded_offset = 0u64;
        let mut selected_offset = 0u64;

        for (block_index, header) in headers.iter().enumerate() {
            let block_start = decoded_offset;
            let block_end = block_start.saturating_add(header.raw_len as u64);
            decoded_offset = block_end;

            if !self.includes(block_index) {
                continue;
            }

            while range_index < ranges.len() && ranges[range_index].end <= block_start {
                range_index += 1;
            }

            let mut local_index = range_index;
            while let Some(range) = ranges.get(local_index) {
                if range.start >= block_end {
                    break;
                }

                let overlap_start = range.start.max(block_start);
                let overlap_end = range.end.min(block_end);
                if overlap_start < overlap_end {
                    let projected_start =
                        selected_offset + overlap_start.saturating_sub(block_start);
                    let projected_end = selected_offset + overlap_end.saturating_sub(block_start);

                    if let Some(last) = projected.last_mut() {
                        if last.end == projected_start {
                            last.end = projected_end;
                        } else {
                            projected.push(projected_start..projected_end);
                        }
                    } else {
                        projected.push(projected_start..projected_end);
                    }
                }

                if range.end <= block_end {
                    local_index += 1;
                } else {
                    break;
                }
            }

            range_index = local_index.min(ranges.len());
            selected_offset = selected_offset.saturating_add(header.raw_len as u64);
        }

        projected
    }
}

pub(crate) struct DirectoryRestoreOutcome {
    pub(crate) decoded: DecodedArchivePayload,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

pub struct Extractor {
    pub num_workers: usize,
    buffer_pool: Arc<BufferPool>,
}

impl Extractor {
    pub fn new(num_workers: usize, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            num_workers,
            buffer_pool,
        }
    }

    pub fn probe_archive_source_kind<R: Read + Seek>(reader: &mut R) -> Result<ArchiveSourceKind> {
        let position = reader.stream_position()?;
        reader.seek(SeekFrom::Start(0))?;
        let header = GlobalHeader::read(reader)?;
        reader.seek(SeekFrom::Start(position))?;
        Ok(header.source_kind())
    }

    pub fn read_archive_payload_with_metrics<R: Read + Seek + Send + 'static>(
        &self,
        reader: R,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new_for_sequential_extract(reader)?;
        let archive_read_elapsed = archive_started.elapsed();
        let writer = VecChunkWriter::default();
        let (decoded, writer) = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            None,
            writer,
        )?;
        let payload = writer.into_inner();
        let payload_len = payload.len() as u64;
        if payload_len != decoded.decoded_bytes_total {
            return Err(crate::OxideError::InvalidFormat(
                "decoded bytes mismatch after ordered write",
            ));
        }

        Ok(DecodedArchivePayload {
            flags: decoded.flags,
            payload,
            decoded_bytes_total: decoded.decoded_bytes_total,
            archive_bytes_total: decoded.archive_bytes_total,
            blocks_total: decoded.blocks_total,
            workers: decoded.workers,
            stage_timings: decoded.stage_timings,
            pipeline_stats: decoded.pipeline_stats,
        })
    }

    pub fn extract_file_to_path_with_metrics<R: Read + Seek + Send + 'static>(
        &self,
        reader: R,
        output_path: &Path,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new_for_sequential_extract(reader)?;
        let archive_read_elapsed = archive_started.elapsed();
        if archive.source_kind() != ArchiveSourceKind::File {
            return Err(crate::OxideError::InvalidFormat(
                "archive is not a file payload",
            ));
        }

        let entry = archive.manifest().entries().first().cloned().ok_or(
            crate::OxideError::InvalidFormat("file archive manifest is empty"),
        )?;
        if !matches!(entry.kind, crate::ArchiveEntryKind::File) {
            return Err(crate::OxideError::InvalidFormat(
                "file archive manifest does not describe a file entry",
            ));
        }

        let writer = FileChunkWriter::create(output_path, entry)?;
        let (mut decoded, mut writer) = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            None,
            writer,
        )?;
        writer.flush_and_apply_metadata()?;
        apply_file_restore_stats(&mut decoded.stage_timings, writer.stats());

        if !matches!(
            directory::source_kind_from_flags(decoded.flags),
            ArchiveSourceKind::File
        ) {
            return Err(crate::OxideError::InvalidFormat(
                "archive is not a file payload",
            ));
        }

        Ok(DecodedArchivePayload {
            flags: decoded.flags,
            payload: Vec::new(),
            decoded_bytes_total: decoded.decoded_bytes_total,
            archive_bytes_total: decoded.archive_bytes_total,
            blocks_total: decoded.blocks_total,
            workers: decoded.workers,
            stage_timings: decoded.stage_timings,
            pipeline_stats: decoded.pipeline_stats,
        })
    }

    pub(crate) fn extract_directory_to_path_with_metrics<R: Read + Seek + Send + 'static>(
        &self,
        reader: R,
        output_path: &Path,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DirectoryRestoreOutcome> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new_for_sequential_extract(reader)?;
        let archive_read_elapsed = archive_started.elapsed();
        if archive.source_kind() != ArchiveSourceKind::Directory {
            return Err(crate::OxideError::InvalidFormat(
                "archive is not a directory payload",
            ));
        }

        let manifest = archive.manifest().clone();
        let writer = DirectoryRestoreWriter::create(output_path, manifest)?;
        let (decoded, mut writer) = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            None,
            writer,
        )?;

        let restore_stats = writer.finish()?;
        let mut stage_timings = decoded.stage_timings;
        apply_directory_restore_stats(&mut stage_timings, restore_stats);

        Ok(DirectoryRestoreOutcome {
            decoded: DecodedArchivePayload {
                flags: decoded.flags,
                payload: Vec::new(),
                decoded_bytes_total: decoded.decoded_bytes_total,
                archive_bytes_total: decoded.archive_bytes_total,
                blocks_total: decoded.blocks_total,
                workers: decoded.workers,
                stage_timings,
                pipeline_stats: decoded.pipeline_stats,
            },
            output_bytes_total: restore_stats.output_bytes_total,
            entry_count: restore_stats.entry_count,
        })
    }

    pub(crate) fn extract_directory_to_path_filtered_with_metrics<R, S, T>(
        &self,
        reader: R,
        output_path: &Path,
        filters: &[S],
        regex_filters: &[T],
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DirectoryRestoreOutcome>
    where
        R: Read + Seek + Send + 'static,
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new_for_sequential_extract(reader)?;
        let archive_read_elapsed = archive_started.elapsed();
        if archive.source_kind() != ArchiveSourceKind::Directory {
            return Err(crate::OxideError::InvalidFormat(
                "archive is not a directory payload",
            ));
        }

        let selection =
            DirectoryExtractSelection::from_filters(archive.manifest(), filters, regex_filters)?;
        let selected_ranges = selection.selected_ranges().to_vec();
        let decode_plan = DecodePlan::from_ranges(archive.block_descriptors(), &selected_ranges);
        let projected_ranges =
            decode_plan.project_ranges(archive.block_descriptors(), &selected_ranges);
        let manifest = selection.into_manifest();
        let writer =
            FilteredDirectoryRestoreWriter::create(output_path, manifest, projected_ranges)?;
        let (decoded, mut writer) = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            Some(&selected_ranges),
            writer,
        )?;

        let restore_stats = writer.finish()?;
        let mut stage_timings = decoded.stage_timings;
        apply_directory_restore_stats(&mut stage_timings, restore_stats);

        Ok(DirectoryRestoreOutcome {
            decoded: DecodedArchivePayload {
                flags: decoded.flags,
                payload: Vec::new(),
                decoded_bytes_total: decoded.decoded_bytes_total,
                archive_bytes_total: decoded.archive_bytes_total,
                blocks_total: decoded.blocks_total,
                workers: decoded.workers,
                stage_timings,
                pipeline_stats: decoded.pipeline_stats,
            },
            output_bytes_total: restore_stats.output_bytes_total,
            entry_count: restore_stats.entry_count,
        })
    }

    fn decode_archive_to_writer<R, W>(
        &self,
        archive: ArchiveReader<R>,
        archive_read_elapsed: Duration,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
        selected_ranges: Option<&[Range<u64>]>,
        writer: W,
    ) -> Result<(DecodeStreamOutcome, W)>
    where
        R: Read + Seek + Send + 'static,
        W: OrderedChunkWriter + Send + 'static,
    {
        let mut stage_timings = ExtractStageTimings::default();
        stage_timings.archive_read += archive_read_elapsed;
        let source_kind = archive.source_kind();
        let flags = directory::source_kind_flags(source_kind);
        let block_descriptors = archive.block_descriptors().to_vec();
        let archive_header = archive.global_header();
        let decode_plan = match selected_ranges {
            Some(ranges) => DecodePlan::from_ranges(&block_descriptors, ranges),
            None => DecodePlan::all(archive.block_count() as usize),
        };
        let block_capacity = decode_plan.block_count();
        let worker_count = self.num_workers.max(1);
        let queue_capacity = decode_queue_capacity(worker_count, block_capacity);
        let ordered_write_queue_capacity =
            ordered_write_queue_capacity(worker_count, queue_capacity, block_capacity);
        let reorder_pending_limit =
            reorder_pending_limit(ordered_write_queue_capacity, block_capacity);

        let (read_request_tx, read_request_rx) = bounded::<ReadRequest>(queue_capacity);
        let (task_tx, task_rx) = bounded::<DecodeTask>(queue_capacity);
        let (result_tx, result_rx) =
            bounded::<(usize, Duration, Result<DecodedBlock>)>(queue_capacity);
        let (ordered_write_tx, ordered_write_rx) =
            bounded::<OrderedWriteTask>(ordered_write_queue_capacity);
        let runtime_state = Arc::new(DecodeRuntimeState::new(worker_count, started_at));
        let mut worker_handles = Vec::with_capacity(worker_count);
        let ordered_writer_handle = spawn_ordered_writer(
            writer,
            ordered_write_rx,
            reorder_pending_limit,
            block_capacity,
        );
        let dictionary_bank = Arc::new(archive.manifest().dictionary_bank().clone());
        let reader_buffer_pool = Arc::clone(&self.buffer_pool);
        let reader_task_tx = task_tx.clone();
        let reader_result_tx = result_tx.clone();
        let io_reader_handle = thread::spawn(move || -> Result<()> {
            spawn_io_reader(
                archive,
                read_request_rx,
                reader_task_tx,
                reader_result_tx,
                reader_buffer_pool,
            )
        });

        for worker_id in 0..worker_count {
            let local_task_rx = task_rx.clone();
            let local_result_tx = result_tx.clone();
            let local_runtime = Arc::clone(&runtime_state);
            let local_buffer_pool = Arc::clone(&self.buffer_pool);
            let local_dictionary_bank = Arc::clone(&dictionary_bank);
            let handle = thread::spawn(move || -> DecodeWorkerOutcome {
                let started = Instant::now();
                let mut tasks_completed = 0usize;
                let mut busy = Duration::ZERO;
                let mut scratch = CompressionScratchArena::new();
                local_runtime.mark_worker_started(worker_id);

                while let Ok(task) = local_task_rx.recv() {
                    let decode_started = Instant::now();
                    let decoded = decode_block_payload_with_scratch(
                        task.header,
                        task.block_data,
                        &mut scratch,
                        &local_buffer_pool,
                        local_dictionary_bank.as_ref(),
                    );
                    let busy_elapsed = decode_started.elapsed();
                    busy += busy_elapsed;
                    local_runtime.record_worker_task(worker_id, busy_elapsed);
                    tasks_completed += 1;
                    if local_result_tx
                        .send((task.index, task.read_elapsed, decoded))
                        .is_err()
                    {
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

        let archive_bytes_total = archive_header.footer_offset + crate::FOOTER_SIZE as u64;
        let mut archive_bytes_completed = archive_header.payload_offset
            + u64::from(archive_header.entry_table_len)
            + u64::from(archive_header.chunk_table_len)
            + crate::FOOTER_SIZE as u64;
        let mut submitted = 0usize;
        let mut received = 0usize;
        let mut first_error: Option<crate::OxideError> = None;
        let mut decoded_bytes_completed = 0u64;
        let mut received_indices = vec![false; block_capacity];
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut decode_task_queue_peak = 0usize;
        let mut decode_result_queue_peak = 0usize;
        let mut ordered_write_queue_peak = 0usize;
        let mut ordered_writer_disconnected = false;
        let mut ordered_write_tx = Some(ordered_write_tx);

        let run_result = (|| -> Result<()> {
            for (block_index, header) in block_descriptors.iter().copied().enumerate() {
                archive_bytes_completed =
                    archive_bytes_completed.saturating_add(header.encoded_len as u64);

                if !decode_plan.includes(block_index) {
                    let force = false;
                    if progress_emit_due(&last_emit_at, emit_every, force) {
                        emit_extract_progress_if_due(
                            source_kind,
                            started_at,
                            archive_bytes_completed,
                            decoded_bytes_completed,
                            block_capacity as u32,
                            received as u32,
                            runtime_state.snapshot(),
                            emit_every,
                            &mut last_emit_at,
                            force,
                            sink,
                        );
                    }
                    continue;
                }

                while submitted.saturating_sub(received) >= queue_capacity {
                    receive_decode_result(
                        &result_rx,
                        &mut stage_timings,
                        block_capacity,
                        &mut received_indices,
                        &runtime_state,
                        &mut decoded_bytes_completed,
                        &mut received,
                        &mut first_error,
                    )
                    .and_then(|outcome| {
                        forward_processed_decode_result(
                            outcome,
                            &mut ordered_write_tx,
                            &mut ordered_write_queue_peak,
                            &mut ordered_writer_disconnected,
                            &mut stage_timings,
                        )
                    })?;
                    decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                    let force = false;
                    if progress_emit_due(&last_emit_at, emit_every, force) {
                        emit_extract_progress_if_due(
                            source_kind,
                            started_at,
                            archive_bytes_completed,
                            decoded_bytes_completed,
                            block_capacity as u32,
                            received as u32,
                            runtime_state.snapshot(),
                            emit_every,
                            &mut last_emit_at,
                            force,
                            sink,
                        );
                    }
                }

                let submit_started = Instant::now();
                read_request_tx
                    .send(ReadRequest {
                        index: submitted,
                        block_index: block_index as u32,
                    })
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode read queue closed before submission completed".to_string(),
                        )
                    })?;
                stage_timings.decode_submit += submit_started.elapsed();
                submitted += 1;
                runtime_state.record_submission();
                decode_task_queue_peak = decode_task_queue_peak.max(task_tx.len());

                let mut drained = 0usize;
                while drained < RESULT_DRAIN_BUDGET {
                    let result = match result_rx.try_recv() {
                        Ok(result) => result,
                        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                    };
                    stage_timings.archive_read += result.1;
                    let outcome = process_decode_result(
                        result,
                        block_capacity,
                        &mut received_indices,
                        &runtime_state,
                        &mut decoded_bytes_completed,
                        &mut received,
                        &mut first_error,
                    )?;
                    forward_processed_decode_result(
                        outcome,
                        &mut ordered_write_tx,
                        &mut ordered_write_queue_peak,
                        &mut ordered_writer_disconnected,
                        &mut stage_timings,
                    )?;
                    drained += 1;
                }

                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                let force = false;
                if progress_emit_due(&last_emit_at, emit_every, force) {
                    emit_extract_progress_if_due(
                        source_kind,
                        started_at,
                        archive_bytes_completed,
                        decoded_bytes_completed,
                        block_capacity as u32,
                        received as u32,
                        runtime_state.snapshot(),
                        emit_every,
                        &mut last_emit_at,
                        force,
                        sink,
                    );
                }
            }

            if submitted != block_capacity && first_error.is_none() {
                return Err(crate::OxideError::InvalidFormat(
                    "archive block count mismatch during decode",
                ));
            }

            while received < submitted {
                receive_decode_result(
                    &result_rx,
                    &mut stage_timings,
                    block_capacity,
                    &mut received_indices,
                    &runtime_state,
                    &mut decoded_bytes_completed,
                    &mut received,
                    &mut first_error,
                )
                .and_then(|outcome| {
                    forward_processed_decode_result(
                        outcome,
                        &mut ordered_write_tx,
                        &mut ordered_write_queue_peak,
                        &mut ordered_writer_disconnected,
                        &mut stage_timings,
                    )
                })?;
                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                let force = false;
                if progress_emit_due(&last_emit_at, emit_every, force) {
                    emit_extract_progress_if_due(
                        source_kind,
                        started_at,
                        archive_bytes_completed,
                        decoded_bytes_completed,
                        block_capacity as u32,
                        received as u32,
                        runtime_state.snapshot(),
                        emit_every,
                        &mut last_emit_at,
                        force,
                        sink,
                    );
                }
            }

            Ok(())
        })();

        drop(read_request_tx);
        drop(task_tx);
        if run_result.is_err() || first_error.is_some() {
            abort_ordered_writer(&mut ordered_write_tx);
        }
        drop(ordered_write_tx);
        drop(result_rx);

        let io_reader_result = join_io_reader(io_reader_handle);
        let workers_result = join_decode_workers(worker_handles);
        let ordered_writer_result = join_ordered_writer(ordered_writer_handle);

        if let Some(error) = first_error {
            return Err(error);
        }
        io_reader_result?;
        if let Err(error) = run_result {
            if ordered_writer_disconnected && let Err(writer_error) = ordered_writer_result {
                return Err(writer_error);
            }
            return Err(error);
        }
        let workers = workers_result?;

        let OrderedWriterOutcome {
            writer,
            stats: reorder_stats,
        } = ordered_writer_result?;

        stage_timings.ordered_write += reorder_stats.write_elapsed;
        stage_timings.merge += reorder_stats
            .push_elapsed
            .saturating_sub(reorder_stats.write_elapsed);
        if options.emit_final_progress {
            let force = true;
            if progress_emit_due(&last_emit_at, emit_every, force) {
                emit_extract_progress(
                    source_kind,
                    started_at,
                    archive_bytes_completed,
                    decoded_bytes_completed,
                    block_capacity as u32,
                    received as u32,
                    runtime_state.snapshot(),
                    sink,
                );
            }
        }

        Ok((
            DecodeStreamOutcome {
                flags,
                decoded_bytes_total: decoded_bytes_completed,
                archive_bytes_total,
                blocks_total: submitted as u32,
                workers,
                stage_timings,
                pipeline_stats: ExtractPipelineStats {
                    decode_task_queue_capacity: queue_capacity,
                    decode_task_queue_peak,
                    decode_result_queue_capacity: queue_capacity,
                    decode_result_queue_peak,
                    ordered_write_queue_capacity,
                    ordered_write_queue_peak,
                    reorder_pending_limit,
                    reorder_pending_peak: reorder_stats.pending_blocks_peak,
                    reorder_pending_bytes_peak: reorder_stats.pending_bytes_peak,
                },
            },
            writer,
        ))
    }

    pub fn restore_decoded_payload(
        &self,
        output_path: &Path,
        decoded: &mut DecodedArchivePayload,
        extensions: &mut BTreeMap<String, ReportValue>,
    ) -> Result<(ArchiveSourceKind, u64)> {
        let directory_decode_started = Instant::now();
        let source_kind = directory::source_kind_from_flags(decoded.flags);
        decoded.stage_timings.directory_decode += directory_decode_started.elapsed();

        if source_kind == ArchiveSourceKind::Directory {
            return Err(crate::OxideError::InvalidFormat(
                "directory payload restoration requires archive metadata; use extract_path or extract_directory_archive",
            ));
        }

        let _ = extensions;
        if let Some(parent) = output_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            let create_started = Instant::now();
            fs::create_dir_all(parent)?;
            let elapsed = create_started.elapsed();
            decoded.stage_timings.output_create += elapsed;
            decoded.stage_timings.output_write += elapsed;
        }
        let write_started = Instant::now();
        fs::write(output_path, &decoded.payload)?;
        let elapsed = write_started.elapsed();
        decoded.stage_timings.output_data += elapsed;
        decoded.stage_timings.output_write += elapsed;
        Ok((ArchiveSourceKind::File, decoded.payload.len() as u64))
    }
}

#[inline]
fn decode_queue_capacity(worker_count: usize, block_capacity: usize) -> usize {
    worker_count
        .saturating_mul(DECODE_QUEUE_MULTIPLIER)
        .max(MIN_DECODE_QUEUE_CAPACITY)
        .min(block_capacity.max(1))
        .max(1)
}

#[inline]
fn ordered_write_queue_capacity(
    worker_count: usize,
    decode_queue_capacity: usize,
    block_capacity: usize,
) -> usize {
    worker_count
        .saturating_mul(ORDERED_WRITE_QUEUE_MULTIPLIER)
        .max(MIN_ORDERED_WRITE_QUEUE_CAPACITY)
        .max(decode_queue_capacity)
        .min(block_capacity.max(1))
        .max(1)
}

#[inline]
fn reorder_pending_limit(ordered_write_queue_capacity: usize, block_capacity: usize) -> usize {
    ordered_write_queue_capacity
        .saturating_mul(REORDER_PENDING_MULTIPLIER)
        .min(block_capacity.max(1))
        .max(1)
}

fn apply_file_restore_stats(stage_timings: &mut ExtractStageTimings, stats: FileRestoreStats) {
    stage_timings.ordered_write = stage_timings
        .ordered_write
        .saturating_sub(stats.ordered_write_time);
    stage_timings.output_write += stats.output_write;
    stage_timings.output_create += stats.output_create;
    stage_timings.output_create_directories += stats.output_create_directories;
    stage_timings.output_create_files += stats.output_create_files;
    stage_timings.output_data += stats.output_data;
    stage_timings.output_flush += stats.output_flush;
    stage_timings.output_metadata += stats.output_metadata;
    stage_timings.output_metadata_files += stats.output_metadata_files;
}

fn apply_directory_restore_stats(
    stage_timings: &mut ExtractStageTimings,
    restore_stats: super::directory_restore::DirectoryRestoreStats,
) {
    stage_timings.ordered_write = stage_timings
        .ordered_write
        .saturating_sub(restore_stats.ordered_write_time);
    stage_timings.directory_decode += restore_stats.directory_decode;
    stage_timings.output_prepare_directories += restore_stats.output_prepare_directories;
    stage_timings.output_write += restore_stats.output_write;
    stage_timings.output_create += restore_stats.output_create;
    stage_timings.output_create_directories += restore_stats.output_create_directories;
    stage_timings.output_create_files += restore_stats.output_create_files;
    stage_timings.output_data += restore_stats.output_data;
    stage_timings.output_flush += restore_stats.output_flush;
    stage_timings.output_metadata += restore_stats.output_metadata;
    stage_timings.output_metadata_files += restore_stats.output_metadata_files;
    stage_timings.output_metadata_directories += restore_stats.output_metadata_directories;
}

fn spawn_io_reader<R>(
    mut archive: ArchiveReader<R>,
    read_request_rx: Receiver<ReadRequest>,
    task_tx: Sender<DecodeTask>,
    result_tx: Sender<(usize, Duration, Result<DecodedBlock>)>,
    buffer_pool: Arc<BufferPool>,
) -> Result<()>
where
    R: Read + Seek,
{
    while let Ok(request) = read_request_rx.recv() {
        let read_started = Instant::now();
        let mut block_data = buffer_pool.acquire();
        let read_result = archive.read_block_into(request.block_index, block_data.as_mut_vec());
        let read_elapsed = read_started.elapsed();

        match read_result {
            Ok(header) => {
                task_tx
                    .send(DecodeTask {
                        index: request.index,
                        header,
                        block_data,
                        read_elapsed,
                    })
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode queue closed before I/O submission completed".to_string(),
                        )
                    })?;
            }
            Err(error) => {
                result_tx
                    .send((request.index, read_elapsed, Err(error)))
                    .map_err(|_| {
                        crate::OxideError::CompressionError(
                            "decode result channel closed before I/O error delivery".to_string(),
                        )
                    })?;
            }
        }
    }

    archive.finish_sequential_extract_validation()
}

fn receive_decode_result(
    result_rx: &Receiver<(usize, Duration, Result<DecodedBlock>)>,
    stage_timings: &mut ExtractStageTimings,
    total_blocks: usize,
    received_indices: &mut [bool],
    runtime_state: &DecodeRuntimeState,
    decoded_bytes_completed: &mut u64,
    received: &mut usize,
    first_error: &mut Option<crate::OxideError>,
) -> Result<ProcessedDecodeResult> {
    let wait_started = Instant::now();
    let result = result_rx.recv().map_err(|_| {
        crate::OxideError::CompressionError(
            "decode result channel closed before completion".to_string(),
        )
    })?;
    stage_timings.decode_wait += wait_started.elapsed();
    stage_timings.archive_read += result.1;
    process_decode_result(
        result,
        total_blocks,
        received_indices,
        runtime_state,
        decoded_bytes_completed,
        received,
        first_error,
    )
}

fn process_decode_result(
    (index, _read_elapsed, block): (usize, Duration, Result<DecodedBlock>),
    total_blocks: usize,
    received_indices: &mut [bool],
    runtime_state: &DecodeRuntimeState,
    decoded_bytes_completed: &mut u64,
    received: &mut usize,
    first_error: &mut Option<crate::OxideError>,
) -> Result<ProcessedDecodeResult> {
    if index >= total_blocks || index >= received_indices.len() {
        return Err(crate::OxideError::InvalidFormat(
            "decode result index out of bounds",
        ));
    }
    if received_indices[index] {
        return Err(crate::OxideError::InvalidFormat(
            "duplicate decode result index",
        ));
    }
    received_indices[index] = true;
    *received += 1;
    runtime_state.record_completion();

    match block {
        Ok(bytes) => {
            *decoded_bytes_completed = decoded_bytes_completed.saturating_add(bytes.len() as u64);
            if first_error.is_none() {
                Ok(ProcessedDecodeResult::Block { index, bytes })
            } else {
                Ok(ProcessedDecodeResult::IgnoredAfterError)
            }
        }
        Err(error) => {
            let is_new_error = first_error.is_none();
            first_error.get_or_insert(error);
            if is_new_error {
                Ok(ProcessedDecodeResult::NewError)
            } else {
                Ok(ProcessedDecodeResult::IgnoredAfterError)
            }
        }
    }
}

fn forward_processed_decode_result(
    outcome: ProcessedDecodeResult,
    ordered_write_tx: &mut Option<crossbeam_channel::Sender<OrderedWriteTask>>,
    ordered_write_queue_peak: &mut usize,
    ordered_writer_disconnected: &mut bool,
    stage_timings: &mut ExtractStageTimings,
) -> Result<()> {
    match outcome {
        ProcessedDecodeResult::Block { index, bytes } => {
            if let Some(tx) = ordered_write_tx.as_ref() {
                let enqueue_started = Instant::now();
                let send_result = tx.send(OrderedWriteTask::Block { index, bytes });
                stage_timings.writer_enqueue_blocked += enqueue_started.elapsed();
                match send_result {
                    Ok(()) => {
                        *ordered_write_queue_peak = (*ordered_write_queue_peak).max(tx.len());
                    }
                    Err(_) => {
                        *ordered_writer_disconnected = true;
                        *ordered_write_tx = None;
                        return Err(crate::OxideError::CompressionError(
                            "ordered write queue closed before completion".to_string(),
                        ));
                    }
                }
            }
        }
        ProcessedDecodeResult::NewError => {
            abort_ordered_writer(ordered_write_tx);
        }
        ProcessedDecodeResult::IgnoredAfterError => {}
    }
    Ok(())
}

fn abort_ordered_writer(
    ordered_write_tx: &mut Option<crossbeam_channel::Sender<OrderedWriteTask>>,
) {
    if let Some(tx) = ordered_write_tx.take() {
        let _ = tx.send(OrderedWriteTask::Abort);
    }
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
            crate::OxideError::CompressionError(format!("decode worker thread panicked: {details}"))
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

fn join_io_reader(handle: thread::JoinHandle<Result<()>>) -> Result<()> {
    handle.join().map_err(|payload| {
        let details = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        };
        crate::OxideError::CompressionError(format!("I/O reader thread panicked: {details}"))
    })?
}

fn spawn_ordered_writer<W>(
    writer: W,
    ordered_write_rx: Receiver<OrderedWriteTask>,
    reorder_pending_limit: usize,
    expected_blocks: usize,
) -> thread::JoinHandle<Result<OrderedWriterOutcome<W>>>
where
    W: OrderedChunkWriter + Send + 'static,
{
    thread::spawn(move || {
        let mut reorder = BoundedReorderWriter::with_limit(writer, reorder_pending_limit);

        while let Ok(task) = ordered_write_rx.recv() {
            match task {
                OrderedWriteTask::Block { index, bytes } => {
                    reorder.push(index, bytes)?;
                }
                OrderedWriteTask::Abort => {
                    let (writer, stats) = reorder.into_parts();
                    return Ok(OrderedWriterOutcome { writer, stats });
                }
            }
        }

        let (writer, stats) = reorder.finish(expected_blocks)?;
        Ok(OrderedWriterOutcome { writer, stats })
    })
}

fn join_ordered_writer<W>(
    handle: thread::JoinHandle<Result<OrderedWriterOutcome<W>>>,
) -> Result<OrderedWriterOutcome<W>> {
    handle.join().map_err(|payload| {
        let details = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "unknown panic payload".to_string()
        };
        crate::OxideError::CompressionError(format!("ordered writer thread panicked: {details}"))
    })?
}

pub fn decode_block_payload(header: ChunkDescriptor, block_data: Vec<u8>) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    let dictionary_bank = ArchiveDictionaryBank::default();
    let compression_meta = header.compression_meta()?;
    let decoded = if compression_meta.raw_passthrough {
        block_data
    } else if crate::compression::supports_direct_buffer_output(compression_meta.algo) {
        let mut decoded = Vec::new();
        crate::compression::reverse_compression_request_with_scratch_into(
            crate::compression::DecompressionRequest {
                data: &block_data,
                algo: compression_meta.algo,
                raw_len: Some(header.raw_len as usize),
                dictionary_id: compression_meta.dictionary_id,
                dictionary: dictionary_bank
                    .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
            },
            &mut scratch,
            &mut decoded,
        )?;
        decoded
    } else {
        crate::compression::reverse_compression_request_with_scratch(
            crate::compression::DecompressionRequest {
                data: &block_data,
                algo: compression_meta.algo,
                raw_len: Some(header.raw_len as usize),
                dictionary_id: compression_meta.dictionary_id,
                dictionary: dictionary_bank
                    .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
            },
            &mut scratch,
        )?
    };
    if decoded.len() != header.raw_len as usize {
        return Err(crate::OxideError::InvalidFormat(
            "decoded block size mismatch",
        ));
    }
    Ok(decoded)
}

fn decode_block_payload_with_scratch(
    header: ChunkDescriptor,
    block_data: PooledBuffer,
    scratch: &mut CompressionScratchArena,
    pool: &BufferPool,
    dictionary_bank: &ArchiveDictionaryBank,
) -> Result<DecodedBlock> {
    let compression_meta = header.compression_meta()?;
    let decoded = if compression_meta.raw_passthrough {
        DecodedBlock::Pooled(block_data)
    } else if crate::compression::supports_direct_buffer_output(compression_meta.algo) {
        let mut decoded = pool.acquire();
        crate::compression::reverse_compression_request_with_scratch_into(
            crate::compression::DecompressionRequest {
                data: block_data.as_slice(),
                algo: compression_meta.algo,
                raw_len: Some(header.raw_len as usize),
                dictionary_id: compression_meta.dictionary_id,
                dictionary: dictionary_bank
                    .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
            },
            scratch,
            decoded.as_mut_vec(),
        )?;
        DecodedBlock::Pooled(decoded)
    } else {
        DecodedBlock::Owned(
            crate::compression::reverse_compression_request_with_scratch(
                crate::compression::DecompressionRequest {
                    data: block_data.as_slice(),
                    algo: compression_meta.algo,
                    raw_len: Some(header.raw_len as usize),
                    dictionary_id: compression_meta.dictionary_id,
                    dictionary: dictionary_bank
                        .dictionary_bytes(compression_meta.dictionary_id, compression_meta.algo),
                },
                scratch,
            )?,
        )
    };
    if decoded.len() != header.raw_len as usize {
        return Err(crate::OxideError::InvalidFormat(
            "decoded block size mismatch",
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
#[path = "../../../tests/pipeline/archive/extractor.rs"]
mod tests;
