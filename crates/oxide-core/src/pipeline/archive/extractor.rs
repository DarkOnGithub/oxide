use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, TryRecvError, bounded};

use crate::core::WorkerRuntimeSnapshot;
use crate::format::{ArchiveMetadata, ArchiveReader, BlockHeader, GlobalHeader};
use crate::telemetry::{ReportValue, RunTelemetryOptions, TelemetrySink};
use crate::types::Result;

use super::super::directory;
use super::super::types::ArchiveSourceKind;
use super::directory_restore::DirectoryRestoreWriter;
use super::reorder_writer::{BoundedReorderWriter, OrderedChunkWriter};
use super::telemetry::*;
use super::types::*;

const DECODE_QUEUE_MULTIPLIER: usize = 4;
const REORDER_PENDING_MULTIPLIER: usize = 2;
const RESULT_DRAIN_BUDGET: usize = 32;

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
}

impl FileChunkWriter {
    fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let file = fs::File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

impl OrderedChunkWriter for FileChunkWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
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

pub(crate) struct DirectoryRestoreOutcome {
    pub(crate) decoded: DecodedArchivePayload,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

pub struct Extractor {
    pub num_workers: usize,
}

impl Extractor {
    pub fn new(num_workers: usize) -> Self {
        Self { num_workers }
    }

    pub fn probe_archive_source_kind<R: Read + Seek>(reader: &mut R) -> Result<ArchiveSourceKind> {
        let position = reader.stream_position()?;
        reader.seek(SeekFrom::Start(0))?;
        let header = GlobalHeader::read(reader)?;
        reader.seek(SeekFrom::Start(header.metadata_offset))?;
        let metadata = ArchiveMetadata::read(reader)?;
        reader.seek(SeekFrom::Start(position))?;
        Ok(metadata.source_kind)
    }

    pub fn read_archive_payload_with_metrics<R: Read + Seek>(
        &self,
        reader: R,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new_for_sequential_extract(reader)?;
        let archive_read_elapsed = archive_started.elapsed();
        let mut writer = VecChunkWriter::default();
        let decoded = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            &mut writer,
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

    pub fn extract_file_to_path_with_metrics<R: Read + Seek>(
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
        let mut writer = FileChunkWriter::create(output_path)?;
        let decoded = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            &mut writer,
        )?;
        writer.flush()?;

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

    pub(crate) fn extract_directory_to_path_with_metrics<R: Read + Seek>(
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
        let mut writer = DirectoryRestoreWriter::create(output_path, manifest)?;
        let decoded = self.decode_archive_to_writer(
            archive,
            archive_read_elapsed,
            started_at,
            options,
            sink,
            &mut writer,
        )?;

        let restore_stats = writer.finish()?;
        let mut stage_timings = decoded.stage_timings;
        let restore_time = restore_stats.directory_decode + restore_stats.output_write;
        stage_timings.ordered_write = stage_timings.ordered_write.saturating_sub(restore_time);
        stage_timings.directory_decode += restore_stats.directory_decode;
        stage_timings.output_write += restore_stats.output_write;

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
        mut archive: ArchiveReader<R>,
        archive_read_elapsed: Duration,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
        writer: &mut W,
    ) -> Result<DecodeStreamOutcome>
    where
        R: Read + Seek,
        W: OrderedChunkWriter,
    {
        let mut stage_timings = ExtractStageTimings::default();
        stage_timings.archive_read += archive_read_elapsed;
        let source_kind = archive.source_kind();
        let flags = directory::source_kind_flags(source_kind);
        let block_capacity = archive.block_count() as usize;
        let worker_count = self.num_workers.max(1);
        let queue_capacity = worker_count.saturating_mul(DECODE_QUEUE_MULTIPLIER).max(1);
        let reorder_pending_limit = queue_capacity
            .saturating_mul(REORDER_PENDING_MULTIPLIER)
            .max(1);

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
                    let decoded = decode_block_payload(
                        task.header,
                        task.block_data,
                        task.dictionary.as_deref(),
                    );
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

        let archive_bytes_total = archive.global_header().footer_offset + crate::FOOTER_SIZE as u64;
        let mut archive_bytes_completed =
            archive.global_header().payload_offset + crate::FOOTER_SIZE as u64;
        let mut submitted = 0usize;
        let mut received = 0usize;
        let mut first_error: Option<crate::OxideError> = None;
        let mut decoded_bytes_completed = 0u64;
        let mut received_indices = vec![false; block_capacity];
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut decode_task_queue_peak = 0usize;
        let mut decode_result_queue_peak = 0usize;
        let mut reorder = BoundedReorderWriter::with_limit(writer, reorder_pending_limit);

        for block_index in 0..block_capacity {
            let read_started = Instant::now();
            let (header, block_data) = archive.read_block(block_index as u32)?;
            stage_timings.archive_read += read_started.elapsed();
            archive_bytes_completed =
                archive_bytes_completed.saturating_add(header.encoded_len as u64);

            while submitted.saturating_sub(received) >= queue_capacity {
                receive_decode_result_to_writer(
                    &result_rx,
                    &mut stage_timings,
                    &mut reorder,
                    block_capacity,
                    &mut received_indices,
                    &runtime_state,
                    &mut decoded_bytes_completed,
                    &mut received,
                    &mut first_error,
                )?;
                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
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
                    false,
                    sink,
                );
            }

            let submit_started = Instant::now();
            let dictionary = archive
                .dictionary(header.dict_id)
                .map(|bytes| bytes.to_vec());
            task_tx
                .send(DecodeTask {
                    index: submitted,
                    header,
                    block_data,
                    dictionary,
                })
                .map_err(|_| {
                    crate::OxideError::CompressionError(
                        "decode queue closed before submission completed".to_string(),
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
                process_decode_result_to_writer(
                    result,
                    &mut stage_timings,
                    &mut reorder,
                    block_capacity,
                    &mut received_indices,
                    &runtime_state,
                    &mut decoded_bytes_completed,
                    &mut received,
                    &mut first_error,
                )?;
                drained += 1;
            }

            decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
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
                false,
                sink,
            );
        }
        drop(task_tx);

        if let Err(error) = archive.finish_sequential_extract_validation() {
            first_error.get_or_insert(error);
        }

        if submitted != block_capacity {
            return Err(crate::OxideError::InvalidFormat(
                "archive block count mismatch during decode",
            ));
        }

        while received < submitted {
            receive_decode_result_to_writer(
                &result_rx,
                &mut stage_timings,
                &mut reorder,
                block_capacity,
                &mut received_indices,
                &runtime_state,
                &mut decoded_bytes_completed,
                &mut received,
                &mut first_error,
            )?;
            decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
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
                false,
                sink,
            );
        }

        let workers = join_decode_workers(worker_handles)?;
        if let Some(error) = first_error {
            return Err(error);
        }

        let (_writer, reorder_stats) = reorder.finish(submitted)?;
        if options.emit_final_progress {
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

        Ok(DecodeStreamOutcome {
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
                reorder_pending_limit,
                reorder_pending_peak: reorder_stats.pending_blocks_peak,
                reorder_pending_bytes_peak: reorder_stats.pending_bytes_peak,
            },
        })
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
            fs::create_dir_all(parent)?;
        }
        let write_started = Instant::now();
        fs::write(output_path, &decoded.payload)?;
        decoded.stage_timings.output_write += write_started.elapsed();
        Ok((ArchiveSourceKind::File, decoded.payload.len() as u64))
    }
}

pub fn receive_decode_result_to_writer<W: OrderedChunkWriter>(
    result_rx: &Receiver<(usize, Result<Vec<u8>>)>,
    stage_timings: &mut ExtractStageTimings,
    reorder: &mut BoundedReorderWriter<W>,
    total_blocks: usize,
    received_indices: &mut [bool],
    runtime_state: &DecodeRuntimeState,
    decoded_bytes_completed: &mut u64,
    received: &mut usize,
    first_error: &mut Option<crate::OxideError>,
) -> Result<()> {
    let wait_started = Instant::now();
    let result = result_rx.recv().map_err(|_| {
        crate::OxideError::CompressionError(
            "decode result channel closed before completion".to_string(),
        )
    })?;
    stage_timings.decode_wait += wait_started.elapsed();
    process_decode_result_to_writer(
        result,
        stage_timings,
        reorder,
        total_blocks,
        received_indices,
        runtime_state,
        decoded_bytes_completed,
        received,
        first_error,
    )
}

pub fn process_decode_result_to_writer<W: OrderedChunkWriter>(
    (index, block): (usize, Result<Vec<u8>>),
    stage_timings: &mut ExtractStageTimings,
    reorder: &mut BoundedReorderWriter<W>,
    total_blocks: usize,
    received_indices: &mut [bool],
    runtime_state: &DecodeRuntimeState,
    decoded_bytes_completed: &mut u64,
    received: &mut usize,
    first_error: &mut Option<crate::OxideError>,
) -> Result<()> {
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
                let reorder_started = Instant::now();
                let push_stats = reorder.push(index, bytes)?;
                let reorder_elapsed = reorder_started.elapsed();
                stage_timings.ordered_write += push_stats.write_elapsed;
                stage_timings.merge += reorder_elapsed.saturating_sub(push_stats.write_elapsed);
            }
        }
        Err(error) => {
            first_error.get_or_insert(error);
        }
    }
    Ok(())
}

pub fn join_decode_workers(
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

pub fn decode_block_payload(
    header: BlockHeader,
    block_data: Vec<u8>,
    dictionary: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let compression_meta = header.compression_meta()?;
    let decoded = if compression_meta.raw_passthrough {
        block_data
    } else {
        crate::compression::reverse_compression_request(crate::compression::DecompressionRequest {
            data: &block_data,
            algo: compression_meta.algo,
            dictionary,
        })?
    };
    let strategy = header.strategy()?;
    let restored = crate::preprocessing::reverse_preprocessing(&decoded, &strategy)?;
    if restored.len() != header.raw_len as usize {
        return Err(crate::OxideError::InvalidFormat(
            "decoded block size mismatch",
        ));
    }
    Ok(restored)
}
