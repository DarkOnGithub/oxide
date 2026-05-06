use std::collections::BTreeMap;
use std::fs;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};

use crate::buffer::{BufferPool, PooledBuffer};
use crate::compression::CompressionScratchArena;
use crate::core::WorkerRuntimeSnapshot;
use crate::dictionary::ArchiveDictionaryBank;
use crate::format::{ArchiveReader, ChunkDescriptor, GlobalHeader};
use crate::pipeline::types::PipelinePerformanceOptions;
use crate::telemetry::{ReportValue, RunTelemetryOptions, TelemetrySink};
use crate::types::Result;

use super::super::directory;
use super::super::types::ArchiveSourceKind;
use super::reorder_writer::{
    BoundedReorderWriter, OrderedChunkWriter, OwnedChunk, ReorderWriterStats,
};
use super::telemetry::*;
use super::types::*;

mod decode_plan;
mod decode_support;
mod directory_restore;
mod file_writer;
mod read_backend;
#[cfg(test)]
mod tests;
mod tuning;

use self::decode_plan::DecodePlan;
use self::decode_support::{
    DecodeResultContext, DecodeStreamOutcome, DecodedBlock, OrderedWriteTask, OrderedWriterOutcome,
    ProcessedDecodeResult, abort_ordered_writer, decode_block_payload_with_scratch, forward_processed_decode_result,
    join_decode_workers, join_io_readers, join_ordered_write_forwarder, join_ordered_writer,
    process_decode_result, read_exact_file_at, receive_decode_result,
    spawn_ordered_write_forwarder, spawn_ordered_writer,
};
use self::directory_restore::{
    DirectDirectoryRestoreWriter, DirectoryExtractSelection, DirectoryRestoreStats, DirectoryRestoreWriter,
    FilteredDirectoryRestoreWriter, OUTPUT_BUFFER_CAPACITY, apply_entry_metadata,
};
use self::file_writer::{FileChunkWriter, VecChunkWriter, apply_file_restore_stats};
use self::read_backend::{
    DecodeReadBackend, ParallelFileDecodeReadBackend, ReadRequest, SequentialDecodeReadBackend,
};
#[cfg(test)]
use self::tuning::ordered_write_headroom_share_numerator;
use self::tuning::{
    ExtractBlockProfile, apply_directory_restore_stats, decode_queue_capacity,
    extract_inflight_block_limit, ordered_write_queue_capacity, reorder_pending_limit,
    select_extract_buffer_pool,
};

const DECODE_QUEUE_MULTIPLIER: usize = 4;
const ORDERED_WRITE_QUEUE_MULTIPLIER: usize = 8;
const ORDERED_WRITE_HEADROOM_SHARE_DENOMINATOR: usize = 4;
const REORDER_PENDING_MULTIPLIER: usize = 8;
const MIN_DECODE_QUEUE_CAPACITY: usize = 8;
const MIN_ORDERED_WRITE_QUEUE_CAPACITY: usize = 16;
const RESULT_DRAIN_BUDGET: usize = 64;

pub(crate) struct DirectoryRestoreOutcome {
    pub(crate) decoded: DecodedArchivePayload,
    pub(crate) output_bytes_total: u64,
    pub(crate) entry_count: u64,
}

pub struct Extractor {
    pub num_workers: usize,
    buffer_pool: Arc<BufferPool>,
    performance: PipelinePerformanceOptions,
}

impl Extractor {
    pub fn new(
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        performance: PipelinePerformanceOptions,
    ) -> Self {
        Self {
            num_workers,
            buffer_pool,
            performance,
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
        let (decoded, writer) = self.decode_archive_to_writer_with_backend(
            SequentialDecodeReadBackend::new(archive),
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
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

    pub fn read_archive_payload_from_file_with_metrics(
        &self,
        file: fs::File,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new(file.try_clone()?)?;
        let archive_read_elapsed = archive_started.elapsed();
        let writer = VecChunkWriter::default();
        let backend = ParallelFileDecodeReadBackend::new(archive, file)?;
        let (decoded, writer) = self.decode_archive_to_writer_with_backend(
            backend,
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
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
        let (mut decoded, mut writer) = self.decode_archive_to_writer_with_backend(
            SequentialDecodeReadBackend::new(archive),
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
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

    pub fn extract_file_to_path_from_file_with_metrics(
        &self,
        file: fs::File,
        output_path: &Path,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DecodedArchivePayload> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new(file.try_clone()?)?;
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
        let backend = ParallelFileDecodeReadBackend::new(archive, file)?;
        let (mut decoded, mut writer) = self.decode_archive_to_writer_with_backend(
            backend,
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
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
        let writer = DirectoryRestoreWriter::create_with_performance(
            output_path,
            manifest,
            &self.performance,
        )?;
        let (decoded, mut writer) = self.decode_archive_to_writer_with_backend(
            SequentialDecodeReadBackend::new(archive),
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
            None,
            writer,
        )?;

        let restore_stats = writer.finish()?;
        let DecodeStreamOutcome {
            flags,
            decoded_bytes_total,
            archive_bytes_total,
            blocks_total,
            workers,
            mut stage_timings,
            mut pipeline_stats,
            ..
        } = decoded;
        apply_directory_restore_stats(&mut pipeline_stats, &mut stage_timings, &restore_stats);

        Ok(DirectoryRestoreOutcome {
            decoded: DecodedArchivePayload {
                flags,
                payload: Vec::new(),
                decoded_bytes_total,
                archive_bytes_total,
                blocks_total,
                workers,
                stage_timings,
                pipeline_stats,
            },
            output_bytes_total: restore_stats.output_bytes_total,
            entry_count: restore_stats.entry_count,
        })
    }

    pub(crate) fn extract_directory_to_path_from_file_with_metrics(
        &self,
        file: fs::File,
        output_path: &Path,
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DirectoryRestoreOutcome> {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new(file.try_clone()?)?;
        let archive_read_elapsed = archive_started.elapsed();
        if archive.source_kind() != ArchiveSourceKind::Directory {
            return Err(crate::OxideError::InvalidFormat(
                "archive is not a directory payload",
            ));
        }

        let manifest = archive.manifest().clone();
        let file_count = manifest
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, crate::ArchiveEntryKind::File))
            .count();
        if file_count < 1024 {
            let writer = DirectoryRestoreWriter::create_with_performance(
                output_path,
                manifest,
                &self.performance,
            )?;
            let backend = ParallelFileDecodeReadBackend::new(archive, file)?;
            let (decoded, mut writer) = self.decode_archive_to_writer_with_backend(
                backend,
                archive_read_elapsed,
                ExtractContext {
                    started_at,
                    options,
                    sink,
                },
                None,
                writer,
            )?;

            let restore_stats = writer.finish()?;
            let DecodeStreamOutcome {
                flags,
                decoded_bytes_total,
                archive_bytes_total,
                blocks_total,
                workers,
                mut stage_timings,
                mut pipeline_stats,
                ..
            } = decoded;
            apply_directory_restore_stats(&mut pipeline_stats, &mut stage_timings, &restore_stats);

            return Ok(DirectoryRestoreOutcome {
                decoded: DecodedArchivePayload {
                    flags,
                    payload: Vec::new(),
                    decoded_bytes_total,
                    archive_bytes_total,
                    blocks_total,
                    workers,
                    stage_timings,
                    pipeline_stats,
                },
                output_bytes_total: restore_stats.output_bytes_total,
                entry_count: restore_stats.entry_count,
            });
        }
        let block_descriptors = archive.block_descriptors().to_vec();
        let writer = DirectDirectoryRestoreWriter::create(
            output_path,
            manifest,
            &block_descriptors,
            self.performance.extract_write_shards.max(1),
            self.performance.extract_preserve_metadata,
        )?;
        let backend = ParallelFileDecodeReadBackend::new(archive, file)?;
        let (decoded, writer) = self.decode_archive_to_direct_directory_with_backend(
            backend,
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
            writer,
        )?;

        let restore_stats = writer.finish()?;
        let DecodeStreamOutcome {
            flags,
            decoded_bytes_total,
            archive_bytes_total,
            blocks_total,
            workers,
            mut stage_timings,
            mut pipeline_stats,
            ..
        } = decoded;
        apply_directory_restore_stats(&mut pipeline_stats, &mut stage_timings, &restore_stats);

        Ok(DirectoryRestoreOutcome {
            decoded: DecodedArchivePayload {
                flags,
                payload: Vec::new(),
                decoded_bytes_total,
                archive_bytes_total,
                blocks_total,
                workers,
                stage_timings,
                pipeline_stats,
            },
            output_bytes_total: restore_stats.output_bytes_total,
            entry_count: restore_stats.entry_count,
        })
    }
}

struct ExtractContext<'a> {
    started_at: Instant,
    options: &'a RunTelemetryOptions,
    sink: &'a mut dyn TelemetrySink,
}

impl Extractor {
    #[allow(clippy::too_many_arguments)]
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
        let writer = FilteredDirectoryRestoreWriter::create_with_performance(
            output_path,
            manifest,
            projected_ranges,
            &self.performance,
        )?;
        let (decoded, mut writer) = self.decode_archive_to_writer_with_backend(
            SequentialDecodeReadBackend::new(archive),
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
            Some(&selected_ranges),
            writer,
        )?;

        let restore_stats = writer.finish()?;
        let DecodeStreamOutcome {
            flags,
            decoded_bytes_total,
            archive_bytes_total,
            blocks_total,
            workers,
            mut stage_timings,
            mut pipeline_stats,
            ..
        } = decoded;
        apply_directory_restore_stats(&mut pipeline_stats, &mut stage_timings, &restore_stats);

        Ok(DirectoryRestoreOutcome {
            decoded: DecodedArchivePayload {
                flags,
                payload: Vec::new(),
                decoded_bytes_total,
                archive_bytes_total,
                blocks_total,
                workers,
                stage_timings,
                pipeline_stats,
            },
            output_bytes_total: restore_stats.output_bytes_total,
            entry_count: restore_stats.entry_count,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn extract_directory_to_path_filtered_from_file_with_metrics<S, T>(
        &self,
        file: fs::File,
        output_path: &Path,
        filters: &[S],
        regex_filters: &[T],
        started_at: Instant,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<DirectoryRestoreOutcome>
    where
        S: AsRef<str>,
        T: AsRef<str>,
    {
        let archive_started = Instant::now();
        let archive = ArchiveReader::new(file.try_clone()?)?;
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
        let writer = FilteredDirectoryRestoreWriter::create_with_performance(
            output_path,
            manifest,
            projected_ranges,
            &self.performance,
        )?;
        let backend = ParallelFileDecodeReadBackend::new(archive, file)?;
        let (decoded, mut writer) = self.decode_archive_to_writer_with_backend(
            backend,
            archive_read_elapsed,
            ExtractContext {
                started_at,
                options,
                sink,
            },
            Some(&selected_ranges),
            writer,
        )?;

        let restore_stats = writer.finish()?;
        let DecodeStreamOutcome {
            flags,
            decoded_bytes_total,
            archive_bytes_total,
            blocks_total,
            workers,
            mut stage_timings,
            mut pipeline_stats,
            ..
        } = decoded;
        apply_directory_restore_stats(&mut pipeline_stats, &mut stage_timings, &restore_stats);

        Ok(DirectoryRestoreOutcome {
            decoded: DecodedArchivePayload {
                flags,
                payload: Vec::new(),
                decoded_bytes_total,
                archive_bytes_total,
                blocks_total,
                workers,
                stage_timings,
                pipeline_stats,
            },
            output_bytes_total: restore_stats.output_bytes_total,
            entry_count: restore_stats.entry_count,
        })
    }

    fn decode_archive_to_direct_directory_with_backend<B>(
        &self,
        backend: B,
        archive_read_elapsed: Duration,
        ctx: ExtractContext<'_>,
        mut writer: DirectDirectoryRestoreWriter,
    ) -> Result<(DecodeStreamOutcome, DirectDirectoryRestoreWriter)>
    where
        B: DecodeReadBackend,
    {
        let mut stage_timings = ExtractStageTimings::default();
        stage_timings.archive_read += archive_read_elapsed;
        let source_kind = backend.source_kind();
        let flags = directory::source_kind_flags(source_kind);
        let block_descriptors = backend.block_descriptors().to_vec();
        let archive_header = backend.global_header();
        let decode_plan = DecodePlan::all(block_descriptors.len());
        let block_capacity = decode_plan.block_count();
        let block_profile = ExtractBlockProfile::from_plan(&block_descriptors, &decode_plan);
        let worker_count = self.num_workers.max(1);
        let inflight_block_limit = extract_inflight_block_limit(
            block_capacity,
            block_profile.budgeted_queue_block_bytes(),
            &self.performance,
        );
        let queue_capacity =
            decode_queue_capacity(worker_count, block_capacity, inflight_block_limit);
        let reader_buffer_pool =
            select_extract_buffer_pool(&self.buffer_pool, block_profile.max_encoded_len);
        let decode_buffer_pool =
            select_extract_buffer_pool(&self.buffer_pool, block_profile.max_raw_len);

        let (read_request_tx, read_request_rx) = bounded::<ReadRequest>(queue_capacity);
        let (task_tx, task_rx) = bounded::<DecodeTask>(queue_capacity);
        let (result_tx, result_rx) =
            bounded::<(usize, Duration, Result<DecodedBlock>)>(queue_capacity);
        let runtime_state = Arc::new(DecodeRuntimeState::new(worker_count, ctx.started_at));
        let dictionary_bank = Arc::new(backend.dictionary_bank());
        let reader_handles = backend.spawn(
            read_request_rx,
            task_tx.clone(),
            result_tx.clone(),
            reader_buffer_pool,
            worker_count,
        );
        let mut worker_handles = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let local_task_rx = task_rx.clone();
            let local_result_tx = result_tx.clone();
            let local_runtime = Arc::clone(&runtime_state);
            let local_buffer_pool = Arc::clone(&decode_buffer_pool);
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
        let emit_every = ctx
            .options
            .progress_interval
            .max(Duration::from_millis(100));
        let mut decode_task_queue_peak = 0usize;
        let mut decode_result_queue_peak = 0usize;

        let run_result = (|| -> Result<()> {
            for (block_index, header) in block_descriptors.iter().copied().enumerate() {
                archive_bytes_completed =
                    archive_bytes_completed.saturating_add(header.encoded_len as u64);

                while submitted.saturating_sub(received) >= queue_capacity {
                    let mut decode_ctx = DecodeResultContext {
                        stage_timings: &mut stage_timings,
                        total_blocks: block_capacity,
                        received_indices: &mut received_indices,
                        runtime_state: &runtime_state,
                        decoded_bytes_completed: &mut decoded_bytes_completed,
                        received: &mut received,
                        first_error: &mut first_error,
                    };
                    let outcome = receive_decode_result(&result_rx, &mut decode_ctx)?;
                    dispatch_direct_decode_result(outcome, &mut writer)?;
                    decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                }

                let submit_started = Instant::now();
                read_request_tx
                    .send(ReadRequest {
                        index: submitted,
                        block_index: block_index as u32,
                        encoded_len: header.encoded_len as usize,
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
                    dispatch_direct_decode_result(outcome, &mut writer)?;
                    drained += 1;
                }
                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());

                let force = false;
                if progress_emit_due(&last_emit_at, emit_every, force) {
                    emit_extract_progress_if_due(ExtractProgressIfDueCtx {
                        source_kind,
                        started_at: ctx.started_at,
                        archive_bytes_completed,
                        decoded_bytes_completed,
                        blocks_total: block_capacity as u32,
                        blocks_completed: received as u32,
                        runtime: runtime_state.snapshot(),
                        emit_every,
                        last_emit_at: &mut last_emit_at,
                        force,
                        sink: ctx.sink,
                    });
                }
            }

            if submitted != block_capacity && first_error.is_none() {
                return Err(crate::OxideError::InvalidFormat(
                    "archive block count mismatch during decode",
                ));
            }

            while received < submitted {
                let mut decode_ctx = DecodeResultContext {
                    stage_timings: &mut stage_timings,
                    total_blocks: block_capacity,
                    received_indices: &mut received_indices,
                    runtime_state: &runtime_state,
                    decoded_bytes_completed: &mut decoded_bytes_completed,
                    received: &mut received,
                    first_error: &mut first_error,
                };
                let outcome = receive_decode_result(&result_rx, &mut decode_ctx)?;
                dispatch_direct_decode_result(outcome, &mut writer)?;
                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
            }

            Ok(())
        })();

        drop(read_request_tx);
        drop(task_tx);
        drop(result_rx);

        let io_reader_result = join_io_readers(reader_handles);
        let workers_result = join_decode_workers(worker_handles);

        if let Some(error) = first_error {
            return Err(error);
        }
        io_reader_result?;
        if let Err(error) = run_result {
            return Err(error);
        }
        let workers = workers_result?;

        if ctx.options.emit_final_progress {
            let force = true;
            if progress_emit_due(&last_emit_at, emit_every, force) {
                emit_extract_progress(
                    source_kind,
                    ctx.started_at,
                    archive_bytes_completed,
                    decoded_bytes_completed,
                    block_capacity as u32,
                    received as u32,
                    runtime_state.snapshot(),
                    ctx.sink,
                );
            }
        }

        let pipeline_stats = ExtractPipelineStats {
            decode_task_queue_capacity: queue_capacity,
            decode_task_queue_peak,
            decode_result_queue_capacity: queue_capacity,
            decode_result_queue_peak,
            ..ExtractPipelineStats::default()
        };

        Ok((
            DecodeStreamOutcome {
                flags,
                decoded_bytes_total: decoded_bytes_completed,
                archive_bytes_total,
                blocks_total: submitted as u32,
                workers,
                stage_timings,
                pipeline_stats,
            },
            writer,
        ))
    }

    fn decode_archive_to_writer_with_backend<W, B>(
        &self,
        backend: B,
        archive_read_elapsed: Duration,
        ctx: ExtractContext<'_>,
        selected_ranges: Option<&[Range<u64>]>,
        writer: W,
    ) -> Result<(DecodeStreamOutcome, W)>
    where
        W: OrderedChunkWriter + Send + 'static,
        B: DecodeReadBackend,
    {
        let mut stage_timings = ExtractStageTimings::default();
        stage_timings.archive_read += archive_read_elapsed;
        let source_kind = backend.source_kind();
        let flags = directory::source_kind_flags(source_kind);
        let block_descriptors = backend.block_descriptors().to_vec();
        let archive_header = backend.global_header();
        let decode_plan = match selected_ranges {
            Some(ranges) => DecodePlan::from_ranges(&block_descriptors, ranges),
            None => DecodePlan::all(block_descriptors.len()),
        };
        let block_capacity = decode_plan.block_count();
        let block_profile = ExtractBlockProfile::from_plan(&block_descriptors, &decode_plan);
        let worker_count = self.num_workers.max(1);
        let inflight_block_limit = extract_inflight_block_limit(
            block_capacity,
            block_profile.budgeted_queue_block_bytes(),
            &self.performance,
        );
        let queue_capacity =
            decode_queue_capacity(worker_count, block_capacity, inflight_block_limit);
        let ordered_write_queue_capacity = ordered_write_queue_capacity(
            worker_count,
            queue_capacity,
            block_capacity,
            inflight_block_limit,
            block_profile,
        );
        let reorder_pending_limit = reorder_pending_limit(
            ordered_write_queue_capacity,
            block_capacity,
            inflight_block_limit,
        );
        let reader_buffer_pool =
            select_extract_buffer_pool(&self.buffer_pool, block_profile.max_encoded_len);
        let decode_buffer_pool =
            select_extract_buffer_pool(&self.buffer_pool, block_profile.max_raw_len);

        let (read_request_tx, read_request_rx) = bounded::<ReadRequest>(queue_capacity);
        let (task_tx, task_rx) = bounded::<DecodeTask>(queue_capacity);
        let (result_tx, result_rx) =
            bounded::<(usize, Duration, Result<DecodedBlock>)>(queue_capacity);
        let (ordered_write_tx, ordered_write_rx) =
            bounded::<OrderedWriteTask>(ordered_write_queue_capacity);
        let (ordered_write_forward_tx, ordered_write_forward_rx) =
            bounded::<OrderedWriteTask>(queue_capacity);
        let runtime_state = Arc::new(DecodeRuntimeState::new(worker_count, ctx.started_at));
        let mut worker_handles = Vec::with_capacity(worker_count);
        let ordered_writer_handle = spawn_ordered_writer(
            writer,
            ordered_write_rx,
            reorder_pending_limit,
            block_capacity,
        );
        let ordered_write_forwarder_handle =
            spawn_ordered_write_forwarder(ordered_write_forward_rx, ordered_write_tx);
        let dictionary_bank = Arc::new(backend.dictionary_bank());
        let reader_handles = backend.spawn(
            read_request_rx,
            task_tx.clone(),
            result_tx.clone(),
            reader_buffer_pool,
            worker_count,
        );

        for worker_id in 0..worker_count {
            let local_task_rx = task_rx.clone();
            let local_result_tx = result_tx.clone();
            let local_runtime = Arc::clone(&runtime_state);
            let local_buffer_pool = Arc::clone(&decode_buffer_pool);
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
        let emit_every = ctx
            .options
            .progress_interval
            .max(Duration::from_millis(100));
        let mut decode_task_queue_peak = 0usize;
        let mut decode_result_queue_peak = 0usize;
        let mut ordered_write_tx = Some(ordered_write_forward_tx);

        let run_result = (|| -> Result<()> {
            for (block_index, header) in block_descriptors.iter().copied().enumerate() {
                archive_bytes_completed =
                    archive_bytes_completed.saturating_add(header.encoded_len as u64);

                if !decode_plan.includes(block_index) {
                    let force = false;
                    if progress_emit_due(&last_emit_at, emit_every, force) {
                        emit_extract_progress_if_due(ExtractProgressIfDueCtx {
                            source_kind,
                            started_at: ctx.started_at,
                            archive_bytes_completed,
                            decoded_bytes_completed,
                            blocks_total: block_capacity as u32,
                            blocks_completed: received as u32,
                            runtime: runtime_state.snapshot(),
                            emit_every,
                            last_emit_at: &mut last_emit_at,
                            force,
                            sink: ctx.sink,
                        });
                    }
                    continue;
                }

                while submitted.saturating_sub(received) >= queue_capacity {
                    let mut decode_ctx = DecodeResultContext {
                        stage_timings: &mut stage_timings,
                        total_blocks: block_capacity,
                        received_indices: &mut received_indices,
                        runtime_state: &runtime_state,
                        decoded_bytes_completed: &mut decoded_bytes_completed,
                        received: &mut received,
                        first_error: &mut first_error,
                    };
                    receive_decode_result(&result_rx, &mut decode_ctx).and_then(|outcome| {
                        forward_processed_decode_result(outcome, &mut ordered_write_tx)
                    })?;
                    decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                    let force = false;
                    if progress_emit_due(&last_emit_at, emit_every, force) {
                        emit_extract_progress_if_due(ExtractProgressIfDueCtx {
                            source_kind,
                            started_at: ctx.started_at,
                            archive_bytes_completed,
                            decoded_bytes_completed,
                            blocks_total: block_capacity as u32,
                            blocks_completed: received as u32,
                            runtime: runtime_state.snapshot(),
                            emit_every,
                            last_emit_at: &mut last_emit_at,
                            force,
                            sink: ctx.sink,
                        });
                    }
                }

                let submit_started = Instant::now();
                read_request_tx
                    .send(ReadRequest {
                        index: submitted,
                        block_index: block_index as u32,
                        encoded_len: header.encoded_len as usize,
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
                    forward_processed_decode_result(outcome, &mut ordered_write_tx)?;
                    drained += 1;
                }

                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                let force = false;
                if progress_emit_due(&last_emit_at, emit_every, force) {
                    emit_extract_progress_if_due(ExtractProgressIfDueCtx {
                        source_kind,
                        started_at: ctx.started_at,
                        archive_bytes_completed,
                        decoded_bytes_completed,
                        blocks_total: block_capacity as u32,
                        blocks_completed: received as u32,
                        runtime: runtime_state.snapshot(),
                        emit_every,
                        last_emit_at: &mut last_emit_at,
                        force,
                        sink: ctx.sink,
                    });
                }
            }

            if submitted != block_capacity && first_error.is_none() {
                return Err(crate::OxideError::InvalidFormat(
                    "archive block count mismatch during decode",
                ));
            }

            while received < submitted {
                let mut decode_ctx = DecodeResultContext {
                    stage_timings: &mut stage_timings,
                    total_blocks: block_capacity,
                    received_indices: &mut received_indices,
                    runtime_state: &runtime_state,
                    decoded_bytes_completed: &mut decoded_bytes_completed,
                    received: &mut received,
                    first_error: &mut first_error,
                };
                receive_decode_result(&result_rx, &mut decode_ctx).and_then(|outcome| {
                    forward_processed_decode_result(outcome, &mut ordered_write_tx)
                })?;
                decode_result_queue_peak = decode_result_queue_peak.max(result_rx.len());
                let force = false;
                if progress_emit_due(&last_emit_at, emit_every, force) {
                    emit_extract_progress_if_due(ExtractProgressIfDueCtx {
                        source_kind,
                        started_at: ctx.started_at,
                        archive_bytes_completed,
                        decoded_bytes_completed,
                        blocks_total: block_capacity as u32,
                        blocks_completed: received as u32,
                        runtime: runtime_state.snapshot(),
                        emit_every,
                        last_emit_at: &mut last_emit_at,
                        force,
                        sink: ctx.sink,
                    });
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

        let io_reader_result = join_io_readers(reader_handles);
        let workers_result = join_decode_workers(worker_handles);
        let ordered_write_forwarder_result =
            join_ordered_write_forwarder(ordered_write_forwarder_handle);
        let ordered_writer_result = join_ordered_writer(ordered_writer_handle);

        if let Some(error) = first_error {
            return Err(error);
        }
        io_reader_result?;
        if let Err(error) = run_result {
            if let Err(writer_error) = ordered_write_forwarder_result {
                return Err(writer_error);
            }
            if let Err(writer_error) = ordered_writer_result {
                return Err(writer_error);
            }
            return Err(error);
        }
        let workers = workers_result?;
        let ordered_write_forwarder_stats = ordered_write_forwarder_result?;

        let OrderedWriterOutcome {
            writer,
            stats: reorder_stats,
        } = ordered_writer_result?;

        stage_timings.writer_enqueue_blocked += ordered_write_forwarder_stats.blocked_elapsed;
        stage_timings.record_write_shard_blocked(0, ordered_write_forwarder_stats.blocked_elapsed);
        stage_timings.ordered_write += reorder_stats.write_elapsed;
        stage_timings.merge += reorder_stats
            .push_elapsed
            .saturating_sub(reorder_stats.write_elapsed);
        if ctx.options.emit_final_progress {
            let force = true;
            if progress_emit_due(&last_emit_at, emit_every, force) {
                emit_extract_progress(
                    source_kind,
                    ctx.started_at,
                    archive_bytes_completed,
                    decoded_bytes_completed,
                    block_capacity as u32,
                    received as u32,
                    runtime_state.snapshot(),
                    ctx.sink,
                );
            }
        }

        let mut pipeline_stats = ExtractPipelineStats {
            decode_task_queue_capacity: queue_capacity,
            decode_task_queue_peak,
            decode_result_queue_capacity: queue_capacity,
            decode_result_queue_peak,
            ordered_write_queue_capacity,
            ordered_write_queue_peak: ordered_write_forwarder_stats.ordered_write_queue_peak,
            reorder_pending_limit,
            reorder_pending_peak: reorder_stats.pending_blocks_peak,
            reorder_pending_bytes_peak: reorder_stats.pending_bytes_peak,
            ..ExtractPipelineStats::default()
        };
        // Single logical output sink (file writer or shard-0 metrics); do not copy
        // `ordered_write_queue_peak` here — that belongs in `ordered_write_queue_peak` only.
        pipeline_stats.set_write_shard_count(1);

        Ok((
            DecodeStreamOutcome {
                flags,
                decoded_bytes_total: decoded_bytes_completed,
                archive_bytes_total,
                blocks_total: submitted as u32,
                workers,
                stage_timings,
                pipeline_stats,
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

pub use self::decode_support::decode_block_payload;

fn dispatch_direct_decode_result(
    outcome: ProcessedDecodeResult,
    writer: &mut DirectDirectoryRestoreWriter,
) -> Result<()> {
    match outcome {
        ProcessedDecodeResult::Block { index, bytes } => {
            writer.write_decoded_block(index, OwnedChunk::from(bytes))
        }
        ProcessedDecodeResult::NewError | ProcessedDecodeResult::IgnoredAfterError => Ok(()),
    }
}
