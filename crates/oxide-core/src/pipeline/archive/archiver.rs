use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_channel::{TryRecvError, bounded, RecvTimeoutError};

use crate::buffer::BufferPool;
use crate::core::{WorkerPool, WorkerPoolHandle};
use crate::format::{
    ArchiveWriter, FOOTER_SIZE, GLOBAL_HEADER_SIZE, CORE_SECTION_COUNT, SECTION_TABLE_ENTRY_SIZE,
    CHUNK_DESCRIPTOR_SIZE,
};
use crate::io::{InputScanner, MmapInput};
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use crate::types::{
    Batch, CompressedBlock, CompressionAlgo, CompressionMeta, CompressionPreset, FileFormat,
    PreProcessingStrategy, Result,
};
use super::directory::{self, DirectoryBatchSubmitter};
use super::types::*;
use super::telemetry::*;
use super::super::types::{ArchivePipelineConfig, ArchiveSourceKind, PipelinePerformanceOptions};

pub const SUBMISSION_DRAIN_BUDGET: usize = 128;
pub const DIRECTORY_FORMAT_PROBE_LIMIT: usize = 64 * 1024;
pub const DIRECTORY_PREFETCH_WINDOW: usize = 8;
pub const MIN_INFLIGHT_BLOCKS: usize = 64;
pub const MAX_INFLIGHT_BLOCKS: usize = 4096;
pub const OXZ_SECTION_TABLE_BYTES: u64 = CORE_SECTION_COUNT as u64 * SECTION_TABLE_ENTRY_SIZE as u64;
pub const AUTOTUNE_CANDIDATE_BLOCK_SIZES: [usize; 5] = [
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    4 * 1024 * 1024,
];

#[inline]
pub fn container_prefix_bytes(block_count: u32) -> u64 {
    GLOBAL_HEADER_SIZE as u64
        + OXZ_SECTION_TABLE_BYTES
        + block_count as u64 * CHUNK_DESCRIPTOR_SIZE as u64
}

pub struct Archiver<'a> {
    pub config: &'a ArchivePipelineConfig,
}

impl<'a> Archiver<'a> {
    pub fn new(config: &'a ArchivePipelineConfig) -> Self {
        Self { config }
    }

    pub fn archive_file_path_with<W: Write>(
        &self,
        path: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>> {
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

    pub fn archive_directory_streaming_with<W: Write + Send + 'static>(
        &self,
        root: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>> {
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
            self.config.performance.preserve_directory_format_boundaries,
        )?;
        let input_bytes_total = discovery.input_bytes_total;
        let total_blocks = usize::try_from(block_count)
            .map_err(|_| crate::OxideError::InvalidFormat("block count exceeds usize range"))?;
        let max_inflight_blocks = max_inflight_blocks(
            total_blocks,
            self.config.workers,
            block_size.selected_block_size,
            &self.config.performance,
        );
        let max_inflight_bytes = max_inflight_blocks.saturating_mul(block_size.selected_block_size);

        let worker_pool = WorkerPool::new(
            self.config.workers,
            Arc::clone(&self.config.buffer_pool),
            self.config.compression_algo,
        );
        let processing_totals = Arc::new(ProcessingThroughputTotals::default());
        let compression_preset = self.config.performance.compression_preset;
        let raw_fallback_enabled = self.config.performance.raw_fallback_enabled;
        let worker_processing_totals = Arc::clone(&processing_totals);
        let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression| {
            process_batch(
                batch,
                pool,
                compression,
                compression_preset,
                raw_fallback_enabled,
                worker_processing_totals.as_ref(),
            )
        });

        let writer_queue_capacity = self
            .config
            .performance
            .writer_result_queue_blocks
            .max(1)
            .min(max_inflight_blocks.max(1));
        let (writer_tx, writer_rx) = bounded::<CompressedBlock>(writer_queue_capacity);
        let writer_output_bytes = Arc::new(AtomicU64::new(container_prefix_bytes(block_count)));
        let writer_output_bytes_shared = Arc::clone(&writer_output_bytes);
        let writer_buffer_pool = Arc::clone(&self.config.buffer_pool);
        let writer_handle = thread::spawn(move || -> Result<DirectoryWriterOutcome<W>> {
            let mut archive_writer = ArchiveWriter::new(writer, writer_buffer_pool);
            archive_writer.write_global_header_with_flags(
                block_count,
                directory::source_kind_flags(ArchiveSourceKind::Directory),
            )?;

            let mut output_bytes_written = container_prefix_bytes(block_count);
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
                    output_bytes_written = output_bytes_written.saturating_add(len as u64);
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
        let mut output_bytes_written = container_prefix_bytes(block_count);
        let mut raw_passthrough_blocks = 0u64;
        let mut writer_queue_peak = 0usize;
        let result_wait_timeout = self
            .config
            .performance
            .result_wait_timeout
            .max(Duration::from_millis(1));

        let (batch_tx, batch_rx) = bounded::<Batch>(max_inflight_blocks.max(1));
        let producer_root = discovery.root.clone();
        let producer_directories = discovery.directories.clone();
        let producer_files = discovery.files.clone();
        let producer_file_formats = file_formats.clone();
        let producer_block_size = block_size.selected_block_size;
        let preserve_boundaries = self.config.performance.preserve_directory_format_boundaries;
        let stream_read_buffer_size = self.config.performance.directory_stream_read_buffer_size.max(1);
        let producer_threads = self.config.performance.producer_threads.clamp(1, 2);
        let mmap_threshold = self.config.performance.directory_mmap_threshold_bytes.max(1);

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
                record_result_to_writer_queue(
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
                    emit_archive_progress_if_due(
                        handle.runtime_snapshot(),
                        processing_totals.snapshot(),
                        ArchiveSourceKind::Directory,
                        started_at,
                        input_bytes_total,
                        completed_bytes,
                        output_bytes_written,
                        block_count,
                        emit_every,
                        &mut last_emit_at,
                        true,
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
                    record_result_to_writer_queue(
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
            emit_archive_progress_if_due(
                handle.runtime_snapshot(),
                processing_totals.snapshot(),
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                false,
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
                record_result_to_writer_queue(
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
            emit_archive_progress_if_due(
                handle.runtime_snapshot(),
                processing_totals.snapshot(),
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                options.emit_final_progress && received_count == submitted_count,
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
        let extensions = build_stats_extensions(
            input_bytes_total,
            output_bytes_written,
            &final_runtime,
            block_size,
            raw_passthrough_blocks,
            self.config.performance.compression_preset,
            max_inflight_blocks,
            max_inflight_bytes,
            pending_write_peak,
            writer_queue_peak,
            stage_timings,
            processing_snapshot,
        );

        let report = build_archive_report(
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
        record_archive_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
        Ok(ArchiveRun {
            writer: writer_outcome.writer,
            report,
        })
    }

    pub fn archive_prepared_with<W: Write>(
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
        let max_inflight_blocks = max_inflight_blocks(
            total_blocks,
            self.config.workers,
            block_size.selected_block_size,
            &self.config.performance,
        );
        let max_inflight_bytes = max_inflight_blocks.saturating_mul(block_size.selected_block_size);

        let worker_pool = WorkerPool::new(
            self.config.workers,
            Arc::clone(&self.config.buffer_pool),
            self.config.compression_algo,
        );
        let processing_totals = Arc::new(ProcessingThroughputTotals::default());

        let compression_preset = self.config.performance.compression_preset;
        let raw_fallback_enabled = self.config.performance.raw_fallback_enabled;
        let worker_processing_totals = Arc::clone(&processing_totals);
        let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression| {
            process_batch(
                batch,
                pool,
                compression,
                compression_preset,
                raw_fallback_enabled,
                worker_processing_totals.as_ref(),
            )
        });

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.config.buffer_pool));
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(source_kind),
        )?;
        let mut output_bytes_written = container_prefix_bytes(block_count);
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
            .config
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

            let drained = drain_results_to_writer(
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
                    emit_archive_progress_if_due(
                        handle.runtime_snapshot(),
                        processing_totals.snapshot(),
                        source_kind,
                        started_at,
                        input_bytes_total,
                        completed_bytes,
                        output_bytes_written,
                        block_count,
                        emit_every,
                        &mut last_emit_at,
                        true,
                        sink,
                    );
                }
                break;
            }

            if drained == 0 {
                let wait_started = Instant::now();
                recv_result_to_writer(
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
            emit_archive_progress_if_due(
                handle.runtime_snapshot(),
                processing_totals.snapshot(),
                source_kind,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                false,
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
        let extensions = build_stats_extensions(
            input_bytes_total,
            output_bytes_written,
            &final_runtime,
            block_size,
            raw_passthrough_blocks,
            self.config.performance.compression_preset,
            max_inflight_blocks,
            max_inflight_bytes,
            pending_write_peak,
            0,
            stage_timings,
            processing_snapshot,
        );
        let report = build_archive_report(
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
        record_archive_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
        Ok(ArchiveRun { writer, report })
    }

    pub fn choose_block_size_for_file(
        &self,
        path: &Path,
        input_bytes: u64,
    ) -> Result<BlockSizeDecision> {
        if !self.config.performance.autotune_enabled
            || input_bytes < self.config.performance.autotune_min_input_bytes
        {
            return Ok(BlockSizeDecision {
                selected_block_size: self.config.target_block_size,
                autotune_requested: self.config.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let sample = collect_file_sample(path, self.config.performance.autotune_sample_bytes)?;
        self.pick_tuned_block_size(&sample)
    }

    pub fn choose_block_size_for_directory(
        &self,
        discovery: &directory::DirectoryDiscovery,
    ) -> Result<BlockSizeDecision> {
        if !self.config.performance.autotune_enabled
            || discovery.input_bytes_total < self.config.performance.autotune_min_input_bytes
        {
            return Ok(BlockSizeDecision {
                selected_block_size: self.config.target_block_size,
                autotune_requested: self.config.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let sample =
            collect_directory_sample(discovery, self.config.performance.autotune_sample_bytes)?;
        self.pick_tuned_block_size(&sample)
    }

    pub fn pick_tuned_block_size(&self, sample: &[u8]) -> Result<BlockSizeDecision> {
        if sample.is_empty() {
            return Ok(BlockSizeDecision {
                selected_block_size: self.config.target_block_size,
                autotune_requested: self.config.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let mut candidates = AUTOTUNE_CANDIDATE_BLOCK_SIZES.to_vec();
        let default_block = self.config.target_block_size.max(1);
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
            autotune_requested: self.config.performance.autotune_enabled,
            autotune_ran: true,
            sampled_bytes: sample.len(),
        })
    }

    pub fn score_block_size(&self, sample: &[u8], block_size: usize) -> Result<BlockSizeScore> {
        let started = Instant::now();
        let mut output_bytes = 0usize;
        let chunk_size = block_size.max(1);
        for chunk in sample.chunks(chunk_size) {
            let compressed = crate::compression::apply_compression(chunk, self.config.compression_algo)?;
            let (stored_payload, _raw_passthrough) = select_stored_payload(
                chunk,
                compressed.as_slice(),
                self.config.performance.raw_fallback_enabled,
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

    pub fn prepare_file(&self, path: &Path, block_size: usize) -> Result<PreparedInput> {
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

pub fn record_result_to_writer_queue(
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

pub fn drain_results_to_writer<W: Write>(
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
            record_result_to_writer(
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

pub fn recv_result_to_writer<W: Write>(
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
        record_result_to_writer(
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

pub fn record_result_to_writer<W: Write>(
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
                *output_bytes_written =
                    (*output_bytes_written).saturating_add(ready.data.len() as u64);
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

pub fn process_batch(
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
    let (output, raw_passthrough) = select_stored_payload(
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

pub fn collect_file_sample(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
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

pub fn collect_directory_sample(
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
