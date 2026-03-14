use crossbeam_channel::{Receiver, TryRecvError, bounded, select};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::thread;
use std::time::{Duration, Instant};

use crate::buffer::BufferPool;
use crate::core::WorkerPool;
use crate::format::FormatDetector;
use crate::format::{ArchiveBlockWriter, ArchiveManifest, FOOTER_SIZE};
use crate::io::MmapInput;
use crate::pipeline::directory::{self, DirectoryBatchSubmitter};
use crate::pipeline::types::{ArchivePipelineConfig, ArchiveSourceKind};
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use crate::types::{Batch, ChunkEncodingPlan, CompressedBlock, FileFormat, Result};

use super::super::telemetry::*;
use super::super::types::*;
use super::processing::process_batch;
use super::utils::*;

pub fn archive_directory_streaming_with_writer<W, AW, F>(
    config: &ArchivePipelineConfig,
    root: &Path,
    writer: W,
    options: &RunTelemetryOptions,
    sink: &mut dyn TelemetrySink,
    writer_factory: F,
) -> Result<ArchiveRun<W>>
where
    W: Write + Send + 'static,
    AW: ArchiveBlockWriter<InnerWriter = W> + Send + 'static,
    F: FnOnce(W, Arc<BufferPool>, ArchiveManifest, usize) -> AW + Send + 'static,
{
    let mut stage_timings = StageTimings::default();
    let preserve_boundaries = config.performance.preserve_directory_format_boundaries;

    let discovery_started = Instant::now();
    let discovery = directory::discover_directory_tree(root)?;
    stage_timings.discovery += discovery_started.elapsed();

    let block_size = config.target_block_size;
    let file_formats = if preserve_boundaries {
        let format_probe_started = Instant::now();
        let formats = directory::detect_file_formats(
            &discovery,
            DIRECTORY_FORMAT_PROBE_LIMIT,
            config.performance.producer_threads,
        )?;
        stage_timings.format_probe += format_probe_started.elapsed();
        formats
    } else {
        Vec::new()
    };

    let block_count = directory::estimate_directory_block_count(
        &discovery,
        &file_formats,
        block_size,
        preserve_boundaries,
    )?;
    let manifest = directory::manifest_from_discovery(&discovery)?;
    let input_bytes_total = discovery.input_bytes_total;
    let manifest_bytes = manifest.encode()?.len();
    let total_blocks = usize::try_from(block_count)
        .map_err(|_| crate::OxideError::InvalidFormat("block count exceeds usize range"))?;
    let max_inflight_blocks = max_inflight_blocks(
        total_blocks,
        config.workers,
        block_size,
        &config.performance,
    );
    let max_inflight_bytes = max_inflight_blocks.saturating_mul(block_size);

    let worker_pool = WorkerPool::new(
        config.workers,
        Arc::clone(&config.buffer_pool),
        config.compression_algo,
    );
    let processing_totals = Arc::new(ProcessingThroughputTotals::default());
    let raw_fallback_enabled = config.performance.raw_fallback_enabled;
    let worker_processing_totals = Arc::clone(&processing_totals);
    let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression, scratch| {
        process_batch(
            batch,
            pool,
            compression,
            raw_fallback_enabled,
            worker_processing_totals.as_ref(),
            scratch,
        )
    });

    let writer_queue_capacity = config
        .performance
        .writer_result_queue_blocks
        .max(1)
        .min(max_inflight_blocks.max(1));
    let writer_reorder_limit = writer_queue_capacity.max(1);
    let (writer_tx, writer_rx) = bounded::<CompressedBlock>(writer_queue_capacity);
    let writer_output_bytes = Arc::new(AtomicU64::new(container_prefix_bytes(
        block_count,
        manifest_bytes,
    )));
    let writer_output_bytes_shared = Arc::clone(&writer_output_bytes);
    let writer_buffer_pool = Arc::clone(&config.buffer_pool);
    let writer_manifest = manifest.clone();
    let writer_handle = thread::spawn(move || -> Result<DirectoryWriterOutcome<W>> {
        let mut archive_writer = writer_factory(
            writer,
            writer_buffer_pool,
            writer_manifest,
            writer_reorder_limit,
        );
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(ArchiveSourceKind::Directory),
        )?;

        let mut output_bytes_written = container_prefix_bytes(block_count, manifest_bytes);
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

        let footer_started = Instant::now();
        let writer = archive_writer.write_footer()?;
        writer_time += footer_started.elapsed();
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
    let mut output_bytes_written = container_prefix_bytes(block_count, manifest_bytes);
    let mut raw_passthrough_blocks = 0u64;
    let mut writer_queue_peak = 0usize;

    let (batch_tx, batch_rx) = bounded::<Batch>(max_inflight_blocks.max(1));
    let producer_root = discovery.root.clone();
    let producer_files = discovery.files.clone();
    let producer_file_formats = file_formats;
    let producer_block_size = block_size;
    let producer_compression_plan = ChunkEncodingPlan::new(
        config.compression_algo,
        config.performance.compression_preset,
    )
    .with_zstd_level(config.performance.zstd_level);
    let stream_read_buffer_size = config.performance.directory_stream_read_buffer_size.max(1);
    let producer_threads = config.performance.producer_threads.max(1);
    let mmap_threshold = config.performance.directory_mmap_threshold_bytes.max(1);

    let producer_handle = thread::spawn(move || -> Result<DirectoryProducerOutcome> {
        let mut submitter = DirectoryBatchSubmitter::new_with_plan(
            producer_root,
            producer_block_size,
            preserve_boundaries,
            producer_compression_plan,
        );
        let mut producer_read = Duration::ZERO;

        let submit_batch = |batch: Batch| -> Result<()> {
            batch_tx.send(batch).map_err(|_| {
                crate::OxideError::CompressionError(
                    "directory producer channel closed before completion".to_string(),
                )
            })
        };

        let mut prefetch_request_tx = None;
        let mut prefetch_result_rx = None;
        let mut prefetch_handles = Vec::new();
        let prefetch_window = producer_threads
            .saturating_mul(DIRECTORY_PREFETCH_WINDOW)
            .max(DIRECTORY_PREFETCH_WINDOW);
        if producer_threads > 1 {
            let helper_threads = producer_threads.saturating_sub(1).max(1);
            let (prefetch_tx, prefetch_rx) = bounded::<PrefetchRequest>(prefetch_window);
            let (result_tx, result_rx) = bounded::<PrefetchResult>(prefetch_window);
            for _ in 0..helper_threads {
                let worker_prefetch_rx = prefetch_rx.clone();
                let worker_result_tx = result_tx.clone();
                let handle = thread::spawn(move || -> Result<()> {
                    while let Ok(request) = worker_prefetch_rx.recv() {
                        let read_started = Instant::now();
                        let data = fs::read(&request.file.full_path)?;
                        let read_elapsed = read_started.elapsed();
                        worker_result_tx
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
                prefetch_handles.push(handle);
            }
            drop(result_tx);
            prefetch_request_tx = Some(prefetch_tx);
            prefetch_result_rx = Some(result_rx);
        }

        let mut prefetched = BTreeMap::<usize, PrefetchResult>::new();
        let mut next_prefetch = 0usize;
        let mut read_buffer = vec![0u8; stream_read_buffer_size];
        let mut format_probe = vec![0u8; DIRECTORY_FORMAT_PROBE_LIMIT];
        for (file_index, file) in producer_files.iter().enumerate() {
            let file_format = if preserve_boundaries {
                producer_file_formats[file_index]
            } else {
                FileFormat::Common
            };
            if let Some(prefetch_tx) = prefetch_request_tx.as_ref() {
                while next_prefetch < producer_files.len()
                    && next_prefetch <= file_index.saturating_add(prefetch_window)
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
                                    "directory prefetch queue closed before completion".to_string(),
                                )
                            })?;
                    }
                    next_prefetch += 1;
                }
            }

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
                                "directory prefetch worker stopped before completion".to_string(),
                            )
                        })?;
                        if result.index == file_index {
                            break result;
                        }
                        prefetched.insert(result.index, result);
                    }
                };
                producer_read += prefetched_file.read_elapsed;
                let file_format = if preserve_boundaries {
                    file_format
                } else {
                    detect_file_format_hint(&prefetched_file.data)
                };
                submitter.push_bytes_with_hint(&prefetched_file.data, file_format, |batch| {
                    submit_batch(batch)
                })?;
            } else if file_size >= mmap_threshold {
                let read_started = Instant::now();
                let mmap = MmapInput::open(&file.full_path)?;
                let file_format = if preserve_boundaries {
                    file_format
                } else {
                    let probe_end = mmap.len().min(DIRECTORY_FORMAT_PROBE_LIMIT);
                    let probe = mmap.mapped_slice(0, probe_end)?;
                    detect_file_format_hint(probe.as_slice())
                };
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
                let file_format = if preserve_boundaries {
                    file_format
                } else {
                    let probe_read = file_reader.read(&mut format_probe)?;
                    let detected = detect_file_format_hint(&format_probe[..probe_read]);
                    if probe_read > 0 {
                        submitter.push_bytes_with_hint(
                            &format_probe[..probe_read],
                            detected,
                            |batch| submit_batch(batch),
                        )?;
                    }
                    detected
                };
                loop {
                    let read_started = Instant::now();
                    let read = file_reader.read(&mut read_buffer)?;
                    producer_read += read_started.elapsed();
                    if read == 0 {
                        break;
                    }

                    submitter.push_bytes_with_hint(&read_buffer[..read], file_format, |batch| {
                        submit_batch(batch)
                    })?;
                }
            }

            if let Some(result_rx) = prefetch_result_rx.as_ref() {
                while let Ok(result) = result_rx.try_recv() {
                    prefetched.insert(result.index, result);
                }
            }
        }

        drop(prefetch_request_tx);
        for prefetch_handle in prefetch_handles {
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

        submitter.finish(submit_batch)?;
        Ok(DirectoryProducerOutcome { producer_read })
    });

    let results_rx = handle.results_receiver().clone();
    let mut producer_done = false;
    let mut shutdown_called = false;
    loop {
        let mut progressed = false;

        while !producer_done && submitted_count.saturating_sub(received_count) < max_inflight_blocks
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

        let drained = drain_worker_results(
            &results_rx,
            SUBMISSION_DRAIN_BUDGET,
            &writer_tx,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut received_count,
        );
        progressed |= drained > 0;

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
            let inflight = submitted_count.saturating_sub(received_count);
            if !producer_done && inflight < max_inflight_blocks {
                let wait_started = Instant::now();
                select! {
                    recv(batch_rx) -> batch => {
                        stage_timings.submit_wait += wait_started.elapsed();
                        match batch {
                            Ok(batch) => {
                                handle.submit(batch)?;
                                submitted_count += 1;
                            }
                            Err(_) => {
                                producer_done = true;
                            }
                        }
                    }
                    recv(results_rx) -> result => {
                        stage_timings.result_wait += wait_started.elapsed();
                        recv_worker_result(
                            result,
                            &writer_tx,
                            &mut completed_bytes,
                            &mut first_error,
                            &mut raw_passthrough_blocks,
                            &mut writer_queue_peak,
                            &mut received_count,
                        )?;
                    }
                }
            } else if received_count < submitted_count {
                let wait_started = Instant::now();
                let result = results_rx.recv();
                let waited = wait_started.elapsed();
                stage_timings.result_wait += waited;
                if inflight >= max_inflight_blocks {
                    stage_timings.submit_wait += waited;
                }
                recv_worker_result(
                    result,
                    &writer_tx,
                    &mut completed_bytes,
                    &mut first_error,
                    &mut raw_passthrough_blocks,
                    &mut writer_queue_peak,
                    &mut received_count,
                )?;
            } else if !producer_done {
                let wait_started = Instant::now();
                match batch_rx.recv() {
                    Ok(batch) => {
                        stage_timings.submit_wait += wait_started.elapsed();
                        handle.submit(batch)?;
                        submitted_count += 1;
                    }
                    Err(_) => {
                        stage_timings.submit_wait += wait_started.elapsed();
                        producer_done = true;
                    }
                }
            } else {
                break;
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
        let result = results_rx.recv();
        stage_timings.result_wait += wait_started.elapsed();
        recv_worker_result(
            result,
            &writer_tx,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut received_count,
        )?;
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
    handle.join()?;

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
        config.performance.compression_preset,
        max_inflight_blocks,
        max_inflight_bytes,
        config.performance.max_inflight_bytes,
        config.performance.max_inflight_blocks_per_worker,
        writer_queue_capacity,
        writer_reorder_limit,
        pending_write_peak,
        writer_queue_peak,
        stage_timings,
        processing_snapshot,
    );

    let elapsed = started_at.elapsed();
    record_archive_run_telemetry(elapsed, stage_timings);
    let report = build_archive_report(
        ArchiveSourceKind::Directory,
        elapsed,
        input_bytes_total,
        output_bytes_written,
        block_count,
        final_runtime.completed as u32,
        final_runtime.workers,
        extensions,
        *options,
    );
    sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
    Ok(ArchiveRun {
        writer: writer_outcome.writer,
        report,
    })
}

fn detect_file_format_hint(bytes: &[u8]) -> FileFormat {
    let probe_len = bytes.len().min(DIRECTORY_FORMAT_PROBE_LIMIT);
    FormatDetector::detect(&bytes[..probe_len])
}

fn drain_worker_results(
    results_rx: &Receiver<Result<CompressedBlock>>,
    max_results: usize,
    writer_tx: &crossbeam_channel::Sender<CompressedBlock>,
    completed_bytes: &mut u64,
    first_error: &mut Option<crate::OxideError>,
    raw_passthrough_blocks: &mut u64,
    writer_queue_peak: &mut usize,
    received_count: &mut usize,
) -> usize {
    let mut drained = 0usize;
    while drained < max_results {
        match results_rx.try_recv() {
            Ok(result) => {
                record_result_to_writer_queue(
                    result,
                    writer_tx,
                    completed_bytes,
                    first_error,
                    raw_passthrough_blocks,
                    writer_queue_peak,
                );
                *received_count += 1;
                drained += 1;
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
        }
    }
    drained
}

fn recv_worker_result(
    result: std::result::Result<Result<CompressedBlock>, crossbeam_channel::RecvError>,
    writer_tx: &crossbeam_channel::Sender<CompressedBlock>,
    completed_bytes: &mut u64,
    first_error: &mut Option<crate::OxideError>,
    raw_passthrough_blocks: &mut u64,
    writer_queue_peak: &mut usize,
    received_count: &mut usize,
) -> Result<()> {
    match result {
        Ok(result) => {
            record_result_to_writer_queue(
                result,
                writer_tx,
                completed_bytes,
                first_error,
                raw_passthrough_blocks,
                writer_queue_peak,
            );
            *received_count += 1;
            Ok(())
        }
        Err(_) => Err(crate::OxideError::CompressionError(
            "worker result channel closed before completion".to_string(),
        )),
    }
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
