use crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError, bounded, select};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::core::WorkerPool;
use crate::dictionary::{ArchiveDictionaryBank, ArchiveDictionaryMode, DictionaryTrainer};
use crate::format::{ArchiveBlockWriter, ArchiveManifest, ReorderBuffer};
use crate::io::MmapInput;
use crate::pipeline::directory::{self, DirectoryBatchSubmitter};
use crate::pipeline::types::{ArchivePipelineConfig, ArchiveSourceKind};
use crate::telemetry::{
    ArchivePlanningCompleteEvent, ArchiveRun, RunTelemetryOptions, TelemetryEvent, TelemetrySink,
};
use crate::types::{Batch, ChunkEncodingPlan, CompressedBlock, Result};

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
    F: FnOnce(W, ArchiveManifest, usize) -> AW + Send + 'static,
{
    let mut stage_timings = StageTimings::default();
    let planning_started = Instant::now();

    let discovery_started = Instant::now();
    let discovery = directory::discover_directory_tree(root)?;
    stage_timings.discovery += discovery_started.elapsed();
    let block_size = config.target_block_size;
    let file_probe_plans =
        directory::detect_file_probe_plans(&discovery, config.performance.producer_threads)?;
    let file_force_raw_storage = file_probe_plans
        .iter()
        .map(|plan| plan.force_raw_storage)
        .collect::<Vec<_>>();

    let block_count =
        directory::estimate_directory_block_count(&discovery, &file_force_raw_storage, block_size)?;
    let dictionary_bank = train_directory_dictionary_bank(config, &discovery)?;
    let manifest = directory::manifest_from_discovery(&discovery)?
        .with_dictionary_bank(dictionary_bank.clone());
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
    let dictionary_bank = Arc::new(dictionary_bank);
    let processing_totals = Arc::new(ProcessingThroughputTotals::default());
    let raw_fallback_enabled = config.performance.raw_fallback_enabled;
    let skip_compression = config.skip_compression;
    let worker_processing_totals = Arc::clone(&processing_totals);
    let worker_dictionary_bank = Arc::clone(&dictionary_bank);
    let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression, scratch| {
        process_batch(
            batch,
            pool,
            compression,
            skip_compression,
            raw_fallback_enabled,
            worker_dictionary_bank.as_ref(),
            worker_processing_totals.as_ref(),
            scratch,
        )
    });

    let writer_queue_capacity = config
        .performance
        .writer_result_queue_blocks
        .max(1)
        .min(max_inflight_blocks.max(1));
    let result_drain_budget = submission_drain_budget(max_inflight_blocks, writer_queue_capacity);
    let writer_reorder_limit = total_blocks.max(1);
    let (writer_tx, writer_rx) = bounded::<CompressedBlock>(writer_queue_capacity);
    let writer_output_bytes = Arc::new(AtomicU64::new(container_prefix_bytes(
        block_count,
        manifest_bytes,
    )));
    let writer_output_bytes_shared = Arc::clone(&writer_output_bytes);
    let writer_failure = Arc::new(Mutex::new(None::<String>));
    let writer_failure_shared = Arc::clone(&writer_failure);
    let writer_manifest = manifest.clone();
    let writer_handle = thread::spawn(move || -> Result<DirectoryWriterOutcome<W>> {
        let outcome = (|| -> Result<DirectoryWriterOutcome<W>> {
            let mut archive_writer = writer_factory(writer, writer_manifest, writer_reorder_limit);
            archive_writer.write_global_header_with_flags(
                block_count,
                directory::source_kind_flags(ArchiveSourceKind::Directory),
            )?;

            let mut output_bytes_written = container_prefix_bytes(block_count, manifest_bytes);
            let mut writer_time = Duration::ZERO;

            while let Ok(block) = writer_rx.recv() {
                let block_len = block.data.len();
                let write_started = Instant::now();
                archive_writer.write_owned_block(block)?;
                writer_time += write_started.elapsed();

                output_bytes_written = output_bytes_written.saturating_add(block_len as u64);
                writer_output_bytes_shared.store(output_bytes_written, AtomicOrdering::Release);
            }

            let footer_started = Instant::now();
            let writer = archive_writer.write_footer()?;
            writer_time += footer_started.elapsed();
            output_bytes_written = output_bytes_written
                .saturating_add(container_trailer_bytes(block_count, manifest_bytes));
            writer_output_bytes_shared.store(output_bytes_written, AtomicOrdering::Release);

            Ok(DirectoryWriterOutcome {
                writer,
                output_bytes_written,
                pending_write_peak: 0,
                writer_time,
            })
        })();

        if let Err(error) = &outcome {
            if let Ok(mut failure) = writer_failure_shared.lock() {
                *failure = Some(error.to_string());
            }
        }

        outcome
    });

    let started_at = Instant::now();
    let mut last_emit_at = Instant::now();
    let emit_every = options.progress_interval.max(Duration::from_millis(100));
    let wait_timeout = config
        .performance
        .result_wait_timeout
        .max(Duration::from_millis(1));
    let mut completed_bytes = 0u64;
    let mut received_count = 0usize;
    let mut retired_count = 0usize;
    let mut submitted_count = 0usize;
    let mut first_error: Option<crate::OxideError> = None;
    let mut output_bytes_written = container_prefix_bytes(block_count, manifest_bytes);
    let mut raw_passthrough_blocks = 0u64;
    let mut writer_queue_peak = 0usize;
    let mut pending_results = ReorderBuffer::<CompressedBlock>::with_limit(total_blocks.max(1));

    let (batch_tx, batch_rx) = bounded::<Batch>(max_inflight_blocks.max(1));
    let producer_root = discovery.root.clone();
    let producer_files = discovery.files.clone();
    let producer_force_raw_storage = file_force_raw_storage;
    let producer_block_size = block_size;
    let producer_compression_plan = ChunkEncodingPlan::new(config.compression_algo)
        .with_level(config.performance.compression_level)
        .with_lzma_extreme(config.performance.lzma_extreme)
        .with_lzma_dictionary_size(config.performance.lzma_dictionary_size);
    let stream_read_buffer_size = config.performance.directory_stream_read_buffer_size.max(1);
    let producer_threads = config.performance.producer_threads.max(1);
    let mmap_threshold = config.performance.directory_mmap_threshold_bytes.max(1);

    let producer_handle = thread::spawn(move || -> Result<DirectoryProducerOutcome> {
        let mut submitter = DirectoryBatchSubmitter::new_with_plan(
            producer_root,
            producer_block_size,
            producer_compression_plan,
        );
        let mut producer_read = Duration::ZERO;
        let mut producer_submit_blocked = Duration::ZERO;

        let mut submit_batch = |batch: Batch| -> Result<()> {
            let send_started = Instant::now();
            let send_result = batch_tx.send(batch);
            producer_submit_blocked += send_started.elapsed();
            send_result.map_err(|_| {
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
                let worker_mmap_threshold = mmap_threshold;
                let handle = thread::spawn(move || -> Result<()> {
                    while let Ok(request) = worker_prefetch_rx.recv() {
                        let read_started = Instant::now();
                        let file_size = usize::try_from(request.file.size).unwrap_or(usize::MAX);
                        let payload = if file_size >= worker_mmap_threshold {
                            PrefetchPayload::Mapped(MmapInput::open(&request.file.full_path)?)
                        } else {
                            PrefetchPayload::Owned(fs::read(&request.file.full_path)?)
                        };
                        let read_elapsed = read_started.elapsed();
                        worker_result_tx
                            .send(PrefetchResult {
                                index: request.index,
                                payload,
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
        for (file_index, file) in producer_files.iter().enumerate() {
            let force_raw_storage = producer_force_raw_storage[file_index];
            if let Some(prefetch_tx) = prefetch_request_tx.as_ref() {
                while next_prefetch < producer_files.len()
                    && next_prefetch <= file_index.saturating_add(prefetch_window)
                {
                    let candidate = &producer_files[next_prefetch];
                    if candidate.size > 0 {
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
            if prefetch_request_tx.is_some() && file_size > 0 {
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
                match prefetched_file.payload {
                    PrefetchPayload::Owned(data) => {
                        submitter
                            .push_bytes(&data, force_raw_storage, |batch| submit_batch(batch))?;
                    }
                    PrefetchPayload::Mapped(mmap) => {
                        if let Some(map) = mmap.mapping() {
                            submitter.push_mapped(
                                map,
                                0,
                                mmap.len(),
                                force_raw_storage,
                                |batch| submit_batch(batch),
                            )?;
                        }
                    }
                }
            } else if file_size >= mmap_threshold {
                let read_started = Instant::now();
                let mmap = MmapInput::open(&file.full_path)?;
                if let Some(map) = mmap.mapping() {
                    submitter.push_mapped(map, 0, mmap.len(), force_raw_storage, |batch| {
                        submit_batch(batch)
                    })?;
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

                    submitter.push_bytes(&read_buffer[..read], force_raw_storage, |batch| {
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
        Ok(DirectoryProducerOutcome {
            producer_read,
            producer_submit_blocked,
        })
    });

    let results_rx = handle.results_receiver().clone();
    sink.on_event(TelemetryEvent::ArchivePlanningComplete(
        ArchivePlanningCompleteEvent {
            elapsed: planning_started.elapsed(),
            input_bytes_total,
            blocks_total: block_count,
        },
    ));
    let mut producer_done = false;
    let mut shutdown_called = false;
    loop {
        let mut progressed = false;

        while !producer_done
            && can_submit_more_work(
                submitted_count,
                received_count,
                retired_count,
                max_inflight_blocks,
                writer_queue_capacity,
            )
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
            result_drain_budget,
            &writer_tx,
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut stage_timings.writer_enqueue_blocked,
            &mut retired_count,
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

            let worker_inflight = submitted_count.saturating_sub(received_count);
            let can_submit = can_submit_more_work(
                submitted_count,
                received_count,
                retired_count,
                max_inflight_blocks,
                writer_queue_capacity,
            );
            if !producer_done && can_submit {
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
                            &writer_failure,
                            &mut pending_results,
                            &mut completed_bytes,
                            &mut first_error,
                            &mut raw_passthrough_blocks,
                            &mut writer_queue_peak,
                            &mut stage_timings.writer_enqueue_blocked,
                            &mut retired_count,
                            &mut received_count,
                        )?;
                    }
                    default(wait_timeout) => {}
                }
            } else if received_count < submitted_count {
                let wait_started = Instant::now();
                let result = results_rx.recv_timeout(wait_timeout);
                let waited = wait_started.elapsed();
                match result {
                    Ok(result) => {
                        stage_timings.result_wait += waited;
                        if worker_inflight >= max_inflight_blocks {
                            stage_timings.submit_wait += waited;
                        }
                        recv_worker_result(
                            Ok(result),
                            &writer_tx,
                            &writer_failure,
                            &mut pending_results,
                            &mut completed_bytes,
                            &mut first_error,
                            &mut raw_passthrough_blocks,
                            &mut writer_queue_peak,
                            &mut stage_timings.writer_enqueue_blocked,
                            &mut retired_count,
                            &mut received_count,
                        )?;
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(crate::OxideError::CompressionError(
                            "worker result channel closed before completion".to_string(),
                        ));
                    }
                }
            } else if !producer_done {
                let wait_started = Instant::now();
                match batch_rx.recv_timeout(wait_timeout) {
                    Ok(batch) => {
                        stage_timings.submit_wait += wait_started.elapsed();
                        handle.submit(batch)?;
                        submitted_count += 1;
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
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
            stage_timings.producer_submit_blocked += outcome.producer_submit_blocked;
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
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut stage_timings.writer_enqueue_blocked,
            &mut retired_count,
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
        config.performance.compression_level,
        config.performance.lzma_extreme,
        config.performance.lzma_dictionary_size,
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

#[inline]
fn can_submit_more_work(
    submitted_count: usize,
    received_count: usize,
    retired_count: usize,
    max_inflight_blocks: usize,
    writer_queue_capacity: usize,
) -> bool {
    let worker_inflight = submitted_count.saturating_sub(received_count);
    if worker_inflight >= max_inflight_blocks {
        return false;
    }

    let post_worker_backlog = received_count.saturating_sub(retired_count);
    post_worker_backlog < writer_queue_capacity.max(1)
}

#[inline]
fn submission_drain_budget(max_inflight_blocks: usize, writer_queue_capacity: usize) -> usize {
    writer_queue_capacity
        .max(SUBMISSION_DRAIN_BUDGET)
        .min(max_inflight_blocks.max(1))
        .min(256)
        .max(1)
}

fn drain_worker_results(
    results_rx: &Receiver<Result<CompressedBlock>>,
    max_results: usize,
    writer_tx: &crossbeam_channel::Sender<CompressedBlock>,
    writer_failure: &Arc<Mutex<Option<String>>>,
    pending_results: &mut ReorderBuffer<CompressedBlock>,
    completed_bytes: &mut u64,
    first_error: &mut Option<crate::OxideError>,
    raw_passthrough_blocks: &mut u64,
    writer_queue_peak: &mut usize,
    writer_enqueue_blocked: &mut Duration,
    retired_count: &mut usize,
    received_count: &mut usize,
) -> usize {
    let mut drained = 0usize;
    while drained < max_results {
        match results_rx.try_recv() {
            Ok(result) => {
                record_result_to_writer_queue(
                    result,
                    writer_tx,
                    writer_failure,
                    pending_results,
                    completed_bytes,
                    first_error,
                    raw_passthrough_blocks,
                    writer_queue_peak,
                    writer_enqueue_blocked,
                    retired_count,
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
    writer_failure: &Arc<Mutex<Option<String>>>,
    pending_results: &mut ReorderBuffer<CompressedBlock>,
    completed_bytes: &mut u64,
    first_error: &mut Option<crate::OxideError>,
    raw_passthrough_blocks: &mut u64,
    writer_queue_peak: &mut usize,
    writer_enqueue_blocked: &mut Duration,
    retired_count: &mut usize,
    received_count: &mut usize,
) -> Result<()> {
    match result {
        Ok(result) => {
            record_result_to_writer_queue(
                result,
                writer_tx,
                writer_failure,
                pending_results,
                completed_bytes,
                first_error,
                raw_passthrough_blocks,
                writer_queue_peak,
                writer_enqueue_blocked,
                retired_count,
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
    writer_failure: &Arc<Mutex<Option<String>>>,
    pending_results: &mut ReorderBuffer<CompressedBlock>,
    completed_bytes: &mut u64,
    first_error: &mut Option<crate::OxideError>,
    raw_passthrough_blocks: &mut u64,
    writer_queue_peak: &mut usize,
    writer_enqueue_blocked: &mut Duration,
    retired_count: &mut usize,
) {
    match result {
        Ok(block) => {
            *completed_bytes = (*completed_bytes).saturating_add(block.original_len);
            if block.raw_passthrough {
                *raw_passthrough_blocks = raw_passthrough_blocks.saturating_add(1);
            }
            if first_error.is_some() {
                *retired_count += 1;
                return;
            }

            let ready = match pending_results.push(block.id, block) {
                Ok(ready) => ready,
                Err(error) => {
                    fail_directory_queueing(first_error, pending_results, retired_count, error);
                    *retired_count += 1;
                    return;
                }
            };

            for block in ready {
                let send_started = Instant::now();
                let send_result = writer_tx.send(block);
                *writer_enqueue_blocked += send_started.elapsed();
                if send_result.is_err() {
                    let writer_error = writer_failure
                        .lock()
                        .ok()
                        .and_then(|mut failure| failure.take());
                    let error = match writer_error {
                        Some(message) => crate::OxideError::CompressionError(message),
                        None => crate::OxideError::CompressionError(
                            "directory writer queue closed before completion".to_string(),
                        ),
                    };
                    fail_directory_queueing(first_error, pending_results, retired_count, error);
                    *retired_count += 1;
                    return;
                }
                *writer_queue_peak = (*writer_queue_peak).max(writer_tx.len());
                *retired_count += 1;
            }
        }
        Err(error) => {
            fail_directory_queueing(first_error, pending_results, retired_count, error);
            *retired_count += 1;
        }
    }
}

fn fail_directory_queueing(
    first_error: &mut Option<crate::OxideError>,
    pending_results: &mut ReorderBuffer<CompressedBlock>,
    retired_count: &mut usize,
    error: crate::OxideError,
) {
    if first_error.is_none() {
        *retired_count += pending_results.clear();
    }
    first_error.get_or_insert(error);
}

fn train_directory_dictionary_bank(
    config: &ArchivePipelineConfig,
    discovery: &directory::DirectoryDiscovery,
) -> Result<ArchiveDictionaryBank> {
    if config.skip_compression
        || config.compression_algo != crate::CompressionAlgo::Zstd
        || config.performance.dictionary_mode == ArchiveDictionaryMode::Off
    {
        return Ok(ArchiveDictionaryBank::default());
    }

    let mut trainer = DictionaryTrainer::new(config.performance.dictionary_mode);
    for file in &discovery.files {
        let mut handle = match fs::File::open(&file.full_path) {
            Ok(handle) => handle,
            Err(_) => continue,
        };

        let mut sample = vec![0u8; 16 * 1024];
        let read = match handle.read(sample.as_mut_slice()) {
            Ok(read) => read,
            Err(_) => continue,
        };
        trainer.observe(&sample[..read]);
    }

    trainer.build(
        config.compression_algo,
        config.performance.compression_level,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CompressionAlgo;
    use std::sync::{Arc, Mutex};

    fn block(id: usize) -> CompressedBlock {
        CompressedBlock::new(id, vec![id as u8], CompressionAlgo::Lz4, 1)
    }

    #[test]
    fn worker_results_are_forwarded_in_order() {
        let (writer_tx, writer_rx) = bounded::<CompressedBlock>(4);
        let writer_failure = Arc::new(Mutex::new(None::<String>));
        let mut pending_results = ReorderBuffer::with_limit(4);
        let mut completed_bytes = 0u64;
        let mut first_error = None;
        let mut raw_passthrough_blocks = 0u64;
        let mut writer_queue_peak = 0usize;
        let mut writer_enqueue_blocked = Duration::ZERO;
        let mut retired_count = 0usize;

        record_result_to_writer_queue(
            Ok(block(2)),
            &writer_tx,
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut writer_enqueue_blocked,
            &mut retired_count,
        );
        record_result_to_writer_queue(
            Ok(block(1)),
            &writer_tx,
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut writer_enqueue_blocked,
            &mut retired_count,
        );
        record_result_to_writer_queue(
            Ok(block(0)),
            &writer_tx,
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut writer_enqueue_blocked,
            &mut retired_count,
        );

        assert!(first_error.is_none());
        assert_eq!(pending_results.pending_len(), 0);
        assert_eq!(retired_count, 3);
        assert_eq!(completed_bytes, 3);
        assert_eq!(writer_rx.recv().unwrap().id, 0);
        assert_eq!(writer_rx.recv().unwrap().id, 1);
        assert_eq!(writer_rx.recv().unwrap().id, 2);
    }

    #[test]
    fn writer_failure_releases_pending_results_and_surfaces_cause() {
        let (writer_tx, writer_rx) = bounded::<CompressedBlock>(1);
        let writer_failure = Arc::new(Mutex::new(Some("I/O error: disk full".to_string())));
        let mut pending_results = ReorderBuffer::with_limit(4);
        let mut completed_bytes = 0u64;
        let mut first_error = None;
        let mut raw_passthrough_blocks = 0u64;
        let mut writer_queue_peak = 0usize;
        let mut writer_enqueue_blocked = Duration::ZERO;
        let mut retired_count = 0usize;

        record_result_to_writer_queue(
            Ok(block(2)),
            &writer_tx,
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut writer_enqueue_blocked,
            &mut retired_count,
        );

        drop(writer_rx);

        record_result_to_writer_queue(
            Ok(block(0)),
            &writer_tx,
            &writer_failure,
            &mut pending_results,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut writer_queue_peak,
            &mut writer_enqueue_blocked,
            &mut retired_count,
        );

        assert_eq!(pending_results.pending_len(), 0);
        assert_eq!(retired_count, 2);
        assert!(matches!(
            first_error,
            Some(crate::OxideError::CompressionError(message)) if message == "I/O error: disk full"
        ));
    }

    #[test]
    fn submission_gate_allows_worker_refill_while_writer_catches_up() {
        assert!(can_submit_more_work(8, 6, 4, 8, 4));
    }

    #[test]
    fn submission_gate_stops_when_post_worker_backlog_is_full() {
        assert!(!can_submit_more_work(8, 6, 2, 8, 4));
    }

    #[test]
    fn submission_drain_budget_scales_with_pipeline_but_stays_bounded() {
        assert_eq!(submission_drain_budget(32, 8), 32);
        assert_eq!(submission_drain_budget(512, 64), 128);
        assert_eq!(submission_drain_budget(512, 512), 256);
    }
}
