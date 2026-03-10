use crossbeam_channel::{bounded, RecvTimeoutError, TryRecvError};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::buffer::BufferPool;
use crate::core::WorkerPool;
use crate::format::{
    ArchiveBlockWriter, ArchiveManifest, FOOTER_SIZE,
};
use crate::io::MmapInput;
use crate::pipeline::directory::{self, DirectoryBatchSubmitter};
use crate::pipeline::types::{ArchivePipelineConfig, ArchiveSourceKind};
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use crate::types::{Batch, CompressedBlock, FileFormat, Result};

use super::super::types::*;
use super::super::telemetry::*;
use super::utils::*;
use super::processing::process_batch;

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
    F: FnOnce(W, Arc<BufferPool>, Vec<crate::format::StoredDictionary>, ArchiveManifest) -> AW
        + Send
        + 'static,
{
    let mut stage_timings = StageTimings::default();

    let discovery_started = Instant::now();
    let discovery = directory::discover_directory_tree(root)?;
    stage_timings.discovery += discovery_started.elapsed();

    let block_size = config.target_block_size;
    let format_probe_started = Instant::now();
    let file_formats =
        directory::detect_file_formats(&discovery, DIRECTORY_FORMAT_PROBE_LIMIT)?;
    stage_timings.format_probe += format_probe_started.elapsed();

    let block_count = directory::estimate_directory_block_count(
        &discovery,
        &file_formats,
        block_size,
        config.performance.preserve_directory_format_boundaries,
    )?;
    let manifest = directory::manifest_from_discovery(&discovery);
    let input_bytes_total = discovery.input_bytes_total;
    let dictionary_bytes = 0;
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
    let (writer_tx, writer_rx) = bounded::<CompressedBlock>(writer_queue_capacity);
    let writer_output_bytes = Arc::new(AtomicU64::new(container_prefix_bytes(
        block_count,
        dictionary_bytes,
        manifest_bytes,
    )));
    let writer_output_bytes_shared = Arc::clone(&writer_output_bytes);
    let writer_buffer_pool = Arc::clone(&config.buffer_pool);
    let writer_dictionaries = Vec::new();
    let writer_manifest = manifest.clone();
    let writer_handle = thread::spawn(move || -> Result<DirectoryWriterOutcome<W>> {
        let mut archive_writer = writer_factory(
            writer,
            writer_buffer_pool,
            writer_dictionaries,
            writer_manifest,
        );
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(ArchiveSourceKind::Directory),
        )?;

        let mut output_bytes_written =
            container_prefix_bytes(block_count, dictionary_bytes, manifest_bytes);
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
    let mut output_bytes_written =
        container_prefix_bytes(block_count, dictionary_bytes, manifest_bytes);
    let mut raw_passthrough_blocks = 0u64;
    let mut writer_queue_peak = 0usize;
    let result_wait_timeout = config
        .performance
        .result_wait_timeout
        .max(Duration::from_millis(1));

    let (batch_tx, batch_rx) = bounded::<Batch>(max_inflight_blocks.max(1));
    let producer_root = discovery.root.clone();
    let producer_directories = discovery.directories.clone();
    let producer_files = discovery.files.clone();
    let producer_file_formats = file_formats.clone();
    let producer_block_size = block_size;
    let preserve_boundaries = config.performance.preserve_directory_format_boundaries;
    let stream_read_buffer_size = config
        .performance
        .directory_stream_read_buffer_size
        .max(1);
    let producer_threads = config.performance.producer_threads.max(1);
    let mmap_threshold = config
        .performance
        .directory_mmap_threshold_bytes
        .max(1);

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
        for (file_index, (file, file_format)) in producer_files
            .iter()
            .zip(producer_file_formats.iter().copied())
            .enumerate()
        {
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
                    &submit_batch,
                )?;
            } else if file_size >= mmap_threshold {
                let read_started = Instant::now();
                let mmap = MmapInput::open(&file.full_path)?;
                let mut offset = 0usize;
                let len = mmap.len();
                while offset < len {
                    let end = offset.saturating_add(stream_read_buffer_size).min(len);
                    let bytes = mmap.mapped_slice(offset, end)?;
                    submitter.push_bytes_with_hint(bytes.as_slice(), file_format, &submit_batch)?;
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
                        &submit_batch,
                    )?;
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
