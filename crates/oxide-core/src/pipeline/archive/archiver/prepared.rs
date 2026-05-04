use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::RecvTimeoutError;

use crate::core::{WorkerPool, WorkerPoolHandle};
use crate::format::{ArchiveBlockWriter, ArchiveManifest, ReorderBuffer};
use crate::pipeline::directory;
use crate::pipeline::types::ArchivePipelineConfig;
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use crate::types::{CompressedBlock, Result};

use super::super::telemetry::*;
use super::super::types::*;
use super::super::types::{CompressionTuning, PipelineQueueStats};
use super::processing::{ProcessBatchConfig, process_batch, shared_raw_chunk_deduper};
use super::utils::*;

pub fn archive_prepared_with_writer<W, AW, F>(
    config: &ArchivePipelineConfig,
    prepared: PreparedInput,
    writer: W,
    options: &RunTelemetryOptions,
    sink: &mut dyn TelemetrySink,
    block_size: usize,
    writer_factory: F,
) -> Result<ArchiveRun<W>>
where
    W: Write,
    AW: ArchiveBlockWriter<InnerWriter = W>,
    F: FnOnce(W, ArchiveManifest) -> AW,
{
    let PreparedInput {
        source_kind,
        manifest,
        batches,
        input_bytes_total,
    } = prepared;
    let total_blocks = batches.len();
    let block_count = u32::try_from(total_blocks)
        .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;
    let manifest_bytes = manifest.encode()?.len();
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
    let dictionary_bank = Arc::new(manifest.dictionary_bank().clone());
    let processing_totals = Arc::new(ProcessingThroughputTotals::default());
    let skip_compression = config.skip_compression;
    let raw_chunk_deduper =
        shared_raw_chunk_deduper(config.performance.raw_chunk_dedup_window_blocks);
    let worker_processing_totals = Arc::clone(&processing_totals);
    let worker_dictionary_bank = Arc::clone(&dictionary_bank);
    let worker_raw_chunk_deduper = Arc::clone(&raw_chunk_deduper);
    let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression, scratch| {
        let config = ProcessBatchConfig {
            skip_compression,
            dictionary_bank: worker_dictionary_bank.as_ref(),
            processing_totals: worker_processing_totals.as_ref(),
            raw_chunk_deduper: Some(&worker_raw_chunk_deduper),
        };
        process_batch(batch, pool, compression, &config, scratch)
    });

    let mut archive_writer = writer_factory(writer, manifest);
    archive_writer
        .write_global_header_with_flags(block_count, directory::source_kind_flags(source_kind))?;
    let mut output_bytes_written = container_prefix_bytes();
    let reorder_limit = total_blocks.max(1);
    let mut pending_write = ReorderBuffer::<CompressedBlock>::with_limit(reorder_limit);
    let mut written_count = 0usize;
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
    let result_wait_timeout = config
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

        let mut writer_state = PreparedWriterState {
            archive_writer: &mut archive_writer,
            pending_write: &mut pending_write,
            written_count: &mut written_count,
            output_bytes_written: &mut output_bytes_written,
            completed_bytes: &mut completed_bytes,
            first_error: &mut first_error,
            raw_passthrough_blocks: &mut raw_passthrough_blocks,
            pending_write_peak: &mut pending_write_peak,
            writer_time: &mut stage_timings.writer,
        };
        let drained = drain_results_to_writer(
            &handle,
            &mut writer_state,
            &mut received_count,
            SUBMISSION_DRAIN_BUDGET,
        )?;

        if submitted_count == total_blocks && !shutdown_called {
            handle.shutdown();
            shutdown_called = true;
        }

        if shutdown_called && received_count == submitted_count {
            if options.emit_final_progress {
                let force = true;
                if progress_emit_due(&last_emit_at, emit_every, force) {
                    emit_archive_progress_if_due(ArchiveProgressCtx {
                        runtime: handle.runtime_snapshot(),
                        processing: processing_totals.snapshot(),
                        source_kind,
                        started_at,
                        input_bytes_total,
                        input_bytes_completed: completed_bytes,
                        output_bytes_completed: output_bytes_written,
                        blocks_total: block_count,
                        emit_every,
                        last_emit_at: &mut last_emit_at,
                        force,
                        sink,
                    });
                }
            }
            break;
        }

        if drained == 0 {
            let force = false;
            if progress_emit_due(&last_emit_at, emit_every, force) {
                emit_archive_progress_if_due(ArchiveProgressCtx {
                    runtime: handle.runtime_snapshot(),
                    processing: processing_totals.snapshot(),
                    source_kind,
                    started_at,
                    input_bytes_total,
                    input_bytes_completed: completed_bytes,
                    output_bytes_completed: output_bytes_written,
                    blocks_total: block_count,
                    emit_every,
                    last_emit_at: &mut last_emit_at,
                    force,
                    sink,
                });
            }
            let wait_started = Instant::now();
            let mut writer_state = PreparedWriterState {
                archive_writer: &mut archive_writer,
                pending_write: &mut pending_write,
                written_count: &mut written_count,
                output_bytes_written: &mut output_bytes_written,
                completed_bytes: &mut completed_bytes,
                first_error: &mut first_error,
                raw_passthrough_blocks: &mut raw_passthrough_blocks,
                pending_write_peak: &mut pending_write_peak,
                writer_time: &mut stage_timings.writer,
            };
            if recv_result_to_writer(&handle, result_wait_timeout, &mut writer_state)? {
                received_count += 1;
            }
            stage_timings.result_wait += wait_started.elapsed();
        }
        let force = false;
        if progress_emit_due(&last_emit_at, emit_every, force) {
            emit_archive_progress_if_due(ArchiveProgressCtx {
                runtime: handle.runtime_snapshot(),
                processing: processing_totals.snapshot(),
                source_kind,
                started_at,
                input_bytes_total,
                input_bytes_completed: completed_bytes,
                output_bytes_completed: output_bytes_written,
                blocks_total: block_count,
                emit_every,
                last_emit_at: &mut last_emit_at,
                force,
                sink,
            });
        }
    }

    let final_runtime = handle.runtime_snapshot();
    handle.join()?;
    if let Some(error) = first_error {
        return Err(error);
    }
    if written_count != submitted_count || pending_write.pending_len() > 0 {
        return Err(crate::OxideError::InvalidFormat(
            "writer has pending blocks after completion",
        ));
    }

    let footer_started = Instant::now();
    let writer = archive_writer.write_footer()?;
    stage_timings.writer += footer_started.elapsed();
    output_bytes_written =
        output_bytes_written.saturating_add(container_trailer_bytes(block_count, manifest_bytes));
    let processing_snapshot = processing_totals.snapshot();
    let extensions = build_stats_extensions(
        input_bytes_total,
        output_bytes_written,
        &final_runtime,
        CompressionTuning {
            block_size,
            raw_passthrough_blocks,
            level: config.performance.compression_level,
            lzma_extreme: config.performance.lzma_extreme,
            lzma_dictionary_size: config.performance.lzma_dictionary_size,
        },
        PipelineQueueStats {
            max_inflight_blocks,
            max_inflight_bytes,
            configured_inflight_bytes: config.performance.max_inflight_bytes,
            max_inflight_blocks_per_worker: config.performance.max_inflight_blocks_per_worker,
            writer_queue_capacity: max_inflight_blocks,
            reorder_pending_limit: reorder_limit,
            pending_write_peak,
            writer_queue_peak: 0,
        },
        stage_timings,
        processing_snapshot,
    );
    let elapsed = started_at.elapsed();
    record_archive_run_telemetry(elapsed, stage_timings);
    let report = build_archive_report(
        source_kind,
        elapsed,
        input_bytes_total,
        output_bytes_written,
        block_count,
        received_count as u32,
        final_runtime.workers,
        extensions,
        *options,
    );
    sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
    Ok(ArchiveRun { writer, report })
}

pub struct PreparedWriterState<'a, AW: ArchiveBlockWriter> {
    pub archive_writer: &'a mut AW,
    pub pending_write: &'a mut ReorderBuffer<CompressedBlock>,
    pub written_count: &'a mut usize,
    pub output_bytes_written: &'a mut u64,
    pub completed_bytes: &'a mut u64,
    pub first_error: &'a mut Option<crate::OxideError>,
    pub raw_passthrough_blocks: &'a mut u64,
    pub pending_write_peak: &'a mut usize,
    pub writer_time: &'a mut Duration,
}

pub fn drain_results_to_writer<AW: ArchiveBlockWriter>(
    handle: &WorkerPoolHandle,
    state: &mut PreparedWriterState<'_, AW>,
    received_count: &mut usize,
    max_results: usize,
) -> Result<usize> {
    let mut drained = 0usize;
    while drained < max_results {
        match handle.recv_timeout(Duration::from_millis(0)) {
            Ok(result) => {
                record_result_to_writer(result, state);
                *received_count += 1;
                drained += 1;
            }
            Err(RecvTimeoutError::Timeout) => break,
            Err(RecvTimeoutError::Disconnected) => {
                if handle.completed_count() >= handle.submitted_count() {
                    break;
                }
                return Err(crate::OxideError::CompressionError(
                    "worker result channel closed before completion".to_string(),
                ));
            }
        }
    }
    Ok(drained)
}

pub fn recv_result_to_writer<AW: ArchiveBlockWriter>(
    handle: &WorkerPoolHandle,
    timeout: Duration,
    state: &mut PreparedWriterState<'_, AW>,
) -> Result<bool> {
    match handle.recv_timeout(timeout) {
        Ok(result) => {
            record_result_to_writer(result, state);
            Ok(true)
        }
        Err(RecvTimeoutError::Timeout) => Ok(false),
        Err(RecvTimeoutError::Disconnected) => {
            if handle.completed_count() >= handle.submitted_count() {
                Ok(false)
            } else {
                Err(crate::OxideError::CompressionError(
                    "worker result channel closed before completion".to_string(),
                ))
            }
        }
    }
}

pub fn record_result_to_writer<AW: ArchiveBlockWriter>(
    result: Result<CompressedBlock>,
    state: &mut PreparedWriterState<'_, AW>,
) {
    match result {
        Ok(block) => {
            *state.completed_bytes = (*state.completed_bytes).saturating_add(block.original_len);
            if block.raw_passthrough {
                *state.raw_passthrough_blocks = state.raw_passthrough_blocks.saturating_add(1);
            }
            if state.first_error.is_some() {
                return;
            }

            let ready = match state.pending_write.push(block.id, block) {
                Ok(ready) => ready,
                Err(error) => {
                    state.first_error.get_or_insert(error);
                    return;
                }
            };
            *state.pending_write_peak =
                (*state.pending_write_peak).max(state.pending_write.pending_len());

            for ready in ready {
                *state.output_bytes_written =
                    (*state.output_bytes_written).saturating_add(ready.data.len() as u64);
                let write_started = Instant::now();
                if let Err(error) = state.archive_writer.write_owned_block(ready) {
                    state.first_error.get_or_insert(error);
                    *state.writer_time += write_started.elapsed();
                    break;
                }
                *state.writer_time += write_started.elapsed();
                *state.written_count += 1;
            }
        }
        Err(error) => {
            state.first_error.get_or_insert(error);
        }
    }
}
