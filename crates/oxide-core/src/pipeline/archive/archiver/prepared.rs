use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::buffer::BufferPool;
use crate::core::{WorkerPool, WorkerPoolHandle};
use crate::format::{ArchiveBlockWriter, ArchiveManifest, FOOTER_SIZE};
use crate::pipeline::directory;
use crate::pipeline::types::ArchivePipelineConfig;
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetryEvent, TelemetrySink};
use crate::types::{CompressedBlock, Result};

use super::super::telemetry::*;
use super::super::types::*;
use super::processing::process_batch;
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
    F: FnOnce(W, Arc<BufferPool>, Vec<crate::format::StoredDictionary>, ArchiveManifest) -> AW,
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
    let dictionary_bytes = 0;
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

    let mut archive_writer = writer_factory(
        writer,
        Arc::clone(&config.buffer_pool),
        Vec::new(),
        manifest,
    );
    archive_writer
        .write_global_header_with_flags(block_count, directory::source_kind_flags(source_kind))?;
    let mut output_bytes_written =
        container_prefix_bytes(block_count, dictionary_bytes, manifest_bytes);
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
    handle.join()?;
    if let Some(error) = first_error {
        return Err(error);
    }
    if next_write_id != submitted_count || !pending_write.is_empty() {
        return Err(crate::OxideError::InvalidFormat(
            "writer has pending blocks after completion",
        ));
    }

    let footer_started = Instant::now();
    let writer = archive_writer.write_footer()?;
    stage_timings.writer += footer_started.elapsed();
    output_bytes_written = output_bytes_written.saturating_add(FOOTER_SIZE as u64);
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
        max_inflight_blocks,
        max_inflight_blocks,
        pending_write_peak,
        0,
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
        final_runtime.completed as u32,
        final_runtime.workers,
        extensions,
        *options,
    );
    sink.on_event(TelemetryEvent::ArchiveCompleted(report.clone()));
    Ok(ArchiveRun { writer, report })
}

pub fn drain_results_to_writer<AW: ArchiveBlockWriter>(
    handle: &WorkerPoolHandle,
    archive_writer: &mut AW,
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

pub fn recv_result_to_writer<AW: ArchiveBlockWriter>(
    handle: &WorkerPoolHandle,
    timeout: Duration,
    archive_writer: &mut AW,
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

pub fn record_result_to_writer<AW: ArchiveBlockWriter>(
    result: Result<CompressedBlock>,
    archive_writer: &mut AW,
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
