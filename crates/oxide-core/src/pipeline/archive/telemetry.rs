use super::super::types::ArchiveSourceKind;
use super::super::types::StatValue;
use super::types::*;
use crate::core::PoolRuntimeSnapshot;
use crate::telemetry::{
    self, profile, tags, ArchiveProgressEvent, ArchiveReport, ExtractProgressEvent, ExtractReport,
    ReportValue, RunTelemetryOptions, TelemetryEvent, TelemetrySink, ThreadReport, WorkerReport,
};
use crate::types::{duration_to_us, CompressionPreset};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub fn record_archive_run_telemetry(elapsed: Duration, stage_timings: StageTimings) {
    let elapsed_us = duration_to_us(elapsed);
    telemetry::increment_counter(tags::METRIC_PIPELINE_ARCHIVE_RUN_COUNT, 1);
    telemetry::record_histogram(tags::METRIC_PIPELINE_ARCHIVE_RUN_LATENCY_US, elapsed_us);
    profile::event(
        tags::PROFILE_PIPELINE,
        &[tags::TAG_PIPELINE],
        "archive_run",
        "ok",
        elapsed_us,
        "archive run completed",
    );
    let _ = stage_timings;
}

pub fn record_extract_run_telemetry(elapsed: Duration, stage_timings: ExtractStageTimings) {
    let elapsed_us = duration_to_us(elapsed);
    telemetry::increment_counter(tags::METRIC_PIPELINE_EXTRACT_RUN_COUNT, 1);
    telemetry::record_histogram(tags::METRIC_PIPELINE_EXTRACT_RUN_LATENCY_US, elapsed_us);
    profile::event(
        tags::PROFILE_PIPELINE,
        &[tags::TAG_PIPELINE],
        "extract_run",
        "ok",
        elapsed_us,
        "extract run completed",
    );
    let _ = stage_timings;
}

pub fn emit_archive_progress_if_due(
    runtime: PoolRuntimeSnapshot,
    processing: ProcessingThroughputSnapshot,
    source_kind: ArchiveSourceKind,
    started_at: Instant,
    input_bytes_total: u64,
    input_bytes_completed: u64,
    output_bytes_completed: u64,
    blocks_total: u32,
    emit_every: Duration,
    last_emit_at: &mut Instant,
    force: bool,
    sink: &mut dyn TelemetrySink,
) {
    if force || last_emit_at.elapsed() >= emit_every {
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

pub fn emit_extract_progress_if_due(
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
        emit_extract_progress(
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

pub fn emit_extract_progress(
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

pub fn build_stats_extensions(
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
    planner_summary: Option<&PlannerArchiveSummary>,
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
    let preprocessing_compression_avg_bps = processing_snapshot.preprocessing_compression_avg_bps();
    let preprocessing_wall_avg_bps =
        processing_snapshot.preprocessing_wall_avg_bps(runtime.elapsed);
    let compression_wall_avg_bps = processing_snapshot.compression_wall_avg_bps(runtime.elapsed);
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
    if let Some(summary) = planner_summary {
        extensions.insert(
            "planner.mode".to_string(),
            StatValue::Text(format!("{:?}", summary.mode)),
        );
        extensions.insert(
            "planner.chunking_mode".to_string(),
            StatValue::Text(summary.chunking_mode.to_string()),
        );
        extensions.insert(
            "planner.superchunk_size".to_string(),
            StatValue::U64(summary.superchunk_size as u64),
        );
        extensions.insert(
            "planner.chunk_count".to_string(),
            StatValue::U64(summary.chunk_count as u64),
        );
        extensions.insert(
            "planner.chunk_bytes_avg".to_string(),
            StatValue::F64(summary.avg_chunk_bytes),
        );
        extensions.insert(
            "planner.chunk_bytes_min".to_string(),
            StatValue::U64(summary.min_chunk_bytes as u64),
        );
        extensions.insert(
            "planner.chunk_bytes_max".to_string(),
            StatValue::U64(summary.max_chunk_bytes as u64),
        );
        extensions.insert(
            "planner.dictionary_count".to_string(),
            StatValue::U64(summary.dictionary_count as u64),
        );
        extensions.insert(
            "planner.dictionary_bytes".to_string(),
            StatValue::U64(summary.dictionary_bytes as u64),
        );
        extensions.insert(
            "planner.chunks_with_dictionaries".to_string(),
            StatValue::U64(summary.chunks_with_dictionaries),
        );
        extensions.insert(
            "planner.preset.fast_chunks".to_string(),
            StatValue::U64(summary.preset_chunk_counts[0]),
        );
        extensions.insert(
            "planner.preset.default_chunks".to_string(),
            StatValue::U64(summary.preset_chunk_counts[1]),
        );
        extensions.insert(
            "planner.preset.high_chunks".to_string(),
            StatValue::U64(summary.preset_chunk_counts[2]),
        );
    }
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

pub fn build_archive_report(
    source_kind: ArchiveSourceKind,
    elapsed: Duration,
    input_bytes_total: u64,
    output_bytes_total: u64,
    blocks_total: u32,
    blocks_completed: u32,
    worker_runtime: Vec<crate::core::WorkerRuntimeSnapshot>,
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
        report_extensions.insert(key, report_value_from_stat(value));
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

pub fn build_extract_report(
    source_kind: ArchiveSourceKind,
    elapsed: Duration,
    archive_bytes_total: u64,
    decoded_bytes_total: u64,
    output_bytes_total: u64,
    blocks_total: u32,
    worker_runtime: Vec<crate::core::WorkerRuntimeSnapshot>,
    stage_timings: ExtractStageTimings,
    pipeline_stats: ExtractPipelineStats,
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
    extensions.insert(
        "pipeline.decode_task_queue_capacity".to_string(),
        ReportValue::U64(pipeline_stats.decode_task_queue_capacity as u64),
    );
    extensions.insert(
        "pipeline.decode_task_queue_peak".to_string(),
        ReportValue::U64(pipeline_stats.decode_task_queue_peak as u64),
    );
    extensions.insert(
        "pipeline.decode_result_queue_capacity".to_string(),
        ReportValue::U64(pipeline_stats.decode_result_queue_capacity as u64),
    );
    extensions.insert(
        "pipeline.decode_result_queue_peak".to_string(),
        ReportValue::U64(pipeline_stats.decode_result_queue_peak as u64),
    );
    extensions.insert(
        "pipeline.reorder_pending_limit".to_string(),
        ReportValue::U64(pipeline_stats.reorder_pending_limit as u64),
    );
    extensions.insert(
        "pipeline.reorder_pending_peak".to_string(),
        ReportValue::U64(pipeline_stats.reorder_pending_peak as u64),
    );
    extensions.insert(
        "pipeline.reorder_pending_bytes_peak".to_string(),
        ReportValue::U64(pipeline_stats.reorder_pending_bytes_peak),
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
        "ordered_write".to_string(),
        stage_timings
            .ordered_write
            .as_micros()
            .min(u64::MAX as u128) as u64,
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

fn report_value_from_stat(value: StatValue) -> ReportValue {
    match value {
        StatValue::U64(value) => ReportValue::U64(value),
        StatValue::F64(value) => ReportValue::F64(value),
        StatValue::Text(value) => ReportValue::Text(value),
    }
}
