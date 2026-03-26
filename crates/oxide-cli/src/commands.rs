use std::fs::File;
use std::io::{self, IsTerminal};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxide_core::{
    ArchivePipeline, ArchivePipelineConfig, BufferPool, CompressionAlgo,
    PipelinePerformanceOptions, RunTelemetryOptions,
};

use crate::AppResult;
use crate::cli::{
    ArchiveArgs, ExtractArgs, TreeArgs, default_extract_output_path, default_output_path,
};
use crate::presets::{ArchiveOverrides, ResolvedArchiveSettings, resolve_archive_settings};
use crate::progress::{ArchiveCliSink, ExtractCliSink, LiveRateStats};
use crate::report::{
    ArchiveReportSummary, ExtractReportSummary, print_archive_report_summary,
    print_extract_report_summary,
};
use crate::tree::print_archive_tree;
use crate::ui::{StreamTarget, Tone, tagged_message};

pub fn archive(args: ArchiveArgs) -> AppResult {
    let ArchiveArgs {
        input,
        output,
        block_size,
        workers,
        compression,
        skip_compression,
        compression_level,
        preset,
        preset_file,
        pool_capacity,
        pool_buffers,
        stats_interval_ms,
        inflight_bytes,
        inflight_blocks_per_worker,
        stream_read_buffer,
        producer_threads,
        directory_mmap_threshold,
        writer_queue_blocks,
        result_wait_ms,
        telemetry_details,
    } = args;
    let settings = resolve_archive_settings(
        preset_file.as_deref(),
        preset.as_deref(),
        ArchiveOverrides {
            compression: compression.map(Into::into),
            skip_compression,
            dictionary_mode: None,
            compression_level,
            block_size,
            workers,
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
            inflight_bytes,
            inflight_blocks_per_worker,
            stream_read_buffer,
            producer_threads,
            directory_mmap_threshold,
            writer_queue_blocks,
            result_wait_ms,
        },
    )?;

    let output_path = output.unwrap_or_else(|| default_output_path(&input));
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let producer_threads = settings.producer_threads.max(1);
    let compression_workers = resolve_archive_workers(settings.workers, producer_threads);
    let buffer_pool = Arc::new(BufferPool::new(
        settings.pool_capacity.max(1),
        settings.pool_buffers.max(1),
    ));
    let pipeline = build_archive_pipeline(&settings, compression_workers, Arc::clone(&buffer_pool));
    let output_file = File::create(&output_path)?;
    let telemetry_options = RunTelemetryOptions {
        progress_interval: Duration::from_millis(settings.stats_interval_ms.max(50)),
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };

    let mut live_rates = LiveRateStats::default();
    let discovery_started = Instant::now();
    let interactive_progress = io::stderr().is_terminal();
    if interactive_progress {
        eprintln!(
            "{}",
            tagged_message(
                StreamTarget::Stderr,
                Tone::Info,
                "preset",
                &format!(
                    "using {} ({}) from {}",
                    settings.profile_name,
                    compression_name(
                        settings.compression,
                        settings.compression_level,
                        settings.compression_extreme,
                    ),
                    settings.profile_source
                ),
            )
        );
        eprintln!(
            "{}",
            tagged_message(
                StreamTarget::Stderr,
                Tone::Info,
                "scan",
                "discovering input and planning blocks...",
            )
        );
    }

    let mut sink = ArchiveCliSink::new(&mut live_rates, discovery_started, interactive_progress);
    let run =
        pipeline.archive_path_seekable(&input, output_file, telemetry_options, Some(&mut sink))?;
    sink.finish_discovery_notice();

    if interactive_progress {
        eprintln!();
    }

    print_archive_report_summary(ArchiveReportSummary {
        input: &input,
        output: &output_path,
        profile_name: &settings.profile_name,
        compression: settings.compression,
        compression_level: settings.compression_level,
        compression_extreme: settings.compression_extreme,
        report: &run.report,
        peak_read_bps: live_rates.peak_read_bps,
        peak_write_bps: live_rates.peak_write_bps,
        buffer_pool: &buffer_pool,
        telemetry_details,
    });

    Ok(())
}

pub fn extract(args: ExtractArgs) -> AppResult {
    let ExtractArgs {
        input,
        output,
        only,
        only_regex,
        stats_interval_ms,
        workers,
        telemetry_details,
    } = args;

    let output_path = output.unwrap_or_else(|| default_extract_output_path(&input));
    let pipeline = build_extract_pipeline(workers);
    let telemetry_options = RunTelemetryOptions {
        progress_interval: Duration::from_millis(stats_interval_ms.max(50)),
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };

    let mut sink = ExtractCliSink::new();
    let report = if only.is_empty() && only_regex.is_empty() {
        pipeline.extract_path(
            File::open(&input)?,
            &output_path,
            telemetry_options,
            Some(&mut sink),
        )?
    } else {
        pipeline.extract_path_filtered_with_regex(
            File::open(&input)?,
            &output_path,
            &only,
            &only_regex,
            telemetry_options,
            Some(&mut sink),
        )?
    };
    if sink.rendered_line() {
        eprintln!();
    }

    print_extract_report_summary(ExtractReportSummary {
        archive_path: &input,
        output_path: &output_path,
        report: &report,
        telemetry_details,
    });

    Ok(())
}

pub fn tree(args: TreeArgs) -> AppResult {
    let reader = oxide_core::ArchiveReader::new(File::open(&args.input)?)?;
    print_archive_tree(&args.input, reader.manifest().entries());
    Ok(())
}

fn build_archive_pipeline(
    settings: &ResolvedArchiveSettings,
    workers: usize,
    buffer_pool: Arc<BufferPool>,
) -> ArchivePipeline {
    let mut performance = PipelinePerformanceOptions::default();
    performance.compression_level = settings.compression_level;
    performance.lzma_extreme = settings.compression_extreme;
    performance.dictionary_mode = settings.dictionary_mode;
    performance.max_inflight_bytes = settings.inflight_bytes.max(1);
    performance.max_inflight_blocks_per_worker = settings.inflight_blocks_per_worker.max(1);
    performance.directory_stream_read_buffer_size = settings.stream_read_buffer.max(1);
    performance.producer_threads = settings.producer_threads.max(1);
    performance.directory_mmap_threshold_bytes = settings.directory_mmap_threshold.max(1);
    performance.writer_result_queue_blocks = settings.writer_queue_blocks.max(1);
    performance.result_wait_timeout = Duration::from_millis(settings.result_wait_ms.max(1));

    let mut config = ArchivePipelineConfig::new(
        settings.block_size.max(1),
        workers.max(1),
        buffer_pool,
        settings.compression,
    );
    config.skip_compression = settings.skip_compression;
    config.performance = performance;
    ArchivePipeline::new(config)
}

fn build_extract_pipeline(workers: usize) -> ArchivePipeline {
    let decode_workers = workers.max(1);
    let buffer_pool = Arc::new(BufferPool::new(
        1024 * 1024,
        decode_workers.saturating_mul(8),
    ));
    let mut config = ArchivePipelineConfig::new(
        1024 * 1024,
        decode_workers,
        buffer_pool,
        CompressionAlgo::Lz4,
    );
    config.performance = PipelinePerformanceOptions::default();
    ArchivePipeline::new(config)
}

fn resolve_archive_workers(requested_workers: usize, producer_threads: usize) -> usize {
    let physical_cores = num_cpus::get_physical().max(1);
    let reserved_producers = producer_threads.min(2);
    let reserved_threads = reserved_producers.saturating_add(1);
    let available_workers = physical_cores.saturating_sub(reserved_threads).max(1);

    if requested_workers > 0 {
        return requested_workers.min(available_workers).max(1);
    }

    available_workers
}

#[cfg(test)]
mod tests {
    use super::resolve_archive_workers;

    #[test]
    fn explicit_worker_count_is_preserved() {
        let physical_cores = num_cpus::get_physical().max(1);
        let available = physical_cores.saturating_sub(3).max(1);
        let requested = available.min(4);

        assert_eq!(resolve_archive_workers(requested, 8), requested);
    }

    #[test]
    fn explicit_worker_count_is_capped_to_available_budget() {
        let physical_cores = num_cpus::get_physical().max(1);
        let expected = physical_cores.saturating_sub(3).max(1);

        assert_eq!(resolve_archive_workers(physical_cores + 8, 4), expected);
        assert_eq!(resolve_archive_workers(physical_cores + 8, 8), expected);
    }

    #[test]
    fn auto_worker_count_caps_reserved_producer_threads() {
        let physical_cores = num_cpus::get_physical().max(1);
        let expected = physical_cores.saturating_sub(3).max(1);

        assert_eq!(resolve_archive_workers(0, 4), expected);
        assert_eq!(resolve_archive_workers(0, 8), expected);
    }
}

fn compression_name(compression: CompressionAlgo, level: Option<i32>, extreme: bool) -> String {
    let name = match compression {
        CompressionAlgo::Lz4 => "lz4",
        CompressionAlgo::Lzma => "lzma",
        CompressionAlgo::Zstd => "zstd",
    };

    match (level, compression == CompressionAlgo::Lzma && extreme) {
        (Some(level), true) => format!("{name}/{level}+extreme"),
        (Some(level), false) => format!("{name}/{level}"),
        (None, true) => format!("{name}+extreme"),
        (None, false) => name.to_string(),
    }
}
