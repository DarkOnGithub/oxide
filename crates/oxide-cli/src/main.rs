use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs::File;
use std::io::IsTerminal;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use oxide_core::telemetry::{HistogramSnapshot, TelemetrySnapshot};
use oxide_core::{
    ArchivePipeline, ArchivePipelineConfig, ArchiveProgressEvent, ArchiveReport, ArchiveSourceKind,
    BufferPool, CompressionAlgo, CompressionPreset, ExtractReport, PipelinePerformanceOptions,
    ReportValue, RunTelemetryOptions, TelemetryEvent, TelemetrySink, ThreadReport, WorkerReport,
};

#[derive(Parser)]
#[command(
    name = "oxide",
    version,
    about = "Oxide archiver CLI",
    long_about = "Archive and extract .oxz files with processing stats."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Archive a file or directory into an .oxz archive.
    Archive {
        /// Source file or directory to archive.
        input: PathBuf,

        /// Destination archive file path (defaults to <input>.oxz).
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Target block size (supports suffixes K/M/G, e.g. 64K, 1M).
        #[arg(long, default_value = "2M", value_parser = parse_size)]
        block_size: usize,

        /// Number of compression worker threads (0 = auto from physical cores).
        #[arg(long, default_value_t = 0)]
        workers: usize,

        /// Compression mode metadata to store.
        #[arg(long, value_enum, default_value_t = CompressionArg::Lz4)]
        compression: CompressionArg,

        /// Planner mode controlling adaptive chunking and per-chunk preset selection.
        #[arg(long, value_enum, default_value_t = PlannerModeArg::Fast)]
        planner_mode: PlannerModeArg,

        /// Enable block-size autotuning for large inputs.
        #[arg(long, default_value_t = false)]
        autotune: bool,

        /// Buffer pool default capacity (supports suffixes K/M/G).
        #[arg(long, default_value = "1M", value_parser = parse_size)]
        pool_capacity: usize,

        /// Maximum number of buffers retained by the pool.
        #[arg(long, default_value_t = 512)]
        pool_buffers: usize,

        /// Progress refresh interval in milliseconds.
        #[arg(long, default_value_t = 250)]
        stats_interval_ms: u64,

        /// Maximum in-flight payload bytes queued through workers.
        #[arg(long, default_value = "2G", value_parser = parse_size)]
        inflight_bytes: usize,

        /// Read buffer size for streaming directory input.
        #[arg(long, default_value = "64M", value_parser = parse_size)]
        stream_read_buffer: usize,

        /// Number of directory producer threads (currently supports 1..=2).
        #[arg(long, default_value_t = 1)]
        producer_threads: usize,

        /// File-size threshold above which directory input uses mmap fast-path.
        #[arg(long, default_value = "8M", value_parser = parse_size)]
        directory_mmap_threshold: usize,

        /// Capacity of the writer result queue (in blocks).
        #[arg(long, default_value_t = 1024)]
        writer_queue_blocks: usize,

        /// Keep file-type boundaries as hard block boundaries in directory mode.
        #[arg(long, default_value_t = false)]
        preserve_format_boundaries: bool,

        /// Timeout in milliseconds while waiting for worker results.
        #[arg(long, default_value_t = 1)]
        result_wait_ms: u64,
    },
    /// Extract an .oxz archive to a file or directory.
    Extract {
        /// Source archive to extract.
        input: PathBuf,

        /// Destination output path.
        ///
        /// For file archives this is the output file path.
        /// For directory archives this is the restored root directory path.
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Progress refresh interval in milliseconds.
        #[arg(long, default_value_t = 250)]
        stats_interval_ms: u64,

        /// Number of decode worker threads (defaults to CPU count).
        #[arg(long, default_value_t = num_cpus::get())]
        workers: usize,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CompressionArg {
    Lz4,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PlannerModeArg {
    Fast,
    Balanced,
    MaxRatio,
}

impl From<CompressionArg> for CompressionAlgo {
    fn from(value: CompressionArg) -> Self {
        match value {
            CompressionArg::Lz4 => CompressionAlgo::Lz4,
        }
    }
}

impl From<PlannerModeArg> for CompressionPreset {
    fn from(value: PlannerModeArg) -> Self {
        match value {
            PlannerModeArg::Fast => CompressionPreset::Fast,
            PlannerModeArg::Balanced => CompressionPreset::Default,
            PlannerModeArg::MaxRatio => CompressionPreset::High,
        }
    }
}

fn main() {
    tracing_subscriber::fmt::init();
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Archive {
            input,
            output,
            block_size,
            workers,
            compression,
            planner_mode,
            autotune,
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
            inflight_bytes,
            stream_read_buffer,
            producer_threads,
            directory_mmap_threshold,
            writer_queue_blocks,
            preserve_format_boundaries,
            result_wait_ms,
        } => archive_command(
            input,
            output,
            block_size,
            workers,
            compression.into(),
            planner_mode.into(),
            autotune,
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
            inflight_bytes,
            stream_read_buffer,
            producer_threads,
            directory_mmap_threshold,
            writer_queue_blocks,
            preserve_format_boundaries,
            result_wait_ms,
        )?,
        Commands::Extract {
            input,
            output,
            stats_interval_ms,
            workers,
        } => extract_command(input, output, stats_interval_ms, workers)?,
    }

    Ok(())
}

fn archive_command(
    input: PathBuf,
    output: Option<PathBuf>,
    block_size: usize,
    workers: usize,
    compression: CompressionAlgo,
    planner_mode: CompressionPreset,
    autotune: bool,
    pool_capacity: usize,
    pool_buffers: usize,
    stats_interval_ms: u64,
    inflight_bytes: usize,
    stream_read_buffer: usize,
    producer_threads: usize,
    directory_mmap_threshold: usize,
    writer_queue_blocks: usize,
    preserve_format_boundaries: bool,
    result_wait_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = output.unwrap_or_else(|| default_output_path(&input));
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let buffer_pool = Arc::new(BufferPool::new(pool_capacity.max(1), pool_buffers.max(1)));
    let mut performance = PipelinePerformanceOptions::default();
    let producer_threads = producer_threads.clamp(1, 2);
    let physical_cores = num_cpus::get_physical().max(1);
    let reserved_threads = producer_threads.saturating_add(1);
    let auto_workers = physical_cores.saturating_sub(reserved_threads).max(1);
    let compression_workers = if workers == 0 {
        auto_workers
    } else {
        workers.max(1)
    };
    performance.autotune_enabled = autotune;
    performance.compression_preset = planner_mode;
    performance.max_inflight_bytes = inflight_bytes.max(1);
    performance.directory_stream_read_buffer_size = stream_read_buffer.max(1);
    performance.producer_threads = producer_threads;
    performance.directory_mmap_threshold_bytes = directory_mmap_threshold.max(1);
    performance.writer_result_queue_blocks = writer_queue_blocks.max(1);
    performance.preserve_directory_format_boundaries = preserve_format_boundaries;
    performance.result_wait_timeout = Duration::from_millis(result_wait_ms.max(1));
    let mut config = ArchivePipelineConfig::new(
        block_size.max(1),
        compression_workers,
        Arc::clone(&buffer_pool),
        compression,
    );
    config.performance = performance;
    let pipeline = ArchivePipeline::new(config);
    let output_file = File::create(&output_path)?;
    let progress_interval = Duration::from_millis(stats_interval_ms.max(50));
    let telemetry_options = RunTelemetryOptions {
        progress_interval,
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };

    let mut live_rates = LiveRateStats::default();
    let discovery_started = Instant::now();
    let interactive_progress = io::stderr().is_terminal();
    if interactive_progress {
        eprintln!(
            "{}",
            paint(
                StreamTarget::Stderr,
                "2",
                "[scan] discovering input and planning blocks..."
            )
        );
    }
    let mut sink = ArchiveCliSink {
        live_rates: &mut live_rates,
        discovery_started,
        discovery_reported: false,
        interactive: interactive_progress,
    };
    let run = pipeline.archive_path(&input, output_file, telemetry_options, Some(&mut sink))?;

    if interactive_progress && !sink.discovery_reported {
        eprintln!(
            "{}",
            paint(
                StreamTarget::Stderr,
                "2",
                &format!(
                    "[ready] discovery complete in {}, starting workers...",
                    format_duration(discovery_started.elapsed())
                )
            )
        );
    }

    if interactive_progress {
        eprintln!();
    }
    print_archive_report_summary(
        &input,
        &output_path,
        compression,
        planner_mode,
        &run.report,
        live_rates.peak_read_bps,
        live_rates.peak_write_bps,
        &buffer_pool,
    );

    Ok(())
}

fn extract_command(
    input: PathBuf,
    output: Option<PathBuf>,
    stats_interval_ms: u64,
    workers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = output.unwrap_or_else(|| default_extract_output_path(&input));
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
    let pipeline = ArchivePipeline::new(config);

    let telemetry_options = RunTelemetryOptions {
        progress_interval: Duration::from_millis(stats_interval_ms.max(50)),
        emit_final_progress: true,
        include_telemetry_snapshot: true,
    };
    let mut sink = ExtractCliSink::default();
    let report = pipeline.extract_path(
        File::open(&input)?,
        &output_path,
        telemetry_options,
        Some(&mut sink),
    )?;
    if sink.rendered_line {
        eprintln!();
    }

    print_extract_report_summary(&input, &output_path, &report);
    Ok(())
}

struct ArchiveCliSink<'a> {
    live_rates: &'a mut LiveRateStats,
    discovery_started: Instant,
    discovery_reported: bool,
    interactive: bool,
}

impl TelemetrySink for ArchiveCliSink<'_> {
    fn on_event(&mut self, event: TelemetryEvent) {
        let TelemetryEvent::ArchiveProgress(snapshot) = event else {
            return;
        };

        if !self.interactive {
            return;
        }

        if !self.discovery_reported {
            self.discovery_reported = true;
            eprintln!(
                "{}",
                paint(
                    StreamTarget::Stderr,
                    "2",
                    &format!(
                        "[ready] discovery complete in {}, starting workers...",
                        format_duration(self.discovery_started.elapsed())
                    )
                )
            );
        }

        let total = snapshot.input_bytes_total;
        let done = snapshot.input_bytes_completed.min(total);
        let (read_avg_bps, read_instant_bps, _write_avg_bps, write_instant_bps) =
            self.live_rates.update(&snapshot);

        let remaining = total.saturating_sub(done);
        let eta = if read_avg_bps > 0.0 {
            Duration::from_secs_f64(remaining as f64 / read_avg_bps)
        } else {
            Duration::from_secs(0)
        };
        let progress = if total > 0 {
            (done as f64 / total as f64) * 100.0
        } else {
            100.0
        };

        let active_workers = snapshot
            .runtime
            .workers
            .iter()
            .filter(|worker| worker.tasks_completed > 0 || worker.busy > Duration::ZERO)
            .count();

        let prefix = paint(
            StreamTarget::Stderr,
            "1;36",
            &format!("[archive {progress:5.1}%]"),
        );

        let line = format!(
            "\r\x1b[2K{prefix} blocks {}/{} | read {} | write {} | cmp {} | wall {} | ratio {:.3}x | queue {} | workers {}/{} | eta {}",
            snapshot.blocks_completed,
            snapshot.blocks_total,
            format_live_rate(read_instant_bps),
            format_live_rate(write_instant_bps),
            format_live_rate(snapshot.compression_avg_bps),
            format_live_rate(snapshot.compression_wall_avg_bps),
            snapshot.output_input_ratio,
            snapshot.blocks_pending,
            active_workers,
            snapshot.runtime.workers.len(),
            format_duration(eta),
        );
        eprint!("{line}");
        let _ = io::stderr().flush();
    }
}

#[derive(Default)]
struct ExtractCliSink {
    last_elapsed: Duration,
    last_archive_bytes: u64,
    rendered_line: bool,
}

impl TelemetrySink for ExtractCliSink {
    fn on_event(&mut self, event: TelemetryEvent) {
        let TelemetryEvent::ExtractProgress(progress) = event else {
            return;
        };
        if !io::stderr().is_terminal() {
            return;
        }
        let elapsed_secs = progress.elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = progress.archive_bytes_completed as f64 / elapsed_secs;
        let delta_bytes = progress
            .archive_bytes_completed
            .saturating_sub(self.last_archive_bytes);
        let delta_elapsed = progress.elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();
        let read_instant_bps = if delta_secs > 0.0 {
            delta_bytes as f64 / delta_secs
        } else {
            read_avg_bps
        };

        self.last_elapsed = progress.elapsed;
        self.last_archive_bytes = progress.archive_bytes_completed;
        let percent = if progress.blocks_total > 0 {
            (progress.blocks_completed as f64 / progress.blocks_total as f64) * 100.0
        } else {
            100.0
        };
        let active_workers = progress
            .runtime
            .workers
            .iter()
            .filter(|worker| worker.tasks_completed > 0 || worker.busy > Duration::ZERO)
            .count();
        let prefix = paint(
            StreamTarget::Stderr,
            "1;36",
            &format!("[extract {percent:5.1}%]"),
        );
        let line = format!(
            "\r\x1b[2K{prefix} blocks {}/{} | archive {} | decoded {} | read {} | inst {} | decode {} | ratio {:.3}x | workers {}/{}",
            progress.blocks_completed,
            progress.blocks_total,
            format_bytes(progress.archive_bytes_completed),
            format_bytes(progress.decoded_bytes_completed),
            format_live_rate(read_avg_bps),
            format_live_rate(read_instant_bps),
            format_live_rate(progress.decode_avg_bps),
            progress.decode_archive_ratio,
            active_workers,
            progress.runtime.workers.len(),
        );
        eprint!("{line}");
        let _ = io::stderr().flush();
        self.rendered_line = true;
    }
}

#[derive(Debug, Default)]
struct LiveRateStats {
    last_input_bytes: u64,
    last_output_bytes: u64,
    last_elapsed: Duration,
    peak_read_bps: f64,
    peak_write_bps: f64,
}

impl LiveRateStats {
    fn update(&mut self, snapshot: &ArchiveProgressEvent) -> (f64, f64, f64, f64) {
        let elapsed = snapshot.elapsed;
        let done = snapshot
            .input_bytes_completed
            .min(snapshot.input_bytes_total);
        let written = snapshot.output_bytes_completed;
        let elapsed_secs = elapsed.as_secs_f64().max(1e-6);

        let read_avg_bps = done as f64 / elapsed_secs;
        let write_avg_bps = written as f64 / elapsed_secs;
        let delta_read_bytes = done.saturating_sub(self.last_input_bytes);
        let delta_write_bytes = written.saturating_sub(self.last_output_bytes);
        let delta_elapsed = elapsed.saturating_sub(self.last_elapsed);
        let delta_secs = delta_elapsed.as_secs_f64();
        let read_instant_bps = if delta_secs > 0.0 {
            delta_read_bytes as f64 / delta_secs
        } else {
            read_avg_bps
        };
        let write_instant_bps = if delta_secs > 0.0 {
            delta_write_bytes as f64 / delta_secs
        } else {
            write_avg_bps
        };

        self.last_input_bytes = done;
        self.last_output_bytes = written;
        self.last_elapsed = elapsed;
        self.peak_read_bps = self.peak_read_bps.max(read_instant_bps);
        self.peak_write_bps = self.peak_write_bps.max(write_instant_bps);

        (
            read_avg_bps,
            read_instant_bps,
            write_avg_bps,
            write_instant_bps,
        )
    }
}

fn print_extract_report_summary(archive_path: &Path, output_path: &Path, report: &ExtractReport) {
    let source_kind = match report.source_kind {
        ArchiveSourceKind::File => "file",
        ArchiveSourceKind::Directory => "directory",
    };

    print_report_title("Extract Complete");
    print_report_section("Overview");
    print_report_value("archive", archive_path.display());
    print_report_value(
        "output",
        format!("{} ({source_kind})", output_path.display()),
    );
    print_report_value("elapsed", format_duration(report.elapsed));
    print_report_value("archive bytes", format_bytes(report.archive_bytes_total));
    print_report_value("decoded bytes", format_bytes(report.decoded_bytes_total));
    print_report_value("output bytes", format_bytes(report.output_bytes_total));
    print_report_value(
        "output/archive ratio",
        format!("{:.3}x", report.output_archive_ratio),
    );
    print_report_value("blocks", report.blocks_total);

    print_report_section("Throughput");
    print_report_value(
        "read average",
        format!("{}/s", format_rate(report.read_avg_bps)),
    );
    print_report_value(
        "decode average",
        format!("{}/s", format_rate(report.decode_avg_bps)),
    );
    print_report_value(
        "output average",
        format!("{}/s", format_rate(report.output_avg_bps)),
    );

    print_report_section("Runtime");
    print_worker_runtime(&report.workers);
    print_thread_stage_summary(
        &report.main_thread,
        &[
            ("archive_read", "archive read"),
            ("decode_submit", "decode submit"),
            ("decode_wait", "decode wait"),
            ("merge", "merge"),
            ("directory_decode", "directory decode"),
            ("output_write", "output write"),
        ],
    );

    if let Some(effective_cores) = extension_f64(&report.extensions, "runtime.effective_cores") {
        print_report_value("effective decode cores", format!("{effective_cores:.2}"));
    }
    if let Some(decode_busy_us) = extension_u64(&report.extensions, "runtime.decode_busy_us") {
        print_report_value(
            "total decode busy time",
            format_duration(Duration::from_micros(decode_busy_us)),
        );
    }

    print_telemetry_summary(report.telemetry.as_ref());
}

fn print_archive_report_summary(
    input: &Path,
    output: &Path,
    compression: CompressionAlgo,
    planner_mode: CompressionPreset,
    report: &ArchiveReport,
    peak_read_bps: f64,
    peak_write_bps: f64,
    buffer_pool: &BufferPool,
) {
    let avg_block = if report.blocks_total > 0 {
        report.input_bytes_total / report.blocks_total as u64
    } else {
        0
    };
    let source_kind = match report.source_kind {
        ArchiveSourceKind::File => "file",
        ArchiveSourceKind::Directory => "directory",
    };

    print_report_title("Archive Complete");
    print_report_section("Overview");
    print_report_value("source", format!("{} ({source_kind})", input.display()));
    print_report_value("output", output.display());
    print_report_value("compression", format!("{:?}", compression));
    print_report_value("planner mode", planner_mode_label(planner_mode));
    print_report_value("elapsed", format_duration(report.elapsed));
    print_report_value("input bytes", format_bytes(report.input_bytes_total));
    print_report_value("output bytes", format_bytes(report.output_bytes_total));
    print_report_value(
        "output/input ratio",
        format!("{:.3}x", report.output_input_ratio),
    );
    print_report_value(
        "blocks",
        format!(
            "{} total (avg block {})",
            report.blocks_total,
            format_bytes(avg_block)
        ),
    );

    print_report_section("Throughput");
    print_report_value(
        "read average",
        format!("{}/s", format_rate(report.read_avg_bps)),
    );
    print_report_value(
        "read peak (live)",
        format!("{}/s", format_rate(peak_read_bps)),
    );
    print_report_value(
        "write average",
        format!("{}/s", format_rate(report.write_avg_bps)),
    );
    print_report_value(
        "write peak (live)",
        format!("{}/s", format_rate(peak_write_bps)),
    );
    if let Some(preprocessing_avg_bps) =
        extension_f64(&report.extensions, "throughput.preprocessing_avg_bps")
    {
        print_report_value(
            "preprocessing avg (core)",
            format!("{}/s", format_rate(preprocessing_avg_bps)),
        );
    }
    if let Some(preprocessing_wall_avg_bps) =
        extension_f64(&report.extensions, "throughput.preprocessing_wall_avg_bps")
    {
        print_report_value(
            "preprocessing avg (wall)",
            format!("{}/s", format_rate(preprocessing_wall_avg_bps)),
        );
    }
    if let Some(compression_avg_bps) =
        extension_f64(&report.extensions, "throughput.compression_avg_bps")
    {
        print_report_value(
            "compression avg (core)",
            format!("{}/s", format_rate(compression_avg_bps)),
        );
    }
    if let Some(compression_wall_avg_bps) =
        extension_f64(&report.extensions, "throughput.compression_wall_avg_bps")
    {
        print_report_value(
            "compression avg (wall)",
            format!("{}/s", format_rate(compression_wall_avg_bps)),
        );
    }
    if let Some(preprocessing_compression_avg_bps) = extension_f64(
        &report.extensions,
        "throughput.preprocessing_compression_avg_bps",
    ) {
        print_report_value(
            "prep+compression avg (core)",
            format!("{}/s", format_rate(preprocessing_compression_avg_bps)),
        );
    }
    if let Some(preprocessing_compression_wall_avg_bps) = extension_f64(
        &report.extensions,
        "throughput.preprocessing_compression_wall_avg_bps",
    ) {
        print_report_value(
            "prep+compression avg (wall)",
            format!("{}/s", format_rate(preprocessing_compression_wall_avg_bps)),
        );
    }

    print_report_section("Runtime");
    print_worker_runtime(&report.workers);
    if let Some(effective_cores) = extension_f64(&report.extensions, "runtime.effective_cores") {
        print_report_value(
            "effective compression cores",
            format!("{effective_cores:.2}"),
        );
    }
    if let Some(compress_busy_us) = extension_u64(&report.extensions, "runtime.compress_busy_us") {
        print_report_value(
            "total compression busy time",
            format_duration(Duration::from_micros(compress_busy_us)),
        );
    }
    if let Some(preprocessing_busy_us) =
        extension_u64(&report.extensions, "runtime.preprocessing_busy_us")
    {
        print_report_value(
            "preprocessing busy time",
            format_duration(Duration::from_micros(preprocessing_busy_us)),
        );
    }
    if let Some(compression_busy_us) =
        extension_u64(&report.extensions, "runtime.compression_busy_us")
    {
        print_report_value(
            "compression stage busy time",
            format_duration(Duration::from_micros(compression_busy_us)),
        );
    }
    if let Some(max_inflight_blocks) =
        extension_u64(&report.extensions, "pipeline.max_inflight_blocks")
    {
        print_report_value("max in-flight blocks", max_inflight_blocks);
    }
    if let Some(max_inflight_bytes) =
        extension_u64(&report.extensions, "pipeline.max_inflight_bytes")
    {
        print_report_value("max in-flight bytes", format_bytes(max_inflight_bytes));
    }
    if let Some(pending_write_peak) =
        extension_u64(&report.extensions, "pipeline.pending_write_peak")
    {
        print_report_value("reorder pending peak", pending_write_peak);
    }

    print_thread_stage_summary(
        &report.main_thread,
        &[
            ("discovery", "discovery"),
            ("format_probe", "probe"),
            ("producer_read", "read"),
            ("submit_wait", "submit wait"),
            ("result_wait", "result wait"),
            ("writer", "writer"),
        ],
    );

    let pool = buffer_pool.metrics();
    print_report_value(
        "buffer pool",
        format!(
            "created {} | recycled {} | dropped {}",
            pool.created, pool.recycled, pool.dropped
        ),
    );
    print_telemetry_summary(report.telemetry.as_ref());
}

fn print_worker_runtime(workers: &[WorkerReport]) {
    let worker_count = workers.len();
    let total_tasks: usize = workers.iter().map(|worker| worker.tasks_completed).sum();
    let max_tasks = workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .max()
        .unwrap_or(0);
    let min_tasks = workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .min()
        .unwrap_or(0);

    print_report_value(
        "scheduler",
        format!(
            "{worker_count} workers | task balance min/max {min_tasks}/{max_tasks} | total tasks {total_tasks}"
        ),
    );
    for worker in workers {
        print_report_value(
            &format!("worker {:02}", worker.worker_id),
            format!(
                "tasks {:>6} | util {:>6.2}% | busy {:>8} | idle {:>8} | uptime {:>8}",
                worker.tasks_completed,
                worker.utilization * 100.0,
                format_duration(worker.busy),
                format_duration(worker.idle),
                format_duration(worker.uptime),
            ),
        );
    }
}

fn print_thread_stage_summary(thread: &ThreadReport, order: &[(&str, &str)]) {
    let mut pieces = Vec::new();
    for (stage_key, label) in order {
        let value_us = thread.stage_us.get(*stage_key).copied().unwrap_or(0);
        if value_us > 0 {
            pieces.push(format!(
                "{label} {}",
                format_duration_compact(Duration::from_micros(value_us))
            ));
        }
    }
    if !pieces.is_empty() {
        print_report_value("stage timings", pieces.join(" | "));
    }
}

fn print_telemetry_summary(snapshot: Option<&TelemetrySnapshot>) {
    let Some(snapshot) = snapshot else {
        return;
    };
    if snapshot.counters.is_empty() && snapshot.gauges.is_empty() && snapshot.histograms.is_empty()
    {
        return;
    }

    print_report_section("Telemetry");
    print_report_value(
        "metrics",
        format!(
            "{} counters | {} gauges | {} histograms",
            snapshot.counters.len(),
            snapshot.gauges.len(),
            snapshot.histograms.len(),
        ),
    );

    let hotspot_rows = telemetry_hotspot_rows(snapshot);
    print_metric_group("hotspots", &hotspot_rows);

    let counters = telemetry_scalar_rows(&snapshot.counters, true);
    let gauges = telemetry_scalar_rows(&snapshot.gauges, true);
    let histograms = telemetry_histogram_rows(&snapshot.histograms, true);

    print_metric_group("counters", &counters);
    print_metric_group("gauges", &gauges);
    print_metric_group("histograms", &histograms);
}

const REPORT_LABEL_WIDTH: usize = 28;
const TELEMETRY_LABEL_WIDTH: usize = 32;

#[derive(Clone, Copy)]
enum StreamTarget {
    Stdout,
    Stderr,
}

fn print_report_title(title: &str) {
    println!(
        "{}",
        paint(StreamTarget::Stdout, "1;32", &format!("== {title} =="))
    );
}

fn print_report_section(title: &str) {
    println!();
    println!(
        "{}",
        paint(StreamTarget::Stdout, "1;36", &format!("-- {title} --"))
    );
}

fn print_report_value(label: &str, value: impl Display) {
    println!("  {label:<REPORT_LABEL_WIDTH$} {value}");
}

fn print_metric_group(title: &str, rows: &[(String, String)]) {
    if rows.is_empty() {
        return;
    }

    println!(
        "  {}",
        paint(StreamTarget::Stdout, "1;33", &format!("[{title}]"))
    );
    for (label, value) in rows {
        println!("    {label:<TELEMETRY_LABEL_WIDTH$} {value}");
    }
}

fn telemetry_hotspot_rows(snapshot: &TelemetrySnapshot) -> Vec<(String, String)> {
    let mut rows = Vec::new();

    for (name, value) in &snapshot.counters {
        if is_duration_metric(name) {
            rows.push((
                raw_telemetry_metric_label(name),
                format_telemetry_scalar(name, *value),
                *value as f64,
            ));
        }
    }

    for (name, value) in &snapshot.gauges {
        if is_duration_metric(name) {
            rows.push((
                raw_telemetry_metric_label(name),
                format_telemetry_scalar(name, *value),
                *value as f64,
            ));
        }
    }

    for (name, histogram) in &snapshot.histograms {
        if is_duration_metric(name) {
            rows.push((
                format!("{}.mean", raw_telemetry_metric_label(name)),
                format_telemetry_mean(name, histogram.mean),
                histogram.mean,
            ));
            rows.push((
                format!("{}.max", raw_telemetry_metric_label(name)),
                format_telemetry_scalar(name, histogram.max),
                histogram.max as f64,
            ));
            rows.push((
                format!("{}.total", raw_telemetry_metric_label(name)),
                format_telemetry_scalar(name, histogram.total),
                histogram.total as f64,
            ));
        }
    }

    rows.sort_by(|left, right| {
        right
            .2
            .total_cmp(&left.2)
            .then_with(|| left.0.cmp(&right.0))
    });
    rows.truncate(12);
    rows.into_iter()
        .map(|(label, value, _)| (label, value))
        .collect()
}

fn telemetry_scalar_rows(
    values: &BTreeMap<String, u64>,
    raw_labels: bool,
) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(name, value)| {
            (
                telemetry_metric_label(name, raw_labels),
                format_telemetry_scalar(name, *value),
            )
        })
        .collect()
}

fn telemetry_histogram_rows(
    values: &BTreeMap<String, HistogramSnapshot>,
    raw_labels: bool,
) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for (name, histogram) in values {
        let label = telemetry_metric_label(name, raw_labels);
        rows.push((format!("{label}.count"), histogram.count.to_string()));
        rows.push((
            format!("{label}.total"),
            format_telemetry_scalar(name, histogram.total),
        ));
        rows.push((
            format!("{label}.mean"),
            format_telemetry_mean(name, histogram.mean),
        ));
        rows.push((
            format!("{label}.min"),
            format_telemetry_scalar(name, histogram.min),
        ));
        rows.push((
            format!("{label}.max"),
            format_telemetry_scalar(name, histogram.max),
        ));
    }
    rows
}

fn telemetry_metric_label(name: &str, raw: bool) -> String {
    if raw {
        return raw_telemetry_metric_label(name);
    }

    let trimmed = name.strip_prefix("oxide.").unwrap_or(name);

    if let Some(base) = trimmed.strip_suffix(".count") {
        return humanize_metric_name(base);
    }
    if let Some(base) = trimmed.strip_suffix(".hist") {
        return humanize_metric_name(base);
    }
    if let Some(base) = trimmed.strip_suffix(".latency_us") {
        return format!("{} latency", humanize_metric_name(base));
    }
    if let Some(base) = trimmed.strip_suffix(".us") {
        return format!("{} time", humanize_metric_name(base));
    }

    humanize_metric_name(trimmed)
}

fn raw_telemetry_metric_label(name: &str) -> String {
    name.to_string()
}

fn humanize_metric_name(name: &str) -> String {
    name.replace('.', " ").replace('_', " ")
}

fn format_telemetry_scalar(name: &str, value: u64) -> String {
    if is_duration_metric(name) {
        format_duration_compact(Duration::from_micros(value))
    } else if is_bytes_metric(name) {
        format_bytes(value)
    } else {
        value.to_string()
    }
}

fn format_telemetry_mean(name: &str, value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return if is_bytes_metric(name) {
            "0 B".to_string()
        } else if is_duration_metric(name) {
            "0us".to_string()
        } else {
            "0".to_string()
        };
    }

    if is_duration_metric(name) {
        format_duration_compact(Duration::from_secs_f64(value / 1_000_000.0))
    } else if is_bytes_metric(name) {
        format_bytes_f64(value)
    } else {
        format!("{value:.2}")
    }
}

fn is_bytes_metric(name: &str) -> bool {
    name.contains("bytes")
}

fn is_duration_metric(name: &str) -> bool {
    name.ends_with(".us") || name.ends_with("_us")
}

fn format_bytes_f64(bytes: f64) -> String {
    if !bytes.is_finite() || bytes <= 0.0 {
        return "0 B".to_string();
    }

    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn format_duration_compact(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs >= 60.0 {
        format_duration(duration)
    } else if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.2}ms", secs * 1_000.0)
    } else {
        format!("{:.0}us", secs * 1_000_000.0)
    }
}

fn paint(target: StreamTarget, code: &str, text: &str) -> String {
    if stream_is_terminal(target) {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn stream_is_terminal(target: StreamTarget) -> bool {
    match target {
        StreamTarget::Stdout => io::stdout().is_terminal(),
        StreamTarget::Stderr => io::stderr().is_terminal(),
    }
}

fn extension_u64(extensions: &BTreeMap<String, ReportValue>, key: &str) -> Option<u64> {
    match extensions.get(key) {
        Some(ReportValue::U64(value)) => Some(*value),
        _ => None,
    }
}

fn extension_f64(extensions: &BTreeMap<String, ReportValue>, key: &str) -> Option<f64> {
    match extensions.get(key) {
        Some(ReportValue::F64(value)) => Some(*value),
        _ => None,
    }
}

fn default_output_path(input: &Path) -> PathBuf {
    let mut out = input.as_os_str().to_os_string();
    out.push(".oxz");
    PathBuf::from(out)
}

fn default_extract_output_path(input: &Path) -> PathBuf {
    let has_oxz_extension = input
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("oxz"))
        .unwrap_or(false);

    if has_oxz_extension {
        let mut out = input.to_path_buf();
        out.set_extension("");
        if out != input {
            return out;
        }
    }

    let mut fallback = input.as_os_str().to_os_string();
    fallback.push(".out");
    PathBuf::from(fallback)
}

fn planner_mode_label(mode: CompressionPreset) -> &'static str {
    match mode {
        CompressionPreset::Fast => "fast",
        CompressionPreset::Default => "balanced",
        CompressionPreset::High => "max-ratio",
    }
}

fn parse_size(value: &str) -> Result<usize, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    let split_at = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (num_part, suffix_part) = trimmed.split_at(split_at);
    if num_part.is_empty() {
        return Err(format!("invalid size: {value}"));
    }

    let base: usize = num_part
        .parse()
        .map_err(|_| format!("invalid size number: {value}"))?;

    let multiplier = match suffix_part.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1usize,
        "k" | "kb" => 1024usize,
        "m" | "mb" => 1024usize * 1024usize,
        "g" | "gb" => 1024usize * 1024usize * 1024usize,
        other => {
            return Err(format!("invalid size suffix '{other}' in '{value}'"));
        }
    };

    base.checked_mul(multiplier)
        .ok_or_else(|| format!("size overflow: {value}"))
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn format_rate(bytes_per_second: f64) -> String {
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return "0 B".to_string();
    }

    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes_per_second;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn format_live_rate(bytes_per_second: f64) -> String {
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return "0B/s".to_string();
    }

    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes_per_second;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{value:.0}B/s")
    } else if value >= 100.0 {
        format!("{value:.0}{}/s", UNITS[unit])
    } else if value >= 10.0 {
        format!("{value:.1}{}/s", UNITS[unit])
    } else {
        format!("{value:.2}{}/s", UNITS[unit])
    }
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else if minutes > 0 {
        format!("{minutes:02}:{seconds:02}")
    } else {
        format!("{seconds}.{millis:03}s")
    }
}
