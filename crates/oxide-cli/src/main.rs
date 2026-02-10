use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use oxide_core::{
    ArchivePipeline, ArchiveProgressSnapshot, ArchiveSourceKind, BufferPool, CompressionAlgo,
    PipelinePerformanceOptions,
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
        #[arg(long, default_value = "1M", value_parser = parse_size)]
        block_size: usize,

        /// Number of worker threads (defaults to CPU count).
        #[arg(long, default_value_t = num_cpus::get())]
        workers: usize,

        /// Compression mode metadata to store.
        #[arg(long, value_enum, default_value_t = CompressionArg::Lz4)]
        compression: CompressionArg,

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
    Lzma,
    Deflate,
}

impl From<CompressionArg> for CompressionAlgo {
    fn from(value: CompressionArg) -> Self {
        match value {
            CompressionArg::Lz4 => CompressionAlgo::Lz4,
            CompressionArg::Lzma => CompressionAlgo::Lzma,
            CompressionArg::Deflate => CompressionAlgo::Deflate,
        }
    }
}

fn main() {
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
            autotune,
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
        } => archive_command(
            input,
            output,
            block_size,
            workers,
            compression.into(),
            autotune,
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
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
    autotune: bool,
    pool_capacity: usize,
    pool_buffers: usize,
    stats_interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = output.unwrap_or_else(|| default_output_path(&input));
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let buffer_pool = Arc::new(BufferPool::new(pool_capacity.max(1), pool_buffers.max(1)));
    let mut performance = PipelinePerformanceOptions::default();
    performance.autotune_enabled = autotune;
    let pipeline = ArchivePipeline::with_performance(
        block_size.max(1),
        workers.max(1),
        Arc::clone(&buffer_pool),
        compression,
        performance,
    );
    let output_file = File::create(&output_path)?;
    let progress_interval = Duration::from_millis(stats_interval_ms.max(50));

    let mut last_input_bytes = 0u64;
    let mut last_output_bytes = 0u64;
    let mut last_elapsed = Duration::ZERO;
    let mut peak_read_bps = 0.0f64;
    let mut peak_write_bps = 0.0f64;
    let discovery_started = Instant::now();
    let mut discovery_reported = false;
    eprintln!("discovering input and planning blocks...");

    let (_writer, stats) = pipeline.archive_path_with_progress(
        &input,
        output_file,
        progress_interval,
        |snapshot: ArchiveProgressSnapshot| {
            if !discovery_reported {
                discovery_reported = true;
                eprintln!(
                    "discovery complete in {}, starting workers...",
                    format_duration(discovery_started.elapsed())
                );
            }

            let elapsed = snapshot.elapsed;
            let total = snapshot.input_bytes_total;
            let done = snapshot.input_bytes_completed.min(total);
            let written = snapshot.output_bytes_completed;
            let elapsed_secs = elapsed.as_secs_f64().max(1e-6);

            let read_avg_bps = done as f64 / elapsed_secs;
            let write_avg_bps = written as f64 / elapsed_secs;
            let delta_read_bytes = done.saturating_sub(last_input_bytes);
            let delta_write_bytes = written.saturating_sub(last_output_bytes);
            let delta_elapsed = elapsed.saturating_sub(last_elapsed);
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
            peak_read_bps = peak_read_bps.max(read_instant_bps);
            peak_write_bps = peak_write_bps.max(write_instant_bps);

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

            let line = format!(
                "\r\x1b[2K[{progress:6.2}%] blocks {}/{} | data {} / {} | rd avg {}/s inst {}/s | wr avg {}/s inst {}/s | ETA {} | pending {} | workers {}/{}",
                snapshot.blocks_completed,
                snapshot.blocks_total,
                format_bytes(done),
                format_bytes(total),
                format_rate(read_avg_bps),
                format_rate(read_instant_bps),
                format_rate(write_avg_bps),
                format_rate(write_instant_bps),
                format_duration(eta),
                snapshot.blocks_pending,
                active_workers,
                snapshot.runtime.workers.len(),
            );
            eprint!("{line}");
            let _ = io::stderr().flush();

            last_input_bytes = done;
            last_output_bytes = written;
            last_elapsed = elapsed;
        },
    )?;

    if !discovery_reported {
        eprintln!(
            "discovery complete in {}, starting workers...",
            format_duration(discovery_started.elapsed())
        );
    }

    eprintln!();
    print_summary(
        &input,
        &output_path,
        compression,
        peak_read_bps,
        peak_write_bps,
        &stats,
        &buffer_pool,
    );

    Ok(())
}

fn extract_command(
    input: PathBuf,
    output: Option<PathBuf>,
    _stats_interval_ms: u64,
    workers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = output.unwrap_or_else(|| default_extract_output_path(&input));
    let archive_bytes = fs::metadata(&input)?.len();
    let decode_workers = workers.max(1);
    let buffer_pool = Arc::new(BufferPool::new(
        1024 * 1024,
        decode_workers.saturating_mul(8),
    ));
    let pipeline = ArchivePipeline::with_performance(
        1024 * 1024,
        decode_workers,
        buffer_pool,
        CompressionAlgo::Lz4,
        PipelinePerformanceOptions::default(),
    );

    let started_at = Instant::now();
    let source_kind =
        pipeline.extract_path_with_workers(File::open(&input)?, &output_path, decode_workers)?;
    let elapsed = started_at.elapsed();
    let restored_bytes = measure_restored_bytes(&output_path, source_kind)?;
    let peak_instant_bps = if elapsed.as_secs_f64() > 0.0 {
        archive_bytes as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    print_extract_summary(
        &input,
        &output_path,
        source_kind,
        archive_bytes,
        restored_bytes,
        elapsed,
        peak_instant_bps,
    );
    Ok(())
}

fn measure_restored_bytes(
    output_path: &Path,
    source_kind: ArchiveSourceKind,
) -> Result<u64, Box<dyn std::error::Error>> {
    match source_kind {
        ArchiveSourceKind::File => Ok(fs::metadata(output_path)?.len()),
        ArchiveSourceKind::Directory => measure_directory_bytes(output_path),
    }
}

fn measure_directory_bytes(root: &Path) -> Result<u64, Box<dyn std::error::Error>> {
    let mut total = 0u64;
    if !root.exists() {
        return Ok(0);
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                total = total.saturating_add(entry.metadata()?.len());
            }
        }
    }

    Ok(total)
}

fn print_extract_summary(
    archive_path: &Path,
    output_path: &Path,
    source_kind: ArchiveSourceKind,
    archive_bytes: u64,
    restored_bytes: u64,
    elapsed: Duration,
    peak_instant_bps: f64,
) {
    let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
    let read_bps = archive_bytes as f64 / elapsed_secs;
    let restore_bps = restored_bytes as f64 / elapsed_secs;
    let source_kind = match source_kind {
        ArchiveSourceKind::File => "file",
        ArchiveSourceKind::Directory => "directory",
    };
    let restored_ratio = if archive_bytes > 0 {
        restored_bytes as f64 / archive_bytes as f64
    } else {
        1.0
    };

    println!("extract complete");
    println!("  archive: {}", archive_path.display());
    println!("  output: {} ({source_kind})", output_path.display());
    println!("  elapsed: {}", format_duration(elapsed));
    println!("  archive bytes read: {}", format_bytes(archive_bytes));
    println!("  restored bytes: {}", format_bytes(restored_bytes));
    println!("  restored/archive ratio: {restored_ratio:.3}x");
    println!("  read throughput: {}/s", format_rate(read_bps));
    println!(
        "  read throughput peak: {}/s",
        format_rate(peak_instant_bps)
    );
    println!("  restore throughput: {}/s", format_rate(restore_bps));
}

fn print_summary(
    input: &Path,
    output: &Path,
    compression: CompressionAlgo,
    peak_read_bps: f64,
    peak_write_bps: f64,
    stats: &oxide_core::ArchiveRunStats,
    buffer_pool: &BufferPool,
) {
    let elapsed_secs = stats.elapsed.as_secs_f64().max(1e-6);
    let read_avg_bps = stats.input_bytes_total as f64 / elapsed_secs;
    let write_avg_bps = stats.output_bytes_total as f64 / elapsed_secs;
    let out_ratio = if stats.input_bytes_total > 0 {
        stats.output_bytes_total as f64 / stats.input_bytes_total as f64
    } else {
        1.0
    };
    let avg_block = if stats.blocks_total > 0 {
        stats.input_bytes_total / stats.blocks_total as u64
    } else {
        0
    };
    let source_kind = match stats.source_kind {
        ArchiveSourceKind::File => "file",
        ArchiveSourceKind::Directory => "directory",
    };

    println!("archive complete");
    println!("  source: {} ({source_kind})", input.display());
    println!("  output: {}", output.display());
    println!("  compression metadata: {:?}", compression);
    println!("  elapsed: {}", format_duration(stats.elapsed));
    println!("  input bytes: {}", format_bytes(stats.input_bytes_total));
    println!("  output bytes: {}", format_bytes(stats.output_bytes_total));
    println!("  expansion ratio: {out_ratio:.3}x");
    println!("  throughput avg: {}/s", format_rate(read_avg_bps));
    println!("  throughput peak: {}/s", format_rate(peak_read_bps));
    println!("  disk read avg: {}/s", format_rate(read_avg_bps));
    println!("  disk read peak: {}/s", format_rate(peak_read_bps));
    println!("  disk write avg: {}/s", format_rate(write_avg_bps));
    println!("  disk write peak: {}/s", format_rate(peak_write_bps));
    println!(
        "  blocks: {} total (avg block {})",
        stats.blocks_total,
        format_bytes(avg_block),
    );

    let worker_count = stats.workers.len();
    let total_tasks: usize = stats
        .workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .sum();
    let max_tasks = stats
        .workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .max()
        .unwrap_or(0);
    let min_tasks = stats
        .workers
        .iter()
        .map(|worker| worker.tasks_completed)
        .min()
        .unwrap_or(0);

    println!(
        "  scheduler: {worker_count} workers | task balance min/max {min_tasks}/{max_tasks} | total tasks {total_tasks}"
    );
    println!("  worker runtime:");
    for worker in &stats.workers {
        println!(
            "    w{:02} tasks {:>6} | uptime {:>8} | busy {:>8} | idle {:>8} | util {:>6.2}%",
            worker.worker_id,
            worker.tasks_completed,
            format_duration(worker.uptime),
            format_duration(worker.busy),
            format_duration(worker.idle),
            worker.utilization * 100.0,
        );
    }

    let pool = buffer_pool.metrics();
    println!(
        "  buffer pool: created {} | recycled {} | dropped {}",
        pool.created, pool.recycled, pool.dropped
    );
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
