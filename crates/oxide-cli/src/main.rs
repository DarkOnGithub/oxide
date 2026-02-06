use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use oxide_core::{
    ArchivePipeline, ArchiveProgressSnapshot, ArchiveSourceKind, BufferPool, CompressionAlgo,
};

#[derive(Parser)]
#[command(
    name = "oxide",
    version,
    about = "Oxide archiver CLI",
    long_about = "Archive files or folders into .oxz with live processing stats."
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
        #[arg(long, default_value = "64K", value_parser = parse_size)]
        block_size: usize,

        /// Number of worker threads (defaults to CPU count).
        #[arg(long, default_value_t = num_cpus::get())]
        workers: usize,

        /// Compression mode metadata to store.
        #[arg(long, value_enum, default_value_t = CompressionArg::Lz4)]
        compression: CompressionArg,

        /// Buffer pool default capacity (supports suffixes K/M/G).
        #[arg(long, default_value = "64K", value_parser = parse_size)]
        pool_capacity: usize,

        /// Maximum number of buffers retained by the pool.
        #[arg(long, default_value_t = 512)]
        pool_buffers: usize,

        /// Progress refresh interval in milliseconds.
        #[arg(long, default_value_t = 250)]
        stats_interval_ms: u64,
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
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
        } => archive_command(
            input,
            output,
            block_size,
            workers,
            compression.into(),
            pool_capacity,
            pool_buffers,
            stats_interval_ms,
        )?,
    }

    Ok(())
}

fn archive_command(
    input: PathBuf,
    output: Option<PathBuf>,
    block_size: usize,
    workers: usize,
    compression: CompressionAlgo,
    pool_capacity: usize,
    pool_buffers: usize,
    stats_interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = output.unwrap_or_else(|| default_output_path(&input));
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let buffer_pool = Arc::new(BufferPool::new(pool_capacity.max(1), pool_buffers.max(1)));
    let pipeline = ArchivePipeline::new(
        block_size.max(1),
        workers.max(1),
        Arc::clone(&buffer_pool),
        compression,
    );
    let output_file = File::create(&output_path)?;
    let progress_interval = Duration::from_millis(stats_interval_ms.max(50));

    let mut last_bytes = 0u64;
    let mut last_elapsed = Duration::ZERO;
    let mut peak_instant_bps = 0.0f64;

    let (_writer, stats) = pipeline.archive_path_with_progress(
        &input,
        output_file,
        progress_interval,
        |snapshot: ArchiveProgressSnapshot| {
            let elapsed = snapshot.elapsed;
            let total = snapshot.input_bytes_total;
            let done = snapshot.input_bytes_completed.min(total);
            let elapsed_secs = elapsed.as_secs_f64().max(1e-6);

            let avg_bps = done as f64 / elapsed_secs;
            let delta_bytes = done.saturating_sub(last_bytes);
            let delta_elapsed = elapsed.saturating_sub(last_elapsed);
            let delta_secs = delta_elapsed.as_secs_f64();
            let instant_bps = if delta_secs > 0.0 {
                delta_bytes as f64 / delta_secs
            } else {
                avg_bps
            };
            peak_instant_bps = peak_instant_bps.max(instant_bps);

            let remaining = total.saturating_sub(done);
            let eta = if avg_bps > 0.0 {
                Duration::from_secs_f64(remaining as f64 / avg_bps)
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
                "\r[{progress:6.2}%] blocks {}/{} | data {} / {} | avg {}/s | inst {}/s | ETA {} | pending {} | workers {}/{}",
                snapshot.blocks_completed,
                snapshot.blocks_total,
                format_bytes(done),
                format_bytes(total),
                format_rate(avg_bps),
                format_rate(instant_bps),
                format_duration(eta),
                snapshot.blocks_pending,
                active_workers,
                snapshot.runtime.workers.len(),
            );
            eprint!("{line}");
            let _ = io::stderr().flush();

            last_bytes = done;
            last_elapsed = elapsed;
        },
    )?;

    eprintln!();
    print_summary(
        &input,
        &output_path,
        compression,
        peak_instant_bps,
        &stats,
        &buffer_pool,
    );

    Ok(())
}

fn print_summary(
    input: &Path,
    output: &Path,
    compression: CompressionAlgo,
    peak_instant_bps: f64,
    stats: &oxide_core::ArchiveRunStats,
    buffer_pool: &BufferPool,
) {
    let elapsed_secs = stats.elapsed.as_secs_f64().max(1e-6);
    let avg_bps = stats.input_bytes_total as f64 / elapsed_secs;
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
    println!("  throughput avg: {}/s", format_rate(avg_bps));
    println!("  throughput peak: {}/s", format_rate(peak_instant_bps));
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
