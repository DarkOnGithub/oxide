use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use oxide_core::{
    ArchivePipeline, ArchiveProgressSnapshot, ArchiveReader, ArchiveSourceKind, BLOCK_HEADER_SIZE,
    BufferPool, CompressionAlgo, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OxideError,
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
        Commands::Extract {
            input,
            output,
            stats_interval_ms,
        } => extract_command(input, output, stats_interval_ms)?,
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

fn extract_command(
    input: PathBuf,
    output: Option<PathBuf>,
    stats_interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let output_path = output.unwrap_or_else(|| default_extract_output_path(&input));
    let archive_bytes = fs::metadata(&input)?.len();
    let mut reader = ArchiveReader::new(File::open(&input)?)?;
    let total_blocks = reader.block_count() as u64;
    let flags = reader.global_header().flags;

    let started_at = Instant::now();
    let mut last_emit_at = Instant::now();
    let emit_every = Duration::from_millis(stats_interval_ms.max(50));
    let mut last_archive_bytes = GLOBAL_HEADER_SIZE as u64;
    let mut last_elapsed = Duration::ZERO;
    let mut peak_instant_bps = 0.0f64;
    let mut archive_processed = GLOBAL_HEADER_SIZE as u64;
    let mut payload_processed = 0u64;
    let mut blocks_done = 0u64;
    let mut payload = Vec::new();

    for entry in reader.iter_blocks() {
        let (header, block_data) = entry?;
        archive_processed = archive_processed
            .saturating_add(BLOCK_HEADER_SIZE as u64)
            .saturating_add(header.compressed_size as u64);
        payload_processed = payload_processed.saturating_add(block_data.len() as u64);
        payload.extend_from_slice(&block_data);
        blocks_done = blocks_done.saturating_add(1);

        if last_emit_at.elapsed() >= emit_every || blocks_done == total_blocks {
            let elapsed = started_at.elapsed();
            emit_extract_progress(
                elapsed,
                archive_processed.min(archive_bytes),
                archive_bytes,
                payload_processed,
                blocks_done,
                total_blocks,
                &mut last_archive_bytes,
                &mut last_elapsed,
                &mut peak_instant_bps,
            );
            last_emit_at = Instant::now();
        }
    }

    archive_processed = archive_processed.saturating_add(FOOTER_SIZE as u64);
    emit_extract_progress(
        started_at.elapsed(),
        archive_processed.min(archive_bytes),
        archive_bytes,
        payload_processed,
        blocks_done,
        total_blocks,
        &mut last_archive_bytes,
        &mut last_elapsed,
        &mut peak_instant_bps,
    );
    eprintln!();

    let (source_kind, restored_bytes) = restore_payload(&output_path, payload, flags)?;
    let elapsed = started_at.elapsed();

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

fn emit_extract_progress(
    elapsed: Duration,
    archive_done: u64,
    archive_total: u64,
    payload_done: u64,
    blocks_done: u64,
    blocks_total: u64,
    last_archive_done: &mut u64,
    last_elapsed: &mut Duration,
    peak_instant_bps: &mut f64,
) {
    let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
    let avg_bps = archive_done as f64 / elapsed_secs;
    let delta_bytes = archive_done.saturating_sub(*last_archive_done);
    let delta_elapsed = elapsed.saturating_sub(*last_elapsed);
    let delta_secs = delta_elapsed.as_secs_f64();
    let instant_bps = if delta_secs > 0.0 {
        delta_bytes as f64 / delta_secs
    } else {
        avg_bps
    };
    *peak_instant_bps = (*peak_instant_bps).max(instant_bps);

    let remaining = archive_total.saturating_sub(archive_done);
    let eta = if avg_bps > 0.0 {
        Duration::from_secs_f64(remaining as f64 / avg_bps)
    } else {
        Duration::ZERO
    };
    let progress = if archive_total > 0 {
        (archive_done as f64 / archive_total as f64) * 100.0
    } else {
        100.0
    };

    let line = format!(
        "\r[{progress:6.2}%] blocks {blocks_done}/{blocks_total} | archive {} / {} | payload {} | avg {}/s | inst {}/s | ETA {}",
        format_bytes(archive_done),
        format_bytes(archive_total),
        format_bytes(payload_done),
        format_rate(avg_bps),
        format_rate(instant_bps),
        format_duration(eta),
    );
    eprint!("{line}");
    let _ = io::stderr().flush();

    *last_archive_done = archive_done;
    *last_elapsed = elapsed;
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

fn restore_payload(
    output_path: &Path,
    payload: Vec<u8>,
    flags: u32,
) -> Result<(ArchiveSourceKind, u64), Box<dyn std::error::Error>> {
    if let Some(entries) = decode_directory_entries(&payload, flags)? {
        let restored_bytes = write_directory_entries(output_path, entries)?;
        return Ok((ArchiveSourceKind::Directory, restored_bytes));
    }

    if let Some(parent) = output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, &payload)?;
    Ok((ArchiveSourceKind::File, payload.len() as u64))
}

#[derive(Debug)]
enum DirectoryBundleEntry {
    Directory { rel_path: String },
    File { rel_path: String, data: Vec<u8> },
}

const DIRECTORY_BUNDLE_MAGIC: [u8; 4] = *b"OXDB";
const DIRECTORY_BUNDLE_VERSION: u16 = 1;
const SOURCE_KIND_DIRECTORY_FLAG: u32 = 1 << 0;

fn decode_directory_entries(
    payload: &[u8],
    flags: u32,
) -> Result<Option<Vec<DirectoryBundleEntry>>, OxideError> {
    if flags & SOURCE_KIND_DIRECTORY_FLAG != 0 {
        return decode_directory_bundle(payload).map(Some);
    }

    match decode_directory_bundle(payload) {
        Ok(entries) => Ok(Some(entries)),
        Err(_) => Ok(None),
    }
}

fn write_directory_entries(
    root: &Path,
    entries: Vec<DirectoryBundleEntry>,
) -> Result<u64, Box<dyn std::error::Error>> {
    fs::create_dir_all(root)?;
    let mut restored_bytes = 0u64;

    for entry in entries {
        match entry {
            DirectoryBundleEntry::Directory { rel_path } => {
                let out_path = join_safe(root, &rel_path)?;
                fs::create_dir_all(out_path)?;
            }
            DirectoryBundleEntry::File { rel_path, data } => {
                let out_path = join_safe(root, &rel_path)?;
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                restored_bytes = restored_bytes.saturating_add(data.len() as u64);
                fs::write(out_path, data)?;
            }
        }
    }

    Ok(restored_bytes)
}

fn decode_directory_bundle(payload: &[u8]) -> Result<Vec<DirectoryBundleEntry>, OxideError> {
    if payload.len() < 10 {
        return Err(OxideError::InvalidFormat("directory bundle is too short"));
    }

    if payload[..4] != DIRECTORY_BUNDLE_MAGIC {
        return Err(OxideError::InvalidFormat(
            "archive payload is not a directory bundle",
        ));
    }

    let version = u16::from_le_bytes([payload[4], payload[5]]);
    if version != DIRECTORY_BUNDLE_VERSION {
        return Err(OxideError::InvalidFormat(
            "unsupported directory bundle version",
        ));
    }

    let entry_count = u32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
    let mut cursor = 10usize;
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        if cursor >= payload.len() {
            return Err(OxideError::InvalidFormat(
                "truncated directory bundle entry kind",
            ));
        }
        let kind = payload[cursor];
        cursor += 1;

        let rel_path = decode_path(payload, &mut cursor)?;
        match kind {
            0 => entries.push(DirectoryBundleEntry::Directory { rel_path }),
            1 => {
                if cursor + 8 > payload.len() {
                    return Err(OxideError::InvalidFormat(
                        "truncated directory bundle file size",
                    ));
                }
                let size = u64::from_le_bytes([
                    payload[cursor],
                    payload[cursor + 1],
                    payload[cursor + 2],
                    payload[cursor + 3],
                    payload[cursor + 4],
                    payload[cursor + 5],
                    payload[cursor + 6],
                    payload[cursor + 7],
                ]);
                cursor += 8;

                let size_usize = usize::try_from(size)
                    .map_err(|_| OxideError::InvalidFormat("file size overflow"))?;
                let end = cursor
                    .checked_add(size_usize)
                    .ok_or(OxideError::InvalidFormat(
                        "directory bundle offset overflow",
                    ))?;
                if end > payload.len() {
                    return Err(OxideError::InvalidFormat(
                        "truncated directory bundle file data",
                    ));
                }

                let data = payload[cursor..end].to_vec();
                cursor = end;
                entries.push(DirectoryBundleEntry::File { rel_path, data });
            }
            _ => {
                return Err(OxideError::InvalidFormat(
                    "invalid directory bundle entry kind",
                ));
            }
        }
    }

    if cursor != payload.len() {
        return Err(OxideError::InvalidFormat(
            "directory bundle has trailing data",
        ));
    }

    Ok(entries)
}

fn decode_path(payload: &[u8], cursor: &mut usize) -> Result<String, OxideError> {
    if *cursor + 4 > payload.len() {
        return Err(OxideError::InvalidFormat(
            "truncated directory bundle path length",
        ));
    }
    let len = u32::from_le_bytes([
        payload[*cursor],
        payload[*cursor + 1],
        payload[*cursor + 2],
        payload[*cursor + 3],
    ]) as usize;
    *cursor += 4;

    let end = cursor
        .checked_add(len)
        .ok_or(OxideError::InvalidFormat("directory path offset overflow"))?;
    if end > payload.len() {
        return Err(OxideError::InvalidFormat(
            "truncated directory bundle path data",
        ));
    }

    let rel_path = std::str::from_utf8(&payload[*cursor..end])
        .map_err(|_| OxideError::InvalidFormat("directory path is not utf8"))?
        .to_string();
    *cursor = end;
    Ok(rel_path)
}

fn join_safe(root: &Path, rel_path: &str) -> Result<PathBuf, OxideError> {
    let rel = Path::new(rel_path);
    if rel.is_absolute() {
        return Err(OxideError::InvalidFormat(
            "absolute paths are not allowed in directory bundle",
        ));
    }

    for component in rel.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(OxideError::InvalidFormat(
                    "unsafe path component in directory bundle",
                ));
            }
        }
    }

    Ok(root.join(rel))
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
