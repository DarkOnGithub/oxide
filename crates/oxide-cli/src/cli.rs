use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};
use oxide_core::CompressionAlgo;

#[derive(Parser)]
#[command(
    name = "oxide",
    version,
    about = "Oxide archiver CLI",
    long_about = "Archive and extract .oxz files with processing stats"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Archive a file or directory into an .oxz archive.
    Archive(Box<ArchiveArgs>),
    /// Extract an .oxz archive to a file or directory.
    Extract(ExtractArgs),
    /// Print the contents of an .oxz archive as a tree with sizes.
    Tree(TreeArgs),
}

#[derive(Args)]
pub struct ArchiveArgs {
    /// Source file or directory to archive.
    pub input: PathBuf,

    /// Destination archive file path (defaults to <input>.oxz).
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Target block size (supports suffixes K/M/G, e.g. 64K, 1M).
    #[arg(long, value_parser = parse_size)]
    pub block_size: Option<usize>,

    /// Chunking mode used to split file payloads into blocks.
    #[arg(long, value_enum)]
    pub chunking: Option<ChunkingArg>,

    /// Minimum block size for CDC chunking.
    #[arg(long, value_parser = parse_size)]
    pub min_block_size: Option<usize>,

    /// Maximum block size for CDC chunking.
    #[arg(long, value_parser = parse_size)]
    pub max_block_size: Option<usize>,

    /// Number of compression worker threads (0 = auto from physical cores).
    #[arg(long)]
    pub workers: Option<usize>,

    /// Compression algorithm to store in archive metadata.
    #[arg(long, value_enum)]
    pub compression: Option<CompressionArg>,

    /// Store blocks without compression.
    #[arg(long, default_value_t = false)]
    pub skip_compression: bool,

    /// Explicit codec-specific compression level.
    #[arg(long)]
    pub compression_level: Option<i32>,

    /// Reuse an archive dictionary bank from an existing .oxz archive.
    #[arg(long)]
    pub dictionary_from: Option<PathBuf>,

    /// Archive tuning preset name from the preset config file.
    #[arg(long)]
    pub preset: Option<String>,

    /// Archive preset config file path. Defaults to the crate's `presets.json` file.
    #[arg(long)]
    pub preset_file: Option<PathBuf>,

    /// Buffer pool default capacity (supports suffixes K/M/G).
    #[arg(long, value_parser = parse_size)]
    pub pool_capacity: Option<usize>,

    /// Maximum number of buffers retained by the pool.
    #[arg(long)]
    pub pool_buffers: Option<usize>,

    /// Progress refresh interval in milliseconds.
    #[arg(long)]
    pub stats_interval_ms: Option<u64>,

    /// Maximum in-flight payload bytes queued through workers.
    #[arg(long, value_parser = parse_size)]
    pub inflight_bytes: Option<usize>,

    /// Maximum in-flight blocks budgeted per worker before byte caps apply.
    #[arg(long)]
    pub inflight_blocks_per_worker: Option<usize>,

    /// Read buffer size for streaming directory input.
    #[arg(long, value_parser = parse_size)]
    pub stream_read_buffer: Option<usize>,

    /// Total number of directory producer threads, including prefetch helpers.
    #[arg(long)]
    pub producer_threads: Option<usize>,

    /// File-size threshold above which directory input uses mmap fast-path.
    #[arg(long, value_parser = parse_size)]
    pub directory_mmap_threshold: Option<usize>,

    /// Capacity of the writer result queue (in blocks).
    #[arg(long)]
    pub writer_queue_blocks: Option<usize>,

    /// Timeout in milliseconds while waiting for worker results.
    #[arg(long)]
    pub result_wait_ms: Option<u64>,

    /// Print full telemetry tables in the final report.
    #[arg(long, default_value_t = false)]
    pub telemetry_details: bool,
}

#[derive(Args)]
pub struct ExtractArgs {
    /// Source archive to extract.
    pub input: PathBuf,

    /// Destination output path.
    ///
    /// For file archives this is the output file path.
    /// For directory archives this is the restored root directory path.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Restore only matching archive-relative paths.
    ///
    /// Repeat this flag to extract multiple files or directory subtrees.
    #[arg(long = "only")]
    pub only: Vec<String>,

    /// Restore paths whose archive-relative path matches the regex.
    ///
    /// Repeat this flag to supply multiple regex patterns.
    #[arg(long = "only-regex")]
    pub only_regex: Vec<String>,

    /// Progress refresh interval in milliseconds.
    #[arg(long, default_value_t = 250)]
    pub stats_interval_ms: u64,

    /// Decode tuning preset name from the preset config file (same `archive` section as `oxide archive`).
    #[arg(long)]
    pub preset: Option<String>,

    /// Preset config file path. Defaults to the crate's `presets.json` file.
    #[arg(long)]
    pub preset_file: Option<PathBuf>,

    /// Number of decode worker threads (0 = auto from logical cores).
    ///
    /// When omitted, uses the selected archive preset's `workers` value (presets typically use `0` for auto).
    #[arg(long)]
    pub workers: Option<usize>,

    /// Number of directory extract write shards.
    #[arg(long, default_value_t = num_cpus::get().max(1))]
    pub extract_write_shards: usize,

    /// Print full telemetry tables in the final report.
    #[arg(long, default_value_t = false)]
    pub telemetry_details: bool,
}

#[derive(Args)]
pub struct TreeArgs {
    /// Source archive to inspect.
    pub input: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompressionArg {
    Lz4,
    Lzma,
    Zstd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ChunkingArg {
    Fixed,
    Cdc,
}

impl From<CompressionArg> for CompressionAlgo {
    fn from(value: CompressionArg) -> Self {
        match value {
            CompressionArg::Lz4 => CompressionAlgo::Lz4,
            CompressionArg::Lzma => CompressionAlgo::Lzma,
            CompressionArg::Zstd => CompressionAlgo::Zstd,
        }
    }
}

pub fn default_output_path(input: &Path) -> PathBuf {
    let mut out = input.as_os_str().to_os_string();
    out.push(".oxz");
    PathBuf::from(out)
}

pub fn default_extract_output_path(input: &Path) -> PathBuf {
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

pub(crate) fn parse_size(value: &str) -> Result<usize, String> {
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

#[cfg(test)]
#[path = "../tests/unit/cli.rs"]
mod tests;
