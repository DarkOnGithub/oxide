use std::path::{Path, PathBuf};

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use oxide_core::CompressionAlgo;

#[derive(Parser)]
#[command(
    name = "oxide",
    version,
    about = "Oxide archiver CLI",
    long_about = "Archive and extract .oxz files with processing stats."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Archive a file or directory into an .oxz archive.
    Archive(ArchiveArgs),
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

    /// Number of compression worker threads (0 = auto from physical cores).
    #[arg(long)]
    pub workers: Option<usize>,

    /// Compression algorithm to store in archive metadata.
    #[arg(long, value_enum)]
    pub compression: Option<CompressionArg>,

    /// Archive tuning preset name from the preset config file.
    #[arg(long)]
    pub preset: Option<String>,

    /// Archive preset config file path. Defaults to the bundled presets file.
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

    /// Keep file-type boundaries as hard block boundaries in directory mode.
    #[arg(
        long,
        action = ArgAction::Set,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true
    )]
    pub preserve_format_boundaries: Option<bool>,

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

    /// Progress refresh interval in milliseconds.
    #[arg(long, default_value_t = 250)]
    pub stats_interval_ms: u64,

    /// Number of decode worker threads (defaults to CPU count).
    #[arg(long, default_value_t = num_cpus::get())]
    pub workers: usize,

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
}

impl From<CompressionArg> for CompressionAlgo {
    fn from(value: CompressionArg) -> Self {
        match value {
            CompressionArg::Lz4 => CompressionAlgo::Lz4,
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
mod tests {
    use std::path::Path;

    use clap::Parser;

    use super::{Cli, Commands, default_extract_output_path, default_output_path, parse_size};

    #[test]
    fn parse_size_supports_binary_suffixes() {
        assert_eq!(parse_size("64K").unwrap(), 64 * 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_size("3gb").unwrap(), 3 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_size_rejects_invalid_values() {
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
        assert!(parse_size("16T").is_err());
    }

    #[test]
    fn default_output_path_appends_archive_extension() {
        assert_eq!(
            default_output_path(Path::new("demo/file.txt")),
            Path::new("demo/file.txt.oxz")
        );
    }

    #[test]
    fn default_extract_output_path_strips_oxz_extension() {
        assert_eq!(
            default_extract_output_path(Path::new("demo/archive.oxz")),
            Path::new("demo/archive")
        );
    }

    #[test]
    fn default_extract_output_path_falls_back_when_extension_is_missing() {
        assert_eq!(
            default_extract_output_path(Path::new("demo/archive")),
            Path::new("demo/archive.out")
        );
    }

    #[test]
    fn archive_command_accepts_preset_flag() {
        let cli = Cli::try_parse_from(["oxide", "archive", "demo/input", "--preset", "compact"])
            .expect("archive arguments should parse");

        match cli.command {
            Commands::Archive(args) => assert_eq!(args.preset.as_deref(), Some("compact")),
            _ => panic!("expected archive command"),
        }
    }

    #[test]
    fn extract_command_accepts_telemetry_flag() {
        let cli =
            Cli::try_parse_from(["oxide", "extract", "demo/input.oxz", "--telemetry-details"])
                .expect("extract arguments should parse");

        match cli.command {
            Commands::Extract(args) => assert!(args.telemetry_details),
            _ => panic!("expected extract command"),
        }
    }
}
