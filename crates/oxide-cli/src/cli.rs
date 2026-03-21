use std::path::{Path, PathBuf};

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use oxide_core::{AudioStrategy, BinaryStrategy, CompressionAlgo, ImageStrategy, TextStrategy};

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

    /// Store blocks without preprocessing.
    #[arg(long, default_value_t = false)]
    pub skip_preprocessing: bool,

    /// Text preprocessing override.
    #[arg(long, value_enum)]
    pub text_preprocessing: Option<TextPreprocessingArg>,

    /// Image preprocessing override.
    #[arg(long, value_enum)]
    pub image_preprocessing: Option<ImagePreprocessingArg>,

    /// Audio preprocessing override.
    #[arg(long, value_enum)]
    pub audio_preprocessing: Option<AudioPreprocessingArg>,

    /// Binary preprocessing override.
    #[arg(long, value_enum)]
    pub binary_preprocessing: Option<BinaryPreprocessingArg>,

    /// Store blocks without compression.
    #[arg(long, default_value_t = false)]
    pub skip_compression: bool,

    /// Explicit zstd level (1-22). Only valid with `--compression zstd` or zstd presets.
    #[arg(long)]
    pub zstd_level: Option<i32>,

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
    Zstd,
}

impl From<CompressionArg> for CompressionAlgo {
    fn from(value: CompressionArg) -> Self {
        match value {
            CompressionArg::Lz4 => CompressionAlgo::Lz4,
            CompressionArg::Zstd => CompressionAlgo::Zstd,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TextPreprocessingArg {
    None,
    Bpe,
    Bwt,
}

impl From<TextPreprocessingArg> for Option<TextStrategy> {
    fn from(value: TextPreprocessingArg) -> Self {
        match value {
            TextPreprocessingArg::None => None,
            TextPreprocessingArg::Bpe => Some(TextStrategy::Bpe),
            TextPreprocessingArg::Bwt => Some(TextStrategy::Bwt),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ImagePreprocessingArg {
    None,
    Ycocgr,
    Paeth,
    Locoi,
}

impl From<ImagePreprocessingArg> for Option<ImageStrategy> {
    fn from(value: ImagePreprocessingArg) -> Self {
        match value {
            ImagePreprocessingArg::None => None,
            ImagePreprocessingArg::Ycocgr => Some(ImageStrategy::YCoCgR),
            ImagePreprocessingArg::Paeth => Some(ImageStrategy::Paeth),
            ImagePreprocessingArg::Locoi => Some(ImageStrategy::LocoI),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AudioPreprocessingArg {
    None,
    Lpc,
}

impl From<AudioPreprocessingArg> for Option<AudioStrategy> {
    fn from(value: AudioPreprocessingArg) -> Self {
        match value {
            AudioPreprocessingArg::None => None,
            AudioPreprocessingArg::Lpc => Some(AudioStrategy::Lpc),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BinaryPreprocessingArg {
    None,
    Bcj,
}

impl From<BinaryPreprocessingArg> for Option<BinaryStrategy> {
    fn from(value: BinaryPreprocessingArg) -> Self {
        match value {
            BinaryPreprocessingArg::None => None,
            BinaryPreprocessingArg::Bcj => Some(BinaryStrategy::Bcj),
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

    use super::{
        Cli, Commands, ImagePreprocessingArg, TextPreprocessingArg, default_extract_output_path,
        default_output_path, parse_size,
    };

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
    fn archive_command_accepts_zstd_compression() {
        let cli = Cli::try_parse_from(["oxide", "archive", "demo/input", "--compression", "zstd"])
            .expect("archive arguments should parse");

        match cli.command {
            Commands::Archive(args) => {
                assert!(matches!(
                    args.compression,
                    Some(super::CompressionArg::Zstd)
                ));
            }
            _ => panic!("expected archive command"),
        }
    }

    #[test]
    fn archive_command_accepts_zstd_level_flag() {
        let cli = Cli::try_parse_from(["oxide", "archive", "demo/input", "--zstd-level", "19"])
            .expect("archive arguments should parse");

        match cli.command {
            Commands::Archive(args) => assert_eq!(args.zstd_level, Some(19)),
            _ => panic!("expected archive command"),
        }
    }

    #[test]
    fn archive_command_accepts_skip_processing_flags_together() {
        let cli = Cli::try_parse_from([
            "oxide",
            "archive",
            "demo/input",
            "--skip-preprocessing",
            "--skip-compression",
        ])
        .expect("archive arguments should parse");

        match cli.command {
            Commands::Archive(args) => {
                assert!(args.skip_preprocessing);
                assert!(args.skip_compression);
            }
            _ => panic!("expected archive command"),
        }
    }

    #[test]
    fn archive_command_accepts_preprocessing_override_flags() {
        let cli = Cli::try_parse_from([
            "oxide",
            "archive",
            "demo/input",
            "--text-preprocessing",
            "bwt",
            "--image-preprocessing",
            "locoi",
        ])
        .expect("archive arguments should parse");

        match cli.command {
            Commands::Archive(args) => {
                assert_eq!(args.text_preprocessing, Some(TextPreprocessingArg::Bwt));
                assert_eq!(args.image_preprocessing, Some(ImagePreprocessingArg::Locoi));
            }
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

    #[test]
    fn extract_command_accepts_repeated_only_flags() {
        let cli = Cli::try_parse_from([
            "oxide",
            "extract",
            "demo/input.oxz",
            "--only",
            "nested",
            "--only",
            "assets/logo.png",
        ])
        .expect("extract arguments should parse");

        match cli.command {
            Commands::Extract(args) => assert_eq!(args.only, ["nested", "assets/logo.png"]),
            _ => panic!("expected extract command"),
        }
    }

    #[test]
    fn extract_command_accepts_repeated_only_regex_flags() {
        let cli = Cli::try_parse_from([
            "oxide",
            "extract",
            "demo/input.oxz",
            "--only-regex",
            ".*\\.png$",
            "--only-regex",
            "^docs/",
        ])
        .expect("extract arguments should parse");

        match cli.command {
            Commands::Extract(args) => assert_eq!(args.only_regex, [".*\\.png$", "^docs/"]),
            _ => panic!("expected extract command"),
        }
    }
}
