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
fn archive_command_accepts_lzma_compression() {
    let cli = Cli::try_parse_from(["oxide", "archive", "demo/input", "--compression", "lzma"])
        .expect("archive arguments should parse");

    match cli.command {
        Commands::Archive(args) => {
            assert!(matches!(
                args.compression,
                Some(super::CompressionArg::Lzma)
            ));
        }
        _ => panic!("expected archive command"),
    }
}

#[test]
fn archive_command_accepts_compression_level_flag() {
    let cli = Cli::try_parse_from([
        "oxide",
        "archive",
        "demo/input",
        "--compression-level",
        "19",
    ])
    .expect("archive arguments should parse");

    match cli.command {
        Commands::Archive(args) => assert_eq!(args.compression_level, Some(19)),
        _ => panic!("expected archive command"),
    }
}

#[test]
fn archive_command_accepts_skip_compression_flag() {
    let cli = Cli::try_parse_from(["oxide", "archive", "demo/input", "--skip-compression"])
        .expect("archive arguments should parse");

    match cli.command {
        Commands::Archive(args) => assert!(args.skip_compression),
        _ => panic!("expected archive command"),
    }
}

#[test]
fn extract_command_accepts_telemetry_flag() {
    let cli = Cli::try_parse_from(["oxide", "extract", "demo/input.oxz", "--telemetry-details"])
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
