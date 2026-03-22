<p align="center">
  <img src="docs/public/logo.png" alt="Oxide logo" width="420" />
</p>

# Oxide

Oxide is a Rust archiver built around a custom `.oxz` container, a parallel archive pipeline, and a CLI focused on fast day-to-day archive workflows. The workspace includes the core engine, the `oxide` command-line tool, a documentation site, benchmark suites, and dataset helpers used to exercise the pipeline.

## Highlights

- Archives a single file or an entire directory tree into `.oxz`
- Extracts full archives or selected paths from directory archives
- Prints archive contents as a tree without unpacking them
- Uses parallel block processing with `lz4` or `zstd`
- Applies format-aware preprocessing for text, image, audio, and binary data
- Falls back to raw storage for many already-compressed formats
- Emits live progress, summaries, and optional detailed telemetry tables

## Workspace

- `crates/oxide-core` - archive format, pipeline, compression, preprocessing, telemetry, tests, and benches
- `crates/oxide-cli` - the `oxide` binary, CLI parsing, preset resolution, progress UI, reports, and tree rendering
- `crates/oxide-gui` - placeholder GUI crate
- `docs/` - VitePress documentation site
- `datasets/` - sample corpora used for experiments and benchmarking
- `scripts/` - helper scripts for dataset preparation
- `latex/` - reports and format/specification notes

## Installation

Build the CLI from the workspace:

```bash
cargo build --release -p oxide-cli
```

The binary will be available at `target/release/oxide`.

To install it into your Cargo bin directory:

```bash
cargo install --path crates/oxide-cli
```

## Quick Start

Create an archive. If `--output` is omitted, Oxide writes to `<input>.oxz`.

```bash
oxide archive path/to/input
```

Extract an archive. If `--output` is omitted, Oxide strips `.oxz` from the input name when possible.

```bash
oxide extract path/to/archive.oxz
```

Inspect an archive without extracting it:

```bash
oxide tree path/to/archive.oxz
```

Show help:

```bash
oxide --help
```

## CLI Overview

### `archive`

Archives a file or directory into an `.oxz` file.

```bash
oxide archive [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - destination archive path
- `--preset <NAME>` - archive profile from the preset file
- `--compression <lz4|zstd>` - override the compression algorithm
- `--zstd-level <1-22>` - explicit zstd level
- `--skip-preprocessing` - disable preprocessing entirely
- `--skip-compression` - store payloads without compression
- `--workers <N>` - compression worker count
- `--block-size <SIZE>` - target block size such as `64K`, `1M`, or `4M`
- `--telemetry-details` - print extended telemetry tables

Oxide also exposes lower-level throughput and buffering controls such as `--inflight-bytes`, `--pool-capacity`, `--stream-read-buffer`, `--producer-threads`, and `--writer-queue-blocks` for tuning large archive runs.

### `extract`

Restores an `.oxz` archive to a file or directory.

```bash
oxide extract [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - destination path
- `--only <PATH>` - extract a specific archive-relative path
- `--only-regex <REGEX>` - extract paths that match a regex
- `--workers <N>` - decode worker count
- `--stats-interval-ms <MS>` - progress update interval
- `--telemetry-details` - print extended telemetry tables

### `tree`

Prints a tree view of archive contents with sizes.

```bash
oxide tree <INPUT>
```

## Presets

Archive settings are loaded from [`crates/oxide-cli/presets.json`](/home/user/Rust/oxide/crates/oxide-cli/presets.json). The default preset is `balanced`.

| Preset | Compression | Preprocessing profile | Intended tradeoff |
| --- | --- | --- | --- |
| `fast` | `lz4` | none by default | highest throughput, lighter CPU usage |
| `balanced` | `zstd` level 6 | `bpe`, `ycocgr`, `lpc`, `bcj` | default profile for mixed data |
| `ultra` | `zstd` level 19 | `bwt`, `locoi`, `lpc`, `bcj` | stronger compression, higher CPU cost |

You can override any preset choice with CLI flags or provide a custom preset file with `--preset-file`.

## Pipeline Notes

The core engine is organized around block-based parallel processing:

- `FormatDetector` classifies input as text, image, audio, binary, or common data using signatures plus heuristics
- preprocessing is selected per file type, not applied blindly to every block
- directory archival builds a manifest that records paths, kinds, sizes, modes, timestamps, and uid/gid metadata
- many already-compressed formats such as `jpg`, `png`, `mp3`, `zip`, and `zst` are marked for raw storage instead of wasteful recompression
- extraction supports full restore, filtered restore, and archive inspection through the manifest reader

Supported preprocessing strategies currently exposed by the CLI:

| Data type | Strategies |
| --- | --- |
| Text | `bpe`, `bwt` |
| Image | `ycocgr`, `paeth`, `locoi` |
| Audio | `lpc` |
| Binary | `bcj` |

## Development

The repository CI currently builds and tests the workspace with Cargo.

```bash
cargo build
cargo test
cargo clippy --all-features
```

Run the CLI locally:

```bash
cargo run -p oxide-cli -- --help
```

## Documentation Site

The documentation site lives in [`docs/`](/home/user/Rust/oxide/docs) and uses VitePress.

```bash
cd docs
npm install
npm run docs:dev
```

Good entry points:

- [`docs/index.md`](/home/user/Rust/oxide/docs/index.md)
- [`docs/cli/index.md`](/home/user/Rust/oxide/docs/cli/index.md)
- [`docs/cli/archive.md`](/home/user/Rust/oxide/docs/cli/archive.md)
- [`docs/cli/extract.md`](/home/user/Rust/oxide/docs/cli/extract.md)
- [`docs/cli/presets.md`](/home/user/Rust/oxide/docs/cli/presets.md)
- [`docs/about/index.md`](/home/user/Rust/oxide/docs/about/index.md)

## Benchmarks And Test Coverage

The workspace includes:

- integration tests for archive, extraction, format, telemetry, preprocessing, and scheduling behavior in `crates/oxide-core/tests`
- Criterion benchmarks in `crates/oxide-core/benches` for throughput, memory management, scanner performance, work scheduling, and format detection

## Status

The core archive engine and CLI are the active parts of the project. The GUI crate currently contains only a placeholder binary.
