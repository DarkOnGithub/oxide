<p align="center">
  <img src="docs/public/logo.png" alt="Oxide logo" width="420" />
</p>

# Oxide

Oxide is a Rust archiver built around the custom `.oxz` container, a parallel block pipeline, and a CLI for fast archive, extraction, encryption, and recovery workflows. The workspace includes the core engine, the `oxide` command-line tool, a GUI crate, documentation, benchmark material, and dataset helpers.

## Highlights

- Archives a single file or a complete directory tree into `.oxz`
- Extracts full archives or selected paths from directory archives
- Prints archive contents as a tree without unpacking them
- Supports `lz4`, `zstd`, and `lzma` compression
- Provides `fast`, `balanced`, and `ultra` presets for throughput, mixed workloads, and stronger compression
- Supports optional password encryption through `--encrypt`, plus standalone `encrypt` and `decrypt` commands
- Supports Reed-Solomon recovery data through `--recovery`, `protect`, and `repair`
- Uses raw storage for many already-compressed or incompressible blocks to avoid wasting CPU and growing archives
- Emits live progress, final summaries, and optional detailed telemetry tables

## Workspace

- `crates/oxide-core` - archive format, compression, pipeline, extraction, telemetry, recovery, tests, and benches
- `crates/oxide-cli` - the `oxide` binary, CLI parsing, preset resolution, progress UI, reports, and tree rendering
- `crates/oxide-gui` - GUI application crate
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

Create an archive with a preset:

```bash
oxide archive --preset fast path/to/input
oxide archive --preset balanced path/to/input
oxide archive --preset ultra path/to/input
```

Extract an archive. If `--output` is omitted, Oxide strips `.oxz` from the input name when possible.

```bash
oxide extract path/to/archive.oxz
```

Inspect an archive without extracting it:

```bash
oxide tree path/to/archive.oxz
```

Encrypt an archive at creation time:

```bash
oxide archive --encrypt path/to/input
```

Add recovery data while archiving:

```bash
oxide archive --recovery 5 path/to/input
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
- `--preset-file <PATH>` - custom preset config file
- `--compression <lz4|lzma|zstd>` - override the compression algorithm
- `--compression-level <N>` - explicit codec-specific compression level
- `--skip-compression` - store payloads without compression
- `--encrypt` - encrypt the created archive; `OXIDE_PASSWORD` can provide the password non-interactively
- `--recovery <1-20>` - add Reed-Solomon recovery data during archive creation
- `--dictionary-from <PATH>` - reuse an archive dictionary bank from an existing `.oxz`
- `--chunking <fixed|cdc>` - choose fixed-size chunking or content-defined chunking
- `--block-size <SIZE>` - target block size such as `64K`, `1M`, or `4M`
- `--min-block-size <SIZE>` - minimum block size for CDC chunking
- `--max-block-size <SIZE>` - maximum block size for CDC chunking
- `--workers <N>` - compression worker count; `0` means automatic
- `--telemetry-details` - print extended telemetry tables

Lower-level throughput and buffering controls are also available: `--inflight-bytes`, `--inflight-blocks-per-worker`, `--pool-capacity`, `--pool-buffers`, `--stream-read-buffer`, `--producer-threads`, `--directory-mmap-threshold`, `--writer-queue-blocks`, `--result-wait-ms`, and `--stats-interval-ms`.

### `extract`

Restores an `.oxz` archive to a file or directory.

```bash
oxide extract [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - destination path
- `--only <PATH>` - extract a specific archive-relative path; repeatable
- `--only-regex <REGEX>` - extract paths that match a regex; repeatable
- `--preset <NAME>` - decode tuning preset from the preset file
- `--preset-file <PATH>` - custom preset config file
- `--workers <N>` - decode worker count; `0` means automatic
- `--extract-write-shards <N>` - directory extraction write shards; `0` means adaptive auto, `1` disables sharding
- `--stats-interval-ms <MS>` - progress update interval
- `--telemetry-details` - print extended telemetry tables

### `tree`

Prints a tree view of archive contents with sizes.

```bash
oxide tree <INPUT>
```

### `encrypt`

Encrypts an existing `.oxz` archive. Without `--output`, Oxide safely rewrites the input archive through a temporary file.

```bash
oxide encrypt [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - write the encrypted archive to a separate path

### `decrypt`

Decrypts an encrypted `.oxz` archive. Without `--output`, Oxide safely rewrites the input archive through a temporary file.

```bash
oxide decrypt [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - write the decrypted archive to a separate path

### `protect`

Adds Reed-Solomon recovery data to an existing archive.

```bash
oxide protect [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - write the protected archive to a separate path
- `--recovery <1-20>` - recovery data percentage; defaults to `5`

### `repair`

Repairs a corrupted archive using embedded recovery data.

```bash
oxide repair [OPTIONS] <INPUT>
```

Useful options:

- `-o, --output <PATH>` - destination path for the repaired archive

## Presets

Archive settings are loaded from [`crates/oxide-cli/presets.json`](/home/user/Rust/oxide/crates/oxide-cli/presets.json). The default preset is `balanced`.

| Preset | Compression | Block size | Dictionary | Intended tradeoff |
| --- | --- | ---: | --- | --- |
| `fast` | `lz4` | `3M` | off | highest throughput and low codec cost |
| `balanced` | `zstd` level 2 | `2M` | off | default profile for mixed data, with stronger ratio than `fast` |
| `ultra` | `lzma` level 7 | `3M` | off | stronger compression with higher CPU cost |

All presets keep fixed chunking by default. The CLI can still override the preset with `--compression`, `--compression-level`, `--block-size`, `--chunking`, `--workers`, or a custom `--preset-file`.

## Engine Improvements

The current `.oxz` format uses a compact header, sequential payload blocks, a manifest, a compact block table, and a footer that stores the final section offsets and block counts. This layout keeps archive writing sequential: Oxide can stream blocks first, then append the metadata needed to reopen and extract the archive.

Block descriptors are compact because payload offsets are reconstructed from encoded block sizes instead of being stored for every block. The descriptor keeps the encoded size, original size, checksum or deduplication reference, and flags for compression, raw storage, and dictionary behavior.

Compression now uses three codec paths: `lz4` for `fast`, `zstd` for `balanced`, and `lzma` for `ultra`. The pipeline can skip compression for known already-compressed formats, store a block raw when compression would grow it, and use entropy checks to avoid expensive work on data that is likely incompressible.

Archiving is organized as a bounded producer, worker, and ordered-writer pipeline. Directory input can use prefetching, memory mapping for larger files, reusable per-worker scratch buffers, backpressure between stages, and a stable output order even when blocks finish out of order.

Extraction uses parallel block decode, preallocated output buffers based on metadata, worker-local scratch space, filtered restoration, and optional write sharding for directory restores. This keeps decode and file output from being serialized behind a single slow block or destination file.

## Benchmarks And Test Coverage

The workspace includes integration tests for archive, extraction, telemetry, scheduling, archive format behavior, encryption, and recovery, plus Criterion benchmarks for memory management, scanner performance, and work scheduling.

### Benchmark Context

The benchmark data below was measured on a Ryzen 9 7950X with 32 logical threads. The mixed dataset is approximately 5.7 GB and combines Silesia, `enwik9`, Linux source code, DIV2K images, and NYC Taxi Parquet data. `Peak RSS` is the peak resident memory used by the process.

The archive score combines throughput, compression ratio, and memory:

```text
S_archive = 100
  * (MiB/s / max_MiB/s)^0.60
  * (min_ratio / ratio)^0.30
  * (log(1 + min_peakRSS) / log(1 + peakRSS))^0.10
```

The extraction score uses throughput and memory because output size is fixed:

```text
S_extract = 100
  * (MiB/s / max_MiB/s)^0.90
  * (log(1 + min_peakRSS) / log(1 + peakRSS))^0.10
```

Higher scores indicate a better combined result.

### Archiving, `fast`

| Tool | Mode | Avg s | MiB/s | Ratio | Peak RSS | Score |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| **oxide** | **fast** | **4.000** | **1463.1** | 0.727 | 2.4 GB | **94.1** |
| squashfs | fast | 4.930 | 1187.2 | 0.727 | 1.4 GB | 83.2 |
| tar+zstd | fast | 5.173 | 1131.4 | **0.672** | **105.5 MB** | 83.9 |

### Archiving, `balanced`

| Tool | Mode | Avg s | MiB/s | Ratio | Peak RSS | Score |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| **oxide** | **balanced** | **4.485** | **1305.2** | 0.665 | 3.0 GB | **90.2** |
| squashfs | balanced | 4.815 | 1215.5 | 0.666 | 1.4 GB | 86.7 |
| tar+zstd | balanced | 5.238 | 1117.5 | **0.659** | **428.7 MB** | 83.2 |

### Archiving, `ultra`

| Tool | Mode | Avg s | MiB/s | Ratio | Peak RSS | Score |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| **oxide** | **ultra** | **37.560** | **155.8** | 0.637 | 4.5 GB | **25.5** |
| 7zip | ultra | 89.904 | 65.1 | 0.629 | **2.7 GB** | 15.2 |
| dwarfs | ultra | 88.414 | 66.2 | 0.634 | 10.1 GB | 15.2 |
| pixz | ultra | 198.498 | 29.5 | **0.626** | 29.5 GB | 9.4 |

### Extraction, `fast`

| Tool | Mode | Avg s | MiB/s | Peak RSS | Score |
| --- | --- | ---: | ---: | ---: | ---: |
| **oxide** | **fast** | **2.993** | **1955.6** | 2.6 GB | **94.2** |
| squashfs | fast | 3.415 | 1714.1 | 589.1 MB | 84.2 |
| tar+zstd | fast | 4.175 | 1402.1 | **11.6 MB** | 71.8 |

### Extraction, `balanced`

| Tool | Mode | Avg s | MiB/s | Peak RSS | Score |
| --- | --- | ---: | ---: | ---: | ---: |
| **oxide** | **balanced** | **2.891** | **2024.6** | 2.6 GB | **97.2** |
| squashfs | balanced | 3.247 | 1803.0 | 591.2 MB | 88.2 |
| tar+zstd | balanced | 4.216 | 1388.3 | **13.2 MB** | 71.2 |

### Extraction, `ultra`

| Tool | Mode | Avg s | MiB/s | Peak RSS | Score |
| --- | --- | ---: | ---: | ---: | ---: |
| **oxide** | **ultra** | **3.196** | **1831.4** | 2.0 GB | **88.9** |
| 7zip | ultra | 10.723 | 545.9 | 2.2 GB | 29.9 |
| dwarfs | ultra | 4.197 | 1394.7 | **2.0 GB** | 69.6 |
| pixz | ultra | 7.346 | 796.8 | 9.8 GB | 41.7 |

Oxide prioritizes throughput. In archiving, it sometimes accepts a slightly weaker ratio and higher memory use to reduce total processing time. In extraction, the same choice is clearer: Oxide keeps the best throughput across the three tested modes.
