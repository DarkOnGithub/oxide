# Oxide Pipeline Detailed Documentation

## Scope

This document describes the **current implemented pipeline** in this repository, from CLI entrypoint through scan, scheduling, processing, archive write, and extraction.

It focuses on code behavior in:

- `crates/oxide-cli/src/main.rs`
- `crates/oxide-core/src/pipeline/archive.rs`
- `crates/oxide-core/src/io/scanner.rs`
- `crates/oxide-core/src/core/worker_pool.rs`
- `crates/oxide-core/src/core/work_stealing.rs`
- `crates/oxide-core/src/format/oxz/*`
- `crates/oxide-core/src/pipeline/directory.rs`
- `crates/oxide-core/src/buffer/pool.rs`

## Executive Summary

The pipeline is a block-oriented archival flow:

1. Discover input (`file` or `directory`).
2. Convert input to ordered `Batch` units.
3. Submit batches to a parallel work-stealing worker pool.
4. Transform each batch into a `CompressedBlock` (currently pass-through payload with metadata).
5. Reorder by block id and write OXZ archive (global header, block headers + data, footer).
6. During extract, read OXZ, verify checksums, reconstruct payload, then restore either file bytes or directory tree.

The architecture already includes concurrency, queueing, metadata encoding, CRC validation, directory bundling, and progress/runtime telemetry.

## Pipeline Components

### 1. CLI Layer (`oxide-cli`)

CLI provides two subcommands:

- `archive`: archive a file or directory into `.oxz`
- `extract`: read `.oxz` and restore file or directory content

Key archive arguments:

- `--block-size`: scanner target block size
- `--workers`: worker thread count
- `--compression`: `lz4|lzma|deflate` (metadata recorded in blocks)
- `--pool-capacity`, `--pool-buffers`: buffer pool behavior
- `--stats-interval-ms`: progress report interval

Key extract argument:

- `--stats-interval-ms`: extraction progress report interval

The CLI wires runtime config into `ArchivePipeline::new(...)` and prints periodic progress snapshots and final summaries.

### 2. Core Orchestration (`ArchivePipeline`)

`ArchivePipeline` owns:

- `scanner: InputScanner`
- `num_workers: usize`
- `compression_algo: CompressionAlgo`
- `buffer_pool: Arc<BufferPool>`

Public flow methods:

- Archive:
  - `archive_file*`
  - `archive_path*`
  - `archive_directory*`
- Extract:
  - `extract_archive`
  - `extract_directory_archive`
  - `extract_path`

Internally there are two archive paths:

- **Prepared path** (`archive_prepared_with`): file batches or prebuilt directory bundle batches
- **Streaming directory path** (`archive_directory_streaming_with`): walks directory and streams bytes into block-sized batches while workers run

### 3. Data Model (`types.rs` + `pipeline/types.rs`)

`Batch`:

- `id`: strict ordering key
- `source_path`
- `data`: either `Owned(Bytes)` or `Mapped { map, start, end }`
- `file_type_hint`: `Text|Binary|Image|Audio|Common`

`CompressedBlock`:

- `id`: same ordering domain as `Batch.id`
- `data: Vec<u8>`
- `pre_proc: PreProcessingStrategy`
- `compression: CompressionAlgo`
- `original_len`
- `crc32` (computed from block data)

Archive runtime/reporting structs:

- `ArchiveProgressSnapshot`
- `ArchiveRunStats`
- `ArchiveOutcome<W>`

### 4. Input Scanner (`io/scanner.rs`)

#### 4.1 File Open and Format Probe

`InputScanner::scan_file`:

1. Opens file via `MmapInput::open`.
2. Reads probe prefix (`min(file_len, 64 KiB)`) for format detection.
3. Uses `FormatDetector::detect`:
   - library signature detection (`infer`)
   - fallback heuristics (`x86` prologue, text checks)
4. Chooses boundary mode:
   - `TextNewline`
   - `ImageRows { row_bytes }`
   - `AudioFrames { frame_bytes }`
   - `Raw`

#### 4.2 Boundary Logic

`find_block_boundary` targets near `start + target_block_size` and adjusts by mode:

- `TextNewline`: scans forward for `\n` within a bounded window to keep chunks near target size.
- `ImageRows`: aligns boundary to row byte stride.
- `AudioFrames`: aligns boundary to frame byte stride.
- `Raw`: fixed-size split.

Safety fallback guarantees progress (`end > start`) to avoid infinite loops.

#### 4.3 Output

Scanner emits ordered `Vec<Batch>` with mapped slices (zero-copy data views).

### 5. Directory Encoding Path (`pipeline/directory.rs`)

Directories are represented as a custom payload bundle (`OXDB`) before entering OXZ blocking.

Bundle format:

- Magic: `OXDB`
- Version: `u16`
- Entry count: `u32`
- Repeated entries:
  - kind `0` for directory: `[kind][path_len][path]`
  - kind `1` for file: `[kind][path_len][path][file_size][file_bytes]`

Important behavior:

- Relative path canonicalization uses UTF-8, slash normalized.
- Decode/write uses `join_safe` to reject unsafe path components (`..`, absolute, prefix/root forms).
- Archive-level source type can be signaled with global header flags (`SOURCE_KIND_DIRECTORY_FLAG`).

Two ingestion modes exist:

- **Collected**: build complete bundle in memory then chunk to batches.
- **Streaming**: discover tree and stream encoded bundle bytes through `DirectoryBatchSubmitter` without building full payload first.

### 6. Worker Pool and Scheduling

### 6.1 Work-Stealing Queue

`WorkStealingQueue<T>` has:

- Global `Injector<T>` for submitted work
- One local LIFO deque per worker
- `Stealer` handles to steal from peers
- Atomic `pending` counter

Worker fetch order (`WorkStealingWorker::steal`):

1. local pop (cache-friendly)
2. steal batch+pop from global injector
3. steal batch+pop from peer queues

Each successful fetch decrements `pending`.

### 6.2 Worker Pool Lifecycle

`WorkerPool::spawn(processor)`:

1. Creates result channel.
2. Builds shared `WorkerPoolState`.
3. Acquires one worker handle per worker id.
4. Spawns threads running `run_worker_loop`.

`WorkerPoolHandle` API:

- `submit(batch)` increments `submitted`, pushes queue item
- `shutdown()` stops acceptance and requests graceful drain-exit
- `recv_timeout(...)` receives one processing result
- `runtime_snapshot()` reports submitted/completed/pending + per-worker utilization
- `join()` / `finish()` joins threads with panic handling

### 6.3 Current Processor Semantics

Current archive processor closure (in `ArchivePipeline`) is pass-through:

1. Acquire pooled scratch vector.
2. Copy batch bytes into scratch.
3. Move pooled vector out using `swap` into owned `Vec<u8>`.
4. Construct `CompressedBlock` with:
   - `pre_proc = None`
   - `compression = configured CompressionAlgo`
   - `original_len = batch.len()`

So compression metadata is preserved, but payload bytes are not transformed yet.

### 7. Buffer Pool

`BufferPool` is a bounded recycler:

- Internally uses bounded channel of `Vec<u8>`.
- `acquire()` returns recycled buffer when available, otherwise allocates with `default_capacity`.
- `PooledBuffer` returns vec on drop; if pool full/disconnected it is dropped.

This supports low-allocation steady-state behavior for high-throughput batch processing and archive write recycling.

### 8. Archive Writing (OXZ)

### 8.1 Writer Order and Constraints

`ArchiveWriter` writes:

1. Global header (`GlobalHeader`) once
2. Blocks in strict sequential block id order
3. Footer (`Footer`) once, only when all expected blocks were written

Enforced checks include:

- header written before blocks
- no duplicate header
- block id must match expected next id
- cannot exceed expected block count
- footer disallowed if reorder buffer still has pending blocks

### 8.2 Reordering and Finalization

Pipeline-level block collection currently sorts all blocks by `id` before writing.

`ArchiveWriter` also has `push_block(...)` with an internal reorder buffer, allowing future streaming out-of-order consumption.

### 8.3 OXZ Physical Layout

- Global header (`16 bytes`):
  - magic `OXZ\0`
  - version
  - reserved
  - flags
  - block count
- For each block:
  - block header (`24 bytes`):
    - `block_id`
    - `original_size`
    - `compressed_size`
    - `strategy_flags`
    - `compression_flags`
    - reserved
    - `crc32` of block data
  - block data bytes
- Footer (`8 bytes`):
  - magic `END\0`
  - `global_crc32` over all bytes up to footer start

### 8.4 Strategy/Compression Flag Encoding

`PreProcessingStrategy` and `CompressionAlgo` map to compact header flags with strict validation on decode.

Invalid/unknown flags produce `InvalidFormat` errors.

### 9. Archive Read and Validation

`ArchiveReader::new(reader)` performs:

1. Read+validate global header magic/version/reserved bits.
2. Build block offset index by scanning block headers.
3. Read footer at computed offset.
4. Compute CRC32 over archive content before footer and compare with footer CRC.
5. Seek to first block for iteration.

Per-block read (`read_block`):

- seeks to indexed offset
- reads block header + payload
- validates payload CRC against block header

`iter_blocks()` yields ordered `(BlockHeader, Vec<u8>)` results.

### 10. Extract Pipeline

At core level (`ArchivePipeline`):

- `read_archive_payload` iterates blocks and concatenates payload bytes.
- `extract_path` attempts directory decode first; if successful writes tree, else writes single file.

At CLI level (`extract_command`):

- Builds `ArchiveReader` and reports progress while iterating block stream.
- Tracks archive bytes processed and payload bytes emitted.
- Calls `restore_payload`:
  - decode+write directory bundle when present
  - otherwise output file write

### 11. Progress and Runtime Metrics

Archive progress emission includes:

- source kind
- elapsed
- input bytes total/completed
- blocks total/completed/pending
- runtime snapshot with per-worker metrics

Per-worker runtime includes:

- task count
- uptime/busy/idle durations
- utilization ratio

Internal telemetry hooks exist across mmap/scanner/format/buffer/worker subsystems.

### 12. Error Handling Model

Representative error classes:

- Input/path validation failures (`InvalidFormat`, fs/io errors)
- Header/flag/checksum validation failures during read
- Worker panic capture converted to `CompressionError`
- Join/channel premature close errors
- Overflow checks for block count, entry count, offsets, lengths
- Directory safety failures for unsafe path components

Archive flow captures first worker error while still draining expected result count, then returns error after worker shutdown/join.

### 13. End-to-End Archive Sequence

1. CLI parses args and creates `BufferPool` + `ArchivePipeline`.
2. Pipeline prepares input:
   - file -> scanner batches
   - directory -> bundle bytes (collected or streaming)
3. Pipeline creates `WorkerPool` and spawns worker threads.
4. Batches are submitted to queue.
5. Workers process each batch into `CompressedBlock` and send result.
6. Pipeline drains results, tracks progress/metrics, and captures first error.
7. After all submitted results are received:
   - joins workers
   - errors out if any worker result failed
8. Blocks sorted by id and written by `ArchiveWriter`.
9. Footer written and run stats returned.
10. CLI prints summary.

### 14. End-to-End Extract Sequence

1. CLI opens archive and constructs `ArchiveReader` (global validation done here).
2. Iterates blocks, validates per-block CRC, appends payload bytes.
3. Emits progress while consuming archive.
4. Restores payload as directory bundle or plain file.
5. Prints extraction summary.

### 15. Concurrency and Ordering Guarantees

- `Batch.id` is monotonic at generation time.
- Worker completion can be out-of-order.
- Final archive write enforces in-order blocks by id.
- `block_count` in global header must match written block count.
- Footer write requires zero pending reordered blocks and full count completion.

### 16. Current Limitations and Next Extension Points

Current implemented limitations:

- No real compression transform in worker path yet; payload is pass-through.
- Preprocessing strategies are encoded in format but only `None` is used in active pipeline.
- Extract path currently reconstructs concatenated payload bytes directly.

Ready extension points:

- Replace pass-through worker closure with real preprocess + compressor stages.
- Use `ArchiveWriter::push_block(...)` to stream out-of-order worker results directly to writer.
- Add per-block decompression and inverse preprocessing in extraction path when compression becomes active.
- Expand strategy selection logic from file hints to content-aware per-batch routing.

## Practical Mental Model

Think of the current system as:

- A production-grade **parallel archival transport** framework
- with robust block metadata/checksums/order guarantees
- and directory packaging support
- where the actual compression transform stage is scaffolded and ready for implementation.

That means all outer infrastructure (I/O, queues, format, integrity, stats) is already integrated and testable.
