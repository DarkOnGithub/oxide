# Oxide v1 Rework Plan

This document defines a major redesign of Oxide while **staying on format version v1**.

Important decision:
- We keep the version as `v1` and are willing to change v1 layout/semantics directly.
- Backward compatibility with existing archives is not a goal for this rework.

The redesign is split into two independent tracks:
- Part A: compression-ratio architecture
- Part B: runtime-speed architecture

Both tracks remain strictly lossless.

## Core Objectives

- Improve compression ratio on mixed corpora (text, source trees, binaries, media).
- Improve compression/decompression throughput and tail latency.
- Reduce memory spikes during archive and extract.
- Make codec/transform strategy adaptive per chunk instead of fixed per archive.
- Keep behavior observable, testable, and deterministic.

## Non-Goals

- No lossy compression.
- No timeline commitments in this document.
- No compatibility shim for pre-rework v1 archives.
- No preprocessing implementation in this rework document (preprocessing remains disabled/out of scope).

## Known Pain Points in Current Design

- Codec preset is mostly metadata; it does not strongly drive codec internals.
- Block autotuning primarily ranks throughput, then output size.
- Several preprocessing paths are placeholder-level.
- Extract path can retain a large decoded block set before final output merge.
- Hot-path debug printing exists in processing path.
- Fixed-block model leaves ratio on the table for cross-boundary redundancy.

---

## Part A - Compression Rework (Ratio-First)

### A1. Redefine v1 container as sectioned metadata + chunk descriptors

Status: Done.

We keep `v1`, but redefine internal layout to be extensible:

- Header fields include:
  - magic
  - version (`1`)
  - feature bits
  - section count
  - section table offset
- Section types:
  - chunk index
  - dictionary store
  - entropy table store
  - transform chain table
  - optional dedup reference table
- Per-chunk descriptor includes:
  - `chunk_id`, `stream_id`
  - `raw_len`, `encoded_len`, checksum
  - `codec_id`, `preset`
  - `transform_chain_id`
  - `dict_id`
  - `entropy_mode`
  - optional `ref_chunk_id` (dedup/delta)
  - flags (raw passthrough, independent, etc.)

Why this helps:
- Enables adaptive strategy at chunk granularity without another format jump.
- Makes ratio features composable instead of hard-coded by one global mode.

Expected impact:
- Direct ratio impact is small alone; enables large gains from A2-A8.

Risk:
- High parser/writer complexity.

### A2. Hybrid chunking: superchunks + adaptive chunk boundaries

Current fixed blocking is replaced by:
- Superchunks (for example, 8 MiB to 32 MiB planning windows).
- Adaptive cut policy per superchunk:
  - fixed size for fast mode
  - content-defined chunking (CDC) for balanced/max-ratio modes
  - hard boundaries where file-format segmentation is important

Why this helps:
- Better boundary placement preserves repeated patterns that fixed block cuts miss.

Expected impact:
- Ratio: +4% to +15% on repos/logs/snapshots.
- Speed: neutral to negative depending CDC aggressiveness.

Risk:
- Medium-high due to tuning complexity and worst-case chunk count controls.

### A3. Multi-tier dictionary strategy

Add three dictionary scopes:
- Global dictionary per archive.
- Type dictionaries (text/code/binary/image/audio).
- Optional rolling stream dictionary for local continuity.

Planner assigns `dict_id` per chunk.

Why this helps:
- Small and moderately repetitive chunks gain most from dictionary matching.

Expected impact:
- Ratio: +6% to +30% on small-file and repetitive datasets.
- Speed: encode can slow in high-ratio modes; decode usually near-neutral.

Risk:
- High (dictionary lifecycle, caching, and validation).

### A4. Per-chunk codec/preset selection

Replace archive-wide codec lock with chunk-level policy:
- Candidate codecs: LZ4-like, Deflate-like, LZMA-like (and future additions).
- Candidate presets: fast/default/high.
- Planner does micro-trials on sampled bytes, then commits deterministic choice.

Why this helps:
- Mixed datasets are not served well by a single codec globally.

Expected impact:
- Ratio: +3% to +15% vs static global codec.
- Speed: can also improve by routing incompressible chunks to fast/raw paths.

Risk:
- Medium (planner quality and compute budget).

### A5. Entropy coding rework

Unify token stream model and support multiple entropy backends:
- Fast: table-driven Huffman.
- Balanced: ANS/rANS.
- Max-ratio: range-coder style path.

Store entropy tables by ID and reference from chunk descriptors.

Why this helps:
- Better entropy modeling is a direct ratio lever and can improve decode speed with table paths.

Expected impact:
- Ratio: +3% to +12%.
- Decode speed: +10% to +35% on entropy-heavy streams (backend-dependent).

Risk:
- High due to correctness sensitivity in bitstream coding.

### A6. Preprocessing/transforms are intentionally not implemented here

For this document and rework scope:
- Keep preprocessing/transform execution disabled.
- Keep transform metadata paths optional and unused.
- Do not include transform algorithms (text/image/audio/binary) in acceptance criteria.

Why this helps:
- Reduces scope and execution risk while focusing on core codec, container, scheduling, and I/O gains.

Expected impact:
- Ratio potential from transforms is deferred.
- Performance work remains cleaner to validate because fewer interacting stages are active.

Risk:
- Lower implementation risk now, but leaves ratio gains on type-specific data unrealized until a later phase.

### A7. Dedup + bounded delta references (optional mode)

Add a reference lane:
- Exact dedup by chunk hash.
- Near-duplicate delta encoding with bounded chain depth.

Why this helps:
- Huge wins on backups/versioned trees/build artifacts.

Expected impact:
- Effective storage reduction: +15% to +80% on dedup-friendly corpora.

Tradeoff:
- Adds decode indirection and random access complexity.

Risk:
- Very high; requires strict validation and chain limits.

### A8. Objective-based planner and mode governance

Planner uses weighted objective instead of a single metric:

`score = w_ratio * ratio_gain + w_speed * throughput - w_cpu * cpu_cost - w_mem * mem_cost`

Modes define weight profiles:
- `fast`
- `balanced`
- `max_ratio`

Why this helps:
- Prevents hidden regressions where one metric improves by sacrificing everything else.

---

## Part B - Speed Rework (Throughput-First)

### B1. Staged streaming DAG pipeline

Replace monolithic flow with bounded stages:
- read
- classify/probe
- chunk
- transform
- match/tokenize
- entropy encode/decode
- ordered write

Each stage uses bounded queues and explicit backpressure.

Why this helps:
- Increases overlap and prevents one slow phase from stalling the entire pipeline.

Expected impact:
- Throughput: +20% to +60%.
- Tail latency: significant p95/p99 reductions.

Risk:
- High due to orchestration complexity.

### B2. Cost-aware scheduler

Dispatch by estimated work cost (bytes + codec complexity), not simple FIFO:
- Separate pools for I/O, transforms, codec heavy work.
- Dynamic rebalancing from queue depth and stall metrics.

Why this helps:
- Avoids worker underutilization and head-of-line blocking.

Expected impact:
- Throughput: +15% to +45% on mixed workloads.

Risk:
- Medium-high.

### B3. Worker-local persistent scratch memory

Introduce per-thread reusable arenas for:
- match finder tables
- token vectors
- entropy scratch buffers
- temporary transform buffers

Why this helps:
- Reduces allocator churn and improves cache locality.

Expected impact:
- Speed: +10% to +35%.
- Memory stability: fewer spikes and fragmentation.

Risk:
- Medium.

### B4. SIMD and CPU feature dispatch

Runtime CPU dispatch for optimized kernels:
- match length scanning
- literal histogramming
- checksum
- copy/overlap paths

Why this helps:
- Hot loops become vectorized for modern CPUs.

Expected impact:
- Speed: +20% to +70% in hotspot kernels.

Risk:
- Medium-high due to cross-platform QA surface.

### B5. Decoder fast paths

Optimize decode critical path:
- Table-based entropy decode frontends.
- Raw passthrough bypass.
- Fast copy kernels for common match-distance patterns.
- Minimize per-symbol branching.

Why this helps:
- Decode is often bottlenecked by branch-heavy bit parsing.

Expected impact:
- Decode speed: +25% to +80% depending codec/data.

Risk:
- High correctness risk without strong tests/fuzzing.

### B6. Extraction memory model rewrite

Avoid materializing all decoded blocks in memory:
- Keep bounded reorder window.
- Emit contiguous ready chunk ranges directly to writer.
- Optional spill strategy if writer becomes bottleneck.

Why this helps:
- Prevents high RSS and improves stability on large archives.

Expected impact:
- Peak memory: -40% to -80%.
- End-to-end speed: +5% to +20% from reduced memory pressure.

Risk:
- Medium-high (ordering correctness and backpressure interaction).

### B7. I/O subsystem modernization

Pluggable backend per platform profile:
- sync std I/O baseline
- mmap path for suitable files
- optional async/uring path
- read-ahead/write-behind windows

Why this helps:
- Keeps CPU stages fed and smooths writer stalls.

Expected impact:
- Wall-clock throughput: +15% to +50% on fast storage.

Risk:
- Medium (OS-specific behavior, tuning variability).

### B8. Observability and control plane

Add always-on low-overhead metrics:
- per-stage throughput/latency
- queue depths and wait times
- bytes in flight
- worker utilization
- planner decisions and fallback reasons

Why this helps:
- Necessary to tune adaptive strategy and prevent regressions.

Expected impact:
- Indirect but essential for sustained performance evolution.

---

## v1 Layout (Redefined) - Conceptual

Header (v1)
- magic
- version = 1
- feature bits
- section_count
- section_table_offset

Section table (v1)
- entry: type, offset, length, checksum

Core sections
- chunk_index
- dictionary_store
- entropy_store
- transform_store
- payload_region

Chunk descriptor (v1)
- chunk id / stream id
- raw length / encoded length
- checksum
- codec / preset / entropy mode
- transform chain id
- dict id
- optional ref id
- flags

This keeps a single format version while enabling adaptive internals.

---

## Testing and Acceptance Gates

### Correctness
- Bit-identical round-trip for all regression corpora.
- Deterministic output for same input + same config.
- Strict malformed-input failure (no panic/UB).

### Performance
- Track ratio, encode MB/s, decode MB/s, p95/p99 latency, CPU%, peak RSS.
- Compare `fast`, `balanced`, `max_ratio` modes on representative corpora.
- Reject regressions outside agreed noise bands.

### Robustness
- Parser/decoder fuzzing (structure-aware and random).
- Differential tests between legacy path and reworked path where semantics overlap.
- Corruption injection tests (truncation, invalid tables, invalid references).

---

## Implementation Priorities (No Timeline)

1. [x] Redefine v1 container internals (section table + chunk descriptors).
2. [x] Introduce streaming DAG and bounded reorder writer path.
3. Add worker-local scratch arenas and remove hot-path debug output.
4. Implement decode fast paths (table entropy + copy kernels).
5. Add adaptive chunking and planner objective framework.
6. Wire dictionaries and per-chunk codec/preset selection.
7. Keep preprocessing/transform execution out of scope for this rework.
8. Add optional dedup/delta lane with strict safety limits.
9. Add SIMD dispatch and platform-specific kernel optimization.
10. Enforce hard correctness/performance gates for all modes.

---

## Summary

This rework keeps the project on **v1** while making v1 internally modern and adaptive. The ratio track and speed track can be developed independently, but the largest gains come from combining:
- adaptive chunk strategy + dictionaries + modern entropy,
- streaming pipeline + cost-aware scheduling + decode/memory fast paths.

The result is a lossless system that can target both high-throughput and high-ratio workloads through explicit mode policy instead of one-size-fits-all behavior.
