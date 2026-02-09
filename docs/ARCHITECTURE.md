# Oxide V4: Content-Aware Transformation & Archival Pipeline

## Executive Summary

Oxide is a high-throughput, content-aware archival system implemented in Rust. Unlike standard compressors (gzip/zstd) that blindly compress bytes, Oxide employs a **Filter Pipeline** approach: it identifies data types (Text, Binary, Audio, Image) and applies domain-specific **Preprocessing Transforms** (BCJ, BWT, LPC) to reduce entropy before handing data to general-purpose compressors (LZ4, LZMA, Deflate).

### Key Performance Features

- **Zero-Copy Input**: Memory-mapped files (mmap) with hybrid boundary detection (`memchr` + media metadata)
- **Boomerang Allocation**: Zero-allocation recycled buffer pool for pipeline stages
- **Smart Transformation**: Heuristic-based selection of preprocessing filters (e.g., x86 executable → BCJ → LZMA)
- **Elastic Parallelism**: Work-stealing thread pool for CPU-heavy BWT/LZMA operations

---

## 1. High-Level Architecture

The pipeline consists of three distinct layers:

### Layer 1: Input & Discovery

```
File System → [mmap + Hybrid Boundary Scan] → Input Scanner → Raw Batch → {Work Stealing Queue}
```

- Uses memory-mapped I/O for zero-copy file access
- Boundary detection by mode: text (`memchr`), image row alignment (`image`), audio frame alignment (`symphonia`), raw fallback
- Pushes batches to a work-stealing queue for dynamic load balancing

### Layer 2: The Smart Worker

```
Work Queue → Worker Thread
    ↓
Content Detection → Strategy Selection
    ↓
┌─────────────────┬─────────────────┬─────────────────┐
│  Is x86?        │  Is Text?       │  Is Audio?      │
│  Strategy: BCJ  │  Strategy: BWT  │  Strategy: LPC  │
└────────┬────────┴────────┬────────┴────────┬────────┘
         ↓                 ↓                 ↓
    Transformed Data → General Compressor → Compressed Block
         ↓
   [Recycle Buffer] → Back to Input Layer
```

Each worker performs:
1. **Content Detection**: Analyzes data to determine optimal preprocessing strategy
2. **Preprocessing Filter**: Applies domain-specific transformation (BCJ, BWT, LPC, etc.)
3. **Compression**: Uses selected algorithm (LZ4, LZMA, Deflate)
4. **Buffer Recycling**: Returns allocated buffers to the Boomerang pool

### Layer 3: Output

```
Compressed Blocks → Reordering Buffer → Sequential Write → Final Archive (.oxz)
```

- Maintains block ordering for reconstructability
- Batched writes for I/O efficiency
- Self-describing format with embedded metadata

---

## 2. Core Data Structures

### 2.1 The "Smart" Batch

```rust
pub struct Batch {
    pub id: usize,
    pub source_path: PathBuf,
    /// Zero-copy slice of memory-mapped file
    pub data: bytes::Bytes,
    pub file_type_hint: FileFormat,
}

pub struct CompressedBlock {
    pub id: usize,
    pub data: Vec<u8>,
    /// Metadata required for decompression
    pub pre_proc: PreProcessingStrategy,
    pub compression: CompressionAlgo,
    pub original_len: u64,
}
```

**Design Rationale:**
- `Bytes` for zero-copy slicing of mmap regions
- Strategy metadata enables correct decompression pipeline
- IDs maintain ordering across parallel processing

### 2.2 Preprocessing Strategy Enums

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreProcessingStrategy {
    Text(TextStrategy),     // BPE, BWT
    Image(ImageStrategy),   // YCoCgR, Paeth, LocoI
    Audio(AudioStrategy),   // LPC
    Binary(BinaryStrategy), // BCJ
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextStrategy {
    BPE,  // Byte Pair Encoding
    BWT,  // Burrows-Wheeler Transform
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageStrategy {
    YCoCgR,  // Color space transform
    Paeth,   // PNG prediction filter
    LocoI,   // JPEG-LS predictor
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioStrategy {
    LPC,  // Linear Predictive Coding
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryStrategy {
    BCJ,  // Branch Call Jump (x86 filter)
}
```

### 2.3 Compression Algorithm Selection

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgo {
    Lz4,              // Fast, low ratio
    Lzma,             // Slow, high ratio
    DeflateHuffman,   // Balanced
}
```

---

## 3. Detailed Stage Implementation

### 3.1 Input Layer: SIMD & Memory Mapping

**Optimization Goal**: Eliminate data copying and use hardware-accelerated scanning.

**Implementation Strategy:**

```rust
use memchr::memchr;
use memmap2::MmapOptions;

pub struct InputScanner {
    target_size: usize,
    boundary_mode: BoundaryMode,
}

impl InputScanner {
    fn process_file(&self, path: &Path, tx: Sender<Batch>) -> Result<()> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        let mut start = 0;
        let mut id = 0;

        while start < mmap.len() {
            let end = (start + self.target_size).min(mmap.len());
            
            // Hybrid boundary detection, chosen once per file.
            let actual_end = match self.boundary_mode {
                BoundaryMode::TextNewline => memchr(b'\n', &mmap[start..end]).unwrap_or(end),
                BoundaryMode::ImageRows { row_bytes } => align_down(end, row_bytes),
                BoundaryMode::AudioFrames { frame_bytes } => align_down(end, frame_bytes),
                BoundaryMode::Raw => end,
            };

            // Zero-copy slice via Bytes
            let chunk = Bytes::copy_from_slice(&mmap[start..actual_end]);
            
            tx.send(Batch { 
                id, 
                data: chunk, 
                file_type_hint: detect_format(path) 
            })?;

            start = actual_end + 1;
            id += 1;
        }
        Ok(())
    }
}
```

**Key Optimizations:**
1. **Memory Mapping**: Direct kernel-managed file access, no user-space buffers
2. **Hybrid Boundary Scanning**: `memchr` handles text, while `image` and `symphonia` provide media-safe alignment metadata
3. **Zero-Copy**: `Bytes` provides refcounted views into mmap regions

### 3.2 Worker Layer: The Filter Pipeline

**Responsibility**: Transform-then-compress with intelligent strategy selection.

**Processing Flow:**

```rust
impl Worker {
    fn process(&self, batch: Batch) -> Result<CompressedBlock> {
        // Step 1: Strategy Selection
        let strategy = self.select_strategy(&batch);

        // Step 2: Preprocessing (The Filter)
        let filtered_data = match &strategy {
            PreProcessingStrategy::Binary(BinaryStrategy::BCJ) => apply_bcj(&batch.data)?,
            PreProcessingStrategy::Text(TextStrategy::BWT) => apply_bwt(&batch.data)?,
            PreProcessingStrategy::Audio(AudioStrategy::LPC) => apply_lpc(&batch.data)?,
            _ => Cow::Borrowed(&batch.data),
        };

        // Step 3: Compression (fetch recycled buffer)
        let mut out_buffer = self.recycler.pop().unwrap_or_else(Vec::new);
        
        match self.compression_algo {
            CompressionAlgo::Lz4 => lz4_compress(&filtered_data, &mut out_buffer)?,
            CompressionAlgo::Lzma => lzma_compress(&filtered_data, &mut out_buffer)?,
            CompressionAlgo::DeflateHuffman => deflate_compress(&filtered_data, &mut out_buffer)?,
        };

        Ok(CompressedBlock {
            id: batch.id,
            data: out_buffer,
            pre_proc: strategy,
            compression: self.compression_algo,
            original_len: batch.data.len() as u64,
        })
    }
    
    fn select_strategy(&self, batch: &Batch) -> PreProcessingStrategy {
        match batch.file_type_hint {
            FileFormat::Binary if is_x86_executable(&batch.data) => 
                PreProcessingStrategy::Binary(BinaryStrategy::BCJ),
            FileFormat::Text if self.compression_algo == CompressionAlgo::Lzma => 
                PreProcessingStrategy::Text(TextStrategy::BWT),
            FileFormat::Audio if batch.boundary_hint.audio_frame_aligned =>
                PreProcessingStrategy::Audio(AudioStrategy::LPC),
            FileFormat::Image if batch.boundary_hint.image_row_aligned =>
                PreProcessingStrategy::Image(ImageStrategy::Paeth),
            _ => PreProcessingStrategy::None,
        }
    }
}
```

**Heuristics:**

| Data Type | Detection Method | Strategy | Rationale |
|-----------|------------------|----------|-----------|
| x86 Binary | 0x55 0x89 0xE5 prologue pattern | BCJ | Converts relative jumps to absolute, improving LZ matches |
| Text (High Entropy) | Repetitive patterns, DNA-like sequences | BWT | Groups similar characters for better compression |
| Audio | `infer` + `symphonia` metadata/packets | LPC (aligned only) | Removes temporal redundancy without splitting sample frames |
| Image | `infer` + `image` metadata/decoder path | Paeth/LocoI (aligned only) | Spatial prediction without splitting scanlines |

### 3.3 Output Layer: The .oxz Format

**Requirement**: Self-describing format enabling correct decompression without external metadata.

**File Structure:**

```
[Global Header - 16 bytes]
  - Magic: "OXZ\x00" (4 bytes)
  - Version: u16 (2 bytes)
  - Reserved: u16 (2 bytes)
  - Flags: u32 (4 bytes)
  - Block Count: u32 (4 bytes)

[Block 1 Header - 24 bytes]
  - ID: u64 (8 bytes)
  - Original Size: u32 (4 bytes)
  - Compressed Size: u32 (4 bytes)
  - Strategy Flags: u8 (1 byte)
    * Bits 0-2: Category (0=None, 1=Text, 2=Image, 3=Audio, 4=Binary)
    * Bits 3-5: Sub-strategy (e.g., BWT vs BPE)
    * Bits 6-7: Reserved
  - Compression Flags: u8 (1 byte)
    * 0x01 = LZ4
    * 0x02 = LZMA
    * 0x03 = Deflate
  - Reserved: u16 (2 bytes)
  - CRC32: u32 (4 bytes)

[Block 1 Data Payload - variable]
  - Raw compressed bytes

[Block 2 Header]...
[Block 2 Data]...
...
[Footer - 8 bytes]
  - End Magic: "END\x00" (4 bytes)
  - Global CRC32: u32 (4 bytes)
```

**Rationale for Format Choices:**
- Fixed-size headers enable random access to block metadata
- Per-block CRC32 enables integrity verification
- Strategy flags enable correct decompression without sidecar files
- Global footer provides whole-file integrity check

---

## 4. Optimization Strategies

### 4.1 The "Boomerang" Buffer System

**Problem**: Memory allocation/deallocation is expensive in tight loops.

**Solution**: Recycle buffers through a return channel.

```rust
// In Pipeline Setup
let (recycle_tx, recycle_rx) = crossbeam_channel::unbounded::<Vec<u8>>();

// In Writer (returns buffers to pool)
fn write_loop(
    rx: Receiver<CompressedBlock>, 
    recycle_tx: Sender<Vec<u8>>
) {
    while let Ok(block) = rx.recv() {
        file.write_all(&block.data)?;
        
        // Return buffer to pool for reuse
        // Preserves allocated capacity
        block.data.clear();
        recycle_tx.send(block.data).ok();
    }
}

// In Worker (acquires buffers from pool)
fn compress_data(&self, input: &[u8]) -> Vec<u8> {
    // Try to get recycled buffer first
    let mut buffer = self.recycler.try_recv()
        .unwrap_or_else(|_| Vec::with_capacity(input.len()));
    
    // Compress into buffer
    self.compressor.compress_into(input, &mut buffer);
    
    buffer
}
```

**Benefits:**
- Eliminates allocator pressure in steady-state operation
- Reduces page faults and memory fragmentation
- Enables predictable memory usage

### 4.2 Work Stealing (Load Balancing)

**Problem**: BWT and LZMA are 10-100x slower than LZ4. Static scheduling causes pipeline stalls.

**Solution**: Use `crossbeam-deque` for work stealing.

```rust
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};

pub struct WorkStealingPool {
    global_queue: Injector<Batch>,
    stealers: Vec<Stealer<Batch>>,
}

impl WorkStealingPool {
    fn worker_loop(&self, local_queue: DequeWorker<Batch>) {
        loop {
            // Try local queue first
            let batch = local_queue.pop()
                // Then try global injector
                .or_else(|| self.global_queue.steal().success())
                // Then steal from other workers
                .or_else(|| {
                    self.stealers.iter()
                        .map(|s| s.steal())
                        .find(|s| s.is_success())
                        .and_then(|s| s.success())
                });

            if let Some(batch) = batch {
                self.process(batch);
            } else {
                // No work available
                break;
            }
        }
    }
}
```

**Benefits:**
- Heavy tasks (BWT+LZMA) don't block light tasks (None+LZ4)
- Automatic load balancing without central coordination
- Near-linear scaling with core count

### 4.3 SIMD-Accelerated Scanning

**Implementation**: Use `memchr` crate for vectorized byte searching.

```rust
// AVX2-accelerated newline detection
let newline_pos = memchr(b'\n', &buffer);

// Multi-pattern search for format detection
let is_json = memchr3(b'{', b'[', b'"', &header).is_some();
```

**Performance**: ~10-20x faster than byte-by-byte scanning.

---

## 5. Preprocessing Algorithms

### 5.1 BCJ (Branch Call Jump Filter)

**Purpose**: Improve compression of x86 executables.

**Algorithm**:
```rust
/// Converts relative jumps to absolute addresses
fn bcj_transform(data: &[u8]) -> Vec<u8> {
    let mut output = data.to_vec();
    let mut i = 0;
    
    while i < data.len() - 5 {
        // Detect x86 CALL/JMP near instructions
        // E8/E9 xx xx xx xx (relative offset)
        if data[i] == 0xE8 || data[i] == 0xE9 {
            let offset = i32::from_le_bytes([
                data[i+1], data[i+2], data[i+3], data[i+4]
            ]);
            // Convert relative to absolute
            let absolute = (i as i32 + 5 + offset) as u32;
            output[i+1..i+5].copy_from_slice(&absolute.to_le_bytes());
        }
        i += 1;
    }
    
    output
}
```

**Benefit**: 10-30% better compression on executables.

### 5.2 BWT (Burrows-Wheeler Transform)

**Purpose**: Group similar characters together for better LZ compression.

**Algorithm**:
```rust
/// Suffix array based BWT
fn bwt_transform(data: &[u8]) -> (Vec<u8>, usize) {
    let n = data.len();
    let mut suffixes: Vec<usize> = (0..n).collect();
    
    // Sort suffixes
    suffixes.sort_by(|&a, &b| {
        data[a..].cmp(&data[b..])
    });
    
    // Build BWT string (last column)
    let mut bwt = Vec::with_capacity(n);
    let mut primary_index = 0;
    
    for (i, &suffix) in suffixes.iter().enumerate() {
        if suffix == 0 {
            bwt.push(data[n-1]);
            primary_index = i;
        } else {
            bwt.push(data[suffix-1]);
        }
    }
    
    (bwt, primary_index)
}
```

**Note**: Requires O(n log n) time via suffix array. Worthwhile only for high-compression modes.

### 5.3 LPC (Linear Predictive Coding)

**Purpose**: Remove temporal redundancy in audio samples.

**Algorithm**:
```rust
/// First-order LPC: predict sample from previous
fn lpc_transform(samples: &[i16]) -> Vec<i16> {
    let mut residuals = Vec::with_capacity(samples.len());
    let mut prev = 0i16;
    
    for &sample in samples {
        let predicted = prev; // Simple predictor
        let residual = sample.wrapping_sub(predicted);
        residuals.push(residual);
        prev = sample;
    }
    
    residuals
}
```

**Benefit**: 20-50% size reduction on uncompressed audio.

### 5.4 Paeth Filter (Image Prediction)

**Purpose**: Spatial prediction for raw images.

**Algorithm** (from PNG specification):
```rust
fn paeth_predictor(a: u8, b: u8, c: u8) -> u8 {
    let pa = (b as i16 - c as i16).abs();
    let pb = (a as i16 - c as i16).abs();
    let pc = ((a as i16 + b as i16 - 2 * c as i16)).abs();
    
    if pa <= pb && pa <= pc { a }
    else if pb <= pc { b }
    else { c }
}
```

**Benefit**: Removes spatial redundancy in natural images.

---

## 6. Compression Algorithms

### 6.1 LZ4

**Use Case**: Speed-critical applications (real-time streaming, hot data).

**Characteristics**:
- Compression: ~400 MB/s
- Decompression: ~1-2 GB/s
- Ratio: ~2:1
- Algorithm: LZ77 variant with simplified matching

### 6.2 LZMA

**Use Case**: Maximum compression (archival storage, cold data).

**Characteristics**:
- Compression: ~1-5 MB/s (depends on level)
- Decompression: ~20-50 MB/s
- Ratio: ~4-6:1
- Algorithm: LZ77 + Range Coder + Large Dictionary

### 6.3 Deflate

**Use Case**: Compatibility (gzip replacement, web assets).

**Characteristics**:
- Compression: ~10-50 MB/s
- Decompression: ~100-200 MB/s
- Ratio: ~3:1
- Algorithm: LZ77 + Huffman Coding

---

## 7. Implementation Roadmap

### Phase 1: Pipeline Skeleton (Week 1)

**Goal**: End-to-end pipeline with zero-allocation hot path.

**Tasks**:
- [ ] Implement mmap input reading with `memmap2`
- [ ] Implement Boomerang buffer recycling system
- [ ] Implement work-stealing queue with `crossbeam-deque`
- [ ] Create Writer with `.oxz` format support
- [ ] **Deliverable**: Raw bytes from disk → output with zero allocations

### Phase 2: Filter Logic (Week 2)

**Goal**: Smart preprocessing with strategy selection.

**Tasks**:
- [ ] Define `Preprocessor` trait
- [ ] Implement BCJ filter for x86 binaries
- [ ] Implement BWT using suffix array library
- [ ] Implement Paeth filter for images
- [ ] Implement LPC for audio
- [ ] Wire up `PreProcessingStrategy` dispatch
- [ ] **Deliverable**: Automatic strategy selection working

### Phase 3: Compression & Heuristics (Week 3)

**Goal**: Full compression pipeline with intelligent defaults.

**Tasks**:
- [ ] Integrate `lz4_flex` for LZ4 compression
- [ ] Integrate `lzma-rs` for LZMA compression
- [ ] Integrate `flate2` for Deflate
- [ ] Implement content detection heuristics
- [ ] Finalize `.oxz` binary format with all flags
- [ ] **Deliverable**: Production-ready archiver matching 7-Zip compression ratios

---

## 8. Dependencies

```toml
[dependencies]
# Core parallelism
crossbeam-channel = "0.5"
crossbeam-deque = "0.8"
num_cpus = "1.16"

# Zero-copy I/O
memmap2 = "0.9"
bytes = "1.5"
memchr = "2.7"
image = "0.25"
symphonia = { version = "0.5", features = ["all"] }

# Compression
lz4_flex = "0.11"
lzma-rs = "0.3"
flate2 = "1.0"

# Serialization
serde = { version = "1.0", features = ["derive"] }

# Error handling
thiserror = "1.0"
anyhow = "1.0"

# Utilities
crc32fast = "1.3"
byteorder = "1.5"
```

---

## 9. Performance Targets

### Compression Speed

| Mode | Target Speed | Ratio | Use Case |
|------|-------------|-------|----------|
| Fast (LZ4) | 400 MB/s | 2:1 | Streaming, hot data |
| Balanced (Deflate) | 50 MB/s | 3:1 | General purpose |
| Ultra (LZMA) | 5 MB/s | 5:1 | Archival storage |

### Memory Usage

- **Input Buffering**: Zero-copy via mmap
- **Worker Memory**: ~64MB per worker (recycled buffers)
- **Peak Memory**: Input file size + 10% overhead

### Scalability

- **Single-threaded**: Baseline performance
- **Multi-threaded**: Linear scaling to 8-16 cores
- **Work-stealing**: Efficient even with mixed workloads

---

## 10. Conclusion

Oxide V4 transforms a simple batch compressor into an industrial-grade archival system. By combining:

1. **Content-aware preprocessing** (BCJ, BWT, LPC with boundary gating)
2. **Zero-copy I/O** (mmap, Bytes)
3. **Smart resource management** (Boomerang buffers, work-stealing)
4. **Self-describing format** (.oxz with embedded metadata)

Oxide achieves compression ratios comparable to 7-Zip and ZPAQ while maintaining the performance and memory efficiency expected of modern Rust systems.

**Version**: 4.0 (Smart Transformation Pipeline)
**Last Updated**: 2026-02-06
