# Oxide Implementation Roadmap

**Version:** 1.1  
**Last Updated:** 2026-02-06  
**Status:** In Progress

## Executive Summary

This roadmap details the complete implementation of the Oxide content-aware archival system. Based on the architecture specification in `docs/ARCHITECTURE.md`, we've organized the work into three major phases spanning approximately 3 weeks of development.

Updates in this revision: output reordering buffer, full OXZ flag/footers spec alignment, complete image preprocessing set (Paeth/YCoCgR/LocoI), and advanced content heuristics tuning.

**Current Status:** Phase 1 - Foundation (Completed)
- [x] Project structure and dependencies
- [x] Core type definitions (strategies, modes)
- [x] Core pipeline components

---

## Phase 1: Pipeline Foundation (Week 1)

**Goal:** End-to-end pipeline with zero-allocation hot path
**Target:** Raw bytes from disk → output with minimal allocations

### 1.1 Core Infrastructure

#### Task 1.1.1: Error Handling Framework
**Priority:** High  
**Dependencies:** None  
**Estimated Time:** 2 hours

```rust
// crates/oxide-core/src/error.rs
pub enum OxideError {
    Io(std::io::Error),
    InvalidFormat(&'static str),
    ChecksumMismatch { expected: u32, actual: u32 },
    CompressionError(String),
    DecompressionError(String),
    InvalidBlockId { expected: u64, actual: u64 },
}
```

**Deliverables:**
- [x] Define `OxideError` enum with all error variants
- [x] Implement `From` traits for std::io::Error, anyhow::Error
- [x] Add structured context to errors
- [x] Unit tests for error conversions

**Verification:** `cargo test error::`

---

#### Task 1.1.2: Result Type and Common Types
**Priority:** High  
**Dependencies:** 1.1.1  
**Estimated Time:** 1 hour

```rust
// crates/oxide-core/src/types.rs
pub type Result<T> = std::result::Result<T, OxideError>;

pub struct Batch {
    pub id: usize,
    pub source_path: PathBuf,
    pub data: bytes::Bytes,
    pub file_type_hint: FileFormat,
}

pub struct CompressedBlock {
    pub id: usize,
    pub data: Vec<u8>,
    pub pre_proc: PreProcessingStrategy,
    pub compression: CompressionAlgo,
    pub original_len: u64,
    pub crc32: u32,
}

pub enum FileFormat {
    Text,
    Binary,
    Image,
    Audio,
    Common,  // Recognized but not explicitly specialized
    Unknown,
}
```

**Deliverables:**
- [x] Define shared result type
- [x] Implement core data structures
- [x] Add constructors and basic methods
- [x] Unit tests for type safety

**Verification:** `cargo test types::`

---

#### Task 1.1.3: File Format Detection
**Priority:** High  
**Dependencies:** 1.1.2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/format/detector.rs
pub struct FormatDetector;

impl FormatDetector {
    pub fn detect(data: &[u8]) -> FileFormat {
        // Heuristic-based detection
        // - Check magic numbers
        // - Analyze byte frequency distribution
        // - Detect text vs binary patterns
    }
    
    fn is_text(data: &[u8]) -> bool {
        // Check for valid UTF-8 + printable chars
    }
    
    fn is_x86_executable(data: &[u8]) -> bool {
        // Check for ELF/PE headers
        // Check for x86 prologue patterns
    }
}
```

**Deliverables:**
- [x] Implement magic number detection
- [x] Implement text/binary heuristics
- [x] Implement x86 executable detection
- [x] Add audio/image header checks (coarse signatures + infer fallback)
- [x] Document heuristic thresholds and fallbacks
- [x] Benchmark detection speed

**Verification:**
- Unit tests with sample files
- Benchmark: >100 MB/s detection throughput

---

### 1.2 Memory Management

#### Task 1.2.1: Boomerang Buffer Pool
**Priority:** Critical  
**Dependencies:** 1.1.2  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/buffer/pool.rs
use crossbeam_channel::{bounded, Sender, Receiver};

pub struct BufferPool {
    recycler: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    default_capacity: usize,
    max_buffers: usize,
}

impl BufferPool {
    pub fn new(default_capacity: usize, max_buffers: usize) -> Self {
        let (tx, rx) = bounded(max_buffers);
        Self {
            recycler: tx,
            receiver: rx,
            default_capacity,
            max_buffers,
        }
    }
    
    pub fn acquire(&self) -> PooledBuffer {
        match self.receiver.try_recv() {
            Ok(mut buffer) => {
                buffer.clear();
                PooledBuffer::new(buffer, self.recycler.clone())
            }
            Err(_) => {
                PooledBuffer::new(
                    Vec::with_capacity(self.default_capacity),
                    self.recycler.clone()
                )
            }
        }
    }
}

pub struct PooledBuffer {
    buffer: Vec<u8>,
    recycler: Sender<Vec<u8>>,
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        let _ = self.recycler.try_send(buffer);
    }
}
```

**Deliverables:**
- [x] Implement buffer pool with bounded channel
- [x] Implement RAII-based buffer return (PooledBuffer)
- [x] Add metrics (buffers created, recycled, dropped)
- [x] Wire return path from output stage (writer) back to pool
- [x] Thread-safe implementation

**Verification:**
- Unit tests for acquire/recycle cycles
- Benchmark: <100ns overhead per operation
- Memory test: stable memory usage during steady-state

---

#### Task 1.2.2: Memory-Mapped Input
**Priority:** Critical  
**Dependencies:** 1.1.2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/io/mmap.rs
use memmap2::Mmap;
use bytes::Bytes;

pub struct MmapInput {
    mmap: Mmap,
    path: PathBuf,
}

impl MmapInput {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            mmap,
            path: path.to_path_buf(),
        })
    }
    
    pub fn len(&self) -> usize {
        self.mmap.len()
    }
    
    pub fn slice(&self, start: usize, end: usize) -> Bytes {
        Bytes::copy_from_slice(&self.mmap[start..end])
    }
    
    pub fn as_bytes(&self) -> Bytes {
        self.slice(0, self.len())
    }
}
```

**Deliverables:**
- [x] Memory-mapped file wrapper
- [x] Zero-copy slicing with Bytes (or document copy-based safety fallback)
- [x] Proper error handling for mmap failures
- [x] Support for large files (>4GB)

**Verification:**
- Unit tests for read operations
- Test with files of various sizes
- Benchmark: compare vs std::fs::read

---

### 1.3 Input Scanner

#### Task 1.3.1: Hybrid Boundary Detection
**Priority:** High  
**Dependencies:** 1.2.2  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/io/scanner.rs
use memchr::memchr;

enum BoundaryMode {
    TextNewline,
    ImageRows { row_bytes: usize },
    AudioFrames { frame_bytes: usize },
    Raw,
}

pub struct InputScanner {
    target_block_size: usize,
    format_detector: FormatDetector,
}

impl InputScanner {
    pub fn new(target_block_size: usize) -> Self {
        Self {
            target_block_size,
            format_detector: FormatDetector::new(),
        }
    }

    pub fn scan_file(&self, path: &Path) -> Result<Vec<Batch>> {
        let mmap = MmapInput::open(path)?;
        let bytes = mmap.as_bytes()?;
        let format = self.format_detector.detect(&bytes);
        let boundary_mode = self.detect_boundary_mode(path, &bytes, format);

        let mut batches = Vec::new();
        let mut start = 0;
        let mut id = 0;

        while start < bytes.len() {
            let end = self.find_block_boundary(&bytes, start, &boundary_mode);

            batches.push(Batch {
                id,
                source_path: path.to_path_buf(),
                data: mmap.slice(start, end)?,
                file_type_hint: format,
            });

            start = end;
            id += 1;
        }

        Ok(batches)
    }

    fn detect_boundary_mode(&self, path: &Path, data: &[u8], format: FileFormat) -> BoundaryMode {
        match format {
            FileFormat::Text => BoundaryMode::TextNewline,
            FileFormat::Image => {
                // Use image crate metadata/decoder path for row-safe chunking.
                image_row_bytes(path, data)
                    .map(|row_bytes| BoundaryMode::ImageRows { row_bytes })
                    .unwrap_or(BoundaryMode::Raw)
            }
            FileFormat::Audio => {
                // Use symphonia metadata/packets for frame-safe chunking.
                audio_frame_bytes(path, data)
                    .map(|frame_bytes| BoundaryMode::AudioFrames { frame_bytes })
                    .unwrap_or(BoundaryMode::Raw)
            }
            _ => BoundaryMode::Raw,
        }
    }

    fn find_block_boundary(&self, data: &[u8], start: usize, mode: &BoundaryMode) -> usize {
        let target = (start + self.target_block_size).min(data.len());

        match mode {
            BoundaryMode::TextNewline => {
                if let Some(pos) = memchr(b'\n', &data[start..target]) {
                    start + pos + 1
                } else {
                    target
                }
            }
            BoundaryMode::ImageRows { row_bytes } => align_down(target, *row_bytes).max(start + 1),
            BoundaryMode::AudioFrames { frame_bytes } => {
                align_down(target, *frame_bytes).max(start + 1)
            }
            BoundaryMode::Raw => target,
        }
    }
}
```

**Deliverables:**
- [x] File scanning with format-aware boundaries
- [x] SIMD-accelerated newline detection for text
- [x] Image row-aware boundaries via `image`
- [x] Audio frame-aware boundaries via `symphonia`
- [x] Configurable block sizes
- [x] Batch ordering preservation
- [x] Safe raw fallback when metadata parsing fails

**Verification:**
- Test with text, binary, image, and audio files
- Benchmark: >500 MB/s scanning throughput
- Verify block alignment for text files
- Verify row/frame alignment for media-aware chunking
- Verify fallback-to-raw does not drop or duplicate bytes

---

### 1.4 Work Scheduling

#### Task 1.4.1: Work-Stealing Queue
**Priority:** Critical  
**Dependencies:** 1.1.2  
**Estimated Time:** 8 hours

```rust
// crates/oxide-core/src/core/work_stealing.rs
use crossbeam_deque::{Injector, Stealer, Worker as DequeWorker};
use std::sync::Arc;

pub struct WorkStealingQueue<T> {
    global: Injector<T>,
    stealers: Vec<Stealer<T>>,
}

impl<T> WorkStealingQueue<T> {
    pub fn new(num_workers: usize) -> Self {
        let stealers: Vec<_> = (0..num_workers)
            .map(|_| DequeWorker::new_lifo().stealer())
            .collect();
        
        Self {
            global: Injector::new(),
            stealers,
        }
    }
    
    pub fn submit(&self, item: T) {
        self.global.push(item);
    }
    
    pub fn worker(&self, id: usize) -> Worker<T> {
        Worker::new(id, self)
    }
}

pub struct Worker<'a, T> {
    id: usize,
    local: DequeWorker<T>,
    queue: &'a WorkStealingQueue<T>,
}

impl<'a, T> Worker<'a, T> {
    fn new(id: usize, queue: &'a WorkStealingQueue<T>) -> Self {
        Self {
            id,
            local: DequeWorker::new_lifo(),
            queue,
        }
    }
    
    pub fn steal(&self) -> Option<T> {
        // Try local queue first (LIFO for cache locality)
        self.local.pop()
            .or_else(|| self.steal_from_global())
            .or_else(|| self.steal_from_others())
    }
    
    fn steal_from_global(&self) -> Option<T> {
        loop {
            match self.queue.global.steal() {
                crossbeam_deque::Steal::Success(item) => return Some(item),
                crossbeam_deque::Steal::Empty => return None,
                crossbeam_deque::Steal::Retry => continue,
            }
        }
    }
    
    fn steal_from_others(&self) -> Option<T> {
        let start = self.id;
        for i in 0..self.queue.stealers.len() {
            let idx = (start + i) % self.queue.stealers.len();
            if idx == self.id {
                continue;
            }
            
            loop {
                match self.queue.stealers[idx].steal() {
                    crossbeam_deque::Steal::Success(item) => return Some(item),
                    crossbeam_deque::Steal::Empty => break,
                    crossbeam_deque::Steal::Retry => continue,
                }
            }
        }
        None
    }
}
```

**Deliverables:**
- [x] Global injector queue
- [x] Per-worker LIFO queues
- [x] Work-stealing algorithm
- [x] Lock-free implementation

**Verification:**
- Unit tests for queue operations
- Load balancing test with mixed workloads
- Benchmark: compare throughput vs static scheduling

---

#### Task 1.4.2: Worker Pool
**Priority:** Critical  
**Dependencies:** 1.4.1, 1.2.1  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/core/worker_pool.rs
pub struct WorkerPool {
    num_workers: usize,
    queue: Arc<WorkStealingQueue<Batch>>,
    buffer_pool: Arc<BufferPool>,
    compression_algo: CompressionAlgo,
}

impl WorkerPool {
    pub fn new(
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        let queue = Arc::new(WorkStealingQueue::new(num_workers));
        
        Self {
            num_workers,
            queue,
            buffer_pool,
            compression_algo,
        }
    }
    
    pub fn spawn<F>(&self, processor: F) -> WorkerPoolHandle
    where
        F: Fn(Batch) -> Result<CompressedBlock> + Send + Sync + 'static,
    {
        let processor = Arc::new(processor);
        let mut handles = Vec::new();
        
        for id in 0..self.num_workers {
            let worker = self.queue.worker(id);
            let proc = Arc::clone(&processor);
            
            let handle = thread::spawn(move || {
                loop {
                    match worker.steal() {
                        Some(batch) => {
                            if let Ok(block) = proc(batch) {
                                // Send to output channel
                            }
                        }
                        None => {
                            // No work available, brief sleep
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            });
            
            handles.push(handle);
        }
        
        WorkerPoolHandle { handles }
    }
}

pub struct WorkerPoolHandle {
    handles: Vec<thread::JoinHandle<()>>,
}
```

**Deliverables:**
- [x] Thread pool with work stealing
- [x] Configurable number of workers
- [x] Graceful shutdown mechanism
- [x] Integration with buffer pool

**Verification:**
- Integration test with sample workloads
- Verify all workers process work
- Test shutdown behavior

---

### 1.5 OXZ File Format

#### Task 1.5.1: Header Serialization
**Priority:** Critical  
**Dependencies:** None  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/format/oxz.rs
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};

pub const OXZ_MAGIC: &[u8] = b"OXZ\0";
pub const OXZ_VERSION: u16 = 1;
pub const BLOCK_HEADER_SIZE: usize = 24;
pub const GLOBAL_HEADER_SIZE: usize = 16;

pub struct GlobalHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub reserved: u16,
    pub flags: u32,
    pub block_count: u32,
}

impl GlobalHeader {
    pub fn new(block_count: u32) -> Self {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(OXZ_MAGIC);
        
        Self {
            magic,
            version: OXZ_VERSION,
            reserved: 0,
            flags: 0,
            block_count,
        }
    }
    
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.magic)?;
        writer.write_u16::<LittleEndian>(self.version)?;
        writer.write_u16::<LittleEndian>(self.reserved)?;
        writer.write_u32::<LittleEndian>(self.flags)?;
        writer.write_u32::<LittleEndian>(self.block_count)?;
        Ok(())
    }
    
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        
        if &magic != OXZ_MAGIC {
            return Err(OxideError::InvalidFormat("Invalid magic number"));
        }
        
        Ok(Self {
            magic,
            version: reader.read_u16::<LittleEndian>()?,
            reserved: reader.read_u16::<LittleEndian>()?,
            flags: reader.read_u32::<LittleEndian>()?,
            block_count: reader.read_u32::<LittleEndian>()?,
        })
    }
}

pub struct BlockHeader {
    pub block_id: u64,
    pub original_size: u32,
    pub compressed_size: u32,
    pub strategy_flags: u8,
    pub compression_flags: u8,
    pub reserved: u16,
    pub crc32: u32,
}

impl BlockHeader {
    pub fn new(
        block_id: u64,
        original_size: u32,
        compressed_size: u32,
        strategy: &PreProcessingStrategy,
        compression: CompressionAlgo,
        crc32: u32,
    ) -> Self {
        Self {
            block_id,
            original_size,
            compressed_size,
            strategy_flags: strategy.to_flags(),
            compression_flags: compression.to_flags(),
            reserved: 0,
            crc32,
        }
    }
    
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u64::<LittleEndian>(self.block_id)?;
        writer.write_u32::<LittleEndian>(self.original_size)?;
        writer.write_u32::<LittleEndian>(self.compressed_size)?;
        writer.write_u8(self.strategy_flags)?;
        writer.write_u8(self.compression_flags)?;
        writer.write_u16::<LittleEndian>(self.reserved)?;
        writer.write_u32::<LittleEndian>(self.crc32)?;
        Ok(())
    }
    
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        Ok(Self {
            block_id: reader.read_u64::<LittleEndian>()?,
            original_size: reader.read_u32::<LittleEndian>()?,
            compressed_size: reader.read_u32::<LittleEndian>()?,
            strategy_flags: reader.read_u8()?,
            compression_flags: reader.read_u8()?,
            reserved: reader.read_u16::<LittleEndian>()?,
            crc32: reader.read_u32::<LittleEndian>()?,
        })
    }
}
```

**Deliverables:**
- [x] Global header serialization/deserialization
- [x] Block header serialization/deserialization
- [x] Flag encoding for strategies and compression (bit layout per ARCHITECTURE.md)
- [x] Footer layout and global CRC32 verification
- [x] Validation and error handling

**Verification:**
- Unit tests for round-trip serialization
- Test with sample OXZ files
- Verify header sizes match specification

---

#### Task 1.5.2: Archive Writer
**Priority:** Critical  
**Dependencies:** 1.5.1, 1.4.2  
**Estimated Time:** 8 hours

```rust
// crates/oxide-core/src/pipeline/archiver/writer.rs
pub struct ArchiveWriter<W: Write> {
    writer: W,
    buffer_pool: Arc<BufferPool>,
    blocks_written: u32,
    global_crc32: crc32fast::Hasher,
}

impl<W: Write> ArchiveWriter<W> {
    pub fn new(writer: W, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            writer,
            buffer_pool,
            blocks_written: 0,
            global_crc32: crc32fast::Hasher::new(),
        }
    }
    
    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        let header = GlobalHeader::new(block_count);
        header.write(&mut self.writer)?;
        
        // Update global CRC32
        let mut temp = Vec::new();
        header.write(&mut temp)?;
        self.global_crc32.update(&temp);
        
        Ok(())
    }
    
    pub fn write_block(&mut self, block: &CompressedBlock) -> Result<()> {
        let header = BlockHeader::new(
            block.id as u64,
            block.original_len as u32,
            block.data.len() as u32,
            &block.pre_proc,
            block.compression,
            block.crc32,
        );
        
        // Write header
        header.write(&mut self.writer)?;
        
        // Write data
        self.writer.write_all(&block.data)?;
        
        // Update global CRC32
        let mut temp = Vec::new();
        header.write(&mut temp)?;
        self.global_crc32.update(&temp);
        self.global_crc32.update(&block.data);
        
        self.blocks_written += 1;
        Ok(())
    }
    
    pub fn write_footer(mut self) -> Result<()> {
        self.writer.write_all(b"END\0")?;
        self.writer.write_u32::<LittleEndian>(self.global_crc32.finalize())?;
        Ok(())
    }
}
```

**Deliverables:**
- [x] Sequential block writing
- [x] Global CRC32 calculation
- [x] Proper header ordering
- [x] Integration with buffer pool
- [x] Optional reorder-buffer hook to accept out-of-order blocks

**Verification:**
- Integration test: create OXZ file
- Verify with hex dump
- Test with multiple blocks

---

#### Task 1.5.3: Archive Reader
**Priority:** High  
**Dependencies:** 1.5.1  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/pipeline/extractor/reader.rs
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
}

impl<R: Read + Seek> ArchiveReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let global_header = GlobalHeader::read(&mut reader)?;
        Ok(Self {
            reader,
            global_header,
        })
    }
    
    pub fn block_count(&self) -> u32 {
        self.global_header.block_count
    }
    
    pub fn read_block(&mut self, index: u32) -> Result<(BlockHeader, Vec<u8>)> {
        // Calculate offset: global header + previous blocks
        let offset = GLOBAL_HEADER_SIZE as u64 
            + index as u64 * (BLOCK_HEADER_SIZE as u64 + /* prev block size */);
        
        self.reader.seek(SeekFrom::Start(offset))?;
        
        let header = BlockHeader::read(&mut self.reader)?;
        let mut data = vec![0u8; header.compressed_size as usize];
        self.reader.read_exact(&mut data)?;
        
        // Verify CRC32
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != header.crc32 {
            return Err(OxideError::ChecksumMismatch {
                expected: header.crc32,
                actual: actual_crc,
            });
        }
        
        Ok((header, data))
    }
    
    pub fn iter_blocks(&mut self) -> BlockIterator<R> {
        BlockIterator::new(&mut self.reader, self.global_header.block_count)
    }
}

pub struct BlockIterator<'a, R: Read + Seek> {
    reader: &'a mut R,
    remaining: u32,
    current_offset: u64,
}
```

**Deliverables:**
- [x] Sequential block reading
- [x] Random access to blocks
- [x] CRC32 verification
- [x] Iterator interface

**Verification:**
- Test reading OXZ files
- Verify CRC32 checks fail on corruption
- Test random access vs sequential

---

#### Task 1.5.4: Output Reordering Buffer
**Priority:** High  
**Dependencies:** 1.4.2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/pipeline/archiver/reorder.rs
pub struct ReorderBuffer<T> {
    next_id: usize,
    pending: BTreeMap<usize, T>,
}

impl<T> ReorderBuffer<T> {
    pub fn new() -> Self {
        Self { next_id: 0, pending: BTreeMap::new() }
    }

    pub fn push(&mut self, id: usize, item: T) -> Vec<T> {
        self.pending.insert(id, item);
        let mut ready = Vec::new();
        while let Some(item) = self.pending.remove(&self.next_id) {
            ready.push(item);
            self.next_id += 1;
        }
        ready
    }
}
```

**Deliverables:**
- [x] Reorder buffer for out-of-order worker completion
- [x] Integrate with writer to preserve block ordering
- [x] Bounded memory behavior under skewed workloads

**Verification:**
- Unit test with out-of-order block IDs
- Integration test: parallel pipeline writes ordered archive

---

### Phase 1 Deliverables

**Success Criteria:**
- [x] Zero-allocation hot path working end-to-end (mmap + buffer pool), with documented exceptions
- [x] Can create and read OXZ files
- [x] All unit tests passing
- [x] Buffer pool maintains stable memory usage
- [x] Work-stealing shows improved throughput with mixed workloads
- [x] Output ordering preserved for parallel processing

**Integration Test:**
```rust
#[test]
fn test_end_to_end_roundtrip() {
    // Create archive
    let input_data = b"Test data for compression...";
    let mut archive = Vec::new();
    
    let buffer_pool = Arc::new(BufferPool::new(64 * 1024, 16));
    let mut writer = ArchiveWriter::new(&mut archive, buffer_pool.clone());
    writer.write_global_header(1).unwrap();
    
    let block = CompressedBlock {
        id: 0,
        data: input_data.to_vec(),
        pre_proc: PreProcessingStrategy::None,
        compression: CompressionAlgo::None,
        original_len: input_data.len() as u64,
        crc32: crc32fast::hash(input_data),
    };
    writer.write_block(&block).unwrap();
    writer.write_footer().unwrap();
    
    // Read archive
    let mut reader = ArchiveReader::new(Cursor::new(&archive)).unwrap();
    assert_eq!(reader.block_count(), 1);
    
    let (header, data) = reader.read_block(0).unwrap();
    assert_eq!(data, input_data);
}
```

---

## Phase 2: Smart Preprocessing (Week 2)

**Goal:** Content-aware preprocessing with automatic strategy selection
**Target:** Automatic strategy selection working end-to-end

### 2.1 Preprocessing Infrastructure

#### Task 2.1.1: Preprocessor Trait
**Priority:** High  
**Dependencies:** Phase 1  
**Estimated Time:** 3 hours

```rust
// crates/oxide-core/src/preprocessing/mod.rs
pub trait Preprocessor: Send + Sync {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>>;
}

pub struct PreprocessingPipeline {
    strategies: Vec<Box<dyn Preprocessor>>,
}

impl PreprocessingPipeline {
    pub fn apply(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut result = data.to_vec();
        for strategy in &self.strategies {
            result = strategy.preprocess(&result)?;
        }
        Ok(result)
    }
    
    pub fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut result = data.to_vec();
        for strategy in self.strategies.iter().rev() {
            result = strategy.reverse(&result)?;
        }
        Ok(result)
    }
}
```

**Deliverables:**
- [ ] Define Preprocessor trait
- [ ] Implement pipeline composition
- [ ] Error handling for preprocessing failures

**Verification:** `cargo test preprocessing::`

---

#### Task 2.1.2: Strategy Selector
**Priority:** High  
**Dependencies:** 2.1.1, 1.1.3  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/preprocessing/selector.rs
pub struct StrategySelector;

impl StrategySelector {
    pub fn select(data: &[u8], format: &FileFormat) -> PreProcessingStrategy {
        match format {
            FileFormat::Binary if Self::is_x86_executable(data) => {
                PreProcessingStrategy::Binary(BinaryStrategy::BCJ)
            }
            FileFormat::Text => {
                // Use BWT for high-entropy text (e.g., DNA sequences)
                if Self::is_high_entropy(data) {
                    PreProcessingStrategy::Text(TextStrategy::BWT)
                } else {
                    PreProcessingStrategy::Text(TextStrategy::BPE)
                }
            }
            FileFormat::Audio => {
                // Enable LPC only when scanner guarantees frame alignment.
                if batch.boundary_hint.audio_frame_aligned {
                    PreProcessingStrategy::Audio(AudioStrategy::LPC)
                } else {
                    PreProcessingStrategy::None
                }
            }
            FileFormat::Image => {
                // Enable image predictors only on verified row boundaries.
                if batch.boundary_hint.image_row_aligned {
                    PreProcessingStrategy::Image(ImageStrategy::Paeth)
                } else {
                    PreProcessingStrategy::None
                }
            }
            _ => PreProcessingStrategy::None,
        }
    }
    
    fn is_x86_executable(data: &[u8]) -> bool {
        // Check for ELF magic
        if data.len() >= 4 && &data[0..4] == b"\x7fELF" {
            return true;
        }
        // Check for PE magic
        if data.len() >= 2 && &data[0..2] == b"MZ" {
            return true;
        }
        // Check for x86 prologue patterns
        if data.len() >= 3 && &data[0..3] == b"\x55\x89\xE5" {
            return true;
        }
        false
    }
    
    fn is_high_entropy(data: &[u8]) -> bool {
        // Calculate byte frequency entropy
        // High entropy indicates repetitive patterns suitable for BWT
        let mut frequencies = [0u32; 256];
        for &byte in data {
            frequencies[byte as usize] += 1;
        }
        
        let len = data.len() as f64;
        let entropy: f64 = frequencies.iter()
            .filter(|&&f| f > 0)
            .map(|&f| {
                let p = f as f64 / len;
                -p * p.log2()
            })
            .sum();
        
        entropy > 7.5 // Threshold for high entropy
    }
}
```

**Deliverables:**
- [ ] Implement format-based strategy selection
- [ ] Implement x86 executable detection
- [ ] Implement entropy-based heuristics
- [ ] Configurable thresholds
- [ ] Add image/audio header heuristics per format detector

**Verification:**
- Test with different file types
- Verify BCJ selected for executables
- Test entropy calculation accuracy

---

### 2.2 BCJ Filter (x86 Binary)

#### Task 2.2.1: BCJ Implementation
**Priority:** High  
**Dependencies:** 2.1.1  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/preprocessing/bcj.rs
pub struct BcjFilter;

impl Preprocessor for BcjFilter {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut output = data.to_vec();
        let mut i = 0;
        
        while i < data.len().saturating_sub(5) {
            // Detect x86 CALL (0xE8) and JMP (0xE9) near instructions
            // Format: E8/E9 xx xx xx xx (relative offset)
            if data[i] == 0xE8 || data[i] == 0xE9 {
                let offset = i32::from_le_bytes([
                    data[i+1], data[i+2], data[i+3], data[i+4]
                ]);
                
                // Convert relative offset to absolute address
                let absolute = (i as i32 + 5 + offset) as u32;
                output[i+1..i+5].copy_from_slice(&absolute.to_le_bytes());
            }
            i += 1;
        }
        
        Ok(output)
    }
    
    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut output = data.to_vec();
        let mut i = 0;
        
        while i < data.len().saturating_sub(5) {
            if data[i] == 0xE8 || data[i] == 0xE9 {
                let absolute = u32::from_le_bytes([
                    data[i+1], data[i+2], data[i+3], data[i+4]
                ]);
                
                // Convert absolute address back to relative offset
                let offset = (absolute as i32) - (i as i32 + 5);
                output[i+1..i+5].copy_from_slice(&offset.to_le_bytes());
            }
            i += 1;
        }
        
        Ok(output)
    }
}
```

**Deliverables:**
- [ ] BCJ transformation (relative → absolute)
- [ ] BCJ reverse transformation (absolute → relative)
- [ ] Handle edge cases (truncated instructions)
- [ ] Tests with real x86 executables

**Verification:**
- Round-trip test: reverse(preprocess(x)) == x
- Test with sample executables
- Measure compression improvement

---

### 2.3 BWT Filter (Text)

#### Task 2.3.1: BWT Implementation
**Priority:** Medium  
**Dependencies:** 2.1.1  
**Estimated Time:** 10 hours

```rust
// crates/oxide-core/src/preprocessing/bwt.rs
pub struct BwtFilter;

impl Preprocessor for BwtFilter {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        let n = data.len();
        let mut suffixes: Vec<usize> = (0..n).collect();
        
        // Sort suffixes using suffix array algorithm
        // O(n log n) using comparison sort
        suffixes.sort_by(|&a, &b| {
            data[a..].cmp(&data[b..])
        });
        
        // Build BWT string (last column of sorted rotations)
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
        
        // Store primary index at the beginning (as u32)
        let mut result = Vec::with_capacity(n + 4);
        result.extend_from_slice(&(primary_index as u32).to_le_bytes());
        result.extend_from_slice(&bwt);
        
        Ok(result)
    }
    
    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < 4 {
            return Ok(Vec::new());
        }
        
        let primary_index = u32::from_le_bytes([
            data[0], data[1], data[2], data[3]
        ]) as usize;
        let bwt = &data[4..];
        let n = bwt.len();
        
        // Build LF mapping
        let mut tuples: Vec<(u8, usize)> = bwt.iter()
            .enumerate()
            .map(|(i, &c)| (c, i))
            .collect();
        
        tuples.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        
        let lf: Vec<usize> = tuples.iter().map(|(_, i)| *i).collect();
        
        // Reconstruct original string
        let mut result = Vec::with_capacity(n);
        let mut idx = primary_index;
        
        for _ in 0..n {
            result.push(bwt[idx]);
            idx = lf[idx];
        }
        
        result.reverse();
        Ok(result)
    }
}
```

**Deliverables:**
- [ ] Burrows-Wheeler Transform implementation
- [ ] Inverse BWT implementation
- [ ] Suffix array construction
- [ ] LF mapping for inverse
- [ ] Store primary index in output

**Verification:**
- Round-trip test
- Verify BWT output is same-length + 4 bytes
- Test with repetitive patterns
- Performance test: <100ms for 1MB

---

### 2.4 BPE Filter (Text)

#### Task 2.4.1: BPE Implementation
**Priority:** Medium  
**Dependencies:** 2.1.1  
**Estimated Time:** 8 hours

```rust
// crates/oxide-core/src/preprocessing/bpe.rs
pub struct BpeFilter {
    max_merges: usize,
}

impl BpeFilter {
    pub fn new(max_merges: usize) -> Self {
        Self { max_merges }
    }
    
    fn build_vocab(&self, data: &[u8]) -> HashMap<(u8, u8), u32> {
        let mut vocab = HashMap::new();
        for window in data.windows(2) {
            *vocab.entry((window[0], window[1])).or_insert(0) += 1;
        }
        vocab
    }
}

impl Preprocessor for BpeFilter {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut tokens: Vec<u8> = data.to_vec();
        let mut merges = Vec::new();
        
        for _ in 0..self.max_merges {
            let vocab = self.build_vocab(&tokens);
            
            if vocab.is_empty() {
                break;
            }
            
            // Find most frequent pair
            let (&(a, b), _) = vocab.iter()
                .max_by_key(|(_, count)| *count)
                .unwrap();
            
            if vocab[&(a, b)] < 2 {
                break; // No more beneficial merges
            }
            
            merges.push((a, b));
            
            // Apply merge to tokens
            let mut new_tokens = Vec::with_capacity(tokens.len());
            let mut i = 0;
            
            while i < tokens.len() {
                if i < tokens.len() - 1 && tokens[i] == a && tokens[i+1] == b {
                    new_tokens.push(a); // Use first byte as merged token
                    i += 2;
                } else {
                    new_tokens.push(tokens[i]);
                    i += 1;
                }
            }
            
            tokens = new_tokens;
        }
        
        // Serialize: [num_merges: u32][merges...][compressed_data...]
        let mut result = Vec::new();
        result.extend_from_slice(&(merges.len() as u32).to_le_bytes());
        for (a, b) in &merges {
            result.push(*a);
            result.push(*b);
        }
        result.extend_from_slice(&tokens);
        
        Ok(result)
    }
    
    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < 4 {
            return Ok(Vec::new());
        }
        
        let num_merges = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut pos = 4;
        
        let mut merges = Vec::with_capacity(num_merges);
        for _ in 0..num_merges {
            if pos + 1 >= data.len() {
                break;
            }
            merges.push((data[pos], data[pos+1]));
            pos += 2;
        }
        
        let mut tokens: Vec<u8> = data[pos..].to_vec();
        
        // Reverse merges in reverse order
        for (a, b) in merges.iter().rev() {
            let mut expanded = Vec::with_capacity(tokens.len() * 2);
            for &token in &tokens {
                if token == *a {
                    expanded.push(*a);
                    expanded.push(*b);
                } else {
                    expanded.push(token);
                }
            }
            tokens = expanded;
        }
        
        Ok(tokens)
    }
}
```

**Deliverables:**
- [ ] BPE tokenization
- [ ] Merge rule serialization
- [ ] Reverse expansion
- [ ] Configurable max merges
- [ ] Frequency-based pair selection

**Verification:**
- Round-trip test
- Verify text compression improvement
- Test with various text types

---

### 2.5 Image Preprocessing

#### Task 2.5.1: Paeth Filter
**Priority:** Medium  
**Dependencies:** 2.1.1  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/preprocessing/paeth.rs
pub struct PaethFilter {
    width: usize,
    height: usize,
    bytes_per_pixel: usize,
}

impl PaethFilter {
    pub fn new(width: usize, height: usize, bytes_per_pixel: usize) -> Self {
        Self {
            width,
            height,
            bytes_per_pixel,
        }
    }
    
    fn paeth_predictor(a: u8, b: u8, c: u8) -> u8 {
        let pa = (b as i16 - c as i16).abs();
        let pb = (a as i16 - c as i16).abs();
        let pc = ((a as i16 + b as i16 - 2 * c as i16)).abs();
        
        if pa <= pb && pa <= pc {
            a
        } else if pb <= pc {
            b
        } else {
            c
        }
    }
}

impl Preprocessor for PaethFilter {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(data.len());
        let stride = self.width * self.bytes_per_pixel;
        
        for y in 0..self.height {
            for x in 0..stride {
                let idx = y * stride + x;
                let current = data[idx];
                
                let a = if x >= self.bytes_per_pixel {
                    data[idx - self.bytes_per_pixel]
                } else {
                    0
                };
                
                let b = if y > 0 {
                    data[idx - stride]
                } else {
                    0
                };
                
                let c = if x >= self.bytes_per_pixel && y > 0 {
                    data[idx - stride - self.bytes_per_pixel]
                } else {
                    0
                };
                
                let predicted = Self::paeth_predictor(a, b, c);
                output.push(current.wrapping_sub(predicted));
            }
        }
        
        Ok(output)
    }
    
    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut output = Vec::with_capacity(data.len());
        let stride = self.width * self.bytes_per_pixel;
        
        for y in 0..self.height {
            for x in 0..stride {
                let idx = y * stride + x;
                let residual = data[idx];
                
                let a = if x >= self.bytes_per_pixel {
                    output[idx - self.bytes_per_pixel]
                } else {
                    0
                };
                
                let b = if y > 0 {
                    output[idx - stride]
                } else {
                    0
                };
                
                let c = if x >= self.bytes_per_pixel && y > 0 {
                    output[idx - stride - self.bytes_per_pixel]
                } else {
                    0
                };
                
                let predicted = Self::paeth_predictor(a, b, c);
                output.push(residual.wrapping_add(predicted));
            }
        }
        
        Ok(output)
    }
}
```

**Deliverables:**
- [ ] Paeth predictor implementation
- [ ] Image dimension handling
- [ ] Row/column boundary handling
- [ ] PNG-compatible filter
- [ ] Enforce row-aligned chunks before applying predictor

**Verification:**
- Round-trip test
- Test with RGB images
- Compare with PNG compression

---

#### Task 2.5.2: YCoCgR Transform
**Priority:** Medium  
**Dependencies:** 2.1.1  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/preprocessing/ycocgr.rs
pub struct YCoCgR;

impl Preprocessor for YCoCgR {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        // RGB -> YCoCgR for 8-bit channels
    }

    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        // YCoCgR -> RGB
    }
}
```

**Deliverables:**
- [ ] RGB <-> YCoCgR transform
- [ ] Channel packing for 24/32-bit pixels
- [ ] Round-trip tests with sample images

**Verification:**
- Round-trip test
- Compare compression ratio with/without transform

---

#### Task 2.5.3: LocoI Predictor (JPEG-LS)
**Priority:** Medium  
**Dependencies:** 2.1.1  
**Estimated Time:** 8 hours

```rust
// crates/oxide-core/src/preprocessing/loco_i.rs
pub struct LocoI;

impl Preprocessor for LocoI {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        // JPEG-LS predictor (Loco-I) for 8-bit channels
    }

    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Inverse predictor
    }
}
```

**Deliverables:**
- [ ] Predictor implementation
- [ ] Boundary handling for first row/col
- [ ] Round-trip tests with sample images
- [ ] Enforce row-aligned chunks before applying predictor

**Verification:**
- Round-trip test
- Compare with Paeth on sample data

---

### 2.6 Audio Preprocessing

#### Task 2.6.1: LPC Filter
**Priority:** Low  
**Dependencies:** 2.1.1  
**Estimated Time:** 6 hours

```rust
// crates/oxide-core/src/preprocessing/lpc.rs
pub struct LpcFilter;

impl Preprocessor for LpcFilter {
    fn preprocess(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Assume 16-bit PCM samples
        if data.len() % 2 != 0 {
            return Err(OxideError::InvalidFormat("Invalid PCM data length"));
        }
        
        let samples: Vec<i16> = data.chunks_exact(2)
            .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        // First-order LPC: predict from previous sample
        let mut residuals = Vec::with_capacity(samples.len() * 2);
        let mut prev = 0i16;
        
        for sample in samples {
            let predicted = prev;
            let residual = sample.wrapping_sub(predicted);
            residuals.extend_from_slice(&residual.to_le_bytes());
            prev = sample;
        }
        
        Ok(residuals)
    }
    
    fn reverse(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() % 2 != 0 {
            return Err(OxideError::InvalidFormat("Invalid residual data length"));
        }
        
        let residuals: Vec<i16> = data.chunks_exact(2)
            .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        let mut samples = Vec::with_capacity(residuals.len() * 2);
        let mut prev = 0i16;
        
        for residual in residuals {
            let sample = residual.wrapping_add(prev);
            samples.extend_from_slice(&sample.to_le_bytes());
            prev = sample;
        }
        
        Ok(samples)
    }
}
```

**Deliverables:**
- [ ] First-order LPC prediction
- [ ] Residual calculation
- [ ] PCM format support (16-bit)
- [ ] Reconstruction from residuals
- [ ] Enforce frame-aligned chunks before applying LPC

**Verification:**
- Round-trip test
- Test with PCM audio
- Measure compression improvement

---

### Phase 2 Deliverables

**Success Criteria:**
- [ ] All preprocessing filters implemented
- [ ] Automatic strategy selection working
- [ ] Round-trip tests passing for all filters
- [ ] Compression improvement measured for each filter type
- [ ] Image pipeline supports Paeth, YCoCgR, and LocoI

**Benchmark Suite:**
```rust
#[bench]
fn bench_bcj_roundtrip(b: &mut Bencher) {
    let data = include_bytes!("testdata/x86_executable.bin");
    let filter = BcjFilter;
    
    b.iter(|| {
        let processed = filter.preprocess(data).unwrap();
        filter.reverse(&processed).unwrap();
    });
}

#[bench]
fn bench_bwt_roundtrip(b: &mut Bencher) {
    let data = b" repetitive text for bwt testing repetitive text ";
    let filter = BwtFilter;
    
    b.iter(|| {
        let processed = filter.preprocess(data).unwrap();
        filter.reverse(&processed).unwrap();
    });
}
```

---

## Phase 3: Compression Integration (Week 3)

**Goal:** Full compression pipeline with intelligent defaults
**Target:** Production-ready archiver matching 7-Zip compression ratios

### 3.1 Compression Algorithms

#### Task 3.1.1: LZ4 Integration
**Priority:** Critical  
**Dependencies:** Phase 2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/compressors/lz4.rs
use lz4_flex::block::{compress, decompress};

pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8], output: &mut Vec<u8>) -> Result<()> {
        let compressed = compress(data);
        output.extend_from_slice(&compressed);
        Ok(())
    }
    
    fn decompress(&self, data: &[u8], original_size: usize) -> Result<Vec<u8>> {
        decompress(data, original_size)
            .map_err(|e| OxideError::DecompressionError(e.to_string()))
    }
    
    fn algorithm(&self) -> CompressionAlgo {
        CompressionAlgo::Lz4
    }
}
```

**Deliverables:**
- [ ] LZ4 compression integration
- [ ] LZ4 decompression
- [ ] Streaming support if needed

**Verification:**
- Round-trip test
- Benchmark: >400 MB/s compression
- Verify against lz4_flex examples

---

#### Task 3.1.2: LZMA Integration
**Priority:** High  
**Dependencies:** Phase 2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/compressors/lzma.rs
use lzma_rs::{lzma_compress, lzma_decompress};
use std::io::Cursor;

pub struct LzmaCompressor;

impl Compressor for LzmaCompressor {
    fn compress(&self, data: &[u8], output: &mut Vec<u8>) -> Result<()> {
        let mut input = Cursor::new(data);
        lzma_compress(&mut input, output)
            .map_err(|e| OxideError::CompressionError(e.to_string()))?;
        Ok(())
    }
    
    fn decompress(&self, data: &[u8], _original_size: usize) -> Result<Vec<u8>> {
        let mut input = Cursor::new(data);
        let mut output = Vec::new();
        lzma_decompress(&mut input, &mut output)
            .map_err(|e| OxideError::DecompressionError(e.to_string()))?;
        Ok(output)
    }
    
    fn algorithm(&self) -> CompressionAlgo {
        CompressionAlgo::Lzma
    }
}
```

**Deliverables:**
- [ ] LZMA compression integration
- [ ] LZMA decompression
- [ ] Error handling for corrupt data

**Verification:**
- Round-trip test
- Benchmark compression ratio vs 7-Zip
- Test with various data types

---

#### Task 3.1.3: Deflate Integration
**Priority:** High  
**Dependencies:** Phase 2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/compressors/deflate.rs
use flate2::{write::DeflateEncoder, read::DeflateDecoder};
use flate2::Compression;

pub struct DeflateCompressor {
    level: Compression,
}

impl DeflateCompressor {
    pub fn new(level: Compression) -> Self {
        Self { level }
    }
}

impl Compressor for DeflateCompressor {
    fn compress(&self, data: &[u8], output: &mut Vec<u8>) -> Result<()> {
        let mut encoder = DeflateEncoder::new(output, self.level);
        encoder.write_all(data)?;
        encoder.finish()?;
        Ok(())
    }
    
    fn decompress(&self, data: &[u8], _original_size: usize) -> Result<Vec<u8>> {
        let mut decoder = DeflateDecoder::new(data);
        let mut output = Vec::new();
        decoder.read_to_end(&mut output)?;
        Ok(output)
    }
    
    fn algorithm(&self) -> CompressionAlgo {
        CompressionAlgo::DeflateHuffman
    }
}
```

**Deliverables:**
- [ ] Deflate compression
- [ ] Deflate decompression
- [ ] Configurable compression levels

**Verification:**
- Round-trip test
- Verify compatibility with gzip
- Test compression ratios

---

### 3.2 Compression Pipeline

#### Task 3.2.1: Compressor Trait and Dispatcher
**Priority:** Critical  
**Dependencies:** 3.1.1, 3.1.2, 3.1.3  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/compressors/mod.rs
pub trait Compressor: Send + Sync {
    fn compress(&self, data: &[u8], output: &mut Vec<u8>) -> Result<()>;
    fn decompress(&self, data: &[u8], original_size: usize) -> Result<Vec<u8>>;
    fn algorithm(&self) -> CompressionAlgo;
}

pub struct CompressorDispatcher;

impl CompressorDispatcher {
    pub fn get_compressor(algo: CompressionAlgo) -> Box<dyn Compressor> {
        match algo {
            CompressionAlgo::Lz4 => Box::new(Lz4Compressor),
            CompressionAlgo::Lzma => Box::new(LzmaCompressor),
            CompressionAlgo::DeflateHuffman => {
                Box::new(DeflateCompressor::new(Compression::default()))
            }
        }
    }
}
```

**Deliverables:**
- [ ] Compressor trait
- [ ] Algorithm dispatcher
- [ ] Factory pattern for compressors

**Verification:** `cargo test compressors::`

---

#### Task 3.2.2: Full Processing Pipeline
**Priority:** Critical  
**Dependencies:** 3.2.1, 2.1.2, 1.4.2  
**Estimated Time:** 8 hours

```rust
// crates/oxide-core/src/pipeline/mod.rs
pub struct ProcessingPipeline {
    worker_pool: WorkerPool,
    buffer_pool: Arc<BufferPool>,
    compression_algo: CompressionAlgo,
}

impl ProcessingPipeline {
    pub fn new(num_workers: usize, compression_algo: CompressionAlgo) -> Self {
        let buffer_pool = Arc::new(BufferPool::new(64 * 1024 * 1024, num_workers * 2));
        let worker_pool = WorkerPool::new(
            num_workers,
            buffer_pool.clone(),
            compression_algo.clone(),
        );
        
        Self {
            worker_pool,
            buffer_pool,
            compression_algo,
        }
    }
    
    pub fn process_batch(&self, batch: Batch) -> Result<CompressedBlock> {
        // Step 1: Select preprocessing strategy
        let strategy = StrategySelector::select(&batch.data, &batch.file_type_hint);
        
        // Step 2: Apply preprocessing
        let preprocessed = match &strategy {
            PreProcessingStrategy::Binary(BinaryStrategy::BCJ) => {
                BcjFilter.preprocess(&batch.data)?
            }
            PreProcessingStrategy::Text(TextStrategy::BWT) => {
                BwtFilter.preprocess(&batch.data)?
            }
            PreProcessingStrategy::Text(TextStrategy::BPE) => {
                BpeFilter::new(256).preprocess(&batch.data)?
            }
            PreProcessingStrategy::Image(ImageStrategy::Paeth) => {
                if batch.boundary_hint.image_row_aligned {
                    PaethFilter.preprocess(&batch.data)?
                } else {
                    batch.data.to_vec()
                }
            }
            PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => {
                batch.data.to_vec()
            }
            PreProcessingStrategy::Image(ImageStrategy::LocoI) => {
                if batch.boundary_hint.image_row_aligned {
                    LocoIFilter.preprocess(&batch.data)?
                } else {
                    batch.data.to_vec()
                }
            }
            PreProcessingStrategy::Audio(AudioStrategy::LPC) => {
                if batch.boundary_hint.audio_frame_aligned {
                    LpcFilter.preprocess(&batch.data)?
                } else {
                    batch.data.to_vec()
                }
            }
            PreProcessingStrategy::None => batch.data.to_vec(),
        };
        
        // Step 3: Compress
        let compressor = CompressorDispatcher::get_compressor(self.compression_algo);
        let mut compressed = self.buffer_pool.acquire();
        compressor.compress(&preprocessed, &mut *compressed)?;
        
        // Step 4: Calculate CRC32
        let crc32 = crc32fast::hash(&*compressed);
        
        Ok(CompressedBlock {
            id: batch.id,
            data: compressed.into_inner(),
            pre_proc: strategy,
            compression: self.compression_algo,
            original_len: batch.data.len() as u64,
            crc32,
        })
    }
}
```

**Deliverables:**
- [ ] Full preprocessing + compression pipeline
- [ ] Strategy selection integration
- [ ] Buffer pool integration
- [ ] Error propagation

**Verification:**
- Integration test with sample files
- Verify correct strategy selection
- Measure end-to-end throughput

---

#### Task 3.2.3: Content Detection Heuristics (Extended)
**Priority:** High  
**Dependencies:** 1.1.3, 2.1.2  
**Estimated Time:** 4 hours

```rust
// crates/oxide-core/src/format/heuristics.rs
pub struct HeuristicConfig {
    pub entropy_threshold: f64,
    pub text_utf8_ratio: f64,
}
```

**Deliverables:**
- [ ] Centralize heuristic thresholds and defaults
- [ ] Keep infer-based coarse detection and add metadata probes via `image`/`symphonia`
- [ ] Add text heuristics (UTF-8 ratio, printable ratio)
- [ ] Document rationale for thresholds

**Verification:**
- Unit tests for signature detection
- Benchmark to ensure detection >100 MB/s

---

### 3.3 CLI Integration

#### Task 3.3.1: Archive Command
**Priority:** High  
**Dependencies:** 3.2.2, 1.5.2  
**Estimated Time:** 6 hours

```rust
// crates/oxide-cli/src/main.rs
use clap::{Parser, Subcommand};
use oxide_core::{ProcessingPipeline, ArchiveWriter};

#[derive(Parser)]
#[command(name = "oxide")]
#[command(about = "Content-aware archival system")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create an archive
    Archive {
        /// Input files/directories
        #[arg(required = true)]
        inputs: Vec<PathBuf>,
        
        /// Output archive path
        #[arg(short, long, default_value = "output.oxz")]
        output: PathBuf,
        
        /// Compression algorithm
        #[arg(short, long, value_enum, default_value = "lz4")]
        algorithm: Algorithm,
        
        /// Number of worker threads
        #[arg(short, long)]
        threads: Option<usize>,
    },
    /// Extract an archive
    Extract {
        /// Archive path
        archive: PathBuf,
        
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output: PathBuf,
    },
    /// List archive contents
    List {
        /// Archive path
        archive: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Archive { inputs, output, algorithm, threads } => {
            let num_threads = threads.unwrap_or_else(num_cpus::get);
            let compression_algo = match algorithm {
                Algorithm::Lz4 => CompressionAlgo::Lz4,
                Algorithm::Lzma => CompressionAlgo::Lzma,
                Algorithm::Deflate => CompressionAlgo::DeflateHuffman,
            };
            
            let pipeline = ProcessingPipeline::new(num_threads, compression_algo);
            
            // Scan all input files
            let scanner = InputScanner::new(64 * 1024 * 1024); // 64MB blocks
            let mut all_batches = Vec::new();
            
            for input in &inputs {
                if input.is_file() {
                    all_batches.extend(scanner.scan_file(input)?);
                } else if input.is_dir() {
                    all_batches.extend(scanner.scan_directory(input)?);
                }
            }
            
            // Create archive
            let file = File::create(&output)?;
            let buffer_pool = Arc::new(BufferPool::new(64 * 1024 * 1024, num_threads * 2));
            let mut writer = ArchiveWriter::new(file, buffer_pool);
            
            writer.write_global_header(all_batches.len() as u32)?;
            
            // Process and write blocks
            for batch in all_batches {
                let block = pipeline.process_batch(batch)?;
                writer.write_block(&block)?;
            }
            
            writer.write_footer()?;
            
            println!("Archive created: {}", output.display());
        }
        Commands::Extract { archive, output } => {
            // Implementation...
        }
        Commands::List { archive } => {
            // Implementation...
        }
    }
    
    Ok(())
}
```

**Deliverables:**
- [ ] Archive command implementation
- [ ] Extract command implementation
- [ ] List command implementation
- [ ] Progress reporting
- [ ] Error handling with user-friendly messages

**Verification:**
- End-to-end test with real files
- Test CLI argument parsing
- Verify output files are valid

---

### Phase 3 Deliverables

**Success Criteria:**
- [ ] All compression algorithms integrated
- [ ] CLI working end-to-end
- [ ] Performance targets met:
  - LZ4: >400 MB/s compression
  - Deflate: >50 MB/s compression  
  - LZMA: >5 MB/s compression
- [ ] Compression ratios competitive with 7-Zip
- [ ] OXZ flags + footer CRC fully validated on read

**Performance Benchmark:**
```bash
# Build benchmarks
cargo build --release

# Run performance tests
cargo bench

# Test with real-world data
oxide archive -a lz4 -o test.oxz /path/to/test/data
oxide extract -o output test.oxz
diff /path/to/test/data output/test/data
```

---

## Testing Strategy

### Unit Tests

**Coverage Targets:**
- All public APIs: 100%
- Preprocessing filters: 100%
- Compression algorithms: 100%
- File format: 100%

**Test Organization:**
```
crates/oxide-core/src/
├── error.rs              # tests/error_tests.rs
├── types.rs              # tests/types_tests.rs
├── format/
│   ├── oxz.rs            # tests/format/oxz_tests.rs
│   └── detector.rs       # tests/format/detector_tests.rs
├── preprocessing/
│   ├── bcj.rs            # tests/preprocessing/bcj_tests.rs
│   ├── bwt.rs            # tests/preprocessing/bwt_tests.rs
│   └── ...
└── compressors/
    ├── lz4.rs            # tests/compressors/lz4_tests.rs
    └── ...
```

### Integration Tests

**Test Scenarios:**
1. **End-to-End Archive/Extract:**
   - Create archive with various file types
   - Extract and verify integrity
   - Test with edge cases (empty files, large files)

2. **Concurrency:**
   - Process multiple files simultaneously
   - Verify work-stealing effectiveness
   - Test memory stability under load

3. **Error Recovery:**
   - Corrupted archive handling
   - Missing files
   - Permission errors

4. **Performance:**
   - Throughput benchmarks
   - Memory usage profiling
   - Comparison with gzip/7-Zip

### Property-Based Tests

```rust
// crates/oxide-core/tests/property_tests.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn bwt_roundtrip(data: Vec<u8>) {
        let filter = BwtFilter;
        let processed = filter.preprocess(&data).unwrap();
        let recovered = filter.reverse(&processed).unwrap();
        prop_assert_eq!(data, recovered);
    }
    
    #[test]
    fn compression_roundtrip(data: Vec<u8>) {
        let compressor = Lz4Compressor;
        let mut compressed = Vec::new();
        compressor.compress(&data, &mut compressed).unwrap();
        let recovered = compressor.decompress(&compressed, data.len()).unwrap();
        prop_assert_eq!(data, recovered);
    }
}
```

---

## Documentation Plan

### 1. API Documentation

**Coverage:**
- All public types and traits
- Function examples
- Error handling guidance

**Command:** `cargo doc --no-deps`

### 2. User Guide

**Sections:**
- Installation
- Quick start
- Command reference
- Performance tuning
- Troubleshooting

### 3. Developer Documentation

**Sections:**
- Architecture overview
- Contributing guide
- Code style
- Testing guide

---

## Timeline Summary

| Phase | Duration | Key Deliverables |
|-------|----------|------------------|
| Phase 1 | Week 1 | Pipeline skeleton, OXZ format, buffer management |
| Phase 2 | Week 2 | All preprocessing filters, strategy selection |
| Phase 3 | Week 3 | Compression integration, CLI, performance optimization |

**Milestones:**
- Day 3: Phase 1 core components complete
- Day 7: End-to-end archive creation working
- Day 10: All preprocessing filters implemented
- Day 14: Smart strategy selection working
- Day 17: All compression algorithms integrated
- Day 21: Production-ready release candidate

---

## Risk Assessment

### Technical Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| BWT performance too slow | High | Use libdivsufsort or suffix array library |
| Memory usage too high | Medium | Tune buffer pool sizes, add memory limits |
| LZMA crate issues | Medium | Evaluate alternatives (rust-lzma, xz2) |
| Cross-platform mmap issues | Low | Add fallback to std::fs::read |

### Schedule Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Phase 1 takes longer than expected | Medium | Prioritize core features, defer optimizations |
| Complex preprocessing algorithms | Medium | Start with simple implementations, optimize later |
| Integration issues | Low | Regular integration testing throughout |

---

## Success Metrics

### Performance
- **Throughput:** 8x improvement over naive implementation
- **Memory:** <1.5x input file size peak usage
- **Latency:** <100ms cold start for 1GB file

### Compression
- **Ratio:** Within 5% of 7-Zip for equivalent algorithms
- **Speed:** Match or exceed 7-Zip decompression speed

### Reliability
- **Test Coverage:** >90% line coverage
- **Crash Rate:** 0 crashes in 10,000 operations
- **Data Integrity:** 100% round-trip accuracy

---

## Next Steps

1. **Immediate:** Begin Phase 1, Task 1.1.1 (Error Handling)
2. **Week 1:** Complete Phase 1 foundation
3. **Week 2:** Implement preprocessing filters
4. **Week 3:** Integrate compression and CLI
5. **Release:** Performance testing and optimization

**Current Blockers:**
- None

**Dependencies:**
- All required crates are in Cargo.toml
- Architecture specification complete
- Initial project structure exists

---

*This roadmap is a living document. Update as implementation progresses and requirements evolve.*
