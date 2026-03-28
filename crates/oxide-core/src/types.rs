use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::buffer::{BufferPool, PooledBuffer};
use crate::checksum::compute_checksum;
use crate::error::OxideError;

pub type Result<T> = std::result::Result<T, OxideError>;

/// Converts a [`Duration`] to microseconds as a u64, capping at [`u64::MAX`].
pub fn duration_to_us(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

/// A batch of data extracted from a file for processing.
///
/// Batches are the primary unit of work in the processing pipeline,
/// containing data along with metadata about its source and type.
#[derive(Debug, Clone)]
pub struct Batch {
    pub id: usize,
    pub source_path: PathBuf,
    pub data: BatchData,
    pub stream_id: u32,
    pub compression_plan: ChunkEncodingPlan,
    /// Whether this batch must be stored raw without compression.
    pub force_raw_storage: bool,
}

/// Planner-selected encoding parameters for a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkEncodingPlan {
    /// Compression algorithm selected for the chunk.
    pub algo: CompressionAlgo,
    /// Optional explicit codec-specific compression level used only by the encoder.
    pub level: Option<i32>,
    /// Enables liblzma's extreme preset variant when using LZMA encoding.
    pub lzma_extreme: bool,
    /// Optional explicit LZMA dictionary size used during encoding.
    pub lzma_dictionary_size: Option<usize>,
    /// Optional advanced Zstd compression parameters used during encoding.
    pub zstd_parameters: ZstdCompressionParameters,
}

impl ChunkEncodingPlan {
    /// Creates a new chunk encoding plan.
    pub fn new(algo: CompressionAlgo) -> Self {
        Self {
            algo,
            level: None,
            lzma_extreme: false,
            lzma_dictionary_size: None,
            zstd_parameters: ZstdCompressionParameters::default(),
        }
    }

    /// Attaches an explicit codec-specific compression level override for encoding.
    pub fn with_level(mut self, level: Option<i32>) -> Self {
        self.level = level;
        self
    }

    /// Enables or disables liblzma's extreme preset variant for LZMA encoding.
    pub fn with_lzma_extreme(mut self, lzma_extreme: bool) -> Self {
        self.lzma_extreme = lzma_extreme;
        self
    }

    /// Attaches an explicit LZMA dictionary size override for encoding.
    pub fn with_lzma_dictionary_size(mut self, lzma_dictionary_size: Option<usize>) -> Self {
        self.lzma_dictionary_size = lzma_dictionary_size;
        self
    }

    /// Attaches advanced Zstd compression parameters for encoding.
    pub fn with_zstd_parameters(mut self, zstd_parameters: ZstdCompressionParameters) -> Self {
        self.zstd_parameters = zstd_parameters;
        self
    }

    /// Builds compression metadata for the final stored payload.
    pub fn compression_meta(self, raw_passthrough: bool) -> CompressionMeta {
        CompressionMeta::new(self.algo, raw_passthrough)
    }
}

impl Default for ChunkEncodingPlan {
    fn default() -> Self {
        Self::new(CompressionAlgo::Lz4)
    }
}

/// Zstd strategy preset used for advanced encoder tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZstdStrategy {
    Fast,
    Dfast,
    Greedy,
    Lazy,
    Lazy2,
    Btlazy2,
    Btopt,
    Btultra,
    Btultra2,
}

/// Optional advanced Zstd encoder parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ZstdCompressionParameters {
    pub window_log: Option<u32>,
    pub strategy: Option<ZstdStrategy>,
    pub enable_long_distance_matching: Option<bool>,
    pub ldm_hash_log: Option<u32>,
    pub ldm_min_match: Option<u32>,
    pub ldm_bucket_size_log: Option<u32>,
    pub ldm_hash_rate_log: Option<u32>,
    pub job_size: Option<u32>,
    pub overlap_log: Option<u32>,
}

impl ZstdCompressionParameters {
    pub const fn is_default(self) -> bool {
        self.window_log.is_none()
            && self.strategy.is_none()
            && self.enable_long_distance_matching.is_none()
            && self.ldm_hash_log.is_none()
            && self.ldm_min_match.is_none()
            && self.ldm_bucket_size_log.is_none()
            && self.ldm_hash_rate_log.is_none()
            && self.job_size.is_none()
            && self.overlap_log.is_none()
    }
}

/// A batch of data extracted from a file for processing.
///
/// This enum supports both owned byte buffers and memory-mapped file regions,
/// allowing the pipeline to minimize copies for large files.
#[derive(Debug, Clone)]
pub enum BatchData {
    /// Owned byte buffer.
    Owned(Bytes),
    /// Memory-mapped file region.
    Mapped {
        /// The underlying memory map.
        map: Arc<Mmap>,
        /// Start offset within the map.
        start: usize,
        /// End offset within the map.
        end: usize,
    },
}

/// Stored payload bytes for an encoded archive block.
#[derive(Debug)]
pub enum CompressedPayload {
    /// Owned heap allocation.
    Owned(Vec<u8>),
    /// Shared immutable bytes reused without copying.
    Bytes(Bytes),
    /// Buffer borrowed from the buffer pool.
    Pooled(PooledBuffer),
    /// Memory-mapped file region reused without copying.
    Mapped {
        /// The underlying memory map.
        map: Arc<Mmap>,
        /// Start offset within the map.
        start: usize,
        /// End offset within the map.
        end: usize,
    },
}

impl Batch {
    /// Creates a new batch
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this batch
    /// * `source_path` - Path to the source file
    /// * `data` - The data bytes
    pub fn new(id: usize, source_path: impl Into<PathBuf>, data: Bytes) -> Self {
        Self {
            id,
            source_path: source_path.into(),
            data: BatchData::Owned(data),
            stream_id: 0,
            compression_plan: ChunkEncodingPlan::default(),
            force_raw_storage: false,
        }
    }

    /// Creates a new batch from a mapped region of a file.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this batch
    /// * `source_path` - Path to the source file
    /// * `map` - The mapped region of the file
    /// * `start` - The start of the mapped region
    /// * `end` - The end of the mapped region
    pub fn from_mapped(
        id: usize,
        source_path: impl Into<PathBuf>,
        map: Arc<Mmap>,
        start: usize,
        end: usize,
    ) -> Self {
        Self {
            id,
            source_path: source_path.into(),
            data: BatchData::Mapped { map, start, end },
            stream_id: 0,
            compression_plan: ChunkEncodingPlan::default(),
            force_raw_storage: false,
        }
    }

    /// Returns the length of the batch data.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the batch data is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a reference to the underlying data as a byte slice.
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    /// Converts the batch data to an owned Bytes object.
    pub fn to_owned(&self) -> Bytes {
        self.data.to_owned()
    }
}

impl From<Batch> for Bytes {
    fn from(batch: Batch) -> Self {
        batch.to_owned()
    }
}

impl BatchData {
    /// Returns the length of the data.
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(data) => data.len(),
            Self::Mapped {
                map: _g,
                start,
                end,
            } => end - start,
        }
    }

    /// Returns true if the data is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to the data as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(data) => &data[..],
            Self::Mapped { map, start, end } => &map[*start..*end],
        }
    }

    /// Converts the data to an owned Bytes object.
    ///
    /// If the data is already owned, it returns a clone of the Bytes object
    /// (which is a cheap reference-counted operation). If the data is mapped,
    /// it copies the mapped region into a new Bytes object.
    pub fn to_owned(&self) -> Bytes {
        match self {
            Self::Owned(data) => data.clone(),
            Self::Mapped { map, start, end } => Bytes::copy_from_slice(&map[*start..*end]),
        }
    }
}

impl Clone for CompressedPayload {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(data) => Self::Owned(data.clone()),
            Self::Bytes(data) => Self::Bytes(data.clone()),
            Self::Pooled(data) => Self::Owned(data.as_slice().to_vec()),
            Self::Mapped { map, start, end } => Self::Mapped {
                map: Arc::clone(map),
                start: *start,
                end: *end,
            },
        }
    }
}

impl CompressedPayload {
    /// Returns the length of the payload.
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(data) => data.len(),
            Self::Bytes(data) => data.len(),
            Self::Pooled(data) => data.len(),
            Self::Mapped { start, end, .. } => end - start,
        }
    }

    /// Returns true if the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the payload as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(data) => data.as_slice(),
            Self::Bytes(data) => data.as_ref(),
            Self::Pooled(data) => data.as_slice(),
            Self::Mapped { map, start, end } => &map[*start..*end],
        }
    }

    /// Copies bytes into a pooled buffer.
    pub fn copy_from_slice_in_pool(data: &[u8], pool: &BufferPool) -> Self {
        let mut pooled = pool.acquire();
        pooled.extend_from_slice(data);
        Self::Pooled(pooled)
    }

    /// Moves an owned vector into a pooled buffer slot for reuse.
    pub fn from_vec_in_pool(mut data: Vec<u8>, pool: &BufferPool) -> Self {
        let mut pooled = pool.acquire();
        std::mem::swap(pooled.as_mut_vec(), &mut data);
        Self::Pooled(pooled)
    }

    /// Converts batch input data into a stored payload, reusing mmap regions when possible.
    pub fn from_batch_data_in_pool(data: BatchData, pool: &BufferPool) -> Self {
        let _ = pool;
        match data {
            BatchData::Owned(data) => Self::Bytes(data),
            BatchData::Mapped { map, start, end } => Self::Mapped { map, start, end },
        }
    }
}

impl From<Vec<u8>> for CompressedPayload {
    fn from(data: Vec<u8>) -> Self {
        Self::Owned(data)
    }
}

impl From<Bytes> for CompressedPayload {
    fn from(data: Bytes) -> Self {
        Self::Bytes(data)
    }
}

impl From<PooledBuffer> for CompressedPayload {
    fn from(data: PooledBuffer) -> Self {
        Self::Pooled(data)
    }
}

impl From<BatchData> for Bytes {
    fn from(data: BatchData) -> Self {
        data.to_owned()
    }
}

/// Compression metadata carried per block.
///
/// Encapsulates the algorithm and whether raw passthrough was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionMeta {
    /// Algorithm used for compression.
    pub algo: CompressionAlgo,
    /// Whether the data is stored in raw format (e.g., if compression failed to reduce size).
    pub raw_passthrough: bool,
    /// Optional archive dictionary id used by the codec for this block.
    pub dictionary_id: u8,
}

impl CompressionMeta {
    /// Creates a new compression metadata object.
    pub fn new(algo: CompressionAlgo, raw_passthrough: bool) -> Self {
        Self {
            algo,
            raw_passthrough,
            dictionary_id: 0,
        }
    }

    /// Attaches an archive dictionary id to the compression metadata.
    pub fn with_dictionary_id(mut self, dictionary_id: u8) -> Self {
        self.dictionary_id = dictionary_id;
        self
    }

    /// Encodes compression metadata into OXZ compression flags.
    ///
    /// Layout:
    /// - Bits 0..=2: algorithm selector (001 LZ4, 010 Zstd, 011 LZMA, 100 ZPAQ)
    /// - Bit 3: raw passthrough marker
    /// - Bits 4..=7: reserved
    pub fn to_flags(self) -> u8 {
        let algo = self.algo.to_flags();
        let raw = if self.raw_passthrough { 1 << 3 } else { 0 };
        algo | raw
    }

    /// Decodes compression metadata from OXZ compression flags.
    pub fn from_flags(flags: u8) -> Result<Self> {
        if flags & 0b1111_0000 != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid compression flags reserved bits",
            ));
        }

        let algo = CompressionAlgo::from_flags(flags)?;
        let raw_passthrough = flags & (1 << 3) != 0;

        Ok(Self {
            algo,
            raw_passthrough,
            dictionary_id: 0,
        })
    }
}

/// A compressed data block with metadata.
///
/// Compressed blocks store the encoded payload, compression parameters, and
/// integrity checksum needed to restore the original bytes.
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    /// Unique identifier for this block.
    pub id: usize,
    /// Logical stream identifier stored in the chunk descriptor.
    pub stream_id: u32,
    /// The compressed (or raw) data bytes.
    pub data: CompressedPayload,
    /// Compression algorithm used.
    pub compression: CompressionAlgo,
    /// Whether the data is stored raw.
    pub raw_passthrough: bool,
    /// Archive dictionary id used for this block, if any.
    pub dictionary_id: u8,
    /// Original uncompressed length.
    pub original_len: u64,
    /// CRC32C checksum of the compressed data.
    pub crc32: u32,
}

impl CompressedBlock {
    /// Creates a new compressed block with a computed checksum.
    pub fn new(
        id: usize,
        data: impl Into<CompressedPayload>,
        compression: CompressionAlgo,
        original_len: u64,
    ) -> Self {
        Self::with_compression_meta(
            id,
            data,
            CompressionMeta::new(compression, false),
            original_len,
        )
    }

    /// Creates a new compressed block using explicit compression metadata.
    pub fn with_compression_meta(
        id: usize,
        data: impl Into<CompressedPayload>,
        compression_meta: CompressionMeta,
        original_len: u64,
    ) -> Self {
        let data = data.into();
        Self {
            id,
            stream_id: 0,
            crc32: compute_checksum(data.as_slice()),
            data,
            compression: compression_meta.algo,
            raw_passthrough: compression_meta.raw_passthrough,
            dictionary_id: if compression_meta.raw_passthrough {
                0
            } else {
                compression_meta.dictionary_id
            },
            original_len,
        }
    }

    /// Creates a new compressed block using an explicit planner encoding plan.
    pub fn with_chunk_encoding(
        id: usize,
        stream_id: u32,
        data: impl Into<CompressedPayload>,
        encoding_plan: ChunkEncodingPlan,
        raw_passthrough: bool,
        original_len: u64,
    ) -> Self {
        Self::with_chunk_encoding_and_dictionary(
            id,
            stream_id,
            data,
            encoding_plan,
            raw_passthrough,
            original_len,
            0,
        )
    }

    /// Creates a new compressed block using an explicit planner encoding plan and dictionary id.
    pub fn with_chunk_encoding_and_dictionary(
        id: usize,
        stream_id: u32,
        data: impl Into<CompressedPayload>,
        encoding_plan: ChunkEncodingPlan,
        raw_passthrough: bool,
        original_len: u64,
        dictionary_id: u8,
    ) -> Self {
        let data = data.into();
        Self {
            id,
            stream_id,
            crc32: compute_checksum(data.as_slice()),
            data,
            compression: encoding_plan.algo,
            raw_passthrough,
            dictionary_id: if raw_passthrough { 0 } else { dictionary_id },
            original_len,
        }
    }

    /// Returns the compression metadata for this block.
    pub fn compression_meta(&self) -> CompressionMeta {
        CompressionMeta {
            algo: self.compression,
            raw_passthrough: self.raw_passthrough,
            dictionary_id: self.dictionary_id,
        }
    }

    /// Verifies the stored CRC32C checksum against the current payload.
    pub fn verify_crc32(&self) -> bool {
        self.crc32 == compute_checksum(self.data.as_slice())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgo {
    /// LZ4 fast compression - Fast
    Lz4,
    /// LZMA/XZ compression with high-ratio presets.
    Lzma,
    /// Zstandard compression with tunable ratio/speed levels.
    Zstd,
}

impl CompressionAlgo {
    /// Encodes the compression algorithm into OXZ compression flags.
    pub fn to_flags(self) -> u8 {
        match self {
            Self::Lz4 => 0b001,
            Self::Zstd => 0b010,
            Self::Lzma => 0b011,
        }
    }

    /// Decodes OXZ compression flags into a compression algorithm.
    pub fn from_flags(flags: u8) -> Result<Self> {
        match flags & 0b111 {
            0b001 => Ok(Self::Lz4),
            0b010 => Ok(Self::Zstd),
            0b011 => Ok(Self::Lzma),
            _ => Err(OxideError::InvalidFormat("invalid compression flags")),
        }
    }
}
