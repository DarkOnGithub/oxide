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
    /// Compression preset selected for the chunk.
    pub preset: CompressionPreset,
    /// Optional explicit zstd compression level used only by the encoder.
    pub zstd_level: Option<i32>,
}

impl ChunkEncodingPlan {
    /// Creates a new chunk encoding plan.
    pub const fn new(algo: CompressionAlgo, preset: CompressionPreset) -> Self {
        Self {
            algo,
            preset,
            zstd_level: None,
        }
    }

    /// Attaches an explicit zstd level override for encoding.
    pub const fn with_zstd_level(mut self, zstd_level: Option<i32>) -> Self {
        self.zstd_level = zstd_level;
        self
    }
    /// Builds compression metadata for the final stored payload.
    pub fn compression_meta(self, raw_passthrough: bool) -> CompressionMeta {
        CompressionMeta::new(self.algo, self.preset, raw_passthrough)
    }
}

impl Default for ChunkEncodingPlan {
    fn default() -> Self {
        Self::new(CompressionAlgo::Lz4, CompressionPreset::Default)
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
        match data {
            BatchData::Owned(data) => Self::copy_from_slice_in_pool(data.as_ref(), pool),
            BatchData::Mapped { map, start, end } => Self::Mapped { map, start, end },
        }
    }
}

impl From<Vec<u8>> for CompressedPayload {
    fn from(data: Vec<u8>) -> Self {
        Self::Owned(data)
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

/// Compression preset used by a codec.
///
/// Presets allow balancing between compression speed and ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionPreset {
    /// High-speed compression with lower ratio.
    Fast,
    /// Balanced compression speed and ratio.
    Default,
    /// High-ratio compression with lower speed.
    High,
}

impl CompressionPreset {
    /// Encodes the preset into bits 3..=4 of compression flags.
    pub fn to_flag_bits(self) -> u8 {
        match self {
            Self::Fast => 0b00 << 3,
            Self::Default => 0b01 << 3,
            Self::High => 0b10 << 3,
        }
    }

    /// Decodes bits 3..=4 from compression flags.
    pub fn from_flag_bits(flags: u8) -> Result<Self> {
        match (flags >> 3) & 0b11 {
            0b00 => Ok(Self::Fast),
            0b01 => Ok(Self::Default),
            0b10 => Ok(Self::High),
            _ => Err(OxideError::InvalidFormat(
                "invalid compression preset flags",
            )),
        }
    }
}

/// Compression metadata carried per block.
///
/// Encapsulates the algorithm, preset, and whether raw passthrough was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionMeta {
    /// Algorithm used for compression.
    pub algo: CompressionAlgo,
    /// Preset used for compression.
    pub preset: CompressionPreset,
    /// Whether the data is stored in raw format (e.g., if compression failed to reduce size).
    pub raw_passthrough: bool,
}

impl CompressionMeta {
    /// Creates a new compression metadata object.
    pub fn new(algo: CompressionAlgo, preset: CompressionPreset, raw_passthrough: bool) -> Self {
        Self {
            algo,
            preset,
            raw_passthrough,
        }
    }

    /// Encodes compression metadata into OXZ compression flags.
    ///
    /// Layout:
    /// - Bits 0..=1: algorithm (01 LZ4, 10 Zstd, 11 LZMA)
    /// - Bit 2: raw passthrough marker
    /// - Bits 3..=4: preset (00 Fast, 01 Default, 10 High)
    /// - Bits 5..=7: reserved (must be zero)
    pub fn to_flags(self) -> u8 {
        let algo = self.algo.to_flags();
        let raw = if self.raw_passthrough { 1 << 2 } else { 0 };
        algo | raw | self.preset.to_flag_bits()
    }

    /// Decodes compression metadata from OXZ compression flags.
    pub fn from_flags(flags: u8) -> Result<Self> {
        if flags & 0b1110_0000 != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid compression flags reserved bits",
            ));
        }

        let algo = CompressionAlgo::from_flags(flags & 0b11)?;
        let preset = CompressionPreset::from_flag_bits(flags)?;
        let raw_passthrough = flags & (1 << 2) != 0;

        Ok(Self {
            algo,
            preset,
            raw_passthrough,
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
    /// Preset used for compression.
    pub compression_preset: CompressionPreset,
    /// Whether the data is stored raw.
    pub raw_passthrough: bool,
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
            CompressionMeta::new(compression, CompressionPreset::Default, false),
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
            compression_preset: compression_meta.preset,
            raw_passthrough: compression_meta.raw_passthrough,
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
        let data = data.into();
        Self {
            id,
            stream_id,
            crc32: compute_checksum(data.as_slice()),
            data,
            compression: encoding_plan.algo,
            compression_preset: encoding_plan.preset,
            raw_passthrough,
            original_len,
        }
    }

    /// Returns the compression metadata for this block.
    pub fn compression_meta(&self) -> CompressionMeta {
        CompressionMeta {
            algo: self.compression,
            preset: self.compression_preset,
            raw_passthrough: self.raw_passthrough,
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
            Self::Lz4 => 0x01,
            Self::Zstd => 0x02,
            Self::Lzma => 0x03,
        }
    }

    /// Decodes OXZ compression flags into a compression algorithm.
    pub fn from_flags(flags: u8) -> Result<Self> {
        match flags {
            0x01 => Ok(Self::Lz4),
            0x02 => Ok(Self::Zstd),
            0x03 => Ok(Self::Lzma),
            _ => Err(OxideError::InvalidFormat("invalid compression flags")),
        }
    }
}
