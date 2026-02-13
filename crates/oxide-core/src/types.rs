use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    pub file_type_hint: FileFormat,
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
            file_type_hint: FileFormat::Common,
        }
    }

    /// Creates a new batch with an explicit file type hint.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this batch
    /// * `source_path` - Path to the source file
    /// * `data` - The data bytes
    /// * `file_type_hint` - Explicit file format hint
    pub fn with_hint(
        id: usize,
        source_path: impl Into<PathBuf>,
        data: Bytes,
        file_type_hint: FileFormat,
    ) -> Self {
        Self {
            id,
            source_path: source_path.into(),
            data: BatchData::Owned(data),
            file_type_hint,
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
    /// * `file_type_hint` - Explicit file format hint
    pub fn from_mapped(
        id: usize,
        source_path: impl Into<PathBuf>,
        map: Arc<Mmap>,
        start: usize,
        end: usize,
        file_type_hint: FileFormat,
    ) -> Self {
        Self {
            id,
            source_path: source_path.into(),
            data: BatchData::Mapped { map, start, end },
            file_type_hint,
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
    /// - Bits 0..=1: algorithm (01 LZ4, 10 LZMA, 11 Deflate)
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
/// Compressed blocks store the compressed data along with information
/// about the compression algorithm used, preprocessing strategy, and
/// integrity checksum.
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    /// Unique identifier for this block.
    pub id: usize,
    /// The compressed (or raw) data bytes.
    pub data: Vec<u8>,
    /// Preprocessing strategy applied before compression.
    pub pre_proc: PreProcessingStrategy,
    /// Compression algorithm used.
    pub compression: CompressionAlgo,
    /// Preset used for compression.
    pub compression_preset: CompressionPreset,
    /// Whether the data is stored raw.
    pub raw_passthrough: bool,
    /// Original uncompressed length.
    pub original_len: u64,
    /// CRC32 checksum of the compressed data
    pub crc32: u32,
}

impl CompressedBlock {
    /// Creates a new compressed block with an auto-computed CRC32 checksum.
    ///
    /// # Arguments
    /// * `id` - Unique identifier for this block
    /// * `data` - The compressed data
    /// * `pre_proc` - Preprocessing strategy used
    /// * `compression` - Compression algorithm used
    /// * `original_len` - Original uncompressed length
    pub fn new(
        id: usize,
        data: Vec<u8>,
        pre_proc: PreProcessingStrategy,
        compression: CompressionAlgo,
        original_len: u64,
    ) -> Self {
        Self::with_compression_meta(
            id,
            data,
            pre_proc,
            CompressionMeta::new(compression, CompressionPreset::Default, false),
            original_len,
        )
    }

    /// Creates a new compressed block using explicit compression metadata.
    pub fn with_compression_meta(
        id: usize,
        data: Vec<u8>,
        pre_proc: PreProcessingStrategy,
        compression_meta: CompressionMeta,
        original_len: u64,
    ) -> Self {
        let crc32 = crc32fast::hash(&data);
        Self {
            id,
            data,
            pre_proc,
            compression: compression_meta.algo,
            compression_preset: compression_meta.preset,
            raw_passthrough: compression_meta.raw_passthrough,
            original_len,
            crc32,
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

    /// Verifies the integrity of the compressed data using CRC32.
    pub fn verify_crc32(&self) -> bool {
        crc32fast::hash(&self.data) == self.crc32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileFormat {
    /// Plain text file
    Text,
    /// Binary data (executable, object code, etc.)
    Binary,
    /// Image file (PNG, JPEG, etc.)
    Image,
    /// Audio file (MP3, WAV, etc.)
    Audio,
    /// Common/generic format or unknown
    Common,
}

/// Preprocessing strategy applied before compression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreProcessingStrategy {
    Text(TextStrategy),
    Image(ImageStrategy),
    Audio(AudioStrategy),
    Binary(BinaryStrategy),
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TextStrategy {
    /// Byte Pair Encoding for text compression
    Bpe,
    /// Burrows-Wheeler Transform
    Bwt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageStrategy {
    /// YCoCg-R color space conversion
    YCoCgR,
    /// Paeth predictor (PNG-style)
    Paeth,
    /// LOCO-I predictor (JPEG-LS)
    LocoI,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioStrategy {
    /// Linear Predictive Coding
    Lpc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryStrategy {
    /// Branch Call Jump filter
    Bcj,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgo {
    /// LZ4 fast compression - Fast
    Lz4,
    /// LZMA high-ratio compression - Balanced
    Lzma,
    /// Deflate (zlib) compression - Ultra
    Deflate,
}

impl PreProcessingStrategy {
    /// Encodes the preprocessing strategy into OXZ strategy flags.
    ///
    /// Layout:
    /// - Bits 0-2: category
    /// - Bits 3-5: sub-strategy
    /// - Bits 6-7: reserved
    pub fn to_flags(&self) -> u8 {
        match self {
            Self::None => 0x00,
            Self::Text(TextStrategy::Bpe) => 0x01,
            Self::Text(TextStrategy::Bwt) => 0x01 | (1 << 3),
            Self::Image(ImageStrategy::YCoCgR) => 0x02,
            Self::Image(ImageStrategy::Paeth) => 0x02 | (1 << 3),
            Self::Image(ImageStrategy::LocoI) => 0x02 | (2 << 3),
            Self::Audio(AudioStrategy::Lpc) => 0x03,
            Self::Binary(BinaryStrategy::Bcj) => 0x04,
        }
    }

    /// Decodes OXZ strategy flags into a preprocessing strategy.
    pub fn from_flags(flags: u8) -> Result<Self> {
        if flags & 0b1100_0000 != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid strategy flags reserved bits",
            ));
        }

        let category = flags & 0x07;
        let sub_strategy = (flags >> 3) & 0x07;

        match (category, sub_strategy) {
            (0x00, 0x00) => Ok(Self::None),
            (0x01, 0x00) => Ok(Self::Text(TextStrategy::Bpe)),
            (0x01, 0x01) => Ok(Self::Text(TextStrategy::Bwt)),
            (0x02, 0x00) => Ok(Self::Image(ImageStrategy::YCoCgR)),
            (0x02, 0x01) => Ok(Self::Image(ImageStrategy::Paeth)),
            (0x02, 0x02) => Ok(Self::Image(ImageStrategy::LocoI)),
            (0x03, 0x00) => Ok(Self::Audio(AudioStrategy::Lpc)),
            (0x04, 0x00) => Ok(Self::Binary(BinaryStrategy::Bcj)),
            _ => Err(OxideError::InvalidFormat("invalid strategy flags")),
        }
    }
}

impl CompressionAlgo {
    /// Encodes the compression algorithm into OXZ compression flags.
    pub fn to_flags(self) -> u8 {
        match self {
            Self::Lz4 => 0x01,
            Self::Lzma => 0x02,
            Self::Deflate => 0x03,
        }
    }

    /// Decodes OXZ compression flags into a compression algorithm.
    pub fn from_flags(flags: u8) -> Result<Self> {
        match flags {
            0x01 => Ok(Self::Lz4),
            0x02 => Ok(Self::Lzma),
            0x03 => Ok(Self::Deflate),
            _ => Err(OxideError::InvalidFormat("invalid compression flags")),
        }
    }
}
