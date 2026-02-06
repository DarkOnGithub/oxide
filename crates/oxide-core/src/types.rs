use std::path::PathBuf;

use bytes::Bytes;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::OxideError;

pub type Result<T> = std::result::Result<T, OxideError>;

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
#[derive(Debug, Clone)]
pub enum BatchData {
    Owned(Bytes),
    Mapped {
        map: Arc<Mmap>,
        start: usize,
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

    pub fn len(&self) -> usize {
        self.data.len()
    }

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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

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

/// A compressed data block with metadata.
///
/// Compressed blocks store the compressed data along with information
/// about the compression algorithm used, preprocessing strategy, and
/// integrity checksum.
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    pub id: usize,
    pub data: Vec<u8>,
    pub pre_proc: PreProcessingStrategy,
    pub compression: CompressionAlgo,
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
        let crc32 = crc32fast::hash(&data);
        Self {
            id,
            data,
            pre_proc,
            compression,
            original_len,
            crc32,
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
