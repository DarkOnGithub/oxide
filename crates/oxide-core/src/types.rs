use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
    pub data: Bytes,
    pub file_type_hint: FileFormat,
}

/// A batch of data extracted from a file for processing.
#[derive(Debug, Clone)]
pub enum BatchData {
    Owned(Bytes),
    Mapped{
        map: std::sync::Arc<memmap2::Mmap>,
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
            data,
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
            data,
            file_type_hint,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl BatchData {
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(data) => data.len(),
            Self::Mapped { map: _g, start, end } => end - start,
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
