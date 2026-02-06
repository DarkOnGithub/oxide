use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::OxideError;

pub type Result<T> = std::result::Result<T, OxideError>;

#[derive(Debug, Clone)]
pub struct Batch {
    pub id: usize,
    pub source_path: PathBuf,
    pub data: Bytes,
    pub file_type_hint: FileFormat,
}

impl Batch {
    pub fn new(id: usize, source_path: impl Into<PathBuf>, data: Bytes) -> Self {
        Self {
            id,
            source_path: source_path.into(),
            data,
            file_type_hint: FileFormat::Unknown,
        }
    }

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

#[derive(Debug, Clone)]
pub struct CompressedBlock {
    pub id: usize,
    pub data: Vec<u8>,
    pub pre_proc: PreProcessingStrategy,
    pub compression: CompressionAlgo,
    pub original_len: u64,
    pub crc32: u32,
}

impl CompressedBlock {
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

    pub fn verify_crc32(&self) -> bool {
        crc32fast::hash(&self.data) == self.crc32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileFormat {
    Text,
    Binary,
    Image,
    Audio,
    Unknown,
}

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
    Bpe,
    Bwt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageStrategy {
    YCoCgR,
    Paeth,
    LocoI,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioStrategy {
    Lpc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryStrategy {
    Bcj,
    Bcj2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgo {
    Lz4,
    Lzma,
    Deflate,
}
