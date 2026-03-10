//! File format detection module.
//!
//! This module provides automatic file format detection using the `infer`
//! crate along with additional heuristics for edge cases.

pub mod detector;
pub mod oxz;

pub use detector::FormatDetector;
pub use oxz::{
    ARCHIVE_METADATA_SIZE, ArchiveBlockWriter, ArchiveManifest, ArchiveMetadata, ArchiveReader,
    ArchiveWriter, BlockHeader, BlockIterator, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE,
    ChunkDescriptor, DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, Footer, GLOBAL_HEADER_SIZE,
    GlobalHeader, OXZ_MAGIC, OXZ_VERSION, ReorderBuffer, SeekableArchiveWriter,
    StoredDictionary,
};
