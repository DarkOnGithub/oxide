//! File format detection module.
//!
//! This module provides automatic file format detection using the `infer`
//! crate along with additional heuristics for edge cases.

pub mod detector;
pub mod oxz;

pub use detector::{
    should_force_raw_storage, should_force_raw_storage_by_extension,
    should_force_raw_storage_by_signature, FormatDetector,
};
pub use oxz::{
    ArchiveBlockWriter, ArchiveManifest, ArchiveMetadata, ArchiveReader, ArchiveWriter,
    BlockIterator, ChunkDescriptor, Footer, GlobalHeader, ReorderBuffer, SeekableArchiveWriter,
    ARCHIVE_METADATA_SIZE, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE,
    DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_MAGIC, OXZ_VERSION,
};
