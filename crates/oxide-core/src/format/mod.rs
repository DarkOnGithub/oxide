//! File format detection module.
//!
//! This module provides automatic file format detection using the `infer`
//! crate along with additional heuristics for edge cases.

pub mod detector;
pub mod oxz;

pub use detector::FormatDetector;
pub use oxz::{
    ArchiveBlockWriter, ArchiveManifest, ArchiveReader, ArchiveWriter, BlockHeader, BlockIterator,
    CHUNK_DESCRIPTOR_SIZE, CORE_SECTION_COUNT, ChunkDescriptor, DEFAULT_REORDER_PENDING_LIMIT,
    FEATURE_DEDUP_REFERENCES, FOOTER_SIZE, Footer, GLOBAL_HEADER_SIZE, GlobalHeader, OXZ_MAGIC,
    OXZ_VERSION, ReorderBuffer, SECTION_TABLE_ENTRY_SIZE, SectionTableEntry, SectionType,
    SeekableArchiveWriter, StoredDictionary,
};
