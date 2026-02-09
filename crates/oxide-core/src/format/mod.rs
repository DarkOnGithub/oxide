//! File format detection module.
//!
//! This module provides automatic file format detection using the `infer`
//! crate along with additional heuristics for edge cases.

pub mod detector;
pub mod oxz;

pub use detector::FormatDetector;
pub use oxz::{
    ArchiveReader, ArchiveWriter, BlockHeader, BlockIterator, Footer, GlobalHeader, ReorderBuffer,
    BLOCK_HEADER_SIZE, DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_MAGIC,
    OXZ_VERSION,
};
