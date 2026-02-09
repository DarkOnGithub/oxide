//! File format detection module.
//!
//! This module provides automatic file format detection using the `infer`
//! crate along with additional heuristics for edge cases.

pub mod detector;
pub mod oxz;

pub use detector::FormatDetector;
pub use oxz::{
    ArchiveReader, ArchiveWriter, BLOCK_HEADER_SIZE, BlockHeader, BlockIterator,
    DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, Footer, GLOBAL_HEADER_SIZE, GlobalHeader,
    OXZ_MAGIC, OXZ_VERSION, ReorderBuffer,
};
