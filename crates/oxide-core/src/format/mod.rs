//! Archive format helpers and raw-storage policy.

pub mod oxz;
pub mod raw_storage;

pub use oxz::{
    ARCHIVE_METADATA_SIZE, ArchiveBlockWriter, ArchiveManifest, ArchiveMetadata, ArchiveReader,
    ArchiveWriter, BlockIterator, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE, ChunkDescriptor,
    DEFAULT_DEDUP_WINDOW_BLOCKS, DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, Footer,
    GLOBAL_HEADER_SIZE, GlobalHeader, OXZ_MAGIC, OXZ_VERSION, ReorderBuffer, SeekableArchiveWriter,
};
pub use raw_storage::{should_force_raw_storage, should_force_raw_storage_by_extension};
