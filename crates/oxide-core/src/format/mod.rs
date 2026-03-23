//! Archive format helpers and raw-storage policy.

pub mod oxz;
pub mod raw_storage;

pub use oxz::{
    ArchiveBlockWriter, ArchiveManifest, ArchiveMetadata, ArchiveReader, ArchiveWriter,
    BlockIterator, ChunkDescriptor, Footer, GlobalHeader, ReorderBuffer, SeekableArchiveWriter,
    ARCHIVE_METADATA_SIZE, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE,
    DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_MAGIC, OXZ_VERSION,
};
pub use raw_storage::{should_force_raw_storage, should_force_raw_storage_by_extension};
