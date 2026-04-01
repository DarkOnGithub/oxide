mod consts;
mod headers;
mod manifest;
mod reader;
mod reorder;
mod writer;

pub use consts::{
    ARCHIVE_METADATA_SIZE, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE,
    DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_END_MAGIC, OXZ_MAGIC,
    OXZ_VERSION,
};
pub use headers::{
    ArchiveMetadata, ChunkDescriptor, Footer, GlobalHeader, decode_chunk_table, encode_chunk_table,
};
pub use manifest::ArchiveManifest;
pub use reader::{ArchiveReader, BlockIterator};
pub use reorder::ReorderBuffer;
pub use writer::DEFAULT_DEDUP_WINDOW_BLOCKS;
pub use writer::{ArchiveBlockWriter, ArchiveWriter, SeekableArchiveWriter};
