mod consts;
mod dictionary_store;
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
pub use dictionary_store::{StoredDictionary, decode_dictionary_store, encode_dictionary_store};
pub use headers::{
    ArchiveMetadata, BlockHeader, ChunkDescriptor, Footer, GlobalHeader, decode_chunk_table,
    encode_chunk_table,
};
pub use manifest::ArchiveManifest;
pub use reader::{ArchiveReader, BlockIterator};
pub use reorder::ReorderBuffer;
pub use writer::{ArchiveBlockWriter, ArchiveWriter, SeekableArchiveWriter};
