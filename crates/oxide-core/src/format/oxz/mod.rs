mod consts;
mod dictionary_store;
mod headers;
mod reader;
mod reorder;
mod writer;

pub use consts::{
    CHUNK_DESCRIPTOR_SIZE, CORE_SECTION_COUNT, DEFAULT_REORDER_PENDING_LIMIT,
    FEATURE_DEDUP_REFERENCES, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_END_MAGIC, OXZ_MAGIC,
    OXZ_VERSION, SECTION_TABLE_ENTRY_SIZE,
};
pub use dictionary_store::{StoredDictionary, decode_dictionary_store, encode_dictionary_store};
pub use headers::{
    BlockHeader, ChunkDescriptor, Footer, GlobalHeader, SectionTableEntry, SectionType,
};
pub use reader::{ArchiveReader, BlockIterator};
pub use reorder::ReorderBuffer;
pub use writer::{ArchiveBlockWriter, ArchiveWriter, SeekableArchiveWriter};
