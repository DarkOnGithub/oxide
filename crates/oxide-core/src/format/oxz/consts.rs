/// Magic bytes at the start of an OXZ archive.
pub const OXZ_MAGIC: [u8; 4] = *b"OXZ\0";
/// Magic bytes at the start of the OXZ footer.
pub const OXZ_END_MAGIC: [u8; 4] = *b"END\0";
/// Current version of the OXZ format.
pub const OXZ_VERSION: u16 = 2;

/// Fixed size of the global header in bytes.
pub const GLOBAL_HEADER_SIZE: usize = 8;
/// OXZ v2 no longer stores a dedicated archive metadata section.
pub const ARCHIVE_METADATA_SIZE: usize = 0;
/// OXZ v2 no longer stores a dedicated chunk table header.
pub const CHUNK_TABLE_HEADER_SIZE: usize = 0;
/// Fixed size of each chunk descriptor in bytes.
pub const CHUNK_DESCRIPTOR_SIZE: usize = 14;
/// Fixed size of the footer in bytes.
pub const FOOTER_SIZE: usize = 40;

/// Default limit for pending blocks in the reorder buffer.
pub const DEFAULT_REORDER_PENDING_LIMIT: usize = 1024;
