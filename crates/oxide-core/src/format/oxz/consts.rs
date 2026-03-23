/// Magic bytes at the start of an OXZ archive.
pub const OXZ_MAGIC: [u8; 4] = *b"OXZ\0";
/// Magic bytes at the start of the OXZ footer.
pub const OXZ_END_MAGIC: [u8; 4] = *b"END\0";
/// Current version of the OXZ format.
pub const OXZ_VERSION: u16 = 2;

/// Fixed size of the global header in bytes.
pub const GLOBAL_HEADER_SIZE: usize = 48;
/// Fixed size of the archive metadata section in bytes.
pub const ARCHIVE_METADATA_SIZE: usize = 8;
/// Fixed size of the chunk table header in bytes.
pub const CHUNK_TABLE_HEADER_SIZE: usize = 12;
/// Fixed size of each chunk descriptor in bytes.
pub const CHUNK_DESCRIPTOR_SIZE: usize = 22;
/// Fixed size of the footer in bytes.
pub const FOOTER_SIZE: usize = 8;

/// Default limit for pending blocks in the reorder buffer.
pub const DEFAULT_REORDER_PENDING_LIMIT: usize = 1024;
