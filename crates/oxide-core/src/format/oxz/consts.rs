/// Magic bytes at the start of an OXZ archive.
pub const OXZ_MAGIC: [u8; 4] = *b"OXZ\0";
/// Magic bytes at the start of the OXZ footer.
pub const OXZ_END_MAGIC: [u8; 4] = *b"END\0";
/// Current version of the OXZ format.
pub const OXZ_VERSION: u16 = 1;

/// Fixed size of the global header in bytes.
pub const GLOBAL_HEADER_SIZE: usize = 16;
/// Fixed size of each block header in bytes.
pub const BLOCK_HEADER_SIZE: usize = 24;
/// Fixed size of the footer in bytes.
pub const FOOTER_SIZE: usize = 8;

/// Default limit for pending blocks in the reorder buffer.
pub const DEFAULT_REORDER_PENDING_LIMIT: usize = 1024;
