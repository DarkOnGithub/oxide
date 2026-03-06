/// Magic bytes at the start of an OXZ archive.
pub const OXZ_MAGIC: [u8; 4] = *b"OXZ\0";
/// Magic bytes at the start of the OXZ footer.
pub const OXZ_END_MAGIC: [u8; 4] = *b"END\0";
/// Current version of the OXZ format.
pub const OXZ_VERSION: u16 = 1;

/// Fixed size of the global header in bytes.
pub const GLOBAL_HEADER_SIZE: usize = 20;
/// Fixed size of each section table entry in bytes.
pub const SECTION_TABLE_ENTRY_SIZE: usize = 24;
/// Number of core sections emitted by v1 containers.
pub const CORE_SECTION_COUNT: u16 = 6;
/// Fixed size of each chunk descriptor in bytes.
pub const CHUNK_DESCRIPTOR_SIZE: usize = 40;
/// Fixed size of the footer in bytes.
pub const FOOTER_SIZE: usize = 8;

/// Default limit for pending blocks in the reorder buffer.
pub const DEFAULT_REORDER_PENDING_LIMIT: usize = 1024;

/// Feature bit indicating the dedup reference section is present and populated.
pub const FEATURE_DEDUP_REFERENCES: u16 = 1 << 0;
