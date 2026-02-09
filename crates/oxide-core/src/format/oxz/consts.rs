pub const OXZ_MAGIC: [u8; 4] = *b"OXZ\0";
pub const OXZ_END_MAGIC: [u8; 4] = *b"END\0";
pub const OXZ_VERSION: u16 = 1;

pub const GLOBAL_HEADER_SIZE: usize = 16;
pub const BLOCK_HEADER_SIZE: usize = 24;
pub const FOOTER_SIZE: usize = 8;

pub const DEFAULT_REORDER_PENDING_LIMIT: usize = 1024;
