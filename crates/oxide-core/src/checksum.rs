#[inline]
pub fn compute_checksum(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}
