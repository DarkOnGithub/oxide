use std::io::{Read, Seek, SeekFrom, Write};

use crate::{
    ArchiveSourceKind, CompressedBlock, CompressionAlgo, CompressionMeta, OxideError, Result,
};

use super::{
    CHUNK_DESCRIPTOR_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_END_MAGIC, OXZ_MAGIC, OXZ_VERSION,
};

const FOOTER_VERSION: u16 = 2;

const HEADER_FLAG_DIRECTORY: u16 = 1 << 0;
const HEADER_FLAG_PATH_PREFIX: u16 = 1 << 1;
const HEADER_FLAG_METADATA_INHERIT: u16 = 1 << 2;
const HEADER_FLAG_IMPLICIT_CONTENT_OFFSETS: u16 = 1 << 3;
const HEADER_FLAG_IMPLICIT_CHUNK_OFFSETS: u16 = 1 << 4;
const SUPPORTED_HEADER_FLAGS: u16 = HEADER_FLAG_DIRECTORY
    | HEADER_FLAG_PATH_PREFIX
    | HEADER_FLAG_METADATA_INHERIT
    | HEADER_FLAG_IMPLICIT_CONTENT_OFFSETS
    | HEADER_FLAG_IMPLICIT_CHUNK_OFFSETS;

/// Resolved OXZ container header.
///
/// On disk, only the prefix (`magic`, `version`, `flags`) is stored at the start
/// of the archive. The remaining offsets are resolved from the footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub flags: u16,
    pub block_count: u32,
    pub payload_offset: u64,
    pub entry_table_offset: u64,
    pub entry_table_len: u32,
    pub chunk_table_offset: u64,
    pub chunk_table_len: u32,
    pub footer_offset: u64,
}

impl GlobalHeader {
    pub fn new(
        source_kind: ArchiveSourceKind,
        block_count: u32,
        entry_table_offset: u64,
        entry_table_len: u32,
        chunk_table_offset: u64,
        chunk_table_len: u32,
        footer_offset: u64,
    ) -> Self {
        Self {
            magic: OXZ_MAGIC,
            version: OXZ_VERSION,
            flags: flags_for_source_kind(source_kind),
            block_count,
            payload_offset: GLOBAL_HEADER_SIZE as u64,
            entry_table_offset,
            entry_table_len,
            chunk_table_offset,
            chunk_table_len,
            footer_offset,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        let start = reader.stream_position()?;

        let mut prefix = [0u8; GLOBAL_HEADER_SIZE];
        reader.read_exact(&mut prefix)?;
        let (magic, version, flags) = parse_prefix(prefix)?;

        let file_len = reader.seek(SeekFrom::End(0))?;
        if file_len < (GLOBAL_HEADER_SIZE + FOOTER_SIZE) as u64 {
            return Err(OxideError::InvalidFormat("archive is too short"));
        }

        let footer_offset = file_len - FOOTER_SIZE as u64;
        reader.seek(SeekFrom::Start(footer_offset))?;
        let footer = Footer::read(reader)?;
        reader.seek(SeekFrom::Start(start + GLOBAL_HEADER_SIZE as u64))?;

        let header = Self {
            magic,
            version,
            flags,
            block_count: footer.block_count,
            payload_offset: GLOBAL_HEADER_SIZE as u64,
            entry_table_offset: footer.entry_table_offset,
            entry_table_len: footer.entry_table_len,
            chunk_table_offset: footer.chunk_table_offset,
            chunk_table_len: footer.chunk_table_len,
            footer_offset,
        };
        header.validate(file_len)?;
        Ok(header)
    }

    pub fn to_bytes(&self) -> [u8; GLOBAL_HEADER_SIZE] {
        let mut bytes = [0u8; GLOBAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(&self.magic);
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.flags.to_le_bytes());
        bytes
    }

    pub fn source_kind(&self) -> ArchiveSourceKind {
        if self.flags & HEADER_FLAG_DIRECTORY != 0 {
            ArchiveSourceKind::Directory
        } else {
            ArchiveSourceKind::File
        }
    }

    pub fn validate(&self, file_len: u64) -> Result<()> {
        if self.payload_offset != GLOBAL_HEADER_SIZE as u64 {
            return Err(OxideError::InvalidFormat("invalid payload offset"));
        }

        if self.flags & !SUPPORTED_HEADER_FLAGS != 0 {
            return Err(OxideError::InvalidFormat("unsupported OXZ header flags"));
        }

        if self.entry_table_offset < self.payload_offset {
            return Err(OxideError::InvalidFormat(
                "manifest offset overlaps payload prefix",
            ));
        }

        let entry_table_end = self
            .entry_table_offset
            .checked_add(self.entry_table_len as u64)
            .ok_or(OxideError::InvalidFormat("manifest range overflow"))?;
        if entry_table_end != self.chunk_table_offset {
            return Err(OxideError::InvalidFormat(
                "manifest and chunk table must be contiguous",
            ));
        }

        let chunk_table_end = self
            .chunk_table_offset
            .checked_add(self.chunk_table_len as u64)
            .ok_or(OxideError::InvalidFormat("chunk table range overflow"))?;
        if chunk_table_end != self.footer_offset {
            return Err(OxideError::InvalidFormat("chunk table must end at footer"));
        }

        let expected_file_len = self
            .footer_offset
            .checked_add(FOOTER_SIZE as u64)
            .ok_or(OxideError::InvalidFormat("archive length overflow"))?;
        if expected_file_len != file_len {
            return Err(OxideError::InvalidFormat(
                "archive length does not match declared footer offset",
            ));
        }

        Ok(())
    }
}

/// Archive-level metadata resolved from the global header flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveMetadata {
    pub source_kind: ArchiveSourceKind,
}

impl ArchiveMetadata {
    pub fn new(source_kind: ArchiveSourceKind) -> Self {
        Self { source_kind }
    }

    pub fn from_header(header: GlobalHeader) -> Self {
        Self {
            source_kind: header.source_kind(),
        }
    }
}

/// Chunk-level metadata descriptor.
///
/// Payload offsets are derived from the compact chunk table and populated only
/// in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkDescriptor {
    pub payload_offset: u64,
    pub encoded_len: u32,
    pub raw_len: u32,
    pub checksum: u32,
    pub compression_flags: u8,
    pub reserved: u8,
}

impl ChunkDescriptor {
    pub fn new(
        payload_offset: u64,
        raw_len: u32,
        encoded_len: u32,
        compression: CompressionAlgo,
        checksum: u32,
    ) -> Self {
        Self::new_with_compression_meta(
            payload_offset,
            raw_len,
            encoded_len,
            CompressionMeta::new(compression, false),
            checksum,
        )
    }

    pub fn new_with_compression_meta(
        payload_offset: u64,
        raw_len: u32,
        encoded_len: u32,
        compression_meta: CompressionMeta,
        checksum: u32,
    ) -> Self {
        Self {
            payload_offset,
            encoded_len,
            raw_len,
            checksum,
            compression_flags: compression_meta.to_flags(),
            reserved: 0,
        }
    }

    pub fn from_block(block: &CompressedBlock, payload_offset: u64) -> Result<Self> {
        let raw_len = u32::try_from(block.original_len)
            .map_err(|_| OxideError::InvalidFormat("raw length exceeds u32 range"))?;
        let encoded_len = u32::try_from(block.data.len())
            .map_err(|_| OxideError::InvalidFormat("encoded length exceeds u32 range"))?;

        Ok(Self::new_with_compression_meta(
            payload_offset,
            raw_len,
            encoded_len,
            block.compression_meta(),
            block.crc32,
        ))
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R, payload_offset: u64) -> Result<Self> {
        let mut bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes, payload_offset)
    }

    pub fn compression(&self) -> Result<CompressionAlgo> {
        Ok(self.compression_meta()?.algo)
    }

    pub fn compression_meta(&self) -> Result<CompressionMeta> {
        CompressionMeta::from_flags(self.compression_flags)
    }

    pub fn payload_end(&self) -> Result<u64> {
        self.payload_offset
            .checked_add(self.encoded_len as u64)
            .ok_or(OxideError::InvalidFormat("chunk payload range overflow"))
    }

    pub fn to_bytes(&self) -> [u8; CHUNK_DESCRIPTOR_SIZE] {
        let mut bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
        bytes[0..4].copy_from_slice(&self.encoded_len.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.raw_len.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[12] = self.compression_flags;
        bytes[13] = self.reserved;
        bytes
    }

    fn from_bytes(bytes: [u8; CHUNK_DESCRIPTOR_SIZE], payload_offset: u64) -> Result<Self> {
        let descriptor = Self {
            payload_offset,
            encoded_len: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            raw_len: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            checksum: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            compression_flags: bytes[12],
            reserved: bytes[13],
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<()> {
        self.payload_end()?;
        self.compression_meta()?;
        if self.reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid chunk descriptor reserved bits",
            ));
        }
        Ok(())
    }
}

pub fn encode_chunk_table(descriptors: &[ChunkDescriptor]) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(descriptors.len() * CHUNK_DESCRIPTOR_SIZE);
    for descriptor in descriptors {
        bytes.extend_from_slice(&descriptor.to_bytes());
    }
    Ok(bytes)
}

pub fn decode_chunk_table(
    bytes: &[u8],
    payload_offset: u64,
    expected_count: u32,
) -> Result<Vec<ChunkDescriptor>> {
    let expected_len = usize::try_from(expected_count)
        .map_err(|_| OxideError::InvalidFormat("chunk count exceeds usize range"))?
        .checked_mul(CHUNK_DESCRIPTOR_SIZE)
        .ok_or(OxideError::InvalidFormat("chunk table length overflow"))?;
    if bytes.len() != expected_len {
        return Err(OxideError::InvalidFormat(
            "chunk table length does not match descriptor count",
        ));
    }

    let mut descriptors = Vec::with_capacity(expected_count as usize);
    let mut next_payload_offset = payload_offset;
    for raw in bytes.chunks_exact(CHUNK_DESCRIPTOR_SIZE) {
        let mut descriptor_bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
        descriptor_bytes.copy_from_slice(raw);
        let descriptor = ChunkDescriptor::from_bytes(descriptor_bytes, next_payload_offset)?;
        next_payload_offset = descriptor.payload_end()?;
        descriptors.push(descriptor);
    }

    Ok(descriptors)
}

/// Footer for an OXZ archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub end_magic: [u8; 4],
    pub version: u16,
    pub flags: u16,
    pub block_count: u32,
    pub entry_table_offset: u64,
    pub entry_table_len: u32,
    pub chunk_table_offset: u64,
    pub chunk_table_len: u32,
    pub global_crc32: u32,
}

impl Footer {
    pub fn new(
        block_count: u32,
        entry_table_offset: u64,
        entry_table_len: u32,
        chunk_table_offset: u64,
        chunk_table_len: u32,
        global_crc32: u32,
    ) -> Self {
        Self {
            end_magic: OXZ_END_MAGIC,
            version: FOOTER_VERSION,
            flags: 0,
            block_count,
            entry_table_offset,
            entry_table_len,
            chunk_table_offset,
            chunk_table_len,
            global_crc32,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; FOOTER_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut bytes = [0u8; FOOTER_SIZE];
        bytes[0..4].copy_from_slice(&self.end_magic);
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.flags.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.block_count.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.entry_table_offset.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.entry_table_len.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.chunk_table_offset.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.chunk_table_len.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.global_crc32.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; FOOTER_SIZE]) -> Result<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if magic != OXZ_END_MAGIC {
            return Err(OxideError::InvalidFormat("invalid OXZ footer magic"));
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != FOOTER_VERSION {
            return Err(OxideError::InvalidFormat("unsupported OXZ footer version"));
        }

        let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
        if flags != 0 {
            return Err(OxideError::InvalidFormat("invalid OXZ footer flags"));
        }

        Ok(Self {
            end_magic: magic,
            version,
            flags,
            block_count: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            entry_table_offset: u64::from_le_bytes([
                bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18],
                bytes[19],
            ]),
            entry_table_len: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
            chunk_table_offset: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                bytes[31],
            ]),
            chunk_table_len: u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]),
            global_crc32: u32::from_le_bytes([bytes[36], bytes[37], bytes[38], bytes[39]]),
        })
    }
}

fn parse_prefix(bytes: [u8; GLOBAL_HEADER_SIZE]) -> Result<([u8; 4], u16, u16)> {
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[..4]);
    if magic != OXZ_MAGIC {
        return Err(OxideError::InvalidFormat("invalid OXZ magic"));
    }

    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != OXZ_VERSION {
        return Err(OxideError::InvalidFormat("unsupported OXZ version"));
    }

    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags & !SUPPORTED_HEADER_FLAGS != 0 {
        return Err(OxideError::InvalidFormat("unsupported OXZ header flags"));
    }

    Ok((magic, version, flags))
}

fn flags_for_source_kind(source_kind: ArchiveSourceKind) -> u16 {
    let mut flags = HEADER_FLAG_PATH_PREFIX
        | HEADER_FLAG_METADATA_INHERIT
        | HEADER_FLAG_IMPLICIT_CONTENT_OFFSETS
        | HEADER_FLAG_IMPLICIT_CHUNK_OFFSETS;
    if matches!(source_kind, ArchiveSourceKind::Directory) {
        flags |= HEADER_FLAG_DIRECTORY;
    }
    flags
}
