use std::io::{Read, Write};

use crate::{
    ArchiveSourceKind, CompressedBlock, CompressionAlgo, CompressionMeta, CompressionPreset,
    OxideError, PreProcessingStrategy, Result,
};

use super::{
    ARCHIVE_METADATA_SIZE, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE, FOOTER_SIZE,
    GLOBAL_HEADER_SIZE, OXZ_END_MAGIC, OXZ_MAGIC, OXZ_VERSION,
};

const ARCHIVE_METADATA_MAGIC: [u8; 4] = *b"OXMD";
const ARCHIVE_METADATA_VERSION: u16 = 1;
const CHUNK_TABLE_MAGIC: [u8; 4] = *b"OXCI";
const CHUNK_TABLE_VERSION: u16 = 1;

/// Global header for an OXZ archive.
///
/// v2 archives use a fixed layout with explicit offsets instead of a generic
/// section table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub metadata_offset: u64,
    pub entry_table_offset: u64,
    pub chunk_table_offset: u64,
    pub payload_offset: u64,
    pub footer_offset: u64,
}

impl GlobalHeader {
    pub fn new(
        metadata_offset: u64,
        entry_table_offset: u64,
        chunk_table_offset: u64,
        payload_offset: u64,
        footer_offset: u64,
    ) -> Self {
        Self {
            magic: OXZ_MAGIC,
            version: OXZ_VERSION,
            metadata_offset,
            entry_table_offset,
            chunk_table_offset,
            payload_offset,
            footer_offset,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; GLOBAL_HEADER_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    pub fn to_bytes(&self) -> [u8; GLOBAL_HEADER_SIZE] {
        let mut bytes = [0u8; GLOBAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(&self.magic);
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());
        bytes[6..8].copy_from_slice(&0u16.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.metadata_offset.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.entry_table_offset.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.chunk_table_offset.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.payload_offset.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.footer_offset.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; GLOBAL_HEADER_SIZE]) -> Result<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[..4]);
        if magic != OXZ_MAGIC {
            return Err(OxideError::InvalidFormat("invalid OXZ magic"));
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != OXZ_VERSION {
            return Err(OxideError::InvalidFormat("unsupported OXZ version"));
        }

        let reserved = u16::from_le_bytes([bytes[6], bytes[7]]);
        if reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid global header reserved bits",
            ));
        }

        let header = Self {
            magic,
            version,
            metadata_offset: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                bytes[15],
            ]),
            entry_table_offset: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ]),
            chunk_table_offset: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30],
                bytes[31],
            ]),
            payload_offset: u64::from_le_bytes([
                bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38],
                bytes[39],
            ]),
            footer_offset: u64::from_le_bytes([
                bytes[40], bytes[41], bytes[42], bytes[43], bytes[44], bytes[45], bytes[46],
                bytes[47],
            ]),
        };
        header.validate()?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<()> {
        if self.metadata_offset < GLOBAL_HEADER_SIZE as u64 {
            return Err(OxideError::InvalidFormat(
                "metadata offset is before end of header",
            ));
        }

        if !(self.metadata_offset <= self.entry_table_offset
            && self.entry_table_offset <= self.chunk_table_offset
            && self.chunk_table_offset <= self.payload_offset
            && self.payload_offset <= self.footer_offset)
        {
            return Err(OxideError::InvalidFormat(
                "global header offsets are not monotonic",
            ));
        }

        Ok(())
    }
}

/// Archive-level metadata stored independently from the global header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveMetadata {
    pub source_kind: ArchiveSourceKind,
}

impl ArchiveMetadata {
    pub fn new(source_kind: ArchiveSourceKind) -> Self {
        Self { source_kind }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; ARCHIVE_METADATA_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    pub fn to_bytes(&self) -> [u8; ARCHIVE_METADATA_SIZE] {
        let mut bytes = [0u8; ARCHIVE_METADATA_SIZE];
        bytes[..4].copy_from_slice(&ARCHIVE_METADATA_MAGIC);
        bytes[4..6].copy_from_slice(&ARCHIVE_METADATA_VERSION.to_le_bytes());
        bytes[6] = archive_source_kind_to_raw(self.source_kind);
        bytes[7] = 0;
        bytes
    }

    fn from_bytes(bytes: [u8; ARCHIVE_METADATA_SIZE]) -> Result<Self> {
        if bytes[..4] != ARCHIVE_METADATA_MAGIC {
            return Err(OxideError::InvalidFormat("invalid archive metadata magic"));
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != ARCHIVE_METADATA_VERSION {
            return Err(OxideError::InvalidFormat(
                "unsupported archive metadata version",
            ));
        }

        if bytes[7] != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid archive metadata reserved bits",
            ));
        }

        Ok(Self {
            source_kind: archive_source_kind_from_raw(bytes[6])?,
        })
    }
}

/// Chunk-level metadata descriptor used by v2 chunk tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkDescriptor {
    pub payload_offset: u64,
    pub encoded_len: u32,
    pub raw_len: u32,
    pub checksum: u32,
    pub compression_flags: u8,
    pub strategy_flags: u8,
}

impl ChunkDescriptor {
    pub fn new(
        payload_offset: u64,
        raw_len: u32,
        encoded_len: u32,
        strategy: PreProcessingStrategy,
        compression: CompressionAlgo,
        checksum: u32,
    ) -> Self {
        Self::new_with_compression_meta(
            payload_offset,
            raw_len,
            encoded_len,
            strategy,
            CompressionMeta::new(compression, CompressionPreset::Default, false),
            checksum,
        )
    }

    pub fn new_with_compression_meta(
        payload_offset: u64,
        raw_len: u32,
        encoded_len: u32,
        strategy: PreProcessingStrategy,
        compression_meta: CompressionMeta,
        checksum: u32,
    ) -> Self {
        Self {
            payload_offset,
            encoded_len,
            raw_len,
            checksum,
            compression_flags: compression_meta.to_flags(),
            strategy_flags: strategy.to_flags(),
        }
    }

    pub fn from_block(block: &CompressedBlock, payload_offset: u64) -> Result<Self> {
        let raw_len = u32::try_from(block.original_len)
            .map_err(|_| OxideError::InvalidFormat("raw length exceeds u32 range"))?;
        let encoded_len = u32::try_from(block.data.len())
            .map_err(|_| OxideError::InvalidFormat("encoded length exceeds u32 range"))?;

        let descriptor = Self::new_with_compression_meta(
            payload_offset,
            raw_len,
            encoded_len,
            block.pre_proc.clone(),
            block.compression_meta(),
            block.crc32,
        );
        Ok(descriptor)
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    pub fn strategy(&self) -> Result<PreProcessingStrategy> {
        PreProcessingStrategy::from_flags(self.strategy_flags)
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
        bytes[0..8].copy_from_slice(&self.payload_offset.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.encoded_len.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.raw_len.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[20] = self.compression_flags;
        bytes[21] = self.strategy_flags;
        bytes
    }

    fn from_bytes(bytes: [u8; CHUNK_DESCRIPTOR_SIZE]) -> Result<Self> {
        let descriptor = Self {
            payload_offset: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            encoded_len: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            raw_len: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            checksum: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            compression_flags: bytes[20],
            strategy_flags: bytes[21],
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<()> {
        self.payload_end()?;
        self.compression_meta()?;
        self.strategy()?;
        Ok(())
    }
}

/// Backward-compatible alias while internals migrate from block headers to chunk descriptors.
pub type BlockHeader = ChunkDescriptor;

pub fn encode_chunk_table(descriptors: &[ChunkDescriptor]) -> Result<Vec<u8>> {
    let count = u32::try_from(descriptors.len())
        .map_err(|_| OxideError::InvalidFormat("chunk count exceeds u32 range"))?;
    let mut bytes =
        Vec::with_capacity(CHUNK_TABLE_HEADER_SIZE + descriptors.len() * CHUNK_DESCRIPTOR_SIZE);
    bytes.extend_from_slice(&CHUNK_TABLE_MAGIC);
    bytes.extend_from_slice(&CHUNK_TABLE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(&count.to_le_bytes());
    for descriptor in descriptors {
        bytes.extend_from_slice(&descriptor.to_bytes());
    }
    Ok(bytes)
}

pub fn decode_chunk_table(bytes: &[u8]) -> Result<Vec<ChunkDescriptor>> {
    if bytes.len() < CHUNK_TABLE_HEADER_SIZE {
        return Err(OxideError::InvalidFormat("chunk table is too short"));
    }
    if bytes[..4] != CHUNK_TABLE_MAGIC {
        return Err(OxideError::InvalidFormat("invalid chunk table magic"));
    }

    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != CHUNK_TABLE_VERSION {
        return Err(OxideError::InvalidFormat("unsupported chunk table version"));
    }

    let reserved = u16::from_le_bytes([bytes[6], bytes[7]]);
    if reserved != 0 {
        return Err(OxideError::InvalidFormat(
            "invalid chunk table reserved bits",
        ));
    }

    let count = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    let descriptor_bytes = &bytes[CHUNK_TABLE_HEADER_SIZE..];
    let expected_len = count
        .checked_mul(CHUNK_DESCRIPTOR_SIZE)
        .ok_or(OxideError::InvalidFormat("chunk table length overflow"))?;
    if descriptor_bytes.len() != expected_len {
        return Err(OxideError::InvalidFormat(
            "chunk table length does not match descriptor count",
        ));
    }

    let mut descriptors = Vec::with_capacity(count);
    for raw in descriptor_bytes.chunks_exact(CHUNK_DESCRIPTOR_SIZE) {
        let mut descriptor = [0u8; CHUNK_DESCRIPTOR_SIZE];
        descriptor.copy_from_slice(raw);
        descriptors.push(ChunkDescriptor::from_bytes(descriptor)?);
    }
    Ok(descriptors)
}

/// Footer for an OXZ archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub end_magic: [u8; 4],
    pub global_crc32: u32,
}

impl Footer {
    pub fn new(global_crc32: u32) -> Self {
        Self {
            end_magic: OXZ_END_MAGIC,
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
        bytes[..4].copy_from_slice(&self.end_magic);
        bytes[4..8].copy_from_slice(&self.global_crc32.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; FOOTER_SIZE]) -> Result<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[..4]);
        if magic != OXZ_END_MAGIC {
            return Err(OxideError::InvalidFormat("invalid OXZ footer magic"));
        }
        Ok(Self {
            end_magic: magic,
            global_crc32: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        })
    }
}

fn archive_source_kind_to_raw(source_kind: ArchiveSourceKind) -> u8 {
    match source_kind {
        ArchiveSourceKind::File => 0,
        ArchiveSourceKind::Directory => 1,
    }
}

fn archive_source_kind_from_raw(raw: u8) -> Result<ArchiveSourceKind> {
    match raw {
        0 => Ok(ArchiveSourceKind::File),
        1 => Ok(ArchiveSourceKind::Directory),
        _ => Err(OxideError::InvalidFormat("invalid archive source kind")),
    }
}
