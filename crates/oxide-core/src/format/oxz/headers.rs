use std::io::{Read, Write};

use crate::{
    CompressedBlock, CompressionAlgo, CompressionMeta, CompressionPreset, OxideError,
    PreProcessingStrategy, Result,
};

use super::{
    CHUNK_DESCRIPTOR_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_END_MAGIC, OXZ_MAGIC, OXZ_VERSION,
    SECTION_TABLE_ENTRY_SIZE,
};

/// Section identifier for the section table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum SectionType {
    ChunkIndex = 1,
    DictionaryStore = 2,
    EntropyTableStore = 3,
    TransformChainTable = 4,
    DedupReferenceTable = 5,
    PayloadRegion = 6,
    ArchiveManifest = 7,
}

impl SectionType {
    pub fn from_raw(raw: u16) -> Result<Self> {
        match raw {
            1 => Ok(Self::ChunkIndex),
            2 => Ok(Self::DictionaryStore),
            3 => Ok(Self::EntropyTableStore),
            4 => Ok(Self::TransformChainTable),
            5 => Ok(Self::DedupReferenceTable),
            6 => Ok(Self::PayloadRegion),
            7 => Ok(Self::ArchiveManifest),
            _ => Err(OxideError::InvalidFormat("unknown section type")),
        }
    }

    pub fn as_raw(self) -> u16 {
        self as u16
    }
}

/// Global header for an OXZ archive.
///
/// v1 layout keeps a fixed header with a section table pointer and a feature bitset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalHeader {
    /// Magic bytes "OXZ\0".
    pub magic: [u8; 4],
    /// Format version.
    pub version: u16,
    /// Archive-wide feature bits.
    pub feature_bits: u16,
    /// Number of entries in the section table.
    pub section_count: u16,
    /// Absolute offset of the section table in bytes.
    pub section_table_offset: u64,
}

impl GlobalHeader {
    /// Creates a new global header.
    pub fn new(section_count: u16, section_table_offset: u64) -> Self {
        Self::with_feature_bits(section_count, 0, section_table_offset)
    }

    /// Creates a new global header with explicit feature bits.
    pub fn with_feature_bits(
        section_count: u16,
        feature_bits: u16,
        section_table_offset: u64,
    ) -> Self {
        Self {
            magic: OXZ_MAGIC,
            version: OXZ_VERSION,
            feature_bits,
            section_count,
            section_table_offset,
        }
    }

    /// Writes the header to a writer.
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    /// Reads a header from a reader.
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; GLOBAL_HEADER_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> [u8; GLOBAL_HEADER_SIZE] {
        let mut bytes = [0u8; GLOBAL_HEADER_SIZE];
        bytes[..4].copy_from_slice(&self.magic);
        bytes[4..6].copy_from_slice(&self.version.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.feature_bits.to_le_bytes());
        bytes[8..10].copy_from_slice(&self.section_count.to_le_bytes());
        bytes[10..12].copy_from_slice(&0u16.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.section_table_offset.to_le_bytes());
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

        let reserved = u16::from_le_bytes([bytes[10], bytes[11]]);
        if reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid global header reserved bits",
            ));
        }

        let section_count = u16::from_le_bytes([bytes[8], bytes[9]]);
        if section_count == 0 {
            return Err(OxideError::InvalidFormat(
                "section table must contain at least one entry",
            ));
        }

        Ok(Self {
            magic,
            version,
            feature_bits: u16::from_le_bytes([bytes[6], bytes[7]]),
            section_count,
            section_table_offset: u64::from_le_bytes([
                bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18],
                bytes[19],
            ]),
        })
    }
}

/// Section table entry used by v1 containers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionTableEntry {
    /// Type of section this entry points to.
    pub section_type: SectionType,
    /// Absolute byte offset of the section.
    pub offset: u64,
    /// Byte length of the section.
    pub length: u64,
    /// CRC32 checksum of the section bytes.
    pub checksum: u32,
}

impl SectionTableEntry {
    pub fn new(section_type: SectionType, offset: u64, length: u64, checksum: u32) -> Self {
        Self {
            section_type,
            offset,
            length,
            checksum,
        }
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; SECTION_TABLE_ENTRY_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    pub fn to_bytes(&self) -> [u8; SECTION_TABLE_ENTRY_SIZE] {
        let mut bytes = [0u8; SECTION_TABLE_ENTRY_SIZE];
        bytes[0..2].copy_from_slice(&self.section_type.as_raw().to_le_bytes());
        bytes[2..4].copy_from_slice(&0u16.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.offset.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.length.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; SECTION_TABLE_ENTRY_SIZE]) -> Result<Self> {
        let reserved = u16::from_le_bytes([bytes[2], bytes[3]]);
        if reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid section table reserved bits",
            ));
        }

        let entry = Self {
            section_type: SectionType::from_raw(u16::from_le_bytes([bytes[0], bytes[1]]))?,
            offset: u64::from_le_bytes([
                bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
            ]),
            length: u64::from_le_bytes([
                bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18],
                bytes[19],
            ]),
            checksum: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
        };
        entry.validate()?;
        Ok(entry)
    }

    fn validate(&self) -> Result<()> {
        let _ = self
            .offset
            .checked_add(self.length)
            .ok_or(OxideError::InvalidFormat("section range overflow"))?;
        Ok(())
    }
}

/// Chunk-level metadata descriptor used by v1 chunk index sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkDescriptor {
    /// Chunk identifier in the archive.
    pub chunk_id: u64,
    /// Logical stream identifier for chunk grouping.
    pub stream_id: u32,
    /// Uncompressed byte length.
    pub raw_len: u32,
    /// Encoded byte length.
    pub encoded_len: u32,
    /// CRC32 checksum of encoded bytes.
    pub checksum: u32,
    /// Codec identifier.
    pub codec_id: u8,
    /// Compression preset identifier.
    pub preset: u8,
    /// Entropy mode identifier.
    pub entropy_mode: u8,
    /// Descriptor flags.
    pub flags: u8,
    /// Transform chain identifier.
    pub transform_chain_id: u16,
    /// Dictionary identifier.
    pub dict_id: u16,
    /// Optional reference chunk identifier.
    pub ref_chunk_id: Option<u64>,
}

impl ChunkDescriptor {
    pub const ENTROPY_MODE_DEFAULT: u8 = 0;
    pub const FLAG_RAW_PASSTHROUGH: u8 = 1 << 0;
    pub const FLAG_INDEPENDENT: u8 = 1 << 1;
    pub const FLAG_HAS_REFERENCE: u8 = 1 << 2;

    const REF_NONE_SENTINEL: u64 = u64::MAX;

    /// Creates a new descriptor.
    pub fn new(
        chunk_id: u64,
        raw_len: u32,
        encoded_len: u32,
        strategy: PreProcessingStrategy,
        compression: CompressionAlgo,
        checksum: u32,
    ) -> Self {
        Self::new_with_compression_meta(
            chunk_id,
            raw_len,
            encoded_len,
            strategy,
            CompressionMeta::new(compression, CompressionPreset::Default, false),
            checksum,
        )
    }

    /// Creates a descriptor with explicit compression metadata.
    pub fn new_with_compression_meta(
        chunk_id: u64,
        raw_len: u32,
        encoded_len: u32,
        strategy: PreProcessingStrategy,
        compression_meta: CompressionMeta,
        checksum: u32,
    ) -> Self {
        let mut flags = Self::FLAG_INDEPENDENT;
        if compression_meta.raw_passthrough {
            flags |= Self::FLAG_RAW_PASSTHROUGH;
        }

        Self {
            chunk_id,
            stream_id: 0,
            raw_len,
            encoded_len,
            checksum,
            codec_id: compression_meta.algo.to_flags(),
            preset: compression_preset_to_id(compression_meta.preset),
            entropy_mode: Self::ENTROPY_MODE_DEFAULT,
            flags,
            transform_chain_id: u16::from(strategy.to_flags()),
            dict_id: 0,
            ref_chunk_id: None,
        }
    }

    /// Creates a descriptor from a [`CompressedBlock`].
    pub fn from_block(block: &CompressedBlock) -> Result<Self> {
        let chunk_id = u64::try_from(block.id)
            .map_err(|_| OxideError::InvalidFormat("chunk id exceeds u64 range"))?;
        let raw_len = u32::try_from(block.original_len)
            .map_err(|_| OxideError::InvalidFormat("raw length exceeds u32 range"))?;
        let encoded_len = u32::try_from(block.data.len())
            .map_err(|_| OxideError::InvalidFormat("encoded length exceeds u32 range"))?;

        let mut descriptor = Self::new_with_compression_meta(
            chunk_id,
            raw_len,
            encoded_len,
            block.pre_proc.clone(),
            block.compression_meta(),
            block.crc32,
        );
        descriptor.stream_id = block.stream_id;
        descriptor.dict_id = block.dict_id;
        Ok(descriptor)
    }

    /// Writes the descriptor to a writer.
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    /// Reads a descriptor from a reader.
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    /// Decodes the preprocessing strategy from the transform chain ID lane.
    pub fn strategy(&self) -> Result<PreProcessingStrategy> {
        let flag_bits = u8::try_from(self.transform_chain_id)
            .map_err(|_| OxideError::InvalidFormat("transform chain id exceeds v1 flag range"))?;
        PreProcessingStrategy::from_flags(flag_bits)
    }

    /// Decodes the compression algorithm from descriptor fields.
    pub fn compression(&self) -> Result<CompressionAlgo> {
        Ok(self.compression_meta()?.algo)
    }

    /// Decodes full compression metadata from descriptor fields.
    pub fn compression_meta(&self) -> Result<CompressionMeta> {
        Ok(CompressionMeta {
            algo: CompressionAlgo::from_flags(self.codec_id)?,
            preset: compression_preset_from_id(self.preset)?,
            raw_passthrough: self.flags & Self::FLAG_RAW_PASSTHROUGH != 0,
        })
    }

    pub fn to_bytes(&self) -> [u8; CHUNK_DESCRIPTOR_SIZE] {
        let mut bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
        bytes[0..8].copy_from_slice(&self.chunk_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.stream_id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.raw_len.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.encoded_len.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[24] = self.codec_id;
        bytes[25] = self.preset;
        bytes[26] = self.entropy_mode;
        bytes[27] = self.flags;
        bytes[28..30].copy_from_slice(&self.transform_chain_id.to_le_bytes());
        bytes[30..32].copy_from_slice(&self.dict_id.to_le_bytes());

        let ref_chunk = self.ref_chunk_id.unwrap_or(Self::REF_NONE_SENTINEL);
        bytes[32..40].copy_from_slice(&ref_chunk.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; CHUNK_DESCRIPTOR_SIZE]) -> Result<Self> {
        let ref_chunk_raw = u64::from_le_bytes([
            bytes[32], bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38], bytes[39],
        ]);
        let ref_chunk_id = if ref_chunk_raw == Self::REF_NONE_SENTINEL {
            None
        } else {
            Some(ref_chunk_raw)
        };

        let descriptor = Self {
            chunk_id: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            stream_id: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            raw_len: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            encoded_len: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            checksum: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
            codec_id: bytes[24],
            preset: bytes[25],
            entropy_mode: bytes[26],
            flags: bytes[27],
            transform_chain_id: u16::from_le_bytes([bytes[28], bytes[29]]),
            dict_id: u16::from_le_bytes([bytes[30], bytes[31]]),
            ref_chunk_id,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<()> {
        if self.entropy_mode != Self::ENTROPY_MODE_DEFAULT {
            return Err(OxideError::InvalidFormat(
                "unsupported entropy mode for v1 chunk descriptor",
            ));
        }
        if self.flags & 0b1111_1000 != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid chunk descriptor reserved flag bits",
            ));
        }

        let has_reference = self.flags & Self::FLAG_HAS_REFERENCE != 0;
        match (has_reference, self.ref_chunk_id) {
            (true, None) => {
                return Err(OxideError::InvalidFormat(
                    "chunk descriptor reference flag set without ref chunk id",
                ));
            }
            (false, Some(_)) => {
                return Err(OxideError::InvalidFormat(
                    "chunk descriptor ref chunk id set without reference flag",
                ));
            }
            _ => {}
        }

        self.strategy()?;
        self.compression_meta()?;
        Ok(())
    }
}

/// Backward-compatible alias while internals migrate from block headers to chunk descriptors.
pub type BlockHeader = ChunkDescriptor;

/// Footer for an OXZ archive.
///
/// Contains end magic bytes and a global CRC32 checksum for the entire archive
/// (excluding the footer itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// End magic bytes "END\0".
    pub end_magic: [u8; 4],
    /// CRC32 checksum of all preceding archive data.
    pub global_crc32: u32,
}

impl Footer {
    /// Creates a new footer with the given global CRC32.
    pub fn new(global_crc32: u32) -> Self {
        Self {
            end_magic: OXZ_END_MAGIC,
            global_crc32,
        }
    }

    /// Writes the footer to a writer.
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    /// Reads a footer from a reader.
    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; FOOTER_SIZE];
        reader.read_exact(&mut bytes)?;
        Self::from_bytes(bytes)
    }

    /// Serializes the footer to bytes.
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

fn compression_preset_to_id(preset: CompressionPreset) -> u8 {
    match preset {
        CompressionPreset::Fast => 0,
        CompressionPreset::Default => 1,
        CompressionPreset::High => 2,
    }
}

fn compression_preset_from_id(id: u8) -> Result<CompressionPreset> {
    match id {
        0 => Ok(CompressionPreset::Fast),
        1 => Ok(CompressionPreset::Default),
        2 => Ok(CompressionPreset::High),
        _ => Err(OxideError::InvalidFormat(
            "invalid compression preset id in chunk descriptor",
        )),
    }
}
