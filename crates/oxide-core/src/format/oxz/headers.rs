use std::io::{Read, Write};

use crate::{
    CompressedBlock, CompressionAlgo, CompressionMeta, CompressionPreset, OxideError,
    PreProcessingStrategy, Result,
};

use super::{
    BLOCK_HEADER_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE, OXZ_END_MAGIC, OXZ_MAGIC, OXZ_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub reserved: u16,
    pub flags: u32,
    pub block_count: u32,
}

impl GlobalHeader {
    pub fn new(block_count: u32) -> Self {
        Self::with_flags(block_count, 0)
    }

    pub fn with_flags(block_count: u32, flags: u32) -> Self {
        Self {
            magic: OXZ_MAGIC,
            version: OXZ_VERSION,
            reserved: 0,
            flags,
            block_count,
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
        bytes[6..8].copy_from_slice(&self.reserved.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.flags.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.block_count.to_le_bytes());
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

        Ok(Self {
            magic,
            version,
            reserved,
            flags: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            block_count: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    pub block_id: u64,
    pub original_size: u32,
    pub compressed_size: u32,
    pub strategy_flags: u8,
    pub compression_flags: u8,
    pub reserved: u16,
    pub crc32: u32,
}

impl BlockHeader {
    pub fn new(
        block_id: u64,
        original_size: u32,
        compressed_size: u32,
        strategy: PreProcessingStrategy,
        compression: CompressionAlgo,
        crc32: u32,
    ) -> Self {
        Self::new_with_compression_meta(
            block_id,
            original_size,
            compressed_size,
            strategy,
            CompressionMeta::new(compression, CompressionPreset::Default, false),
            crc32,
        )
    }

    pub fn new_with_compression_meta(
        block_id: u64,
        original_size: u32,
        compressed_size: u32,
        strategy: PreProcessingStrategy,
        compression_meta: CompressionMeta,
        crc32: u32,
    ) -> Self {
        Self {
            block_id,
            original_size,
            compressed_size,
            strategy_flags: strategy.to_flags(),
            compression_flags: compression_meta.to_flags(),
            reserved: 0,
            crc32,
        }
    }

    pub fn from_block(block: &CompressedBlock) -> Result<Self> {
        let block_id = u64::try_from(block.id)
            .map_err(|_| OxideError::InvalidFormat("block id exceeds u64 range"))?;
        let original_size = u32::try_from(block.original_len)
            .map_err(|_| OxideError::InvalidFormat("original size exceeds u32 range"))?;
        let compressed_size = u32::try_from(block.data.len())
            .map_err(|_| OxideError::InvalidFormat("compressed size exceeds u32 range"))?;

        Ok(Self::new_with_compression_meta(
            block_id,
            original_size,
            compressed_size,
            block.pre_proc.clone(),
            block.compression_meta(),
            block.crc32,
        ))
    }

    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.to_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<Self> {
        let mut bytes = [0u8; BLOCK_HEADER_SIZE];
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

    pub fn to_bytes(&self) -> [u8; BLOCK_HEADER_SIZE] {
        let mut bytes = [0u8; BLOCK_HEADER_SIZE];
        bytes[..8].copy_from_slice(&self.block_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.original_size.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.compressed_size.to_le_bytes());
        bytes[16] = self.strategy_flags;
        bytes[17] = self.compression_flags;
        bytes[18..20].copy_from_slice(&self.reserved.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.crc32.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: [u8; BLOCK_HEADER_SIZE]) -> Result<Self> {
        let header = Self {
            block_id: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            original_size: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            compressed_size: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            strategy_flags: bytes[16],
            compression_flags: bytes[17],
            reserved: u16::from_le_bytes([bytes[18], bytes[19]]),
            crc32: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
        };
        header.validate()?;
        Ok(header)
    }

    fn validate(&self) -> Result<()> {
        if self.reserved != 0 {
            return Err(OxideError::InvalidFormat(
                "invalid block header reserved bits",
            ));
        }
        PreProcessingStrategy::from_flags(self.strategy_flags)?;
        self.compression_meta()?;
        Ok(())
    }
}

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
