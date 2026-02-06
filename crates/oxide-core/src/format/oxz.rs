use std::cmp::min;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use crc32fast::Hasher;

use crate::{
    BufferPool, CompressedBlock, CompressionAlgo, OxideError, PreProcessingStrategy, Result,
};

pub const OXZ_MAGIC: [u8; 4] = *b"OXZ\0";
pub const OXZ_END_MAGIC: [u8; 4] = *b"END\0";
pub const OXZ_VERSION: u16 = 1;

pub const GLOBAL_HEADER_SIZE: usize = 16;
pub const BLOCK_HEADER_SIZE: usize = 24;
pub const FOOTER_SIZE: usize = 8;

pub const DEFAULT_REORDER_PENDING_LIMIT: usize = 1024;

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
        Self {
            block_id,
            original_size,
            compressed_size,
            strategy_flags: strategy.to_flags(),
            compression_flags: compression.to_flags(),
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

        Ok(Self::new(
            block_id,
            original_size,
            compressed_size,
            block.pre_proc.clone(),
            block.compression,
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
        CompressionAlgo::from_flags(self.compression_flags)
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
        CompressionAlgo::from_flags(self.compression_flags)?;
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

#[derive(Debug)]
pub struct ReorderBuffer<T> {
    next_id: usize,
    pending: BTreeMap<usize, T>,
    max_pending: usize,
}

impl<T> Default for ReorderBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ReorderBuffer<T> {
    pub fn new() -> Self {
        Self::with_limit(DEFAULT_REORDER_PENDING_LIMIT)
    }

    pub fn with_limit(max_pending: usize) -> Self {
        Self {
            next_id: 0,
            pending: BTreeMap::new(),
            max_pending: max_pending.max(1),
        }
    }

    pub fn push(&mut self, id: usize, item: T) -> Result<Vec<T>> {
        if id < self.next_id {
            return Err(OxideError::InvalidBlockId {
                expected: self.next_id as u64,
                actual: id as u64,
            });
        }

        if self.pending.contains_key(&id) {
            return Err(OxideError::InvalidFormat(
                "duplicate block id in reorder buffer",
            ));
        }

        if id != self.next_id && self.pending.len() >= self.max_pending {
            return Err(OxideError::InvalidFormat(
                "reorder buffer capacity exceeded",
            ));
        }

        self.pending.insert(id, item);

        let mut ready = Vec::new();
        while let Some(item) = self.pending.remove(&self.next_id) {
            ready.push(item);
            self.next_id += 1;
        }
        Ok(ready)
    }

    pub fn next_expected(&self) -> usize {
        self.next_id
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn max_pending(&self) -> usize {
        self.max_pending
    }
}

#[derive(Debug)]
pub struct ArchiveWriter<W: Write> {
    writer: W,
    buffer_pool: Arc<BufferPool>,
    expected_block_count: Option<u32>,
    blocks_written: u32,
    global_crc32: Hasher,
    reorder: ReorderBuffer<CompressedBlock>,
}

impl<W: Write> ArchiveWriter<W> {
    pub fn new(writer: W, buffer_pool: Arc<BufferPool>) -> Self {
        Self::with_reorder_limit(writer, buffer_pool, DEFAULT_REORDER_PENDING_LIMIT)
    }

    pub fn with_reorder_limit(writer: W, buffer_pool: Arc<BufferPool>, max_pending: usize) -> Self {
        Self {
            writer,
            buffer_pool,
            expected_block_count: None,
            blocks_written: 0,
            global_crc32: Hasher::new(),
            reorder: ReorderBuffer::with_limit(max_pending),
        }
    }

    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        self.write_global_header_with_flags(block_count, 0)
    }

    pub fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        if self.expected_block_count.is_some() {
            return Err(OxideError::InvalidFormat("global header already written"));
        }

        let header = GlobalHeader::with_flags(block_count, flags);
        let bytes = header.to_bytes();
        self.writer.write_all(&bytes)?;
        self.global_crc32.update(&bytes);
        self.expected_block_count = Some(block_count);
        Ok(())
    }

    pub fn write_block(&mut self, block: &CompressedBlock) -> Result<()> {
        self.ensure_header_written()?;

        let expected = usize::try_from(self.blocks_written)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;
        if block.id != expected {
            return Err(OxideError::InvalidBlockId {
                expected: expected as u64,
                actual: block.id as u64,
            });
        }

        self.write_block_unchecked(block)
    }

    pub fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        self.write_block(&block)?;
        self.recycle_block_data(block.data);
        Ok(())
    }

    pub fn push_block(&mut self, block: CompressedBlock) -> Result<usize> {
        self.ensure_header_written()?;

        let ready = self.reorder.push(block.id, block)?;
        let mut written = 0usize;
        for block in ready {
            self.write_block_unchecked(&block)?;
            self.recycle_block_data(block.data);
            written += 1;
        }
        Ok(written)
    }

    pub fn blocks_written(&self) -> u32 {
        self.blocks_written
    }

    pub fn pending_blocks(&self) -> usize {
        self.reorder.pending_len()
    }

    pub fn write_footer(mut self) -> Result<W> {
        self.ensure_header_written()?;

        if self.pending_blocks() > 0 {
            return Err(OxideError::InvalidFormat(
                "cannot finalize archive with pending reordered blocks",
            ));
        }

        let expected = self.expected_block_count.unwrap_or(0);
        if self.blocks_written != expected {
            return Err(OxideError::InvalidFormat(
                "block count mismatch before writing footer",
            ));
        }

        let footer = Footer::new(self.global_crc32.finalize());
        footer.write(&mut self.writer)?;
        Ok(self.writer)
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    fn ensure_header_written(&self) -> Result<()> {
        if self.expected_block_count.is_none() {
            return Err(OxideError::InvalidFormat(
                "global header must be written first",
            ));
        }
        Ok(())
    }

    fn write_block_unchecked(&mut self, block: &CompressedBlock) -> Result<()> {
        let expected = self.expected_block_count.unwrap_or(0);
        if self.blocks_written >= expected {
            return Err(OxideError::InvalidFormat("archive block count exceeded"));
        }

        let header = BlockHeader::from_block(block)?;
        let header_bytes = header.to_bytes();
        self.writer.write_all(&header_bytes)?;
        self.writer.write_all(&block.data)?;
        self.global_crc32.update(&header_bytes);
        self.global_crc32.update(&block.data);
        self.blocks_written += 1;
        Ok(())
    }

    fn recycle_block_data(&self, mut data: Vec<u8>) {
        let mut pooled = self.buffer_pool.acquire();
        std::mem::swap(pooled.as_mut_vec(), &mut data);
    }
}

#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    block_offsets: Vec<u64>,
    footer: Footer,
}

impl<R: Read + Seek> ArchiveReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let global_header = GlobalHeader::read(&mut reader)?;
        let (block_offsets, footer_offset) =
            Self::build_block_index(&mut reader, global_header.block_count)?;

        reader.seek(SeekFrom::Start(footer_offset))?;
        let footer = Footer::read(&mut reader)?;

        let computed_crc = Self::compute_crc32_up_to(&mut reader, footer_offset)?;
        if computed_crc != footer.global_crc32 {
            return Err(OxideError::ChecksumMismatch {
                expected: footer.global_crc32,
                actual: computed_crc,
            });
        }

        reader.seek(SeekFrom::Start(GLOBAL_HEADER_SIZE as u64))?;
        Ok(Self {
            reader,
            global_header,
            block_offsets,
            footer,
        })
    }

    pub fn block_count(&self) -> u32 {
        self.global_header.block_count
    }

    pub fn global_header(&self) -> GlobalHeader {
        self.global_header
    }

    pub fn footer(&self) -> Footer {
        self.footer
    }

    pub fn read_block(&mut self, index: u32) -> Result<(BlockHeader, Vec<u8>)> {
        let index = usize::try_from(index)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;
        let offset = *self
            .block_offsets
            .get(index)
            .ok_or(OxideError::InvalidFormat("block index out of range"))?;

        self.reader.seek(SeekFrom::Start(offset))?;
        let header = BlockHeader::read(&mut self.reader)?;
        let mut data = vec![0u8; header.compressed_size as usize];
        self.reader.read_exact(&mut data)?;

        let actual_crc = crc32fast::hash(&data);
        if actual_crc != header.crc32 {
            return Err(OxideError::ChecksumMismatch {
                expected: header.crc32,
                actual: actual_crc,
            });
        }

        Ok((header, data))
    }

    pub fn iter_blocks(&mut self) -> BlockIterator<'_, R> {
        BlockIterator {
            reader: self,
            next_index: 0,
        }
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    fn build_block_index(reader: &mut R, block_count: u32) -> Result<(Vec<u64>, u64)> {
        let mut offsets = Vec::with_capacity(block_count as usize);
        let mut offset = GLOBAL_HEADER_SIZE as u64;

        for _ in 0..block_count {
            reader.seek(SeekFrom::Start(offset))?;
            let header = BlockHeader::read(reader)?;
            offsets.push(offset);
            offset = offset
                .checked_add(BLOCK_HEADER_SIZE as u64)
                .and_then(|value| value.checked_add(header.compressed_size as u64))
                .ok_or(OxideError::InvalidFormat("archive offsets overflow"))?;
        }

        Ok((offsets, offset))
    }

    fn compute_crc32_up_to(reader: &mut R, len: u64) -> Result<u32> {
        reader.seek(SeekFrom::Start(0))?;
        let mut hasher = Hasher::new();
        let mut remaining = len;
        let mut buffer = [0u8; 8 * 1024];

        while remaining > 0 {
            let to_read = min(remaining as usize, buffer.len());
            reader.read_exact(&mut buffer[..to_read])?;
            hasher.update(&buffer[..to_read]);
            remaining -= to_read as u64;
        }

        Ok(hasher.finalize())
    }
}

pub struct BlockIterator<'a, R: Read + Seek> {
    reader: &'a mut ArchiveReader<R>,
    next_index: u32,
}

impl<R: Read + Seek> Iterator for BlockIterator<'_, R> {
    type Item = Result<(BlockHeader, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.reader.block_count() {
            return None;
        }

        let current = self.next_index;
        self.next_index += 1;
        Some(self.reader.read_block(current))
    }
}
