use std::cmp::min;
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use crc32fast::Hasher;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{OxideError, Result};

use super::{BLOCK_HEADER_SIZE, BlockHeader, Footer, GLOBAL_HEADER_SIZE, GlobalHeader};

/// Reads OXZ archives and provides access to individual blocks.
///
/// Supports both sequential iteration and random access to blocks.
#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    block_offsets: Vec<u64>,
    footer: Footer,
}

impl<R: Read + Seek> ArchiveReader<R> {
    /// Creates a new archive reader from a readable and seekable source.
    ///
    /// Validates the global header, builds an index of block offsets, and
    /// verifies the global CRC32 in the footer.
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

    /// Returns the number of blocks in the archive.
    pub fn block_count(&self) -> u32 {
        self.global_header.block_count
    }

    /// Returns the global header of the archive.
    pub fn global_header(&self) -> GlobalHeader {
        self.global_header
    }

    /// Returns the footer of the archive.
    pub fn footer(&self) -> Footer {
        self.footer
    }

    /// Reads a specific block by its index.
    ///
    /// Validates the block's CRC32 checksum before returning.
    pub fn read_block(&mut self, index: u32) -> Result<(BlockHeader, Vec<u8>)> {
        let start = Instant::now();
        let index_val = usize::try_from(index)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;
        let offset = *self
            .block_offsets
            .get(index_val)
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

        let elapsed_us = duration_to_us(start.elapsed());
        telemetry::increment_counter(
            tags::METRIC_OXZ_READ_BLOCK_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "read_block")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_READ_BLOCK_LATENCY_US,
            elapsed_us,
            &[("subsystem", "oxz"), ("op", "read_block")],
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_block",
            "ok",
            elapsed_us,
            "oxz block read successfully",
        );

        Ok((header, data))
    }

    /// Returns an iterator over all blocks in the archive.
    pub fn iter_blocks(&mut self) -> BlockIterator<'_, R> {
        BlockIterator {
            reader: self,
            next_index: 0,
        }
    }

    /// Consumes the reader and returns the underlying inner reader.
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

/// An iterator over blocks in an OXZ archive.
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
