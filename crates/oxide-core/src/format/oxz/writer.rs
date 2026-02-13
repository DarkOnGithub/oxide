use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use crc32fast::Hasher;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{BufferPool, CompressedBlock, OxideError, Result};

use super::{BlockHeader, DEFAULT_REORDER_PENDING_LIMIT, Footer, GlobalHeader, ReorderBuffer};

/// Writes OXZ archives by appending blocks and finalizing with a footer.
///
/// Includes an internal [`ReorderBuffer`] to handle out-of-order block submissions
/// from parallel workers.
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
    /// Creates a new archive writer with the default reorder limit.
    pub fn new(writer: W, buffer_pool: Arc<BufferPool>) -> Self {
        Self::with_reorder_limit(writer, buffer_pool, DEFAULT_REORDER_PENDING_LIMIT)
    }

    /// Creates a new archive writer with a custom reorder limit.
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

    /// Writes the global header to the archive.
    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        self.write_global_header_with_flags(block_count, 0)
    }

    /// Writes the global header with specific flags.
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

    /// Writes a single block to the archive.
    ///
    /// Requires blocks to be written in strict sequential order by ID.
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

    /// Writes a block and recycles its data buffer back to the pool.
    pub fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        self.write_block(&block)?;
        self.recycle_block_data(block.data);
        Ok(())
    }

    /// Pushes a block into the reorder buffer.
    ///
    /// If the block completes a contiguous sequence starting from the next
    /// expected ID, it and all subsequent ready blocks are written to the archive.
    ///
    /// Returns the number of blocks actually written to the underlying writer.
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

    /// Returns the number of blocks already written to the archive.
    pub fn blocks_written(&self) -> u32 {
        self.blocks_written
    }

    /// Returns the number of blocks currently pending in the reorder buffer.
    pub fn pending_blocks(&self) -> usize {
        self.reorder.pending_len()
    }

    /// Finalizes the archive by writing the footer.
    ///
    /// Fails if there are still pending blocks in the reorder buffer or if
    /// the written block count does not match the expected count.
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

    /// Consumes the writer and returns the underlying inner writer.
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
        let start = Instant::now();
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

        let elapsed_us = duration_to_us(start.elapsed());
        telemetry::increment_counter(
            tags::METRIC_OXZ_WRITE_BLOCK_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "write_block")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_WRITE_BLOCK_LATENCY_US,
            elapsed_us,
            &[("subsystem", "oxz"), ("op", "write_block")],
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "write_block",
            "ok",
            elapsed_us,
            "oxz block written successfully",
        );

        Ok(())
    }

    fn recycle_block_data(&self, mut data: Vec<u8>) {
        let mut pooled = self.buffer_pool.acquire();
        std::mem::swap(pooled.as_mut_vec(), &mut data);
    }
}
