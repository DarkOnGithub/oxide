use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use crc32fast::Hasher;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{BufferPool, CompressedBlock, OxideError, Result};

use super::{
    ChunkDescriptor, Footer, GlobalHeader, ReorderBuffer, SectionTableEntry, SectionType,
    CHUNK_DESCRIPTOR_SIZE, CORE_SECTION_COUNT, DEFAULT_REORDER_PENDING_LIMIT, GLOBAL_HEADER_SIZE,
    SECTION_TABLE_ENTRY_SIZE,
};

#[derive(Debug)]
struct PendingChunk {
    descriptor: ChunkDescriptor,
    data: Vec<u8>,
}

/// Writes OXZ archives by staging chunk descriptors and payloads, then
/// emitting a sectioned v1 container at finalization.
#[derive(Debug)]
pub struct ArchiveWriter<W: Write> {
    writer: W,
    buffer_pool: Arc<BufferPool>,
    expected_block_count: Option<u32>,
    feature_bits: u16,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    pending_chunks: Vec<PendingChunk>,
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
            feature_bits: 0,
            blocks_written: 0,
            reorder: ReorderBuffer::with_limit(max_pending),
            pending_chunks: Vec::new(),
        }
    }

    /// Writes the global header to the archive.
    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        self.write_global_header_with_flags(block_count, 0)
    }

    /// Stores archive-level feature bits and expected chunk count.
    pub fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        if self.expected_block_count.is_some() {
            return Err(OxideError::InvalidFormat("global header already written"));
        }

        let feature_bits = u16::try_from(flags)
            .map_err(|_| OxideError::InvalidFormat("feature bits exceed v1 header width"))?;
        self.expected_block_count = Some(block_count);
        self.feature_bits = feature_bits;
        self.pending_chunks.reserve(block_count as usize);
        Ok(())
    }

    /// Writes a single block to the archive state.
    ///
    /// Requires blocks to be submitted in strict sequential order by ID.
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

        self.stage_block_clone(block)
    }

    /// Writes a block and recycles its data buffer back to the pool after finalization.
    pub fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        self.ensure_header_written()?;

        let expected = usize::try_from(self.blocks_written)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;
        if block.id != expected {
            return Err(OxideError::InvalidBlockId {
                expected: expected as u64,
                actual: block.id as u64,
            });
        }

        self.stage_owned_block(block)
    }

    /// Pushes a block into the reorder buffer.
    ///
    /// If the block completes a contiguous sequence starting from the next
    /// expected ID, it and all subsequent ready blocks are staged.
    ///
    /// Returns the number of blocks accepted from the reorder drain.
    pub fn push_block(&mut self, block: CompressedBlock) -> Result<usize> {
        self.ensure_header_written()?;

        let ready = self.reorder.push(block.id, block)?;
        let mut accepted = 0usize;
        for block in ready {
            self.stage_owned_block(block)?;
            accepted += 1;
        }
        Ok(accepted)
    }

    /// Returns the number of blocks already staged for final output.
    pub fn blocks_written(&self) -> u32 {
        self.blocks_written
    }

    /// Returns the number of blocks currently pending in the reorder buffer.
    pub fn pending_blocks(&self) -> usize {
        self.reorder.pending_len()
    }

    /// Finalizes the archive by emitting the sectioned container and footer.
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

        let finalize_started = Instant::now();
        let section_table_offset = GLOBAL_HEADER_SIZE as u64;
        let header = GlobalHeader::with_feature_bits(
            CORE_SECTION_COUNT,
            self.feature_bits,
            section_table_offset,
        );

        let chunk_index_started = Instant::now();
        let mut chunk_index_bytes = Vec::with_capacity(
            self.pending_chunks
                .len()
                .saturating_mul(CHUNK_DESCRIPTOR_SIZE),
        );
        for chunk in &self.pending_chunks {
            chunk_index_bytes.extend_from_slice(&chunk.descriptor.to_bytes());
        }
        let chunk_index_elapsed_us = duration_to_us(chunk_index_started.elapsed());

        let payload_started = Instant::now();
        let mut payload_len = 0u64;
        let mut payload_hasher = Hasher::new();
        for chunk in &self.pending_chunks {
            payload_len = payload_len
                .checked_add(chunk.data.len() as u64)
                .ok_or(OxideError::InvalidFormat("payload length overflow"))?;
            payload_hasher.update(&chunk.data);
        }
        let payload_checksum = payload_hasher.finalize();
        let payload_elapsed_us = duration_to_us(payload_started.elapsed());

        let section_table_len = u64::from(CORE_SECTION_COUNT) * SECTION_TABLE_ENTRY_SIZE as u64;
        let chunk_index_offset = section_table_offset
            .checked_add(section_table_len)
            .ok_or(OxideError::InvalidFormat("section table offset overflow"))?;
        let chunk_index_len = chunk_index_bytes.len() as u64;
        let payload_offset = chunk_index_offset
            .checked_add(chunk_index_len)
            .ok_or(OxideError::InvalidFormat("payload offset overflow"))?;

        let chunk_index_checksum = crc32fast::hash(&chunk_index_bytes);
        let section_entries = [
            SectionTableEntry::new(
                SectionType::ChunkIndex,
                chunk_index_offset,
                chunk_index_len,
                chunk_index_checksum,
            ),
            SectionTableEntry::new(SectionType::DictionaryStore, payload_offset, 0, 0),
            SectionTableEntry::new(SectionType::EntropyTableStore, payload_offset, 0, 0),
            SectionTableEntry::new(SectionType::TransformChainTable, payload_offset, 0, 0),
            SectionTableEntry::new(SectionType::DedupReferenceTable, payload_offset, 0, 0),
            SectionTableEntry::new(
                SectionType::PayloadRegion,
                payload_offset,
                payload_len,
                payload_checksum,
            ),
        ];

        let section_table_started = Instant::now();
        let mut section_table_bytes = Vec::with_capacity(
            section_entries
                .len()
                .saturating_mul(SECTION_TABLE_ENTRY_SIZE),
        );
        for entry in section_entries {
            section_table_bytes.extend_from_slice(&entry.to_bytes());
        }
        let section_table_elapsed_us = duration_to_us(section_table_started.elapsed());

        let mut global_crc = Hasher::new();
        let header_bytes = header.to_bytes();
        self.write_tracked_bytes(&header_bytes, &mut global_crc)?;
        self.write_tracked_bytes(&section_table_bytes, &mut global_crc)?;
        self.write_tracked_bytes(&chunk_index_bytes, &mut global_crc)?;

        let pending_chunks = std::mem::take(&mut self.pending_chunks);
        for chunk in pending_chunks {
            self.write_tracked_bytes(&chunk.data, &mut global_crc)?;
            self.recycle_block_data(chunk.data);
        }

        let footer = Footer::new(global_crc.finalize());
        footer.write(&mut self.writer)?;

        let finalize_elapsed_us = duration_to_us(finalize_started.elapsed());
        telemetry::increment_counter(
            tags::METRIC_OXZ_WRITE_SECTION_TABLE_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "write_section_table")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_WRITE_SECTION_TABLE_LATENCY_US,
            section_table_elapsed_us,
            &[("subsystem", "oxz"), ("op", "write_section_table")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_WRITE_CHUNK_INDEX_LATENCY_US,
            chunk_index_elapsed_us,
            &[("subsystem", "oxz"), ("op", "write_chunk_index")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_WRITE_PAYLOAD_INDEX_LATENCY_US,
            payload_elapsed_us,
            &[("subsystem", "oxz"), ("op", "write_payload_meta")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_WRITE_CONTAINER_LATENCY_US,
            finalize_elapsed_us,
            &[("subsystem", "oxz"), ("op", "finalize_container")],
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "write_section_table",
            "ok",
            section_table_elapsed_us,
            "oxz section table written successfully",
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "write_chunk_index",
            "ok",
            chunk_index_elapsed_us,
            "oxz chunk index written successfully",
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "write_container",
            "ok",
            finalize_elapsed_us,
            "oxz container finalized successfully",
        );

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

    fn stage_block_clone(&mut self, block: &CompressedBlock) -> Result<()> {
        let owned = CompressedBlock {
            id: block.id,
            data: block.data.clone(),
            pre_proc: block.pre_proc.clone(),
            compression: block.compression,
            compression_preset: block.compression_preset,
            raw_passthrough: block.raw_passthrough,
            original_len: block.original_len,
            crc32: block.crc32,
        };
        self.stage_owned_block(owned)
    }

    fn stage_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        let start = Instant::now();
        let expected = self.expected_block_count.unwrap_or(0);
        if self.blocks_written >= expected {
            return Err(OxideError::InvalidFormat("archive block count exceeded"));
        }

        let descriptor = ChunkDescriptor::from_block(&block)?;
        self.pending_chunks.push(PendingChunk {
            descriptor,
            data: block.data,
        });
        self.blocks_written += 1;

        let elapsed_us = duration_to_us(start.elapsed());
        telemetry::increment_counter(
            tags::METRIC_OXZ_WRITE_BLOCK_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "stage_chunk")],
        );
        telemetry::increment_counter(
            tags::METRIC_OXZ_WRITE_CHUNK_DESCRIPTOR_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "stage_chunk_descriptor")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_WRITE_BLOCK_LATENCY_US,
            elapsed_us,
            &[("subsystem", "oxz"), ("op", "stage_chunk")],
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "stage_chunk",
            "ok",
            elapsed_us,
            "oxz chunk staged successfully",
        );

        Ok(())
    }

    fn write_tracked_bytes(&mut self, bytes: &[u8], global_crc: &mut Hasher) -> Result<()> {
        self.writer.write_all(bytes)?;
        global_crc.update(bytes);
        Ok(())
    }

    fn recycle_block_data(&self, mut data: Vec<u8>) {
        let mut pooled = self.buffer_pool.acquire();
        std::mem::swap(pooled.as_mut_vec(), &mut data);
    }
}
