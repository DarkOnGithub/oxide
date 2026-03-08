use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Instant;

use crate::telemetry::{profile, tags};
use crate::types::{duration_to_us, placeholder_checksum, PLACEHOLDER_CHECKSUM};
use crate::{BufferPool, CompressedBlock, OxideError, Result};

use super::{
    encode_dictionary_store, ArchiveManifest, ChunkDescriptor, Footer, GlobalHeader, ReorderBuffer,
    SectionTableEntry, SectionType, StoredDictionary, CHUNK_DESCRIPTOR_SIZE, CORE_SECTION_COUNT,
    DEFAULT_REORDER_PENDING_LIMIT, GLOBAL_HEADER_SIZE, SECTION_TABLE_ENTRY_SIZE,
};

#[derive(Debug)]
struct PendingChunk {
    descriptor: ChunkDescriptor,
    data: Vec<u8>,
}

#[derive(Debug)]
struct FinalizedSections {
    header_bytes: [u8; GLOBAL_HEADER_SIZE],
    section_table_bytes: Vec<u8>,
    chunk_index_bytes: Vec<u8>,
    dictionary_store_bytes: Vec<u8>,
    manifest_bytes: Vec<u8>,
    chunk_index_elapsed_us: u64,
    dictionary_store_elapsed_us: u64,
    section_table_elapsed_us: u64,
}

pub trait ArchiveBlockWriter {
    type InnerWriter;

    fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()>;

    fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()>;

    fn push_block(&mut self, block: CompressedBlock) -> Result<usize>;

    fn pending_blocks(&self) -> usize;

    fn write_footer(self) -> Result<Self::InnerWriter>;
}

/// Writes OXZ archives by staging chunk descriptors and payloads, then
/// emitting a sectioned v1 container at finalization.
#[derive(Debug)]
pub struct ArchiveWriter<W: Write> {
    writer: W,
    buffer_pool: Arc<BufferPool>,
    dictionaries: Vec<StoredDictionary>,
    manifest_bytes: Vec<u8>,
    expected_block_count: Option<u32>,
    feature_bits: u16,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    pending_chunks: Vec<PendingChunk>,
}

/// Writes OXZ archives directly to a seekable destination by reserving the
/// container prefix up front, streaming payload bytes in block order, then
/// backfilling the header and section tables during finalization.
#[derive(Debug)]
pub struct SeekableArchiveWriter<W: Write + Seek> {
    writer: W,
    buffer_pool: Arc<BufferPool>,
    dictionaries: Vec<StoredDictionary>,
    dictionary_store_bytes: Vec<u8>,
    manifest_bytes: Vec<u8>,
    expected_block_count: Option<u32>,
    feature_bits: u16,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    pending_descriptors: Vec<ChunkDescriptor>,
    payload_offset: u64,
    payload_len: u64,
}

impl<W: Write> ArchiveWriter<W> {
    /// Creates a new archive writer with the default reorder limit.
    pub fn new(writer: W, buffer_pool: Arc<BufferPool>) -> Self {
        Self::with_dictionaries_and_manifest(writer, buffer_pool, Vec::new(), None)
    }

    /// Creates a new archive writer with explicit dictionary contents.
    pub fn with_dictionaries(
        writer: W,
        buffer_pool: Arc<BufferPool>,
        dictionaries: Vec<StoredDictionary>,
    ) -> Self {
        Self::with_dictionaries_and_manifest(writer, buffer_pool, dictionaries, None)
    }

    pub fn with_dictionaries_and_manifest(
        writer: W,
        buffer_pool: Arc<BufferPool>,
        dictionaries: Vec<StoredDictionary>,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self::with_reorder_limit_and_dictionaries(
            writer,
            buffer_pool,
            DEFAULT_REORDER_PENDING_LIMIT,
            dictionaries,
            manifest,
        )
    }

    /// Creates a new archive writer with a custom reorder limit.
    pub fn with_reorder_limit(writer: W, buffer_pool: Arc<BufferPool>, max_pending: usize) -> Self {
        Self::with_reorder_limit_and_dictionaries(
            writer,
            buffer_pool,
            max_pending,
            Vec::new(),
            None,
        )
    }

    fn with_reorder_limit_and_dictionaries(
        writer: W,
        buffer_pool: Arc<BufferPool>,
        max_pending: usize,
        dictionaries: Vec<StoredDictionary>,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self {
            writer,
            buffer_pool,
            dictionaries,
            manifest_bytes: manifest
                .map(|manifest| manifest.encode())
                .transpose()
                .expect("manifest encoding should succeed during writer construction")
                .unwrap_or_default(),
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
        validate_completion(
            self.expected_block_count,
            self.blocks_written,
            self.pending_blocks(),
        )?;

        let finalize_started = Instant::now();
        let payload_started = Instant::now();
        let mut payload_len = 0u64;
        for chunk in &self.pending_chunks {
            payload_len = payload_len
                .checked_add(chunk.data.len() as u64)
                .ok_or(OxideError::InvalidFormat("payload length overflow"))?;
        }
        let _payload_elapsed_us = duration_to_us(payload_started.elapsed());

        let sections = finalize_sections(
            self.pending_chunks.iter().map(|chunk| chunk.descriptor),
            &self.dictionaries,
            &self.manifest_bytes,
            payload_len,
            self.feature_bits,
        )?;

        self.write_tracked_bytes(&sections.header_bytes)?;
        self.write_tracked_bytes(&sections.section_table_bytes)?;
        self.write_tracked_bytes(&sections.chunk_index_bytes)?;
        self.write_tracked_bytes(&sections.dictionary_store_bytes)?;
        self.write_tracked_bytes(&sections.manifest_bytes)?;

        let pending_chunks = std::mem::take(&mut self.pending_chunks);
        for chunk in pending_chunks {
            self.write_tracked_bytes(&chunk.data)?;
            self.recycle_block_data(chunk.data);
        }

        let footer = Footer::new(PLACEHOLDER_CHECKSUM);
        footer.write(&mut self.writer)?;

        record_finalize_telemetry(&sections, finalize_started.elapsed());

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
            stream_id: block.stream_id,
            data: block.data.clone(),
            pre_proc: block.pre_proc.clone(),
            compression: block.compression,
            compression_preset: block.compression_preset,
            dict_id: block.dict_id,
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

        record_stage_telemetry(start.elapsed());

        Ok(())
    }

    fn write_tracked_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
        Ok(())
    }

    fn recycle_block_data(&self, mut data: Vec<u8>) {
        let mut pooled = self.buffer_pool.acquire();
        std::mem::swap(pooled.as_mut_vec(), &mut data);
    }
}

impl<W: Write> ArchiveBlockWriter for ArchiveWriter<W> {
    type InnerWriter = W;

    fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        Self::write_global_header_with_flags(self, block_count, flags)
    }

    fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        Self::write_owned_block(self, block)
    }

    fn push_block(&mut self, block: CompressedBlock) -> Result<usize> {
        Self::push_block(self, block)
    }

    fn pending_blocks(&self) -> usize {
        Self::pending_blocks(self)
    }

    fn write_footer(self) -> Result<Self::InnerWriter> {
        Self::write_footer(self)
    }
}

impl<W: Write + Seek> SeekableArchiveWriter<W> {
    /// Creates a new seekable archive writer with the default reorder limit.
    pub fn new(writer: W, buffer_pool: Arc<BufferPool>) -> Self {
        Self::with_dictionaries_and_manifest(writer, buffer_pool, Vec::new(), None)
    }

    /// Creates a new seekable archive writer with explicit dictionary contents.
    pub fn with_dictionaries(
        writer: W,
        buffer_pool: Arc<BufferPool>,
        dictionaries: Vec<StoredDictionary>,
    ) -> Self {
        Self::with_dictionaries_and_manifest(writer, buffer_pool, dictionaries, None)
    }

    pub fn with_dictionaries_and_manifest(
        writer: W,
        buffer_pool: Arc<BufferPool>,
        dictionaries: Vec<StoredDictionary>,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self::with_reorder_limit_and_dictionaries(
            writer,
            buffer_pool,
            DEFAULT_REORDER_PENDING_LIMIT,
            dictionaries,
            manifest,
        )
    }

    /// Creates a new seekable archive writer with a custom reorder limit.
    pub fn with_reorder_limit(writer: W, buffer_pool: Arc<BufferPool>, max_pending: usize) -> Self {
        Self::with_reorder_limit_and_dictionaries(
            writer,
            buffer_pool,
            max_pending,
            Vec::new(),
            None,
        )
    }

    fn with_reorder_limit_and_dictionaries(
        writer: W,
        buffer_pool: Arc<BufferPool>,
        max_pending: usize,
        dictionaries: Vec<StoredDictionary>,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self {
            writer,
            buffer_pool,
            dictionaries,
            dictionary_store_bytes: Vec::new(),
            manifest_bytes: manifest
                .map(|manifest| manifest.encode())
                .transpose()
                .expect("manifest encoding should succeed during writer construction")
                .unwrap_or_default(),
            expected_block_count: None,
            feature_bits: 0,
            blocks_written: 0,
            reorder: ReorderBuffer::with_limit(max_pending),
            pending_descriptors: Vec::new(),
            payload_offset: 0,
            payload_len: 0,
        }
    }

    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        self.write_global_header_with_flags(block_count, 0)
    }

    pub fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        if self.expected_block_count.is_some() {
            return Err(OxideError::InvalidFormat("global header already written"));
        }

        if self.writer.stream_position()? != 0 {
            return Err(OxideError::InvalidFormat(
                "seekable archive writer requires an empty destination positioned at start",
            ));
        }

        let feature_bits = u16::try_from(flags)
            .map_err(|_| OxideError::InvalidFormat("feature bits exceed v1 header width"))?;
        let dictionary_store_bytes = encode_dictionary_store(&self.dictionaries)?;
        let payload_offset = payload_offset_bytes(
            block_count,
            dictionary_store_bytes.len(),
            self.manifest_bytes.len(),
        )?;

        reserve_prefix(&mut self.writer, payload_offset)?;

        self.dictionary_store_bytes = dictionary_store_bytes;
        self.expected_block_count = Some(block_count);
        self.feature_bits = feature_bits;
        self.pending_descriptors.reserve(block_count as usize);
        self.payload_offset = payload_offset;
        self.payload_len = 0;

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

        let owned = CompressedBlock {
            id: block.id,
            stream_id: block.stream_id,
            data: block.data.clone(),
            pre_proc: block.pre_proc.clone(),
            compression: block.compression,
            compression_preset: block.compression_preset,
            dict_id: block.dict_id,
            raw_passthrough: block.raw_passthrough,
            original_len: block.original_len,
            crc32: block.crc32,
        };
        self.stage_owned_block(owned)
    }

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

    pub fn blocks_written(&self) -> u32 {
        self.blocks_written
    }

    pub fn pending_blocks(&self) -> usize {
        self.reorder.pending_len()
    }

    pub fn write_footer(mut self) -> Result<W> {
        self.ensure_header_written()?;
        validate_completion(
            self.expected_block_count,
            self.blocks_written,
            self.pending_blocks(),
        )?;

        let finalize_started = Instant::now();
        let sections = finalize_sections(
            self.pending_descriptors.iter().copied(),
            &self.dictionaries,
            &self.manifest_bytes,
            self.payload_len,
            self.feature_bits,
        )?;

        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&sections.header_bytes)?;
        self.writer.write_all(&sections.section_table_bytes)?;
        self.writer.write_all(&sections.chunk_index_bytes)?;
        self.writer.write_all(&sections.dictionary_store_bytes)?;
        self.writer.write_all(&sections.manifest_bytes)?;
        self.writer.seek(SeekFrom::Start(
            self.payload_offset.saturating_add(self.payload_len),
        ))?;

        let footer = Footer::new(PLACEHOLDER_CHECKSUM);
        footer.write(&mut self.writer)?;

        record_finalize_telemetry(&sections, finalize_started.elapsed());

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

    fn stage_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        let start = Instant::now();
        let expected = self.expected_block_count.unwrap_or(0);
        if self.blocks_written >= expected {
            return Err(OxideError::InvalidFormat("archive block count exceeded"));
        }

        let descriptor = ChunkDescriptor::from_block(&block)?;
        self.writer.write_all(&block.data)?;
        self.payload_len = self
            .payload_len
            .checked_add(block.data.len() as u64)
            .ok_or(OxideError::InvalidFormat("payload length overflow"))?;
        self.pending_descriptors.push(descriptor);
        self.blocks_written += 1;
        self.recycle_block_data(block.data);

        record_stage_telemetry(start.elapsed());

        Ok(())
    }

    fn recycle_block_data(&self, mut data: Vec<u8>) {
        let mut pooled = self.buffer_pool.acquire();
        std::mem::swap(pooled.as_mut_vec(), &mut data);
    }
}

impl<W: Write + Seek> ArchiveBlockWriter for SeekableArchiveWriter<W> {
    type InnerWriter = W;

    fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        Self::write_global_header_with_flags(self, block_count, flags)
    }

    fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        Self::write_owned_block(self, block)
    }

    fn push_block(&mut self, block: CompressedBlock) -> Result<usize> {
        Self::push_block(self, block)
    }

    fn pending_blocks(&self) -> usize {
        Self::pending_blocks(self)
    }

    fn write_footer(self) -> Result<Self::InnerWriter> {
        Self::write_footer(self)
    }
}

fn validate_completion(
    expected_block_count: Option<u32>,
    blocks_written: u32,
    pending_blocks: usize,
) -> Result<()> {
    if pending_blocks > 0 {
        return Err(OxideError::InvalidFormat(
            "cannot finalize archive with pending reordered blocks",
        ));
    }

    let expected = expected_block_count.unwrap_or(0);
    if blocks_written != expected {
        return Err(OxideError::InvalidFormat(
            "block count mismatch before writing footer",
        ));
    }

    Ok(())
}

fn section_count(has_manifest: bool) -> u16 {
    CORE_SECTION_COUNT + if has_manifest { 1 } else { 0 }
}

fn payload_offset_bytes(
    block_count: u32,
    dictionary_bytes: usize,
    manifest_bytes: usize,
) -> Result<u64> {
    let section_table_len =
        u64::from(section_count(manifest_bytes > 0)) * SECTION_TABLE_ENTRY_SIZE as u64;
    let chunk_index_len = u64::from(block_count) * CHUNK_DESCRIPTOR_SIZE as u64;
    (GLOBAL_HEADER_SIZE as u64)
        .checked_add(section_table_len)
        .and_then(|offset| offset.checked_add(chunk_index_len))
        .and_then(|offset| offset.checked_add(dictionary_bytes as u64))
        .and_then(|offset| offset.checked_add(manifest_bytes as u64))
        .ok_or(OxideError::InvalidFormat("payload offset overflow"))
}

fn reserve_prefix<W: Write>(writer: &mut W, bytes: u64) -> Result<()> {
    let zeros = [0u8; 8192];
    let mut remaining = bytes;
    while remaining > 0 {
        let chunk = remaining.min(zeros.len() as u64) as usize;
        writer.write_all(&zeros[..chunk])?;
        remaining -= chunk as u64;
    }
    Ok(())
}

fn finalize_sections<I>(
    descriptors: I,
    dictionaries: &[StoredDictionary],
    manifest_bytes: &[u8],
    payload_len: u64,
    feature_bits: u16,
) -> Result<FinalizedSections>
where
    I: IntoIterator<Item = ChunkDescriptor>,
{
    let section_table_offset = GLOBAL_HEADER_SIZE as u64;
    let header = GlobalHeader::with_feature_bits(
        section_count(!manifest_bytes.is_empty()),
        feature_bits,
        section_table_offset,
    );

    let chunk_index_started = Instant::now();
    let mut chunk_index_bytes = Vec::new();
    for descriptor in descriptors {
        chunk_index_bytes.extend_from_slice(&descriptor.to_bytes());
    }
    let chunk_index_elapsed_us = duration_to_us(chunk_index_started.elapsed());

    let dictionary_store_started = Instant::now();
    let dictionary_store_bytes = encode_dictionary_store(dictionaries)?;
    let dictionary_store_elapsed_us = duration_to_us(dictionary_store_started.elapsed());

    let manifest_bytes = manifest_bytes.to_vec();

    let section_table_len =
        u64::from(section_count(!manifest_bytes.is_empty())) * SECTION_TABLE_ENTRY_SIZE as u64;
    let chunk_index_offset = section_table_offset
        .checked_add(section_table_len)
        .ok_or(OxideError::InvalidFormat("section table offset overflow"))?;
    let chunk_index_len = chunk_index_bytes.len() as u64;
    let dictionary_store_offset =
        chunk_index_offset
            .checked_add(chunk_index_len)
            .ok_or(OxideError::InvalidFormat(
                "dictionary store offset overflow",
            ))?;
    let dictionary_store_len = dictionary_store_bytes.len() as u64;
    let manifest_offset = dictionary_store_offset
        .checked_add(dictionary_store_len)
        .ok_or(OxideError::InvalidFormat("manifest offset overflow"))?;
    let manifest_len = manifest_bytes.len() as u64;
    let payload_offset = manifest_offset
        .checked_add(manifest_len)
        .ok_or(OxideError::InvalidFormat("payload offset overflow"))?;

    let chunk_index_checksum = placeholder_checksum(&chunk_index_bytes);
    let dictionary_store_checksum = placeholder_checksum(&dictionary_store_bytes);
    let manifest_checksum = placeholder_checksum(&manifest_bytes);
    let mut section_entries = vec![
        SectionTableEntry::new(
            SectionType::ChunkIndex,
            chunk_index_offset,
            chunk_index_len,
            chunk_index_checksum,
        ),
        SectionTableEntry::new(
            SectionType::DictionaryStore,
            dictionary_store_offset,
            dictionary_store_len,
            dictionary_store_checksum,
        ),
        SectionTableEntry::new(SectionType::EntropyTableStore, payload_offset, 0, 0),
        SectionTableEntry::new(SectionType::TransformChainTable, payload_offset, 0, 0),
        SectionTableEntry::new(SectionType::DedupReferenceTable, payload_offset, 0, 0),
    ];
    if !manifest_bytes.is_empty() {
        section_entries.push(SectionTableEntry::new(
            SectionType::ArchiveManifest,
            manifest_offset,
            manifest_len,
            manifest_checksum,
        ));
    }
    section_entries.push(SectionTableEntry::new(
        SectionType::PayloadRegion,
        payload_offset,
        payload_len,
        PLACEHOLDER_CHECKSUM,
    ));

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

    Ok(FinalizedSections {
        header_bytes: header.to_bytes(),
        section_table_bytes,
        chunk_index_bytes,
        dictionary_store_bytes,
        manifest_bytes,
        chunk_index_elapsed_us,
        dictionary_store_elapsed_us,
        section_table_elapsed_us,
    })
}

fn record_stage_telemetry(elapsed: std::time::Duration) {
    let elapsed_us = duration_to_us(elapsed);
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "stage_chunk",
        "ok",
        elapsed_us,
        "oxz chunk staged successfully",
    );
}

fn record_finalize_telemetry(sections: &FinalizedSections, finalize_elapsed: std::time::Duration) {
    let finalize_elapsed_us = duration_to_us(finalize_elapsed);
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_section_table",
        "ok",
        sections.section_table_elapsed_us,
        "oxz section table written successfully",
    );
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_chunk_index",
        "ok",
        sections.chunk_index_elapsed_us,
        "oxz chunk index written successfully",
    );
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_dictionary_store",
        "ok",
        sections.dictionary_store_elapsed_us,
        "oxz dictionary store written successfully",
    );
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_container",
        "ok",
        finalize_elapsed_us,
        "oxz container finalized successfully",
    );
}
