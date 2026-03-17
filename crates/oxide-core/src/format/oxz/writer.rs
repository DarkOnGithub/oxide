use std::io::{Seek, SeekFrom, Write};
use std::time::Instant;

use crate::checksum::compute_checksum;
use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{ArchiveSourceKind, CompressedBlock, OxideError, Result};

use super::{
    encode_chunk_table, ArchiveManifest, ArchiveMetadata, Footer, GlobalHeader, ReorderBuffer,
    ARCHIVE_METADATA_SIZE, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE,
    DEFAULT_REORDER_PENDING_LIMIT, GLOBAL_HEADER_SIZE,
};

#[derive(Debug)]
struct PendingChunk {
    descriptor: super::ChunkDescriptor,
    data: crate::CompressedPayload,
}

#[derive(Debug)]
struct FinalizedLayout {
    header_bytes: [u8; GLOBAL_HEADER_SIZE],
    metadata_bytes: [u8; ARCHIVE_METADATA_SIZE],
    entry_table_bytes: Vec<u8>,
    chunk_table_bytes: Vec<u8>,
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
/// emitting the fixed-layout v1 container at finalization.
#[derive(Debug)]
pub struct ArchiveWriter<W: Write> {
    writer: W,
    manifest: ArchiveManifest,
    entry_table_bytes: Vec<u8>,
    source_kind: Option<ArchiveSourceKind>,
    expected_block_count: Option<u32>,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    payload_offset: u64,
    next_payload_offset: u64,
    pending_chunks: Vec<PendingChunk>,
}

/// Writes OXZ archives directly to a seekable destination by reserving the
/// container prefix up front, streaming payload bytes in block order, then
/// backfilling the header and metadata during finalization.
#[derive(Debug)]
pub struct SeekableArchiveWriter<W: Write + Seek> {
    writer: W,
    manifest: ArchiveManifest,
    entry_table_bytes: Vec<u8>,
    source_kind: Option<ArchiveSourceKind>,
    expected_block_count: Option<u32>,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    pending_descriptors: Vec<super::ChunkDescriptor>,
    payload_offset: u64,
    next_payload_offset: u64,
}

impl<W: Write> ArchiveWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_manifest(writer, None)
    }

    pub fn with_manifest(writer: W, manifest: Option<ArchiveManifest>) -> Self {
        Self::with_reorder_limit_and_manifest(writer, DEFAULT_REORDER_PENDING_LIMIT, manifest)
    }

    pub fn with_reorder_limit_and_manifest(
        writer: W,
        max_pending: usize,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self {
            writer,
            manifest: manifest.unwrap_or_default(),
            entry_table_bytes: Vec::new(),
            source_kind: None,
            expected_block_count: None,
            blocks_written: 0,
            reorder: ReorderBuffer::with_limit(max_pending),
            payload_offset: 0,
            next_payload_offset: 0,
            pending_chunks: Vec::new(),
        }
    }

    pub fn with_reorder_limit(writer: W, max_pending: usize) -> Self {
        Self::with_reorder_limit_and_manifest(writer, max_pending, None)
    }

    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        self.write_global_header_with_flags(block_count, 0)
    }

    pub fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        if self.expected_block_count.is_some() {
            return Err(OxideError::InvalidFormat("global header already written"));
        }

        let source_kind = archive_source_kind_from_flags(flags)?;
        let entry_table_bytes = self.manifest.encode()?;
        let payload_offset = payload_offset_bytes(block_count, entry_table_bytes.len())?;

        self.entry_table_bytes = entry_table_bytes;
        self.source_kind = Some(source_kind);
        self.expected_block_count = Some(block_count);
        self.payload_offset = payload_offset;
        self.next_payload_offset = payload_offset;
        self.pending_chunks.reserve(block_count as usize);
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

        self.stage_block_clone(block)
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
        let descriptors = self
            .pending_chunks
            .iter()
            .map(|chunk| chunk.descriptor)
            .collect::<Vec<_>>();
        let layout = finalize_layout(
            self.expected_block_count.unwrap_or(0),
            self.source_kind.unwrap_or(ArchiveSourceKind::File),
            &self.entry_table_bytes,
            &descriptors,
            self.next_payload_offset,
        )?;

        let header_write_elapsed = write_all_timed(&mut self.writer, &layout.header_bytes)?;
        write_all_timed(&mut self.writer, &layout.metadata_bytes)?;
        write_all_timed(&mut self.writer, &layout.entry_table_bytes)?;
        let chunk_table_write_elapsed =
            write_all_timed(&mut self.writer, &layout.chunk_table_bytes)?;

        let pending_chunks = std::mem::take(&mut self.pending_chunks);
        for chunk in pending_chunks {
            self.writer.write_all(chunk.data.as_slice())?;
        }

        let footer = Footer::new(compute_checksum(&[]));
        footer.write(&mut self.writer)?;

        record_finalize_telemetry(
            header_write_elapsed,
            chunk_table_write_elapsed,
            finalize_started.elapsed(),
        );
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

    fn stage_block_clone(&mut self, block: &CompressedBlock) -> Result<()> {
        let owned = CompressedBlock {
            id: block.id,
            stream_id: block.stream_id,
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

        let descriptor = super::ChunkDescriptor::from_block(&block, self.next_payload_offset)?;
        self.next_payload_offset = descriptor.payload_end()?;
        self.pending_chunks.push(PendingChunk {
            descriptor,
            data: block.data,
        });
        self.blocks_written += 1;

        record_stage_telemetry(start.elapsed());
        Ok(())
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
    pub fn new(writer: W) -> Self {
        Self::with_manifest(writer, None)
    }

    pub fn with_manifest(writer: W, manifest: Option<ArchiveManifest>) -> Self {
        Self::with_reorder_limit_and_manifest(writer, DEFAULT_REORDER_PENDING_LIMIT, manifest)
    }

    pub fn with_reorder_limit_and_manifest(
        writer: W,
        max_pending: usize,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self {
            writer,
            manifest: manifest.unwrap_or_default(),
            entry_table_bytes: Vec::new(),
            source_kind: None,
            expected_block_count: None,
            blocks_written: 0,
            reorder: ReorderBuffer::with_limit(max_pending),
            pending_descriptors: Vec::new(),
            payload_offset: 0,
            next_payload_offset: 0,
        }
    }

    pub fn with_reorder_limit(writer: W, max_pending: usize) -> Self {
        Self::with_reorder_limit_and_manifest(writer, max_pending, None)
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

        let source_kind = archive_source_kind_from_flags(flags)?;
        let entry_table_bytes = self.manifest.encode()?;
        let payload_offset = payload_offset_bytes(block_count, entry_table_bytes.len())?;

        reserve_prefix(&mut self.writer, payload_offset)?;

        self.entry_table_bytes = entry_table_bytes;
        self.source_kind = Some(source_kind);
        self.expected_block_count = Some(block_count);
        self.payload_offset = payload_offset;
        self.next_payload_offset = payload_offset;
        self.pending_descriptors.reserve(block_count as usize);
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
        let layout = finalize_layout(
            self.expected_block_count.unwrap_or(0),
            self.source_kind.unwrap_or(ArchiveSourceKind::File),
            &self.entry_table_bytes,
            &self.pending_descriptors,
            self.next_payload_offset,
        )?;

        self.writer.seek(SeekFrom::Start(0))?;
        let header_write_elapsed = write_all_timed(&mut self.writer, &layout.header_bytes)?;
        write_all_timed(&mut self.writer, &layout.metadata_bytes)?;
        write_all_timed(&mut self.writer, &layout.entry_table_bytes)?;
        let chunk_table_write_elapsed =
            write_all_timed(&mut self.writer, &layout.chunk_table_bytes)?;
        self.writer
            .seek(SeekFrom::Start(self.next_payload_offset))?;

        let footer = Footer::new(compute_checksum(&[]));
        footer.write(&mut self.writer)?;

        record_finalize_telemetry(
            header_write_elapsed,
            chunk_table_write_elapsed,
            finalize_started.elapsed(),
        );
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

        let descriptor = super::ChunkDescriptor::from_block(&block, self.next_payload_offset)?;
        self.next_payload_offset = descriptor.payload_end()?;
        self.writer.write_all(block.data.as_slice())?;
        self.pending_descriptors.push(descriptor);
        self.blocks_written += 1;

        record_stage_telemetry(start.elapsed());
        Ok(())
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

fn payload_offset_bytes(block_count: u32, entry_table_len: usize) -> Result<u64> {
    let chunk_table_len =
        CHUNK_TABLE_HEADER_SIZE as u64 + u64::from(block_count) * CHUNK_DESCRIPTOR_SIZE as u64;
    (GLOBAL_HEADER_SIZE as u64)
        .checked_add(ARCHIVE_METADATA_SIZE as u64)
        .and_then(|offset| offset.checked_add(entry_table_len as u64))
        .and_then(|offset| offset.checked_add(chunk_table_len))
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

fn finalize_layout(
    block_count: u32,
    source_kind: ArchiveSourceKind,
    entry_table_bytes: &[u8],
    descriptors: &[super::ChunkDescriptor],
    footer_offset: u64,
) -> Result<FinalizedLayout> {
    if descriptors.len() != block_count as usize {
        return Err(OxideError::InvalidFormat(
            "chunk descriptor count does not match declared block count",
        ));
    }

    let metadata_offset = GLOBAL_HEADER_SIZE as u64;
    let entry_table_offset = metadata_offset + ARCHIVE_METADATA_SIZE as u64;
    let chunk_table_offset = entry_table_offset
        .checked_add(entry_table_bytes.len() as u64)
        .ok_or(OxideError::InvalidFormat("entry table offset overflow"))?;
    let chunk_table_len =
        CHUNK_TABLE_HEADER_SIZE as u64 + descriptors.len() as u64 * CHUNK_DESCRIPTOR_SIZE as u64;
    let payload_offset = chunk_table_offset
        .checked_add(chunk_table_len)
        .ok_or(OxideError::InvalidFormat("chunk table offset overflow"))?;

    let mut expected_payload_offset = payload_offset;
    for descriptor in descriptors {
        if descriptor.payload_offset != expected_payload_offset {
            return Err(OxideError::InvalidFormat(
                "chunk payload offsets are not contiguous",
            ));
        }
        expected_payload_offset = descriptor.payload_end()?;
    }
    if expected_payload_offset != footer_offset {
        return Err(OxideError::InvalidFormat(
            "footer offset does not match chunk payload layout",
        ));
    }

    let metadata_bytes = ArchiveMetadata::new(source_kind).to_bytes();
    let chunk_table_bytes = encode_chunk_table(descriptors)?;
    let header_bytes = GlobalHeader::new(
        metadata_offset,
        entry_table_offset,
        chunk_table_offset,
        payload_offset,
        footer_offset,
    )
    .to_bytes();

    Ok(FinalizedLayout {
        header_bytes,
        metadata_bytes,
        entry_table_bytes: entry_table_bytes.to_vec(),
        chunk_table_bytes,
    })
}

fn write_all_timed<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<std::time::Duration> {
    let started = Instant::now();
    writer.write_all(bytes)?;
    Ok(started.elapsed())
}

fn archive_source_kind_from_flags(flags: u32) -> Result<ArchiveSourceKind> {
    match flags {
        0 => Ok(ArchiveSourceKind::File),
        1 => Ok(ArchiveSourceKind::Directory),
        _ => Err(OxideError::InvalidFormat(
            "unsupported archive metadata flags for OXZ v2",
        )),
    }
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

fn record_finalize_telemetry(
    header_write_elapsed: std::time::Duration,
    chunk_table_write_elapsed: std::time::Duration,
    finalize_elapsed: std::time::Duration,
) {
    let finalize_elapsed_us = duration_to_us(finalize_elapsed);
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_header",
        "ok",
        duration_to_us(header_write_elapsed),
        "oxz header written successfully",
    );
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_chunk_table",
        "ok",
        duration_to_us(chunk_table_write_elapsed),
        "oxz chunk table written successfully",
    );
    profile::event(
        tags::PROFILE_OXZ,
        &[tags::TAG_OXZ],
        "write_container",
        "ok",
        finalize_elapsed_us,
        "oxz container written successfully",
    );
}
