use std::io::{Seek, Write};
use std::time::Instant;

use crate::checksum::compute_checksum;
use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{ArchiveSourceKind, CompressedBlock, OxideError, Result};

use super::{
    ArchiveManifest, ChunkDescriptor, DEFAULT_REORDER_PENDING_LIMIT, Footer, GLOBAL_HEADER_SIZE,
    GlobalHeader, ReorderBuffer, encode_chunk_table,
};

pub trait ArchiveBlockWriter {
    type InnerWriter;

    fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()>;

    fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()>;

    fn push_block(&mut self, block: CompressedBlock) -> Result<usize>;

    fn pending_blocks(&self) -> usize;

    fn write_footer(self) -> Result<Self::InnerWriter>;
}

#[derive(Debug)]
pub struct ArchiveWriter<W: Write> {
    writer: W,
    manifest: ArchiveManifest,
    entry_table_bytes: Vec<u8>,
    source_kind: Option<ArchiveSourceKind>,
    expected_block_count: Option<u32>,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    next_payload_offset: u64,
    pending_descriptors: Vec<ChunkDescriptor>,
}

#[derive(Debug)]
pub struct SeekableArchiveWriter<W: Write + Seek> {
    writer: W,
    manifest: ArchiveManifest,
    entry_table_bytes: Vec<u8>,
    source_kind: Option<ArchiveSourceKind>,
    expected_block_count: Option<u32>,
    blocks_written: u32,
    reorder: ReorderBuffer<CompressedBlock>,
    next_payload_offset: u64,
    pending_descriptors: Vec<ChunkDescriptor>,
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
            next_payload_offset: GLOBAL_HEADER_SIZE as u64,
            pending_descriptors: Vec::new(),
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
        self.entry_table_bytes = self.manifest.encode()?;
        self.source_kind = Some(source_kind);
        self.expected_block_count = Some(block_count);
        self.pending_descriptors.reserve(block_count as usize);
        self.next_payload_offset = GLOBAL_HEADER_SIZE as u64;

        let header = GlobalHeader::new(source_kind, block_count, 0, 0, 0, 0, 0);
        let started = Instant::now();
        header.write(&mut self.writer)?;
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "write_header",
            "ok",
            duration_to_us(started.elapsed()),
            "oxz header written successfully",
        );
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
            compression: block.compression,
            raw_passthrough: block.raw_passthrough,
            dictionary_id: block.dictionary_id,
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
        let footer = build_footer(
            self.source_kind.unwrap_or(ArchiveSourceKind::File),
            self.expected_block_count.unwrap_or(0),
            &self.entry_table_bytes,
            &self.pending_descriptors,
        )?;

        let chunk_table_started = Instant::now();
        self.writer.write_all(&self.entry_table_bytes)?;
        let chunk_table_bytes = encode_chunk_table(&self.pending_descriptors)?;
        self.writer.write_all(&chunk_table_bytes)?;
        footer.write(&mut self.writer)?;

        record_finalize_telemetry(chunk_table_started.elapsed(), finalize_started.elapsed());
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

        let descriptor = ChunkDescriptor::from_block(&block, self.next_payload_offset)?;
        self.next_payload_offset = descriptor.payload_end()?;
        self.writer.write_all(block.data.as_slice())?;
        self.pending_descriptors.push(descriptor);
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
            next_payload_offset: GLOBAL_HEADER_SIZE as u64,
            pending_descriptors: Vec::new(),
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
        self.entry_table_bytes = self.manifest.encode()?;
        self.source_kind = Some(source_kind);
        self.expected_block_count = Some(block_count);
        self.pending_descriptors.reserve(block_count as usize);
        self.next_payload_offset = GLOBAL_HEADER_SIZE as u64;

        let header = GlobalHeader::new(source_kind, block_count, 0, 0, 0, 0, 0);
        let started = Instant::now();
        header.write(&mut self.writer)?;
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "write_header",
            "ok",
            duration_to_us(started.elapsed()),
            "oxz header written successfully",
        );
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
            compression: block.compression,
            raw_passthrough: block.raw_passthrough,
            dictionary_id: block.dictionary_id,
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
        let footer = build_footer(
            self.source_kind.unwrap_or(ArchiveSourceKind::File),
            self.expected_block_count.unwrap_or(0),
            &self.entry_table_bytes,
            &self.pending_descriptors,
        )?;

        let chunk_table_started = Instant::now();
        self.writer.write_all(&self.entry_table_bytes)?;
        let chunk_table_bytes = encode_chunk_table(&self.pending_descriptors)?;
        self.writer.write_all(&chunk_table_bytes)?;
        footer.write(&mut self.writer)?;

        record_finalize_telemetry(chunk_table_started.elapsed(), finalize_started.elapsed());
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

        let descriptor = ChunkDescriptor::from_block(&block, self.next_payload_offset)?;
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

fn build_footer(
    source_kind: ArchiveSourceKind,
    block_count: u32,
    entry_table_bytes: &[u8],
    descriptors: &[ChunkDescriptor],
) -> Result<Footer> {
    if descriptors.len() != block_count as usize {
        return Err(OxideError::InvalidFormat(
            "chunk descriptor count does not match declared block count",
        ));
    }

    let payload_end = descriptors
        .last()
        .map(ChunkDescriptor::payload_end)
        .transpose()?
        .unwrap_or(GLOBAL_HEADER_SIZE as u64);
    let entry_table_offset = payload_end;
    let entry_table_len = u32::try_from(entry_table_bytes.len())
        .map_err(|_| OxideError::InvalidFormat("manifest length exceeds u32 range"))?;
    let chunk_table_offset = entry_table_offset
        .checked_add(entry_table_len as u64)
        .ok_or(OxideError::InvalidFormat("manifest offset overflow"))?;
    let chunk_table_len = u32::try_from(
        descriptors
            .len()
            .checked_mul(super::CHUNK_DESCRIPTOR_SIZE)
            .ok_or(OxideError::InvalidFormat("chunk table length overflow"))?,
    )
    .map_err(|_| OxideError::InvalidFormat("chunk table length exceeds u32 range"))?;
    let footer_offset = chunk_table_offset
        .checked_add(chunk_table_len as u64)
        .ok_or(OxideError::InvalidFormat("footer offset overflow"))?;

    let _header = GlobalHeader::new(
        source_kind,
        block_count,
        entry_table_offset,
        entry_table_len,
        chunk_table_offset,
        chunk_table_len,
        footer_offset,
    );

    Ok(Footer::new(
        block_count,
        entry_table_offset,
        entry_table_len,
        chunk_table_offset,
        chunk_table_len,
        compute_checksum(&[]),
    ))
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
    chunk_table_write_elapsed: std::time::Duration,
    finalize_elapsed: std::time::Duration,
) {
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
        duration_to_us(finalize_elapsed),
        "oxz container written successfully",
    );
}
