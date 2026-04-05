use std::collections::{HashMap, VecDeque};
use std::io::{Seek, Write};
use std::time::Instant;

use blake3::Hash as Blake3Hash;

use crate::checksum::compute_checksum;
use crate::compression::apply_compression;
use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{ArchiveSourceKind, CompressedBlock, CompressionAlgo, OxideError, Result};

use super::headers::{FOOTER_FLAG_COMPRESSED_CHUNK_TABLE, FOOTER_FLAG_COMPRESSED_MANIFEST};
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

pub const DEFAULT_DEDUP_WINDOW_BLOCKS: usize = 131_072;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BlockDedupeKey {
    compression_flags: u8,
    raw_passthrough: bool,
    dictionary_id: u8,
    original_len: u64,
    encoded_len: usize,
    payload_hash: Blake3Hash,
}

impl BlockDedupeKey {
    fn from_block(block: &CompressedBlock) -> Self {
        Self {
            compression_flags: block.compression.to_flags(),
            raw_passthrough: block.raw_passthrough,
            dictionary_id: block.dictionary_id,
            original_len: block.original_len,
            encoded_len: block.data.len(),
            payload_hash: blake3::hash(block.data.as_slice()),
        }
    }
}

#[derive(Debug)]
struct BlockDeduper {
    max_entries: usize,
    entries: HashMap<BlockDedupeKey, u32>,
    insertion_order: VecDeque<BlockDedupeKey>,
}

impl Default for BlockDeduper {
    fn default() -> Self {
        Self::new(DEFAULT_DEDUP_WINDOW_BLOCKS)
    }
}

impl BlockDeduper {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    fn rewrite(&mut self, block: CompressedBlock) -> Result<CompressedBlock> {
        if block.is_reference() || self.max_entries == 0 {
            return Ok(block);
        }

        let key = BlockDedupeKey::from_block(&block);
        if let Some(&reference_target) = self.entries.get(&key) {
            return Ok(CompressedBlock::reference(
                block.id,
                block.stream_id,
                reference_target,
                block.original_len,
            ));
        }

        let block_id = u32::try_from(block.id)
            .map_err(|_| OxideError::InvalidFormat("block id exceeds u32 range"))?;
        self.entries.insert(key.clone(), block_id);
        self.insertion_order.push_back(key);

        while self.insertion_order.len() > self.max_entries {
            if let Some(evicted) = self.insertion_order.pop_front() {
                self.entries.remove(&evicted);
            }
        }

        Ok(block)
    }
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
    block_deduper: BlockDeduper,
}

/// Seekable variant of [`ArchiveWriter`] that validates the writer position
/// is at byte 0 before writing the global header.
#[derive(Debug)]
pub struct SeekableArchiveWriter<W: Write + Seek>(ArchiveWriter<W>);

impl<W: Write> ArchiveWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_manifest(writer, None)
    }

    pub fn with_manifest(writer: W, manifest: Option<ArchiveManifest>) -> Self {
        Self::with_limits_and_manifest(
            writer,
            DEFAULT_REORDER_PENDING_LIMIT,
            DEFAULT_DEDUP_WINDOW_BLOCKS,
            manifest,
        )
    }

    pub fn with_reorder_limit_and_manifest(
        writer: W,
        max_pending: usize,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self::with_limits_and_manifest(writer, max_pending, DEFAULT_DEDUP_WINDOW_BLOCKS, manifest)
    }

    pub fn with_limits_and_manifest(
        writer: W,
        max_pending: usize,
        dedupe_window_blocks: usize,
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
            block_deduper: BlockDeduper::new(dedupe_window_blocks),
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
            reference_target: block.reference_target,
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
        let chunk_table_raw_bytes = encode_chunk_table(&self.pending_descriptors)?;
        let metadata = encode_metadata_sections(&self.entry_table_bytes, &chunk_table_raw_bytes)?;
        let footer = build_footer(
            self.expected_block_count.unwrap_or(0),
            metadata.entry_table_bytes.as_slice(),
            metadata.chunk_table_bytes.as_slice(),
            metadata.footer_flags,
            &self.pending_descriptors,
        )?;

        let chunk_table_started = Instant::now();
        self.writer
            .write_all(metadata.entry_table_bytes.as_slice())?;
        self.writer
            .write_all(metadata.chunk_table_bytes.as_slice())?;
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

        let block = self.block_deduper.rewrite(block)?;
        let descriptor = ChunkDescriptor::from_block(&block, self.next_payload_offset)?;
        self.next_payload_offset = descriptor.payload_end()?;
        self.writer.write_all(block.data.as_slice())?;
        self.pending_descriptors.push(descriptor);
        self.blocks_written += 1;

        record_stage_telemetry(start.elapsed());
        Ok(())
    }
}

impl<W: Write + Seek> SeekableArchiveWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_manifest(writer, None)
    }

    pub fn with_manifest(writer: W, manifest: Option<ArchiveManifest>) -> Self {
        Self::with_limits_and_manifest(
            writer,
            DEFAULT_REORDER_PENDING_LIMIT,
            DEFAULT_DEDUP_WINDOW_BLOCKS,
            manifest,
        )
    }

    pub fn with_reorder_limit_and_manifest(
        writer: W,
        max_pending: usize,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self::with_limits_and_manifest(writer, max_pending, DEFAULT_DEDUP_WINDOW_BLOCKS, manifest)
    }

    pub fn with_limits_and_manifest(
        writer: W,
        max_pending: usize,
        dedupe_window_blocks: usize,
        manifest: Option<ArchiveManifest>,
    ) -> Self {
        Self(ArchiveWriter::with_limits_and_manifest(
            writer,
            max_pending,
            dedupe_window_blocks,
            manifest,
        ))
    }

    pub fn with_reorder_limit(writer: W, max_pending: usize) -> Self {
        Self::with_reorder_limit_and_manifest(writer, max_pending, None)
    }

    pub fn write_global_header(&mut self, block_count: u32) -> Result<()> {
        self.write_global_header_with_flags(block_count, 0)
    }

    pub fn write_global_header_with_flags(&mut self, block_count: u32, flags: u32) -> Result<()> {
        if self.0.writer.stream_position()? != 0 {
            return Err(OxideError::InvalidFormat(
                "seekable archive writer requires an empty destination positioned at start",
            ));
        }
        self.0.write_global_header_with_flags(block_count, flags)
    }

    pub fn write_block(&mut self, block: &CompressedBlock) -> Result<()> {
        self.0.write_block(block)
    }

    pub fn write_owned_block(&mut self, block: CompressedBlock) -> Result<()> {
        self.0.write_owned_block(block)
    }

    pub fn push_block(&mut self, block: CompressedBlock) -> Result<usize> {
        self.0.push_block(block)
    }

    pub fn blocks_written(&self) -> u32 {
        self.0.blocks_written()
    }

    pub fn pending_blocks(&self) -> usize {
        self.0.pending_blocks()
    }

    pub fn write_footer(self) -> Result<W> {
        self.0.write_footer()
    }

    pub fn into_inner(self) -> W {
        self.0.into_inner()
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
    block_count: u32,
    entry_table_bytes: &[u8],
    chunk_table_bytes: &[u8],
    footer_flags: u16,
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
    let chunk_table_len = u32::try_from(chunk_table_bytes.len())
        .map_err(|_| OxideError::InvalidFormat("chunk table length exceeds u32 range"))?;
    let _footer_offset = chunk_table_offset
        .checked_add(chunk_table_len as u64)
        .ok_or(OxideError::InvalidFormat("footer offset overflow"))?;

    Ok(Footer::new(
        block_count,
        entry_table_offset,
        entry_table_len,
        chunk_table_offset,
        chunk_table_len,
        footer_flags,
        compute_checksum(&[]),
    ))
}

struct EncodedMetadataSections {
    entry_table_bytes: Vec<u8>,
    chunk_table_bytes: Vec<u8>,
    footer_flags: u16,
}

fn encode_metadata_sections(
    entry_table_bytes: &[u8],
    chunk_table_bytes: &[u8],
) -> Result<EncodedMetadataSections> {
    let (entry_table_bytes, manifest_compressed) = maybe_compress_metadata(entry_table_bytes)?;
    let (chunk_table_bytes, chunk_table_compressed) = maybe_compress_metadata(chunk_table_bytes)?;

    let mut footer_flags = 0u16;
    if manifest_compressed {
        footer_flags |= FOOTER_FLAG_COMPRESSED_MANIFEST;
    }
    if chunk_table_compressed {
        footer_flags |= FOOTER_FLAG_COMPRESSED_CHUNK_TABLE;
    }

    Ok(EncodedMetadataSections {
        entry_table_bytes,
        chunk_table_bytes,
        footer_flags,
    })
}

fn maybe_compress_metadata(bytes: &[u8]) -> Result<(Vec<u8>, bool)> {
    if bytes.is_empty() {
        return Ok((Vec::new(), false));
    }

    let compressed = apply_compression(bytes, CompressionAlgo::Zstd)?;
    if compressed.len() < bytes.len() {
        Ok((compressed, true))
    } else {
        Ok((bytes.to_vec(), false))
    }
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
