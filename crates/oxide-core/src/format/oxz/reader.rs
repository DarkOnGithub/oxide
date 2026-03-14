use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{ArchiveSourceKind, OxideError, Result};

use super::{
    ARCHIVE_METADATA_SIZE, ArchiveManifest, ArchiveMetadata, CHUNK_TABLE_HEADER_SIZE,
    ChunkDescriptor, FOOTER_SIZE, Footer, GlobalHeader, decode_chunk_table,
};

/// Reads OXZ archives and provides access to individual chunk payloads.
#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    metadata: ArchiveMetadata,
    manifest: ArchiveManifest,
    chunk_descriptors: Vec<ChunkDescriptor>,
    footer: Footer,
}

impl<R: Read + Seek> ArchiveReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let header_started = Instant::now();
        let global_header = GlobalHeader::read(&mut reader)?;
        let header_elapsed_us = duration_to_us(header_started.elapsed());

        let file_len = reader.seek(SeekFrom::End(0))?;
        let expected_file_len = global_header
            .footer_offset
            .checked_add(FOOTER_SIZE as u64)
            .ok_or(OxideError::InvalidFormat("archive length overflow"))?;
        if file_len != expected_file_len {
            return Err(OxideError::InvalidFormat(
                "archive length does not match declared footer offset",
            ));
        }

        let metadata_started = Instant::now();
        let metadata = Self::read_metadata(&mut reader, global_header)?;
        let metadata_elapsed_us = duration_to_us(metadata_started.elapsed());

        let manifest = Self::read_manifest(&mut reader, global_header)?;

        let chunk_table_started = Instant::now();
        let chunk_descriptors = Self::read_chunk_table(&mut reader, global_header)?;
        let chunk_table_elapsed_us = duration_to_us(chunk_table_started.elapsed());

        Self::validate_chunk_layout(&chunk_descriptors, global_header)?;

        reader.seek(SeekFrom::Start(global_header.footer_offset))?;
        let footer = Footer::read(&mut reader)?;

        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_header",
            "ok",
            header_elapsed_us,
            "oxz header read successfully",
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_archive_metadata",
            "ok",
            metadata_elapsed_us,
            "oxz archive metadata read successfully",
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_chunk_table",
            "ok",
            chunk_table_elapsed_us,
            "oxz chunk table read successfully",
        );
        Ok(Self {
            reader,
            global_header,
            metadata,
            manifest,
            chunk_descriptors,
            footer,
        })
    }

    pub(crate) fn new_for_sequential_extract(reader: R) -> Result<Self> {
        Self::new(reader)
    }

    pub fn block_count(&self) -> u32 {
        self.chunk_descriptors.len() as u32
    }

    pub fn global_header(&self) -> GlobalHeader {
        self.global_header
    }

    pub fn source_kind(&self) -> ArchiveSourceKind {
        self.metadata.source_kind
    }

    pub fn footer(&self) -> Footer {
        self.footer
    }

    pub fn manifest(&self) -> &ArchiveManifest {
        &self.manifest
    }

    pub(crate) fn block_descriptor(&self, index: u32) -> Result<ChunkDescriptor> {
        let index_val = usize::try_from(index)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;

        self.chunk_descriptors
            .get(index_val)
            .copied()
            .ok_or(OxideError::InvalidFormat("block index out of range"))
    }

    pub(crate) fn block_descriptors(&self) -> &[ChunkDescriptor] {
        &self.chunk_descriptors
    }

    pub fn read_block(&mut self, index: u32) -> Result<(ChunkDescriptor, Vec<u8>)> {
        let start = Instant::now();
        let descriptor = self.block_descriptor(index)?;

        self.reader
            .seek(SeekFrom::Start(descriptor.payload_offset))?;
        let mut data = vec![0u8; descriptor.encoded_len as usize];
        self.reader.read_exact(&mut data)?;

        let elapsed_us = duration_to_us(start.elapsed());
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_chunk",
            "ok",
            elapsed_us,
            "oxz chunk read successfully",
        );

        Ok((descriptor, data))
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

    pub(crate) fn finish_sequential_extract_validation(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_metadata(reader: &mut R, header: GlobalHeader) -> Result<ArchiveMetadata> {
        let len = header
            .entry_table_offset
            .checked_sub(header.metadata_offset)
            .ok_or(OxideError::InvalidFormat("metadata length underflow"))?;
        if len != ARCHIVE_METADATA_SIZE as u64 {
            return Err(OxideError::InvalidFormat(
                "archive metadata length does not match fixed size",
            ));
        }

        reader.seek(SeekFrom::Start(header.metadata_offset))?;
        ArchiveMetadata::read(reader)
    }

    fn read_manifest(reader: &mut R, header: GlobalHeader) -> Result<ArchiveManifest> {
        let len = header
            .chunk_table_offset
            .checked_sub(header.entry_table_offset)
            .ok_or(OxideError::InvalidFormat("entry table length underflow"))?;
        let len = usize::try_from(len)
            .map_err(|_| OxideError::InvalidFormat("entry table length exceeds usize range"))?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(header.entry_table_offset))?;
        reader.read_exact(&mut bytes)?;
        ArchiveManifest::decode(&bytes)
    }

    fn read_chunk_table(reader: &mut R, header: GlobalHeader) -> Result<Vec<ChunkDescriptor>> {
        let len = header
            .payload_offset
            .checked_sub(header.chunk_table_offset)
            .ok_or(OxideError::InvalidFormat("chunk table length underflow"))?;
        let len = usize::try_from(len)
            .map_err(|_| OxideError::InvalidFormat("chunk table length exceeds usize range"))?;
        if len < CHUNK_TABLE_HEADER_SIZE {
            return Err(OxideError::InvalidFormat("chunk table is too short"));
        }
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(header.chunk_table_offset))?;
        reader.read_exact(&mut bytes)?;
        decode_chunk_table(&bytes)
    }

    fn validate_chunk_layout(descriptors: &[ChunkDescriptor], header: GlobalHeader) -> Result<()> {
        let mut expected_offset = header.payload_offset;
        for descriptor in descriptors {
            if descriptor.payload_offset != expected_offset {
                return Err(OxideError::InvalidFormat(
                    "chunk payload offsets are not contiguous",
                ));
            }
            expected_offset = descriptor.payload_end()?;
        }

        if expected_offset != header.footer_offset {
            return Err(OxideError::InvalidFormat(
                "footer offset does not match chunk payload layout",
            ));
        }

        Ok(())
    }
}

/// An iterator over chunks in an OXZ archive.
pub struct BlockIterator<'a, R: Read + Seek> {
    reader: &'a mut ArchiveReader<R>,
    next_index: u32,
}

impl<R: Read + Seek> Iterator for BlockIterator<'_, R> {
    type Item = Result<(ChunkDescriptor, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.reader.block_count() {
            return None;
        }

        let current = self.next_index;
        self.next_index += 1;
        Some(self.reader.read_block(current))
    }
}
