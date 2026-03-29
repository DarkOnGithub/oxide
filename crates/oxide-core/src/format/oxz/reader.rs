use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{ArchiveSourceKind, OxideError, Result};

use super::{
    ArchiveManifest, ArchiveMetadata, ChunkDescriptor, Footer, GlobalHeader, decode_chunk_table,
};

#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    metadata: ArchiveMetadata,
    manifest: ArchiveManifest,
    chunk_descriptors: Vec<ChunkDescriptor>,
    footer: Footer,
    sequential_extract_state: Option<SequentialExtractState>,
}

#[derive(Debug, Clone, Copy)]
struct SequentialExtractState {
    next_block_index: u32,
    next_payload_offset: u64,
}

impl<R: Read + Seek> ArchiveReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let header_started = Instant::now();
        let global_header = GlobalHeader::read(&mut reader)?;
        let header_elapsed_us = duration_to_us(header_started.elapsed());

        let metadata_started = Instant::now();
        let metadata = ArchiveMetadata::from_header(global_header);
        let metadata_elapsed_us = duration_to_us(metadata_started.elapsed());

        let manifest = Self::read_manifest(&mut reader, global_header)?;

        let chunk_table_started = Instant::now();
        let chunk_descriptors = Self::read_chunk_table(&mut reader, global_header)?;
        let chunk_table_elapsed_us = duration_to_us(chunk_table_started.elapsed());

        Self::validate_chunk_layout(
            &chunk_descriptors,
            global_header,
            manifest.dictionary_bank(),
        )?;

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
            sequential_extract_state: None,
        })
    }

    pub(crate) fn new_for_sequential_extract(reader: R) -> Result<Self> {
        let mut archive = Self::new(reader)?;
        archive
            .reader
            .seek(SeekFrom::Start(archive.global_header.payload_offset))?;
        archive.sequential_extract_state = Some(SequentialExtractState {
            next_block_index: 0,
            next_payload_offset: archive.global_header.payload_offset,
        });
        Ok(archive)
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
        let mut data = Vec::new();
        let descriptor = self.read_block_into(index, &mut data)?;
        Ok((descriptor, data))
    }

    pub(crate) fn read_block_into(
        &mut self,
        index: u32,
        buffer: &mut Vec<u8>,
    ) -> Result<ChunkDescriptor> {
        let start = Instant::now();
        let descriptor = self.block_descriptor(index)?;
        let resolved = self.resolve_data_descriptor(index, descriptor)?;
        let is_reference = descriptor.is_reference();

        if self.sequential_extract_state.map(|state| {
            state.next_block_index == index && state.next_payload_offset == resolved.payload_offset
        }) != Some(true)
        {
            self.reader.seek(SeekFrom::Start(resolved.payload_offset))?;
        }

        buffer.clear();
        buffer.resize(resolved.encoded_len as usize, 0);
        self.reader.read_exact(buffer.as_mut_slice())?;

        if let Some(state) = self.sequential_extract_state.as_mut() {
            state.next_block_index = index.saturating_add(1);
            state.next_payload_offset = descriptor.payload_end()?;
        }

        if is_reference && self.sequential_extract_state.is_some() {
            self.reader
                .seek(SeekFrom::Start(descriptor.payload_end()?))?;
        }

        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_chunk",
            "ok",
            duration_to_us(start.elapsed()),
            "oxz chunk read successfully",
        );

        Ok(resolved)
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

    fn read_manifest(reader: &mut R, header: GlobalHeader) -> Result<ArchiveManifest> {
        let len = usize::try_from(header.entry_table_len)
            .map_err(|_| OxideError::InvalidFormat("entry table length exceeds usize range"))?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(header.entry_table_offset))?;
        reader.read_exact(&mut bytes)?;
        ArchiveManifest::decode(&bytes)
    }

    fn read_chunk_table(reader: &mut R, header: GlobalHeader) -> Result<Vec<ChunkDescriptor>> {
        let len = usize::try_from(header.chunk_table_len)
            .map_err(|_| OxideError::InvalidFormat("chunk table length exceeds usize range"))?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(header.chunk_table_offset))?;
        reader.read_exact(&mut bytes)?;
        decode_chunk_table(&bytes, header.payload_offset, header.block_count)
    }

    fn validate_chunk_layout(
        descriptors: &[ChunkDescriptor],
        header: GlobalHeader,
        dictionary_bank: &crate::ArchiveDictionaryBank,
    ) -> Result<()> {
        let expected_payload_end = descriptors
            .last()
            .map(ChunkDescriptor::payload_end)
            .transpose()?
            .unwrap_or(header.payload_offset);

        if expected_payload_end != header.entry_table_offset {
            return Err(OxideError::InvalidFormat(
                "manifest offset does not match chunk payload layout",
            ));
        }

        for (index, descriptor) in descriptors.iter().enumerate() {
            if let Some(reference_target) = descriptor.reference_target {
                let reference_target = usize::try_from(reference_target).map_err(|_| {
                    OxideError::InvalidFormat("reference chunk target exceeds usize range")
                })?;
                if reference_target >= index {
                    return Err(OxideError::InvalidFormat(
                        "reference chunk descriptors must target earlier blocks",
                    ));
                }
                let target = descriptors[reference_target];
                if target.is_reference() {
                    return Err(OxideError::InvalidFormat(
                        "reference chunk descriptors must target canonical data blocks",
                    ));
                }
                if descriptor.raw_len != target.raw_len {
                    return Err(OxideError::InvalidFormat(
                        "reference chunk descriptors must preserve raw length",
                    ));
                }
                continue;
            }

            let compression_meta = descriptor.compression_meta()?;
            if compression_meta.dictionary_id != 0
                && dictionary_bank
                    .dictionary(compression_meta.dictionary_id, compression_meta.algo)
                    .is_none()
            {
                return Err(OxideError::InvalidFormat(
                    "chunk descriptor references unknown archive dictionary",
                ));
            }
        }

        Ok(())
    }

    fn resolve_data_descriptor(
        &self,
        index: u32,
        descriptor: ChunkDescriptor,
    ) -> Result<ChunkDescriptor> {
        let Some(reference_target) = descriptor.reference_target else {
            return Ok(descriptor);
        };

        let reference_target = usize::try_from(reference_target)
            .map_err(|_| OxideError::InvalidFormat("reference chunk target exceeds usize range"))?;
        let index = usize::try_from(index)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;
        if reference_target >= index {
            return Err(OxideError::InvalidFormat(
                "reference chunk descriptors must target earlier blocks",
            ));
        }

        let target = self
            .chunk_descriptors
            .get(reference_target)
            .copied()
            .ok_or(OxideError::InvalidFormat(
                "reference chunk target out of range",
            ))?;
        if target.is_reference() {
            return Err(OxideError::InvalidFormat(
                "reference chunk descriptors must target canonical data blocks",
            ));
        }
        if descriptor.raw_len != target.raw_len {
            return Err(OxideError::InvalidFormat(
                "reference chunk descriptors must preserve raw length",
            ));
        }
        Ok(target)
    }
}

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

#[cfg(test)]
#[path = "../../../tests/format/oxz/reader.rs"]
mod tests;
