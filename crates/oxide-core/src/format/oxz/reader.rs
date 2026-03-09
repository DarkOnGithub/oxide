use std::cmp::min;
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{OxideError, Result};

use super::{
    ArchiveManifest, CHUNK_DESCRIPTOR_SIZE, ChunkDescriptor, FOOTER_SIZE, Footer,
    GLOBAL_HEADER_SIZE, GlobalHeader, SectionTableEntry, SectionType, StoredDictionary,
    decode_dictionary_store,
};

/// Reads OXZ archives and provides access to individual chunk payloads.
///
/// Supports both sequential iteration and random access to chunks.
#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    section_table: Vec<SectionTableEntry>,
    dictionaries: Vec<StoredDictionary>,
    manifest: Option<ArchiveManifest>,
    chunk_descriptors: Vec<ChunkDescriptor>,
    chunk_offsets: Vec<u64>,
    footer: Footer,
}

impl<R: Read + Seek> ArchiveReader<R> {
    /// Creates a new archive reader from a readable and seekable source.
    ///
    /// Validates the global header, section table, chunk index, payload layout,
    /// and declared archive length. Checksum fields are currently ignored.
    pub fn new(mut reader: R) -> Result<Self> {
        let section_table_started = Instant::now();
        let global_header = GlobalHeader::read(&mut reader)?;
        let section_table = Self::read_section_table(&mut reader, global_header)?;
        let section_table_elapsed_us = duration_to_us(section_table_started.elapsed());

        let chunk_index_entry = Self::require_section(&section_table, SectionType::ChunkIndex)?;
        let dictionary_store_entry =
            Self::require_section(&section_table, SectionType::DictionaryStore)?;
        let payload_entry = Self::require_section(&section_table, SectionType::PayloadRegion)?;

        let dictionary_store_started = Instant::now();
        let dictionaries = Self::read_dictionary_store(&mut reader, *dictionary_store_entry)?;
        let dictionary_store_elapsed_us = duration_to_us(dictionary_store_started.elapsed());
        let manifest = Self::find_section(&section_table, SectionType::ArchiveManifest)
            .map(|entry| Self::read_manifest(&mut reader, *entry))
            .transpose()?;

        let chunk_index_started = Instant::now();
        let (chunk_descriptors, chunk_offsets) =
            Self::read_chunk_index(&mut reader, *chunk_index_entry, *payload_entry)?;
        let chunk_index_elapsed_us = duration_to_us(chunk_index_started.elapsed());

        let footer_offset = Self::section_data_end(&section_table)?;
        let file_len = reader.seek(SeekFrom::End(0))?;
        let expected_file_len = footer_offset
            .checked_add(FOOTER_SIZE as u64)
            .ok_or(OxideError::InvalidFormat("archive length overflow"))?;
        if file_len != expected_file_len {
            return Err(OxideError::InvalidFormat(
                "archive length does not match declared section ranges",
            ));
        }

        reader.seek(SeekFrom::Start(footer_offset))?;
        let footer = Footer::read(&mut reader)?;

        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_section_table",
            "ok",
            section_table_elapsed_us,
            "oxz section table read successfully",
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_chunk_index",
            "ok",
            chunk_index_elapsed_us,
            "oxz chunk index read successfully",
        );
        profile::event(
            tags::PROFILE_OXZ,
            &[tags::TAG_OXZ],
            "read_dictionary_store",
            "ok",
            dictionary_store_elapsed_us,
            "oxz dictionary store read successfully",
        );

        reader.seek(SeekFrom::Start(payload_entry.offset))?;

        Ok(Self {
            reader,
            global_header,
            section_table,
            dictionaries,
            manifest,
            chunk_descriptors,
            chunk_offsets,
            footer,
        })
    }

    pub(crate) fn new_for_sequential_extract(reader: R) -> Result<Self> {
        Self::new(reader)
    }

    /// Returns the number of chunks in the archive.
    pub fn block_count(&self) -> u32 {
        self.chunk_descriptors.len() as u32
    }

    /// Returns the global header of the archive.
    pub fn global_header(&self) -> GlobalHeader {
        self.global_header
    }

    /// Returns the parsed section table.
    pub fn section_table(&self) -> &[SectionTableEntry] {
        &self.section_table
    }

    /// Returns the footer of the archive.
    pub fn footer(&self) -> Footer {
        self.footer
    }

    /// Returns the dictionary payload referenced by `dict_id`, if present.
    pub fn dictionary(&self, dict_id: u16) -> Option<&[u8]> {
        if dict_id == 0 {
            return None;
        }

        self.dictionaries
            .iter()
            .find(|dictionary| dictionary.id == dict_id)
            .map(|dictionary| dictionary.data.as_slice())
    }

    pub fn manifest(&self) -> Option<&ArchiveManifest> {
        self.manifest.as_ref()
    }

    /// Reads a specific chunk by its index.
    ///
    /// Checksum fields are currently ignored.
    pub fn read_block(&mut self, index: u32) -> Result<(ChunkDescriptor, Vec<u8>)> {
        let start = Instant::now();
        let index_val = usize::try_from(index)
            .map_err(|_| OxideError::InvalidFormat("block index exceeds usize range"))?;

        let descriptor = *self
            .chunk_descriptors
            .get(index_val)
            .ok_or(OxideError::InvalidFormat("block index out of range"))?;
        let offset = *self
            .chunk_offsets
            .get(index_val)
            .ok_or(OxideError::InvalidFormat("block offset out of range"))?;

        self.reader.seek(SeekFrom::Start(offset))?;
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

    /// Returns an iterator over all chunks in the archive.
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

    pub(crate) fn finish_sequential_extract_validation(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_section_table(reader: &mut R, header: GlobalHeader) -> Result<Vec<SectionTableEntry>> {
        if header.section_table_offset < GLOBAL_HEADER_SIZE as u64 {
            return Err(OxideError::InvalidFormat(
                "section table offset is before end of header",
            ));
        }

        reader.seek(SeekFrom::Start(header.section_table_offset))?;
        let mut entries = Vec::with_capacity(header.section_count as usize);
        for _ in 0..header.section_count {
            entries.push(SectionTableEntry::read(reader)?);
        }
        Self::validate_section_table(&entries)?;
        Ok(entries)
    }

    fn validate_section_table(entries: &[SectionTableEntry]) -> Result<()> {
        if entries.is_empty() {
            return Err(OxideError::InvalidFormat("section table is empty"));
        }

        let mut seen = std::collections::BTreeSet::<SectionType>::new();
        for entry in entries {
            if !seen.insert(entry.section_type) {
                return Err(OxideError::InvalidFormat(
                    "duplicate section type in section table",
                ));
            }
        }

        Ok(())
    }

    fn require_section(
        entries: &[SectionTableEntry],
        section_type: SectionType,
    ) -> Result<&SectionTableEntry> {
        Self::find_section(entries, section_type)
            .ok_or(OxideError::InvalidFormat("missing required section"))
    }

    fn find_section(
        entries: &[SectionTableEntry],
        section_type: SectionType,
    ) -> Option<&SectionTableEntry> {
        entries
            .iter()
            .find(|entry| entry.section_type == section_type)
    }

    fn read_chunk_index(
        reader: &mut R,
        chunk_index_entry: SectionTableEntry,
        payload_entry: SectionTableEntry,
    ) -> Result<(Vec<ChunkDescriptor>, Vec<u64>)> {
        if chunk_index_entry.length % CHUNK_DESCRIPTOR_SIZE as u64 != 0 {
            return Err(OxideError::InvalidFormat(
                "chunk index section is not descriptor-aligned",
            ));
        }

        let chunk_count = usize::try_from(chunk_index_entry.length / CHUNK_DESCRIPTOR_SIZE as u64)
            .map_err(|_| OxideError::InvalidFormat("chunk count exceeds usize range"))?;
        let mut descriptors = Vec::with_capacity(chunk_count);

        reader.seek(SeekFrom::Start(chunk_index_entry.offset))?;
        let chunk_index_len = usize::try_from(chunk_index_entry.length)
            .map_err(|_| OxideError::InvalidFormat("chunk index length exceeds usize range"))?;
        let mut chunk_index_bytes = vec![0u8; chunk_index_len];
        reader.read_exact(&mut chunk_index_bytes)?;

        for raw in chunk_index_bytes.chunks_exact(CHUNK_DESCRIPTOR_SIZE) {
            let mut bytes = [0u8; CHUNK_DESCRIPTOR_SIZE];
            bytes.copy_from_slice(raw);
            descriptors.push(ChunkDescriptor::read(&mut std::io::Cursor::new(bytes))?);
        }

        let mut offsets = Vec::with_capacity(chunk_count);
        let mut running_offset = payload_entry.offset;
        for descriptor in &descriptors {
            offsets.push(running_offset);
            running_offset = running_offset
                .checked_add(descriptor.encoded_len as u64)
                .ok_or(OxideError::InvalidFormat("payload offsets overflow"))?;
        }

        let expected_payload_end = payload_entry
            .offset
            .checked_add(payload_entry.length)
            .ok_or(OxideError::InvalidFormat("payload range overflow"))?;
        if running_offset != expected_payload_end {
            return Err(OxideError::InvalidFormat(
                "payload length does not match chunk descriptor sum",
            ));
        }

        Ok((descriptors, offsets))
    }

    fn read_dictionary_store(
        reader: &mut R,
        dictionary_store_entry: SectionTableEntry,
    ) -> Result<Vec<StoredDictionary>> {
        if dictionary_store_entry.length == 0 {
            return Ok(Vec::new());
        }

        let len = usize::try_from(dictionary_store_entry.length).map_err(|_| {
            OxideError::InvalidFormat("dictionary store length exceeds usize range")
        })?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(dictionary_store_entry.offset))?;
        reader.read_exact(&mut bytes)?;
        decode_dictionary_store(&bytes)
    }

    fn read_manifest(reader: &mut R, manifest_entry: SectionTableEntry) -> Result<ArchiveManifest> {
        let len = usize::try_from(manifest_entry.length)
            .map_err(|_| OxideError::InvalidFormat("manifest length exceeds usize range"))?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(manifest_entry.offset))?;
        reader.read_exact(&mut bytes)?;
        ArchiveManifest::decode(&bytes)
    }

    fn section_data_end(entries: &[SectionTableEntry]) -> Result<u64> {
        entries
            .iter()
            .map(|entry| {
                entry
                    .offset
                    .checked_add(entry.length)
                    .ok_or(OxideError::InvalidFormat("section range overflow"))
            })
            .try_fold(0u64, |max_end, end| end.map(|value| max_end.max(value)))
    }

    #[allow(dead_code)]
    fn compute_crc32_up_to(reader: &mut R, len: u64) -> Result<u32> {
        reader.seek(SeekFrom::Start(0))?;
        let mut remaining = len;
        let mut buffer = [0u8; 8 * 1024];

        while remaining > 0 {
            let to_read = min(remaining as usize, buffer.len());
            reader.read_exact(&mut buffer[..to_read])?;
            remaining -= to_read as u64;
        }

        Ok(0)
    }

    #[allow(dead_code)]
    fn compute_crc32_range(reader: &mut R, offset: u64, len: u64) -> Result<u32> {
        reader.seek(SeekFrom::Start(offset))?;
        let mut remaining = len;
        let mut buffer = [0u8; 8 * 1024];

        while remaining > 0 {
            let to_read = min(remaining as usize, buffer.len());
            reader.read_exact(&mut buffer[..to_read])?;
            remaining -= to_read as u64;
        }

        Ok(0)
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
