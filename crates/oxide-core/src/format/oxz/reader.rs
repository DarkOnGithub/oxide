use std::cmp::min;
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use crc32fast::Hasher;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{OxideError, Result};

use super::{
    CHUNK_DESCRIPTOR_SIZE, ChunkDescriptor, FOOTER_SIZE, Footer, GLOBAL_HEADER_SIZE, GlobalHeader,
    SectionTableEntry, SectionType,
};

/// Reads OXZ archives and provides access to individual chunk payloads.
///
/// Supports both sequential iteration and random access to chunks.
#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    section_table: Vec<SectionTableEntry>,
    chunk_descriptors: Vec<ChunkDescriptor>,
    chunk_offsets: Vec<u64>,
    footer: Footer,
}

impl<R: Read + Seek> ArchiveReader<R> {
    /// Creates a new archive reader from a readable and seekable source.
    ///
    /// Validates the global header, section table, chunk index, payload layout,
    /// and global footer CRC32.
    pub fn new(mut reader: R) -> Result<Self> {
        let section_table_started = Instant::now();
        let global_header = GlobalHeader::read(&mut reader)?;
        let section_table = Self::read_section_table(&mut reader, global_header)?;
        let section_table_elapsed_us = duration_to_us(section_table_started.elapsed());

        let chunk_index_entry = Self::require_section(&section_table, SectionType::ChunkIndex)?;
        let payload_entry = Self::require_section(&section_table, SectionType::PayloadRegion)?;

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

        let computed_crc = Self::compute_crc32_up_to(&mut reader, footer_offset)?;
        if computed_crc != footer.global_crc32 {
            return Err(OxideError::ChecksumMismatch {
                expected: footer.global_crc32,
                actual: computed_crc,
            });
        }

        if payload_entry.length > 0 {
            let payload_crc =
                Self::compute_crc32_range(&mut reader, payload_entry.offset, payload_entry.length)?;
            if payload_crc != payload_entry.checksum {
                return Err(OxideError::ChecksumMismatch {
                    expected: payload_entry.checksum,
                    actual: payload_crc,
                });
            }
        }

        telemetry::increment_counter(
            tags::METRIC_OXZ_READ_SECTION_TABLE_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "read_section_table")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_READ_SECTION_TABLE_LATENCY_US,
            section_table_elapsed_us,
            &[("subsystem", "oxz"), ("op", "read_section_table")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_READ_CHUNK_INDEX_LATENCY_US,
            chunk_index_elapsed_us,
            &[("subsystem", "oxz"), ("op", "read_chunk_index")],
        );
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

        reader.seek(SeekFrom::Start(payload_entry.offset))?;

        Ok(Self {
            reader,
            global_header,
            section_table,
            chunk_descriptors,
            chunk_offsets,
            footer,
        })
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

    /// Reads a specific chunk by its index.
    ///
    /// Validates the chunk checksum before returning.
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

        let actual_crc = crc32fast::hash(&data);
        if actual_crc != descriptor.checksum {
            return Err(OxideError::ChecksumMismatch {
                expected: descriptor.checksum,
                actual: actual_crc,
            });
        }

        let elapsed_us = duration_to_us(start.elapsed());
        telemetry::increment_counter(
            tags::METRIC_OXZ_READ_BLOCK_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "read_chunk")],
        );
        telemetry::increment_counter(
            tags::METRIC_OXZ_READ_CHUNK_DESCRIPTOR_COUNT,
            1,
            &[("subsystem", "oxz"), ("op", "read_chunk_descriptor")],
        );
        telemetry::record_histogram(
            tags::METRIC_OXZ_READ_BLOCK_LATENCY_US,
            elapsed_us,
            &[("subsystem", "oxz"), ("op", "read_chunk")],
        );
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
        entries
            .iter()
            .find(|entry| entry.section_type == section_type)
            .ok_or(OxideError::InvalidFormat("missing required section"))
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
        let actual_chunk_index_crc = crc32fast::hash(&chunk_index_bytes);
        if actual_chunk_index_crc != chunk_index_entry.checksum {
            return Err(OxideError::ChecksumMismatch {
                expected: chunk_index_entry.checksum,
                actual: actual_chunk_index_crc,
            });
        }

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

    fn compute_crc32_range(reader: &mut R, offset: u64, len: u64) -> Result<u32> {
        reader.seek(SeekFrom::Start(offset))?;
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
