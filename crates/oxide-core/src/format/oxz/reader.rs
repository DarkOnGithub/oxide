use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

use crate::compression::{DecompressionRequest, reverse_compression_request};
use crate::telemetry::{profile, tags};
use crate::types::duration_to_us;
use crate::{ArchiveSourceKind, CompressionAlgo, OxideError, Result};

use super::{
    ArchiveManifest, ArchiveMetadata, ChunkDescriptor, Footer, GlobalHeader, decode_chunk_table,
};

const MAX_REFERENCE_TARGET_CACHE_ENTRIES: usize = 128;
const MAX_REFERENCE_TARGET_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_DECOMPRESSED_MANIFEST_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug)]
pub struct ArchiveReader<R: Read + Seek> {
    reader: R,
    global_header: GlobalHeader,
    metadata: ArchiveMetadata,
    manifest: ArchiveManifest,
    chunk_descriptors: Vec<ChunkDescriptor>,
    footer: Footer,
    sequential_extract_state: Option<SequentialExtractState>,
    reference_target_cache: Option<ReferenceTargetCache>,
}

#[derive(Debug, Clone, Copy)]
struct SequentialExtractState {
    next_block_index: u32,
    next_payload_offset: u64,
}

#[derive(Debug)]
struct ReferenceTargetCache {
    remaining_reference_counts: Vec<u32>,
    entries: HashMap<u32, Vec<u8>>,
    insertion_order: VecDeque<u32>,
    cached_bytes_total: usize,
}

impl ReferenceTargetCache {
    fn new(descriptors: &[ChunkDescriptor]) -> Option<Self> {
        let mut remaining_reference_counts = vec![0u32; descriptors.len()];
        for descriptor in descriptors {
            let Some(reference_target) = descriptor.reference_target else {
                continue;
            };
            let Ok(reference_target) = usize::try_from(reference_target) else {
                return None;
            };
            let Some(count) = remaining_reference_counts.get_mut(reference_target) else {
                return None;
            };
            *count = count.saturating_add(1);
        }

        if remaining_reference_counts.iter().all(|count| *count == 0) {
            return None;
        }

        Some(Self {
            remaining_reference_counts,
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            cached_bytes_total: 0,
        })
    }

    fn has_pending_references(&self, index: u32) -> bool {
        let Ok(index) = usize::try_from(index) else {
            return false;
        };

        self.remaining_reference_counts
            .get(index)
            .copied()
            .unwrap_or_default()
            > 0
    }

    fn cached(&self, index: u32) -> Option<&[u8]> {
        self.entries.get(&index).map(Vec::as_slice)
    }

    fn store(&mut self, index: u32, bytes: &[u8]) {
        if !self.has_pending_references(index)
            || self.entries.contains_key(&index)
            || bytes.is_empty()
            || bytes.len() > MAX_REFERENCE_TARGET_CACHE_BYTES
        {
            return;
        }

        while (!self.entries.is_empty())
            && (self.entries.len() >= MAX_REFERENCE_TARGET_CACHE_ENTRIES
                || self.cached_bytes_total.saturating_add(bytes.len())
                    > MAX_REFERENCE_TARGET_CACHE_BYTES)
        {
            self.evict_oldest();
        }

        if self.entries.len() >= MAX_REFERENCE_TARGET_CACHE_ENTRIES
            || self.cached_bytes_total.saturating_add(bytes.len())
                > MAX_REFERENCE_TARGET_CACHE_BYTES
        {
            return;
        }

        self.cached_bytes_total = self.cached_bytes_total.saturating_add(bytes.len());
        self.entries.insert(index, bytes.to_vec());
        self.insertion_order.push_back(index);
    }

    fn consume_reference(&mut self, index: u32) {
        let Ok(index_usize) = usize::try_from(index) else {
            self.remove(index);
            return;
        };

        let remaining = self
            .remaining_reference_counts
            .get_mut(index_usize)
            .map(|count| {
                *count = count.saturating_sub(1);
                *count
            })
            .unwrap_or_default();

        if remaining == 0 {
            self.remove(index);
        }
    }

    fn remove(&mut self, index: u32) {
        if let Some(bytes) = self.entries.remove(&index) {
            self.cached_bytes_total = self.cached_bytes_total.saturating_sub(bytes.len());
        }
        self.insertion_order
            .retain(|cached_index| *cached_index != index);
    }

    fn evict_oldest(&mut self) {
        while let Some(index) = self.insertion_order.pop_front() {
            if let Some(bytes) = self.entries.remove(&index) {
                self.cached_bytes_total = self.cached_bytes_total.saturating_sub(bytes.len());
                break;
            }
        }
    }
}

impl<R: Read + Seek> ArchiveReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let header_started = Instant::now();
        let global_header = GlobalHeader::read(&mut reader)?;
        let header_elapsed_us = duration_to_us(header_started.elapsed());

        let metadata_started = Instant::now();
        let metadata = ArchiveMetadata::from_header(global_header);
        let metadata_elapsed_us = duration_to_us(metadata_started.elapsed());

        reader.seek(SeekFrom::Start(global_header.footer_offset))?;
        let footer = Footer::read(&mut reader)?;

        let manifest = Self::read_manifest(&mut reader, global_header, footer)?;

        let chunk_table_started = Instant::now();
        let chunk_descriptors = Self::read_chunk_table(&mut reader, global_header, footer)?;
        let chunk_table_elapsed_us = duration_to_us(chunk_table_started.elapsed());

        Self::validate_chunk_layout(
            &chunk_descriptors,
            global_header,
            manifest.dictionary_bank(),
        )?;

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
            reference_target_cache: None,
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
        archive.reference_target_cache = ReferenceTargetCache::new(&archive.chunk_descriptors);
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

    pub(crate) fn resolved_block_descriptor(&self, index: u32) -> Result<ChunkDescriptor> {
        let descriptor = self.block_descriptor(index)?;
        self.resolve_data_descriptor(index, descriptor)
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
        let sequentially_aligned = self.sequential_extract_state.map(|state| {
            state.next_block_index == index
                && state.next_payload_offset == descriptor.payload_offset
        }) == Some(true);
        let mut performed_io = true;

        if let Some(reference_target) = descriptor.reference_target
            && sequentially_aligned
            && let Some(cached) = self
                .reference_target_cache
                .as_ref()
                .and_then(|cache| cache.cached(reference_target))
        {
            buffer.clear();
            buffer.extend_from_slice(cached);
            performed_io = false;
        }

        if performed_io {
            if descriptor.is_reference() || !sequentially_aligned {
                self.reader.seek(SeekFrom::Start(resolved.payload_offset))?;
            }

            buffer.clear();
            buffer.resize(resolved.encoded_len as usize, 0);
            self.reader.read_exact(buffer.as_mut_slice())?;
        }

        if let Some(reference_target) = descriptor.reference_target {
            if let Some(cache) = self.reference_target_cache.as_mut() {
                cache.store(reference_target, buffer.as_slice());
                cache.consume_reference(reference_target);
            }
        } else if let Some(cache) = self.reference_target_cache.as_mut() {
            cache.store(index, buffer.as_slice());
        }

        if let Some(state) = self.sequential_extract_state.as_mut() {
            state.next_block_index = index.saturating_add(1);
            state.next_payload_offset = descriptor.payload_end()?;
        }

        if descriptor.is_reference() && self.sequential_extract_state.is_some() && performed_io {
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

    fn read_manifest(
        reader: &mut R,
        header: GlobalHeader,
        footer: Footer,
    ) -> Result<ArchiveManifest> {
        let len = usize::try_from(header.entry_table_len)
            .map_err(|_| OxideError::InvalidFormat("entry table length exceeds usize range"))?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(header.entry_table_offset))?;
        reader.read_exact(&mut bytes)?;
        if footer.manifest_compressed() {
            if let Ok(Some(content_size)) = zstd::zstd_safe::get_frame_content_size(&bytes)
                && content_size > MAX_DECOMPRESSED_MANIFEST_BYTES as u64
            {
                return Err(OxideError::InvalidFormat(
                    "archive manifest decompressed size exceeds safety limit",
                ));
            }
            bytes = reverse_compression_request(DecompressionRequest::new(
                bytes.as_slice(),
                CompressionAlgo::Zstd,
            ))?;
            if bytes.len() > MAX_DECOMPRESSED_MANIFEST_BYTES {
                return Err(OxideError::InvalidFormat(
                    "archive manifest decompressed size exceeds safety limit",
                ));
            }
        }
        ArchiveManifest::decode(&bytes)
    }

    fn read_chunk_table(
        reader: &mut R,
        header: GlobalHeader,
        footer: Footer,
    ) -> Result<Vec<ChunkDescriptor>> {
        let len = usize::try_from(header.chunk_table_len)
            .map_err(|_| OxideError::InvalidFormat("chunk table length exceeds usize range"))?;
        let mut bytes = vec![0u8; len];
        reader.seek(SeekFrom::Start(header.chunk_table_offset))?;
        reader.read_exact(&mut bytes)?;
        if footer.chunk_table_compressed() {
            let expected_len = usize::try_from(header.block_count)
                .map_err(|_| OxideError::InvalidFormat("chunk count exceeds usize range"))?
                .checked_mul(super::CHUNK_DESCRIPTOR_SIZE)
                .ok_or(OxideError::InvalidFormat("chunk table length overflow"))?;
            bytes = reverse_compression_request(
                DecompressionRequest::new(bytes.as_slice(), CompressionAlgo::Zstd)
                    .with_raw_len(expected_len),
            )?;
        }
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
mod tests {
    use std::cell::Cell;
    use std::io::Cursor;
    use std::rc::Rc;

    use crate::{ArchiveWriter, CompressedBlock, CompressionAlgo, CompressionMeta};

    use super::*;

    #[derive(Debug, Clone)]
    struct SeekCountingCursor {
        inner: Cursor<Vec<u8>>,
        seek_count: Rc<Cell<usize>>,
    }

    impl SeekCountingCursor {
        fn new(bytes: Vec<u8>) -> (Self, Rc<Cell<usize>>) {
            let seek_count = Rc::new(Cell::new(0));
            (
                Self {
                    inner: Cursor::new(bytes),
                    seek_count: Rc::clone(&seek_count),
                },
                seek_count,
            )
        }
    }

    impl Read for SeekCountingCursor {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read(buf)
        }
    }

    impl Seek for SeekCountingCursor {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            self.seek_count.set(self.seek_count.get().saturating_add(1));
            self.inner.seek(pos)
        }
    }

    fn build_test_archive() -> Vec<u8> {
        let mut writer = ArchiveWriter::with_manifest(Vec::new(), Some(ArchiveManifest::default()));
        writer
            .write_global_header_with_flags(2, 0)
            .expect("test archive header should write");

        let meta = CompressionMeta::new(CompressionAlgo::Lz4, true);
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                0,
                b"alpha".to_vec(),
                meta,
                5,
            ))
            .expect("first test block should write");
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                1,
                b"beta".to_vec(),
                meta,
                4,
            ))
            .expect("second test block should write");

        writer
            .write_footer()
            .expect("test archive footer should write")
    }

    fn build_reference_test_archive() -> Vec<u8> {
        let mut writer = ArchiveWriter::with_manifest(Vec::new(), Some(ArchiveManifest::default()));
        writer
            .write_global_header_with_flags(4, 0)
            .expect("test archive header should write");

        let meta = CompressionMeta::new(CompressionAlgo::Lz4, true);
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                0,
                b"alpha".to_vec(),
                meta,
                5,
            ))
            .expect("first test block should write");
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                1,
                b"beta".to_vec(),
                meta,
                4,
            ))
            .expect("second test block should write");
        writer
            .write_owned_block(CompressedBlock::reference(2, 0, 0, 5))
            .expect("reference test block should write");
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                3,
                b"gamma".to_vec(),
                meta,
                5,
            ))
            .expect("fourth test block should write");

        writer
            .write_footer()
            .expect("test archive footer should write")
    }

    fn build_multi_reference_test_archive() -> Vec<u8> {
        let mut writer = ArchiveWriter::with_manifest(Vec::new(), Some(ArchiveManifest::default()));
        writer
            .write_global_header_with_flags(5, 0)
            .expect("test archive header should write");

        let meta = CompressionMeta::new(CompressionAlgo::Lz4, true);
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                0,
                b"alpha".to_vec(),
                meta,
                5,
            ))
            .expect("first test block should write");
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                1,
                b"beta".to_vec(),
                meta,
                4,
            ))
            .expect("second test block should write");
        writer
            .write_owned_block(CompressedBlock::reference(2, 0, 0, 5))
            .expect("first reference test block should write");
        writer
            .write_owned_block(CompressedBlock::reference(3, 0, 0, 5))
            .expect("second reference test block should write");
        writer
            .write_owned_block(CompressedBlock::with_compression_meta(
                4,
                b"gamma".to_vec(),
                meta,
                5,
            ))
            .expect("fifth test block should write");

        writer
            .write_footer()
            .expect("test archive footer should write")
    }

    #[test]
    fn sequential_block_reads_reuse_buffer_without_extra_seeks() {
        let archive_bytes = build_test_archive();

        let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
        let mut archive =
            ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");
        let seek_count_after_open = seek_count.get();

        let mut first_buffer = Vec::new();
        let first: ChunkDescriptor = archive
            .read_block_into(0, &mut first_buffer)
            .expect("first block should read");
        assert_eq!(seek_count.get(), seek_count_after_open);
        assert_eq!(first.encoded_len, 5);
        assert_eq!(&first_buffer, b"alpha");

        let mut buffer = Vec::with_capacity(16);
        let second = archive
            .read_block_into(1, &mut buffer)
            .expect("second block should read");
        assert_eq!(seek_count.get(), seek_count_after_open);
        assert_eq!(&buffer, b"beta");
        assert_eq!(second.encoded_len, 4);
        assert!(buffer.capacity() >= 16);
    }

    #[test]
    fn sequential_reader_seeks_when_blocks_are_requested_out_of_order() {
        let archive_bytes = build_test_archive();

        let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
        let mut archive =
            ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");
        let seek_count_after_open = seek_count.get();

        let mut buffer = Vec::new();
        archive
            .read_block_into(1, &mut buffer)
            .expect("out-of-order block should read");

        assert!(seek_count.get() > seek_count_after_open);
        assert_eq!(&buffer, b"beta");
    }

    #[test]
    fn sequential_reference_blocks_keep_cursor_aligned() {
        let archive_bytes = build_reference_test_archive();

        let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
        let mut archive =
            ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");
        let seek_count_after_open = seek_count.get();

        let mut buffer = Vec::new();
        archive
            .read_block_into(0, &mut buffer)
            .expect("first block should read");
        assert_eq!(&buffer, b"alpha");
        assert_eq!(seek_count.get(), seek_count_after_open);

        archive
            .read_block_into(1, &mut buffer)
            .expect("second block should read");
        assert_eq!(&buffer, b"beta");
        assert_eq!(seek_count.get(), seek_count_after_open);

        archive
            .read_block_into(2, &mut buffer)
            .expect("reference block should read");
        assert_eq!(&buffer, b"alpha");
        assert_eq!(seek_count.get(), seek_count_after_open);

        archive
            .read_block_into(3, &mut buffer)
            .expect("block after reference should read");
        assert_eq!(&buffer, b"gamma");
        assert_eq!(seek_count.get(), seek_count_after_open);
    }

    #[test]
    fn sequential_reference_cache_reuses_canonical_blocks_for_repeated_targets() {
        let archive_bytes = build_multi_reference_test_archive();

        let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
        let mut archive =
            ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");
        let seek_count_after_open = seek_count.get();

        let mut buffer = Vec::new();

        archive
            .read_block_into(0, &mut buffer)
            .expect("first block should read");
        archive
            .read_block_into(1, &mut buffer)
            .expect("second block should read");
        assert_eq!(seek_count.get(), seek_count_after_open);

        archive
            .read_block_into(2, &mut buffer)
            .expect("first reference block should read");
        assert_eq!(&buffer, b"alpha");
        assert_eq!(seek_count.get(), seek_count_after_open);

        archive
            .read_block_into(3, &mut buffer)
            .expect("second reference block should read");
        assert_eq!(&buffer, b"alpha");
        assert_eq!(seek_count.get(), seek_count_after_open);

        archive
            .read_block_into(4, &mut buffer)
            .expect("fifth block should read");
        assert_eq!(&buffer, b"gamma");
        assert_eq!(seek_count.get(), seek_count_after_open);
    }

    #[test]
    fn reference_reads_cache_targets_even_when_canonical_block_is_skipped() {
        let archive_bytes = build_multi_reference_test_archive();

        let (reader, seek_count) = SeekCountingCursor::new(archive_bytes);
        let mut archive =
            ArchiveReader::new_for_sequential_extract(reader).expect("archive should open");

        let mut buffer = Vec::new();
        archive
            .read_block_into(2, &mut buffer)
            .expect("first reference block should read");
        assert_eq!(&buffer, b"alpha");
        let seek_count_after_first_reference = seek_count.get();

        archive
            .read_block_into(3, &mut buffer)
            .expect("second reference block should read");
        assert_eq!(&buffer, b"alpha");
        assert_eq!(seek_count.get(), seek_count_after_first_reference);
    }
}
