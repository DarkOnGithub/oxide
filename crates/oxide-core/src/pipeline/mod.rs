use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use crate::buffer::BufferPool;
use crate::core::WorkerPool;
use crate::format::{ArchiveReader, ArchiveWriter};
use crate::io::InputScanner;
use crate::types::{
    Batch, CompressedBlock, CompressionAlgo, FileFormat, PreProcessingStrategy, Result,
};

const DIRECTORY_BUNDLE_MAGIC: [u8; 4] = *b"OXDB";
const DIRECTORY_BUNDLE_VERSION: u16 = 1;

#[derive(Debug)]
enum DirectoryBundleEntry {
    Directory { rel_path: String },
    File { rel_path: String, data: Vec<u8> },
}

/// End-to-end Phase 1 pipeline that wires scanner, workers, and OXZ I/O.
///
/// This pipeline performs pass-through processing for block payloads while preserving
/// the metadata and ordering guarantees needed by the archive format.
pub struct ArchivePipeline {
    scanner: InputScanner,
    num_workers: usize,
    compression_algo: CompressionAlgo,
    buffer_pool: Arc<BufferPool>,
}

impl ArchivePipeline {
    /// Creates a new archive pipeline.
    pub fn new(
        target_block_size: usize,
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        Self {
            scanner: InputScanner::new(target_block_size),
            num_workers,
            compression_algo,
            buffer_pool,
        }
    }

    /// Reads an input file, processes blocks in parallel, and writes an OXZ archive.
    pub fn archive_file<P, W>(&self, path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let batches = self.scanner.scan_file(path.as_ref())?;
        self.archive_batches(batches, writer)
    }

    /// Archives either a single file or a directory tree.
    pub fn archive_path<P, W>(&self, path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let path = path.as_ref();
        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            self.archive_file(path, writer)
        } else if metadata.is_dir() {
            self.archive_directory(path, writer)
        } else {
            Err(crate::OxideError::InvalidFormat(
                "path must be a file or directory",
            ))
        }
    }

    /// Archives a directory tree as a single OXZ payload.
    pub fn archive_directory<P, W>(&self, dir_path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let root = dir_path.as_ref();
        if !root.is_dir() {
            return Err(crate::OxideError::InvalidFormat(
                "archive_directory expects a directory path",
            ));
        }

        let entries = self.collect_directory_entries(root)?;
        let bundle = Self::encode_directory_bundle(entries)?;

        let batches = self.bytes_to_batches(root.to_path_buf(), bundle, FileFormat::Binary);
        self.archive_batches(batches, writer)
    }

    /// Extracts all block payload bytes from an OXZ archive in block order.
    pub fn extract_archive<R: Read + std::io::Seek>(&self, reader: R) -> Result<Vec<u8>> {
        let mut archive = ArchiveReader::new(reader)?;
        let mut output = Vec::new();

        for entry in archive.iter_blocks() {
            let (_header, block_data) = entry?;
            output.extend_from_slice(&block_data);
        }

        Ok(output)
    }

    /// Extracts a directory tree archive produced by [`archive_directory`].
    pub fn extract_directory_archive<R, P>(&self, reader: R, output_dir: P) -> Result<()>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        let payload = self.extract_archive(reader)?;
        let entries = Self::decode_directory_bundle(&payload)?;

        let root = output_dir.as_ref();
        fs::create_dir_all(root)?;

        for entry in entries {
            match entry {
                DirectoryBundleEntry::Directory { rel_path } => {
                    let out_path = Self::join_safe(root, &rel_path)?;
                    fs::create_dir_all(out_path)?;
                }
                DirectoryBundleEntry::File { rel_path, data } => {
                    let out_path = Self::join_safe(root, &rel_path)?;
                    if let Some(parent) = out_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::write(out_path, data)?;
                }
            }
        }

        Ok(())
    }

    fn archive_batches<W: Write>(&self, batches: Vec<Batch>, writer: W) -> Result<W> {
        let block_count = u32::try_from(batches.len())
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );

        let handle = worker_pool.spawn(|_worker_id, batch, pool, compression| {
            let mut scratch = pool.acquire();
            scratch.extend_from_slice(batch.data());

            // Move the pooled Vec out without allocating.
            let mut data = Vec::new();
            std::mem::swap(scratch.as_mut_vec(), &mut data);

            Ok(CompressedBlock::new(
                batch.id,
                data,
                PreProcessingStrategy::None,
                compression,
                batch.len() as u64,
            ))
        });

        for batch in batches {
            handle.submit(batch)?;
        }
        let blocks = handle.finish()?;

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.buffer_pool));
        archive_writer.write_global_header(block_count)?;
        for block in blocks {
            archive_writer.write_owned_block(block)?;
        }
        archive_writer.write_footer()
    }

    fn bytes_to_batches(
        &self,
        source_path: PathBuf,
        bytes: Vec<u8>,
        file_type_hint: FileFormat,
    ) -> Vec<Batch> {
        let data = Bytes::from(bytes);
        let mut batches = Vec::new();
        let mut start = 0usize;
        let mut id = 0usize;
        let block_size = self.scanner.target_block_size();

        while start < data.len() {
            let end = start.saturating_add(block_size).min(data.len());
            let chunk = data.slice(start..end);
            batches.push(Batch::with_hint(
                id,
                source_path.clone(),
                chunk,
                file_type_hint,
            ));
            start = end;
            id += 1;
        }

        if batches.is_empty() {
            batches.push(Batch::with_hint(
                0,
                source_path,
                Bytes::new(),
                file_type_hint,
            ));
        }

        batches
    }

    fn collect_directory_entries(&self, root: &Path) -> Result<Vec<DirectoryBundleEntry>> {
        let mut directories = Vec::<PathBuf>::new();
        let mut files = Vec::<PathBuf>::new();

        Self::walk_directory(root, root, &mut directories, &mut files)?;
        directories.sort();
        files.sort();

        let mut entries = Vec::with_capacity(directories.len() + files.len());
        for directory in directories {
            let rel_path = Self::relative_path_to_utf8(&directory)?;
            entries.push(DirectoryBundleEntry::Directory { rel_path });
        }

        for file_rel in files {
            let rel_path = Self::relative_path_to_utf8(&file_rel)?;
            let full_path = root.join(&file_rel);
            let data = fs::read(full_path)?;
            entries.push(DirectoryBundleEntry::File { rel_path, data });
        }

        Ok(entries)
    }

    fn walk_directory(
        root: &Path,
        current: &Path,
        directories: &mut Vec<PathBuf>,
        files: &mut Vec<PathBuf>,
    ) -> Result<()> {
        let mut children = Vec::new();
        for child in fs::read_dir(current)? {
            children.push(child?.path());
        }
        children.sort();

        for child_path in children {
            let metadata = fs::symlink_metadata(&child_path)?;
            let rel_path = child_path
                .strip_prefix(root)
                .map_err(|_| crate::OxideError::InvalidFormat("invalid relative path"))?
                .to_path_buf();

            if metadata.is_dir() {
                directories.push(rel_path.clone());
                Self::walk_directory(root, &child_path, directories, files)?;
            } else if metadata.is_file() {
                files.push(rel_path);
            } else {
                return Err(crate::OxideError::InvalidFormat(
                    "directory archive supports regular files/directories only",
                ));
            }
        }

        Ok(())
    }

    fn relative_path_to_utf8(path: &Path) -> Result<String> {
        let raw = path.to_str().ok_or(crate::OxideError::InvalidFormat(
            "non-utf8 path not supported",
        ))?;
        Ok(raw.replace('\\', "/"))
    }

    fn encode_directory_bundle(entries: Vec<DirectoryBundleEntry>) -> Result<Vec<u8>> {
        let entry_count = u32::try_from(entries.len())
            .map_err(|_| crate::OxideError::InvalidFormat("directory entry count overflow"))?;

        let mut out = Vec::new();
        out.extend_from_slice(&DIRECTORY_BUNDLE_MAGIC);
        out.extend_from_slice(&DIRECTORY_BUNDLE_VERSION.to_le_bytes());
        out.extend_from_slice(&entry_count.to_le_bytes());

        for entry in entries {
            match entry {
                DirectoryBundleEntry::Directory { rel_path } => {
                    out.push(0);
                    Self::encode_path(&mut out, &rel_path)?;
                }
                DirectoryBundleEntry::File { rel_path, data } => {
                    out.push(1);
                    Self::encode_path(&mut out, &rel_path)?;
                    out.extend_from_slice(
                        &(u64::try_from(data.len())
                            .map_err(|_| crate::OxideError::InvalidFormat("file size overflow"))?)
                        .to_le_bytes(),
                    );
                    out.extend_from_slice(&data);
                }
            }
        }

        Ok(out)
    }

    fn encode_path(out: &mut Vec<u8>, rel_path: &str) -> Result<()> {
        let bytes = rel_path.as_bytes();
        let len = u32::try_from(bytes.len())
            .map_err(|_| crate::OxideError::InvalidFormat("path length overflow"))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(bytes);
        Ok(())
    }

    fn decode_directory_bundle(payload: &[u8]) -> Result<Vec<DirectoryBundleEntry>> {
        if payload.len() < 10 {
            return Err(crate::OxideError::InvalidFormat(
                "directory bundle is too short",
            ));
        }

        if payload[..4] != DIRECTORY_BUNDLE_MAGIC {
            return Err(crate::OxideError::InvalidFormat(
                "archive payload is not a directory bundle",
            ));
        }

        let version = u16::from_le_bytes([payload[4], payload[5]]);
        if version != DIRECTORY_BUNDLE_VERSION {
            return Err(crate::OxideError::InvalidFormat(
                "unsupported directory bundle version",
            ));
        }

        let entry_count = u32::from_le_bytes([payload[6], payload[7], payload[8], payload[9]]);
        let mut cursor = 10usize;
        let mut entries = Vec::with_capacity(entry_count as usize);

        for _ in 0..entry_count {
            if cursor >= payload.len() {
                return Err(crate::OxideError::InvalidFormat(
                    "truncated directory bundle entry kind",
                ));
            }
            let kind = payload[cursor];
            cursor += 1;

            let rel_path = Self::decode_path(payload, &mut cursor)?;
            match kind {
                0 => entries.push(DirectoryBundleEntry::Directory { rel_path }),
                1 => {
                    if cursor + 8 > payload.len() {
                        return Err(crate::OxideError::InvalidFormat(
                            "truncated directory bundle file size",
                        ));
                    }
                    let size = u64::from_le_bytes([
                        payload[cursor],
                        payload[cursor + 1],
                        payload[cursor + 2],
                        payload[cursor + 3],
                        payload[cursor + 4],
                        payload[cursor + 5],
                        payload[cursor + 6],
                        payload[cursor + 7],
                    ]);
                    cursor += 8;

                    let size_usize = usize::try_from(size)
                        .map_err(|_| crate::OxideError::InvalidFormat("file size overflow"))?;
                    let end =
                        cursor
                            .checked_add(size_usize)
                            .ok_or(crate::OxideError::InvalidFormat(
                                "directory bundle offset overflow",
                            ))?;
                    if end > payload.len() {
                        return Err(crate::OxideError::InvalidFormat(
                            "truncated directory bundle file data",
                        ));
                    }

                    let data = payload[cursor..end].to_vec();
                    cursor = end;
                    entries.push(DirectoryBundleEntry::File { rel_path, data });
                }
                _ => {
                    return Err(crate::OxideError::InvalidFormat(
                        "invalid directory bundle entry kind",
                    ));
                }
            }
        }

        if cursor != payload.len() {
            return Err(crate::OxideError::InvalidFormat(
                "directory bundle has trailing data",
            ));
        }

        Ok(entries)
    }

    fn decode_path(payload: &[u8], cursor: &mut usize) -> Result<String> {
        if *cursor + 4 > payload.len() {
            return Err(crate::OxideError::InvalidFormat(
                "truncated directory bundle path length",
            ));
        }
        let len = u32::from_le_bytes([
            payload[*cursor],
            payload[*cursor + 1],
            payload[*cursor + 2],
            payload[*cursor + 3],
        ]) as usize;
        *cursor += 4;

        let end = cursor
            .checked_add(len)
            .ok_or(crate::OxideError::InvalidFormat(
                "directory path offset overflow",
            ))?;
        if end > payload.len() {
            return Err(crate::OxideError::InvalidFormat(
                "truncated directory bundle path data",
            ));
        }

        let rel_path = std::str::from_utf8(&payload[*cursor..end])
            .map_err(|_| crate::OxideError::InvalidFormat("directory path is not utf8"))?
            .to_string();
        *cursor = end;
        Ok(rel_path)
    }

    fn join_safe(root: &Path, rel_path: &str) -> Result<PathBuf> {
        let rel = Path::new(rel_path);
        if rel.is_absolute() {
            return Err(crate::OxideError::InvalidFormat(
                "absolute paths are not allowed in directory bundle",
            ));
        }

        for component in rel.components() {
            match component {
                Component::Normal(_) => {}
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(crate::OxideError::InvalidFormat(
                        "unsafe path component in directory bundle",
                    ));
                }
            }
        }

        Ok(root.join(rel))
    }
}
