use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rayon::prelude::*;

use crate::buffer::BufferPool;
use crate::core::{PoolRuntimeSnapshot, WorkerPool, WorkerRuntimeSnapshot};
use crate::format::{
    ArchiveReader, ArchiveWriter, BLOCK_HEADER_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE,
};
use crate::io::InputScanner;
use crate::types::{
    Batch, CompressedBlock, CompressionAlgo, FileFormat, PreProcessingStrategy, Result,
};

const DIRECTORY_BUNDLE_MAGIC: [u8; 4] = *b"OXDB";
const DIRECTORY_BUNDLE_VERSION: u16 = 1;
const SOURCE_KIND_DIRECTORY_FLAG: u32 = 1 << 0;
const PARALLEL_DIRECTORY_READ_THRESHOLD: usize = 1024;

#[derive(Debug)]
enum DirectoryBundleEntry {
    Directory { rel_path: String },
    File { rel_path: String, data: Vec<u8> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveSourceKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
pub struct ArchiveProgressSnapshot {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub input_bytes_total: u64,
    pub input_bytes_completed: u64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub blocks_pending: u32,
    pub runtime: PoolRuntimeSnapshot,
}

#[derive(Debug, Clone)]
pub struct ArchiveRunStats {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub input_bytes_total: u64,
    pub output_bytes_total: u64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub workers: Vec<WorkerRuntimeSnapshot>,
}

#[derive(Debug)]
struct PreparedInput {
    source_kind: ArchiveSourceKind,
    batches: Vec<Batch>,
    input_bytes_total: u64,
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
        let prepared = self.prepare_file(path.as_ref())?;
        self.archive_prepared(prepared, writer)
            .map(|(writer, _)| writer)
    }

    /// Archives either a single file or a directory tree.
    pub fn archive_path<P, W>(&self, path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let prepared = self.prepare_path(path.as_ref())?;
        self.archive_prepared(prepared, writer)
            .map(|(writer, _)| writer)
    }

    /// Archives a directory tree as a single OXZ payload.
    pub fn archive_directory<P, W>(&self, dir_path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let prepared = self.prepare_directory(dir_path.as_ref())?;
        self.archive_prepared(prepared, writer)
            .map(|(writer, _)| writer)
    }

    /// Archives a file or directory and emits progress snapshots while processing.
    pub fn archive_path_with_progress<P, W, F>(
        &self,
        path: P,
        writer: W,
        progress_interval: Duration,
        on_progress: F,
    ) -> Result<(W, ArchiveRunStats)>
    where
        P: AsRef<Path>,
        W: Write,
        F: FnMut(ArchiveProgressSnapshot),
    {
        let prepared = self.prepare_path(path.as_ref())?;
        self.archive_prepared_with_progress(prepared, writer, progress_interval, on_progress)
    }

    /// Extracts all block payload bytes from an OXZ archive in block order.
    pub fn extract_archive<R: Read + std::io::Seek>(&self, reader: R) -> Result<Vec<u8>> {
        let (_flags, payload) = Self::read_archive_payload(reader)?;
        Ok(payload)
    }

    /// Extracts a directory tree archive produced by [`archive_directory`].
    pub fn extract_directory_archive<R, P>(&self, reader: R, output_dir: P) -> Result<()>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        let (flags, payload) = Self::read_archive_payload(reader)?;
        let entries = Self::decode_directory_entries(&payload, flags)?.ok_or(
            crate::OxideError::InvalidFormat("archive does not contain a directory bundle"),
        )?;
        Self::write_directory_entries(output_dir.as_ref(), entries)
    }

    /// Extracts an archive to `output_path`.
    ///
    /// File payloads are written to `output_path` as a single file.
    /// Directory payloads are restored under `output_path` as a root directory.
    pub fn extract_path<R, P>(&self, reader: R, output_path: P) -> Result<ArchiveSourceKind>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        let (flags, payload) = Self::read_archive_payload(reader)?;
        let output_path = output_path.as_ref();

        if let Some(entries) = Self::decode_directory_entries(&payload, flags)? {
            Self::write_directory_entries(output_path, entries)?;
            return Ok(ArchiveSourceKind::Directory);
        }

        if let Some(parent) = output_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(output_path, payload)?;
        Ok(ArchiveSourceKind::File)
    }

    fn read_archive_payload<R: Read + std::io::Seek>(reader: R) -> Result<(u32, Vec<u8>)> {
        let mut archive = ArchiveReader::new(reader)?;
        let flags = archive.global_header().flags;
        let mut output = Vec::new();

        for entry in archive.iter_blocks() {
            let (_header, block_data) = entry?;
            output.extend_from_slice(&block_data);
        }

        Ok((flags, output))
    }

    fn decode_directory_entries(
        payload: &[u8],
        flags: u32,
    ) -> Result<Option<Vec<DirectoryBundleEntry>>> {
        if flags & SOURCE_KIND_DIRECTORY_FLAG != 0 {
            return Self::decode_directory_bundle(payload).map(Some);
        }

        match Self::decode_directory_bundle(payload) {
            Ok(entries) => Ok(Some(entries)),
            Err(_) => Ok(None),
        }
    }

    fn write_directory_entries(root: &Path, entries: Vec<DirectoryBundleEntry>) -> Result<()> {
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

    fn source_kind_flags(source_kind: ArchiveSourceKind) -> u32 {
        match source_kind {
            ArchiveSourceKind::File => 0,
            ArchiveSourceKind::Directory => SOURCE_KIND_DIRECTORY_FLAG,
        }
    }

    fn archive_prepared<W: Write>(
        &self,
        prepared: PreparedInput,
        writer: W,
    ) -> Result<(W, ArchiveRunStats)> {
        self.archive_prepared_with_progress(
            prepared,
            writer,
            Duration::from_secs(3600),
            |_snapshot| {},
        )
    }

    fn archive_prepared_with_progress<W: Write, F>(
        &self,
        prepared: PreparedInput,
        writer: W,
        progress_interval: Duration,
        mut on_progress: F,
    ) -> Result<(W, ArchiveRunStats)>
    where
        F: FnMut(ArchiveProgressSnapshot),
    {
        let PreparedInput {
            source_kind,
            batches,
            input_bytes_total,
        } = prepared;
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
        let expected = handle.submitted_count();
        handle.shutdown();

        let started_at = Instant::now();
        let mut last_emit_at = Instant::now();
        let emit_every = progress_interval.max(Duration::from_millis(100));
        let mut blocks = Vec::with_capacity(expected);
        let mut first_error: Option<crate::OxideError> = None;
        let mut completed_bytes = 0u64;
        let mut received_count = 0usize;

        while received_count < expected {
            if let Some(result) = handle.recv_timeout(Duration::from_millis(100)) {
                match result {
                    Ok(block) => {
                        completed_bytes = completed_bytes.saturating_add(block.original_len);
                        blocks.push(block);
                    }
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
                received_count += 1;
            }

            if last_emit_at.elapsed() >= emit_every || received_count == expected {
                let runtime = handle.runtime_snapshot();
                on_progress(ArchiveProgressSnapshot {
                    source_kind,
                    elapsed: started_at.elapsed(),
                    input_bytes_total,
                    input_bytes_completed: completed_bytes.min(input_bytes_total),
                    blocks_total: block_count,
                    blocks_completed: runtime.completed as u32,
                    blocks_pending: runtime.pending as u32,
                    runtime,
                });
                last_emit_at = Instant::now();
            }
        }

        let final_runtime = handle.runtime_snapshot();
        if let Err(join_error) = handle.join() {
            return Err(join_error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        blocks.sort_by_key(|block| block.id);

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.buffer_pool));
        archive_writer
            .write_global_header_with_flags(block_count, Self::source_kind_flags(source_kind))?;
        let mut output_bytes_total = (GLOBAL_HEADER_SIZE + FOOTER_SIZE) as u64;
        for block in blocks {
            output_bytes_total = output_bytes_total
                .saturating_add(BLOCK_HEADER_SIZE as u64)
                .saturating_add(block.data.len() as u64);
            archive_writer.write_owned_block(block)?;
        }
        let writer = archive_writer.write_footer()?;

        let stats = ArchiveRunStats {
            source_kind,
            elapsed: started_at.elapsed(),
            input_bytes_total,
            output_bytes_total,
            blocks_total: block_count,
            blocks_completed: final_runtime.completed as u32,
            workers: final_runtime.workers,
        };

        Ok((writer, stats))
    }

    fn prepare_path(&self, path: &Path) -> Result<PreparedInput> {
        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            self.prepare_file(path)
        } else if metadata.is_dir() {
            self.prepare_directory(path)
        } else {
            Err(crate::OxideError::InvalidFormat(
                "path must be a file or directory",
            ))
        }
    }

    fn prepare_file(&self, path: &Path) -> Result<PreparedInput> {
        let batches = self.scanner.scan_file(path)?;
        let input_bytes_total = batches.iter().map(|batch| batch.len() as u64).sum();
        Ok(PreparedInput {
            source_kind: ArchiveSourceKind::File,
            batches,
            input_bytes_total,
        })
    }

    fn prepare_directory(&self, root: &Path) -> Result<PreparedInput> {
        if !root.is_dir() {
            return Err(crate::OxideError::InvalidFormat(
                "archive_directory expects a directory path",
            ));
        }

        let entries = self.collect_directory_entries(root)?;
        let bundle = Self::encode_directory_bundle(entries)?;
        let input_bytes_total = bundle.len() as u64;
        let batches = self.bytes_to_batches(root.to_path_buf(), bundle, FileFormat::Binary);

        Ok(PreparedInput {
            source_kind: ArchiveSourceKind::Directory,
            batches,
            input_bytes_total,
        })
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

        if files.len() >= PARALLEL_DIRECTORY_READ_THRESHOLD {
            let parallel_entries: std::result::Result<
                Vec<DirectoryBundleEntry>,
                crate::OxideError,
            > = files
                .par_iter()
                .map(|file_rel| {
                    let rel_path = Self::relative_path_to_utf8(file_rel)?;
                    let full_path = root.join(file_rel);
                    let data = fs::read(full_path)?;
                    Ok(DirectoryBundleEntry::File { rel_path, data })
                })
                .collect();
            entries.extend(parallel_entries?);
        } else {
            for file_rel in files {
                let rel_path = Self::relative_path_to_utf8(&file_rel)?;
                let full_path = root.join(&file_rel);
                let data = fs::read(full_path)?;
                entries.push(DirectoryBundleEntry::File { rel_path, data });
            }
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
