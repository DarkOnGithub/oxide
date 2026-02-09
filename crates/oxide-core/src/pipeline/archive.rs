use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::buffer::BufferPool;
use crate::core::{PoolRuntimeSnapshot, WorkerPool, WorkerPoolHandle};
use crate::format::{
    ArchiveReader, ArchiveWriter, BLOCK_HEADER_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE,
};
use crate::io::InputScanner;
use crate::types::{
    Batch, CompressedBlock, CompressionAlgo, FileFormat, PreProcessingStrategy, Result,
};

use super::directory::{self, DirectoryBatchSubmitter, STREAM_READ_BUFFER_SIZE};
use super::types::{
    ArchiveOptions, ArchiveOutcome, ArchiveProgressSnapshot, ArchiveRunStats, ArchiveSourceKind,
    FnProgressSink, NoopProgress, ProgressSink, StatValue,
};

const SUBMISSION_DRAIN_BUDGET: usize = 128;

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
        let mut sink = NoopProgress;
        self.archive_file_with(path, writer, ArchiveOptions::default(), &mut sink)
            .map(|outcome| outcome.writer)
    }

    /// Archives a file and returns detailed run stats.
    pub fn archive_file_with<P, W, S>(
        &self,
        path: P,
        writer: W,
        options: ArchiveOptions,
        sink: &mut S,
    ) -> Result<ArchiveOutcome<W>>
    where
        P: AsRef<Path>,
        W: Write,
        S: ProgressSink + ?Sized,
    {
        let prepared = self.prepare_file(path.as_ref())?;
        self.archive_prepared_with(prepared, writer, &options, sink)
    }

    /// Archives either a single file or a directory tree.
    pub fn archive_path<P, W>(&self, path: P, writer: W) -> Result<W>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let mut sink = NoopProgress;
        self.archive_path_with(path, writer, ArchiveOptions::default(), &mut sink)
            .map(|outcome| outcome.writer)
    }

    /// Archives a file or directory and returns detailed run stats.
    pub fn archive_path_with<P, W, S>(
        &self,
        path: P,
        writer: W,
        options: ArchiveOptions,
        sink: &mut S,
    ) -> Result<ArchiveOutcome<W>>
    where
        P: AsRef<Path>,
        W: Write,
        S: ProgressSink + ?Sized,
    {
        let path = path.as_ref();
        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            let prepared = self.prepare_file(path)?;
            self.archive_prepared_with(prepared, writer, &options, sink)
        } else if metadata.is_dir() {
            self.archive_directory_streaming_with(path, writer, &options, sink)
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
        let mut sink = NoopProgress;
        self.archive_directory_with(dir_path, writer, ArchiveOptions::default(), &mut sink)
            .map(|outcome| outcome.writer)
    }

    /// Archives a directory tree and returns detailed run stats.
    pub fn archive_directory_with<P, W, S>(
        &self,
        dir_path: P,
        writer: W,
        options: ArchiveOptions,
        sink: &mut S,
    ) -> Result<ArchiveOutcome<W>>
    where
        P: AsRef<Path>,
        W: Write,
        S: ProgressSink + ?Sized,
    {
        let prepared = self.prepare_directory(dir_path.as_ref())?;
        self.archive_prepared_with(prepared, writer, &options, sink)
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
        let options = ArchiveOptions {
            progress_interval,
            emit_final_progress: true,
        };
        let mut sink = FnProgressSink {
            callback: on_progress,
        };
        self.archive_path_with(path, writer, options, &mut sink)
            .map(|outcome| (outcome.writer, outcome.stats))
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
        let entries = directory::decode_directory_entries(&payload, flags)?.ok_or(
            crate::OxideError::InvalidFormat("archive does not contain a directory bundle"),
        )?;
        directory::write_directory_entries(output_dir.as_ref(), entries)
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

        if let Some(entries) = directory::decode_directory_entries(&payload, flags)? {
            directory::write_directory_entries(output_path, entries)?;
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

    fn archive_directory_streaming_with<W: Write, S: ProgressSink + ?Sized>(
        &self,
        root: &Path,
        writer: W,
        options: &ArchiveOptions,
        sink: &mut S,
    ) -> Result<ArchiveOutcome<W>> {
        let discovery = directory::discover_directory_tree(root)?;
        let input_bytes_total = discovery.input_bytes_total;
        let estimated_blocks_total =
            directory::block_count_for_bytes(input_bytes_total, self.scanner.target_block_size())?;

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );
        let handle = worker_pool.spawn(|_worker_id, batch, pool, compression| {
            let mut scratch = pool.acquire();
            scratch.extend_from_slice(batch.data());

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

        let started_at = Instant::now();
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut completed_bytes = 0u64;
        let mut received_count = 0usize;
        let mut blocks = Vec::with_capacity(estimated_blocks_total as usize);
        let mut first_error: Option<crate::OxideError> = None;

        let mut submitter =
            DirectoryBatchSubmitter::new(discovery.root.clone(), self.scanner.target_block_size());

        let entry_count = discovery
            .directories
            .len()
            .checked_add(discovery.files.len())
            .ok_or(crate::OxideError::InvalidFormat(
                "directory entry count overflow",
            ))?;
        let entry_count = u32::try_from(entry_count)
            .map_err(|_| crate::OxideError::InvalidFormat("directory entry count overflow"))?;

        let mut bundle_header = Vec::with_capacity(10);
        bundle_header.extend_from_slice(&directory::DIRECTORY_BUNDLE_MAGIC);
        bundle_header.extend_from_slice(&directory::DIRECTORY_BUNDLE_VERSION.to_le_bytes());
        bundle_header.extend_from_slice(&entry_count.to_le_bytes());
        submitter.push_bytes(&bundle_header, |batch| handle.submit(batch))?;
        Self::drain_some_ready_results(
            &handle,
            &mut blocks,
            &mut first_error,
            &mut completed_bytes,
            &mut received_count,
            SUBMISSION_DRAIN_BUDGET,
        );
        Self::emit_archive_progress_if_due(
            &handle,
            ArchiveSourceKind::Directory,
            started_at,
            input_bytes_total,
            completed_bytes,
            estimated_blocks_total,
            emit_every,
            &mut last_emit_at,
            false,
            sink,
        );

        for rel_path in &discovery.directories {
            let mut encoded = Vec::with_capacity(1 + 4 + rel_path.len());
            encoded.push(0);
            directory::encode_path(&mut encoded, rel_path)?;
            submitter.push_bytes(&encoded, |batch| handle.submit(batch))?;
            Self::drain_some_ready_results(
                &handle,
                &mut blocks,
                &mut first_error,
                &mut completed_bytes,
                &mut received_count,
                SUBMISSION_DRAIN_BUDGET,
            );
            Self::emit_archive_progress_if_due(
                &handle,
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                estimated_blocks_total,
                emit_every,
                &mut last_emit_at,
                false,
                sink,
            );
        }

        let mut read_buffer = vec![0u8; STREAM_READ_BUFFER_SIZE];
        for file in &discovery.files {
            let mut encoded = Vec::with_capacity(1 + 4 + file.rel_path.len() + 8);
            encoded.push(1);
            directory::encode_path(&mut encoded, &file.rel_path)?;
            encoded.extend_from_slice(&file.size.to_le_bytes());
            submitter.push_bytes(&encoded, |batch| handle.submit(batch))?;

            let mut file_reader = fs::File::open(&file.full_path)?;
            loop {
                let read = file_reader.read(&mut read_buffer)?;
                if read == 0 {
                    break;
                }

                submitter.push_bytes(&read_buffer[..read], |batch| handle.submit(batch))?;
                Self::drain_some_ready_results(
                    &handle,
                    &mut blocks,
                    &mut first_error,
                    &mut completed_bytes,
                    &mut received_count,
                    SUBMISSION_DRAIN_BUDGET,
                );
                Self::emit_archive_progress_if_due(
                    &handle,
                    ArchiveSourceKind::Directory,
                    started_at,
                    input_bytes_total,
                    completed_bytes,
                    estimated_blocks_total,
                    emit_every,
                    &mut last_emit_at,
                    false,
                    sink,
                );
            }
        }

        submitter.finish(|batch| handle.submit(batch))?;
        Self::drain_some_ready_results(
            &handle,
            &mut blocks,
            &mut first_error,
            &mut completed_bytes,
            &mut received_count,
            usize::MAX,
        );

        let expected = handle.submitted_count();
        let expected_u32 = u32::try_from(expected)
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;
        if expected_u32 != estimated_blocks_total {
            return Err(crate::OxideError::InvalidFormat(
                "directory block count mismatch",
            ));
        }

        handle.shutdown();

        while received_count < expected {
            if let Some(result) = handle.recv_timeout(Duration::from_millis(100)) {
                Self::record_worker_result(
                    result,
                    &mut blocks,
                    &mut first_error,
                    &mut completed_bytes,
                    &mut received_count,
                );
            }
            Self::emit_archive_progress_if_due(
                &handle,
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                estimated_blocks_total,
                emit_every,
                &mut last_emit_at,
                options.emit_final_progress && received_count == expected,
                sink,
            );
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
        archive_writer.write_global_header_with_flags(
            expected_u32,
            directory::source_kind_flags(ArchiveSourceKind::Directory),
        )?;
        let mut output_bytes_total = (GLOBAL_HEADER_SIZE + FOOTER_SIZE) as u64;
        for block in blocks {
            output_bytes_total = output_bytes_total
                .saturating_add(BLOCK_HEADER_SIZE as u64)
                .saturating_add(block.data.len() as u64);
            archive_writer.write_owned_block(block)?;
        }
        let writer = archive_writer.write_footer()?;
        let extensions =
            Self::build_stats_extensions(input_bytes_total, output_bytes_total, &final_runtime);

        let stats = ArchiveRunStats {
            source_kind: ArchiveSourceKind::Directory,
            elapsed: started_at.elapsed(),
            input_bytes_total,
            output_bytes_total,
            blocks_total: expected_u32,
            blocks_completed: final_runtime.completed as u32,
            workers: final_runtime.workers,
            extensions,
        };

        Ok(ArchiveOutcome { writer, stats })
    }

    fn emit_archive_progress_if_due<S: ProgressSink + ?Sized>(
        handle: &WorkerPoolHandle,
        source_kind: ArchiveSourceKind,
        started_at: Instant,
        input_bytes_total: u64,
        input_bytes_completed: u64,
        blocks_total: u32,
        emit_every: Duration,
        last_emit_at: &mut Instant,
        force: bool,
        sink: &mut S,
    ) {
        if force || last_emit_at.elapsed() >= emit_every {
            let runtime = handle.runtime_snapshot();
            sink.on_progress(ArchiveProgressSnapshot {
                source_kind,
                elapsed: started_at.elapsed(),
                input_bytes_total,
                input_bytes_completed: input_bytes_completed.min(input_bytes_total),
                blocks_total,
                blocks_completed: runtime.completed as u32,
                blocks_pending: runtime.pending as u32,
                runtime,
            });
            *last_emit_at = Instant::now();
        }
    }

    fn drain_some_ready_results(
        handle: &WorkerPoolHandle,
        blocks: &mut Vec<CompressedBlock>,
        first_error: &mut Option<crate::OxideError>,
        completed_bytes: &mut u64,
        received_count: &mut usize,
        max_results: usize,
    ) {
        let mut drained = 0usize;
        while drained < max_results {
            if let Some(result) = handle.recv_timeout(Duration::from_millis(0)) {
                Self::record_worker_result(
                    result,
                    blocks,
                    first_error,
                    completed_bytes,
                    received_count,
                );
                drained += 1;
            } else {
                break;
            }
        }
    }

    fn record_worker_result(
        result: Result<CompressedBlock>,
        blocks: &mut Vec<CompressedBlock>,
        first_error: &mut Option<crate::OxideError>,
        completed_bytes: &mut u64,
        received_count: &mut usize,
    ) {
        match result {
            Ok(block) => {
                *completed_bytes = (*completed_bytes).saturating_add(block.original_len);
                blocks.push(block);
            }
            Err(error) => {
                if first_error.is_none() {
                    *first_error = Some(error);
                }
            }
        }
        *received_count += 1;
    }

    fn build_stats_extensions(
        input_bytes_total: u64,
        output_bytes_total: u64,
        runtime: &PoolRuntimeSnapshot,
    ) -> BTreeMap<String, StatValue> {
        let mut extensions = BTreeMap::new();
        extensions.insert(
            "runtime.submitted".to_string(),
            StatValue::U64(runtime.submitted as u64),
        );
        extensions.insert(
            "runtime.completed".to_string(),
            StatValue::U64(runtime.completed as u64),
        );
        extensions.insert(
            "runtime.pending".to_string(),
            StatValue::U64(runtime.pending as u64),
        );
        extensions.insert(
            "runtime.elapsed_us".to_string(),
            StatValue::U64(runtime.elapsed.as_micros().min(u64::MAX as u128) as u64),
        );
        extensions.insert(
            "runtime.worker_count".to_string(),
            StatValue::U64(runtime.workers.len() as u64),
        );
        if input_bytes_total > 0 {
            extensions.insert(
                "archive.output_input_ratio".to_string(),
                StatValue::F64(output_bytes_total as f64 / input_bytes_total as f64),
            );
        }
        extensions
    }

    fn archive_prepared_with<W: Write, S: ProgressSink + ?Sized>(
        &self,
        prepared: PreparedInput,
        writer: W,
        options: &ArchiveOptions,
        sink: &mut S,
    ) -> Result<ArchiveOutcome<W>> {
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
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
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

            if last_emit_at.elapsed() >= emit_every
                || (options.emit_final_progress && received_count == expected)
            {
                let runtime = handle.runtime_snapshot();
                sink.on_progress(ArchiveProgressSnapshot {
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
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(source_kind),
        )?;
        let mut output_bytes_total = (GLOBAL_HEADER_SIZE + FOOTER_SIZE) as u64;
        for block in blocks {
            output_bytes_total = output_bytes_total
                .saturating_add(BLOCK_HEADER_SIZE as u64)
                .saturating_add(block.data.len() as u64);
            archive_writer.write_owned_block(block)?;
        }
        let writer = archive_writer.write_footer()?;
        let extensions =
            Self::build_stats_extensions(input_bytes_total, output_bytes_total, &final_runtime);

        let stats = ArchiveRunStats {
            source_kind,
            elapsed: started_at.elapsed(),
            input_bytes_total,
            output_bytes_total,
            blocks_total: block_count,
            blocks_completed: final_runtime.completed as u32,
            workers: final_runtime.workers,
            extensions,
        };

        Ok(ArchiveOutcome { writer, stats })
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

        let entries = directory::collect_directory_entries(root)?;
        let bundle = directory::encode_directory_bundle(entries)?;
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
}
