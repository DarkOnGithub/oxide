use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;

use crate::buffer::BufferPool;
use crate::core::{PoolRuntimeSnapshot, WorkerPool, WorkerPoolHandle};
use crate::format::{
    ArchiveReader, ArchiveWriter, BLOCK_HEADER_SIZE, FOOTER_SIZE, GLOBAL_HEADER_SIZE,
};
use crate::io::InputScanner;
use crate::types::{
    Batch, CompressedBlock, CompressionAlgo, CompressionMeta, CompressionPreset, FileFormat,
    PreProcessingStrategy, Result,
};

use super::directory::{self, DirectoryBatchSubmitter, STREAM_READ_BUFFER_SIZE};
use super::types::{
    ArchiveOptions, ArchiveOutcome, ArchiveProgressSnapshot, ArchiveRunStats, ArchiveSourceKind,
    FnProgressSink, NoopProgress, PipelinePerformanceOptions, ProgressSink, StatValue,
};

const SUBMISSION_DRAIN_BUDGET: usize = 128;
const DIRECTORY_FORMAT_PROBE_LIMIT: usize = 64 * 1024;
const MAX_INFLIGHT_BLOCKS_PER_WORKER: usize = 8;
const MIN_INFLIGHT_BLOCKS: usize = 64;
const MAX_INFLIGHT_BLOCKS: usize = 4096;
const AUTOTUNE_CANDIDATE_BLOCK_SIZES: [usize; 5] = [
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    4 * 1024 * 1024,
];

#[derive(Debug)]
struct PreparedInput {
    source_kind: ArchiveSourceKind,
    batches: Vec<Batch>,
    input_bytes_total: u64,
}

#[derive(Debug, Clone, Copy)]
struct BlockSizeDecision {
    selected_block_size: usize,
    autotune_requested: bool,
    autotune_ran: bool,
    sampled_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct BlockSizeScore {
    block_size: usize,
    throughput_bps: f64,
    compressed_bytes: usize,
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
    performance: PipelinePerformanceOptions,
}

impl ArchivePipeline {
    /// Creates a new archive pipeline.
    pub fn new(
        target_block_size: usize,
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        Self::with_performance(
            target_block_size,
            num_workers,
            buffer_pool,
            compression_algo,
            PipelinePerformanceOptions::default(),
        )
    }

    /// Creates a new archive pipeline with explicit performance options.
    pub fn with_performance(
        target_block_size: usize,
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
        performance: PipelinePerformanceOptions,
    ) -> Self {
        Self {
            scanner: InputScanner::new(target_block_size),
            num_workers,
            compression_algo,
            buffer_pool,
            performance,
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
        let path = path.as_ref();
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(crate::OxideError::InvalidFormat(
                "archive_file expects a file path",
            ));
        }
        let block_size = self.choose_block_size_for_file(path, metadata.len())?;
        let prepared = self.prepare_file(path, block_size.selected_block_size)?;
        self.archive_prepared_with(prepared, writer, &options, sink, block_size)
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
            let block_size = self.choose_block_size_for_file(path, metadata.len())?;
            let prepared = self.prepare_file(path, block_size.selected_block_size)?;
            self.archive_prepared_with(prepared, writer, &options, sink, block_size)
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
        self.archive_directory_streaming_with(dir_path.as_ref(), writer, &options, sink)
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
        let (_flags, payload) = Self::read_archive_payload(reader, self.num_workers)?;
        Ok(payload)
    }

    /// Extracts all block payload bytes from an OXZ archive in block order
    /// using the provided decode worker count.
    pub fn extract_archive_with_workers<R: Read + std::io::Seek>(
        &self,
        reader: R,
        workers: usize,
    ) -> Result<Vec<u8>> {
        let (_flags, payload) = Self::read_archive_payload(reader, workers)?;
        Ok(payload)
    }

    /// Extracts a directory tree archive produced by [`archive_directory`].
    pub fn extract_directory_archive<R, P>(&self, reader: R, output_dir: P) -> Result<()>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        self.extract_directory_archive_with_workers(reader, output_dir, self.num_workers)
    }

    /// Extracts a directory tree archive with explicit decode worker count.
    pub fn extract_directory_archive_with_workers<R, P>(
        &self,
        reader: R,
        output_dir: P,
        workers: usize,
    ) -> Result<()>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        let (flags, payload) = Self::read_archive_payload(reader, workers)?;
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
        self.extract_path_with_workers(reader, output_path, self.num_workers)
    }

    /// Extracts an archive with explicit decode worker count.
    pub fn extract_path_with_workers<R, P>(
        &self,
        reader: R,
        output_path: P,
        workers: usize,
    ) -> Result<ArchiveSourceKind>
    where
        R: Read + std::io::Seek,
        P: AsRef<Path>,
    {
        let (flags, payload) = Self::read_archive_payload(reader, workers)?;
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

    fn read_archive_payload<R: Read + std::io::Seek>(
        reader: R,
        workers: usize,
    ) -> Result<(u32, Vec<u8>)> {
        let mut archive = ArchiveReader::new(reader)?;
        let flags = archive.global_header().flags;
        let mut blocks = Vec::with_capacity(archive.block_count() as usize);
        let mut expected_total = 0usize;

        for entry in archive.iter_blocks() {
            let (header, block_data) = entry?;
            expected_total = expected_total.saturating_add(header.original_size as usize);
            blocks.push((header, block_data));
        }

        let worker_count = workers.max(1);
        let decode = |(header, block_data)| Self::decode_block_payload(header, block_data);
        let decoded_blocks: Vec<Result<Vec<u8>>> = if worker_count <= 1 || blocks.len() <= 1 {
            blocks.into_iter().map(decode).collect()
        } else {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(worker_count)
                .build()
                .map_err(|err| {
                    crate::OxideError::CompressionError(format!(
                        "failed to build decode thread pool: {err}"
                    ))
                })?;
            pool.install(|| blocks.into_par_iter().map(decode).collect())
        };

        let mut output = Vec::with_capacity(expected_total);
        for block in decoded_blocks {
            output.extend_from_slice(&block?);
        }

        Ok((flags, output))
    }

    fn decode_block_payload(
        header: crate::format::BlockHeader,
        block_data: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let compression_meta = header.compression_meta()?;
        let decoded = if compression_meta.raw_passthrough {
            block_data
        } else {
            crate::compression::reverse_compression(&block_data, compression_meta.algo)?
        };
        let strategy = header.strategy()?;
        let restored = crate::preprocessing::reverse_preprocessing(&decoded, &strategy)?;
        if restored.len() != header.original_size as usize {
            return Err(crate::OxideError::InvalidFormat(
                "decoded block size mismatch",
            ));
        }
        Ok(restored)
    }

    fn archive_directory_streaming_with<W: Write, S: ProgressSink + ?Sized>(
        &self,
        root: &Path,
        writer: W,
        options: &ArchiveOptions,
        sink: &mut S,
    ) -> Result<ArchiveOutcome<W>> {
        let discovery = directory::discover_directory_tree(root)?;
        let block_size = self.choose_block_size_for_directory(&discovery)?;
        let file_formats =
            directory::detect_file_formats(&discovery, DIRECTORY_FORMAT_PROBE_LIMIT)?;
        let block_count = directory::estimate_directory_block_count(
            &discovery,
            &file_formats,
            block_size.selected_block_size,
        )?;
        let input_bytes_total = discovery.input_bytes_total;
        let total_blocks = usize::try_from(block_count)
            .map_err(|_| crate::OxideError::InvalidFormat("block count exceeds usize range"))?;
        let max_inflight_blocks = Self::max_inflight_blocks(total_blocks, self.num_workers);

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );
        let compression_preset = self.performance.compression_preset;
        let raw_fallback_enabled = self.performance.raw_fallback_enabled;
        let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression| {
            Self::process_batch(
                batch,
                pool,
                compression,
                compression_preset,
                raw_fallback_enabled,
            )
        });

        let started_at = Instant::now();
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut completed_bytes = 0u64;
        let mut received_count = 0usize;
        let mut submitted_count = 0usize;
        let mut first_error: Option<crate::OxideError> = None;
        let mut pending_write = BTreeMap::<usize, CompressedBlock>::new();
        let mut next_write_id = 0usize;
        let mut output_bytes_written = GLOBAL_HEADER_SIZE as u64;
        let mut raw_passthrough_blocks = 0u64;

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.buffer_pool));
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(ArchiveSourceKind::Directory),
        )?;

        let mut submitter =
            DirectoryBatchSubmitter::new(discovery.root.clone(), block_size.selected_block_size);

        let mut submit_batch = |batch: Batch| -> Result<()> {
            while submitted_count.saturating_sub(received_count) >= max_inflight_blocks {
                Self::recv_result_to_writer(
                    &handle,
                    Duration::from_millis(100),
                    &mut archive_writer,
                    &mut pending_write,
                    &mut next_write_id,
                    &mut output_bytes_written,
                    &mut completed_bytes,
                    &mut first_error,
                    &mut raw_passthrough_blocks,
                    &mut received_count,
                );
                Self::emit_archive_progress_if_due(
                    &handle,
                    ArchiveSourceKind::Directory,
                    started_at,
                    input_bytes_total,
                    completed_bytes,
                    output_bytes_written,
                    block_count,
                    emit_every,
                    &mut last_emit_at,
                    false,
                    sink,
                );
            }

            handle.submit(batch)?;
            submitted_count += 1;
            Self::drain_results_to_writer(
                &handle,
                &mut archive_writer,
                &mut pending_write,
                &mut next_write_id,
                &mut output_bytes_written,
                &mut completed_bytes,
                &mut first_error,
                &mut raw_passthrough_blocks,
                &mut received_count,
                SUBMISSION_DRAIN_BUDGET,
            );
            Self::emit_archive_progress_if_due(
                &handle,
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                false,
                sink,
            );
            Ok(())
        };

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
        submitter.push_bytes_with_hint(&bundle_header, FileFormat::Common, |batch| {
            submit_batch(batch)
        })?;

        for rel_path in &discovery.directories {
            let mut encoded = Vec::with_capacity(1 + 4 + rel_path.len());
            encoded.push(0);
            directory::encode_path(&mut encoded, rel_path)?;
            submitter
                .push_bytes_with_hint(&encoded, FileFormat::Common, |batch| submit_batch(batch))?;
        }

        let mut read_buffer = vec![0u8; STREAM_READ_BUFFER_SIZE];
        for (file, file_format) in discovery.files.iter().zip(file_formats.iter().copied()) {
            let mut encoded = Vec::with_capacity(1 + 4 + file.rel_path.len() + 8);
            encoded.push(1);
            directory::encode_path(&mut encoded, &file.rel_path)?;
            encoded.extend_from_slice(&file.size.to_le_bytes());
            submitter
                .push_bytes_with_hint(&encoded, FileFormat::Common, |batch| submit_batch(batch))?;

            let mut file_reader = fs::File::open(&file.full_path)?;
            loop {
                let read = file_reader.read(&mut read_buffer)?;
                if read == 0 {
                    break;
                }

                submitter.push_bytes_with_hint(&read_buffer[..read], file_format, |batch| {
                    submit_batch(batch)
                })?;
            }
        }

        submitter.finish(|batch| submit_batch(batch))?;
        Self::drain_results_to_writer(
            &handle,
            &mut archive_writer,
            &mut pending_write,
            &mut next_write_id,
            &mut output_bytes_written,
            &mut completed_bytes,
            &mut first_error,
            &mut raw_passthrough_blocks,
            &mut received_count,
            usize::MAX,
        );

        if submitted_count != total_blocks {
            first_error.get_or_insert(crate::OxideError::InvalidFormat(
                "directory block count mismatch",
            ));
        }
        let expected = handle.submitted_count();
        if expected != submitted_count {
            first_error.get_or_insert(crate::OxideError::InvalidFormat(
                "submitted block count drift detected",
            ));
        }
        handle.shutdown();

        while received_count < submitted_count {
            Self::recv_result_to_writer(
                &handle,
                Duration::from_millis(100),
                &mut archive_writer,
                &mut pending_write,
                &mut next_write_id,
                &mut output_bytes_written,
                &mut completed_bytes,
                &mut first_error,
                &mut raw_passthrough_blocks,
                &mut received_count,
            );
            Self::emit_archive_progress_if_due(
                &handle,
                ArchiveSourceKind::Directory,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                options.emit_final_progress && received_count == submitted_count,
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
        if next_write_id != submitted_count || !pending_write.is_empty() {
            return Err(crate::OxideError::InvalidFormat(
                "directory writer has pending blocks after completion",
            ));
        }

        let writer = archive_writer.write_footer()?;
        output_bytes_written = output_bytes_written.saturating_add(FOOTER_SIZE as u64);
        let extensions = Self::build_stats_extensions(
            input_bytes_total,
            output_bytes_written,
            &final_runtime,
            block_size,
            raw_passthrough_blocks,
            self.performance.compression_preset,
        );

        let stats = ArchiveRunStats {
            source_kind: ArchiveSourceKind::Directory,
            elapsed: started_at.elapsed(),
            input_bytes_total,
            output_bytes_total: output_bytes_written,
            blocks_total: block_count,
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
        output_bytes_completed: u64,
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
                output_bytes_completed,
                blocks_total,
                blocks_completed: runtime.completed as u32,
                blocks_pending: runtime.pending as u32,
                runtime,
            });
            *last_emit_at = Instant::now();
        }
    }

    fn max_inflight_blocks(total_blocks: usize, num_workers: usize) -> usize {
        let scaled = num_workers.saturating_mul(MAX_INFLIGHT_BLOCKS_PER_WORKER);
        let bounded = scaled.clamp(MIN_INFLIGHT_BLOCKS, MAX_INFLIGHT_BLOCKS);
        bounded.min(total_blocks.max(1))
    }

    fn drain_results_to_writer<W: Write>(
        handle: &WorkerPoolHandle,
        archive_writer: &mut ArchiveWriter<W>,
        pending_write: &mut BTreeMap<usize, CompressedBlock>,
        next_write_id: &mut usize,
        output_bytes_written: &mut u64,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
        received_count: &mut usize,
        max_results: usize,
    ) {
        let mut drained = 0usize;
        while drained < max_results {
            if let Some(result) = handle.recv_timeout(Duration::from_millis(0)) {
                Self::record_result_to_writer(
                    result,
                    archive_writer,
                    pending_write,
                    next_write_id,
                    output_bytes_written,
                    completed_bytes,
                    first_error,
                    raw_passthrough_blocks,
                );
                *received_count += 1;
                drained += 1;
            } else {
                break;
            }
        }
    }

    fn recv_result_to_writer<W: Write>(
        handle: &WorkerPoolHandle,
        timeout: Duration,
        archive_writer: &mut ArchiveWriter<W>,
        pending_write: &mut BTreeMap<usize, CompressedBlock>,
        next_write_id: &mut usize,
        output_bytes_written: &mut u64,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
        received_count: &mut usize,
    ) {
        if let Some(result) = handle.recv_timeout(timeout) {
            Self::record_result_to_writer(
                result,
                archive_writer,
                pending_write,
                next_write_id,
                output_bytes_written,
                completed_bytes,
                first_error,
                raw_passthrough_blocks,
            );
            *received_count += 1;
        }
    }

    fn record_result_to_writer<W: Write>(
        result: Result<CompressedBlock>,
        archive_writer: &mut ArchiveWriter<W>,
        pending_write: &mut BTreeMap<usize, CompressedBlock>,
        next_write_id: &mut usize,
        output_bytes_written: &mut u64,
        completed_bytes: &mut u64,
        first_error: &mut Option<crate::OxideError>,
        raw_passthrough_blocks: &mut u64,
    ) {
        match result {
            Ok(block) => {
                *completed_bytes = (*completed_bytes).saturating_add(block.original_len);
                if block.raw_passthrough {
                    *raw_passthrough_blocks = raw_passthrough_blocks.saturating_add(1);
                }
                if first_error.is_some() {
                    return;
                }

                let block_id = block.id;
                if pending_write.insert(block_id, block).is_some() {
                    first_error.get_or_insert(crate::OxideError::InvalidFormat(
                        "duplicate block id received from worker",
                    ));
                    return;
                }

                while let Some(ready) = pending_write.remove(next_write_id) {
                    *output_bytes_written = (*output_bytes_written)
                        .saturating_add(BLOCK_HEADER_SIZE as u64)
                        .saturating_add(ready.data.len() as u64);
                    if let Err(error) = archive_writer.write_owned_block(ready) {
                        first_error.get_or_insert(error);
                        break;
                    }
                    *next_write_id += 1;
                }
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }

    fn build_stats_extensions(
        input_bytes_total: u64,
        output_bytes_total: u64,
        runtime: &PoolRuntimeSnapshot,
        block_size: BlockSizeDecision,
        raw_passthrough_blocks: u64,
        compression_preset: CompressionPreset,
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
        extensions.insert(
            "tuning.block_size".to_string(),
            StatValue::U64(block_size.selected_block_size as u64),
        );
        extensions.insert(
            "tuning.autotune_requested".to_string(),
            StatValue::U64(block_size.autotune_requested as u64),
        );
        extensions.insert(
            "tuning.autotune_ran".to_string(),
            StatValue::U64(block_size.autotune_ran as u64),
        );
        extensions.insert(
            "tuning.autotune_sampled_bytes".to_string(),
            StatValue::U64(block_size.sampled_bytes as u64),
        );
        extensions.insert(
            "compression.raw_passthrough_blocks".to_string(),
            StatValue::U64(raw_passthrough_blocks),
        );
        extensions.insert(
            "compression.preset".to_string(),
            StatValue::Text(format!("{compression_preset:?}")),
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
        block_size: BlockSizeDecision,
    ) -> Result<ArchiveOutcome<W>> {
        let PreparedInput {
            source_kind,
            batches,
            input_bytes_total,
        } = prepared;
        let total_blocks = batches.len();
        let block_count = u32::try_from(total_blocks)
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;
        let max_inflight_blocks = Self::max_inflight_blocks(total_blocks, self.num_workers);

        let worker_pool = WorkerPool::new(
            self.num_workers,
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
        );

        let compression_preset = self.performance.compression_preset;
        let raw_fallback_enabled = self.performance.raw_fallback_enabled;
        let handle = worker_pool.spawn(move |_worker_id, batch, pool, compression| {
            Self::process_batch(
                batch,
                pool,
                compression,
                compression_preset,
                raw_fallback_enabled,
            )
        });

        let mut archive_writer = ArchiveWriter::new(writer, Arc::clone(&self.buffer_pool));
        archive_writer.write_global_header_with_flags(
            block_count,
            directory::source_kind_flags(source_kind),
        )?;
        let mut output_bytes_written = GLOBAL_HEADER_SIZE as u64;
        let mut pending_write = BTreeMap::<usize, CompressedBlock>::new();
        let mut next_write_id = 0usize;

        let started_at = Instant::now();
        let mut last_emit_at = Instant::now();
        let emit_every = options.progress_interval.max(Duration::from_millis(100));
        let mut first_error: Option<crate::OxideError> = None;
        let mut submitted_count = 0usize;
        let mut completed_bytes = 0u64;
        let mut received_count = 0usize;
        let mut raw_passthrough_blocks = 0u64;
        let mut shutdown_called = false;
        let mut batches = batches.into_iter();

        loop {
            while submitted_count.saturating_sub(received_count) < max_inflight_blocks {
                let Some(batch) = batches.next() else {
                    break;
                };
                handle.submit(batch)?;
                submitted_count += 1;
            }

            Self::drain_results_to_writer(
                &handle,
                &mut archive_writer,
                &mut pending_write,
                &mut next_write_id,
                &mut output_bytes_written,
                &mut completed_bytes,
                &mut first_error,
                &mut raw_passthrough_blocks,
                &mut received_count,
                SUBMISSION_DRAIN_BUDGET,
            );

            if submitted_count == total_blocks && !shutdown_called {
                handle.shutdown();
                shutdown_called = true;
            }

            if shutdown_called && received_count == submitted_count {
                if options.emit_final_progress {
                    Self::emit_archive_progress_if_due(
                        &handle,
                        source_kind,
                        started_at,
                        input_bytes_total,
                        completed_bytes,
                        output_bytes_written,
                        block_count,
                        emit_every,
                        &mut last_emit_at,
                        true,
                        sink,
                    );
                }
                break;
            }

            Self::recv_result_to_writer(
                &handle,
                Duration::from_millis(100),
                &mut archive_writer,
                &mut pending_write,
                &mut next_write_id,
                &mut output_bytes_written,
                &mut completed_bytes,
                &mut first_error,
                &mut raw_passthrough_blocks,
                &mut received_count,
            );
            Self::emit_archive_progress_if_due(
                &handle,
                source_kind,
                started_at,
                input_bytes_total,
                completed_bytes,
                output_bytes_written,
                block_count,
                emit_every,
                &mut last_emit_at,
                false,
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
        if next_write_id != submitted_count || !pending_write.is_empty() {
            return Err(crate::OxideError::InvalidFormat(
                "writer has pending blocks after completion",
            ));
        }

        let writer = archive_writer.write_footer()?;
        output_bytes_written = output_bytes_written.saturating_add(FOOTER_SIZE as u64);
        let extensions = Self::build_stats_extensions(
            input_bytes_total,
            output_bytes_written,
            &final_runtime,
            block_size,
            raw_passthrough_blocks,
            self.performance.compression_preset,
        );

        let stats = ArchiveRunStats {
            source_kind,
            elapsed: started_at.elapsed(),
            input_bytes_total,
            output_bytes_total: output_bytes_written,
            blocks_total: block_count,
            blocks_completed: final_runtime.completed as u32,
            workers: final_runtime.workers,
            extensions,
        };

        Ok(ArchiveOutcome { writer, stats })
    }

    fn process_batch(
        batch: Batch,
        pool: &BufferPool,
        compression: CompressionAlgo,
        compression_preset: CompressionPreset,
        raw_fallback_enabled: bool,
    ) -> Result<CompressedBlock> {
        // High-performance mode currently bypasses preprocessing. Transform stages can be
        // re-enabled later without changing on-disk block metadata shape.
        let pre_proc = PreProcessingStrategy::None;
        let source = batch.data();
        let compressed = crate::compression::apply_compression(source, compression)?;
        let raw_passthrough = raw_fallback_enabled && compressed.len() >= source.len();
        let output = if raw_passthrough {
            source
        } else {
            compressed.as_slice()
        };

        let mut scratch = pool.acquire();
        scratch.extend_from_slice(output);

        // Move the pooled Vec out without allocating.
        let mut data = Vec::new();
        std::mem::swap(scratch.as_mut_vec(), &mut data);

        Ok(CompressedBlock::with_compression_meta(
            batch.id,
            data,
            pre_proc,
            CompressionMeta::new(compression, compression_preset, raw_passthrough),
            batch.len() as u64,
        ))
    }

    fn choose_block_size_for_file(
        &self,
        path: &Path,
        input_bytes: u64,
    ) -> Result<BlockSizeDecision> {
        if !self.performance.autotune_enabled
            || input_bytes < self.performance.autotune_min_input_bytes
        {
            return Ok(BlockSizeDecision {
                selected_block_size: self.scanner.target_block_size(),
                autotune_requested: self.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let sample = Self::collect_file_sample(path, self.performance.autotune_sample_bytes)?;
        self.pick_tuned_block_size(&sample)
    }

    fn choose_block_size_for_directory(
        &self,
        discovery: &directory::DirectoryDiscovery,
    ) -> Result<BlockSizeDecision> {
        if !self.performance.autotune_enabled
            || discovery.input_bytes_total < self.performance.autotune_min_input_bytes
        {
            return Ok(BlockSizeDecision {
                selected_block_size: self.scanner.target_block_size(),
                autotune_requested: self.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let sample =
            Self::collect_directory_sample(discovery, self.performance.autotune_sample_bytes)?;
        self.pick_tuned_block_size(&sample)
    }

    fn pick_tuned_block_size(&self, sample: &[u8]) -> Result<BlockSizeDecision> {
        if sample.is_empty() {
            return Ok(BlockSizeDecision {
                selected_block_size: self.scanner.target_block_size(),
                autotune_requested: self.performance.autotune_enabled,
                autotune_ran: false,
                sampled_bytes: 0,
            });
        }

        let mut candidates = AUTOTUNE_CANDIDATE_BLOCK_SIZES.to_vec();
        let default_block = self.scanner.target_block_size().max(1);
        if !candidates.contains(&default_block) {
            candidates.push(default_block);
        }
        candidates.sort_unstable();
        candidates.dedup();

        let mut scores = Vec::with_capacity(candidates.len());
        for block_size in candidates {
            scores.push(self.score_block_size(sample, block_size)?);
        }

        scores.sort_by(|left, right| {
            right
                .throughput_bps
                .partial_cmp(&left.throughput_bps)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.compressed_bytes.cmp(&right.compressed_bytes))
                .then_with(|| left.block_size.cmp(&right.block_size))
        });

        let selected = scores
            .first()
            .map(|score| score.block_size)
            .unwrap_or(default_block);
        Ok(BlockSizeDecision {
            selected_block_size: selected.max(1),
            autotune_requested: self.performance.autotune_enabled,
            autotune_ran: true,
            sampled_bytes: sample.len(),
        })
    }

    fn score_block_size(&self, sample: &[u8], block_size: usize) -> Result<BlockSizeScore> {
        let started = Instant::now();
        let mut compressed_bytes = 0usize;
        let chunk_size = block_size.max(1);
        for chunk in sample.chunks(chunk_size) {
            let compressed = crate::compression::apply_compression(chunk, self.compression_algo)?;
            compressed_bytes = compressed_bytes.saturating_add(compressed.len());
        }
        let elapsed = started.elapsed().as_secs_f64().max(1e-9);
        Ok(BlockSizeScore {
            block_size: chunk_size,
            throughput_bps: sample.len() as f64 / elapsed,
            compressed_bytes,
        })
    }

    fn collect_file_sample(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
        let limit = max_bytes.max(1);
        let mut file = fs::File::open(path)?;
        let mut sample = Vec::with_capacity(limit);
        let mut scratch = vec![0u8; 64 * 1024];
        while sample.len() < limit {
            let to_read = (limit - sample.len()).min(scratch.len());
            let read = file.read(&mut scratch[..to_read])?;
            if read == 0 {
                break;
            }
            sample.extend_from_slice(&scratch[..read]);
        }
        Ok(sample)
    }

    fn collect_directory_sample(
        discovery: &directory::DirectoryDiscovery,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        let limit = max_bytes.max(1);
        let mut sample = Vec::with_capacity(limit);
        let mut scratch = vec![0u8; 64 * 1024];
        for file in &discovery.files {
            if sample.len() >= limit {
                break;
            }
            let mut reader = fs::File::open(&file.full_path)?;
            while sample.len() < limit {
                let to_read = (limit - sample.len()).min(scratch.len());
                let read = reader.read(&mut scratch[..to_read])?;
                if read == 0 {
                    break;
                }
                sample.extend_from_slice(&scratch[..read]);
            }
        }
        Ok(sample)
    }

    fn prepare_file(&self, path: &Path, block_size: usize) -> Result<PreparedInput> {
        let scanner = InputScanner::new(block_size);
        let batches = scanner.scan_file(path)?;
        let input_bytes_total = batches.iter().map(|batch| batch.len() as u64).sum();
        Ok(PreparedInput {
            source_kind: ArchiveSourceKind::File,
            batches,
            input_bytes_total,
        })
    }
}
