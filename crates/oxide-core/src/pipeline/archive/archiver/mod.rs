use std::fs;
use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;

use crate::buffer::BufferPool;
use crate::format::{ArchiveBlockWriter, ArchiveWriter, SeekableArchiveWriter};
use crate::io::{ChunkingPolicy, InputScanner};
use crate::pipeline::types::{ArchivePipelineConfig, ArchiveSourceKind};
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetrySink};
use crate::types::Result;

pub mod directory;
pub mod prepared;
pub mod processing;
pub mod utils;

pub use directory::record_result_to_writer_queue;
pub use prepared::{drain_results_to_writer, record_result_to_writer, recv_result_to_writer};
pub use processing::*;
pub use utils::*;

use self::directory::archive_directory_streaming_with_writer;
use self::prepared::archive_prepared_with_writer;
use super::telemetry;
use super::types::PreparedInput;

pub struct Archiver<'a> {
    pub config: &'a ArchivePipelineConfig,
}

impl<'a> Archiver<'a> {
    pub fn new(config: &'a ArchivePipelineConfig) -> Self {
        Self { config }
    }

    pub fn archive_file_path_with<W: Write>(
        &self,
        path: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>> {
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(crate::OxideError::InvalidFormat(
                "archive_file expects a file path",
            ));
        }
        telemetry::begin_archive_run_telemetry();
        let block_size = self.config.target_block_size;
        let prepared = self.prepare_file(path, block_size)?;
        self.archive_prepared_with_writer(
            prepared,
            writer,
            options,
            sink,
            block_size,
            |writer, pool, dictionaries, manifest| {
                ArchiveWriter::with_dictionaries_and_manifest(
                    writer,
                    pool,
                    dictionaries,
                    Some(manifest),
                )
            },
        )
    }

    pub fn archive_file_path_seekable_with<W: Write + Seek>(
        &self,
        path: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>> {
        let metadata = fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(crate::OxideError::InvalidFormat(
                "archive_file expects a file path",
            ));
        }
        telemetry::begin_archive_run_telemetry();
        let block_size = self.config.target_block_size;
        let prepared = self.prepare_file(path, block_size)?;
        self.archive_prepared_with_writer(
            prepared,
            writer,
            options,
            sink,
            block_size,
            |writer, pool, dictionaries, manifest| {
                SeekableArchiveWriter::with_dictionaries_and_manifest(
                    writer,
                    pool,
                    dictionaries,
                    Some(manifest),
                )
            },
        )
    }

    pub fn archive_directory_streaming_with<W: Write + Send + 'static>(
        &self,
        root: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>> {
        telemetry::begin_archive_run_telemetry();
        archive_directory_streaming_with_writer(
            self.config,
            root,
            writer,
            options,
            sink,
            |writer, pool, dictionaries, manifest, reorder_limit| {
                ArchiveWriter::with_reorder_limit_and_manifest(
                    writer,
                    pool,
                    reorder_limit,
                    dictionaries,
                    Some(manifest),
                )
            },
        )
    }

    pub fn archive_directory_streaming_seekable_with<W: Write + Seek + Send + 'static>(
        &self,
        root: &Path,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
    ) -> Result<ArchiveRun<W>> {
        telemetry::begin_archive_run_telemetry();
        archive_directory_streaming_with_writer(
            self.config,
            root,
            writer,
            options,
            sink,
            |writer, pool, dictionaries, manifest, reorder_limit| {
                SeekableArchiveWriter::with_reorder_limit_and_manifest(
                    writer,
                    pool,
                    reorder_limit,
                    dictionaries,
                    Some(manifest),
                )
            },
        )
    }

    pub fn archive_prepared_with<W: Write>(
        &self,
        prepared: PreparedInput,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
        block_size: usize,
    ) -> Result<ArchiveRun<W>> {
        telemetry::begin_archive_run_telemetry();
        self.archive_prepared_with_writer(
            prepared,
            writer,
            options,
            sink,
            block_size,
            |writer, pool, dictionaries, manifest| {
                ArchiveWriter::with_dictionaries_and_manifest(
                    writer,
                    pool,
                    dictionaries,
                    Some(manifest),
                )
            },
        )
    }

    pub fn archive_prepared_seekable_with<W: Write + Seek>(
        &self,
        prepared: PreparedInput,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
        block_size: usize,
    ) -> Result<ArchiveRun<W>> {
        telemetry::begin_archive_run_telemetry();
        self.archive_prepared_with_writer(
            prepared,
            writer,
            options,
            sink,
            block_size,
            |writer, pool, dictionaries, manifest| {
                SeekableArchiveWriter::with_dictionaries_and_manifest(
                    writer,
                    pool,
                    dictionaries,
                    Some(manifest),
                )
            },
        )
    }

    fn archive_prepared_with_writer<W, AW, F>(
        &self,
        prepared: PreparedInput,
        writer: W,
        options: &RunTelemetryOptions,
        sink: &mut dyn TelemetrySink,
        block_size: usize,
        writer_factory: F,
    ) -> Result<ArchiveRun<W>>
    where
        W: Write,
        AW: ArchiveBlockWriter<InnerWriter = W>,
        F: FnOnce(
            W,
            Arc<BufferPool>,
            Vec<crate::format::StoredDictionary>,
            crate::format::ArchiveManifest,
        ) -> AW,
    {
        archive_prepared_with_writer(
            self.config,
            prepared,
            writer,
            options,
            sink,
            block_size,
            writer_factory,
        )
    }

    pub fn prepare_file(&self, path: &Path, block_size: usize) -> Result<PreparedInput> {
        let scanner = InputScanner::with_chunking_policy(ChunkingPolicy::for_preset(
            block_size,
            self.config.performance.compression_preset,
        ));
        let batches = scanner.scan_file(path)?;
        let input_bytes_total = batches.iter().map(|batch| batch.len() as u64).sum();
        let manifest = file_manifest(path, input_bytes_total)?;
        Ok(PreparedInput {
            source_kind: ArchiveSourceKind::File,
            manifest,
            batches,
            input_bytes_total,
        })
    }
}
