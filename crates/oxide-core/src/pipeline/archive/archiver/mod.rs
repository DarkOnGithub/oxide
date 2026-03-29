use crate::dictionary::{ArchiveDictionaryBank, ArchiveDictionaryMode, DictionaryTrainer};
use crate::format::{
    should_force_raw_storage, ArchiveBlockWriter, ArchiveWriter, SeekableArchiveWriter,
};
use crate::io::InputScanner;
use crate::pipeline::types::{ArchivePipelineConfig, ArchiveSourceKind};
use crate::telemetry::{ArchiveRun, RunTelemetryOptions, TelemetrySink};
use crate::types::ChunkEncodingPlan;
use crate::types::Result;
use std::fs;
use std::io::{Seek, Write};
use std::path::Path;
use std::time::Instant;

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
use crate::telemetry::{ArchivePlanningCompleteEvent, TelemetryEvent};

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
        let planning_started = Instant::now();
        let prepared = self.prepare_file(path, block_size)?;
        let blocks_total = u32::try_from(prepared.batches.len())
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;
        sink.on_event(TelemetryEvent::ArchivePlanningComplete(
            ArchivePlanningCompleteEvent {
                elapsed: planning_started.elapsed(),
                input_bytes_total: prepared.input_bytes_total,
                blocks_total,
            },
        ));
        self.archive_prepared_with_writer(
            prepared,
            writer,
            options,
            sink,
            block_size,
            |writer, manifest| ArchiveWriter::with_manifest(writer, Some(manifest)),
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
        let planning_started = Instant::now();
        let prepared = self.prepare_file(path, block_size)?;
        let blocks_total = u32::try_from(prepared.batches.len())
            .map_err(|_| crate::OxideError::InvalidFormat("too many blocks for OXZ v1"))?;
        sink.on_event(TelemetryEvent::ArchivePlanningComplete(
            ArchivePlanningCompleteEvent {
                elapsed: planning_started.elapsed(),
                input_bytes_total: prepared.input_bytes_total,
                blocks_total,
            },
        ));
        self.archive_prepared_with_writer(
            prepared,
            writer,
            options,
            sink,
            block_size,
            |writer, manifest| SeekableArchiveWriter::with_manifest(writer, Some(manifest)),
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
            |writer, manifest, reorder_limit| {
                ArchiveWriter::with_reorder_limit_and_manifest(
                    writer,
                    reorder_limit,
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
            |writer, manifest, reorder_limit| {
                SeekableArchiveWriter::with_reorder_limit_and_manifest(
                    writer,
                    reorder_limit,
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
            |writer, manifest| ArchiveWriter::with_manifest(writer, Some(manifest)),
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
            |writer, manifest| SeekableArchiveWriter::with_manifest(writer, Some(manifest)),
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
        F: FnOnce(W, crate::format::ArchiveManifest) -> AW,
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

    pub fn prepare_file(&self, path: &Path, _block_size: usize) -> Result<PreparedInput> {
        let scanner = InputScanner::with_chunking_policy_and_plan(
            self.config.chunking_policy,
            ChunkEncodingPlan::new(self.config.compression_algo)
                .with_level(self.config.performance.compression_level)
                .with_lzma_extreme(self.config.performance.lzma_extreme)
                .with_lzma_dictionary_size(self.config.performance.lzma_dictionary_size),
        );
        let mut batches = scanner.scan_file(path)?;
        let force_raw_storage = batches
            .first()
            .map(|_| should_force_raw_storage(path))
            .unwrap_or(false);
        if force_raw_storage {
            for batch in &mut batches {
                batch.force_raw_storage = true;
            }
        }
        let input_bytes_total = batches.iter().map(|batch| batch.len() as u64).sum();
        let dictionary_bank = train_file_dictionary_bank(self.config, &batches)?;
        let manifest =
            file_manifest(path, input_bytes_total)?.with_dictionary_bank(dictionary_bank);
        Ok(PreparedInput {
            source_kind: ArchiveSourceKind::File,
            manifest,
            batches,
            input_bytes_total,
        })
    }
}

fn train_file_dictionary_bank(
    config: &ArchivePipelineConfig,
    batches: &[crate::Batch],
) -> Result<ArchiveDictionaryBank> {
    if let Some(dictionary_bank) = config.imported_dictionary_bank.as_ref() {
        return Ok(dictionary_bank.clone());
    }

    if config.skip_compression
        || config.compression_algo != crate::CompressionAlgo::Zstd
        || config.performance.dictionary_mode == ArchiveDictionaryMode::Off
    {
        return Ok(ArchiveDictionaryBank::default());
    }

    let mut trainer = DictionaryTrainer::new(config.performance.dictionary_mode);
    for batch in batches {
        trainer.observe_with_path(&batch.source_path, batch.data());
    }

    trainer.build(
        config.compression_algo,
        config.performance.compression_level,
    )
}
