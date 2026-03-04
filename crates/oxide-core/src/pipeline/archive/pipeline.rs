use std::fs;
use std::io::{Read, Write, Seek};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::buffer::BufferPool;
use crate::io::InputScanner;
use crate::telemetry::{
    ArchiveRun, ExtractReport, RunTelemetryOptions, TelemetryEvent, TelemetrySink,
};
use crate::types::{CompressionAlgo, Result};

use super::archiver::Archiver;
use super::extractor::Extractor;
use super::telemetry::*;
use super::super::directory;
use super::super::types::{ArchivePipelineConfig, ArchiveSourceKind, PipelinePerformanceOptions};

pub struct NoopTelemetrySink;

impl TelemetrySink for NoopTelemetrySink {
    fn on_event(&mut self, _event: TelemetryEvent) {}
}

/// This pipeline performs pass-through processing for block payloads while preserving
/// the metadata and ordering guarantees needed by the archive format.
pub struct ArchivePipeline {
    pub(crate) scanner: InputScanner,
    pub(crate) num_workers: usize,
    pub(crate) compression_algo: CompressionAlgo,
    pub(crate) buffer_pool: Arc<BufferPool>,
    pub(crate) performance: PipelinePerformanceOptions,
}

impl ArchivePipeline {
    /// Creates a new archive pipeline.
    pub fn new(config: ArchivePipelineConfig) -> Self {
        Self {
            scanner: InputScanner::new(config.target_block_size),
            num_workers: config.workers.max(1),
            compression_algo: config.compression_algo,
            buffer_pool: config.buffer_pool,
            performance: config.performance,
        }
    }

    fn config(&self) -> ArchivePipelineConfig {
        ArchivePipelineConfig {
            target_block_size: self.scanner.target_block_size(),
            workers: self.num_workers,
            compression_algo: self.compression_algo,
            buffer_pool: Arc::clone(&self.buffer_pool),
            performance: self.performance,
        }
    }

    /// Reads an input file, processes blocks in parallel, and writes an OXZ archive.
    pub fn archive_file<P, W>(
        &self,
        path: P,
        writer: W,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ArchiveRun<W>>
    where
        P: AsRef<Path>,
        W: Write,
    {
        let path = path.as_ref();
        tracing::info!(path = %path.display(), "starting file archival");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let archiver = Archiver::new(&self.config());
        archiver.archive_file_path_with(path, writer, &options, sink)
    }

    /// Archives either a single file or a directory tree.
    pub fn archive_path<P, W>(
        &self,
        path: P,
        writer: W,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ArchiveRun<W>>
    where
        P: AsRef<Path>,
        W: Write + Send + 'static,
    {
        let path = path.as_ref();
        tracing::info!(path = %path.display(), "starting path archival");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let metadata = fs::metadata(path)?;
        if metadata.is_file() {
            let archiver = Archiver::new(&self.config());
            archiver.archive_file_path_with(path, writer, &options, sink)
        } else if metadata.is_dir() {
            let archiver = Archiver::new(&self.config());
            archiver.archive_directory_streaming_with(path, writer, &options, sink)
        } else {
            Err(crate::OxideError::InvalidFormat(
                "path must be a file or directory",
            ))
        }
    }

    /// Archives a directory tree as a single OXZ payload.
    pub fn archive_directory<P, W>(
        &self,
        dir_path: P,
        writer: W,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ArchiveRun<W>>
    where
        P: AsRef<Path>,
        W: Write + Send + 'static,
    {
        let path = dir_path.as_ref();
        tracing::info!(path = %path.display(), "starting directory archival");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let archiver = Archiver::new(&self.config());
        archiver.archive_directory_streaming_with(path, writer, &options, sink)
    }

    /// Extracts all block payload bytes from an OXZ archive in block order and
    /// returns a detailed report.
    pub fn extract_archive<R: Read + Seek>(
        &self,
        reader: R,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<(Vec<u8>, ExtractReport)> {
        tracing::info!("starting archive extraction");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let started_at = Instant::now();
        let extractor = Extractor::new(self.num_workers);
        let mut decoded = extractor.read_archive_payload_with_metrics(
            reader,
            started_at,
            &options,
            sink,
        )?;
        let source_kind = directory::source_kind_from_flags(decoded.flags);
        let extensions = extract_extensions_from_flags(decoded.flags);
        let decoded_bytes_total = decoded.payload.len() as u64;
        let stage_timings = decoded.stage_timings;
        let payload = std::mem::take(&mut decoded.payload);
        let report = build_extract_report_helper(
            started_at,
            decoded,
            source_kind,
            decoded_bytes_total,
            decoded_bytes_total,
            extensions,
            options,
        );
        record_extract_run_telemetry(report.elapsed, stage_timings);

        sink.on_event(TelemetryEvent::ExtractCompleted(report.clone()));

        Ok((payload, report))
    }

    /// Extracts a directory tree archive produced by [`archive_directory`].
    pub fn extract_directory_archive<R, P>(
        &self,
        reader: R,
        output_dir: P,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ExtractReport>
    where
        R: Read + Seek,
        P: AsRef<Path>,
    {
        tracing::info!(output_dir = %output_dir.as_ref().display(), "starting directory archive extraction");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let started_at = Instant::now();
        let extractor = Extractor::new(self.num_workers);
        let mut decoded = extractor.read_archive_payload_with_metrics(
            reader,
            started_at,
            &options,
            sink,
        )?;
        let mut extensions = extract_extensions_from_flags(decoded.flags);
        let decoded_bytes_total = decoded.payload.len() as u64;

        let directory_decode_started = Instant::now();
        let entries = directory::decode_directory_entries(&decoded.payload, decoded.flags)?.ok_or(
            crate::OxideError::InvalidFormat("archive does not contain a directory bundle"),
        )?;
        decoded.stage_timings.directory_decode += directory_decode_started.elapsed();

        let output_bytes_total = entries
            .iter()
            .filter_map(|entry| match entry {
                directory::DirectoryBundleEntry::File { data, .. } => Some(data.len() as u64),
                directory::DirectoryBundleEntry::Directory { .. } => None,
            })
            .sum();
        extensions.insert(
            "extract.directory_entries".to_string(),
            crate::telemetry::ReportValue::U64(entries.len() as u64),
        );

        let write_started = Instant::now();
        directory::write_directory_entries(output_dir.as_ref(), entries)?;
        decoded.stage_timings.output_write += write_started.elapsed();
        let stage_timings = decoded.stage_timings;

        let report = build_extract_report_helper(
            started_at,
            decoded,
            ArchiveSourceKind::Directory,
            decoded_bytes_total,
            output_bytes_total,
            extensions,
            options,
        );
        record_extract_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ExtractCompleted(report.clone()));
        Ok(report)
    }

    /// Extracts an archive to `output_path` and returns the extract report.
    ///
    /// File payloads are written to `output_path` as a single file.
    /// Directory payloads are restored under `output_path` as a root directory.
    pub fn extract_path<R, P>(
        &self,
        reader: R,
        output_path: P,
        options: RunTelemetryOptions,
        sink: Option<&mut dyn TelemetrySink>,
    ) -> Result<ExtractReport>
    where
        R: Read + Seek,
        P: AsRef<Path>,
    {
        tracing::info!(output_path = %output_path.as_ref().display(), "starting path extraction");
        let mut noop = NoopTelemetrySink;
        let sink = sink.unwrap_or(&mut noop);
        let started_at = Instant::now();
        let extractor = Extractor::new(self.num_workers);
        let mut decoded = extractor.read_archive_payload_with_metrics(
            reader,
            started_at,
            &options,
            sink,
        )?;
        let mut extensions = extract_extensions_from_flags(decoded.flags);
        let decoded_bytes_total = decoded.payload.len() as u64;
        let (source_kind, output_bytes_total) =
            extractor.restore_decoded_payload(output_path.as_ref(), &mut decoded, &mut extensions)?;
        let stage_timings = decoded.stage_timings;
        let report = build_extract_report_helper(
            started_at,
            decoded,
            source_kind,
            decoded_bytes_total,
            output_bytes_total,
            extensions,
            options,
        );
        record_extract_run_telemetry(report.elapsed, stage_timings);
        sink.on_event(TelemetryEvent::ExtractCompleted(report.clone()));
        Ok(report)
    }
}

fn extract_extensions_from_flags(flags: u32) -> std::collections::BTreeMap<String, crate::telemetry::ReportValue> {
    let mut extensions = std::collections::BTreeMap::new();
    extensions.insert("extract.flags".to_string(), crate::telemetry::ReportValue::U64(flags as u64));
    extensions
}

fn build_extract_report_helper(
    started_at: Instant,
    decoded: super::types::DecodedArchivePayload,
    source_kind: ArchiveSourceKind,
    decoded_bytes_total: u64,
    output_bytes_total: u64,
    extensions: std::collections::BTreeMap<String, crate::telemetry::ReportValue>,
    options: RunTelemetryOptions,
) -> ExtractReport {
    super::telemetry::build_extract_report(
        source_kind,
        started_at.elapsed(),
        decoded.archive_bytes_total,
        decoded_bytes_total,
        output_bytes_total,
        decoded.blocks_total,
        decoded.workers,
        decoded.stage_timings,
        extensions,
        options,
    )
}
