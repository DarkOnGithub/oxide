use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::core::WorkerRuntimeSnapshot;
use crate::pipeline::ArchiveSourceKind;
use crate::telemetry::{self, TelemetrySnapshot};
use crate::types::duration_to_us;

/// Unified runtime telemetry options for archive/extract operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTelemetryOptions {
    /// Progress emission interval for real-time run events.
    pub progress_interval: Duration,
    /// Emit a final forced progress event before completion.
    pub emit_final_progress: bool,
    /// Attach a point-in-time telemetry snapshot to final reports.
    pub include_telemetry_snapshot: bool,
}

impl Default for RunTelemetryOptions {
    fn default() -> Self {
        Self {
            progress_interval: Duration::from_millis(250),
            emit_final_progress: true,
            include_telemetry_snapshot: true,
        }
    }
}

/// Extensible scalar value used by report exports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReportValue {
    /// Unsigned 64-bit integer.
    U64(u64),
    /// 64-bit floating point number.
    F64(f64),
    /// Time duration.
    Duration(Duration),
    /// Boolean flag.
    Bool(bool),
    /// UTF-8 string.
    Text(String),
}

/// Worker-level metrics used in exported reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerReport {
    /// Unique identifier for the worker.
    pub worker_id: usize,
    /// Total number of tasks completed by this worker.
    pub tasks_completed: usize,
    /// Total time the worker thread was alive.
    pub uptime: Duration,
    /// Total time the worker was actively processing tasks.
    pub busy: Duration,
    /// Total time the worker was idle.
    pub idle: Duration,
    /// Ratio of busy time to uptime.
    pub utilization: f64,
    /// Extensible metrics for this specific worker.
    pub extensions: BTreeMap<String, ReportValue>,
}

impl WorkerReport {
    /// Creates a report from a runtime snapshot.
    pub fn from_runtime(runtime: &WorkerRuntimeSnapshot) -> Self {
        Self {
            worker_id: runtime.worker_id,
            tasks_completed: runtime.tasks_completed,
            uptime: runtime.uptime,
            busy: runtime.busy,
            idle: runtime.idle,
            utilization: runtime.utilization,
            extensions: BTreeMap::new(),
        }
    }
}

/// Main-thread stage timings and optional extra values.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ThreadReport {
    /// Name of the thread (e.g., "main").
    pub name: String,
    /// Microseconds spent in various stages of the pipeline.
    pub stage_us: BTreeMap<String, u64>,
    /// Extensible metrics for this thread.
    pub extensions: BTreeMap<String, ReportValue>,
}

impl ThreadReport {
    /// Creates a new thread report with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            stage_us: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }
}

/// Detailed archive report built from a completed run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchiveReport {
    /// Type of source archived.
    pub source_kind: ArchiveSourceKind,
    /// Total wall-clock time for the operation.
    pub elapsed: Duration,
    /// Total size of all input files.
    pub input_bytes_total: u64,
    /// Total size of the produced OXZ archive.
    pub output_bytes_total: u64,
    /// Total number of blocks processed.
    pub blocks_total: u32,
    /// Number of blocks successfully completed.
    pub blocks_completed: u32,
    /// Average read throughput in bytes per second.
    pub read_avg_bps: f64,
    /// Average write throughput in bytes per second.
    pub write_avg_bps: f64,
    /// Ratio of output size to input size.
    pub output_input_ratio: f64,
    /// Reports for each individual worker.
    pub workers: Vec<WorkerReport>,
    /// Timing report for the main pipeline thread.
    pub main_thread: ThreadReport,
    /// Extensible statistics for the entire run.
    pub extensions: BTreeMap<String, ReportValue>,
    /// Optional point-in-time snapshot of internal telemetry.
    pub telemetry: Option<TelemetrySnapshot>,
}

impl ArchiveReport {
    /// Optionally attaches a telemetry snapshot to the report.
    pub fn with_telemetry_snapshot(mut self, include_telemetry_snapshot: bool) -> Self {
        self.telemetry = if include_telemetry_snapshot {
            Some(telemetry::snapshot())
        } else {
            None
        };
        self
    }
}

/// Wrapper for archive operations that return writer + report.
#[derive(Debug)]
pub struct ArchiveRun<W> {
    /// The underlying writer containing the archive data.
    pub writer: W,
    /// Summary report for the archive operation.
    pub report: ArchiveReport,
}

/// Detailed extract report including worker and main-thread timing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtractReport {
    /// Type of source extracted.
    pub source_kind: ArchiveSourceKind,
    /// Total wall-clock time for the operation.
    pub elapsed: Duration,
    /// Total size of the input archive.
    pub archive_bytes_total: u64,
    /// Total size of all blocks after decompression.
    pub decoded_bytes_total: u64,
    /// Total size of all files written to disk.
    pub output_bytes_total: u64,
    /// Total number of blocks in the archive.
    pub blocks_total: u32,
    /// Average read throughput in bytes per second.
    pub read_avg_bps: f64,
    /// Average decode throughput in bytes per second.
    pub decode_avg_bps: f64,
    /// Average write throughput in bytes per second.
    pub output_avg_bps: f64,
    /// Ratio of output size to archive size.
    pub output_archive_ratio: f64,
    /// Reports for each individual worker.
    pub workers: Vec<WorkerReport>,
    /// Timing report for the main pipeline thread.
    pub main_thread: ThreadReport,
    /// Extensible statistics for the extraction.
    pub extensions: BTreeMap<String, ReportValue>,
    /// Optional point-in-time snapshot of internal telemetry.
    pub telemetry: Option<TelemetrySnapshot>,
}

impl ExtractReport {
    /// Creates a new extract report from raw measurements.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_kind: ArchiveSourceKind,
        elapsed: Duration,
        archive_bytes_total: u64,
        decoded_bytes_total: u64,
        output_bytes_total: u64,
        blocks_total: u32,
        workers: Vec<WorkerReport>,
        main_thread: ThreadReport,
        extensions: BTreeMap<String, ReportValue>,
        options: RunTelemetryOptions,
    ) -> Self {
        let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = archive_bytes_total as f64 / elapsed_secs;
        let decode_avg_bps = decoded_bytes_total as f64 / elapsed_secs;
        let output_avg_bps = output_bytes_total as f64 / elapsed_secs;
        let output_archive_ratio = if archive_bytes_total == 0 {
            1.0
        } else {
            output_bytes_total as f64 / archive_bytes_total as f64
        };
        let telemetry = if options.include_telemetry_snapshot {
            Some(telemetry::snapshot())
        } else {
            None
        };

        Self {
            source_kind,
            elapsed,
            archive_bytes_total,
            decoded_bytes_total,
            output_bytes_total,
            blocks_total,
            read_avg_bps,
            decode_avg_bps,
            output_avg_bps,
            output_archive_ratio,
            workers,
            main_thread,
            extensions,
            telemetry,
        }
    }
}

/// Unified run report enum.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RunReport {
    /// Result of an archive operation.
    Archive(ArchiveReport),
    /// Result of an extract operation.
    Extract(ExtractReport),
}

/// Common export shape for report types.
pub trait ReportExport {
    fn to_flat_map(&self) -> BTreeMap<String, ReportValue>;
}

impl ReportExport for ArchiveReport {
    fn to_flat_map(&self) -> BTreeMap<String, ReportValue> {
        let mut out = BTreeMap::new();
        out.insert(
            "archive.source_kind".to_string(),
            ReportValue::Text(source_kind_label(self.source_kind).to_string()),
        );
        out.insert(
            "archive.elapsed_us".to_string(),
            ReportValue::U64(duration_to_us(self.elapsed)),
        );
        out.insert(
            "archive.input_bytes_total".to_string(),
            ReportValue::U64(self.input_bytes_total),
        );
        out.insert(
            "archive.output_bytes_total".to_string(),
            ReportValue::U64(self.output_bytes_total),
        );
        out.insert(
            "archive.blocks_total".to_string(),
            ReportValue::U64(self.blocks_total as u64),
        );
        out.insert(
            "archive.blocks_completed".to_string(),
            ReportValue::U64(self.blocks_completed as u64),
        );
        out.insert(
            "archive.read_avg_bps".to_string(),
            ReportValue::F64(self.read_avg_bps),
        );
        out.insert(
            "archive.write_avg_bps".to_string(),
            ReportValue::F64(self.write_avg_bps),
        );
        out.insert(
            "archive.output_input_ratio".to_string(),
            ReportValue::F64(self.output_input_ratio),
        );
        out.insert(
            "archive.worker_count".to_string(),
            ReportValue::U64(self.workers.len() as u64),
        );

        flatten_thread("thread.main", &self.main_thread, &mut out);
        flatten_workers(&self.workers, &mut out);
        flatten_extension_map("archive.extension", &self.extensions, &mut out);
        if let Some(snapshot) = &self.telemetry {
            flatten_telemetry(snapshot, &mut out);
        }

        out
    }
}

impl ReportExport for ExtractReport {
    fn to_flat_map(&self) -> BTreeMap<String, ReportValue> {
        let mut out = BTreeMap::new();
        out.insert(
            "extract.source_kind".to_string(),
            ReportValue::Text(source_kind_label(self.source_kind).to_string()),
        );
        out.insert(
            "extract.elapsed_us".to_string(),
            ReportValue::U64(duration_to_us(self.elapsed)),
        );
        out.insert(
            "extract.archive_bytes_total".to_string(),
            ReportValue::U64(self.archive_bytes_total),
        );
        out.insert(
            "extract.decoded_bytes_total".to_string(),
            ReportValue::U64(self.decoded_bytes_total),
        );
        out.insert(
            "extract.output_bytes_total".to_string(),
            ReportValue::U64(self.output_bytes_total),
        );
        out.insert(
            "extract.blocks_total".to_string(),
            ReportValue::U64(self.blocks_total as u64),
        );
        out.insert(
            "extract.read_avg_bps".to_string(),
            ReportValue::F64(self.read_avg_bps),
        );
        out.insert(
            "extract.decode_avg_bps".to_string(),
            ReportValue::F64(self.decode_avg_bps),
        );
        out.insert(
            "extract.output_avg_bps".to_string(),
            ReportValue::F64(self.output_avg_bps),
        );
        out.insert(
            "extract.output_archive_ratio".to_string(),
            ReportValue::F64(self.output_archive_ratio),
        );
        out.insert(
            "extract.worker_count".to_string(),
            ReportValue::U64(self.workers.len() as u64),
        );

        flatten_thread("thread.main", &self.main_thread, &mut out);
        flatten_workers(&self.workers, &mut out);
        flatten_extension_map("extract.extension", &self.extensions, &mut out);
        if let Some(snapshot) = &self.telemetry {
            flatten_telemetry(snapshot, &mut out);
        }

        out
    }
}

impl ReportExport for RunReport {
    fn to_flat_map(&self) -> BTreeMap<String, ReportValue> {
        match self {
            RunReport::Archive(report) => report.to_flat_map(),
            RunReport::Extract(report) => report.to_flat_map(),
        }
    }
}

fn source_kind_label(source_kind: ArchiveSourceKind) -> &'static str {
    match source_kind {
        ArchiveSourceKind::File => "file",
        ArchiveSourceKind::Directory => "directory",
    }
}

fn flatten_workers(workers: &[WorkerReport], out: &mut BTreeMap<String, ReportValue>) {
    for worker in workers {
        let prefix = format!("worker.{}", worker.worker_id);
        out.insert(
            format!("{prefix}.tasks_completed"),
            ReportValue::U64(worker.tasks_completed as u64),
        );
        out.insert(
            format!("{prefix}.uptime_us"),
            ReportValue::U64(duration_to_us(worker.uptime)),
        );
        out.insert(
            format!("{prefix}.busy_us"),
            ReportValue::U64(duration_to_us(worker.busy)),
        );
        out.insert(
            format!("{prefix}.idle_us"),
            ReportValue::U64(duration_to_us(worker.idle)),
        );
        out.insert(
            format!("{prefix}.utilization"),
            ReportValue::F64(worker.utilization),
        );
        flatten_extension_map(&format!("{prefix}.extension"), &worker.extensions, out);
    }
}

fn flatten_thread(prefix: &str, thread: &ThreadReport, out: &mut BTreeMap<String, ReportValue>) {
    out.insert(
        format!("{prefix}.name"),
        ReportValue::Text(thread.name.clone()),
    );
    for (stage, value) in &thread.stage_us {
        out.insert(
            format!("{prefix}.stage.{stage}_us"),
            ReportValue::U64(*value),
        );
    }
    flatten_extension_map(&format!("{prefix}.extension"), &thread.extensions, out);
}

fn flatten_extension_map(
    prefix: &str,
    extensions: &BTreeMap<String, ReportValue>,
    out: &mut BTreeMap<String, ReportValue>,
) {
    for (key, value) in extensions {
        out.insert(format!("{prefix}.{key}"), value.clone());
    }
}

fn flatten_telemetry(snapshot: &TelemetrySnapshot, out: &mut BTreeMap<String, ReportValue>) {
    for (name, value) in &snapshot.counters {
        out.insert(
            format!("telemetry.counter.{name}"),
            ReportValue::U64(*value),
        );
    }

    for (name, value) in &snapshot.gauges {
        out.insert(format!("telemetry.gauge.{name}"), ReportValue::U64(*value));
    }

    for (name, histogram) in &snapshot.histograms {
        out.insert(
            format!("telemetry.histogram.{name}.count"),
            ReportValue::U64(histogram.count),
        );
        out.insert(
            format!("telemetry.histogram.{name}.total"),
            ReportValue::U64(histogram.total),
        );
        out.insert(
            format!("telemetry.histogram.{name}.min"),
            ReportValue::U64(histogram.min),
        );
        out.insert(
            format!("telemetry.histogram.{name}.max"),
            ReportValue::U64(histogram.max),
        );
        out.insert(
            format!("telemetry.histogram.{name}.mean"),
            ReportValue::F64(histogram.mean),
        );
    }
}
