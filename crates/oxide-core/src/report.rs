use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::core::WorkerRuntimeSnapshot;
use crate::pipeline::{ArchiveRunStats, ArchiveSourceKind, StatValue};
use crate::telemetry::{self, TelemetrySnapshot};

/// Options controlling how reports are built.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportBuildOptions {
    /// Attaches a point-in-time telemetry snapshot to the report.
    pub include_telemetry_snapshot: bool,
}

/// Extensible scalar value used by report exports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReportValue {
    U64(u64),
    F64(f64),
    Duration(Duration),
    Bool(bool),
    Text(String),
}

impl From<&StatValue> for ReportValue {
    fn from(value: &StatValue) -> Self {
        match value {
            StatValue::U64(value) => Self::U64(*value),
            StatValue::F64(value) => Self::F64(*value),
            StatValue::Duration(value) => Self::Duration(*value),
            StatValue::Text(value) => Self::Text(value.clone()),
        }
    }
}

/// Worker-level metrics used in exported reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerReport {
    pub worker_id: usize,
    pub tasks_completed: usize,
    pub uptime: Duration,
    pub busy: Duration,
    pub idle: Duration,
    pub utilization: f64,
    pub extensions: BTreeMap<String, ReportValue>,
}

impl WorkerReport {
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
    pub name: String,
    pub stage_us: BTreeMap<String, u64>,
    pub extensions: BTreeMap<String, ReportValue>,
}

impl ThreadReport {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            stage_us: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }
    }
}

/// Detailed archive report built from run stats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchiveReport {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub input_bytes_total: u64,
    pub output_bytes_total: u64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub read_avg_bps: f64,
    pub write_avg_bps: f64,
    pub output_input_ratio: f64,
    pub workers: Vec<WorkerReport>,
    pub main_thread: ThreadReport,
    pub extensions: BTreeMap<String, ReportValue>,
    pub telemetry: Option<TelemetrySnapshot>,
}

impl ArchiveReport {
    pub fn from_run_stats(stats: &ArchiveRunStats, options: ReportBuildOptions) -> Self {
        let elapsed_secs = stats.elapsed.as_secs_f64().max(1e-6);
        let read_avg_bps = stats.input_bytes_total as f64 / elapsed_secs;
        let write_avg_bps = stats.output_bytes_total as f64 / elapsed_secs;
        let output_input_ratio = if stats.input_bytes_total == 0 {
            1.0
        } else {
            stats.output_bytes_total as f64 / stats.input_bytes_total as f64
        };

        let workers = stats
            .workers
            .iter()
            .map(WorkerReport::from_runtime)
            .collect::<Vec<_>>();
        let extensions = stats
            .extensions
            .iter()
            .map(|(key, value)| (key.clone(), ReportValue::from(value)))
            .collect::<BTreeMap<_, _>>();

        let mut main_thread = ThreadReport::new("main");
        for (key, value) in &stats.extensions {
            if let Some(stage) = key
                .strip_prefix("stage.")
                .and_then(|stage| stage.strip_suffix("_us"))
            {
                if let StatValue::U64(value) = value {
                    main_thread.stage_us.insert(stage.to_string(), *value);
                }
            }
        }

        let telemetry = if options.include_telemetry_snapshot {
            Some(telemetry::snapshot())
        } else {
            None
        };

        Self {
            source_kind: stats.source_kind,
            elapsed: stats.elapsed,
            input_bytes_total: stats.input_bytes_total,
            output_bytes_total: stats.output_bytes_total,
            blocks_total: stats.blocks_total,
            blocks_completed: stats.blocks_completed,
            read_avg_bps,
            write_avg_bps,
            output_input_ratio,
            workers,
            main_thread,
            extensions,
            telemetry,
        }
    }
}

/// Detailed extract report including worker and main-thread timing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtractReport {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub archive_bytes_total: u64,
    pub decoded_bytes_total: u64,
    pub output_bytes_total: u64,
    pub blocks_total: u32,
    pub read_avg_bps: f64,
    pub decode_avg_bps: f64,
    pub output_avg_bps: f64,
    pub output_archive_ratio: f64,
    pub workers: Vec<WorkerReport>,
    pub main_thread: ThreadReport,
    pub extensions: BTreeMap<String, ReportValue>,
    pub telemetry: Option<TelemetrySnapshot>,
}

impl ExtractReport {
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
        options: ReportBuildOptions,
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

/// Unified run report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RunReport {
    Archive(ArchiveReport),
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

impl ArchiveRunStats {
    /// Converts archive run stats into an export-oriented report.
    pub fn to_report(&self, options: ReportBuildOptions) -> ArchiveReport {
        ArchiveReport::from_run_stats(self, options)
    }

    /// Converts archive run stats to a flat export map.
    pub fn to_flat_map(&self, options: ReportBuildOptions) -> BTreeMap<String, ReportValue> {
        self.to_report(options).to_flat_map()
    }
}

fn duration_to_us(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
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
