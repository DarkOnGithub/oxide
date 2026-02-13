use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::core::PoolRuntimeSnapshot;
use crate::pipeline::ArchiveSourceKind;

use super::report::{ArchiveReport, ExtractReport};

/// Event emitted during the archive process to report real-time progress.
#[derive(Debug, Clone)]
pub struct ArchiveProgressEvent {
    /// Type of source being archived.
    pub source_kind: ArchiveSourceKind,
    /// Total time elapsed since the start of the operation.
    pub elapsed: Duration,
    /// Total input bytes to be processed.
    pub input_bytes_total: u64,
    /// Number of input bytes processed so far.
    pub input_bytes_completed: u64,
    /// Number of output bytes written so far.
    pub output_bytes_completed: u64,
    /// Average read throughput in bytes per second.
    pub read_avg_bps: f64,
    /// Average write throughput in bytes per second.
    pub write_avg_bps: f64,
    /// Average preprocessing throughput in bytes per second.
    pub preprocessing_avg_bps: f64,
    /// Average compression throughput in bytes per second.
    pub compression_avg_bps: f64,
    /// Combined preprocessing and compression throughput.
    pub preprocessing_compression_avg_bps: f64,
    /// Preprocessing throughput relative to wall clock time.
    pub preprocessing_wall_avg_bps: f64,
    /// Compression throughput relative to wall clock time.
    pub compression_wall_avg_bps: f64,
    /// Combined throughput relative to wall clock time.
    pub preprocessing_compression_wall_avg_bps: f64,
    /// Ratio of output bytes to processed input bytes.
    pub output_input_ratio: f64,
    /// Compression ratio (input / output).
    pub compression_ratio: f64,
    /// Total number of blocks to process.
    pub blocks_total: u32,
    /// Number of blocks successfully completed.
    pub blocks_completed: u32,
    /// Number of blocks currently in-flight.
    pub blocks_pending: u32,
    /// Snapshot of the worker pool runtime state.
    pub runtime: PoolRuntimeSnapshot,
}

/// Event emitted during the extraction process to report real-time progress.
#[derive(Debug, Clone)]
pub struct ExtractProgressEvent {
    /// Type of source being extracted.
    pub source_kind: ArchiveSourceKind,
    /// Total time elapsed since the start of the operation.
    pub elapsed: Duration,
    /// Number of archive bytes read so far.
    pub archive_bytes_completed: u64,
    /// Number of bytes decoded so far.
    pub decoded_bytes_completed: u64,
    /// Average read throughput in bytes per second.
    pub read_avg_bps: f64,
    /// Average decode throughput in bytes per second.
    pub decode_avg_bps: f64,
    /// Ratio of decoded bytes to archive bytes processed.
    pub decode_archive_ratio: f64,
    /// Total number of blocks in the archive.
    pub blocks_total: u32,
    /// Number of blocks successfully decoded.
    pub blocks_completed: u32,
    /// Snapshot of the worker pool runtime state.
    pub runtime: PoolRuntimeSnapshot,
}

/// Event emitted for detailed profiling of internal operations.
#[derive(Debug, Clone)]
pub struct ProfileEvent {
    /// The subsystem or target being profiled.
    pub target: &'static str,
    /// The specific operation being performed.
    pub op: &'static str,
    /// The result or status of the operation.
    pub result: &'static str,
    /// Time taken for the operation in microseconds.
    pub elapsed_us: u64,
    /// Hierarchical tags associated with the event.
    pub tags: Vec<String>,
    /// Human-readable message or context.
    pub message: &'static str,
}

/// Unified telemetry event enum covering progress, completion, and profiling.
#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    /// Periodic progress update for an archive operation.
    ArchiveProgress(ArchiveProgressEvent),
    /// Periodic progress update for an extract operation.
    ExtractProgress(ExtractProgressEvent),
    /// Final report for a completed archive operation.
    ArchiveCompleted(ArchiveReport),
    /// Final report for a completed extract operation.
    ExtractCompleted(ExtractReport),
    /// Low-level profiling event.
    Profile(ProfileEvent),
}

/// Trait for objects that can consume telemetry events.
pub trait TelemetrySink {
    /// Called when a telemetry event is emitted.
    fn on_event(&mut self, event: TelemetryEvent);
}

/// Thread-safe version of [`TelemetrySink`] for global registration.
pub trait GlobalTelemetrySink: Send {
    /// Called when a telemetry event is emitted.
    fn on_event(&mut self, event: TelemetryEvent);
}

fn global_sink() -> &'static Mutex<Option<Box<dyn GlobalTelemetrySink>>> {
    static GLOBAL_SINK: OnceLock<Mutex<Option<Box<dyn GlobalTelemetrySink>>>> = OnceLock::new();
    GLOBAL_SINK.get_or_init(|| Mutex::new(None))
}

/// Registers a process-wide telemetry event sink used by subsystem-level emitters.
pub fn set_global_sink(sink: Option<Box<dyn GlobalTelemetrySink>>) {
    let mut guard = match global_sink().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = sink;
}

/// Emits an event to the process-wide sink when configured.
pub fn emit_global(event: TelemetryEvent) {
    let mut guard = match global_sink().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if let Some(sink) = guard.as_mut() {
        sink.on_event(event);
    }
}
