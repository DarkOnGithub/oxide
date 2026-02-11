use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::core::PoolRuntimeSnapshot;
use crate::pipeline::ArchiveSourceKind;

use super::report::{ArchiveReport, ExtractReport};

#[derive(Debug, Clone)]
pub struct ArchiveProgressEvent {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub input_bytes_total: u64,
    pub input_bytes_completed: u64,
    pub output_bytes_completed: u64,
    pub read_avg_bps: f64,
    pub write_avg_bps: f64,
    pub output_input_ratio: f64,
    pub compression_ratio: f64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub blocks_pending: u32,
    pub runtime: PoolRuntimeSnapshot,
}

#[derive(Debug, Clone)]
pub struct ExtractProgressEvent {
    pub source_kind: ArchiveSourceKind,
    pub elapsed: Duration,
    pub archive_bytes_completed: u64,
    pub decoded_bytes_completed: u64,
    pub read_avg_bps: f64,
    pub decode_avg_bps: f64,
    pub decode_archive_ratio: f64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub runtime: PoolRuntimeSnapshot,
}

#[derive(Debug, Clone)]
pub struct ProfileEvent {
    pub target: &'static str,
    pub op: &'static str,
    pub result: &'static str,
    pub elapsed_us: u64,
    pub tags: Vec<String>,
    pub message: &'static str,
}

#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    ArchiveProgress(ArchiveProgressEvent),
    ExtractProgress(ExtractProgressEvent),
    ArchiveCompleted(ArchiveReport),
    ExtractCompleted(ExtractReport),
    Profile(ProfileEvent),
}

pub trait TelemetrySink {
    fn on_event(&mut self, event: TelemetryEvent);
}

pub trait GlobalTelemetrySink: Send {
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
