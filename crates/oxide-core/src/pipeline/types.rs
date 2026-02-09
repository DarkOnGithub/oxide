use std::collections::BTreeMap;
use std::time::Duration;

use crate::core::{PoolRuntimeSnapshot, WorkerRuntimeSnapshot};

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
    pub output_bytes_completed: u64,
    pub blocks_total: u32,
    pub blocks_completed: u32,
    pub blocks_pending: u32,
    pub runtime: PoolRuntimeSnapshot,
}

/// Extensible metric value attached to archive run stats.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    U64(u64),
    F64(f64),
    Duration(Duration),
    Text(String),
}

/// Runtime options for archive operations.
#[derive(Debug, Clone)]
pub struct ArchiveOptions {
    pub progress_interval: Duration,
    pub emit_final_progress: bool,
}

impl Default for ArchiveOptions {
    fn default() -> Self {
        Self {
            progress_interval: Duration::from_millis(250),
            emit_final_progress: true,
        }
    }
}

/// Full output from archive operations that track run metadata.
#[derive(Debug)]
pub struct ArchiveOutcome<W> {
    pub writer: W,
    pub stats: ArchiveRunStats,
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
    pub extensions: BTreeMap<String, StatValue>,
}

impl ArchiveRunStats {
    pub fn extension(&self, key: &str) -> Option<&StatValue> {
        self.extensions.get(key)
    }

    pub fn extension_u64(&self, key: &str) -> Option<u64> {
        match self.extension(key) {
            Some(StatValue::U64(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn extension_f64(&self, key: &str) -> Option<f64> {
        match self.extension(key) {
            Some(StatValue::F64(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn extension_duration(&self, key: &str) -> Option<Duration> {
        match self.extension(key) {
            Some(StatValue::Duration(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn extension_text(&self, key: &str) -> Option<&str> {
        match self.extension(key) {
            Some(StatValue::Text(value)) => Some(value),
            _ => None,
        }
    }
}

/// Progress hook for archive operations.
pub trait ProgressSink {
    fn on_progress(&mut self, snapshot: ArchiveProgressSnapshot);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopProgress;

impl ProgressSink for NoopProgress {
    fn on_progress(&mut self, _snapshot: ArchiveProgressSnapshot) {}
}

pub(crate) struct FnProgressSink<F> {
    pub(crate) callback: F,
}

impl<F> ProgressSink for FnProgressSink<F>
where
    F: FnMut(ArchiveProgressSnapshot),
{
    fn on_progress(&mut self, snapshot: ArchiveProgressSnapshot) {
        (self.callback)(snapshot);
    }
}
