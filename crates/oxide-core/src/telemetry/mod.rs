use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod events;
pub mod memory;
pub mod profile;
pub mod report;
pub mod tags;
pub mod worker;

pub use events::{
    ArchiveProgressEvent, ExtractProgressEvent, GlobalTelemetrySink, ProfileEvent, TelemetryEvent,
    TelemetrySink,
};
pub use memory::ProcessMemorySample;
pub use report::{
    ArchiveReport, ArchiveRun, ExtractReport, ReportExport, ReportValue, RunReport,
    RunTelemetryOptions, ThreadReport, WorkerReport,
};

/// Histogram summary captured in telemetry snapshots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct HistogramSnapshot {
    /// Total number of samples recorded.
    pub count: u64,
    /// Sum of all sample values.
    pub total: u64,
    /// Minimum sample value observed.
    pub min: u64,
    /// Maximum sample value observed.
    pub max: u64,
    /// Arithmetic mean of all sample values.
    pub mean: f64,
}

/// In-memory view of collected telemetry metrics.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    /// Map of counter names to their current values.
    pub counters: BTreeMap<String, u64>,
    /// Map of gauge names to their current values.
    pub gauges: BTreeMap<String, u64>,
    /// Map of histogram names to their summarized snapshots.
    pub histograms: BTreeMap<String, HistogramSnapshot>,
}

impl TelemetrySnapshot {
    /// Returns the value of a counter if it exists.
    pub fn counter(&self, name: &str) -> Option<u64> {
        self.counters.get(name).copied()
    }

    /// Returns the value of a gauge if it exists.
    pub fn gauge(&self, name: &str) -> Option<u64> {
        self.gauges.get(name).copied()
    }

    /// Returns the snapshot of a histogram if it exists.
    pub fn histogram(&self, name: &str) -> Option<HistogramSnapshot> {
        self.histograms.get(name).copied()
    }
}

/// Increments a named counter by `value`.
///
/// Labels are currently unused in the internal registry but preserved for API compatibility.
#[inline]
pub fn increment_counter(name: &'static str, value: u64, _labels: &[(&str, &str)]) {
    #[cfg(feature = "telemetry")]
    registry::increment_counter(name, value);

    let _ = (name, value);
}

/// Records a histogram sample.
#[inline]
pub fn record_histogram(name: &'static str, value: u64, _labels: &[(&str, &str)]) {
    #[cfg(feature = "telemetry")]
    registry::record_histogram(name, value);

    let _ = (name, value);
}

/// Sets a gauge to an absolute value.
#[inline]
pub fn set_gauge(name: &'static str, value: u64, _labels: &[(&str, &str)]) {
    #[cfg(feature = "telemetry")]
    registry::set_gauge(name, value);

    let _ = (name, value);
}

/// Adds `delta` to a gauge.
#[inline]
pub fn add_gauge(name: &'static str, delta: u64, _labels: &[(&str, &str)]) {
    #[cfg(feature = "telemetry")]
    registry::add_gauge(name, delta);

    let _ = (name, delta);
}

/// Subtracts `delta` from a gauge with floor at zero.
#[inline]
pub fn sub_gauge_saturating(name: &'static str, delta: u64, _labels: &[(&str, &str)]) {
    #[cfg(feature = "telemetry")]
    registry::sub_gauge_saturating(name, delta);

    let _ = (name, delta);
}

/// Captures current process memory and updates memory gauges.
pub fn sample_process_memory() -> ProcessMemorySample {
    let sample = memory::sample_process_memory();

    if let Some(rss) = sample.rss_bytes {
        set_gauge(
            tags::METRIC_MEMORY_PROCESS_RSS_BYTES,
            rss,
            &[("subsystem", "memory"), ("op", "sample")],
        );
    }
    if let Some(virtual_bytes) = sample.virtual_bytes {
        set_gauge(
            tags::METRIC_MEMORY_PROCESS_VIRTUAL_BYTES,
            virtual_bytes,
            &[("subsystem", "memory"), ("op", "sample")],
        );
    }

    sample
}

/// Returns a point-in-time snapshot of all collected telemetry.
pub fn snapshot() -> TelemetrySnapshot {
    #[cfg(feature = "telemetry")]
    {
        return registry::snapshot();
    }

    #[cfg(not(feature = "telemetry"))]
    {
        TelemetrySnapshot::default()
    }
}

/// Clears in-memory telemetry state.
pub fn reset() {
    #[cfg(feature = "telemetry")]
    registry::reset();
}

#[cfg(feature = "telemetry")]
mod registry {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, OnceLock};

    use super::{HistogramSnapshot, TelemetrySnapshot};

    #[derive(Debug, Clone, Copy, Default)]
    struct HistogramAggregate {
        count: u64,
        total: u64,
        min: u64,
        max: u64,
    }

    impl HistogramAggregate {
        fn record(&mut self, value: u64) {
            if self.count == 0 {
                self.min = value;
                self.max = value;
            } else {
                self.min = self.min.min(value);
                self.max = self.max.max(value);
            }

            self.count = self.count.saturating_add(1);
            self.total = self.total.saturating_add(value);
        }

        fn snapshot(&self) -> HistogramSnapshot {
            let mean = if self.count == 0 {
                0.0
            } else {
                self.total as f64 / self.count as f64
            };

            HistogramSnapshot {
                count: self.count,
                total: self.total,
                min: self.min,
                max: self.max,
                mean,
            }
        }
    }

    #[derive(Default)]
    struct Store {
        counters: Mutex<BTreeMap<&'static str, u64>>,
        gauges: Mutex<BTreeMap<&'static str, u64>>,
        histograms: Mutex<BTreeMap<&'static str, HistogramAggregate>>,
    }

    fn store() -> &'static Store {
        static STORE: OnceLock<Store> = OnceLock::new();
        STORE.get_or_init(Store::default)
    }

    fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(super) fn increment_counter(name: &'static str, value: u64) {
        let mut counters = lock_unpoisoned(&store().counters);
        let entry = counters.entry(name).or_insert(0);
        *entry = entry.saturating_add(value);
    }

    pub(super) fn record_histogram(name: &'static str, value: u64) {
        let mut histograms = lock_unpoisoned(&store().histograms);
        let entry = histograms.entry(name).or_default();
        entry.record(value);
    }

    pub(super) fn set_gauge(name: &'static str, value: u64) {
        let mut gauges = lock_unpoisoned(&store().gauges);
        gauges.insert(name, value);
    }

    pub(super) fn add_gauge(name: &'static str, delta: u64) {
        let mut gauges = lock_unpoisoned(&store().gauges);
        let entry = gauges.entry(name).or_insert(0);
        *entry = entry.saturating_add(delta);
    }

    pub(super) fn sub_gauge_saturating(name: &'static str, delta: u64) {
        let mut gauges = lock_unpoisoned(&store().gauges);
        let entry = gauges.entry(name).or_insert(0);
        *entry = entry.saturating_sub(delta);
    }

    pub(super) fn snapshot() -> TelemetrySnapshot {
        let counters = lock_unpoisoned(&store().counters)
            .iter()
            .map(|(name, value)| ((*name).to_owned(), *value))
            .collect();

        let gauges = lock_unpoisoned(&store().gauges)
            .iter()
            .map(|(name, value)| ((*name).to_owned(), *value))
            .collect();

        let histograms = lock_unpoisoned(&store().histograms)
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.snapshot()))
            .collect();

        TelemetrySnapshot {
            counters,
            gauges,
            histograms,
        }
    }

    pub(super) fn reset() {
        lock_unpoisoned(&store().counters).clear();
        lock_unpoisoned(&store().gauges).clear();
        lock_unpoisoned(&store().histograms).clear();
    }
}
