use std::time::Duration;

use crate::telemetry;
#[cfg(feature = "profiling")]
use crate::telemetry::profile;
use crate::telemetry::tags;

#[cfg(feature = "profiling")]
const PROFILE_TAG_STACK_WORKER: [&str; 1] = [tags::TAG_WORKER];

/// Telemetry contract for worker runtimes.
///
/// Worker implementations can call these hooks to emit stable metrics
/// and profiling events without depending on a specific backend.
pub trait WorkerTelemetry: Send + Sync {
    fn on_queue_depth(&self, worker_id: usize, depth: usize);
    fn on_runtime_sample(&self, queue_depth: usize, active_tasks: usize) {
        let _ = (queue_depth, active_tasks);
    }
    fn on_task_started(&self, worker_id: usize, task_kind: &str);
    fn on_task_finished(&self, worker_id: usize, task_kind: &str, elapsed: Duration);
    fn on_task_failed(&self, worker_id: usize, task_kind: &str, elapsed: Duration);
    fn on_worker_scratch_ready(&self, _worker_id: usize, _allocated_bytes: usize) {}
    fn on_worker_scratch_sample(&self, _worker_id: usize, _allocated_bytes: usize) {}
}

/// Default telemetry implementation that reports worker metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultWorkerTelemetry;

impl WorkerTelemetry for DefaultWorkerTelemetry {
    fn on_queue_depth(&self, _worker_id: usize, depth: usize) {
        telemetry::set_gauge(tags::METRIC_WORKER_QUEUE_DEPTH, depth as u64);

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "queue_depth",
            "sample",
            0,
            "worker queue depth sampled",
        );
    }

    fn on_task_started(&self, _worker_id: usize, _task_kind: &str) {
        telemetry::add_gauge(tags::METRIC_WORKER_ACTIVE_COUNT, 1);

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "task_start",
            "ok",
            0,
            "worker task started",
        );
    }

    fn on_task_finished(&self, _worker_id: usize, _task_kind: &str, elapsed: Duration) {
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

        telemetry::increment_counter(tags::METRIC_WORKER_TASK_COUNT, 1);
        telemetry::record_histogram(tags::METRIC_WORKER_TASK_LATENCY_US, elapsed_us);
        telemetry::sub_gauge_saturating(tags::METRIC_WORKER_ACTIVE_COUNT, 1);

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "task_finish",
            "ok",
            elapsed_us,
            "worker task finished",
        );
    }

    fn on_task_failed(&self, _worker_id: usize, _task_kind: &str, elapsed: Duration) {
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

        telemetry::increment_counter(tags::METRIC_WORKER_TASK_COUNT, 1);
        telemetry::record_histogram(tags::METRIC_WORKER_TASK_LATENCY_US, elapsed_us);
        telemetry::sub_gauge_saturating(tags::METRIC_WORKER_ACTIVE_COUNT, 1);

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "task_finish",
            "error",
            elapsed_us,
            "worker task failed",
        );
    }

    fn on_runtime_sample(&self, queue_depth: usize, active_tasks: usize) {
        telemetry::set_gauge(tags::METRIC_WORKER_QUEUE_DEPTH, queue_depth as u64);
        telemetry::set_gauge(tags::METRIC_WORKER_ACTIVE_COUNT, active_tasks as u64);

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "runtime_sample",
            "sample",
            0,
            "worker runtime sampled",
        );
    }

    fn on_worker_scratch_ready(&self, _worker_id: usize, _allocated_bytes: usize) {}

    fn on_worker_scratch_sample(&self, _worker_id: usize, _allocated_bytes: usize) {}
}
