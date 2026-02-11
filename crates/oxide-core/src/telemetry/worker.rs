use std::time::Duration;

use crate::telemetry;
#[cfg(feature = "profiling")]
use crate::telemetry::profile;
use crate::telemetry::tags;

#[cfg(feature = "profiling")]
const PROFILE_TAG_STACK_WORKER: [&str; 2] = [tags::TAG_SYSTEM, tags::TAG_WORKER];

/// Telemetry contract for worker runtimes.
///
/// Worker implementations can call these hooks to emit stable metrics
/// and profiling events without depending on a specific backend.
pub trait WorkerTelemetry: Send + Sync {
    fn on_queue_depth(&self, worker_id: usize, depth: usize);
    fn on_task_started(&self, worker_id: usize, task_kind: &str);
    fn on_task_finished(&self, worker_id: usize, task_kind: &str, elapsed: Duration);
    fn on_task_failed(&self, worker_id: usize, task_kind: &str, elapsed: Duration);
}

/// Default telemetry implementation that reports worker metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultWorkerTelemetry;

impl WorkerTelemetry for DefaultWorkerTelemetry {
    fn on_queue_depth(&self, _worker_id: usize, depth: usize) {
        telemetry::increment_counter(
            tags::METRIC_WORKER_QUEUE_DEPTH_SAMPLES,
            1,
            &[("subsystem", "worker"), ("op", "queue_depth")],
        );
        telemetry::set_gauge(
            tags::METRIC_WORKER_QUEUE_DEPTH,
            depth as u64,
            &[("subsystem", "worker"), ("op", "queue_depth")],
        );
        telemetry::record_histogram(
            tags::METRIC_WORKER_QUEUE_DEPTH_HIST,
            depth as u64,
            &[("subsystem", "worker"), ("op", "queue_depth")],
        );

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "queue_depth",
            "sample",
            0,
            "worker queue depth sampled",
        );

        #[cfg(feature = "profiling")]
        if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_WORKER) {
            tracing::debug!(
                target: tags::PROFILE_WORKER,
                op = "queue_depth",
                queue_depth = depth,
                tags = ?PROFILE_TAG_STACK_WORKER,
                "worker queue depth sampled"
            );
        }
    }

    fn on_task_started(&self, _worker_id: usize, _task_kind: &str) {
        telemetry::increment_counter(
            tags::METRIC_WORKER_TASK_START_COUNT,
            1,
            &[("subsystem", "worker"), ("op", "task_start")],
        );
        telemetry::add_gauge(
            tags::METRIC_WORKER_ACTIVE_COUNT,
            1,
            &[("subsystem", "worker"), ("op", "task_start")],
        );

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "task_start",
            "ok",
            0,
            "worker task started",
        );

        #[cfg(feature = "profiling")]
        if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_WORKER) {
            tracing::debug!(
                target: tags::PROFILE_WORKER,
                op = "task_start",
                task_kind = _task_kind,
                tags = ?PROFILE_TAG_STACK_WORKER,
                "worker task started"
            );
        }
    }

    fn on_task_finished(&self, _worker_id: usize, _task_kind: &str, elapsed: Duration) {
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

        telemetry::increment_counter(
            tags::METRIC_WORKER_TASK_FINISH_COUNT,
            1,
            &[
                ("subsystem", "worker"),
                ("op", "task_finish"),
                ("result", "ok"),
            ],
        );
        telemetry::increment_counter(
            tags::METRIC_WORKER_TASK_COUNT,
            1,
            &[("subsystem", "worker"), ("op", "task"), ("result", "ok")],
        );
        telemetry::record_histogram(
            tags::METRIC_WORKER_TASK_LATENCY_US,
            elapsed_us,
            &[("subsystem", "worker"), ("op", "task"), ("result", "ok")],
        );
        telemetry::sub_gauge_saturating(
            tags::METRIC_WORKER_ACTIVE_COUNT,
            1,
            &[("subsystem", "worker"), ("op", "task_finish")],
        );

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "task_finish",
            "ok",
            elapsed_us,
            "worker task finished",
        );

        #[cfg(feature = "profiling")]
        if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_WORKER) {
            tracing::debug!(
                target: tags::PROFILE_WORKER,
                op = "task_finish",
                result = "ok",
                task_kind = _task_kind,
                elapsed_us,
                tags = ?PROFILE_TAG_STACK_WORKER,
                "worker task finished"
            );
        }
    }

    fn on_task_failed(&self, _worker_id: usize, _task_kind: &str, elapsed: Duration) {
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

        telemetry::increment_counter(
            tags::METRIC_WORKER_TASK_FAIL_COUNT,
            1,
            &[
                ("subsystem", "worker"),
                ("op", "task_finish"),
                ("result", "error"),
            ],
        );
        telemetry::increment_counter(
            tags::METRIC_WORKER_TASK_COUNT,
            1,
            &[("subsystem", "worker"), ("op", "task"), ("result", "error")],
        );
        telemetry::record_histogram(
            tags::METRIC_WORKER_TASK_LATENCY_US,
            elapsed_us,
            &[("subsystem", "worker"), ("op", "task"), ("result", "error")],
        );
        telemetry::sub_gauge_saturating(
            tags::METRIC_WORKER_ACTIVE_COUNT,
            1,
            &[("subsystem", "worker"), ("op", "task_failed")],
        );

        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_WORKER,
            &PROFILE_TAG_STACK_WORKER,
            "task_finish",
            "error",
            elapsed_us,
            "worker task failed",
        );

        #[cfg(feature = "profiling")]
        if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_WORKER) {
            tracing::debug!(
                target: tags::PROFILE_WORKER,
                op = "task_finish",
                result = "error",
                task_kind = _task_kind,
                elapsed_us,
                tags = ?PROFILE_TAG_STACK_WORKER,
                "worker task failed"
            );
        }
    }
}
