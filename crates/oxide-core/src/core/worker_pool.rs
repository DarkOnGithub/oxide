use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};

use crate::OxideError;
use crate::buffer::BufferPool;
use crate::core::work_stealing::{WorkStealingQueue, WorkStealingWorker};
use crate::telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
use crate::types::{Batch, CompressedBlock, CompressionAlgo, Result};

/// Parallel worker pool backed by a work-stealing queue.
pub struct WorkerPool {
    num_workers: usize,
    queue: Arc<WorkStealingQueue<Batch>>,
    buffer_pool: Arc<BufferPool>,
    compression_algo: CompressionAlgo,
    telemetry: Arc<dyn WorkerTelemetry>,
}

impl WorkerPool {
    /// Creates a worker pool using the default worker telemetry implementation.
    pub fn new(
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
    ) -> Self {
        Self::with_telemetry(
            num_workers,
            buffer_pool,
            compression_algo,
            Arc::new(DefaultWorkerTelemetry),
        )
    }

    /// Creates a worker pool with a custom telemetry backend.
    pub fn with_telemetry(
        num_workers: usize,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
        telemetry: Arc<dyn WorkerTelemetry>,
    ) -> Self {
        let workers = num_workers.max(1);
        Self {
            num_workers: workers,
            queue: Arc::new(WorkStealingQueue::new(workers)),
            buffer_pool,
            compression_algo,
            telemetry,
        }
    }

    /// Number of workers configured in this pool.
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Spawns worker threads and returns a handle for submission and collection.
    pub fn spawn<F>(&self, processor: F) -> WorkerPoolHandle
    where
        F: Fn(usize, Batch, &BufferPool, CompressionAlgo) -> Result<CompressedBlock>
            + Send
            + Sync
            + 'static,
    {
        let (results_tx, results_rx) = unbounded();
        let state = Arc::new(WorkerPoolState::new(
            Arc::clone(&self.queue),
            Arc::clone(&self.buffer_pool),
            self.compression_algo,
            Arc::clone(&self.telemetry),
            self.num_workers,
        ));
        let processor = Arc::new(processor);

        let mut worker_handles = Vec::with_capacity(self.num_workers);
        for worker_id in 0..self.num_workers {
            let worker = state
                .queue
                .worker(worker_id)
                .expect("worker already acquired; spawn can only be called once per pool");
            let worker_state = Arc::clone(&state);
            let worker_tx = results_tx.clone();
            let worker_processor = Arc::clone(&processor);

            let handle = thread::spawn(move || {
                run_worker_loop(worker, worker_state, worker_processor, worker_tx);
            });
            worker_handles.push(handle);
        }

        drop(results_tx);

        WorkerPoolHandle {
            state,
            results_rx,
            worker_handles,
        }
    }
}

struct WorkerPoolState {
    queue: Arc<WorkStealingQueue<Batch>>,
    buffer_pool: Arc<BufferPool>,
    compression_algo: CompressionAlgo,
    telemetry: Arc<dyn WorkerTelemetry>,
    started_at: Instant,
    accepting: AtomicBool,
    shutdown_requested: AtomicBool,
    submitted: AtomicUsize,
    completed: AtomicUsize,
    task_counts: Vec<AtomicUsize>,
    worker_started_offsets_us: Vec<AtomicU64>,
    worker_stopped_offsets_us: Vec<AtomicU64>,
    worker_busy_us: Vec<AtomicU64>,
}

impl WorkerPoolState {
    fn new(
        queue: Arc<WorkStealingQueue<Batch>>,
        buffer_pool: Arc<BufferPool>,
        compression_algo: CompressionAlgo,
        telemetry: Arc<dyn WorkerTelemetry>,
        num_workers: usize,
    ) -> Self {
        let task_counts = (0..num_workers).map(|_| AtomicUsize::new(0)).collect();
        let worker_started_offsets_us = (0..num_workers).map(|_| AtomicU64::new(0)).collect();
        let worker_stopped_offsets_us = (0..num_workers).map(|_| AtomicU64::new(0)).collect();
        let worker_busy_us = (0..num_workers).map(|_| AtomicU64::new(0)).collect();
        Self {
            queue,
            buffer_pool,
            compression_algo,
            telemetry,
            started_at: Instant::now(),
            accepting: AtomicBool::new(true),
            shutdown_requested: AtomicBool::new(false),
            submitted: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            task_counts,
            worker_started_offsets_us,
            worker_stopped_offsets_us,
            worker_busy_us,
        }
    }

    fn should_shutdown(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
            && self.completed.load(Ordering::Acquire) >= self.submitted.load(Ordering::Acquire)
    }
}

/// Per-worker runtime metrics captured by the worker pool.
#[derive(Debug, Clone)]
pub struct WorkerRuntimeSnapshot {
    pub worker_id: usize,
    pub tasks_completed: usize,
    pub uptime: Duration,
    pub busy: Duration,
    pub idle: Duration,
    pub utilization: f64,
}

/// Runtime metrics snapshot for the worker pool.
#[derive(Debug, Clone)]
pub struct PoolRuntimeSnapshot {
    pub elapsed: Duration,
    pub submitted: usize,
    pub completed: usize,
    pub pending: usize,
    pub workers: Vec<WorkerRuntimeSnapshot>,
}

/// Runtime handle for a spawned worker pool.
pub struct WorkerPoolHandle {
    state: Arc<WorkerPoolState>,
    results_rx: Receiver<Result<CompressedBlock>>,
    worker_handles: Vec<JoinHandle<()>>,
}

impl WorkerPoolHandle {
    /// Submits a batch to the worker queue.
    pub fn submit(&self, batch: Batch) -> Result<()> {
        if !self.state.accepting.load(Ordering::Acquire) {
            return Err(OxideError::CompressionError(
                "worker pool is shutting down; no new work accepted".to_string(),
            ));
        }

        self.state.submitted.fetch_add(1, Ordering::AcqRel);
        self.state.queue.submit(batch);
        Ok(())
    }

    /// Stops accepting new tasks and signals workers to exit once drained.
    pub fn shutdown(&self) {
        self.state.accepting.store(false, Ordering::Release);
        self.state.shutdown_requested.store(true, Ordering::Release);
    }

    /// Total submitted task count.
    pub fn submitted_count(&self) -> usize {
        self.state.submitted.load(Ordering::Acquire)
    }

    /// Total completed task count.
    pub fn completed_count(&self) -> usize {
        self.state.completed.load(Ordering::Acquire)
    }

    /// Remaining tasks that have not completed yet.
    pub fn pending_count(&self) -> usize {
        self.submitted_count()
            .saturating_sub(self.completed_count())
    }

    /// Per-worker processed task counts.
    pub fn worker_task_counts(&self) -> Vec<usize> {
        self.state
            .task_counts
            .iter()
            .map(|counter| counter.load(Ordering::Acquire))
            .collect()
    }

    /// Returns runtime metrics for the pool and each worker.
    pub fn runtime_snapshot(&self) -> PoolRuntimeSnapshot {
        let elapsed = self.state.started_at.elapsed();
        let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let submitted = self.submitted_count();
        let completed = self.completed_count();
        let pending = submitted.saturating_sub(completed);

        let mut workers = Vec::with_capacity(self.state.task_counts.len());
        for worker_id in 0..self.state.task_counts.len() {
            let started_raw =
                self.state.worker_started_offsets_us[worker_id].load(Ordering::Acquire);
            let stopped_raw =
                self.state.worker_stopped_offsets_us[worker_id].load(Ordering::Acquire);
            let busy_us_raw = self.state.worker_busy_us[worker_id].load(Ordering::Acquire);

            let start_us = started_raw.saturating_sub(1);
            let stop_us = if stopped_raw == 0 {
                elapsed_us
            } else {
                stopped_raw.saturating_sub(1)
            };
            let uptime_us = if started_raw == 0 {
                0
            } else {
                stop_us.saturating_sub(start_us)
            };
            let busy_us = busy_us_raw.min(uptime_us);
            let idle_us = uptime_us.saturating_sub(busy_us);
            let utilization = if uptime_us == 0 {
                0.0
            } else {
                busy_us as f64 / uptime_us as f64
            };

            workers.push(WorkerRuntimeSnapshot {
                worker_id,
                tasks_completed: self.state.task_counts[worker_id].load(Ordering::Acquire),
                uptime: Duration::from_micros(uptime_us),
                busy: Duration::from_micros(busy_us),
                idle: Duration::from_micros(idle_us),
                utilization,
            });
        }

        PoolRuntimeSnapshot {
            elapsed,
            submitted,
            completed,
            pending,
            workers,
        }
    }

    /// Receives one worker result, waiting up to `timeout`.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<Result<CompressedBlock>> {
        match self.results_rx.recv_timeout(timeout) {
            Ok(result) => Some(result),
            Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => None,
        }
    }

    /// Shuts down the pool, drains all submitted results, and joins workers.
    pub fn finish(mut self) -> Result<Vec<CompressedBlock>> {
        self.shutdown();
        let expected = self.submitted_count();

        let mut blocks = Vec::with_capacity(expected);
        let mut first_error: Option<OxideError> = None;

        for _ in 0..expected {
            match self.results_rx.recv() {
                Ok(Ok(block)) => blocks.push(block),
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(OxideError::CompressionError(
                            "worker result channel closed before all tasks completed".to_string(),
                        ));
                    }
                    break;
                }
            }
        }

        if let Err(join_err) = self.join_workers() {
            if first_error.is_none() {
                first_error = Some(OxideError::CompressionError(join_err));
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            blocks.sort_by_key(|block| block.id);
            Ok(blocks)
        }
    }

    /// Shuts down and joins workers without draining output.
    pub fn join(mut self) -> Result<()> {
        self.shutdown();
        self.join_workers().map_err(OxideError::CompressionError)
    }

    fn join_workers(&mut self) -> std::result::Result<(), String> {
        for handle in self.worker_handles.drain(..) {
            if let Err(payload) = handle.join() {
                let details = if let Some(message) = payload.downcast_ref::<&str>() {
                    (*message).to_string()
                } else if let Some(message) = payload.downcast_ref::<String>() {
                    message.clone()
                } else {
                    "unknown panic payload".to_string()
                };

                return Err(format!("worker thread panicked: {details}"));
            }
        }

        Ok(())
    }
}

fn run_worker_loop<F>(
    worker: WorkStealingWorker<Batch>,
    state: Arc<WorkerPoolState>,
    processor: Arc<F>,
    results_tx: Sender<Result<CompressedBlock>>,
) where
    F: Fn(usize, Batch, &BufferPool, CompressionAlgo) -> Result<CompressedBlock> + Send + Sync,
{
    let worker_started_us = state.started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
    state.worker_started_offsets_us[worker.id()]
        .store(worker_started_us.saturating_add(1), Ordering::Release);

    loop {
        if let Some(batch) = worker.steal() {
            state
                .telemetry
                .on_queue_depth(worker.id(), worker.queue_depth());
            state.telemetry.on_task_started(worker.id(), "compress");
            let started_at = Instant::now();

            let result = match catch_unwind(AssertUnwindSafe(|| {
                processor(
                    worker.id(),
                    batch,
                    &state.buffer_pool,
                    state.compression_algo,
                )
            })) {
                Ok(result) => result,
                Err(_) => Err(OxideError::CompressionError(
                    "worker task panicked while processing batch".to_string(),
                )),
            };

            let elapsed = started_at.elapsed();
            let elapsed_us = elapsed.as_micros().min(u64::MAX as u128) as u64;
            state.worker_busy_us[worker.id()].fetch_add(elapsed_us, Ordering::AcqRel);
            match &result {
                Ok(_) => state
                    .telemetry
                    .on_task_finished(worker.id(), "compress", elapsed),
                Err(_) => state
                    .telemetry
                    .on_task_failed(worker.id(), "compress", elapsed),
            }

            state.completed.fetch_add(1, Ordering::AcqRel);
            state.task_counts[worker.id()].fetch_add(1, Ordering::AcqRel);

            if results_tx.send(result).is_err() {
                break;
            }
            continue;
        }

        state
            .telemetry
            .on_queue_depth(worker.id(), worker.queue_depth());

        if state.should_shutdown() {
            let worker_stopped_us =
                state.started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
            state.worker_stopped_offsets_us[worker.id()]
                .store(worker_stopped_us.saturating_add(1), Ordering::Release);
            break;
        }

        thread::sleep(Duration::from_millis(1));
    }

    let worker_stopped_us = state.started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
    state.worker_stopped_offsets_us[worker.id()]
        .store(worker_stopped_us.saturating_add(1), Ordering::Release);
}
