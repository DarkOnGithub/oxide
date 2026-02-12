use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crossbeam_deque::{Injector, Steal, Stealer, Worker as DequeWorker};

/// Lock-free work queue with a global injector and per-worker local queues.
pub struct WorkStealingQueue<T> {
    global: Injector<T>,
    local_workers: Vec<Mutex<Option<DequeWorker<T>>>>,
    stealers: Vec<Stealer<T>>,
    pending: AtomicUsize,
    wait_mutex: Mutex<()>,
    wait_condvar: Condvar,
}

impl<T> WorkStealingQueue<T> {
    /// Creates a new queue with one local LIFO queue per worker.
    pub fn new(num_workers: usize) -> Self {
        let worker_count = num_workers.max(1);

        let mut local_workers = Vec::with_capacity(worker_count);
        let mut stealers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let worker = DequeWorker::new_lifo();
            stealers.push(worker.stealer());
            local_workers.push(Mutex::new(Some(worker)));
        }

        Self {
            global: Injector::new(),
            local_workers,
            stealers,
            pending: AtomicUsize::new(0),
            wait_mutex: Mutex::new(()),
            wait_condvar: Condvar::new(),
        }
    }

    /// Number of local workers configured for this queue.
    pub fn worker_count(&self) -> usize {
        self.stealers.len()
    }

    /// Approximate number of queued tasks across global/local queues.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    /// Returns true when no queued tasks remain.
    pub fn is_empty(&self) -> bool {
        self.pending() == 0
    }

    /// Submits work to the global injector queue.
    pub fn submit(&self, item: T) {
        self.global.push(item);
        self.pending.fetch_add(1, Ordering::AcqRel);
        self.wait_condvar.notify_one();
    }

    /// Waits for new work to become available, or times out.
    pub fn wait_for_work(&self, timeout: Duration) {
        if self.pending() > 0 {
            return;
        }

        let guard = self.wait_mutex.lock().expect("wait mutex poisoned");
        if self.pending() > 0 {
            return;
        }

        let _ = self
            .wait_condvar
            .wait_timeout(guard, timeout)
            .expect("wait mutex poisoned");
    }

    /// Wakes all workers that may be waiting for new work.
    pub fn notify_all_waiters(&self) {
        self.wait_condvar.notify_all();
    }

    /// Creates a worker handle for the given worker id.
    ///
    /// A worker id can only be acquired once.
    pub fn worker(self: &Arc<Self>, id: usize) -> Option<WorkStealingWorker<T>> {
        if id >= self.local_workers.len() {
            return None;
        }

        let local = self.local_workers[id]
            .lock()
            .expect("worker mutex poisoned")
            .take()?;

        Some(WorkStealingWorker {
            id,
            local,
            queue: Arc::clone(self),
        })
    }

    fn decrement_pending(&self) {
        let mut current = self.pending.load(Ordering::Acquire);
        while current > 0 {
            match self.pending.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

/// Per-thread worker state for [`WorkStealingQueue`].
pub struct WorkStealingWorker<T> {
    id: usize,
    local: DequeWorker<T>,
    queue: Arc<WorkStealingQueue<T>>,
}

impl<T> WorkStealingWorker<T> {
    /// Returns this worker id.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Pushes a task to the local LIFO queue.
    pub fn push_local(&self, item: T) {
        self.local.push(item);
        self.queue.pending.fetch_add(1, Ordering::AcqRel);
        self.queue.wait_condvar.notify_one();
    }

    /// Approximate local queue depth.
    pub fn local_len(&self) -> usize {
        self.local.len()
    }

    /// Approximate queue depth visible to this worker.
    pub fn queue_depth(&self) -> usize {
        self.local.len().saturating_add(self.queue.global.len())
    }

    /// Fetches one task, preferring local work for cache locality.
    pub fn steal(&self) -> Option<T> {
        if let Some(item) = self.local.pop() {
            self.queue.decrement_pending();
            return Some(item);
        }

        if let Some(item) = self.steal_from_global() {
            self.queue.decrement_pending();
            return Some(item);
        }

        let item = self.steal_from_others()?;
        self.queue.decrement_pending();
        Some(item)
    }

    /// Waits for work to become available for this worker.
    pub fn wait_for_work(&self, timeout: Duration) {
        self.queue.wait_for_work(timeout);
    }

    fn steal_from_global(&self) -> Option<T> {
        self.retry_steal(|| self.queue.global.steal_batch_and_pop(&self.local))
    }

    fn steal_from_others(&self) -> Option<T> {
        let len = self.queue.stealers.len();
        if len <= 1 {
            return None;
        }

        for offset in 1..=len {
            let idx = (self.id + offset) % len;
            if idx == self.id {
                continue;
            }

            if let Some(item) =
                self.retry_steal(|| self.queue.stealers[idx].steal_batch_and_pop(&self.local))
            {
                return Some(item);
            }
        }

        None
    }

    fn retry_steal<F>(&self, mut op: F) -> Option<T>
    where
        F: FnMut() -> Steal<T>,
    {
        loop {
            match op() {
                Steal::Success(item) => return Some(item),
                Steal::Empty => return None,
                Steal::Retry => std::hint::spin_loop(),
            }
        }
    }
}
