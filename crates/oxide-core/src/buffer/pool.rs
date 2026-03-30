use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};

#[cfg(feature = "profiling")]
use crate::telemetry::profile;
#[cfg(feature = "profiling")]
use crate::telemetry::tags;

#[cfg(feature = "profiling")]
const PROFILE_TAG_STACK_BUFFER: [&str; 1] = [tags::TAG_BUFFER];

const RECYCLED_BUFFER_CAPACITY_MULTIPLIER: usize = 2;
const MAX_RECYCLED_BUFFER_CAPACITY: usize = 16 * 1024 * 1024;

#[inline]
fn retained_recycle_capacity(default_capacity: usize) -> usize {
    default_capacity
        .saturating_mul(RECYCLED_BUFFER_CAPACITY_MULTIPLIER)
        .min(MAX_RECYCLED_BUFFER_CAPACITY.max(default_capacity))
        .max(default_capacity.max(1))
}

/// A pool of reusable byte buffers to reduce allocation overhead.
///
/// The buffer pool maintains a set of pre-allocated buffers that can be
/// acquired and released back to the pool for reuse. This reduces memory
/// allocation pressure during high-throughput data processing.
///
/// # Example
/// ```
/// use oxide_core::BufferPool;
///
/// let pool = BufferPool::new(4096, 100);
/// let buffer = pool.acquire();
/// // use buffer...
/// drop(buffer); // returns to pool automatically
/// ```
#[derive(Debug)]
pub struct BufferPool {
    recycler: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    default_capacity: usize,
    max_buffers: usize,
    metrics: Arc<PoolMetricsInner>,
}

impl BufferPool {
    /// Creates a new buffer pool with the specified configuration.
    ///
    /// # Arguments
    /// * `default_capacity` - Initial capacity for newly created buffers
    /// * `max_buffers` - Maximum number of buffers to keep in the pool
    pub fn new(default_capacity: usize, max_buffers: usize) -> Self {
        let (tx, rx) = bounded(max_buffers);
        Self {
            recycler: tx,
            receiver: rx,
            default_capacity,
            max_buffers,
            metrics: Arc::new(PoolMetricsInner::default()),
        }
    }

    /// Acquires a buffer from the pool.
    ///
    /// Returns a recycled buffer if available, otherwise creates a new one.
    /// The buffer will be automatically returned to the pool when dropped.
    pub fn acquire(&self) -> PooledBuffer {
        self.acquire_with_capacity(self.default_capacity)
    }

    /// Acquires a buffer with enough capacity for at least `min_capacity` bytes.
    pub fn acquire_with_capacity(&self, min_capacity: usize) -> PooledBuffer {
        let started_at = Instant::now();
        let requested_capacity = min_capacity.max(self.default_capacity).max(1);
        let (result, capacity, buffer) = match self.receiver.try_recv() {
            Ok(mut buffer) => {
                buffer.clear();
                if buffer.capacity() < requested_capacity {
                    buffer.reserve(requested_capacity - buffer.len());
                }
                let capacity = buffer.capacity();
                self.metrics.recycled.fetch_add(1, Ordering::Relaxed);
                ("recycled", capacity, buffer)
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                self.metrics.created.fetch_add(1, Ordering::Relaxed);
                (
                    "created",
                    requested_capacity,
                    Vec::with_capacity(requested_capacity),
                )
            }
        };
        #[cfg(not(feature = "profiling"))]
        let _ = (started_at, result, capacity);
        #[cfg(feature = "profiling")]
        let elapsed_us = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
        #[cfg(feature = "profiling")]
        profile::event(
            tags::PROFILE_BUFFER,
            &PROFILE_TAG_STACK_BUFFER,
            "acquire",
            result,
            elapsed_us,
            "buffer acquire completed",
        );
        #[cfg(feature = "profiling")]
        if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
            tracing::debug!(
                target: tags::PROFILE_BUFFER,
                op = "acquire",
                result,
                elapsed_us,
                tags = ?PROFILE_TAG_STACK_BUFFER,
                buffer_capacity = capacity,
                "buffer acquire completed"
            );
        }

        PooledBuffer::new(buffer, self.recycler.clone(), Arc::clone(&self.metrics))
            .with_recycle_capacity(requested_capacity)
    }

    /// Returns a snapshot of the current pool metrics.
    pub fn metrics(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            created: self.metrics.created.load(Ordering::Relaxed),
            recycled: self.metrics.recycled.load(Ordering::Relaxed),
            dropped: self.metrics.dropped.load(Ordering::Relaxed),
        }
    }

    /// Returns the default capacity for newly created buffers.
    pub fn default_capacity(&self) -> usize {
        self.default_capacity
    }

    /// Returns the maximum number of buffers the pool can hold.
    pub fn max_buffers(&self) -> usize {
        self.max_buffers
    }
}

/// A snapshot of buffer pool metrics at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolMetricsSnapshot {
    /// Number of buffers created by the pool
    pub created: usize,
    /// Number of buffers successfully recycled
    pub recycled: usize,
    /// Number of buffers dropped (pool full)
    pub dropped: usize,
}

#[derive(Debug, Default)]
struct PoolMetricsInner {
    created: AtomicUsize,
    recycled: AtomicUsize,
    dropped: AtomicUsize,
}

/// A buffer allocated from a [`BufferPool`].
///
/// When dropped, this buffer is automatically returned to the pool
/// for reuse. Implements `Deref` and `DerefMut` for transparent
/// access to the underlying `Vec<u8>`.
#[derive(Debug)]
pub struct PooledBuffer {
    buffer: Vec<u8>,
    recycler: Sender<Vec<u8>>,
    metrics: Arc<PoolMetricsInner>,
    recycle_capacity: usize,
}

impl PooledBuffer {
    fn new(buffer: Vec<u8>, recycler: Sender<Vec<u8>>, metrics: Arc<PoolMetricsInner>) -> Self {
        Self {
            buffer,
            recycler,
            metrics,
            recycle_capacity: 0,
        }
    }

    fn with_recycle_capacity(mut self, recycle_capacity: usize) -> Self {
        self.recycle_capacity = recycle_capacity.max(1);
        self
    }

    /// Returns a slice reference to the buffer contents.
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    /// Returns a mutable reference to the underlying Vec.
    pub fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }
}

impl Deref for PooledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        #[cfg(feature = "profiling")]
        let started_at = Instant::now();
        let mut buffer = std::mem::take(&mut self.buffer);
        let retained_capacity = retained_recycle_capacity(self.recycle_capacity.max(1));
        buffer.clear();
        if buffer.capacity() > retained_capacity {
            buffer.shrink_to(retained_capacity);
        }
        #[cfg(feature = "profiling")]
        let capacity = buffer.capacity();
        if let Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) =
            self.recycler.try_send(buffer)
        {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "profiling")]
            let elapsed_us = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
            #[cfg(feature = "profiling")]
            profile::event(
                tags::PROFILE_BUFFER,
                &PROFILE_TAG_STACK_BUFFER,
                "recycle",
                "dropped",
                elapsed_us,
                "buffer recycle dropped",
            );
            #[cfg(feature = "profiling")]
            if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
                tracing::debug!(
                    target: tags::PROFILE_BUFFER,
                    op = "recycle",
                    result = "dropped",
                    elapsed_us,
                    tags = ?PROFILE_TAG_STACK_BUFFER,
                    buffer_capacity = capacity,
                    "buffer dropped instead of recycled"
                );
            }
        } else {
            #[cfg(feature = "profiling")]
            let elapsed_us = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
            #[cfg(feature = "profiling")]
            profile::event(
                tags::PROFILE_BUFFER,
                &PROFILE_TAG_STACK_BUFFER,
                "recycle",
                "recycled",
                elapsed_us,
                "buffer recycled to pool",
            );
            #[cfg(feature = "profiling")]
            if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
                tracing::debug!(
                    target: tags::PROFILE_BUFFER,
                    op = "recycle",
                    result = "recycled",
                    elapsed_us,
                    tags = ?PROFILE_TAG_STACK_BUFFER,
                    buffer_capacity = capacity,
                    "buffer recycled to pool"
                );
            }
        }
    }
}

#[cfg(test)]
#[path = "../../tests/buffer/pool.rs"]
mod tests;
