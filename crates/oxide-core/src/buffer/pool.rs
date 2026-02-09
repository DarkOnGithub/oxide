use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};

use crate::telemetry;
#[cfg(feature = "profiling")]
use crate::telemetry::profile;
use crate::telemetry::tags;

#[cfg(feature = "profiling")]
const PROFILE_TAG_STACK_BUFFER: [&str; 2] = [tags::TAG_SYSTEM, tags::TAG_BUFFER];

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
        match self.receiver.try_recv() {
            Ok(mut buffer) => {
                let capacity = buffer.capacity();
                buffer.clear();
                self.metrics.recycled.fetch_add(1, Ordering::Relaxed);
                telemetry::increment_counter(
                    tags::METRIC_BUFFER_ACQUIRE_RECYCLED_COUNT,
                    1,
                    &[
                        ("subsystem", "buffer"),
                        ("op", "acquire"),
                        ("result", "recycled"),
                    ],
                );
                telemetry::sub_gauge_saturating(
                    tags::METRIC_MEMORY_POOL_ESTIMATED_BYTES,
                    capacity as u64,
                    &[("subsystem", "buffer"), ("op", "acquire")],
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
                    tracing::debug!(
                        target: tags::PROFILE_BUFFER,
                        op = "acquire",
                        result = "recycled",
                        tags = ?PROFILE_TAG_STACK_BUFFER,
                        buffer_capacity = capacity,
                        "buffer recycled from pool"
                    );
                }
                PooledBuffer::new(buffer, self.recycler.clone(), Arc::clone(&self.metrics))
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                self.metrics.created.fetch_add(1, Ordering::Relaxed);
                telemetry::increment_counter(
                    tags::METRIC_BUFFER_ACQUIRE_CREATED_COUNT,
                    1,
                    &[
                        ("subsystem", "buffer"),
                        ("op", "acquire"),
                        ("result", "created"),
                    ],
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
                    tracing::debug!(
                        target: tags::PROFILE_BUFFER,
                        op = "acquire",
                        result = "created",
                        tags = ?PROFILE_TAG_STACK_BUFFER,
                        buffer_capacity = self.default_capacity,
                        "buffer created for pool"
                    );
                }
                PooledBuffer::new(
                    Vec::with_capacity(self.default_capacity),
                    self.recycler.clone(),
                    Arc::clone(&self.metrics),
                )
            }
        }
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
}

impl PooledBuffer {
    fn new(buffer: Vec<u8>, recycler: Sender<Vec<u8>>, metrics: Arc<PoolMetricsInner>) -> Self {
        Self {
            buffer,
            recycler,
            metrics,
        }
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
        let buffer = std::mem::take(&mut self.buffer);
        let capacity = buffer.capacity();
        if let Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) =
            self.recycler.try_send(buffer)
        {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            telemetry::increment_counter(
                tags::METRIC_BUFFER_RECYCLE_DROPPED_COUNT,
                1,
                &[
                    ("subsystem", "buffer"),
                    ("op", "recycle"),
                    ("result", "dropped"),
                ],
            );
            #[cfg(feature = "profiling")]
            if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
                tracing::debug!(
                    target: tags::PROFILE_BUFFER,
                    op = "recycle",
                    result = "dropped",
                    tags = ?PROFILE_TAG_STACK_BUFFER,
                    buffer_capacity = capacity,
                    "buffer dropped instead of recycled"
                );
            }
        } else {
            telemetry::add_gauge(
                tags::METRIC_MEMORY_POOL_ESTIMATED_BYTES,
                capacity as u64,
                &[("subsystem", "buffer"), ("op", "recycle")],
            );
            #[cfg(feature = "profiling")]
            if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_BUFFER) {
                tracing::debug!(
                    target: tags::PROFILE_BUFFER,
                    op = "recycle",
                    result = "recycled",
                    tags = ?PROFILE_TAG_STACK_BUFFER,
                    buffer_capacity = capacity,
                    "buffer recycled to pool"
                );
            }
        }
    }
}
