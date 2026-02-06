use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};

#[derive(Debug)]
pub struct BufferPool {
    recycler: Sender<Vec<u8>>,
    receiver: Receiver<Vec<u8>>,
    default_capacity: usize,
    max_buffers: usize,
    metrics: Arc<PoolMetricsInner>,
}

impl BufferPool {
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

    pub fn acquire(&self) -> PooledBuffer {
        match self.receiver.try_recv() {
            Ok(mut buffer) => {
                buffer.clear();
                self.metrics.recycled.fetch_add(1, Ordering::Relaxed);
                PooledBuffer::new(buffer, self.recycler.clone(), Arc::clone(&self.metrics))
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {
                self.metrics.created.fetch_add(1, Ordering::Relaxed);
                PooledBuffer::new(
                    Vec::with_capacity(self.default_capacity),
                    self.recycler.clone(),
                    Arc::clone(&self.metrics),
                )
            }
        }
    }

    pub fn metrics(&self) -> PoolMetricsSnapshot {
        PoolMetricsSnapshot {
            created: self.metrics.created.load(Ordering::Relaxed),
            recycled: self.metrics.recycled.load(Ordering::Relaxed),
            dropped: self.metrics.dropped.load(Ordering::Relaxed),
        }
    }

    pub fn default_capacity(&self) -> usize {
        self.default_capacity
    }

    pub fn max_buffers(&self) -> usize {
        self.max_buffers
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolMetricsSnapshot {
    pub created: usize,
    pub recycled: usize,
    pub dropped: usize,
}

#[derive(Debug, Default)]
struct PoolMetricsInner {
    created: AtomicUsize,
    recycled: AtomicUsize,
    dropped: AtomicUsize,
}

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

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

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
        if let Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) =
            self.recycler.try_send(buffer)
        {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
