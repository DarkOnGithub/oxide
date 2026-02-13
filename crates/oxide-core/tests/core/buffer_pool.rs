use std::io;
use std::sync::Arc;
use std::thread;

use oxide_core::BufferPool;

#[test]
fn acquire_recycle_cycle_reuses_buffers() {
    let pool = BufferPool::new(64, 2);
    assert_eq!(pool.default_capacity(), 64);
    assert_eq!(pool.max_buffers(), 2);

    {
        let mut buffer = pool.acquire();
        buffer.extend_from_slice(b"hello");
        assert_eq!(buffer.len(), 5);
    }

    let metrics = pool.metrics();
    assert_eq!(metrics.created, 1);
    assert_eq!(metrics.recycled, 0);
    assert_eq!(metrics.dropped, 0);

    {
        let buffer = pool.acquire();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.capacity() >= 64);
    }

    let metrics = pool.metrics();
    assert_eq!(metrics.created, 1);
    assert_eq!(metrics.recycled, 1);
    assert_eq!(metrics.dropped, 0);
}

#[test]
fn full_pool_counts_dropped_buffer() {
    let pool = BufferPool::new(32, 1);

    let first = pool.acquire();
    let second = pool.acquire();

    drop(first);
    drop(second);

    let metrics = pool.metrics();
    assert_eq!(metrics.created, 2);
    assert_eq!(metrics.dropped, 1);
}

#[test]
fn acquire_is_thread_safe() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Arc::new(BufferPool::new(128, 8));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let pool = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
            for _ in 0..500 {
                let mut buffer = pool.acquire();
                buffer.extend_from_slice(b"oxide");
            }
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| io::Error::other("worker thread panicked"))?;
    }

    let metrics = pool.metrics();
    assert!(metrics.created > 0);
    Ok(())
}
