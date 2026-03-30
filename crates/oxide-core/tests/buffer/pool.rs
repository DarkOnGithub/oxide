use super::{retained_recycle_capacity, BufferPool, MAX_RECYCLED_BUFFER_CAPACITY};

#[test]
fn recycle_capacity_scales_from_default_capacity() {
    assert_eq!(retained_recycle_capacity(1024 * 1024), 2 * 1024 * 1024);
    assert_eq!(retained_recycle_capacity(8 * 1024 * 1024), 16 * 1024 * 1024);
    assert_eq!(
        retained_recycle_capacity(32 * 1024 * 1024),
        32 * 1024 * 1024
    );
    assert_eq!(retained_recycle_capacity(0), 1);
}

#[test]
fn recycled_buffers_are_shrunk_before_reuse() {
    let pool = BufferPool::new(1024 * 1024, 1);
    let mut buffer = pool.acquire();
    buffer.reserve(MAX_RECYCLED_BUFFER_CAPACITY * 2);
    drop(buffer);

    let recycled = pool.acquire();
    assert!(recycled.capacity() <= retained_recycle_capacity(1024 * 1024));
}

#[test]
fn acquire_with_capacity_grows_recycled_buffers_to_requested_size() {
    let pool = BufferPool::new(1024, 1);
    drop(pool.acquire());

    let recycled = pool.acquire_with_capacity(16 * 1024);
    assert!(recycled.capacity() >= 16 * 1024);
}
