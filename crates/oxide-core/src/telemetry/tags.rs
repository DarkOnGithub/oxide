/// Profiling target for memory-mapped file operations.
pub const PROFILE_MMAP: &str = "oxide.profile.mmap";
/// Profiling target for format detection operations.
pub const PROFILE_FORMAT: &str = "oxide.profile.format";
/// Profiling target for buffer pool operations.
pub const PROFILE_BUFFER: &str = "oxide.profile.buffer";
/// Reserved profiling target for worker runtime.
pub const PROFILE_WORKER: &str = "oxide.profile.worker";

/// Global system-level tag shared by all profiling events.
pub const TAG_SYSTEM: &str = "system";
/// Logical tag for mmap subsystem events.
pub const TAG_MMAP: &str = "mmap";
/// Logical tag for format subsystem events.
pub const TAG_FORMAT: &str = "format";
/// Logical tag for buffer subsystem events.
pub const TAG_BUFFER: &str = "buffer";
/// Logical tag for worker subsystem events.
pub const TAG_WORKER: &str = "worker";
/// Logical tag for memory telemetry events.
pub const TAG_MEMORY: &str = "memory";

pub const METRIC_MMAP_OPEN_COUNT: &str = "oxide.mmap.open.count";
pub const METRIC_MMAP_SLICE_COUNT: &str = "oxide.mmap.slice.count";
pub const METRIC_FORMAT_DETECT_COUNT: &str = "oxide.format.detect.count";
pub const METRIC_BUFFER_ACQUIRE_CREATED_COUNT: &str = "oxide.buffer.acquire.created.count";
pub const METRIC_BUFFER_ACQUIRE_RECYCLED_COUNT: &str = "oxide.buffer.acquire.recycled.count";
pub const METRIC_BUFFER_RECYCLE_DROPPED_COUNT: &str = "oxide.buffer.recycle.dropped.count";

pub const METRIC_MMAP_OPEN_LATENCY_US: &str = "oxide.mmap.open.latency_us";
pub const METRIC_MMAP_SLICE_LATENCY_US: &str = "oxide.mmap.slice.latency_us";
pub const METRIC_FORMAT_DETECT_LATENCY_US: &str = "oxide.format.detect.latency_us";
pub const METRIC_WORKER_TASK_LATENCY_US: &str = "oxide.worker.task.latency_us";

pub const METRIC_MEMORY_PROCESS_RSS_BYTES: &str = "oxide.memory.process.rss_bytes";
pub const METRIC_MEMORY_PROCESS_VIRTUAL_BYTES: &str = "oxide.memory.process.virtual_bytes";
pub const METRIC_MEMORY_POOL_ESTIMATED_BYTES: &str = "oxide.memory.pool.estimated_bytes";

pub const METRIC_WORKER_TASK_COUNT: &str = "oxide.worker.task.count";
pub const METRIC_WORKER_QUEUE_DEPTH: &str = "oxide.worker.queue.depth";
pub const METRIC_WORKER_ACTIVE_COUNT: &str = "oxide.worker.active.count";
