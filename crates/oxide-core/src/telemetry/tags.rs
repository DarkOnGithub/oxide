/// Profiling target for memory-mapped file operations.
pub const PROFILE_MMAP: &str = "oxide.profile.mmap";
/// Profiling target for format detection operations.
pub const PROFILE_FORMAT: &str = "oxide.profile.format";
/// Profiling target for buffer pool operations.
pub const PROFILE_BUFFER: &str = "oxide.profile.buffer";
/// Reserved profiling target for input scanner operations.
pub const PROFILE_SCANNER: &str = "oxide.profile.scanner";
/// Reserved profiling target for worker runtime.
pub const PROFILE_WORKER: &str = "oxide.profile.worker";
/// Profiling target for pipeline-level operations.
pub const PROFILE_PIPELINE: &str = "oxide.profile.pipeline";
/// Profiling target for OXZ format operations.
pub const PROFILE_OXZ: &str = "oxide.profile.oxz";
/// Logical tag for mmap subsystem events.
pub const TAG_MMAP: &str = "mmap";
/// Logical tag for format subsystem events.
pub const TAG_FORMAT: &str = "format";
/// Logical tag for buffer subsystem events.
pub const TAG_BUFFER: &str = "buffer";
/// Logical tag for scanner subsystem events.
pub const TAG_SCANNER: &str = "scanner";
/// Logical tag for worker subsystem events.
pub const TAG_WORKER: &str = "worker";
/// Logical tag for pipeline subsystem events.
pub const TAG_PIPELINE: &str = "pipeline";
/// Logical tag for OXZ format subsystem events.
pub const TAG_OXZ: &str = "oxz";

/// Counter for successful file scan operations.
pub const METRIC_SCANNER_SCAN_COUNT: &str = "oxide.scanner.scan.count";
/// Counter for times the scanner fell back to raw mode due to errors.
pub const METRIC_SCANNER_FALLBACK_COUNT: &str = "oxide.scanner.fallback.count";
/// Counter for the number of archive operations started.
pub const METRIC_PIPELINE_ARCHIVE_RUN_COUNT: &str = "oxide.pipeline.archive.run.count";
/// Counter for the number of extract operations started.
pub const METRIC_PIPELINE_EXTRACT_RUN_COUNT: &str = "oxide.pipeline.extract.run.count";

/// Histogram for memory map open latency in microseconds.
pub const METRIC_MMAP_OPEN_LATENCY_US: &str = "oxide.mmap.open.latency_us";
/// Histogram for format detection latency in microseconds.
pub const METRIC_FORMAT_DETECT_LATENCY_US: &str = "oxide.format.detect.latency_us";
/// Histogram for file scanning latency in microseconds.
pub const METRIC_SCANNER_SCAN_LATENCY_US: &str = "oxide.scanner.scan.latency_us";
/// Histogram for worker task processing latency in microseconds.
pub const METRIC_WORKER_TASK_LATENCY_US: &str = "oxide.worker.task.latency_us";
/// Histogram for total archive operation latency in microseconds.
pub const METRIC_PIPELINE_ARCHIVE_RUN_LATENCY_US: &str = "oxide.pipeline.archive.run.latency_us";
/// Histogram for total extract operation latency in microseconds.
pub const METRIC_PIPELINE_EXTRACT_RUN_LATENCY_US: &str = "oxide.pipeline.extract.run.latency_us";

/// Gauge for the current process Resident Set Size (RSS) in bytes.
pub const METRIC_MEMORY_PROCESS_RSS_BYTES: &str = "oxide.memory.process.rss_bytes";
/// Gauge for the current process virtual memory size in bytes.
pub const METRIC_MEMORY_PROCESS_VIRTUAL_BYTES: &str = "oxide.memory.process.virtual_bytes";

/// Total count of tasks processed by workers.
pub const METRIC_WORKER_TASK_COUNT: &str = "oxide.worker.task.count";
/// Current depth of the worker queues.
pub const METRIC_WORKER_QUEUE_DEPTH: &str = "oxide.worker.queue.depth";
/// Number of workers currently active.
pub const METRIC_WORKER_ACTIVE_COUNT: &str = "oxide.worker.active.count";
