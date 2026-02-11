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

/// Global system-level tag shared by all profiling events.
pub const TAG_SYSTEM: &str = "system";
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
/// Logical tag for memory telemetry events.
pub const TAG_MEMORY: &str = "memory";

pub const METRIC_MMAP_OPEN_COUNT: &str = "oxide.mmap.open.count";
pub const METRIC_MMAP_SLICE_COUNT: &str = "oxide.mmap.slice.count";
pub const METRIC_FORMAT_DETECT_COUNT: &str = "oxide.format.detect.count";
pub const METRIC_BUFFER_ACQUIRE_CREATED_COUNT: &str = "oxide.buffer.acquire.created.count";
pub const METRIC_BUFFER_ACQUIRE_RECYCLED_COUNT: &str = "oxide.buffer.acquire.recycled.count";
pub const METRIC_BUFFER_RECYCLE_OK_COUNT: &str = "oxide.buffer.recycle.ok.count";
pub const METRIC_BUFFER_RECYCLE_DROPPED_COUNT: &str = "oxide.buffer.recycle.dropped.count";
pub const METRIC_SCANNER_SCAN_COUNT: &str = "oxide.scanner.scan.count";
pub const METRIC_SCANNER_MODE_TEXT_COUNT: &str = "oxide.scanner.mode.text.count";
pub const METRIC_SCANNER_MODE_IMAGE_COUNT: &str = "oxide.scanner.mode.image.count";
pub const METRIC_SCANNER_MODE_AUDIO_COUNT: &str = "oxide.scanner.mode.audio.count";
pub const METRIC_SCANNER_MODE_RAW_COUNT: &str = "oxide.scanner.mode.raw.count";
pub const METRIC_SCANNER_FALLBACK_COUNT: &str = "oxide.scanner.fallback.count";
pub const METRIC_WORKER_TASK_START_COUNT: &str = "oxide.worker.task.start.count";
pub const METRIC_WORKER_TASK_FINISH_COUNT: &str = "oxide.worker.task.finish.count";
pub const METRIC_WORKER_TASK_FAIL_COUNT: &str = "oxide.worker.task.fail.count";
pub const METRIC_WORKER_QUEUE_DEPTH_SAMPLES: &str = "oxide.worker.queue.depth.samples";
pub const METRIC_PIPELINE_ARCHIVE_RUN_COUNT: &str = "oxide.pipeline.archive.run.count";
pub const METRIC_PIPELINE_EXTRACT_RUN_COUNT: &str = "oxide.pipeline.extract.run.count";

pub const METRIC_MMAP_OPEN_LATENCY_US: &str = "oxide.mmap.open.latency_us";
pub const METRIC_MMAP_SLICE_LATENCY_US: &str = "oxide.mmap.slice.latency_us";
pub const METRIC_FORMAT_DETECT_LATENCY_US: &str = "oxide.format.detect.latency_us";
pub const METRIC_BUFFER_ACQUIRE_LATENCY_US: &str = "oxide.buffer.acquire.latency_us";
pub const METRIC_BUFFER_RECYCLE_LATENCY_US: &str = "oxide.buffer.recycle.latency_us";
pub const METRIC_SCANNER_SCAN_LATENCY_US: &str = "oxide.scanner.scan.latency_us";
pub const METRIC_WORKER_TASK_LATENCY_US: &str = "oxide.worker.task.latency_us";
pub const METRIC_WORKER_QUEUE_DEPTH_HIST: &str = "oxide.worker.queue.depth.hist";
pub const METRIC_PIPELINE_ARCHIVE_RUN_LATENCY_US: &str = "oxide.pipeline.archive.run.latency_us";
pub const METRIC_PIPELINE_EXTRACT_RUN_LATENCY_US: &str = "oxide.pipeline.extract.run.latency_us";
pub const METRIC_PIPELINE_STAGE_DISCOVERY_US: &str = "oxide.pipeline.stage.discovery.us";
pub const METRIC_PIPELINE_STAGE_FORMAT_PROBE_US: &str = "oxide.pipeline.stage.format_probe.us";
pub const METRIC_PIPELINE_STAGE_PRODUCER_READ_US: &str = "oxide.pipeline.stage.producer_read.us";
pub const METRIC_PIPELINE_STAGE_SUBMIT_WAIT_US: &str = "oxide.pipeline.stage.submit_wait.us";
pub const METRIC_PIPELINE_STAGE_RESULT_WAIT_US: &str = "oxide.pipeline.stage.result_wait.us";
pub const METRIC_PIPELINE_STAGE_WRITER_US: &str = "oxide.pipeline.stage.writer.us";
pub const METRIC_PIPELINE_STAGE_ARCHIVE_READ_US: &str = "oxide.pipeline.stage.archive_read.us";
pub const METRIC_PIPELINE_STAGE_DECODE_SUBMIT_US: &str = "oxide.pipeline.stage.decode_submit.us";
pub const METRIC_PIPELINE_STAGE_DECODE_WAIT_US: &str = "oxide.pipeline.stage.decode_wait.us";
pub const METRIC_PIPELINE_STAGE_MERGE_US: &str = "oxide.pipeline.stage.merge.us";
pub const METRIC_PIPELINE_STAGE_DIRECTORY_DECODE_US: &str =
    "oxide.pipeline.stage.directory_decode.us";
pub const METRIC_PIPELINE_STAGE_OUTPUT_WRITE_US: &str = "oxide.pipeline.stage.output_write.us";

pub const METRIC_MEMORY_PROCESS_RSS_BYTES: &str = "oxide.memory.process.rss_bytes";
pub const METRIC_MEMORY_PROCESS_VIRTUAL_BYTES: &str = "oxide.memory.process.virtual_bytes";
pub const METRIC_MEMORY_POOL_ESTIMATED_BYTES: &str = "oxide.memory.pool.estimated_bytes";

pub const METRIC_WORKER_TASK_COUNT: &str = "oxide.worker.task.count";
pub const METRIC_WORKER_QUEUE_DEPTH: &str = "oxide.worker.queue.depth";
pub const METRIC_WORKER_ACTIVE_COUNT: &str = "oxide.worker.active.count";
