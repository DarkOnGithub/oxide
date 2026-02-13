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
/// Profiling target for compression operations.
pub const PROFILE_COMPRESSION: &str = "oxide.profile.compression";
/// Profiling target for preprocessing operations.
pub const PROFILE_PREPROCESSING: &str = "oxide.profile.preprocessing";
/// Profiling target for OXZ format operations.
pub const PROFILE_OXZ: &str = "oxide.profile.oxz";

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
/// Logical tag for compression subsystem events.
pub const TAG_COMPRESSION: &str = "compression";
/// Logical tag for preprocessing subsystem events.
pub const TAG_PREPROCESSING: &str = "preprocessing";
/// Logical tag for OXZ format subsystem events.
pub const TAG_OXZ: &str = "oxz";

/// Counter for the number of times a memory map was successfully opened.
pub const METRIC_MMAP_OPEN_COUNT: &str = "oxide.mmap.open.count";
/// Counter for the number of slices created from memory maps.
pub const METRIC_MMAP_SLICE_COUNT: &str = "oxide.mmap.slice.count";
/// Counter for the number of file format detection operations.
pub const METRIC_FORMAT_DETECT_COUNT: &str = "oxide.format.detect.count";
/// Counter for new buffers created in the buffer pool.
pub const METRIC_BUFFER_ACQUIRE_CREATED_COUNT: &str = "oxide.buffer.acquire.created.count";
/// Counter for existing buffers recycled from the buffer pool.
pub const METRIC_BUFFER_ACQUIRE_RECYCLED_COUNT: &str = "oxide.buffer.acquire.recycled.count";
/// Counter for buffers successfully returned to the pool.
pub const METRIC_BUFFER_RECYCLE_OK_COUNT: &str = "oxide.buffer.recycle.ok.count";
/// Counter for buffers dropped because the pool was full.
pub const METRIC_BUFFER_RECYCLE_DROPPED_COUNT: &str = "oxide.buffer.recycle.dropped.count";
/// Counter for successful file scan operations.
pub const METRIC_SCANNER_SCAN_COUNT: &str = "oxide.scanner.scan.count";
/// Counter for files scanned in text mode.
pub const METRIC_SCANNER_MODE_TEXT_COUNT: &str = "oxide.scanner.mode.text.count";
/// Counter for files scanned in image mode.
pub const METRIC_SCANNER_MODE_IMAGE_COUNT: &str = "oxide.scanner.mode.image.count";
/// Counter for files scanned in audio mode.
pub const METRIC_SCANNER_MODE_AUDIO_COUNT: &str = "oxide.scanner.mode.audio.count";
/// Counter for files scanned in raw mode.
pub const METRIC_SCANNER_MODE_RAW_COUNT: &str = "oxide.scanner.mode.raw.count";
/// Counter for times the scanner fell back to raw mode due to errors.
pub const METRIC_SCANNER_FALLBACK_COUNT: &str = "oxide.scanner.fallback.count";
/// Counter for the start of worker tasks.
pub const METRIC_WORKER_TASK_START_COUNT: &str = "oxide.worker.task.start.count";
/// Counter for successfully finished worker tasks.
pub const METRIC_WORKER_TASK_FINISH_COUNT: &str = "oxide.worker.task.finish.count";
/// Counter for failed worker tasks.
pub const METRIC_WORKER_TASK_FAIL_COUNT: &str = "oxide.worker.task.fail.count";
/// Counter for worker queue depth samples taken.
pub const METRIC_WORKER_QUEUE_DEPTH_SAMPLES: &str = "oxide.worker.queue.depth.samples";
/// Counter for the number of archive operations started.
pub const METRIC_PIPELINE_ARCHIVE_RUN_COUNT: &str = "oxide.pipeline.archive.run.count";
/// Counter for the number of extract operations started.
pub const METRIC_PIPELINE_EXTRACT_RUN_COUNT: &str = "oxide.pipeline.extract.run.count";
/// Counter for compression application operations.
pub const METRIC_COMPRESSION_APPLY_COUNT: &str = "oxide.compression.apply.count";
/// Counter for compression reversal operations.
pub const METRIC_COMPRESSION_REVERSE_COUNT: &str = "oxide.compression.reverse.count";
/// Counter for preprocessing application operations.
pub const METRIC_PREPROCESSING_APPLY_COUNT: &str = "oxide.preprocessing.apply.count";
/// Counter for preprocessing reversal operations.
pub const METRIC_PREPROCESSING_REVERSE_COUNT: &str = "oxide.preprocessing.reverse.count";
/// Counter for OXZ block reading operations.
pub const METRIC_OXZ_READ_BLOCK_COUNT: &str = "oxide.oxz.read.block.count";
/// Counter for OXZ block writing operations.
pub const METRIC_OXZ_WRITE_BLOCK_COUNT: &str = "oxide.oxz.write.block.count";

/// Histogram for memory map open latency in microseconds.
pub const METRIC_MMAP_OPEN_LATENCY_US: &str = "oxide.mmap.open.latency_us";
/// Histogram for memory map slice creation latency in microseconds.
pub const METRIC_MMAP_SLICE_LATENCY_US: &str = "oxide.mmap.slice.latency_us";
/// Histogram for format detection latency in microseconds.
pub const METRIC_FORMAT_DETECT_LATENCY_US: &str = "oxide.format.detect.latency_us";
/// Histogram for buffer acquisition latency in microseconds.
pub const METRIC_BUFFER_ACQUIRE_LATENCY_US: &str = "oxide.buffer.acquire.latency_us";
/// Histogram for buffer recycling latency in microseconds.
pub const METRIC_BUFFER_RECYCLE_LATENCY_US: &str = "oxide.buffer.recycle.latency_us";
/// Histogram for file scanning latency in microseconds.
pub const METRIC_SCANNER_SCAN_LATENCY_US: &str = "oxide.scanner.scan.latency_us";
/// Histogram for worker task processing latency in microseconds.
pub const METRIC_WORKER_TASK_LATENCY_US: &str = "oxide.worker.task.latency_us";
/// Histogram for worker queue depth observations.
pub const METRIC_WORKER_QUEUE_DEPTH_HIST: &str = "oxide.worker.queue.depth.hist";
/// Histogram for total archive operation latency in microseconds.
pub const METRIC_PIPELINE_ARCHIVE_RUN_LATENCY_US: &str = "oxide.pipeline.archive.run.latency_us";
/// Histogram for total extract operation latency in microseconds.
pub const METRIC_PIPELINE_EXTRACT_RUN_LATENCY_US: &str = "oxide.pipeline.extract.run.latency_us";
/// Histogram for directory discovery stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_DISCOVERY_US: &str = "oxide.pipeline.stage.discovery.us";
/// Histogram for format probing stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_FORMAT_PROBE_US: &str = "oxide.pipeline.stage.format_probe.us";
/// Histogram for producer read stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_PRODUCER_READ_US: &str = "oxide.pipeline.stage.producer_read.us";
/// Histogram for submission wait stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_SUBMIT_WAIT_US: &str = "oxide.pipeline.stage.submit_wait.us";
/// Histogram for result wait stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_RESULT_WAIT_US: &str = "oxide.pipeline.stage.result_wait.us";
/// Histogram for archive writing stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_WRITER_US: &str = "oxide.pipeline.stage.writer.us";
/// Histogram for archive reading stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_ARCHIVE_READ_US: &str = "oxide.pipeline.stage.archive_read.us";
/// Histogram for decode submission stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_DECODE_SUBMIT_US: &str = "oxide.pipeline.stage.decode_submit.us";
/// Histogram for decode wait stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_DECODE_WAIT_US: &str = "oxide.pipeline.stage.decode_wait.us";
/// Histogram for block merging stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_MERGE_US: &str = "oxide.pipeline.stage.merge.us";
/// Histogram for directory decoding stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_DIRECTORY_DECODE_US: &str =
    "oxide.pipeline.stage.directory_decode.us";
/// Histogram for output file writing stage latency in microseconds.
pub const METRIC_PIPELINE_STAGE_OUTPUT_WRITE_US: &str = "oxide.pipeline.stage.output_write.us";

/// Histogram for compression application latency in microseconds.
pub const METRIC_COMPRESSION_APPLY_LATENCY_US: &str = "oxide.compression.apply.latency_us";
/// Histogram for compression reversal latency in microseconds.
pub const METRIC_COMPRESSION_REVERSE_LATENCY_US: &str = "oxide.compression.reverse.latency_us";
/// Histogram for preprocessing application latency in microseconds.
pub const METRIC_PREPROCESSING_APPLY_LATENCY_US: &str = "oxide.preprocessing.apply.latency_us";
/// Histogram for preprocessing reversal latency in microseconds.
pub const METRIC_PREPROCESSING_REVERSE_LATENCY_US: &str = "oxide.preprocessing.reverse.latency_us";
/// Histogram for compression input bytes.
pub const METRIC_COMPRESSION_INPUT_BYTES: &str = "oxide.compression.input_bytes";
/// Histogram for compression output bytes.
pub const METRIC_COMPRESSION_OUTPUT_BYTES: &str = "oxide.compression.output_bytes";
/// Histogram for preprocessing input bytes.
pub const METRIC_PREPROCESSING_INPUT_BYTES: &str = "oxide.preprocessing.input_bytes";
/// Histogram for preprocessing output bytes.
pub const METRIC_PREPROCESSING_OUTPUT_BYTES: &str = "oxide.preprocessing.output_bytes";
/// Histogram for OXZ block reading latency in microseconds.
pub const METRIC_OXZ_READ_BLOCK_LATENCY_US: &str = "oxide.oxz.read.block.latency_us";
/// Histogram for OXZ block writing latency in microseconds.
pub const METRIC_OXZ_WRITE_BLOCK_LATENCY_US: &str = "oxide.oxz.write.block.latency_us";

/// Gauge for the current process Resident Set Size (RSS) in bytes.
pub const METRIC_MEMORY_PROCESS_RSS_BYTES: &str = "oxide.memory.process.rss_bytes";
/// Gauge for the current process virtual memory size in bytes.
pub const METRIC_MEMORY_PROCESS_VIRTUAL_BYTES: &str = "oxide.memory.process.virtual_bytes";
/// Gauge for the estimated number of bytes held in memory pools.
pub const METRIC_MEMORY_POOL_ESTIMATED_BYTES: &str = "oxide.memory.pool.estimated_bytes";

/// Total count of tasks processed by workers.
pub const METRIC_WORKER_TASK_COUNT: &str = "oxide.worker.task.count";
/// Current depth of the worker queues.
pub const METRIC_WORKER_QUEUE_DEPTH: &str = "oxide.worker.queue.depth";
/// Number of workers currently active.
pub const METRIC_WORKER_ACTIVE_COUNT: &str = "oxide.worker.active.count";
