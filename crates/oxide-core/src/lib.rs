pub mod buffer;
pub mod compression;
pub mod core;
pub mod error;
pub mod format;
pub mod io;
pub mod pipeline;
pub mod preprocessing;
pub mod telemetry;
pub mod types;

pub use buffer::{BufferPool, PoolMetricsSnapshot, PooledBuffer};
pub use compression::{apply_compression, reverse_compression};
pub use core::{
    PoolRuntimeSnapshot, WorkStealingQueue, WorkStealingWorker, WorkerPool, WorkerPoolHandle,
    WorkerRuntimeSnapshot,
};
pub use error::OxideError;
pub use format::{
    ArchiveReader, ArchiveWriter, BLOCK_HEADER_SIZE, BlockHeader, BlockIterator,
    DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, Footer, FormatDetector, GLOBAL_HEADER_SIZE,
    GlobalHeader, OXZ_MAGIC, OXZ_VERSION, ReorderBuffer,
};
pub use io::{BoundaryMode, InputScanner, MmapInput};
pub use pipeline::{
    ArchivePipeline, ArchivePipelineConfig, ArchiveSourceKind, PipelinePerformanceOptions,
};
pub use preprocessing::{apply_preprocessing, reverse_preprocessing};
pub use telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
pub use telemetry::{
    ArchiveProgressEvent, ArchiveReport, ArchiveRun, ExtractProgressEvent, ExtractReport,
    GlobalTelemetrySink, ProfileEvent, ReportExport, ReportValue, RunReport, RunTelemetryOptions,
    TelemetryEvent, TelemetrySink, ThreadReport, WorkerReport,
};
pub use types::{
    AudioStrategy, Batch, BatchData, BinaryStrategy, CompressedBlock, CompressionAlgo,
    CompressionMeta, CompressionPreset, FileFormat, ImageStrategy, PreProcessingStrategy, Result,
    TextStrategy,
};
