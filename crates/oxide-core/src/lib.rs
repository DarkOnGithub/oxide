pub mod buffer;
pub mod core;
pub mod error;
pub mod format;
pub mod io;
pub mod pipeline;
pub mod telemetry;
pub mod types;

pub use buffer::{BufferPool, PoolMetricsSnapshot, PooledBuffer};
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
pub use pipeline::{ArchivePipeline, ArchiveProgressSnapshot, ArchiveRunStats, ArchiveSourceKind};
pub use telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
pub use types::{
    AudioStrategy, Batch, BatchData, BinaryStrategy, CompressedBlock, CompressionAlgo, FileFormat,
    ImageStrategy, PreProcessingStrategy, Result, TextStrategy,
};
