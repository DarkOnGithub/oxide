pub mod buffer;
pub mod core;
pub mod error;
pub mod format;
pub mod io;
pub mod telemetry;
pub mod types;

pub use buffer::{BufferPool, PoolMetricsSnapshot, PooledBuffer};
pub use core::{WorkStealingQueue, WorkStealingWorker, WorkerPool, WorkerPoolHandle};
pub use error::OxideError;
pub use io::{BoundaryMode, InputScanner, MmapInput};
pub use telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
pub use types::{
    AudioStrategy, Batch, BatchData, BinaryStrategy, CompressedBlock, CompressionAlgo, FileFormat,
    ImageStrategy, PreProcessingStrategy, Result, TextStrategy,
};
