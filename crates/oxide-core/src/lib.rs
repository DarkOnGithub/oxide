//! # Oxide Core
//!
//! `oxide-core` is the engine behind the Oxide archival system. It provides
//! high-performance parallel processing for data compression and archival
//! using the OXZ format.
//!
//! ## Key Components
//!
//! - **Pipeline**: Orchestrates the scanning, processing, and writing of archives.
//! - **Compression**: LZ4, LZMA, ZPAQ, and Zstd codec dispatch.
//! - **IO**: Efficient I/O operations, including memory-mapped files and scanners.
//! - **Telemetry**: Comprehensive instrumentation for monitoring and profiling.

pub mod buffer;
pub mod checksum;
pub mod compression;
pub mod core;
pub mod error;
pub mod format;
pub mod io;
pub mod pipeline;
pub mod telemetry;
pub mod types;

pub use buffer::{BufferPool, PoolMetricsSnapshot, PooledBuffer};
pub use checksum::compute_checksum;
pub use compression::{apply_compression, reverse_compression};
pub use core::{
    PoolRuntimeSnapshot, WorkStealingQueue, WorkStealingWorker, WorkerPool, WorkerPoolHandle,
    WorkerRuntimeSnapshot, WorkerScratchArena,
};
pub use error::OxideError;
pub use format::{
    ARCHIVE_METADATA_SIZE, ArchiveBlockWriter, ArchiveManifest, ArchiveMetadata, ArchiveReader,
    ArchiveWriter, BlockIterator, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE, ChunkDescriptor,
    DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, Footer, GLOBAL_HEADER_SIZE, GlobalHeader,
    OXZ_MAGIC, OXZ_VERSION, ReorderBuffer, SeekableArchiveWriter, should_force_raw_storage,
    should_force_raw_storage_by_extension,
};
pub use io::{ChunkingMode, ChunkingPolicy, InputScanner, MmapInput};
pub use pipeline::{
    ArchiveEntryKind, ArchiveListingEntry, ArchivePipeline, ArchivePipelineConfig,
    ArchiveSourceKind, ArchiveTimestamp, PipelinePerformanceOptions,
};
pub use telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
pub use telemetry::{
    ArchiveProgressEvent, ArchiveReport, ArchiveRun, ExtractProgressEvent, ExtractReport,
    GlobalTelemetrySink, ProfileEvent, ReportExport, ReportValue, RunReport, RunTelemetryOptions,
    TelemetryEvent, TelemetrySink, ThreadReport, WorkerReport,
};
pub use types::{
    Batch, BatchData, ChunkEncodingPlan, CompressedBlock, CompressedPayload, CompressionAlgo,
    CompressionMeta, CompressionPreset, Result,
};
