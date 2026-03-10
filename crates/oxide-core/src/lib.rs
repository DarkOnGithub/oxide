//! # Oxide Core
//!
//! `oxide-core` is the engine behind the Oxide archival system. It provides
//! high-performance parallel processing for data compression and archival
//! using the OXZ format.
//!
//! ## Key Components
//!
//! - **Pipeline**: Orchestrates the scanning, processing, and writing of archives.
//! - **Compression**: LZ4 codec implementation and dispatch.
//! - **Preprocessing**: Format-aware data transformations to improve compression ratios.
//! - **IO**: Efficient I/O operations, including memory-mapped files and smart scanners.
//! - **Telemetry**: Comprehensive instrumentation for monitoring and profiling.

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
    WorkerRuntimeSnapshot, WorkerScratchArena,
};
pub use error::OxideError;
pub use format::{
    ArchiveBlockWriter, ArchiveManifest, ArchiveReader, ArchiveWriter, BlockHeader, BlockIterator,
    CHUNK_DESCRIPTOR_SIZE, CORE_SECTION_COUNT, ChunkDescriptor, DEFAULT_REORDER_PENDING_LIMIT,
    FEATURE_DEDUP_REFERENCES, FOOTER_SIZE, Footer, FormatDetector, GLOBAL_HEADER_SIZE,
    GlobalHeader, OXZ_MAGIC, OXZ_VERSION, ReorderBuffer, SECTION_TABLE_ENTRY_SIZE,
    SectionTableEntry, SectionType, SeekableArchiveWriter, StoredDictionary,
};
pub use io::{BoundaryMode, ChunkingMode, ChunkingPolicy, InputScanner, MmapInput};
pub use pipeline::{
    ArchiveEntryKind, ArchiveListingEntry, ArchivePipeline, ArchivePipelineConfig,
    ArchiveSourceKind, PipelinePerformanceOptions,
};
pub use preprocessing::{
    AudioEndian, AudioMetadata, AudioSampleEncoding, ImageMetadata, ImagePixelFormat,
    PreprocessingMetadata, apply_preprocessing, apply_preprocessing_with_metadata,
    reverse_preprocessing,
};
pub use telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
pub use telemetry::{
    ArchiveProgressEvent, ArchiveReport, ArchiveRun, ExtractProgressEvent, ExtractReport,
    GlobalTelemetrySink, ProfileEvent, ReportExport, ReportValue, RunReport, RunTelemetryOptions,
    TelemetryEvent, TelemetrySink, ThreadReport, WorkerReport,
};
pub use types::{
    AudioStrategy, Batch, BatchData, BinaryStrategy, ChunkEncodingPlan, CompressedBlock,
    CompressionAlgo, CompressionMeta, CompressionPreset, FileFormat, ImageStrategy,
    PreProcessingStrategy, Result, TextStrategy,
};
