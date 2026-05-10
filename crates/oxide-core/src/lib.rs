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
pub mod dictionary;
pub mod error;
pub mod format;
pub mod io;
pub mod pipeline;
pub mod telemetry;
pub mod types;
pub mod crypto;

pub use buffer::{BufferPool, PoolMetricsSnapshot, PooledBuffer};
pub use checksum::compute_checksum;
pub use compression::{apply_compression, reverse_compression};
pub use core::{
    PoolRuntimeSnapshot, WorkStealingQueue, WorkStealingWorker, WorkerPool, WorkerPoolHandle,
    WorkerRuntimeSnapshot, WorkerScratchArena,
};
pub use dictionary::{
    ArchiveDictionary, ArchiveDictionaryBank, ArchiveDictionaryMode, DictionaryClass,
    DictionaryTrainer,
};
pub use error::OxideError;
pub use format::{
    ARCHIVE_METADATA_SIZE, ArchiveBlockWriter, ArchiveManifest, ArchiveMetadata, ArchiveReader,
    ArchiveWriter, BlockIterator, CHUNK_DESCRIPTOR_SIZE, CHUNK_TABLE_HEADER_SIZE, ChunkDescriptor,
    DEFAULT_DEDUP_WINDOW_BLOCKS, DEFAULT_REORDER_PENDING_LIMIT, FOOTER_SIZE, Footer,
    GLOBAL_HEADER_SIZE, GlobalHeader, OXZ_MAGIC, OXZ_VERSION, ReorderBuffer, SeekableArchiveWriter,
    should_force_raw_storage, should_force_raw_storage_by_extension,
};
pub use io::{ChunkingMode, ChunkingPolicy, InputScanner, MmapInput};
pub use pipeline::{
    ArchiveEntryKind, ArchiveListingEntry, ArchivePipeline, ArchivePipelineConfig,
    ArchiveSourceKind, ArchiveTimestamp, PipelinePerformanceOptions,
};
pub use telemetry::worker::{DefaultWorkerTelemetry, WorkerTelemetry};
pub use telemetry::{
    ArchiveProgressEvent, ArchiveReport, ArchiveRun, ExtractProgressEvent, ExtractReport,
    ProfileEvent, ReportValue, RunReport, RunTelemetryOptions, TelemetryEvent, TelemetrySink,
    ThreadReport, WorkerReport,
};
pub use types::{
    Batch, BatchData, ChunkEncodingPlan, CompressedBlock, CompressedPayload, CompressionAlgo,
    CompressionMeta, Result,
};

use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Quickly checks if an OXZ archive is encrypted by reading only its header flags.
/// This is extremely fast as it only reads the first 8 bytes of the file.
pub fn probe_encryption(path: &Path) -> crate::Result<bool> {
    // Attempt to open the file. If it fails, we return false and let the 
    // main extractor pipeline handle the standard file error later.
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return Ok(false),
    };

    // We only need the first 8 bytes (Magic is 0..4, Version is 4..6, Flags are 6..8)
    let mut buffer = [0u8; 8];
    if file.read_exact(&mut buffer).is_err() {
        return Ok(false); // File is too short to be a valid archive
    }

    // Verify the OXZ magic signature to ensure it's actually our format
    // Note: adjust the path to OXZ_MAGIC if your imports are structured differently
    if buffer[0..4] != crate::format::oxz::OXZ_MAGIC {
        return Ok(false);
    }

    // Read the flags (bytes 6 and 7 in Little Endian)
    let flags = u16::from_le_bytes([buffer[6], buffer[7]]);
    
    // Check if the encryption flag is set
    let is_encrypted = (flags & crate::format::oxz::headers::HEADER_FLAG_ENCRYPTED) != 0;

    Ok(is_encrypted)
}

/// Vérifie si un mot de passe est correct en tentant de déchiffrer le tout premier bloc.
pub fn verify_archive_password(path: &std::path::Path, password: &str) -> crate::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    
    let mut file = std::fs::File::open(path)?;
    let archive = crate::format::ArchiveReader::new(file.try_clone()?)?;
    
    let header = archive.global_header();
    if (header.flags & crate::format::oxz::headers::HEADER_FLAG_ENCRYPTED) == 0 {
        return Ok(true); // L'archive n'est pas chiffrée
    }
    
    if archive.block_count() == 0 {
        return Ok(true); // Archive vide
    }
    
    // On récupère la taille et la position du TOUT PREMIER bloc
    let descriptor = archive.block_descriptor(0)?;
    
    // On lit les octets chiffrés directement sur le disque
    let mut buffer = vec![0u8; descriptor.encoded_len as usize];
    file.seek(SeekFrom::Start(descriptor.payload_offset))?;
    file.read_exact(&mut buffer)?;
    
    // On dérive la clé et on teste de déchiffrer JUSTE CE BLOC !
    let key = crate::crypto::derive_key(password, &header.salt)?;
    
    match crate::crypto::decrypt_block(&key, &buffer) {
        Ok(_) => Ok(true),  // Déchiffrement réussi = Bon mot de passe !
        Err(_) => Ok(false), // Échec = Mauvais mot de passe !
    }
}