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

/// Chiffre une archive existante sans la décompresser (Pass-through)
pub fn encrypt_existing_archive(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    password: &str,
) -> crate::Result<()> {
    use std::fs::File;
    
    // On ouvre le lecteur sans mot de passe
    let mut reader = crate::format::ArchiveReader::new(File::open(input_path)?)?;
    
    if (reader.global_header().flags & crate::format::oxz::headers::HEADER_FLAG_ENCRYPTED) != 0 {
        return Err(crate::OxideError::InvalidFormat("L'archive est déjà chiffrée."));
    }

    // On ouvre le writer AVEC le mot de passe (il va générer le Sel et la clé)
    let out_file = File::create(output_path)?;
    let mut writer = crate::format::ArchiveWriter::with_manifest(out_file, Some(reader.manifest().clone()))
        .with_password(Some(password.to_string()));

    let source_flag = match reader.source_kind() {
        crate::ArchiveSourceKind::File => 0,
        crate::ArchiveSourceKind::Directory => 1,
    };
    
    writer.write_global_header_with_flags(reader.block_count(), source_flag)?;

    // La boucle magique : on lit, on encapsule, on écrit (le writer s'occupe de chiffrer)
    for index in 0..reader.block_count() {
        let (descriptor, data) = reader.read_block(index)?;
        let meta = descriptor.compression_meta()?;
        
        let block = crate::types::CompressedBlock {
            id: index as usize,
            stream_id: 0,
            data: crate::types::CompressedPayload::Owned(data),
            compression: meta.algo,
            raw_passthrough: meta.raw_passthrough,
            dictionary_id: meta.dictionary_id,
            original_len: descriptor.raw_len as u64,
            crc32: 0,
            reference_target: descriptor.reference_target,
        };
        
        writer.write_owned_block(block)?;
    }

    writer.write_footer()?;
    Ok(())
}

/// Déchiffre une archive existante sans la décompresser (Pass-through)
pub fn decrypt_existing_archive(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    password: &str,
) -> crate::Result<()> {
    use std::fs::File;
    
    // On ouvre le lecteur AVEC le mot de passe (il va vérifier le Sel et déchiffrer)
    let mut reader = crate::format::ArchiveReader::new(File::open(input_path)?)?
        .with_password(Some(password.to_string()))?;

    // On ouvre le writer SANS mot de passe
    let out_file = File::create(output_path)?;
    let mut writer = crate::format::ArchiveWriter::with_manifest(out_file, Some(reader.manifest().clone()));

    let source_flag = match reader.source_kind() {
        crate::ArchiveSourceKind::File => 0,
        crate::ArchiveSourceKind::Directory => 1,
    };
    
    writer.write_global_header_with_flags(reader.block_count(), source_flag)?;

    // La même boucle : le lecteur déchiffre à la volée, le writer écrit en clair
    for index in 0..reader.block_count() {
        let (descriptor, data) = reader.read_block(index)?;
        let meta = descriptor.compression_meta()?;
        
        let block = crate::types::CompressedBlock {
            id: index as usize,
            stream_id: 0,
            data: crate::types::CompressedPayload::Owned(data),
            compression: meta.algo,
            raw_passthrough: meta.raw_passthrough,
            dictionary_id: meta.dictionary_id,
            original_len: descriptor.raw_len as u64,
            crc32: 0,
            reference_target: descriptor.reference_target,
        };
        
        writer.write_owned_block(block)?;
    }

    writer.write_footer()?;
    Ok(())
}