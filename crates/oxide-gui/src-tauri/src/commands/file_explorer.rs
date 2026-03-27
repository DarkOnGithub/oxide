//! File explorer commands.

use oxide_core::{
    ArchiveDictionaryMode, ArchiveEntryKind, ArchivePipeline, ArchivePipelineConfig, ArchiveReader,
    ArchiveSourceKind, BufferPool, CompressionAlgo, PipelinePerformanceOptions,
    RunTelemetryOptions, OXZ_MAGIC,
};
use serde::{Deserialize, Serialize};
use specta::Type;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const OXZ_EXTENSION: &str = "oxz";

/// Checks if a file is likely an Oxide archive based on its extension.
/// This is a cheap prefilter that avoids opening the file.
fn has_oxide_archive_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case(OXZ_EXTENSION))
        .unwrap_or(false)
}

/// Verifies that a file is an Oxide archive by checking magic bytes.
/// This requires opening and reading from the file.
fn is_oxide_archive_path(path: &Path) -> Result<bool, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| format!("Failed to read metadata for {}: {e}", path.display()))?;

    if !metadata.is_file() {
        return Ok(false);
    }

    let mut file =
        File::open(path).map_err(|e| format!("Failed to open {}: {e}", path.display()))?;
    let mut magic = [0u8; 4];
    let bytes_read = file
        .read(&mut magic)
        .map_err(|e| format!("Failed to read {}: {e}", path.display()))?;

    Ok(bytes_read == OXZ_MAGIC.len() && magic == OXZ_MAGIC)
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerDirectoryEntry {
    pub name: String,
    pub path: String,
    pub is_directory: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub size: f64,
    pub modified_at: Option<f64>,
    pub is_oxide_archive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerPathMetadata {
    pub is_directory: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub size: f64,
    pub modified_at: Option<f64>,
    pub accessed_at: Option<f64>,
    pub created_at: Option<f64>,
    pub readonly: bool,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
pub enum ExplorerArchiveEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
pub enum ExplorerArchiveSourceKind {
    File,
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerArchiveEntry {
    pub path: String,
    pub kind: ExplorerArchiveEntryKind,
    pub target: Option<String>,
    pub size: f64,
    pub modified_at: Option<f64>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct ExplorerArchiveIndex {
    pub source_kind: ExplorerArchiveSourceKind,
    pub entries: Vec<ExplorerArchiveEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
pub enum ArchivePreset {
    Fast,
    Balanced,
    Ultra,
    Extreme,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
pub enum ArchiveCompressionAlgo {
    Lz4,
    Zstd,
    Lzma,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Type)]
pub enum ArchiveDictionaryModeOption {
    Off,
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct CreateArchiveOptions {
    pub preset: ArchivePreset,
    pub compression_algo: ArchiveCompressionAlgo,
    pub compression_level: Option<i32>,
    pub dictionary_mode: ArchiveDictionaryModeOption,
    pub block_size: u32,
    pub workers: u16,
    pub producer_threads: u16,
    pub lzma_extreme: bool,
    pub lzma_dictionary_size: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct PresetArchiveSettings {
    pool_capacity: usize,
    pool_buffers: usize,
    inflight_bytes: usize,
    inflight_blocks_per_worker: usize,
    stream_read_buffer: usize,
    producer_threads: usize,
    directory_mmap_threshold: usize,
    writer_queue_blocks: usize,
    result_wait_ms: u64,
}

fn system_time_to_millis(time: std::time::SystemTime) -> Option<f64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as f64)
}

#[cfg(unix)]
fn metadata_mode(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.mode())
}

#[cfg(not(unix))]
fn metadata_mode(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn metadata_uid(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.uid())
}

#[cfg(not(unix))]
fn metadata_uid(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn metadata_gid(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.gid())
}

#[cfg(not(unix))]
fn metadata_gid(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

fn path_metadata(path: &Path) -> Result<ExplorerPathMetadata, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| format!("Failed to read metadata for {}: {e}", path.display()))?;

    let file_type = metadata.file_type();

    Ok(ExplorerPathMetadata {
        is_directory: file_type.is_dir(),
        is_file: file_type.is_file(),
        is_symlink: file_type.is_symlink(),
        size: metadata.len() as f64,
        modified_at: metadata.modified().ok().and_then(system_time_to_millis),
        accessed_at: metadata.accessed().ok().and_then(system_time_to_millis),
        created_at: metadata.created().ok().and_then(system_time_to_millis),
        readonly: metadata.permissions().readonly(),
        mode: metadata_mode(&metadata),
        uid: metadata_uid(&metadata),
        gid: metadata_gid(&metadata),
    })
}

fn default_workers() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1)
}

fn compression_algo_from_option(value: ArchiveCompressionAlgo) -> CompressionAlgo {
    match value {
        ArchiveCompressionAlgo::Lz4 => CompressionAlgo::Lz4,
        ArchiveCompressionAlgo::Zstd => CompressionAlgo::Zstd,
        ArchiveCompressionAlgo::Lzma => CompressionAlgo::Lzma,
    }
}

fn dictionary_mode_from_option(value: ArchiveDictionaryModeOption) -> ArchiveDictionaryMode {
    match value {
        ArchiveDictionaryModeOption::Off => ArchiveDictionaryMode::Off,
        ArchiveDictionaryModeOption::Auto => ArchiveDictionaryMode::Auto,
    }
}

fn preset_archive_settings(preset: ArchivePreset) -> PresetArchiveSettings {
    match preset {
        ArchivePreset::Fast => PresetArchiveSettings {
            pool_capacity: 2 * 1024 * 1024,
            pool_buffers: 1024,
            inflight_bytes: 2 * 1024 * 1024 * 1024,
            inflight_blocks_per_worker: 64,
            stream_read_buffer: 64 * 1024 * 1024,
            producer_threads: 1,
            directory_mmap_threshold: 8 * 1024 * 1024,
            writer_queue_blocks: 1024,
            result_wait_ms: 1,
        },
        ArchivePreset::Balanced => PresetArchiveSettings {
            pool_capacity: 2 * 1024 * 1024,
            pool_buffers: 512,
            inflight_bytes: 2 * 1024 * 1024 * 1024,
            inflight_blocks_per_worker: 48,
            stream_read_buffer: 64 * 1024 * 1024,
            producer_threads: 3,
            directory_mmap_threshold: 8 * 1024 * 1024,
            writer_queue_blocks: 768,
            result_wait_ms: 1,
        },
        ArchivePreset::Ultra => PresetArchiveSettings {
            pool_capacity: 2 * 1024 * 1024,
            pool_buffers: 192,
            inflight_bytes: 512 * 1024 * 1024,
            inflight_blocks_per_worker: 8,
            stream_read_buffer: 16 * 1024 * 1024,
            producer_threads: 1,
            directory_mmap_threshold: 16 * 1024 * 1024,
            writer_queue_blocks: 96,
            result_wait_ms: 4,
        },
        ArchivePreset::Extreme => PresetArchiveSettings {
            pool_capacity: 4 * 1024 * 1024,
            pool_buffers: 192,
            inflight_bytes: 768 * 1024 * 1024,
            inflight_blocks_per_worker: 6,
            stream_read_buffer: 16 * 1024 * 1024,
            producer_threads: 1,
            directory_mmap_threshold: 16 * 1024 * 1024,
            writer_queue_blocks: 64,
            result_wait_ms: 6,
        },
    }
}

fn validate_create_archive_options(options: &CreateArchiveOptions) -> Result<(), String> {
    if options.block_size == 0 {
        return Err("Block size must be greater than 0".to_string());
    }

    if let Some(level) = options.compression_level {
        match options.compression_algo {
            ArchiveCompressionAlgo::Lz4 => {
                return Err("Compression level is not supported for LZ4".to_string())
            }
            ArchiveCompressionAlgo::Zstd if !(1..=22).contains(&level) => {
                return Err("Zstd level must be between 1 and 22".to_string())
            }
            ArchiveCompressionAlgo::Lzma if !(1..=9).contains(&level) => {
                return Err("LZMA level must be between 1 and 9".to_string())
            }
            _ => {}
        }
    }

    if options.lzma_extreme && !matches!(options.compression_algo, ArchiveCompressionAlgo::Lzma) {
        return Err("LZMA extreme is only supported with the LZMA compressor".to_string());
    }

    if let Some(dictionary_size) = options.lzma_dictionary_size {
        if !matches!(options.compression_algo, ArchiveCompressionAlgo::Lzma) {
            return Err(
                "LZMA dictionary size is only supported with the LZMA compressor".to_string(),
            );
        }

        if dictionary_size < 4096 {
            return Err("LZMA dictionary size must be at least 4096 bytes".to_string());
        }
    }

    Ok(())
}

fn build_archive_pipeline_with_options(
    options: &CreateArchiveOptions,
) -> Result<ArchivePipeline, String> {
    validate_create_archive_options(options)?;

    let preset = preset_archive_settings(options.preset);
    let workers = if options.workers == 0 {
        default_workers()
    } else {
        usize::from(options.workers)
    };
    let producer_threads = if options.producer_threads == 0 {
        preset.producer_threads
    } else {
        usize::from(options.producer_threads)
    };
    let block_size = options.block_size as usize;
    let pool_capacity = preset.pool_capacity.max(block_size);
    let buffer_pool = Arc::new(BufferPool::new(pool_capacity, preset.pool_buffers));

    let mut performance = PipelinePerformanceOptions::default();
    performance.dictionary_mode = dictionary_mode_from_option(options.dictionary_mode);
    performance.compression_level = options.compression_level;
    performance.lzma_extreme = options.lzma_extreme;
    performance.lzma_dictionary_size = options.lzma_dictionary_size.map(|value| value as usize);
    performance.max_inflight_bytes = preset.inflight_bytes;
    performance.max_inflight_blocks_per_worker = preset.inflight_blocks_per_worker;
    performance.directory_stream_read_buffer_size = preset.stream_read_buffer;
    performance.producer_threads = producer_threads.max(1);
    performance.directory_mmap_threshold_bytes = preset.directory_mmap_threshold;
    performance.writer_result_queue_blocks = preset.writer_queue_blocks;
    performance.result_wait_timeout =
        std::time::Duration::from_millis(preset.result_wait_ms.max(1));

    let mut config = ArchivePipelineConfig::new(
        block_size.max(1),
        workers.max(1),
        buffer_pool,
        compression_algo_from_option(options.compression_algo),
    );
    config.performance = performance;

    Ok(ArchivePipeline::new(config))
}

fn build_extract_pipeline() -> ArchivePipeline {
    let workers = default_workers();
    let buffer_pool = Arc::new(BufferPool::new(
        1024 * 1024,
        workers.saturating_mul(8).max(8),
    ));
    let mut config =
        ArchivePipelineConfig::new(1024 * 1024, workers, buffer_pool, CompressionAlgo::Lz4);
    config.performance = PipelinePerformanceOptions::default();
    ArchivePipeline::new(config)
}

fn archive_entry_kind(kind: ArchiveEntryKind) -> ExplorerArchiveEntryKind {
    match kind {
        ArchiveEntryKind::File => ExplorerArchiveEntryKind::File,
        ArchiveEntryKind::Directory => ExplorerArchiveEntryKind::Directory,
        ArchiveEntryKind::Symlink => ExplorerArchiveEntryKind::Symlink,
    }
}

fn archive_source_kind(kind: ArchiveSourceKind) -> ExplorerArchiveSourceKind {
    match kind {
        ArchiveSourceKind::File => ExplorerArchiveSourceKind::File,
        ArchiveSourceKind::Directory => ExplorerArchiveSourceKind::Directory,
    }
}

fn archive_index_from_reader<R: Read + Seek>(reader: ArchiveReader<R>) -> ExplorerArchiveIndex {
    let source_kind = archive_source_kind(reader.source_kind());
    let entries = reader
        .manifest()
        .entries()
        .iter()
        .map(|entry| ExplorerArchiveEntry {
            path: entry.path.clone(),
            kind: archive_entry_kind(entry.kind),
            target: entry.target.clone(),
            size: entry.size as f64,
            modified_at: system_time_to_millis(entry.mtime.to_system_time()),
            mode: Some(entry.mode),
            uid: Some(entry.uid),
            gid: Some(entry.gid),
        })
        .collect();

    ExplorerArchiveIndex {
        source_kind,
        entries,
    }
}

fn default_extract_target(archive_path: &Path, output_directory: &Path) -> PathBuf {
    let stem = archive_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("extracted");
    output_directory.join(stem)
}

/// Lists a directory and includes basic metadata for each entry.
#[tauri::command]
#[specta::specta]
pub async fn list_directory_entries(path: String) -> Result<Vec<ExplorerDirectoryEntry>, String> {
    let directory = Path::new(&path);

    let entries = std::fs::read_dir(directory)
        .map_err(|e| format!("Failed to read directory {}: {e}", directory.display()))?;

    let mut result = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {e}"))?;
        let entry_path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        let metadata = match std::fs::symlink_metadata(&entry_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                log::warn!(
                    "Failed to read entry metadata for {:?}: {}",
                    entry_path,
                    error
                );
                continue;
            }
        };

        let file_type = metadata.file_type();
        let is_oxide_archive = file_type.is_file() && has_oxide_archive_extension(&entry_path);

        result.push(ExplorerDirectoryEntry {
            name,
            path: entry_path.to_string_lossy().to_string(),
            is_directory: file_type.is_dir(),
            is_file: file_type.is_file(),
            is_symlink: file_type.is_symlink(),
            size: metadata.len() as f64,
            modified_at: metadata.modified().ok().and_then(system_time_to_millis),
            is_oxide_archive,
        });
    }

    Ok(result)
}

/// Returns metadata for a file or folder path.
#[tauri::command]
#[specta::specta]
pub async fn get_path_metadata(path: String) -> Result<ExplorerPathMetadata, String> {
    path_metadata(Path::new(&path))
}

/// Returns whether a path is an Oxide archive.
#[tauri::command]
#[specta::specta]
pub async fn is_oxide_archive(path: String) -> Result<bool, String> {
    is_oxide_archive_path(Path::new(&path))
}

/// Reads the metadata index of an Oxide archive without extracting payloads.
#[tauri::command]
#[specta::specta]
pub async fn read_oxide_archive_index(path: String) -> Result<ExplorerArchiveIndex, String> {
    let file = File::open(&path).map_err(|e| format!("Failed to open archive {}: {e}", path))?;
    let reader = ArchiveReader::new(file)
        .map_err(|e| format!("Failed to read archive index for {}: {e}", path))?;
    Ok(archive_index_from_reader(reader))
}

/// Creates an Oxide archive from a folder.
#[tauri::command]
#[specta::specta]
pub async fn create_oxide_archive(
    source_path: String,
    output_path: String,
    options: CreateArchiveOptions,
) -> Result<(), String> {
    let source = Path::new(&source_path);
    if !source.is_dir() {
        return Err(format!("Source is not a directory: {}", source.display()));
    }

    if let Some(parent) = Path::new(&output_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create output directory {}: {e}",
                    parent.display()
                )
            })?;
        }
    }

    let output = File::create(&output_path)
        .map_err(|e| format!("Failed to create archive {}: {e}", output_path))?;

    build_archive_pipeline_with_options(&options)?
        .archive_directory_seekable(source, output, RunTelemetryOptions::default(), None)
        .map_err(|e| format!("Failed to create archive {}: {e}", output_path))?;

    Ok(())
}

/// Extracts an Oxide archive.
#[tauri::command]
#[specta::specta]
pub async fn extract_oxide_archive(
    archive_path: String,
    output_directory: String,
    delete_source: bool,
) -> Result<(), String> {
    let archive = Path::new(&archive_path);
    let output_directory = Path::new(&output_directory);
    std::fs::create_dir_all(output_directory).map_err(|e| {
        format!(
            "Failed to create extraction directory {}: {e}",
            output_directory.display()
        )
    })?;

    let source_kind = {
        let file = File::open(archive)
            .map_err(|e| format!("Failed to open archive {}: {e}", archive.display()))?;
        let reader = ArchiveReader::new(file)
            .map_err(|e| format!("Failed to inspect archive {}: {e}", archive.display()))?;
        reader.source_kind()
    };

    let output_path = match source_kind {
        ArchiveSourceKind::Directory => output_directory.to_path_buf(),
        ArchiveSourceKind::File => default_extract_target(archive, output_directory),
    };

    let file = File::open(archive)
        .map_err(|e| format!("Failed to open archive {}: {e}", archive.display()))?;

    build_extract_pipeline()
        .extract_path(file, &output_path, RunTelemetryOptions::default(), None)
        .map_err(|e| format!("Failed to extract archive {}: {e}", archive.display()))?;

    if delete_source {
        std::fs::remove_file(archive)
            .map_err(|e| format!("Failed to delete archive {}: {e}", archive.display()))?;
    }

    Ok(())
}
