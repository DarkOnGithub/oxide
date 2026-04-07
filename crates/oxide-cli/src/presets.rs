use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use oxide_core::{
    ArchiveDictionaryMode, ChunkingPolicy, CompressionAlgo, DEFAULT_DEDUP_WINDOW_BLOCKS,
};
use serde::Deserialize;

use crate::AppResult;
use crate::cli::{ChunkingArg, CompressionArg, parse_size};

const DEFAULT_PRESETS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/presets.json");
const MAX_RAW_CHUNK_DEDUP_WINDOW_BLOCKS: usize = 1 << 20;
const MAX_BLOCK_DEDUP_WINDOW_BLOCKS: usize = 1 << 20;

#[derive(Debug, Clone)]
pub struct ResolvedArchiveSettings {
    pub profile_name: String,
    pub profile_source: String,
    pub compression: CompressionAlgo,
    pub skip_compression: bool,
    pub dictionary_mode: ArchiveDictionaryMode,
    pub chunking_policy: ChunkingPolicy,
    pub dictionary_from: Option<std::path::PathBuf>,
    pub compression_level: Option<i32>,
    pub compression_extreme: bool,
    pub lzma_dictionary_size: Option<usize>,
    pub block_size: usize,
    pub workers: usize,
    pub pool_capacity: usize,
    pub pool_buffers: usize,
    pub stats_interval_ms: u64,
    pub inflight_bytes: usize,
    pub inflight_blocks_per_worker: usize,
    pub stream_read_buffer: usize,
    pub producer_threads: usize,
    pub directory_mmap_threshold: usize,
    pub writer_queue_blocks: usize,
    pub result_wait_ms: u64,
    pub raw_chunk_dedup_window_blocks: usize,
    pub block_dedup_window_blocks: usize,
}

#[derive(Debug, Default)]
pub struct ArchiveOverrides {
    pub compression: Option<CompressionAlgo>,
    pub skip_compression: bool,
    pub dictionary_mode: Option<ArchiveDictionaryMode>,
    pub chunking_mode: Option<ChunkingArg>,
    pub chunking_min_block_size: Option<usize>,
    pub chunking_max_block_size: Option<usize>,
    pub dictionary_from: Option<std::path::PathBuf>,
    pub compression_level: Option<i32>,
    pub block_size: Option<usize>,
    pub workers: Option<usize>,
    pub pool_capacity: Option<usize>,
    pub pool_buffers: Option<usize>,
    pub stats_interval_ms: Option<u64>,
    pub inflight_bytes: Option<usize>,
    pub inflight_blocks_per_worker: Option<usize>,
    pub stream_read_buffer: Option<usize>,
    pub producer_threads: Option<usize>,
    pub directory_mmap_threshold: Option<usize>,
    pub writer_queue_blocks: Option<usize>,
    pub result_wait_ms: Option<u64>,
    pub raw_chunk_dedup_window_blocks: Option<usize>,
}

pub fn resolve_archive_settings(
    preset_file: Option<&Path>,
    requested_preset: Option<&str>,
    overrides: ArchiveOverrides,
) -> AppResult<ResolvedArchiveSettings> {
    let (source, source_label) = match preset_file {
        Some(path) => (fs::read_to_string(path)?, path.display().to_string()),
        None => (
            fs::read_to_string(DEFAULT_PRESETS_PATH).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "failed to read default preset file '{}': {error}",
                        DEFAULT_PRESETS_PATH
                    ),
                )
            })?,
            DEFAULT_PRESETS_PATH.to_string(),
        ),
    };

    let file: PresetFile = serde_json::from_str(&source)?;
    let registry = file.archive;
    let preset_name = requested_preset
        .or(registry.default_preset.as_deref())
        .ok_or_else(|| invalid_input("preset config is missing archive.default_preset"))?;
    let preset = registry.presets.get(preset_name).ok_or_else(|| {
        let available = registry.presets.keys().cloned().collect::<Vec<_>>().join(", ");
        invalid_input(format!(
            "unknown archive preset '{preset_name}' in {source_label}; available presets: {available}"
        ))
    })?;

    let mut merged = registry.defaults.clone();
    merged.merge_from(preset);
    merged.resolve(preset_name, &source_label, overrides)
}

#[derive(Debug, Deserialize)]
struct PresetFile {
    archive: ArchivePresetRegistry,
}

#[derive(Debug, Deserialize)]
struct ArchivePresetRegistry {
    #[serde(default)]
    default_preset: Option<String>,
    defaults: ArchivePresetConfig,
    presets: BTreeMap<String, ArchivePresetConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ArchivePresetConfig {
    compression: Option<CompressionConfig>,
    dictionary_mode: Option<String>,
    chunking: Option<ChunkingConfig>,
    dictionary_from: Option<std::path::PathBuf>,
    block_size: Option<SizeValue>,
    workers: Option<usize>,
    pool_capacity: Option<SizeValue>,
    pool_buffers: Option<usize>,
    stats_interval_ms: Option<u64>,
    inflight_bytes: Option<SizeValue>,
    inflight_blocks_per_worker: Option<usize>,
    stream_read_buffer: Option<SizeValue>,
    producer_threads: Option<usize>,
    directory_mmap_threshold: Option<SizeValue>,
    writer_queue_blocks: Option<usize>,
    result_wait_ms: Option<u64>,
    raw_chunk_dedup_window_blocks: Option<usize>,
    block_dedup_window_blocks: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompressionConfig {
    compressor: Option<String>,
    level: Option<i32>,
    extreme: Option<bool>,
    dictionary_size: Option<SizeValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChunkingConfig {
    mode: Option<String>,
    min_block_size: Option<SizeValue>,
    max_block_size: Option<SizeValue>,
}

impl ArchivePresetConfig {
    fn merge_from(&mut self, other: &Self) {
        merge_nested_option(
            &mut self.compression,
            other.compression.clone(),
            |current, incoming| {
                current.merge_from(&incoming);
            },
        );
        merge_option(&mut self.dictionary_mode, other.dictionary_mode.clone());
        merge_nested_option(
            &mut self.chunking,
            other.chunking.clone(),
            |current, incoming| {
                current.merge_from(&incoming);
            },
        );
        merge_option(&mut self.dictionary_from, other.dictionary_from.clone());
        merge_option(&mut self.block_size, other.block_size.clone());
        merge_option(&mut self.workers, other.workers);
        merge_option(&mut self.pool_capacity, other.pool_capacity.clone());
        merge_option(&mut self.pool_buffers, other.pool_buffers);
        merge_option(&mut self.stats_interval_ms, other.stats_interval_ms);
        merge_option(&mut self.inflight_bytes, other.inflight_bytes.clone());
        merge_option(
            &mut self.inflight_blocks_per_worker,
            other.inflight_blocks_per_worker,
        );
        merge_option(
            &mut self.stream_read_buffer,
            other.stream_read_buffer.clone(),
        );
        merge_option(&mut self.producer_threads, other.producer_threads);
        merge_option(
            &mut self.directory_mmap_threshold,
            other.directory_mmap_threshold.clone(),
        );
        merge_option(&mut self.writer_queue_blocks, other.writer_queue_blocks);
        merge_option(&mut self.result_wait_ms, other.result_wait_ms);
        merge_option(
            &mut self.raw_chunk_dedup_window_blocks,
            other.raw_chunk_dedup_window_blocks,
        );
        merge_option(
            &mut self.block_dedup_window_blocks,
            other.block_dedup_window_blocks,
        );
    }

    fn resolve(
        self,
        preset_name: &str,
        source_label: &str,
        overrides: ArchiveOverrides,
    ) -> AppResult<ResolvedArchiveSettings> {
        let compression = self
            .compression
            .ok_or_else(|| invalid_input("archive preset is missing compression"))?
            .resolve(overrides.compression, overrides.compression_level)?;
        let block_size = resolve_usize(overrides.block_size, self.block_size, "block_size")?;
        let preset_chunking_mode = self
            .chunking
            .as_ref()
            .and_then(|chunking| chunking.mode.as_deref())
            .map(parse_chunking_mode)
            .transpose()?;

        let raw_chunk_dedup_window_blocks = overrides
            .raw_chunk_dedup_window_blocks
            .or(self.raw_chunk_dedup_window_blocks)
            .unwrap_or(DEFAULT_DEDUP_WINDOW_BLOCKS);
        validate_raw_chunk_dedup_window_blocks(raw_chunk_dedup_window_blocks)?;

        let block_dedup_window_blocks = self
            .block_dedup_window_blocks
            .unwrap_or(DEFAULT_DEDUP_WINDOW_BLOCKS);
        validate_block_dedup_window_blocks(block_dedup_window_blocks)?;

        Ok(ResolvedArchiveSettings {
            profile_name: preset_name.to_string(),
            profile_source: source_label.to_string(),
            compression: compression.algo,
            skip_compression: overrides.skip_compression,
            dictionary_mode: overrides.dictionary_mode.unwrap_or(
                self.dictionary_mode
                    .as_deref()
                    .map(parse_dictionary_mode)
                    .transpose()?
                    .unwrap_or(ArchiveDictionaryMode::Off),
            ),
            chunking_policy: resolve_chunking_policy(
                block_size,
                overrides.chunking_mode.or(preset_chunking_mode),
                overrides.chunking_min_block_size,
                overrides.chunking_max_block_size,
                self.chunking.as_ref(),
            )?,
            dictionary_from: overrides
                .dictionary_from
                .or_else(|| self.dictionary_from.clone()),
            compression_level: compression.level,
            compression_extreme: compression.extreme,
            lzma_dictionary_size: compression.lzma_dictionary_size,
            block_size,
            workers: resolve_number(overrides.workers, self.workers, "workers")?,
            pool_capacity: resolve_usize(
                overrides.pool_capacity,
                self.pool_capacity,
                "pool_capacity",
            )?,
            pool_buffers: resolve_number(
                overrides.pool_buffers,
                self.pool_buffers,
                "pool_buffers",
            )?,
            stats_interval_ms: resolve_number(
                overrides.stats_interval_ms,
                self.stats_interval_ms,
                "stats_interval_ms",
            )?,
            inflight_bytes: resolve_usize(
                overrides.inflight_bytes,
                self.inflight_bytes,
                "inflight_bytes",
            )?,
            inflight_blocks_per_worker: resolve_number(
                overrides.inflight_blocks_per_worker,
                self.inflight_blocks_per_worker,
                "inflight_blocks_per_worker",
            )?,
            stream_read_buffer: resolve_usize(
                overrides.stream_read_buffer,
                self.stream_read_buffer,
                "stream_read_buffer",
            )?,
            producer_threads: resolve_number(
                overrides.producer_threads,
                self.producer_threads,
                "producer_threads",
            )?,
            directory_mmap_threshold: resolve_usize(
                overrides.directory_mmap_threshold,
                self.directory_mmap_threshold,
                "directory_mmap_threshold",
            )?,
            writer_queue_blocks: resolve_number(
                overrides.writer_queue_blocks,
                self.writer_queue_blocks,
                "writer_queue_blocks",
            )?,
            result_wait_ms: resolve_number(
                overrides.result_wait_ms,
                self.result_wait_ms,
                "result_wait_ms",
            )?,
            raw_chunk_dedup_window_blocks,
            block_dedup_window_blocks,
        })
    }
}

fn validate_raw_chunk_dedup_window_blocks(value: usize) -> Result<(), io::Error> {
    if value > MAX_RAW_CHUNK_DEDUP_WINDOW_BLOCKS {
        return Err(invalid_input(format!(
            "invalid raw_chunk_dedup_window_blocks '{value}': expected a value between 0 and {MAX_RAW_CHUNK_DEDUP_WINDOW_BLOCKS}"
        )));
    }

    Ok(())
}

fn validate_block_dedup_window_blocks(value: usize) -> Result<(), io::Error> {
    if value > MAX_BLOCK_DEDUP_WINDOW_BLOCKS {
        return Err(invalid_input(format!(
            "invalid block_dedup_window_blocks '{value}': expected a value between 0 and {MAX_BLOCK_DEDUP_WINDOW_BLOCKS}"
        )));
    }

    Ok(())
}

fn parse_dictionary_mode(value: &str) -> Result<ArchiveDictionaryMode, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(ArchiveDictionaryMode::Off),
        "auto" => Ok(ArchiveDictionaryMode::Auto),
        other => Err(invalid_input(format!(
            "unknown dictionary_mode '{other}': expected off or auto"
        ))),
    }
}

fn parse_chunking_mode(value: &str) -> Result<ChunkingArg, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fixed" => Ok(ChunkingArg::Fixed),
        "cdc" => Ok(ChunkingArg::Cdc),
        other => Err(invalid_input(format!(
            "unknown chunking mode '{other}': expected fixed or cdc"
        ))),
    }
}

fn resolve_chunking_policy(
    block_size: usize,
    override_mode: Option<ChunkingArg>,
    override_min: Option<usize>,
    override_max: Option<usize>,
    preset_chunking: Option<&ChunkingConfig>,
) -> Result<ChunkingPolicy, io::Error> {
    let default_mode = preset_chunking
        .and_then(|chunking| chunking.mode.as_deref())
        .map(parse_chunking_mode)
        .transpose()?;
    let mode = override_mode.or(default_mode).unwrap_or(ChunkingArg::Fixed);

    let preset_min = preset_chunking
        .and_then(|chunking| chunking.min_block_size.as_ref())
        .map(|value| value.parse("chunking.min_block_size"))
        .transpose()?;
    let preset_max = preset_chunking
        .and_then(|chunking| chunking.max_block_size.as_ref())
        .map(|value| value.parse("chunking.max_block_size"))
        .transpose()?;

    Ok(match mode {
        ChunkingArg::Fixed => ChunkingPolicy::fixed_for_target(block_size),
        ChunkingArg::Cdc => ChunkingPolicy::cdc(
            block_size,
            override_min
                .or(preset_min)
                .unwrap_or((block_size / 4).max(1)),
            override_max
                .or(preset_max)
                .unwrap_or(block_size.saturating_mul(2).max(block_size)),
        ),
    })
}

impl CompressionConfig {
    fn merge_from(&mut self, other: &Self) {
        if other.compressor.is_some() {
            *self = Self::default();
        }

        merge_option(&mut self.compressor, other.compressor.clone());
        merge_option(&mut self.level, other.level);
        merge_option(&mut self.extreme, other.extreme);
        merge_option(&mut self.dictionary_size, other.dictionary_size.clone());
    }

    fn resolve(
        self,
        override_algo: Option<CompressionAlgo>,
        override_level: Option<i32>,
    ) -> Result<ResolvedCompressionSettings, io::Error> {
        let algo = match override_algo {
            Some(algo) => algo,
            None => self
                .compressor
                .as_deref()
                .map(parse_compression_algo)
                .transpose()?
                .ok_or_else(|| invalid_input("archive preset compression is missing compressor"))?,
        };
        let level = override_level.or(self.level);
        let extreme = self.extreme.unwrap_or(false);
        let lzma_dictionary_size = self
            .dictionary_size
            .as_ref()
            .map(|value| value.parse("compression.dictionary_size"))
            .transpose()?;
        validate_compression_settings(algo, level, extreme, lzma_dictionary_size)?;

        Ok(ResolvedCompressionSettings {
            algo,
            level,
            extreme,
            lzma_dictionary_size,
        })
    }
}

impl ChunkingConfig {
    fn merge_from(&mut self, other: &Self) {
        merge_option(&mut self.mode, other.mode.clone());
        merge_option(&mut self.min_block_size, other.min_block_size.clone());
        merge_option(&mut self.max_block_size, other.max_block_size.clone());
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedCompressionSettings {
    algo: CompressionAlgo,
    level: Option<i32>,
    extreme: bool,
    lzma_dictionary_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SizeValue {
    Text(String),
    Number(usize),
}

impl SizeValue {
    fn parse(&self, field: &str) -> Result<usize, io::Error> {
        match self {
            Self::Text(value) => parse_size(value).map_err(|error| {
                invalid_input(format!("invalid {field} value '{value}': {error}"))
            }),
            Self::Number(value) => Ok(*value),
        }
    }
}

fn resolve_usize(
    override_value: Option<usize>,
    config_value: Option<SizeValue>,
    field: &str,
) -> Result<usize, io::Error> {
    match override_value {
        Some(value) => Ok(value),
        None => config_value
            .ok_or_else(|| invalid_input(format!("archive preset is missing {field}")))?
            .parse(field),
    }
}

fn resolve_number<T: Copy>(
    override_value: Option<T>,
    config_value: Option<T>,
    field: &str,
) -> Result<T, io::Error> {
    override_value
        .or(config_value)
        .ok_or_else(|| invalid_input(format!("archive preset is missing {field}")))
}

fn parse_compression_algo(value: &str) -> Result<CompressionAlgo, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "lz4" => Ok(CompressionArg::Lz4.into()),
        "lzma" | "xz" => Ok(CompressionArg::Lzma.into()),
        "zstd" => Ok(CompressionArg::Zstd.into()),
        other => Err(invalid_input(format!("unsupported compression '{other}'"))),
    }
}

fn validate_compression_settings(
    compression: CompressionAlgo,
    level: Option<i32>,
    extreme: bool,
    lzma_dictionary_size: Option<usize>,
) -> Result<(), io::Error> {
    if extreme && compression != CompressionAlgo::Lzma {
        return Err(invalid_input(
            "compression.extreme is only supported when compression.compressor is 'lzma'",
        ));
    }

    if lzma_dictionary_size.is_some() && compression != CompressionAlgo::Lzma {
        return Err(invalid_input(
            "compression.dictionary_size is only supported when compression.compressor is 'lzma'",
        ));
    }

    if let Some(dictionary_size) = lzma_dictionary_size
        && dictionary_size < 4096
    {
        return Err(invalid_input(format!(
            "invalid compression.dictionary_size '{dictionary_size}' for lzma: expected at least 4096"
        )));
    }

    let Some(level) = level else {
        return Ok(());
    };

    match compression {
        CompressionAlgo::Lz4 => Err(invalid_input(
            "compression.level is not supported when compression.compressor is 'lz4'",
        )),
        CompressionAlgo::Zstd if !(1..=22).contains(&level) => Err(invalid_input(format!(
            "invalid compression.level '{level}' for zstd: expected an integer between 1 and 22"
        ))),
        CompressionAlgo::Lzma if !(1..=9).contains(&level) => Err(invalid_input(format!(
            "invalid compression.level '{level}' for lzma: expected an integer between 1 and 9"
        ))),
        _ => Ok(()),
    }
}

fn merge_option<T>(slot: &mut Option<T>, incoming: Option<T>) {
    if let Some(value) = incoming {
        *slot = Some(value);
    }
}

fn merge_nested_option<T>(
    slot: &mut Option<T>,
    incoming: Option<T>,
    merge: impl FnOnce(&mut T, T),
) {
    match (slot.as_mut(), incoming) {
        (Some(current), Some(incoming)) => merge(current, incoming),
        (None, Some(incoming)) => *slot = Some(incoming),
        _ => {}
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
#[path = "../tests/unit/presets.rs"]
mod tests;
