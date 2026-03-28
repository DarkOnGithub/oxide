use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use oxide_core::{
    ArchiveDictionaryMode, ChunkingMode, ChunkingPolicy, CompressionAlgo,
    ZstdCompressionParameters, ZstdStrategy,
};
use serde::Deserialize;

use crate::AppResult;
use crate::cli::{CompressionArg, parse_size};

const DEFAULT_PRESETS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/presets.json");

#[derive(Debug, Clone)]
pub struct ResolvedArchiveSettings {
    pub profile_name: String,
    pub profile_source: String,
    pub compression: CompressionAlgo,
    pub skip_compression: bool,
    pub dictionary_mode: ArchiveDictionaryMode,
    pub compression_level: Option<i32>,
    pub compression_extreme: bool,
    pub lzma_dictionary_size: Option<usize>,
    pub zstd_parameters: ZstdCompressionParameters,
    pub chunking_policy: ChunkingPolicy,
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
}

#[derive(Debug, Default)]
pub struct ArchiveOverrides {
    pub compression: Option<CompressionAlgo>,
    pub skip_compression: bool,
    pub dictionary_mode: Option<ArchiveDictionaryMode>,
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
    chunking: Option<ChunkingConfig>,
    dictionary_mode: Option<String>,
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
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompressionConfig {
    compressor: Option<String>,
    level: Option<i32>,
    extreme: Option<bool>,
    dictionary_size: Option<SizeValue>,
    window_log: Option<u32>,
    strategy: Option<String>,
    long_distance_matching: Option<bool>,
    ldm_hash_log: Option<u32>,
    ldm_min_match: Option<u32>,
    ldm_bucket_size_log: Option<u32>,
    ldm_hash_rate_log: Option<u32>,
    job_size: Option<u32>,
    overlap_log: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChunkingConfig {
    mode: Option<String>,
    min_size: Option<SizeValue>,
    max_size: Option<SizeValue>,
    superchunk_size: Option<SizeValue>,
    cdc_mask_bits: Option<u32>,
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
        merge_nested_option(
            &mut self.chunking,
            other.chunking.clone(),
            |current, incoming| {
                current.merge_from(&incoming);
            },
        );
        merge_option(&mut self.dictionary_mode, other.dictionary_mode.clone());
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
        let chunking_policy = self
            .chunking
            .as_ref()
            .map(|chunking| chunking.resolve(block_size))
            .transpose()?
            .unwrap_or_else(|| ChunkingPolicy::fixed_for_target(block_size));

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
            compression_level: compression.level,
            compression_extreme: compression.extreme,
            lzma_dictionary_size: compression.lzma_dictionary_size,
            zstd_parameters: compression.zstd_parameters,
            chunking_policy,
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
        })
    }
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

impl CompressionConfig {
    fn merge_from(&mut self, other: &Self) {
        if other.compressor.is_some() {
            *self = Self::default();
        }

        merge_option(&mut self.compressor, other.compressor.clone());
        merge_option(&mut self.level, other.level);
        merge_option(&mut self.extreme, other.extreme);
        merge_option(&mut self.dictionary_size, other.dictionary_size.clone());
        merge_option(&mut self.window_log, other.window_log);
        merge_option(&mut self.strategy, other.strategy.clone());
        merge_option(
            &mut self.long_distance_matching,
            other.long_distance_matching,
        );
        merge_option(&mut self.ldm_hash_log, other.ldm_hash_log);
        merge_option(&mut self.ldm_min_match, other.ldm_min_match);
        merge_option(&mut self.ldm_bucket_size_log, other.ldm_bucket_size_log);
        merge_option(&mut self.ldm_hash_rate_log, other.ldm_hash_rate_log);
        merge_option(&mut self.job_size, other.job_size);
        merge_option(&mut self.overlap_log, other.overlap_log);
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
        let zstd_parameters = ZstdCompressionParameters {
            window_log: self.window_log,
            strategy: self
                .strategy
                .as_deref()
                .map(parse_zstd_strategy)
                .transpose()?,
            enable_long_distance_matching: self.long_distance_matching,
            ldm_hash_log: self.ldm_hash_log,
            ldm_min_match: self.ldm_min_match,
            ldm_bucket_size_log: self.ldm_bucket_size_log,
            ldm_hash_rate_log: self.ldm_hash_rate_log,
            job_size: self.job_size,
            overlap_log: self.overlap_log,
        };
        validate_compression_settings(algo, level, extreme, lzma_dictionary_size, zstd_parameters)?;

        Ok(ResolvedCompressionSettings {
            algo,
            level,
            extreme,
            lzma_dictionary_size,
            zstd_parameters,
        })
    }
}

impl ChunkingConfig {
    fn merge_from(&mut self, other: &Self) {
        merge_option(&mut self.mode, other.mode.clone());
        merge_option(&mut self.min_size, other.min_size.clone());
        merge_option(&mut self.max_size, other.max_size.clone());
        merge_option(&mut self.superchunk_size, other.superchunk_size.clone());
        merge_option(&mut self.cdc_mask_bits, other.cdc_mask_bits);
    }

    fn resolve(&self, block_size: usize) -> Result<ChunkingPolicy, io::Error> {
        let mode = match self
            .mode
            .as_deref()
            .unwrap_or("fixed")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "fixed" => ChunkingMode::Fixed,
            "cdc" | "content_defined" | "content-defined" => ChunkingMode::ContentDefined,
            other => {
                return Err(invalid_input(format!(
                    "unknown chunking.mode '{other}': expected fixed or cdc"
                )));
            }
        };

        let min_size = self
            .min_size
            .as_ref()
            .map(|value| value.parse("chunking.min_size"))
            .transpose()?;
        let max_size = self
            .max_size
            .as_ref()
            .map(|value| value.parse("chunking.max_size"))
            .transpose()?;
        let superchunk_size = self
            .superchunk_size
            .as_ref()
            .map(|value| value.parse("chunking.superchunk_size"))
            .transpose()?;

        let mut policy = match mode {
            ChunkingMode::Fixed => ChunkingPolicy::fixed_for_target(block_size),
            ChunkingMode::ContentDefined => ChunkingPolicy::content_defined_for_target(block_size),
        };

        if let Some(min_size) = min_size {
            policy.min_size = min_size.max(1);
        }
        if let Some(max_size) = max_size {
            policy.max_size = max_size.max(policy.min_size).max(block_size.max(1));
        }
        if let Some(superchunk_size) = superchunk_size {
            policy.superchunk_size = superchunk_size.max(policy.max_size);
        }
        if let Some(mask_bits) = self.cdc_mask_bits {
            if mode != ChunkingMode::ContentDefined {
                return Err(invalid_input(
                    "chunking.cdc_mask_bits is only supported when chunking.mode is 'cdc'",
                ));
            }
            policy = policy.with_cdc_mask_bits(mask_bits);
        }

        if policy.min_size > policy.target_size {
            return Err(invalid_input(
                "chunking.min_size must be less than or equal to block_size",
            ));
        }
        if policy.max_size < policy.target_size {
            return Err(invalid_input(
                "chunking.max_size must be greater than or equal to block_size",
            ));
        }

        Ok(policy)
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedCompressionSettings {
    algo: CompressionAlgo,
    level: Option<i32>,
    extreme: bool,
    lzma_dictionary_size: Option<usize>,
    zstd_parameters: ZstdCompressionParameters,
}

fn parse_zstd_strategy(value: &str) -> Result<ZstdStrategy, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fast" => Ok(ZstdStrategy::Fast),
        "dfast" => Ok(ZstdStrategy::Dfast),
        "greedy" => Ok(ZstdStrategy::Greedy),
        "lazy" => Ok(ZstdStrategy::Lazy),
        "lazy2" => Ok(ZstdStrategy::Lazy2),
        "btlazy2" => Ok(ZstdStrategy::Btlazy2),
        "btopt" => Ok(ZstdStrategy::Btopt),
        "btultra" => Ok(ZstdStrategy::Btultra),
        "btultra2" => Ok(ZstdStrategy::Btultra2),
        other => Err(invalid_input(format!(
            "unknown compression.strategy '{other}' for zstd"
        ))),
    }
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
    zstd_parameters: ZstdCompressionParameters,
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

    if !zstd_parameters.is_default() && compression != CompressionAlgo::Zstd {
        return Err(invalid_input(
            "advanced zstd compression settings are only supported when compression.compressor is 'zstd'",
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
