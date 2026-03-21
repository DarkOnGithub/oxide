use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use oxide_core::{
    AudioStrategy, BinaryStrategy, CompressionAlgo, CompressionPreset, ImageStrategy,
    PreprocessingProfile, TextStrategy,
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
    pub skip_preprocessing: bool,
    pub skip_compression: bool,
    pub compression_preset: CompressionPreset,
    pub preprocessing_profile: PreprocessingProfile,
    pub zstd_level: Option<i32>,
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
    pub preserve_format_boundaries: bool,
    pub result_wait_ms: u64,
}

#[derive(Debug, Default)]
pub struct ArchiveOverrides {
    pub compression: Option<CompressionAlgo>,
    pub skip_preprocessing: bool,
    pub skip_compression: bool,
    pub zstd_level: Option<i32>,
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
    pub preserve_format_boundaries: Option<bool>,
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
#[serde(default)]
struct ArchivePresetConfig {
    compression: Option<String>,
    compression_preset: Option<String>,
    text_preprocessing: Option<String>,
    image_preprocessing: Option<String>,
    audio_preprocessing: Option<String>,
    binary_preprocessing: Option<String>,
    zstd_level: Option<i32>,
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
    preserve_format_boundaries: Option<bool>,
    result_wait_ms: Option<u64>,
}

impl ArchivePresetConfig {
    fn merge_from(&mut self, other: &Self) {
        merge_option(&mut self.compression, other.compression.clone());
        merge_option(
            &mut self.compression_preset,
            other.compression_preset.clone(),
        );
        merge_option(
            &mut self.text_preprocessing,
            other.text_preprocessing.clone(),
        );
        merge_option(
            &mut self.image_preprocessing,
            other.image_preprocessing.clone(),
        );
        merge_option(
            &mut self.audio_preprocessing,
            other.audio_preprocessing.clone(),
        );
        merge_option(
            &mut self.binary_preprocessing,
            other.binary_preprocessing.clone(),
        );
        merge_option(&mut self.zstd_level, other.zstd_level);
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
        merge_option(
            &mut self.preserve_format_boundaries,
            other.preserve_format_boundaries,
        );
        merge_option(&mut self.result_wait_ms, other.result_wait_ms);
    }

    fn resolve(
        self,
        preset_name: &str,
        source_label: &str,
        overrides: ArchiveOverrides,
    ) -> AppResult<ResolvedArchiveSettings> {
        let compression = match overrides.compression {
            Some(compression) => compression,
            None => self
                .compression
                .as_deref()
                .map(parse_compression_algo)
                .transpose()?
                .ok_or_else(|| invalid_input("archive preset is missing compression"))?,
        };
        let compression_preset = self
            .compression_preset
            .as_deref()
            .map(parse_compression_preset)
            .transpose()?
            .ok_or_else(|| invalid_input("archive preset is missing compression_preset"))?;
        let preprocessing_profile = PreprocessingProfile::new(
            resolve_text_preprocessing(self.text_preprocessing.as_deref())?,
            resolve_image_preprocessing(self.image_preprocessing.as_deref())?,
            resolve_audio_preprocessing(self.audio_preprocessing.as_deref())?,
            resolve_binary_preprocessing(self.binary_preprocessing.as_deref())?,
        );
        let zstd_level = resolve_zstd_level(compression, overrides.zstd_level.or(self.zstd_level))?;

        Ok(ResolvedArchiveSettings {
            profile_name: preset_name.to_string(),
            profile_source: source_label.to_string(),
            compression,
            skip_preprocessing: overrides.skip_preprocessing,
            skip_compression: overrides.skip_compression,
            compression_preset,
            preprocessing_profile,
            zstd_level,
            block_size: resolve_usize(overrides.block_size, self.block_size, "block_size")?,
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
            preserve_format_boundaries: resolve_number(
                overrides.preserve_format_boundaries,
                self.preserve_format_boundaries,
                "preserve_format_boundaries",
            )?,
            result_wait_ms: resolve_number(
                overrides.result_wait_ms,
                self.result_wait_ms,
                "result_wait_ms",
            )?,
        })
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
        "zstd" => Ok(CompressionArg::Zstd.into()),
        other => Err(invalid_input(format!("unsupported compression '{other}'"))),
    }
}

fn parse_compression_preset(value: &str) -> Result<CompressionPreset, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fast" => Ok(CompressionPreset::Fast),
        "default" | "balanced" => Ok(CompressionPreset::Default),
        "high" | "compact" => Ok(CompressionPreset::High),
        other => Err(invalid_input(format!(
            "unsupported compression_preset '{other}'"
        ))),
    }
}

fn resolve_text_preprocessing(value: Option<&str>) -> Result<Option<TextStrategy>, io::Error> {
    parse_required_preprocessing(value, "text_preprocessing", parse_text_preprocessing)
}

fn resolve_image_preprocessing(value: Option<&str>) -> Result<Option<ImageStrategy>, io::Error> {
    parse_required_preprocessing(value, "image_preprocessing", parse_image_preprocessing)
}

fn resolve_audio_preprocessing(value: Option<&str>) -> Result<Option<AudioStrategy>, io::Error> {
    parse_required_preprocessing(value, "audio_preprocessing", parse_audio_preprocessing)
}

fn resolve_binary_preprocessing(value: Option<&str>) -> Result<Option<BinaryStrategy>, io::Error> {
    parse_required_preprocessing(value, "binary_preprocessing", parse_binary_preprocessing)
}

fn parse_required_preprocessing<T>(
    value: Option<&str>,
    field: &str,
    parser: impl FnOnce(&str) -> Result<Option<T>, io::Error>,
) -> Result<Option<T>, io::Error> {
    match value {
        Some(value) => parser(value),
        None => Err(invalid_input(format!("archive preset is missing {field}"))),
    }
}

fn parse_text_preprocessing(value: &str) -> Result<Option<TextStrategy>, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(None),
        "bpe" => Ok(Some(TextStrategy::Bpe)),
        "bwt" => Ok(Some(TextStrategy::Bwt)),
        other => Err(invalid_input(format!(
            "unsupported text_preprocessing '{other}'"
        ))),
    }
}

fn parse_image_preprocessing(value: &str) -> Result<Option<ImageStrategy>, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(None),
        "ycocgr" => Ok(Some(ImageStrategy::YCoCgR)),
        "paeth" => Ok(Some(ImageStrategy::Paeth)),
        "locoi" => Ok(Some(ImageStrategy::LocoI)),
        other => Err(invalid_input(format!(
            "unsupported image_preprocessing '{other}'"
        ))),
    }
}

fn parse_audio_preprocessing(value: &str) -> Result<Option<AudioStrategy>, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(None),
        "lpc" => Ok(Some(AudioStrategy::Lpc)),
        other => Err(invalid_input(format!(
            "unsupported audio_preprocessing '{other}'"
        ))),
    }
}

fn parse_binary_preprocessing(value: &str) -> Result<Option<BinaryStrategy>, io::Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(None),
        "bcj" => Ok(Some(BinaryStrategy::Bcj)),
        other => Err(invalid_input(format!(
            "unsupported binary_preprocessing '{other}'"
        ))),
    }
}

fn resolve_zstd_level(
    compression: CompressionAlgo,
    zstd_level: Option<i32>,
) -> Result<Option<i32>, io::Error> {
    let Some(level) = zstd_level else {
        return Ok(None);
    };

    if compression != CompressionAlgo::Zstd {
        return Err(invalid_input(
            "zstd_level can only be used when compression is 'zstd'",
        ));
    }

    if !(1..=22).contains(&level) {
        return Err(invalid_input(format!(
            "invalid zstd_level '{level}': expected an integer between 1 and 22"
        )));
    }

    Ok(Some(level))
}

fn merge_option<T>(slot: &mut Option<T>, incoming: Option<T>) {
    if let Some(value) = incoming {
        *slot = Some(value);
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use oxide_core::{
        CompressionAlgo, CompressionPreset, ImageStrategy, PreprocessingProfile, TextStrategy,
    };

    use super::{ArchiveOverrides, DEFAULT_PRESETS_PATH, PresetFile};

    fn parse_fixture(json: &str) -> PresetFile {
        serde_json::from_str(json).expect("fixture should parse")
    }

    #[test]
    fn resolve_uses_named_preset_and_cli_overrides() {
        let file = parse_fixture(
            r#"{
              "archive": {
                "default_preset": "fast",
                "defaults": {
                  "compression": "lz4",
                  "compression_preset": "fast",
                  "text_preprocessing": "none",
                  "image_preprocessing": "none",
                  "audio_preprocessing": "none",
                  "binary_preprocessing": "none",
                  "zstd_level": null,
                  "block_size": "2M",
                  "workers": 0,
                  "pool_capacity": "1M",
                  "pool_buffers": 512,
                  "stats_interval_ms": 250,
                  "inflight_bytes": "2G",
                  "inflight_blocks_per_worker": 256,
                  "stream_read_buffer": "64M",
                  "producer_threads": 1,
                  "directory_mmap_threshold": "8M",
                  "writer_queue_blocks": 1024,
                  "preserve_format_boundaries": false,
                  "result_wait_ms": 1
                },
                "presets": {
                  "fast": {
                    "compression_preset": "fast"
                  },
                  "compact": {
                    "compression_preset": "high",
                    "text_preprocessing": "bwt",
                    "image_preprocessing": "locoi",
                    "block_size": "8M"
                  }
                }
              }
            }"#,
        );

        let preset = file
            .archive
            .presets
            .get("compact")
            .expect("compact preset should exist");
        let mut merged = file.archive.defaults.clone();
        merged.merge_from(preset);
        let preset = merged
            .resolve(
                "compact",
                "fixture",
                ArchiveOverrides {
                    zstd_level: None,
                    block_size: Some(16 * 1024 * 1024),
                    ..ArchiveOverrides::default()
                },
            )
            .expect("settings should resolve");

        assert_eq!(preset.profile_name, "compact");
        assert_eq!(preset.compression, CompressionAlgo::Lz4);
        assert!(!preset.skip_preprocessing);
        assert!(!preset.skip_compression);
        assert_eq!(preset.compression_preset, CompressionPreset::High);
        assert_eq!(
            preset.preprocessing_profile,
            PreprocessingProfile::new(
                Some(TextStrategy::Bwt),
                Some(ImageStrategy::LocoI),
                None,
                None,
            )
        );
        assert_eq!(preset.zstd_level, None);
        assert_eq!(preset.block_size, 16 * 1024 * 1024);
    }

    #[test]
    fn default_preset_file_balanced_and_ultra_use_zstd() {
        let file = parse_fixture(
            &fs::read_to_string(DEFAULT_PRESETS_PATH).expect("default presets should be readable"),
        );

        let mut balanced = file.archive.defaults.clone();
        balanced.merge_from(file.archive.presets.get("balanced").unwrap());
        let balanced = balanced
            .resolve("balanced", "default file", ArchiveOverrides::default())
            .expect("balanced should resolve");

        let mut ultra = file.archive.defaults.clone();
        ultra.merge_from(file.archive.presets.get("ultra").unwrap());
        let ultra = ultra
            .resolve("ultra", "default file", ArchiveOverrides::default())
            .expect("ultra should resolve");

        assert_eq!(balanced.compression, CompressionAlgo::Zstd);
        assert_eq!(balanced.compression_preset, CompressionPreset::Default);
        assert_eq!(
            balanced.preprocessing_profile,
            PreprocessingProfile::for_compression_preset(CompressionPreset::Default)
        );
        assert_eq!(balanced.zstd_level, Some(6));
        assert_eq!(ultra.compression, CompressionAlgo::Zstd);
        assert_eq!(ultra.compression_preset, CompressionPreset::High);
        assert_eq!(
            ultra.preprocessing_profile,
            PreprocessingProfile::for_compression_preset(CompressionPreset::High)
        );
        assert_eq!(ultra.zstd_level, Some(19));
    }

    #[test]
    fn rejects_zstd_level_for_non_zstd_compression() {
        let file = parse_fixture(
            r#"{
              "archive": {
                "default_preset": "bad",
                "defaults": {
                  "compression": "lz4",
                  "compression_preset": "fast",
                  "text_preprocessing": "none",
                  "image_preprocessing": "none",
                  "audio_preprocessing": "none",
                  "binary_preprocessing": "none",
                  "block_size": "2M",
                  "workers": 0,
                  "pool_capacity": "1M",
                  "pool_buffers": 512,
                  "stats_interval_ms": 250,
                  "inflight_bytes": "2G",
                  "inflight_blocks_per_worker": 256,
                  "stream_read_buffer": "64M",
                  "producer_threads": 1,
                  "directory_mmap_threshold": "8M",
                  "writer_queue_blocks": 1024,
                  "preserve_format_boundaries": false,
                  "result_wait_ms": 1
                },
                "presets": {
                  "bad": {
                    "zstd_level": 7
                  }
                }
              }
            }"#,
        );

        let preset = file.archive.presets.get("bad").unwrap();
        let mut merged = file.archive.defaults.clone();
        merged.merge_from(preset);

        let error = merged
            .resolve("bad", "fixture", ArchiveOverrides::default())
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "zstd_level can only be used when compression is 'zstd'"
        );
    }

    #[test]
    fn rejects_unknown_preprocessing_strategy() {
        let file = parse_fixture(
            r#"{
              "archive": {
                "default_preset": "bad",
                "defaults": {
                  "compression": "lz4",
                  "compression_preset": "fast",
                  "text_preprocessing": "none",
                  "image_preprocessing": "none",
                  "audio_preprocessing": "none",
                  "binary_preprocessing": "none",
                  "block_size": "2M",
                  "workers": 0,
                  "pool_capacity": "1M",
                  "pool_buffers": 512,
                  "stats_interval_ms": 250,
                  "inflight_bytes": "2G",
                  "inflight_blocks_per_worker": 256,
                  "stream_read_buffer": "64M",
                  "producer_threads": 1,
                  "directory_mmap_threshold": "8M",
                  "writer_queue_blocks": 1024,
                  "preserve_format_boundaries": false,
                  "result_wait_ms": 1
                },
                "presets": {
                  "bad": {
                    "text_preprocessing": "zip"
                  }
                }
              }
            }"#,
        );

        let preset = file.archive.presets.get("bad").unwrap();
        let mut merged = file.archive.defaults.clone();
        merged.merge_from(preset);

        let error = merged
            .resolve("bad", "fixture", ArchiveOverrides::default())
            .unwrap_err();
        assert_eq!(error.to_string(), "unsupported text_preprocessing 'zip'");
    }
}
