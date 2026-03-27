use std::fs;

use oxide_core::CompressionAlgo;

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
              "compression": {
                "compressor": "lz4"
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "fast": {},
              "compact": {
                "block_size": "8M"
              }
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("compact").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let preset = merged
        .resolve(
            "compact",
            "fixture",
            ArchiveOverrides {
                block_size: Some(16 * 1024 * 1024),
                ..ArchiveOverrides::default()
            },
        )
        .expect("settings should resolve");

    assert_eq!(preset.profile_name, "compact");
    assert_eq!(preset.compression, CompressionAlgo::Lz4);
    assert_eq!(preset.compression_level, None);
    assert_eq!(preset.block_size, 16 * 1024 * 1024);
}

#[test]
fn default_preset_file_balanced_ultra_and_extreme_use_expected_codecs() {
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

    let mut extreme = file.archive.defaults.clone();
    extreme.merge_from(file.archive.presets.get("extreme").unwrap());
    let extreme = extreme
        .resolve("extreme", "default file", ArchiveOverrides::default())
        .expect("extreme should resolve");

    assert_eq!(balanced.compression, CompressionAlgo::Zstd);
    assert_eq!(balanced.compression_level, Some(6));
    assert!(!balanced.compression_extreme);
    assert_eq!(balanced.block_size, 2 * 1024 * 1024);
    assert_eq!(ultra.compression, CompressionAlgo::Lzma);
    assert_eq!(ultra.compression_level, Some(7));
    assert!(!ultra.compression_extreme);
    assert_eq!(ultra.lzma_dictionary_size, Some(2 * 1024 * 1024));
    assert_eq!(ultra.block_size, 2 * 1024 * 1024);
    assert_eq!(ultra.pool_capacity, 2 * 1024 * 1024);
    assert_eq!(extreme.compression, CompressionAlgo::Lzma);
    assert_eq!(extreme.compression_level, Some(9));
    assert!(!extreme.compression_extreme);
    assert_eq!(extreme.lzma_dictionary_size, Some(4 * 1024 * 1024));
    assert_eq!(extreme.block_size, 4 * 1024 * 1024);
    assert_eq!(extreme.pool_capacity, 4 * 1024 * 1024);
}

#[test]
fn resolves_maximum_lzma_compression_from_config() {
    let file = parse_fixture(
        r#"{
          "archive": {
            "default_preset": "compact",
            "defaults": {
              "compression": {
                "compressor": "lz4"
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "compact": {
                "compression": {
                  "compressor": "lzma",
                  "level": 9
                }
              }
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("compact").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let resolved = merged
        .resolve("compact", "fixture", ArchiveOverrides::default())
        .expect("settings should resolve");

    assert_eq!(resolved.compression, CompressionAlgo::Lzma);
    assert_eq!(resolved.compression_level, Some(9));
    assert!(!resolved.compression_extreme);
}

#[test]
fn resolves_lzma_compression_from_config() {
    let file = parse_fixture(
        r#"{
          "archive": {
            "default_preset": "maximum",
            "defaults": {
              "compression": {
                "compressor": "lz4"
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "maximum": {
                "compression": {
                  "compressor": "lzma",
                  "level": 9
                }
              }
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("maximum").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let resolved = merged
        .resolve("maximum", "fixture", ArchiveOverrides::default())
        .expect("settings should resolve");

    assert_eq!(resolved.compression, CompressionAlgo::Lzma);
    assert_eq!(resolved.compression_level, Some(9));
    assert!(!resolved.compression_extreme);
}

#[test]
fn resolves_lzma_extreme_from_config() {
    let file = parse_fixture(
        r#"{
          "archive": {
            "default_preset": "extreme",
            "defaults": {
              "compression": {
                "compressor": "lz4"
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "extreme": {
                "compression": {
                  "compressor": "lzma",
                  "level": 9,
                  "extreme": true
                }
              }
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("extreme").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let resolved = merged
        .resolve("extreme", "fixture", ArchiveOverrides::default())
        .expect("settings should resolve");

    assert_eq!(resolved.compression, CompressionAlgo::Lzma);
    assert_eq!(resolved.compression_level, Some(9));
    assert!(resolved.compression_extreme);
}

#[test]
fn rejects_extreme_for_non_lzma() {
    let file = parse_fixture(
        r#"{
          "archive": {
            "default_preset": "bad",
            "defaults": {
              "compression": {
                "compressor": "zstd",
                "level": 6,
                "extreme": true
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "bad": {}
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
        "compression.extreme is only supported when compression.compressor is 'lzma'"
    );
}

#[test]
fn rejects_level_for_lz4() {
    let file = parse_fixture(
        r#"{
          "archive": {
            "default_preset": "bad",
            "defaults": {
              "compression": {
                "compressor": "lz4"
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "bad": {
                "compression": {
                  "level": 7
                }
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
        "compression.level is not supported when compression.compressor is 'lz4'"
    );
}

#[test]
fn rejects_invalid_zstd_level() {
    let file = parse_fixture(
        r#"{
          "archive": {
            "default_preset": "bad",
            "defaults": {
              "compression": {
                "compressor": "zstd",
                "level": 99
              },
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
              "result_wait_ms": 1
            },
            "presets": {
              "bad": {}
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
        "invalid compression.level '99' for zstd: expected an integer between 1 and 22"
    );
}
