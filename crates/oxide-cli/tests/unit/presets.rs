use std::path::PathBuf;

use oxide_core::{ChunkingPolicy, CompressionAlgo};

use super::{ArchiveOverrides, PresetFile};

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
              "result_wait_ms": 1,
              "dictionary_from": "seed.oxz"
              ,"chunking": {
                "mode": "fixed"
              }
            },
            "presets": {
              "fast": {},
              "compact": {
                "block_size": "8M",
                "chunking": {
                  "mode": "cdc",
                  "min_block_size": "2M",
                  "max_block_size": "16M"
                }
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
    assert_eq!(preset.dictionary_from, Some(PathBuf::from("seed.oxz")));
    assert_eq!(
        preset.chunking_policy,
        ChunkingPolicy::cdc(16 * 1024 * 1024, 2 * 1024 * 1024, 16 * 1024 * 1024)
    );
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

#[test]
fn resolves_raw_and_block_dedup_window_blocks_independently() {
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
              "result_wait_ms": 1,
              "raw_chunk_dedup_window_blocks": 4096,
              "block_dedup_window_blocks": 2048
            },
            "presets": {
              "fast": {
                "raw_chunk_dedup_window_blocks": 0,
                "block_dedup_window_blocks": 64
              }
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("fast").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let resolved = merged
        .resolve("fast", "fixture", ArchiveOverrides::default())
        .expect("settings should resolve");

    assert_eq!(resolved.raw_chunk_dedup_window_blocks, 0);
    assert_eq!(resolved.block_dedup_window_blocks, 64);
}

#[test]
fn resolves_block_dedup_window_blocks_from_config() {
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
              "result_wait_ms": 1,
              "block_dedup_window_blocks": 4096
            },
            "presets": {
              "fast": {
                "block_dedup_window_blocks": 0
              }
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("fast").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let resolved = merged
        .resolve("fast", "fixture", ArchiveOverrides::default())
        .expect("settings should resolve");

    assert_eq!(resolved.block_dedup_window_blocks, 0);
}

#[test]
fn rejects_raw_chunk_dedup_window_blocks_above_limit() {
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
              "result_wait_ms": 1,
              "raw_chunk_dedup_window_blocks": 1048577
            },
            "presets": {
              "fast": {}
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("fast").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let error = merged
        .resolve("fast", "fixture", ArchiveOverrides::default())
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid raw_chunk_dedup_window_blocks '1048577': expected a value between 0 and 1048576"
    );
}

#[test]
fn rejects_block_dedup_window_blocks_above_limit() {
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
              "result_wait_ms": 1,
              "block_dedup_window_blocks": 1048577
            },
            "presets": {
              "fast": {}
            }
          }
        }"#,
    );

    let preset = file.archive.presets.get("fast").unwrap();
    let mut merged = file.archive.defaults.clone();
    merged.merge_from(preset);
    let error = merged
        .resolve("fast", "fixture", ArchiveOverrides::default())
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "invalid block_dedup_window_blocks '1048577': expected a value between 0 and 1048576"
    );
}
