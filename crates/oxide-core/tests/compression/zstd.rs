use super::{
    ZSTD_DEFAULT_LEVEL, apply_into_vec, apply_with_scratch, resolve_level, reverse_into_vec,
    reverse_with_scratch,
};
use crate::compression::scratch::ZstdScratch;
use crate::{ZstdCompressionParameters, ZstdStrategy};

#[test]
fn default_level_is_used_when_omitted() {
    assert_eq!(resolve_level(None).unwrap(), ZSTD_DEFAULT_LEVEL);
}

#[test]
fn explicit_level_is_respected() {
    assert_eq!(resolve_level(Some(19)).unwrap(), 19);
}

#[test]
fn reusable_scratch_supports_multiple_levels_and_round_trips() {
    let payload = b"banana bandana banana";
    let mut scratch = ZstdScratch::default();

    let balanced = apply_with_scratch(
        payload,
        Some(3),
        ZstdCompressionParameters::default(),
        0,
        None,
        &mut scratch,
    )
    .expect("zstd compression should succeed");
    let ultra = apply_with_scratch(
        payload,
        Some(19),
        ZstdCompressionParameters::default(),
        0,
        None,
        &mut scratch,
    )
    .expect("zstd compression should succeed");

    let balanced_decoded =
        reverse_with_scratch(&balanced, Some(payload.len()), 0, None, &mut scratch)
            .expect("zstd decompression should succeed");
    let ultra_decoded = reverse_with_scratch(&ultra, Some(payload.len()), 0, None, &mut scratch)
        .expect("zstd decompression should succeed");

    assert_eq!(balanced_decoded, payload);
    assert_eq!(ultra_decoded, payload);
}

#[test]
fn direct_buffer_round_trip_grows_undersized_vecs() {
    let payload: Vec<u8> = (0..16_384).map(|idx| ((idx * 73) % 251) as u8).collect();
    let mut scratch = ZstdScratch::default();

    let mut compressed = Vec::with_capacity(512);
    apply_into_vec(
        &payload,
        Some(3),
        ZstdCompressionParameters::default(),
        0,
        None,
        &mut scratch,
        &mut compressed,
    )
    .expect("zstd compression should grow undersized output");

    let mut decoded = Vec::with_capacity(1024);
    reverse_into_vec(
        &compressed,
        Some(payload.len()),
        0,
        None,
        &mut scratch,
        &mut decoded,
    )
    .expect("zstd decompression should grow undersized output");

    assert_eq!(decoded, payload);
}

#[test]
fn advanced_parameters_round_trip() {
    let payload = b"banana bandana banana banana banana";
    let mut scratch = ZstdScratch::default();
    let params = ZstdCompressionParameters {
        strategy: Some(ZstdStrategy::Lazy),
        window_log: Some(20),
        ..ZstdCompressionParameters::default()
    };

    let compressed = apply_with_scratch(payload, Some(6), params, 0, None, &mut scratch)
        .expect("zstd compression with advanced params should succeed");
    let decoded = reverse_with_scratch(&compressed, Some(payload.len()), 0, None, &mut scratch)
        .expect("zstd decompression should succeed");

    assert_eq!(decoded, payload);
}
