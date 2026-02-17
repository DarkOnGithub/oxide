use oxide_core::{
    AudioStrategy, BinaryStrategy, CompressionAlgo, ImageStrategy, PreProcessingStrategy,
    TextStrategy, apply_compression, apply_preprocessing, reverse_compression,
    reverse_preprocessing,
};

fn fixture() -> Vec<u8> {
    b"Oxide preprocessing/compression stub fixture bytes\n\x00\x01\x02".to_vec()
}

fn all_preprocessing_strategies() -> Vec<PreProcessingStrategy> {
    vec![
        PreProcessingStrategy::None,
        PreProcessingStrategy::Text(TextStrategy::Bpe),
        PreProcessingStrategy::Text(TextStrategy::Bwt),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR),
        PreProcessingStrategy::Image(ImageStrategy::Paeth),
        PreProcessingStrategy::Image(ImageStrategy::LocoI),
        PreProcessingStrategy::Audio(AudioStrategy::Lpc),
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj),
    ]
}

#[test]
fn preprocessing_router_apply_returns_original_for_all_strategies() {
    let input = fixture();
    for strategy in all_preprocessing_strategies() {
        let output = apply_preprocessing(&input, &strategy).expect("apply should succeed");
        assert_eq!(
            output, input,
            "strategy should be pass-through: {strategy:?}"
        );
    }
}

#[test]
fn preprocessing_router_reverse_returns_original_for_all_strategies() {
    let input = fixture();
    for strategy in all_preprocessing_strategies() {
        let output = reverse_preprocessing(&input, &strategy).expect("reverse should succeed");
        assert_eq!(
            output, input,
            "strategy should be pass-through: {strategy:?}"
        );
    }
}

#[test]
fn preprocessing_round_trip_is_identity_for_all_strategies() {
    let input = fixture();
    for strategy in all_preprocessing_strategies() {
        let preprocessed = apply_preprocessing(&input, &strategy).expect("apply should succeed");
        let restored =
            reverse_preprocessing(&preprocessed, &strategy).expect("reverse should succeed");
        assert_eq!(
            restored, input,
            "round-trip should be identity: {strategy:?}"
        );
    }
}

#[test]
fn compression_router_apply_is_codec_specific() {
    let input = fixture();
    let lz4 = apply_compression(&input, CompressionAlgo::Lz4).expect("lz4 apply should succeed");
    assert_ne!(lz4, input, "lz4 should transform bytes");

    let lzma = apply_compression(&input, CompressionAlgo::Lzma).expect("lzma apply should succeed");
    assert_ne!(lzma, input, "lzma should transform bytes");

    let deflate =
        apply_compression(&input, CompressionAlgo::Deflate).expect("deflate apply should succeed");
    assert_ne!(deflate, input, "deflate should transform bytes");
}

#[test]
fn compression_router_reverse_fails_on_non_codec_payloads() {
    let input = fixture();
    for algo in [
        CompressionAlgo::Lz4,
        CompressionAlgo::Lzma,
        CompressionAlgo::Deflate,
    ] {
        assert!(
            reverse_compression(&input, algo).is_err(),
            "reverse should fail for non-{algo:?} payload"
        );
    }
}

#[test]
fn compression_round_trip_is_identity_for_all_algorithms() {
    let input = fixture();
    for algo in [
        CompressionAlgo::Lz4,
        CompressionAlgo::Lzma,
        CompressionAlgo::Deflate,
    ] {
        let compressed = apply_compression(&input, algo).expect("apply should succeed");
        let restored = reverse_compression(&compressed, algo).expect("reverse should succeed");
        assert_eq!(restored, input, "round-trip should be identity: {algo:?}");
    }
}
