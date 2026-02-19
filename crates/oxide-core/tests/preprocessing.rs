use oxide_core::preprocessing::{audio_lpc, image_locoi, image_paeth, image_ycocgr};
use oxide_core::{
    AudioEndian, AudioMetadata, AudioSampleEncoding, AudioStrategy, ImageMetadata,
    ImagePixelFormat, ImageStrategy, OxideError, PreProcessingStrategy, PreprocessingMetadata,
    apply_preprocessing_with_metadata,
};

#[test]
fn image_paeth_converter_uses_pixel_metadata() {
    let metadata = ImageMetadata::packed(ImagePixelFormat::Bgr8).with_dimensions(2, 1);
    let raw = [10u8, 20, 30, 40, 50, 60];

    let pixels = image_paeth::bytes_to_data(&raw, &metadata).expect("conversion should succeed");

    assert_eq!(pixels, vec![[30, 20, 10], [60, 50, 40]]);
}

#[test]
fn image_ycocgr_converter_respects_row_stride() {
    let metadata = ImageMetadata::packed(ImagePixelFormat::Rgb8)
        .with_dimensions(1, 2)
        .with_row_stride(4);
    let raw = [1u8, 2, 3, 99, 4, 5, 6, 77];

    let pixels = image_ycocgr::bytes_to_data(&raw, &metadata).expect("conversion should succeed");

    assert_eq!(pixels, vec![[1, 2, 3], [4, 5, 6]]);
}

#[test]
fn image_locoi_converter_uses_metadata_for_grayscale() {
    let metadata = ImageMetadata::packed(ImagePixelFormat::Rgb8).with_dimensions(2, 1);
    let raw = [255u8, 0, 0, 0, 255, 0];

    let pixels = image_locoi::bytes_to_data(&raw, &metadata).expect("conversion should succeed");

    assert_eq!(pixels, vec![77, 149]);
}

#[test]
fn audio_lpc_converter_uses_audio_metadata() {
    let metadata = AudioMetadata::pcm_i16_le(2);
    let samples = [1000i16, -1000, 2000, -2000];
    let mut raw = Vec::new();
    for sample in samples {
        raw.extend_from_slice(&sample.to_le_bytes());
    }

    let decoded = audio_lpc::bytes_to_data(&raw, &metadata).expect("conversion should succeed");

    assert_eq!(decoded, samples);
}

#[test]
fn audio_lpc_converter_rejects_invalid_metadata() {
    let metadata = AudioMetadata {
        channels: 0,
        bytes_per_sample: 2,
        encoding: AudioSampleEncoding::SignedPcm,
        endian: AudioEndian::Little,
    };

    let err = audio_lpc::bytes_to_data(&[0, 0], &metadata).expect_err("metadata should fail");

    assert!(matches!(
        err,
        OxideError::InvalidFormat("audio channels must be non-zero")
    ));
}

#[test]
fn image_and_audio_apply_reverse_keep_raw_bytes() {
    let raw = vec![0u8, 1, 2, 3, 4, 5, 6, 7];
    let image_metadata = ImageMetadata::packed(ImagePixelFormat::Rgb8);
    let audio_metadata = AudioMetadata::pcm_i16_le(1);

    assert_eq!(
        image_paeth::apply(&raw, Some(&image_metadata)).expect("apply should succeed"),
        raw
    );
    assert_eq!(
        image_paeth::reverse(&raw).expect("reverse should succeed"),
        raw
    );
    assert_eq!(
        audio_lpc::apply(&raw, Some(&audio_metadata)).expect("apply should succeed"),
        raw
    );
    assert_eq!(
        audio_lpc::reverse(&raw).expect("reverse should succeed"),
        raw
    );
}

#[test]
fn preprocessing_router_validates_metadata_type_for_strategy() {
    let raw = [1u8, 2, 3, 4, 5, 6];
    let image_metadata =
        PreprocessingMetadata::Image(ImageMetadata::packed(ImagePixelFormat::Rgb8));
    let audio_metadata = PreprocessingMetadata::Audio(AudioMetadata::pcm_i16_le(1));

    let image_strategy = PreProcessingStrategy::Image(ImageStrategy::Paeth);
    let audio_strategy = PreProcessingStrategy::Audio(AudioStrategy::Lpc);

    let image_ok = apply_preprocessing_with_metadata(&raw, &image_strategy, Some(&image_metadata))
        .expect("image metadata should be accepted");
    assert_eq!(image_ok, raw);

    let audio_ok = apply_preprocessing_with_metadata(&raw, &audio_strategy, Some(&audio_metadata))
        .expect("audio metadata should be accepted");
    assert_eq!(audio_ok, raw);

    let image_err = apply_preprocessing_with_metadata(&raw, &image_strategy, Some(&audio_metadata))
        .expect_err("mismatched metadata should fail");
    assert!(matches!(
        image_err,
        OxideError::InvalidFormat("preprocessing metadata type mismatch for image strategy")
    ));

    let audio_err = apply_preprocessing_with_metadata(&raw, &audio_strategy, Some(&image_metadata))
        .expect_err("mismatched metadata should fail");
    assert!(matches!(
        audio_err,
        OxideError::InvalidFormat("preprocessing metadata type mismatch for audio strategy")
    ));
}
