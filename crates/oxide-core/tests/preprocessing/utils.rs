use oxide_core::preprocessing::utils::*;
use oxide_core::OxideError;

#[test]
fn converts_bgra_to_rgb_using_metadata() {
    let metadata = ImageMetadata::packed(ImagePixelFormat::Bgra8).with_dimensions(2, 1);
    let input = [10u8, 20, 30, 255, 40, 50, 60, 255];
    let output = bytes_to_rgb_pixels(&input, &metadata).unwrap();
    assert_eq!(output, vec![[30, 20, 10], [60, 50, 40]]);
}

#[test]
fn converts_rgb_to_grayscale_using_luma() {
    let metadata = ImageMetadata::packed(ImagePixelFormat::Rgb8).with_dimensions(1, 2);
    let input = [255u8, 0, 0, 0, 255, 0];
    let output = bytes_to_grayscale_pixels(&input, &metadata).unwrap();
    assert_eq!(output, vec![77, 149]);
}

#[test]
fn rejects_incomplete_image_metadata_dimensions() {
    let metadata = ImageMetadata {
        pixel_format: ImagePixelFormat::Rgb8,
        width: None,
        height: Some(2),
        row_stride: None,
    };
    let err = bytes_to_rgb_pixels(&[0u8; 6], &metadata).unwrap_err();
    assert!(matches!(
        err,
        OxideError::InvalidFormat("image metadata requires width and height together")
    ));
}

#[test]
fn supports_row_layout_without_total_height() {
    let metadata = ImageMetadata::packed(ImagePixelFormat::Rgb8).with_row_layout(2, 8);
    let input = [
        255u8, 0, 0, 0, 255, 0, 99, 88, 0, 0, 255, 20, 30, 40, 77, 66,
    ];
    let output = bytes_to_rgb_pixels(&input, &metadata).unwrap();
    assert_eq!(
        output,
        vec![[255, 0, 0], [0, 255, 0], [0, 0, 255], [20, 30, 40]]
    );
}

#[test]
fn converts_unsigned_8bit_audio_to_i16() {
    let metadata = AudioMetadata {
        channels: 1,
        bytes_per_sample: 1,
        encoding: AudioSampleEncoding::UnsignedPcm,
        endian: AudioEndian::Little,
    };
    let output = bytes_to_i16_samples(&[0u8, 128, 255], &metadata).unwrap();
    assert_eq!(output, vec![-32768, 0, 32512]);
}

#[test]
fn converts_f32_audio_to_i16() {
    let metadata = AudioMetadata {
        channels: 1,
        bytes_per_sample: 4,
        encoding: AudioSampleEncoding::Float,
        endian: AudioEndian::Little,
    };

    let mut input = Vec::new();
    input.extend_from_slice(&(-1.0f32).to_le_bytes());
    input.extend_from_slice(&(0.0f32).to_le_bytes());
    input.extend_from_slice(&(1.0f32).to_le_bytes());

    let output = bytes_to_i16_samples(&input, &metadata).unwrap();
    assert_eq!(output, vec![i16::MIN, 0, i16::MAX]);
}
