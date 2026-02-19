use std::io::Write;

use oxide_core::{
    AudioEndian, AudioSampleEncoding, BoundaryMode, FileFormat, ImagePixelFormat, InputScanner,
    PreprocessingMetadata,
};
use tempfile::{Builder, NamedTempFile};

fn write_temp(bytes: &[u8]) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(bytes)?;
    file.flush()?;
    Ok(file)
}

fn write_png_rgb8(
    width: u32,
    height: u32,
    pixels: &[u8],
) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let file = Builder::new().suffix(".png").tempfile()?;
    image::save_buffer_with_format(
        file.path(),
        pixels,
        width,
        height,
        image::ColorType::Rgb8,
        image::ImageFormat::Png,
    )?;
    Ok(file)
}

fn write_wav_pcm16(
    channels: u16,
    sample_rate: u32,
    samples: &[i16],
) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut file = Builder::new().suffix(".wav").tempfile()?;

    let bytes_per_sample = 2u16;
    let block_align = channels
        .checked_mul(bytes_per_sample)
        .ok_or("wav block align overflow")?;
    let byte_rate = sample_rate
        .checked_mul(u32::from(block_align))
        .ok_or("wav byte rate overflow")?;

    let mut data = Vec::with_capacity(samples.len() * 2);
    for sample in samples {
        data.extend_from_slice(&sample.to_le_bytes());
    }
    let data_len = u32::try_from(data.len()).map_err(|_| "wav data too large")?;
    let riff_len = 36u32
        .checked_add(data_len)
        .ok_or("wav riff size overflow")?;

    file.write_all(b"RIFF")?;
    file.write_all(&riff_len.to_le_bytes())?;
    file.write_all(b"WAVE")?;
    file.write_all(b"fmt ")?;
    file.write_all(&16u32.to_le_bytes())?;
    file.write_all(&1u16.to_le_bytes())?;
    file.write_all(&channels.to_le_bytes())?;
    file.write_all(&sample_rate.to_le_bytes())?;
    file.write_all(&byte_rate.to_le_bytes())?;
    file.write_all(&block_align.to_le_bytes())?;
    file.write_all(&16u16.to_le_bytes())?;
    file.write_all(b"data")?;
    file.write_all(&data_len.to_le_bytes())?;
    file.write_all(&data)?;
    file.flush()?;

    Ok(file)
}

fn flatten_batches(batches: &[oxide_core::Batch]) -> Vec<u8> {
    let mut out = Vec::new();
    for batch in batches {
        out.extend_from_slice(batch.data());
    }
    out
}

fn assert_contiguous_ids(batches: &[oxide_core::Batch]) {
    for (idx, batch) in batches.iter().enumerate() {
        assert_eq!(batch.id, idx);
    }
}

#[test]
fn text_splits_on_newline_and_preserves_order() -> Result<(), Box<dyn std::error::Error>> {
    let data = b"alpha line\nbeta line\ngamma line\ndelta line\n";
    let file = write_temp(data)?;

    let scanner = InputScanner::new(11);
    let batches = scanner.scan_file(file.path())?;

    assert!(!batches.is_empty());
    assert_contiguous_ids(&batches);
    assert_eq!(flatten_batches(&batches), data);
    assert!(
        batches
            .iter()
            .all(|batch| batch.file_type_hint == FileFormat::Text)
    );
    assert!(
        batches
            .iter()
            .all(|batch| batch.preprocessing_metadata.is_none())
    );
    assert!(batches.iter().all(|batch| !batch.is_empty()));

    Ok(())
}

#[test]
fn raw_mode_covers_all_bytes_without_overlap() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<u8> = (0..4096).map(|idx| ((idx * 37) % 251) as u8).collect();
    let file = write_temp(&data)?;

    let scanner = InputScanner::new(257);
    let batches = scanner.scan_file(file.path())?;

    assert!(!batches.is_empty());
    assert_contiguous_ids(&batches);
    assert!(batches.iter().all(|batch| !batch.is_empty()));
    assert_eq!(flatten_batches(&batches), data);

    Ok(())
}

#[test]
fn image_metadata_failure_falls_back_to_raw() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake_png = b"\x89PNG\r\n\x1a\n".to_vec();
    fake_png.extend_from_slice(&[0u8; 64]);
    let file = write_temp(&fake_png)?;

    let scanner = InputScanner::new(64);
    let mode = scanner.detect_boundary_mode(file.path(), FileFormat::Image);
    assert_eq!(mode, BoundaryMode::Raw);

    Ok(())
}

#[test]
fn audio_metadata_failure_falls_back_to_raw() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake_wav = b"RIFF".to_vec();
    fake_wav.extend_from_slice(&[0, 0, 0, 0]);
    fake_wav.extend_from_slice(b"WAVEfmt ");
    fake_wav.extend_from_slice(&[0u8; 64]);
    let file = write_temp(&fake_wav)?;

    let scanner = InputScanner::new(64);
    let mode = scanner.detect_boundary_mode(file.path(), FileFormat::Audio);
    assert_eq!(mode, BoundaryMode::Raw);

    Ok(())
}

#[test]
fn scanner_handles_empty_file() -> Result<(), Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    let scanner = InputScanner::new(128);
    let batches = scanner.scan_file(file.path())?;
    assert!(batches.is_empty());
    Ok(())
}

#[test]
fn scanner_last_chunk_closes_exactly_at_len() -> Result<(), Box<dyn std::error::Error>> {
    let data: Vec<u8> = (0..1031).map(|idx| ((idx * 13) % 251) as u8).collect();
    let file = write_temp(&data)?;

    let scanner = InputScanner::new(128);
    let batches = scanner.scan_file(file.path())?;

    assert!(!batches.is_empty());
    assert_eq!(flatten_batches(&batches).len(), data.len());
    assert_eq!(flatten_batches(&batches), data);
    assert!(batches.iter().all(|batch| !batch.is_empty()));

    Ok(())
}

#[test]
fn scanner_ids_are_contiguous() -> Result<(), Box<dyn std::error::Error>> {
    let data = b"one two three four five six seven eight nine ten";
    let file = write_temp(data)?;

    let scanner = InputScanner::new(8);
    let batches = scanner.scan_file(file.path())?;

    assert!(!batches.is_empty());
    assert_contiguous_ids(&batches);
    assert_eq!(flatten_batches(&batches), data);

    Ok(())
}

#[test]
fn scanner_attaches_image_preprocessing_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let file = write_png_rgb8(2, 1, &[255, 0, 0, 0, 255, 0])?;
    let scanner = InputScanner::new(64);
    let batches = scanner.scan_file(file.path())?;

    assert!(!batches.is_empty());
    assert!(
        batches
            .iter()
            .all(|batch| batch.file_type_hint == FileFormat::Image)
    );
    for batch in batches {
        let metadata = batch
            .preprocessing_metadata
            .expect("image batches should have preprocessing metadata");
        match metadata {
            PreprocessingMetadata::Image(image) => {
                assert_eq!(image.pixel_format, ImagePixelFormat::Rgb8);
                assert_eq!(image.width, Some(2));
                assert_eq!(image.height, None);
                assert_eq!(image.row_stride, Some(6));
            }
            PreprocessingMetadata::Audio(_) => panic!("expected image metadata"),
        }
    }

    Ok(())
}

#[test]
fn scanner_attaches_audio_preprocessing_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let samples = [0i16, 1000, -1000, 500, -500, 0, 250, -250];
    let file = write_wav_pcm16(1, 8_000, &samples)?;
    let scanner = InputScanner::new(32);
    let batches = scanner.scan_file(file.path())?;

    assert!(!batches.is_empty());
    assert!(
        batches
            .iter()
            .all(|batch| batch.file_type_hint == FileFormat::Audio)
    );
    for batch in batches {
        let metadata = batch
            .preprocessing_metadata
            .expect("audio batches should have preprocessing metadata");
        match metadata {
            PreprocessingMetadata::Audio(audio) => {
                assert_eq!(audio.channels, 1);
                assert_eq!(audio.bytes_per_sample, 2);
                assert_eq!(audio.encoding, AudioSampleEncoding::SignedPcm);
                assert_eq!(audio.endian, AudioEndian::Little);
            }
            PreprocessingMetadata::Image(_) => panic!("expected audio metadata"),
        }
    }

    Ok(())
}

#[test]
fn media_modes_align_boundaries_with_forward_progress() {
    let scanner = InputScanner::new(70);
    let data = vec![0u8; 300];

    let image_end_0 =
        scanner.find_block_boundary(&data, 0, &BoundaryMode::ImageRows { row_bytes: 64 });
    let image_end_64 = scanner.find_block_boundary(
        &data,
        image_end_0,
        &BoundaryMode::ImageRows { row_bytes: 64 },
    );
    assert_eq!(image_end_0, 64);
    assert_eq!(image_end_64, 128);
    assert!(image_end_64 > image_end_0);

    let audio_end_0 =
        scanner.find_block_boundary(&data, 0, &BoundaryMode::AudioFrames { frame_bytes: 48 });
    let audio_end_48 = scanner.find_block_boundary(
        &data,
        audio_end_0,
        &BoundaryMode::AudioFrames { frame_bytes: 48 },
    );
    assert_eq!(audio_end_0, 48);
    assert_eq!(audio_end_48, 96);
    assert!(audio_end_48 > audio_end_0);
}
