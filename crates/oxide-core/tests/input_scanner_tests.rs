use std::io::Write;

use oxide_core::{BoundaryMode, FileFormat, InputScanner};
use tempfile::NamedTempFile;

fn write_temp(bytes: &[u8]) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut file = NamedTempFile::new()?;
    file.write_all(bytes)?;
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
    assert!(batches
        .iter()
        .all(|batch| batch.file_type_hint == FileFormat::Text));
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
