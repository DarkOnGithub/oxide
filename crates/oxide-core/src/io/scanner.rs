use std::fs::File;
use std::path::Path;
use std::time::Instant;

use image::ImageDecoder;
use memchr::memchr;
use symphonia::core::codecs::{
    CODEC_TYPE_PCM_F32BE, CODEC_TYPE_PCM_F32LE, CODEC_TYPE_PCM_F64BE, CODEC_TYPE_PCM_F64LE,
    CODEC_TYPE_PCM_S8, CODEC_TYPE_PCM_S16BE, CODEC_TYPE_PCM_S16LE, CODEC_TYPE_PCM_S24BE,
    CODEC_TYPE_PCM_S24LE, CODEC_TYPE_PCM_S32BE, CODEC_TYPE_PCM_S32LE, CODEC_TYPE_PCM_U8,
    CODEC_TYPE_PCM_U16BE, CODEC_TYPE_PCM_U16LE, CODEC_TYPE_PCM_U24BE, CODEC_TYPE_PCM_U24LE,
    CODEC_TYPE_PCM_U32BE, CODEC_TYPE_PCM_U32LE, CodecType,
};
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

use crate::format::FormatDetector;
use crate::io::MmapInput;
use crate::preprocessing::{
    AudioEndian, AudioMetadata, AudioSampleEncoding, ImageMetadata, ImagePixelFormat,
    PreprocessingMetadata,
};
use crate::telemetry;
use crate::telemetry::profile;
use crate::telemetry::tags;
use crate::types::{Batch, FileFormat, Result};

/// Tag stack for scanner profiling events.
const PROFILE_TAG_STACK_SCANNER: &[&str] = &[tags::TAG_SYSTEM, tags::TAG_SCANNER];
const FORMAT_PROBE_LIMIT: usize = 64 * 1024;

/// Boundary mode selected for chunking a file into batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryMode {
    TextNewline,
    ImageRows { row_bytes: usize },
    AudioFrames { frame_bytes: usize },
    Raw,
}

#[derive(Debug, Clone, Copy)]
struct ScanDetection {
    boundary_mode: BoundaryMode,
    preprocessing_metadata: Option<PreprocessingMetadata>,
}

/// Aligns a boundary to the specified alignment size.
///
/// Finds the largest aligned position at or before `value`.
fn align_down(value: usize, alignment: usize) -> usize {
    if alignment == 0 {
        value
    } else {
        value - (value % alignment)
    }
}

/// Finds the next aligned position strictly after `start`.
fn next_aligned_after(start: usize, alignment: usize) -> usize {
    if alignment == 0 {
        return start;
    }

    let remainder = start % alignment;
    if remainder == 0 {
        start.saturating_add(alignment)
    } else {
        start.saturating_add(alignment - remainder)
    }
}

/// Scanner configuration and boundary selection entrypoint.
///
#[derive(Debug, Clone)]
pub struct InputScanner {
    target_block_size: usize,
}

impl InputScanner {
    /// Creates a new scanner with a target block size.
    pub fn new(target_block_size: usize) -> Self {
        Self {
            target_block_size: target_block_size.max(1),
        }
    }

    /// Returns configured target block size.
    pub fn target_block_size(&self) -> usize {
        self.target_block_size
    }

    /// Scans a file into ordered batches.
    ///
    /// # Arguments
    /// * `path` - Path to the file to scan
    ///
    /// # Returns
    /// A vector of batches, or an error if the file cannot be scanned.
    pub fn scan_file(&self, path: &Path) -> Result<Vec<Batch>> {
        let started_at = Instant::now();
        let result = self.scan_file_inner(path);

        let elapsed_us = profile::elapsed_us(started_at);
        telemetry::increment_counter(
            tags::METRIC_SCANNER_SCAN_COUNT,
            1,
            &[("subsystem", "scanner"), ("op", "scan_file")],
        );
        telemetry::record_histogram(
            tags::METRIC_SCANNER_SCAN_LATENCY_US,
            elapsed_us,
            &[("subsystem", "scanner"), ("op", "scan_file")],
        );

        match &result {
            Ok(_batches) => {
                profile::event(
                    tags::PROFILE_SCANNER,
                    &PROFILE_TAG_STACK_SCANNER,
                    "scan_file",
                    "ok",
                    elapsed_us,
                    "scanner completed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_SCANNER) {
                    tracing::debug!(
                        target: tags::PROFILE_SCANNER,
                        op = "scan_file",
                        result = "ok",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_SCANNER,
                        batch_count = _batches.len(),
                        path = %path.display(),
                        "scanner context"
                    );
                }
            }
            Err(_error) => {
                profile::event(
                    tags::PROFILE_SCANNER,
                    &PROFILE_TAG_STACK_SCANNER,
                    "scan_file",
                    "error",
                    elapsed_us,
                    "scanner failed",
                );
                #[cfg(feature = "profiling")]
                if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_SCANNER) {
                    tracing::debug!(
                        target: tags::PROFILE_SCANNER,
                        op = "scan_file",
                        result = "error",
                        elapsed_us,
                        tags = ?PROFILE_TAG_STACK_SCANNER,
                        path = %path.display(),
                        error = %_error,
                        "scanner context"
                    );
                }
            }
        }

        result
    }

    fn scan_file_inner(&self, path: &Path) -> Result<Vec<Batch>> {
        let mmap = MmapInput::open(path)?;
        if mmap.is_empty() {
            return Ok(Vec::new());
        }

        let len = mmap.len();
        let probe_len = len.min(FORMAT_PROBE_LIMIT);
        let probe = mmap.mapped_slice(0, probe_len)?;
        let format = FormatDetector::detect(probe.as_slice());
        let mapped_data = mmap.mapped_slice(0, len)?;
        let data = mapped_data.as_slice();
        let scan_detection = self.detect_scan_detection(path, format);
        let boundary_mode = scan_detection.boundary_mode;
        self.record_mode(boundary_mode);

        let estimated_batches =
            len.saturating_add(self.target_block_size - 1) / self.target_block_size;
        let mut batches = Vec::with_capacity(estimated_batches.max(1));
        let mut start = 0usize;
        let source_path = path.to_path_buf();
        let mut id = 0usize;

        while start < len {
            let mut end = self.find_block_boundary(data, start, &boundary_mode);
            if end <= start {
                end = start.saturating_add(1).min(len);
            }

            let batch_data = mmap.mapped_slice(start, end)?;
            batches.push(Batch {
                id,
                source_path: source_path.clone(),
                data: batch_data,
                file_type_hint: format,
                preprocessing_metadata: scan_detection.preprocessing_metadata,
            });
            start = end;
            id += 1;
        }

        Ok(batches)
    }

    /// Chooses boundary mode from format hints and metadata.
    pub fn detect_boundary_mode(&self, path: &Path, format: FileFormat) -> BoundaryMode {
        self.detect_scan_detection(path, format).boundary_mode
    }

    /// Detects preprocessing metadata from format hints and source metadata.
    pub fn detect_preprocessing_metadata(
        &self,
        path: &Path,
        format: FileFormat,
    ) -> Option<PreprocessingMetadata> {
        self.detect_scan_detection(path, format)
            .preprocessing_metadata
    }

    fn detect_scan_detection(&self, path: &Path, format: FileFormat) -> ScanDetection {
        match format {
            FileFormat::Text => ScanDetection {
                boundary_mode: BoundaryMode::TextNewline,
                preprocessing_metadata: None,
            },
            FileFormat::Image => self
                .detect_image_mode_with_metadata(path)
                .map(|(boundary_mode, metadata)| ScanDetection {
                    boundary_mode,
                    preprocessing_metadata: Some(PreprocessingMetadata::Image(metadata)),
                })
                .unwrap_or_else(|| ScanDetection {
                    boundary_mode: self.fallback_to_raw("image metadata detection failed"),
                    preprocessing_metadata: None,
                }),
            FileFormat::Audio => self
                .detect_audio_mode_with_metadata(path)
                .map(|(boundary_mode, metadata)| ScanDetection {
                    boundary_mode,
                    preprocessing_metadata: Some(PreprocessingMetadata::Audio(metadata)),
                })
                .unwrap_or_else(|| ScanDetection {
                    boundary_mode: self.fallback_to_raw("audio metadata detection failed"),
                    preprocessing_metadata: None,
                }),
            _ => ScanDetection {
                boundary_mode: BoundaryMode::Raw,
                preprocessing_metadata: None,
            },
        }
    }

    /// Finds next boundary index for the current scanning mode.
    pub fn find_block_boundary(&self, data: &[u8], start: usize, mode: &BoundaryMode) -> usize {
        let len = data.len();
        if start >= len {
            return len;
        }

        let target = start.saturating_add(self.target_block_size).min(len);
        match mode {
            BoundaryMode::TextNewline => {
                if target >= len {
                    len
                } else {
                    // Search ahead from target to keep chunks near target size while still
                    // snapping text boundaries to line ends when possible.
                    let search_end = target.saturating_add(self.target_block_size).min(len);
                    if let Some(offset) = memchr(b'\n', &data[target..search_end]) {
                        target + offset + 1
                    } else {
                        target
                    }
                }
            }
            BoundaryMode::ImageRows { row_bytes } => {
                self.find_aligned_boundary(start, target, len, *row_bytes)
            }
            BoundaryMode::AudioFrames { frame_bytes } => {
                self.find_aligned_boundary(start, target, len, *frame_bytes)
            }
            BoundaryMode::Raw => target,
        }
    }

    fn find_aligned_boundary(
        &self,
        start: usize,
        target: usize,
        len: usize,
        alignment: usize,
    ) -> usize {
        if alignment == 0 {
            return target;
        }

        let aligned = align_down(target, alignment);
        if aligned > start {
            return aligned;
        }

        let next = next_aligned_after(start, alignment).min(len);
        if next > start {
            next
        } else {
            start.saturating_add(1).min(len)
        }
    }

    /// Detects image mode from image metadata.
    ///
    /// # Arguments
    /// * `path` - Path to the image file
    ///
    /// # Returns
    /// The detected image mode, or `None` if the image is not supported.
    pub fn detect_image_mode(&self, path: &Path) -> Option<BoundaryMode> {
        self.detect_image_mode_with_metadata(path)
            .map(|(mode, _metadata)| mode)
    }

    fn detect_image_mode_with_metadata(
        &self,
        path: &Path,
    ) -> Option<(BoundaryMode, ImageMetadata)> {
        let reader = image::ImageReader::open(path).ok()?;
        let reader = reader.with_guessed_format().ok()?;
        let decoder = reader.into_decoder().ok()?;
        //we don't need height
        let (width, _) = decoder.dimensions();
        let color = decoder.color_type();

        let width = usize::try_from(width).ok()?;
        let channels = color.channel_count() as usize;
        let bytes_per_pixel = color.bytes_per_pixel() as usize;
        if channels == 0 || bytes_per_pixel == 0 || bytes_per_pixel != channels {
            return None;
        }

        let pixel_format = match channels {
            1 => ImagePixelFormat::Gray8,
            2 => ImagePixelFormat::GrayAlpha8,
            3 => ImagePixelFormat::Rgb8,
            4 => ImagePixelFormat::Rgba8,
            _ => return None,
        };

        let row_bytes = width.checked_mul(bytes_per_pixel)?;
        if row_bytes == 0 {
            return None;
        }

        let metadata = ImageMetadata::packed(pixel_format).with_row_layout(width, row_bytes);
        Some((BoundaryMode::ImageRows { row_bytes }, metadata))
    }

    /// Detects audio mode from audio metadata.
    ///
    /// # Arguments
    /// * `path` - Path to the audio file
    ///
    /// # Returns
    /// The detected audio mode, or `None` if the audio is not supported.
    pub fn detect_audio_mode(&self, path: &Path) -> Option<BoundaryMode> {
        self.detect_audio_mode_with_metadata(path)
            .map(|(mode, _metadata)| mode)
    }

    fn detect_audio_mode_with_metadata(
        &self,
        path: &Path,
    ) -> Option<(BoundaryMode, AudioMetadata)> {
        let file = File::open(path).ok()?;
        let mss = MediaSourceStream::new(Box::new(file), Default::default());
        let mut hint = Hint::new();
        if let Some(ext) = path.extension().and_then(|ext| ext.to_str()) {
            hint.with_extension(ext);
        }

        let probed = symphonia::default::get_probe()
            .format(
                &hint,
                mss,
                &FormatOptions::default(),
                &MetadataOptions::default(),
            )
            .ok()?;

        for track in probed.format.tracks() {
            let params = &track.codec_params;
            let channels = params.channels.map(|channels| channels.count() as usize);
            let bits_per_sample = params.bits_per_sample.map(|bits| bits as usize);
            let frames_per_packet = params
                .max_frames_per_packet
                .and_then(|frames| usize::try_from(frames).ok())
                .unwrap_or(1);
            let (encoding, endian) = audio_sample_layout_for_codec(params.codec)?;

            if let (Some(channels), Some(bits_per_sample)) = (channels, bits_per_sample) {
                if bits_per_sample == 0 || bits_per_sample % 8 != 0 {
                    continue;
                }

                let bytes_per_sample = bits_per_sample / 8;
                if bytes_per_sample == 0 || channels == 0 || frames_per_packet == 0 {
                    continue;
                }

                let metadata = AudioMetadata {
                    channels,
                    bytes_per_sample,
                    encoding,
                    endian,
                };

                if let Some(frame_bytes) = bytes_per_sample
                    .checked_mul(channels)
                    .and_then(|v| v.checked_mul(frames_per_packet))
                {
                    if frame_bytes > 0 {
                        return Some((BoundaryMode::AudioFrames { frame_bytes }, metadata));
                    }
                }
            }
        }

        None
    }

    /// Falls back to raw mode if the metadata detection fails.
    pub fn fallback_to_raw(&self, _reason: &'static str) -> BoundaryMode {
        telemetry::increment_counter(
            tags::METRIC_SCANNER_FALLBACK_COUNT,
            1,
            &[("subsystem", "scanner"), ("op", "fallback")],
        );
        BoundaryMode::Raw
    }

    pub fn record_mode(&self, mode: BoundaryMode) {
        let metric_tag = match mode {
            BoundaryMode::TextNewline => tags::METRIC_SCANNER_MODE_TEXT_COUNT,
            BoundaryMode::ImageRows { row_bytes: _ } => tags::METRIC_SCANNER_MODE_IMAGE_COUNT,
            BoundaryMode::AudioFrames { frame_bytes: _ } => tags::METRIC_SCANNER_MODE_AUDIO_COUNT,
            BoundaryMode::Raw => tags::METRIC_SCANNER_MODE_RAW_COUNT,
        };
        telemetry::increment_counter(metric_tag, 1, &[("subsystem", "scanner"), ("op", "mode")]);
    }
}

fn audio_sample_layout_for_codec(codec: CodecType) -> Option<(AudioSampleEncoding, AudioEndian)> {
    match codec {
        CODEC_TYPE_PCM_S8 | CODEC_TYPE_PCM_S16LE | CODEC_TYPE_PCM_S24LE | CODEC_TYPE_PCM_S32LE => {
            Some((AudioSampleEncoding::SignedPcm, AudioEndian::Little))
        }
        CODEC_TYPE_PCM_S16BE | CODEC_TYPE_PCM_S24BE | CODEC_TYPE_PCM_S32BE => {
            Some((AudioSampleEncoding::SignedPcm, AudioEndian::Big))
        }
        CODEC_TYPE_PCM_U8 | CODEC_TYPE_PCM_U16LE | CODEC_TYPE_PCM_U24LE | CODEC_TYPE_PCM_U32LE => {
            Some((AudioSampleEncoding::UnsignedPcm, AudioEndian::Little))
        }
        CODEC_TYPE_PCM_U16BE | CODEC_TYPE_PCM_U24BE | CODEC_TYPE_PCM_U32BE => {
            Some((AudioSampleEncoding::UnsignedPcm, AudioEndian::Big))
        }
        CODEC_TYPE_PCM_F32LE | CODEC_TYPE_PCM_F64LE => {
            Some((AudioSampleEncoding::Float, AudioEndian::Little))
        }
        CODEC_TYPE_PCM_F32BE | CODEC_TYPE_PCM_F64BE => {
            Some((AudioSampleEncoding::Float, AudioEndian::Big))
        }
        _ => None,
    }
}
