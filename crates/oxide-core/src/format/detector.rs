use std::path::Path;
use std::time::Instant;

use crate::telemetry;
use crate::telemetry::profile;
use crate::telemetry::tags;
use crate::types::FileFormat;
use infer::MatcherType;

const PROFILE_TAG_STACK_FORMAT: [&str; 1] = [tags::TAG_FORMAT];

const TEXT_SAMPLE_LIMIT: usize = 16 * 1024;
const UTF8_RATIO_THRESHOLD: f32 = 0.85;
const PRINTABLE_RATIO_THRESHOLD: f32 = 0.90;
const CONTROL_RATIO_THRESHOLD: f32 = 0.02;

/// File format detector using library-based and heuristic methods.
///
/// Detects file formats using the `infer` crate for known file signatures,
/// with fallback heuristics for text detection and binary identification.
///
/// # Example
/// ```
/// use oxide_core::format::FormatDetector;
///
/// let file_bytes = b"Hello, world!";
/// let format = FormatDetector::detect(file_bytes);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct FormatDetector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawStorageDecision {
    pub force_raw_storage: bool,
}

impl FormatDetector {
    /// Creates a new format detector instance.
    ///
    /// Note: This is equivalent to using `FormatDetector::default()`.
    pub fn new() -> Self {
        Self
    }

    /// Detects the format of the provided data.
    ///
    /// First attempts detection using the `infer` library for known
    /// file signatures. Falls back to heuristic detection for text
    /// and binary formats.
    ///
    /// # Arguments
    /// * `data` - The data bytes to analyze
    ///
    /// # Returns
    /// The detected [`FileFormat`], or [`FileFormat::Common`] if unknown.
    pub fn detect(data: &[u8]) -> FileFormat {
        let started_at = Instant::now();

        let (format, decision_path) = if data.is_empty() {
            (FileFormat::Common, "empty_input")
        } else if let Some(format) = Self::detect_with_library(data) {
            (format, "library")
        } else if Self::has_x86_prologue(data) {
            // Keep non-signature heuristics for cases infer does not classify.
            (FileFormat::Binary, "x86_prologue")
        } else if Self::is_text(data) {
            (FileFormat::Text, "text_heuristic")
        } else {
            (FileFormat::Common, "fallback_common")
        };

        let elapsed_us = profile::elapsed_us(started_at);
        telemetry::record_histogram(tags::METRIC_FORMAT_DETECT_LATENCY_US, elapsed_us);

        profile::event(
            tags::PROFILE_FORMAT,
            &PROFILE_TAG_STACK_FORMAT,
            "detect",
            decision_path,
            elapsed_us,
            "format detect completed",
        );
        #[cfg(feature = "profiling")]
        if profile::is_tag_stack_enabled(&PROFILE_TAG_STACK_FORMAT) {
            tracing::debug!(
                target: tags::PROFILE_FORMAT,
                op = "detect",
                decision_path,
                input_len = data.len(),
                detected_format = ?format,
                elapsed_us,
                tags = ?PROFILE_TAG_STACK_FORMAT,
                "format detect context"
            );
        }

        format
    }

    fn detect_with_library(data: &[u8]) -> Option<FileFormat> {
        let kind = infer::get(data)?;

        match kind.matcher_type() {
            MatcherType::Image => Some(FileFormat::Image),
            MatcherType::Audio => Some(FileFormat::Audio),
            MatcherType::Text => Some(FileFormat::Text),
            MatcherType::App => Some(FileFormat::Binary),
            MatcherType::Archive
            | MatcherType::Book
            | MatcherType::Doc
            | MatcherType::Font
            | MatcherType::Video
            | MatcherType::Custom => Some(FileFormat::Common),
        }
    }

    // Heuristics:
    // - UTF-8 validity ratio over a bounded sample
    // - Printable/whitespace ratio
    // - Control-byte ratio (excluding common whitespace)
    fn is_text(data: &[u8]) -> bool {
        let sample = &data[..data.len().min(TEXT_SAMPLE_LIMIT)];
        if sample.is_empty() {
            return false;
        }

        let utf8_ratio = match std::str::from_utf8(sample) {
            Ok(_) => 1.0,
            Err(err) => err.valid_up_to() as f32 / sample.len() as f32,
        };
        if utf8_ratio < UTF8_RATIO_THRESHOLD {
            return false;
        }

        let mut printable = 0usize;
        let mut control = 0usize;
        for &byte in sample {
            if byte.is_ascii_graphic() || byte.is_ascii_whitespace() {
                printable += 1;
            } else if byte.is_ascii_control() {
                control += 1;
            }
        }

        let len = sample.len() as f32;
        let printable_ratio = printable as f32 / len;
        let control_ratio = control as f32 / len;

        printable_ratio >= PRINTABLE_RATIO_THRESHOLD && control_ratio <= CONTROL_RATIO_THRESHOLD
    }

    fn has_x86_prologue(data: &[u8]) -> bool {
        const PROLOGUES: [&[u8]; 4] = [
            b"\x55\x48\x89\xE5", // x86_64 push rbp; mov rbp,rsp
            b"\x55\x89\xE5",     // x86 push ebp; mov ebp,esp
            b"\x48\x83\xEC",     // x86_64 stack allocation
            b"\x53\x48\x83\xEC", // push rbx; sub rsp,...
        ];

        let probe = &data[..data.len().min(4096)];
        PROLOGUES
            .iter()
            .any(|pattern| probe.windows(pattern.len()).any(|w| w == *pattern))
    }
}

#[inline]
fn matches_force_raw_storage_label(label: &str) -> bool {
    matches!(
        label,
        "7z" | "aac"
            | "apk"
            | "avif"
            | "br"
            | "bz2"
            | "docx"
            | "epub"
            | "flac"
            | "gif"
            | "gz"
            | "heic"
            | "heif"
            | "jar"
            | "jpeg"
            | "jpg"
            | "jxl"
            | "lz4"
            | "m4a"
            | "m4v"
            | "mkv"
            | "mov"
            | "mp3"
            | "mp4"
            | "ogg"
            | "opus"
            | "oxz"
            | "png"
            | "pptx"
            | "rar"
            | "webm"
            | "webp"
            | "whl"
            | "xlsx"
            | "xz"
            | "zip"
            | "zst"
            | "zstd"
    )
}

pub fn should_force_raw_storage_by_extension(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
        return false;
    };

    matches_force_raw_storage_label(&ext.to_ascii_lowercase())
}

pub fn should_force_raw_storage_by_signature(data: &[u8]) -> bool {
    let Some(kind) = infer::get(data) else {
        return false;
    };

    matches_force_raw_storage_label(kind.extension())
}

pub fn should_force_raw_storage(path: &Path, data: &[u8]) -> bool {
    should_force_raw_storage_by_signature(data) || should_force_raw_storage_by_extension(path)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        should_force_raw_storage, should_force_raw_storage_by_extension,
        should_force_raw_storage_by_signature,
    };

    #[test]
    fn raw_storage_extension_policy_matches_known_compressed_types() {
        assert!(should_force_raw_storage_by_extension(Path::new(
            "photo.jpg"
        )));
        assert!(should_force_raw_storage_by_extension(Path::new(
            "bundle.ZIP"
        )));
        assert!(should_force_raw_storage_by_extension(Path::new(
            "archive.tar.zst"
        )));
        assert!(!should_force_raw_storage_by_extension(Path::new(
            "notes.txt"
        )));
        assert!(!should_force_raw_storage_by_extension(Path::new(
            "bitmap.bmp"
        )));
        assert!(!should_force_raw_storage_by_extension(Path::new("README")));
    }

    #[test]
    fn raw_storage_signature_policy_matches_known_headers() {
        assert!(should_force_raw_storage_by_signature(&[
            0xFF, 0xD8, 0xFF, 0xE0
        ]));
        assert!(should_force_raw_storage_by_signature(b"PK\x03\x04payload"));
        assert!(!should_force_raw_storage_by_signature(
            b"plain text payload"
        ));
    }

    #[test]
    fn raw_storage_policy_prefers_signature_when_extension_is_not_listed() {
        assert!(should_force_raw_storage(
            Path::new("payload.bin"),
            &[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]
        ));
        assert!(!should_force_raw_storage(
            Path::new("payload.bin"),
            b"plain text payload"
        ));
    }
}
