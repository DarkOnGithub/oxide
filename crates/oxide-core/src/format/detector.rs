use crate::types::FileFormat;
use infer::MatcherType;

const TEXT_SAMPLE_LIMIT: usize = 16 * 1024;
const UTF8_RATIO_THRESHOLD: f32 = 0.85;
const PRINTABLE_RATIO_THRESHOLD: f32 = 0.90;
const CONTROL_RATIO_THRESHOLD: f32 = 0.02;

#[derive(Debug, Clone, Copy, Default)]
pub struct FormatDetector;

impl FormatDetector {
    pub fn new() -> Self {
        Self
    }

    pub fn detect(data: &[u8]) -> FileFormat {
        if data.is_empty() {
            return FileFormat::Unknown;
        }

        if let Some(format) = Self::detect_with_library(data) {
            return format;
        }

        // Keep non-signature heuristics for cases infer does not classify.
        if Self::has_x86_prologue(data) {
            return FileFormat::Binary;
        }

        if Self::is_text(data) {
            return FileFormat::Text;
        }

        FileFormat::Binary
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
            | MatcherType::Custom => Some(FileFormat::Binary),
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
