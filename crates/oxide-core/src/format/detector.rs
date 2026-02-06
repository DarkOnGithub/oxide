use crate::types::FileFormat;

const MAGIC_PNG: &[u8] = b"\x89PNG\r\n\x1a\n";
const MAGIC_BMP: &[u8] = b"BM";
const MAGIC_RIFF: &[u8] = b"RIFF";
const MAGIC_WAVE: &[u8] = b"WAVE";
const MAGIC_FORM: &[u8] = b"FORM";
const MAGIC_AIFF: &[u8] = b"AIFF";
const MAGIC_AIFC: &[u8] = b"AIFC";
const MAGIC_ELF: &[u8] = b"\x7fELF";
const MAGIC_MZ: &[u8] = b"MZ";
const MAGIC_PE: &[u8] = b"PE\0\0";

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

        if Self::is_image(data) {
            return FileFormat::Image;
        }

        if Self::is_audio(data) {
            return FileFormat::Audio;
        }

        if Self::is_text(data) {
            return FileFormat::Text;
        }

        if Self::is_x86_executable(data) {
            return FileFormat::Binary;
        }

        FileFormat::Binary
    }

    fn is_image(data: &[u8]) -> bool {
        data.starts_with(MAGIC_PNG) || data.starts_with(MAGIC_BMP)
    }

    fn is_audio(data: &[u8]) -> bool {
        (data.len() >= 12 && data.starts_with(MAGIC_RIFF) && &data[8..12] == MAGIC_WAVE)
            || (data.len() >= 12
                && data.starts_with(MAGIC_FORM)
                && (&data[8..12] == MAGIC_AIFF || &data[8..12] == MAGIC_AIFC))
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

    fn is_x86_executable(data: &[u8]) -> bool {
        Self::is_elf_x86(data) || Self::is_pe_x86(data) || Self::has_x86_prologue(data)
    }

    fn is_elf_x86(data: &[u8]) -> bool {
        if data.len() < 20 || !data.starts_with(MAGIC_ELF) {
            return false;
        }
        let machine = u16::from_le_bytes([data[18], data[19]]);
        machine == 0x03 || machine == 0x3E
    }

    fn is_pe_x86(data: &[u8]) -> bool {
        if data.len() < 0x40 || !data.starts_with(MAGIC_MZ) {
            return false;
        }

        let pe_offset =
            u32::from_le_bytes([data[0x3C], data[0x3D], data[0x3E], data[0x3F]]) as usize;
        if pe_offset + 6 > data.len() {
            return false;
        }
        if &data[pe_offset..pe_offset + 4] != MAGIC_PE {
            return false;
        }

        let machine = u16::from_le_bytes([data[pe_offset + 4], data[pe_offset + 5]]);
        machine == 0x014c || machine == 0x8664
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
