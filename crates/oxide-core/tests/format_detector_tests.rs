use oxide_core::{format::FormatDetector, FileFormat};

#[test]
fn detects_png() {
    let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
    bytes.extend_from_slice(&[0u8; 16]);
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Image);
}

#[test]
fn detects_bmp() {
    let bytes = b"BM\x36\x00\x00\x00rest".to_vec();
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Image);
}

#[test]
fn detects_gif_via_library_signature() {
    let bytes = b"GIF89a\x01\x00\x01\x00\x00";
    assert_eq!(FormatDetector::detect(bytes), FileFormat::Image);
}

#[test]
fn detects_wav() {
    let mut bytes = b"RIFF".to_vec();
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    bytes.extend_from_slice(b"WAVEfmt ");
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Audio);
}

#[test]
fn detects_aiff() {
    let mut bytes = b"FORM".to_vec();
    bytes.extend_from_slice(&[0, 0, 0, 0]);
    bytes.extend_from_slice(b"AIFFCOMM");
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Audio);
}

#[test]
fn detects_utf8_text() {
    let bytes = b"Oxide archival pipeline handles text reliably.\nSecond line.\n";
    assert_eq!(FormatDetector::detect(bytes), FileFormat::Text);
}

#[test]
fn detects_binary_with_controls() {
    let bytes = [0x00, 0xFF, 0x13, 0x00, 0x02, 0x99, 0x80, 0x00, 0x00];
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Common);
}

#[test]
fn detects_elf_x86() {
    let mut bytes = vec![0u8; 64];
    bytes[0..4].copy_from_slice(b"\x7fELF");
    bytes[18..20].copy_from_slice(&0x003Eu16.to_le_bytes());
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Binary);
}

#[test]
fn detects_pe_x86() {
    let mut bytes = vec![0u8; 512];
    bytes[0..2].copy_from_slice(b"MZ");
    bytes[0x3C..0x40].copy_from_slice(&0x80u32.to_le_bytes());
    bytes[0x80..0x84].copy_from_slice(b"PE\0\0");
    bytes[0x84..0x86].copy_from_slice(&0x014cu16.to_le_bytes());
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Binary);
}

#[test]
fn detects_x86_prologue_pattern() {
    let mut bytes = vec![0u8; 128];
    bytes[24..28].copy_from_slice(&[0x55, 0x48, 0x89, 0xE5]);
    assert_eq!(FormatDetector::detect(&bytes), FileFormat::Binary);
}

#[test]
fn detects_archive_as_common() {
    let bytes = b"PK\x03\x04\x14\x00\x00\x00\x08\x00";
    assert_eq!(FormatDetector::detect(bytes), FileFormat::Common);
}

#[test]
fn empty_is_unknown() {
    assert_eq!(FormatDetector::detect(&[]), FileFormat::Unknown);
}
