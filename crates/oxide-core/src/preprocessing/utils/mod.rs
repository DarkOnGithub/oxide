/// Converts raw bytes into little-endian 16-bit signed audio samples.
///
/// Input: `data` is a PCM-like byte stream where each sample is encoded as
/// two bytes in little-endian order. A trailing odd byte is ignored.
pub fn bytes_to_i16_samples_le(data: &[u8]) -> Vec<i16> {
    data.chunks_exact(2)
        .map(|chunk| i16::from_le_bytes([chunk[0], chunk[1]]))
        .collect()
}

/// Converts raw bytes into little-endian 32-bit words.
///
/// Input: `data` is a byte stream where each logical value is four bytes in
/// little-endian order. Trailing bytes that do not form a full word are
/// ignored.
pub fn bytes_to_u32_words_le(data: &[u8]) -> Vec<u32> {
    data.chunks_exact(4)
        .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

/// Converts raw bytes into RGB pixels represented as `[r, g, b]` triplets.
///
/// Input: `data` is a packed RGB byte stream in `RGBRGB...` order. Trailing
/// bytes that do not form a full pixel are ignored.
pub fn bytes_to_rgb_pixels(data: &[u8]) -> Vec<[u8; 3]> {
    data.chunks_exact(3)
        .map(|chunk| [chunk[0], chunk[1], chunk[2]])
        .collect()
}

/// Converts raw bytes into grayscale pixel intensities.
///
/// Input: `data` is a stream where each byte already represents one grayscale
/// pixel sample.
pub fn bytes_to_grayscale_pixels(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

/// Converts raw bytes into symbol ids for text-style preprocessing.
///
/// Input: `data` is a byte stream where each byte is treated as one symbol.
/// Each output symbol is widened to `u32` for algorithms that operate on
/// integer symbol domains.
pub fn bytes_to_symbols(data: &[u8]) -> Vec<u32> {
    data.iter().map(|&byte| u32::from(byte)).collect()
}
