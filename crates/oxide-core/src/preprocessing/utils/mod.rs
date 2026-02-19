use crate::{OxideError, Result};

/// Pixel packing of an image byte stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImagePixelFormat {
    Gray8,
    GrayAlpha8,
    Rgb8,
    Rgba8,
    Bgr8,
    Bgra8,
}

impl ImagePixelFormat {
    /// Returns bytes per pixel for this format.
    pub const fn bytes_per_pixel(self) -> usize {
        match self {
            Self::Gray8 => 1,
            Self::GrayAlpha8 => 2,
            Self::Rgb8 | Self::Bgr8 => 3,
            Self::Rgba8 | Self::Bgra8 => 4,
        }
    }
}

/// Metadata required to decode raw image bytes into pixel samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImageMetadata {
    /// Pixel format of the source bytes.
    pub pixel_format: ImagePixelFormat,
    /// Image width in pixels.
    ///
    /// When `None`, bytes are treated as a packed stream.
    /// When set and `height` is `None`, bytes are treated as a row-structured stream
    /// with unknown total row count.
    pub width: Option<usize>,
    /// Image height in pixels when known.
    pub height: Option<usize>,
    /// Number of source bytes between the start of consecutive rows.
    ///
    /// When not provided, rows are treated as tightly packed.
    pub row_stride: Option<usize>,
}

impl ImageMetadata {
    /// Creates packed stream metadata with no explicit dimensions.
    pub const fn packed(pixel_format: ImagePixelFormat) -> Self {
        Self {
            pixel_format,
            width: None,
            height: None,
            row_stride: None,
        }
    }

    /// Adds dimensions to metadata.
    pub fn with_dimensions(self, width: usize, height: usize) -> Self {
        Self {
            width: Some(width),
            height: Some(height),
            ..self
        }
    }

    /// Adds row-structured layout with unknown total row count.
    pub fn with_row_layout(self, width: usize, row_stride: usize) -> Self {
        Self {
            width: Some(width),
            height: None,
            row_stride: Some(row_stride),
            ..self
        }
    }

    /// Adds explicit row stride to metadata.
    pub fn with_row_stride(self, row_stride: usize) -> Self {
        Self {
            row_stride: Some(row_stride),
            ..self
        }
    }

    fn pixel_count_for(self, data_len: usize) -> Result<usize> {
        let bytes_per_pixel = self.pixel_format.bytes_per_pixel();
        match (self.width, self.height) {
            (None, None) => Ok(data_len / bytes_per_pixel),
            (Some(width), None) => {
                let row_bytes = width
                    .checked_mul(bytes_per_pixel)
                    .ok_or(OxideError::InvalidFormat("image row size overflow"))?;
                let row_stride = self.row_stride.unwrap_or(row_bytes);
                if row_stride < row_bytes {
                    return Err(OxideError::InvalidFormat(
                        "image row stride smaller than packed row size",
                    ));
                }

                let full_rows = data_len / row_stride;
                full_rows
                    .checked_mul(width)
                    .ok_or(OxideError::InvalidFormat("image pixel count overflow"))
            }
            (Some(width), Some(height)) => {
                let row_bytes = width
                    .checked_mul(bytes_per_pixel)
                    .ok_or(OxideError::InvalidFormat("image row size overflow"))?;
                let row_stride = self.row_stride.unwrap_or(row_bytes);
                if row_stride < row_bytes {
                    return Err(OxideError::InvalidFormat(
                        "image row stride smaller than packed row size",
                    ));
                }

                let required_bytes = row_stride
                    .checked_mul(height)
                    .ok_or(OxideError::InvalidFormat("image size overflow"))?;
                if data_len < required_bytes {
                    return Err(OxideError::InvalidFormat(
                        "image data shorter than metadata dimensions",
                    ));
                }

                width
                    .checked_mul(height)
                    .ok_or(OxideError::InvalidFormat("image pixel count overflow"))
            }
            _ => Err(OxideError::InvalidFormat(
                "image metadata requires width and height together",
            )),
        }
    }
}

/// Numeric representation of raw audio samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioSampleEncoding {
    SignedPcm,
    UnsignedPcm,
    Float,
}

/// Endianness for multi-byte audio samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioEndian {
    Little,
    Big,
}

/// Metadata required to decode raw audio bytes into sample values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioMetadata {
    /// Channel count in interleaved source stream.
    pub channels: usize,
    /// Bytes per single channel sample.
    pub bytes_per_sample: usize,
    /// Numeric sample representation.
    pub encoding: AudioSampleEncoding,
    /// Endianness of multi-byte fields.
    pub endian: AudioEndian,
}

impl AudioMetadata {
    /// Metadata for little-endian signed 16-bit PCM.
    pub const fn pcm_i16_le(channels: usize) -> Self {
        Self {
            channels,
            bytes_per_sample: 2,
            encoding: AudioSampleEncoding::SignedPcm,
            endian: AudioEndian::Little,
        }
    }
}

/// Converts raw audio bytes into signed 16-bit sample values using metadata.
///
/// Returns interleaved channel samples. Trailing bytes that do not make a full
/// frame are ignored.
pub fn bytes_to_i16_samples(data: &[u8], metadata: &AudioMetadata) -> Result<Vec<i16>> {
    if metadata.channels == 0 {
        return Err(OxideError::InvalidFormat("audio channels must be non-zero"));
    }
    if metadata.bytes_per_sample == 0 {
        return Err(OxideError::InvalidFormat(
            "audio bytes_per_sample must be non-zero",
        ));
    }

    let frame_bytes = metadata
        .channels
        .checked_mul(metadata.bytes_per_sample)
        .ok_or(OxideError::InvalidFormat("audio frame size overflow"))?;

    let full_frame_bytes = (data.len() / frame_bytes) * frame_bytes;
    let samples = full_frame_bytes / metadata.bytes_per_sample;
    let mut out = Vec::with_capacity(samples);

    for sample in data[..full_frame_bytes].chunks_exact(metadata.bytes_per_sample) {
        out.push(decode_audio_sample_to_i16(sample, *metadata)?);
    }

    Ok(out)
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

/// Converts raw bytes into RGB pixels using image metadata.
///
/// Supports grayscale, RGB/BGR, and RGBA/BGRA source layouts.
pub fn bytes_to_rgb_pixels(data: &[u8], metadata: &ImageMetadata) -> Result<Vec<[u8; 3]>> {
    let mut out = Vec::with_capacity(metadata.pixel_count_for(data.len())?);
    for_each_image_pixel(data, metadata, |pixel| {
        out.push(pixel_to_rgb(pixel, metadata.pixel_format));
    })?;
    Ok(out)
}

/// Converts raw bytes into grayscale pixel values using image metadata.
///
/// Color inputs are converted with an integer luma approximation.
pub fn bytes_to_grayscale_pixels(data: &[u8], metadata: &ImageMetadata) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(metadata.pixel_count_for(data.len())?);
    for_each_image_pixel(data, metadata, |pixel| {
        out.push(pixel_to_gray(pixel, metadata.pixel_format));
    })?;
    Ok(out)
}

/// Converts raw bytes into symbol ids for text-style preprocessing.
///
/// Input: `data` is a byte stream where each byte is treated as one symbol.
/// Each output symbol is widened to `u32` for algorithms that operate on
/// integer symbol domains.
pub fn bytes_to_symbols(data: &[u8]) -> Vec<u32> {
    data.iter().map(|&byte| u32::from(byte)).collect()
}

fn for_each_image_pixel<F>(data: &[u8], metadata: &ImageMetadata, mut visitor: F) -> Result<()>
where
    F: FnMut(&[u8]),
{
    let bytes_per_pixel = metadata.pixel_format.bytes_per_pixel();

    match (metadata.width, metadata.height) {
        (None, None) => {
            for pixel in data.chunks_exact(bytes_per_pixel) {
                visitor(pixel);
            }
        }
        (Some(width), None) => {
            let row_bytes = width
                .checked_mul(bytes_per_pixel)
                .ok_or(OxideError::InvalidFormat("image row size overflow"))?;
            let row_stride = metadata.row_stride.unwrap_or(row_bytes);
            if row_stride < row_bytes {
                return Err(OxideError::InvalidFormat(
                    "image row stride smaller than packed row size",
                ));
            }

            for row in data.chunks_exact(row_stride) {
                for pixel in row[..row_bytes].chunks_exact(bytes_per_pixel) {
                    visitor(pixel);
                }
            }
        }
        (Some(width), Some(height)) => {
            let row_bytes = width
                .checked_mul(bytes_per_pixel)
                .ok_or(OxideError::InvalidFormat("image row size overflow"))?;
            let row_stride = metadata.row_stride.unwrap_or(row_bytes);
            if row_stride < row_bytes {
                return Err(OxideError::InvalidFormat(
                    "image row stride smaller than packed row size",
                ));
            }

            let required_bytes = row_stride
                .checked_mul(height)
                .ok_or(OxideError::InvalidFormat("image size overflow"))?;
            if data.len() < required_bytes {
                return Err(OxideError::InvalidFormat(
                    "image data shorter than metadata dimensions",
                ));
            }

            for row_index in 0..height {
                let row_start = row_index * row_stride;
                let row_end = row_start + row_bytes;
                for pixel in data[row_start..row_end].chunks_exact(bytes_per_pixel) {
                    visitor(pixel);
                }
            }
        }
        _ => {
            return Err(OxideError::InvalidFormat(
                "image metadata requires width and height together",
            ));
        }
    }

    Ok(())
}

fn pixel_to_rgb(pixel: &[u8], pixel_format: ImagePixelFormat) -> [u8; 3] {
    match pixel_format {
        ImagePixelFormat::Gray8 | ImagePixelFormat::GrayAlpha8 => [pixel[0], pixel[0], pixel[0]],
        ImagePixelFormat::Rgb8 | ImagePixelFormat::Rgba8 => [pixel[0], pixel[1], pixel[2]],
        ImagePixelFormat::Bgr8 | ImagePixelFormat::Bgra8 => [pixel[2], pixel[1], pixel[0]],
    }
}

fn pixel_to_gray(pixel: &[u8], pixel_format: ImagePixelFormat) -> u8 {
    match pixel_format {
        ImagePixelFormat::Gray8 | ImagePixelFormat::GrayAlpha8 => pixel[0],
        _ => {
            let [r, g, b] = pixel_to_rgb(pixel, pixel_format);
            ((77u16 * r as u16 + 150u16 * g as u16 + 29u16 * b as u16 + 128u16) >> 8) as u8
        }
    }
}

fn decode_audio_sample_to_i16(sample: &[u8], metadata: AudioMetadata) -> Result<i16> {
    match metadata.encoding {
        AudioSampleEncoding::SignedPcm => decode_signed_pcm_to_i16(sample, metadata.endian),
        AudioSampleEncoding::UnsignedPcm => decode_unsigned_pcm_to_i16(sample, metadata.endian),
        AudioSampleEncoding::Float => decode_float_to_i16(sample, metadata.endian),
    }
}

fn decode_signed_pcm_to_i16(sample: &[u8], endian: AudioEndian) -> Result<i16> {
    let value = match sample.len() {
        1 => (sample[0] as i8 as i16) << 8,
        2 => match endian {
            AudioEndian::Little => i16::from_le_bytes([sample[0], sample[1]]),
            AudioEndian::Big => i16::from_be_bytes([sample[0], sample[1]]),
        },
        3 => {
            let signed = sign_extend_24(match endian {
                AudioEndian::Little => {
                    i32::from(sample[0])
                        | (i32::from(sample[1]) << 8)
                        | (i32::from(sample[2]) << 16)
                }
                AudioEndian::Big => {
                    i32::from(sample[2])
                        | (i32::from(sample[1]) << 8)
                        | (i32::from(sample[0]) << 16)
                }
            });
            (signed >> 8) as i16
        }
        4 => {
            let signed = match endian {
                AudioEndian::Little => {
                    i32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]])
                }
                AudioEndian::Big => {
                    i32::from_be_bytes([sample[0], sample[1], sample[2], sample[3]])
                }
            };
            (signed >> 16) as i16
        }
        _ => {
            return Err(OxideError::InvalidFormat(
                "unsupported signed PCM bytes_per_sample",
            ));
        }
    };
    Ok(value)
}

fn decode_unsigned_pcm_to_i16(sample: &[u8], endian: AudioEndian) -> Result<i16> {
    let value = match sample.len() {
        1 => ((i16::from(sample[0])) - 128) << 8,
        2 => {
            let unsigned = match endian {
                AudioEndian::Little => u16::from_le_bytes([sample[0], sample[1]]),
                AudioEndian::Big => u16::from_be_bytes([sample[0], sample[1]]),
            };
            (i32::from(unsigned) - 32768) as i16
        }
        3 => {
            let unsigned = match endian {
                AudioEndian::Little => {
                    u32::from(sample[0])
                        | (u32::from(sample[1]) << 8)
                        | (u32::from(sample[2]) << 16)
                }
                AudioEndian::Big => {
                    u32::from(sample[2])
                        | (u32::from(sample[1]) << 8)
                        | (u32::from(sample[0]) << 16)
                }
            };
            let centered = i32::try_from(unsigned)
                .map_err(|_| OxideError::InvalidFormat("unsigned PCM conversion overflow"))?
                - 8_388_608;
            (centered >> 8) as i16
        }
        4 => {
            let unsigned = match endian {
                AudioEndian::Little => {
                    u32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]])
                }
                AudioEndian::Big => {
                    u32::from_be_bytes([sample[0], sample[1], sample[2], sample[3]])
                }
            };
            let centered = i64::from(unsigned) - 2_147_483_648;
            (centered >> 16) as i16
        }
        _ => {
            return Err(OxideError::InvalidFormat(
                "unsupported unsigned PCM bytes_per_sample",
            ));
        }
    };
    Ok(value)
}

fn decode_float_to_i16(sample: &[u8], endian: AudioEndian) -> Result<i16> {
    let normalized = match sample.len() {
        4 => match endian {
            AudioEndian::Little => f32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]]),
            AudioEndian::Big => f32::from_be_bytes([sample[0], sample[1], sample[2], sample[3]]),
        },
        8 => {
            let value = match endian {
                AudioEndian::Little => f64::from_le_bytes([
                    sample[0], sample[1], sample[2], sample[3], sample[4], sample[5], sample[6],
                    sample[7],
                ]),
                AudioEndian::Big => f64::from_be_bytes([
                    sample[0], sample[1], sample[2], sample[3], sample[4], sample[5], sample[6],
                    sample[7],
                ]),
            };
            value as f32
        }
        _ => {
            return Err(OxideError::InvalidFormat(
                "float audio must use 4 or 8 bytes per sample",
            ));
        }
    };

    let clamped = normalized.clamp(-1.0, 1.0);
    let scaled = if clamped <= -1.0 {
        i16::MIN
    } else if clamped >= 1.0 {
        i16::MAX
    } else {
        (clamped * i16::MAX as f32).round() as i16
    };
    Ok(scaled)
}

fn sign_extend_24(value: i32) -> i32 {
    if value & 0x0080_0000 != 0 {
        value | !0x00FF_FFFF
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
