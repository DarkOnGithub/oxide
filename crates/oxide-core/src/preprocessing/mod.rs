use crate::OxideError;
use crate::{
    AudioStrategy, BinaryStrategy, CompressionAlgo, FileFormat, ImageStrategy,
    PreProcessingStrategy, Result, TextStrategy,
};

pub mod audio_lpc;
pub mod binary_bcj;
pub mod image_locoi;
pub mod image_paeth;
pub mod image_ycocgr;
pub mod text_bpe;
pub mod text_bwt;
pub mod utils;

pub use utils::{AudioEndian, AudioMetadata, AudioSampleEncoding, ImageMetadata, ImagePixelFormat};

/// Optional metadata used to interpret raw bytes for format-aware preprocessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreprocessingMetadata {
    Image(ImageMetadata),
    Audio(AudioMetadata),
}

/// Dispatches preprocessing to the specified strategy.
pub fn apply_preprocessing(data: &[u8], strategy: &PreProcessingStrategy) -> Result<Vec<u8>> {
    apply_preprocessing_with_metadata(data, strategy, None)
}

/// Dispatches preprocessing to the specified strategy with optional input metadata.
pub fn apply_preprocessing_with_metadata(
    data: &[u8],
    strategy: &PreProcessingStrategy,
    metadata: Option<&PreprocessingMetadata>,
) -> Result<Vec<u8>> {
    let result = match strategy {
        PreProcessingStrategy::None => Ok(data.to_vec()),
        PreProcessingStrategy::Text(TextStrategy::Bpe) => text_bpe::apply(data),
        PreProcessingStrategy::Text(TextStrategy::Bwt) => text_bwt::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => {
            image_ycocgr::apply(data, metadata_as_image(metadata)?)
        }
        PreProcessingStrategy::Image(ImageStrategy::Paeth) => {
            image_paeth::apply(data, metadata_as_image(metadata)?)
        }
        PreProcessingStrategy::Image(ImageStrategy::LocoI) => {
            image_locoi::apply(data, metadata_as_image(metadata)?)
        }
        PreProcessingStrategy::Audio(AudioStrategy::Lpc) => {
            audio_lpc::apply(data, metadata_as_audio(metadata)?)
        }
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj) => binary_bcj::apply(data),
    };

    result
}

fn metadata_as_image(metadata: Option<&PreprocessingMetadata>) -> Result<Option<&ImageMetadata>> {
    match metadata {
        None => Ok(None),
        Some(PreprocessingMetadata::Image(metadata)) => Ok(Some(metadata)),
        Some(_) => Err(OxideError::InvalidFormat(
            "preprocessing metadata type mismatch for image strategy",
        )),
    }
}

fn metadata_as_audio(metadata: Option<&PreprocessingMetadata>) -> Result<Option<&AudioMetadata>> {
    match metadata {
        None => Ok(None),
        Some(PreprocessingMetadata::Audio(metadata)) => Ok(Some(metadata)),
        Some(_) => Err(OxideError::InvalidFormat(
            "preprocessing metadata type mismatch for audio strategy",
        )),
    }
}

pub fn get_preprocessing_strategy(
    file_type_hint: FileFormat,
    compression_algo: CompressionAlgo,
) -> PreProcessingStrategy {
    match (file_type_hint, compression_algo) {
        (FileFormat::Image, _) => PreProcessingStrategy::Image(ImageStrategy::YCoCgR),
        (FileFormat::Audio, _) => PreProcessingStrategy::Audio(AudioStrategy::Lpc),
        (FileFormat::Binary, _) => PreProcessingStrategy::Binary(BinaryStrategy::Bcj),
        _ => PreProcessingStrategy::None,
    }
}

/// Dispatches reverse preprocessing to the specified strategy.
pub fn reverse_preprocessing(data: &[u8], strategy: &PreProcessingStrategy) -> Result<Vec<u8>> {
    let result = match strategy {
        PreProcessingStrategy::None => Ok(data.to_vec()),
        PreProcessingStrategy::Text(TextStrategy::Bpe) => text_bpe::reverse(data),
        PreProcessingStrategy::Text(TextStrategy::Bwt) => text_bwt::reverse(data),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => image_ycocgr::reverse(data),
        PreProcessingStrategy::Image(ImageStrategy::Paeth) => image_paeth::reverse(data),
        PreProcessingStrategy::Image(ImageStrategy::LocoI) => image_locoi::reverse(data),
        PreProcessingStrategy::Audio(AudioStrategy::Lpc) => audio_lpc::reverse(data),
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj) => binary_bcj::reverse(data),
    };

    result
}
