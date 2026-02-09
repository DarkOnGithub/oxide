use crate::{AudioStrategy, BinaryStrategy, ImageStrategy, PreProcessingStrategy, Result, TextStrategy};

pub mod audio_lpc;
pub mod binary_bcj;
pub mod image_locoi;
pub mod image_paeth;
pub mod image_ycocgr;
pub mod text_bpe;
pub mod text_bwt;

pub fn apply_preprocessing(data: &[u8], strategy: &PreProcessingStrategy) -> Result<Vec<u8>> {
    match strategy {
        PreProcessingStrategy::None => Ok(data.to_vec()),
        PreProcessingStrategy::Text(TextStrategy::Bpe) => text_bpe::apply(data),
        PreProcessingStrategy::Text(TextStrategy::Bwt) => text_bwt::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => image_ycocgr::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::Paeth) => image_paeth::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::LocoI) => image_locoi::apply(data),
        PreProcessingStrategy::Audio(AudioStrategy::Lpc) => audio_lpc::apply(data),
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj) => binary_bcj::apply(data),
    }
}

pub fn reverse_preprocessing(data: &[u8], strategy: &PreProcessingStrategy) -> Result<Vec<u8>> {
    match strategy {
        PreProcessingStrategy::None => Ok(data.to_vec()),
        PreProcessingStrategy::Text(TextStrategy::Bpe) => text_bpe::reverse(data),
        PreProcessingStrategy::Text(TextStrategy::Bwt) => text_bwt::reverse(data),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => image_ycocgr::reverse(data),
        PreProcessingStrategy::Image(ImageStrategy::Paeth) => image_paeth::reverse(data),
        PreProcessingStrategy::Image(ImageStrategy::LocoI) => image_locoi::reverse(data),
        PreProcessingStrategy::Audio(AudioStrategy::Lpc) => audio_lpc::reverse(data),
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj) => binary_bcj::reverse(data),
    }
}
