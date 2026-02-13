use std::time::Instant;

use crate::telemetry::{self, profile, tags};
use crate::types::duration_to_us;
use crate::{
    AudioStrategy, BinaryStrategy, ImageStrategy, PreProcessingStrategy, Result, TextStrategy,
};

pub mod audio_lpc;
pub mod binary_bcj;
pub mod image_locoi;
pub mod image_paeth;
pub mod image_ycocgr;
pub mod text_bpe;
pub mod text_bwt;

/// Dispatches preprocessing to the specified strategy.
pub fn apply_preprocessing(data: &[u8], strategy: &PreProcessingStrategy) -> Result<Vec<u8>> {
    let start = Instant::now();
    let input_bytes = data.len() as u64;

    let result = match strategy {
        PreProcessingStrategy::None => Ok(data.to_vec()),
        PreProcessingStrategy::Text(TextStrategy::Bpe) => text_bpe::apply(data),
        PreProcessingStrategy::Text(TextStrategy::Bwt) => text_bwt::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => image_ycocgr::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::Paeth) => image_paeth::apply(data),
        PreProcessingStrategy::Image(ImageStrategy::LocoI) => image_locoi::apply(data),
        PreProcessingStrategy::Audio(AudioStrategy::Lpc) => audio_lpc::apply(data),
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj) => binary_bcj::apply(data),
    };

    if let Ok(ref processed) = result {
        let elapsed_us = duration_to_us(start.elapsed());
        let output_bytes = processed.len() as u64;
        let strategy_str = strategy_to_str(strategy);

        let labels = [("strategy", strategy_str)];
        telemetry::increment_counter(tags::METRIC_PREPROCESSING_APPLY_COUNT, 1, &labels);
        telemetry::record_histogram(
            tags::METRIC_PREPROCESSING_APPLY_LATENCY_US,
            elapsed_us,
            &labels,
        );
        telemetry::record_histogram(tags::METRIC_PREPROCESSING_INPUT_BYTES, input_bytes, &labels);
        telemetry::record_histogram(tags::METRIC_PREPROCESSING_OUTPUT_BYTES, output_bytes, &labels);

        profile::event(
            tags::PROFILE_PREPROCESSING,
            &[tags::TAG_PREPROCESSING, strategy_str],
            "apply",
            "ok",
            elapsed_us,
            "preprocessing applied",
        );
    }

    result
}

/// Dispatches reverse preprocessing to the specified strategy.
pub fn reverse_preprocessing(data: &[u8], strategy: &PreProcessingStrategy) -> Result<Vec<u8>> {
    let start = Instant::now();
    let input_bytes = data.len() as u64;

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

    if let Ok(ref reversed) = result {
        let elapsed_us = duration_to_us(start.elapsed());
        let output_bytes = reversed.len() as u64;
        let strategy_str = strategy_to_str(strategy);

        let labels = [("strategy", strategy_str)];
        telemetry::increment_counter(tags::METRIC_PREPROCESSING_REVERSE_COUNT, 1, &labels);
        telemetry::record_histogram(
            tags::METRIC_PREPROCESSING_REVERSE_LATENCY_US,
            elapsed_us,
            &labels,
        );
        telemetry::record_histogram(tags::METRIC_PREPROCESSING_INPUT_BYTES, input_bytes, &labels);
        telemetry::record_histogram(tags::METRIC_PREPROCESSING_OUTPUT_BYTES, output_bytes, &labels);

        profile::event(
            tags::PROFILE_PREPROCESSING,
            &[tags::TAG_PREPROCESSING, strategy_str],
            "reverse",
            "ok",
            elapsed_us,
            "preprocessing reversed",
        );
    }

    result
}

fn strategy_to_str(strategy: &PreProcessingStrategy) -> &'static str {
    match strategy {
        PreProcessingStrategy::None => "none",
        PreProcessingStrategy::Text(TextStrategy::Bpe) => "text_bpe",
        PreProcessingStrategy::Text(TextStrategy::Bwt) => "text_bwt",
        PreProcessingStrategy::Image(ImageStrategy::YCoCgR) => "image_ycocgr",
        PreProcessingStrategy::Image(ImageStrategy::Paeth) => "image_paeth",
        PreProcessingStrategy::Image(ImageStrategy::LocoI) => "image_locoi",
        PreProcessingStrategy::Audio(AudioStrategy::Lpc) => "audio_lpc",
        PreProcessingStrategy::Binary(BinaryStrategy::Bcj) => "binary_bcj",
    }
}
