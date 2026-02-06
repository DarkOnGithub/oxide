//! File format detection module.
//!
//! This module provides automatic file format detection using the `infer`
//! crate along with additional heuristics for edge cases.

pub mod detector;

pub use detector::FormatDetector;
