use crate::{CompressionAlgo, Result};

pub mod deflate;
pub mod lz4;
pub mod lzma;

pub fn apply_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgo::Lz4 => lz4::apply(data),
        CompressionAlgo::Lzma => lzma::apply(data),
        CompressionAlgo::Deflate => deflate::apply(data),
    }
}

pub fn reverse_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgo::Lz4 => lz4::reverse(data),
        CompressionAlgo::Lzma => lzma::reverse(data),
        CompressionAlgo::Deflate => deflate::reverse(data),
    }
}
