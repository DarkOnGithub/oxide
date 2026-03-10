use crate::{CompressionAlgo, CompressionPreset, Result};

pub mod lz4;
pub(crate) mod scratch;

pub(crate) use scratch::CompressionScratchArena;

/// Compression parameters for a single encode request.
#[derive(Debug, Clone, Copy)]
pub struct CompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub preset: CompressionPreset,
    pub dictionary: Option<&'a [u8]>,
}

impl<'a> CompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            preset: CompressionPreset::Default,
            dictionary: None,
        }
    }
}

/// Decompression parameters for a single decode request.
#[derive(Debug, Clone, Copy)]
pub struct DecompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub dictionary: Option<&'a [u8]>,
}

impl<'a> DecompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            dictionary: None,
        }
    }
}

/// Dispatches compression to the specified algorithm.
pub fn apply_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    apply_compression_request_with_scratch(CompressionRequest::new(data, algo), &mut scratch)
}

pub(crate) fn apply_compression_request_with_scratch(
    request: CompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
) -> Result<Vec<u8>> {
    match request.algo {
        CompressionAlgo::Lz4 => lz4::apply_with_scratch(
            request.data,
            request.preset,
            request.dictionary,
            scratch.lz4(),
        ),
    }
}

/// Dispatches decompression to the specified algorithm.
pub fn reverse_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    reverse_compression_request(DecompressionRequest::new(data, algo))
}

pub(crate) fn reverse_compression_request(request: DecompressionRequest<'_>) -> Result<Vec<u8>> {
    match request.algo {
        CompressionAlgo::Lz4 => lz4::reverse_with_dictionary(request.data, request.dictionary),
    }
}
