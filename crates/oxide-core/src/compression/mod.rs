use crate::{CompressionAlgo, Result};

pub mod lz4;
pub mod lzma;
pub(crate) mod scratch;
pub mod zstd;

pub(crate) use scratch::CompressionScratchArena;

/// Compression parameters for a single encode request.
#[derive(Debug, Clone, Copy)]
pub struct CompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub level: Option<i32>,
    pub lzma_extreme: bool,
    pub lzma_dictionary_size: Option<usize>,
    pub stream_id: u32,
    pub dictionary_id: u8,
    pub dictionary: Option<&'a [u8]>,
}

impl<'a> CompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            level: None,
            lzma_extreme: false,
            lzma_dictionary_size: None,
            stream_id: 0,
            dictionary_id: 0,
            dictionary: None,
        }
    }
}

/// Decompression parameters for a single decode request.
#[derive(Debug, Clone, Copy)]
pub struct DecompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub raw_len: Option<usize>,
    pub dictionary_id: u8,
    pub dictionary: Option<&'a [u8]>,
}

impl<'a> DecompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            raw_len: None,
            dictionary_id: 0,
            dictionary: None,
        }
    }

    pub fn with_raw_len(mut self, raw_len: usize) -> Self {
        self.raw_len = Some(raw_len);
        self
    }
}

/// Dispatches compression to the specified algorithm.
pub fn apply_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    apply_compression_request_with_scratch(CompressionRequest::new(data, algo), &mut scratch)
}

#[inline]
pub(crate) fn supports_direct_buffer_output(algo: CompressionAlgo) -> bool {
    matches!(
        algo,
        CompressionAlgo::Lz4 | CompressionAlgo::Lzma | CompressionAlgo::Zstd
    )
}

pub(crate) fn apply_compression_request_with_scratch(
    request: CompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
) -> Result<Vec<u8>> {
    match request.algo {
        CompressionAlgo::Lz4 => lz4::apply_with_scratch(request.data, scratch.lz4()),
        CompressionAlgo::Lzma => lzma::apply_with_scratch(
            request.data,
            request.level,
            request.lzma_extreme,
            request.lzma_dictionary_size,
            scratch.lzma(),
        ),
        CompressionAlgo::Zstd => zstd::apply_with_scratch(
            request.data,
            request.level,
            request.stream_id,
            request.dictionary_id,
            request.dictionary,
            scratch.zstd(),
        ),
    }
}

pub(crate) fn apply_compression_request_with_scratch_into(
    request: CompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
    output: &mut Vec<u8>,
) -> Result<()> {
    match request.algo {
        CompressionAlgo::Lz4 => lz4::apply_into_vec(request.data, scratch.lz4(), output),
        CompressionAlgo::Lzma => lzma::apply_into_vec(
            request.data,
            request.level,
            request.lzma_extreme,
            request.lzma_dictionary_size,
            output,
        ),
        CompressionAlgo::Zstd => zstd::apply_into_vec(
            request.data,
            request.level,
            request.stream_id,
            request.dictionary_id,
            request.dictionary,
            scratch.zstd(),
            output,
        ),
    }
}

pub(crate) fn recycle_compression_buffer(
    algo: CompressionAlgo,
    buffer: Vec<u8>,
    scratch: &mut CompressionScratchArena,
) {
    match algo {
        CompressionAlgo::Lzma => lzma::recycle_output(buffer, scratch.lzma()),
        CompressionAlgo::Lz4 => lz4::recycle_output(buffer, scratch.lz4()),
        CompressionAlgo::Zstd => zstd::recycle_output(buffer, scratch.zstd()),
    }
}

/// Dispatches decompression to the specified algorithm.
pub fn reverse_compression(data: &[u8], algo: CompressionAlgo) -> Result<Vec<u8>> {
    reverse_compression_request(DecompressionRequest::new(data, algo))
}

pub(crate) fn reverse_compression_request(request: DecompressionRequest<'_>) -> Result<Vec<u8>> {
    let mut scratch = CompressionScratchArena::new();
    reverse_compression_request_with_scratch(request, &mut scratch)
}

pub(crate) fn reverse_compression_request_with_scratch(
    request: DecompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
) -> Result<Vec<u8>> {
    match request.algo {
        CompressionAlgo::Lz4 => lz4::reverse_with_scratch(request.data, scratch.lz4()),
        CompressionAlgo::Lzma => lzma::reverse_with_scratch(request.data, scratch.lzma()),
        CompressionAlgo::Zstd => zstd::reverse_with_scratch(
            request.data,
            request.raw_len,
            request.dictionary_id,
            request.dictionary,
            scratch.zstd(),
        ),
    }
}

pub(crate) fn reverse_compression_request_with_scratch_into(
    request: DecompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
    output: &mut Vec<u8>,
) -> Result<()> {
    match request.algo {
        CompressionAlgo::Lz4 => lz4::reverse_into_vec(request.data, output),
        CompressionAlgo::Lzma => lzma::reverse_into_vec(request.data, output),
        CompressionAlgo::Zstd => zstd::reverse_into_vec(
            request.data,
            request.raw_len,
            request.dictionary_id,
            request.dictionary,
            scratch.zstd(),
            output,
        ),
    }
}
