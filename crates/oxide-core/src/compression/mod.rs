use crate::{CompressionAlgo, CompressionPreset, Result};

pub mod lz4;
pub mod lzma;
pub(crate) mod scratch;
pub mod zpaq;
pub mod zstd;

pub(crate) use scratch::CompressionScratchArena;

/// Compression parameters for a single encode request.
#[derive(Debug, Clone, Copy)]
pub struct CompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub preset: CompressionPreset,
    pub zstd_level: Option<i32>,
}

impl<'a> CompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            preset: CompressionPreset::Default,
            zstd_level: None,
        }
    }
}

/// Decompression parameters for a single decode request.
#[derive(Debug, Clone, Copy)]
pub struct DecompressionRequest<'a> {
    pub data: &'a [u8],
    pub algo: CompressionAlgo,
    pub raw_len: Option<usize>,
}

impl<'a> DecompressionRequest<'a> {
    pub fn new(data: &'a [u8], algo: CompressionAlgo) -> Self {
        Self {
            data,
            algo,
            raw_len: None,
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
    matches!(algo, CompressionAlgo::Lzma | CompressionAlgo::Zpaq)
}

pub(crate) fn apply_compression_request_with_scratch(
    request: CompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
) -> Result<Vec<u8>> {
    match request.algo {
        CompressionAlgo::Lz4 => {
            lz4::apply_with_scratch(request.data, request.preset, scratch.lz4())
        }
        CompressionAlgo::Lzma => {
            lzma::apply_with_scratch(request.data, request.preset, scratch.lzma())
        }
        CompressionAlgo::Zpaq => {
            zpaq::apply_with_scratch(request.data, request.preset, scratch.zpaq())
        }
        CompressionAlgo::Zstd => zstd::apply_with_scratch(
            request.data,
            request.preset,
            request.zstd_level,
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
        CompressionAlgo::Lz4 => {
            let compressed = lz4::apply_with_scratch(request.data, request.preset, scratch.lz4())?;
            output.clear();
            output.extend_from_slice(&compressed);
            Ok(())
        }
        CompressionAlgo::Lzma => lzma::apply_into_vec(request.data, request.preset, output),
        CompressionAlgo::Zpaq => zpaq::apply_into_vec(request.data, request.preset, output),
        CompressionAlgo::Zstd => {
            let compressed = zstd::apply_with_scratch(
                request.data,
                request.preset,
                request.zstd_level,
                scratch.zstd(),
            )?;
            output.clear();
            output.extend_from_slice(&compressed);
            Ok(())
        }
    }
}

pub(crate) fn recycle_compression_buffer(
    algo: CompressionAlgo,
    buffer: Vec<u8>,
    scratch: &mut CompressionScratchArena,
) {
    match algo {
        CompressionAlgo::Lzma => lzma::recycle_output(buffer, scratch.lzma()),
        CompressionAlgo::Zpaq => zpaq::recycle_output(buffer, scratch.zpaq()),
        CompressionAlgo::Lz4 | CompressionAlgo::Zstd => {
            let _ = buffer;
        }
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
        CompressionAlgo::Lz4 => lz4::reverse(request.data),
        CompressionAlgo::Lzma => lzma::reverse_with_scratch(request.data, scratch.lzma()),
        CompressionAlgo::Zpaq => zpaq::reverse_with_scratch(request.data, scratch.zpaq()),
        CompressionAlgo::Zstd => {
            zstd::reverse_with_scratch(request.data, request.raw_len, scratch.zstd())
        }
    }
}

pub(crate) fn reverse_compression_request_with_scratch_into(
    request: DecompressionRequest<'_>,
    scratch: &mut CompressionScratchArena,
    output: &mut Vec<u8>,
) -> Result<()> {
    match request.algo {
        CompressionAlgo::Lz4 => {
            let decoded = lz4::reverse(request.data)?;
            output.clear();
            output.extend_from_slice(&decoded);
            Ok(())
        }
        CompressionAlgo::Lzma => lzma::reverse_into_vec(request.data, output),
        CompressionAlgo::Zpaq => zpaq::reverse_into_vec(request.data, output),
        CompressionAlgo::Zstd => {
            let decoded =
                zstd::reverse_with_scratch(request.data, request.raw_len, scratch.zstd())?;
            output.clear();
            output.extend_from_slice(&decoded);
            Ok(())
        }
    }
}
