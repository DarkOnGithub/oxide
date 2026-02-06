use thiserror::Error;

#[derive(Debug, Error)]
pub enum OxideError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid format: {0}")]
    InvalidFormat(&'static str),
    #[error("checksum mismatch (expected {expected:#010x}, actual {actual:#010x})")]
    ChecksumMismatch { expected: u32, actual: u32 },
    #[error("compression error: {0}")]
    CompressionError(String),
    #[error("decompression error: {0}")]
    DecompressionError(String),
    #[error("invalid block id (expected {expected}, actual {actual})")]
    InvalidBlockId { expected: u64, actual: u64 },
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<OxideError>,
    },
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl OxideError {
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Self::Context {
            context: context.into(),
            source: Box::new(self),
        }
    }
}
