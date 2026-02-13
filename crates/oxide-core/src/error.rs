use thiserror::Error;

/// Core error type for all Oxide operations.
#[derive(Debug, Error)]
pub enum OxideError {
    /// I/O operation failed
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid format or corrupted data
    #[error("invalid format: {0}")]
    InvalidFormat(&'static str),

    /// CRC32 checksum verification failed
    #[error("checksum mismatch (expected {expected:#010x}, actual {actual:#010x})")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// Compression operation failed
    #[error("compression error: {0}")]
    CompressionError(String),

    /// Decompression operation failed
    #[error("decompression error: {0}")]
    DecompressionError(String),

    /// Block ID mismatch during processing
    #[error("invalid block id (expected {expected}, actual {actual})")]
    InvalidBlockId { expected: u64, actual: u64 },

    /// Error with additional context
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<OxideError>,
    },

    /// Other error types
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl OxideError {
    /// Wraps this error with additional context.
    ///
    /// # Arguments
    /// * `context` - Description of what was being attempted when the error occurred
    ///
    /// # Example
    /// ```ignore
    /// let result = some_operation().map_err(|e| e.with_context("reading header"));
    /// ```
    pub fn with_context(self, context: impl Into<String>) -> Self {
        Self::Context {
            context: context.into(),
            source: Box::new(self),
        }
    }
}
