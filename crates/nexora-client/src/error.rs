//! Error types for the nexora-client SDK.
//!
//! All client methods return `Result<T, NexoraClientError>`.

use thiserror::Error;

/// The unified error type returned by all `NexoraClient` methods.
#[derive(Debug, Error)]
pub enum NexoraClientError {
    /// HTTP request failed at the transport level (connection refused, DNS, TLS, etc.).
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// The server returned a non-success status code.
    ///
    /// The variant stores the status code and the response body text (which may
    /// contain a server-side error message).
    #[error("HTTP {status}: {body}")]
    Status {
        /// HTTP status code returned by the server.
        status: u16,
        /// Raw response body text.
        body: String,
    },

    /// The response body could not be deserialized into the expected type.
    #[error("Failed to deserialize response: {0}")]
    Deserialize(#[from] serde_json::Error),

    /// The client was configured with an invalid URL.
    #[error("Invalid URL: {0}")]
    Url(String),

    /// A request timed out.
    #[error("Request timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// The maximum number of retries was exceeded.
    #[error("Max retries ({0}) exceeded")]
    MaxRetriesExceeded(u32),

    /// A catch-all for errors that don't fit the above categories.
    #[error("{0}")]
    Other(String),
}

impl NexoraClientError {
    /// Returns the HTTP status code if this is a `Status` variant.
    pub fn status_code(&self) -> Option<u16> {
        match self {
            NexoraClientError::Status { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// Returns `true` if the error is due to a server-side error (5xx).
    pub fn is_server_error(&self) -> bool {
        matches!(self.status_code(), Some(s) if s >= 500)
    }

    /// Returns `true` if the error is retryable (connection errors, 5xx, 429).
    pub fn is_retryable(&self) -> bool {
        match self {
            NexoraClientError::Http(e) => e.is_timeout() || e.is_connect(),
            NexoraClientError::Status { status, .. } => *status == 429 || *status >= 500,
            NexoraClientError::Timeout(_) => true,
            _ => false,
        }
    }
}
