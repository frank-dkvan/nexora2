//! Error types for RPC operations.

use thiserror::Error;

/// Result type for RPC operations.
pub type Result<T> = std::result::Result<T, RpcError>;

/// Errors that can occur during RPC operations.
#[derive(Debug, Error)]
pub enum RpcError {
    /// Server failed to start.
    #[error("server start failed: {0}")]
    ServerStart(String),

    /// Server failed to shutdown.
    #[error("server shutdown failed: {0}")]
    ServerShutdown(String),

    /// Client connection failed.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// RPC call failed.
    #[error("rpc call failed: {0}")]
    CallFailed(String),

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Transport error.
    #[error("transport error: {0}")]
    Transport(String),

    /// Timeout error.
    #[error("timeout after {0}ms")]
    Timeout(u64),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Generic error.
    #[error("rpc error: {0}")]
    Other(String),
}

impl RpcError {
    /// Create a connection error from any error type.
    pub fn connection<E: std::fmt::Display>(err: E) -> Self {
        Self::ConnectionFailed(err.to_string())
    }

    /// Create a call error from any error type.
    pub fn call<E: std::fmt::Display>(err: E) -> Self {
        Self::CallFailed(err.to_string())
    }

    /// Create a transport error from any error type.
    pub fn transport<E: std::fmt::Display>(err: E) -> Self {
        Self::Transport(err.to_string())
    }

    /// Create a serialization error from any error type.
    pub fn serialization<E: std::fmt::Display>(err: E) -> Self {
        Self::Serialization(err.to_string())
    }
}

// Convert from tonic errors
impl From<tonic::transport::Error> for RpcError {
    fn from(err: tonic::transport::Error) -> Self {
        Self::Transport(err.to_string())
    }
}

impl From<tonic::Status> for RpcError {
    fn from(err: tonic::Status) -> Self {
        Self::CallFailed(err.message().to_string())
    }
}
