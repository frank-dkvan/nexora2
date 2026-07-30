//! Error types for RisingWave integration.

use thiserror::Error;

/// Result type for RisingWave operations.
pub type Result<T> = std::result::Result<T, EventStreamingError>;

/// Errors that can occur during RisingWave operations.
#[derive(Debug, Error)]
pub enum EventStreamingError {
    /// Meta node failed to start.
    #[error("meta node start failed: {0}")]
    MetaStartFailed(String),

    /// Frontend node failed to start.
    #[error("frontend node start failed: {0}")]
    FrontendStartFailed(String),

    /// Compute node failed to start.
    #[error("compute node start failed: {0}")]
    ComputeStartFailed(String),

    /// DDL execution failed.
    #[error("ddl execution failed: {0}")]
    DdlFailed(String),

    /// Query execution failed.
    #[error("query execution failed: {0}")]
    QueryFailed(String),

    /// Materialized view not found.
    #[error("materialized view not found: {0}")]
    MvNotFound(String),

    /// Subscription failed.
    #[error("subscription failed: {0}")]
    SubscriptionFailed(String),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Configuration error (alias for compatibility).
    #[error("config error: {0}")]
    Config(String),

    /// Timeout error.
    #[error("timeout: {0}")]
    Timeout(String),

    /// Consensus error.
    #[error("consensus error: {0}")]
    Consensus(#[from] nexora_consensus::ConsensusError),

    /// RPC error.
    #[error("rpc error: {0}")]
    Rpc(#[from] nexora_rpc::RpcError),

    /// Generic error.
    #[error("risingwave error: {0}")]
    Other(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl EventStreamingError {
    /// Create a DDL error from any error type.
    #[allow(dead_code)]
    pub fn ddl<E: std::fmt::Display>(err: E) -> Self {
        Self::DdlFailed(err.to_string())
    }

    /// Create a query error from any error type.
    #[allow(dead_code)]
    pub fn query<E: std::fmt::Display>(err: E) -> Self {
        Self::QueryFailed(err.to_string())
    }

    /// Create a generic error from any error type.
    #[allow(dead_code)]
    pub fn other<E: std::fmt::Display>(err: E) -> Self {
        Self::Other(err.to_string())
    }
}
