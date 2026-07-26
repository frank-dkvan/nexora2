//! Error types for consensus operations.

use thiserror::Error;

/// Result type for consensus operations.
pub type Result<T> = std::result::Result<T, ConsensusError>;

/// Errors that can occur during consensus operations.
#[derive(Debug, Error)]
pub enum ConsensusError {
    /// Node is not the leader and cannot process writes.
    #[error("not leader: current leader is node {leader_id:?}")]
    NotLeader {
        /// ID of the current leader, if known.
        leader_id: Option<u64>,
    },

    /// Raft operation failed.
    #[error("raft error: {0}")]
    Raft(String),

    /// Network communication error.
    #[error("network error: {0}")]
    Network(String),

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Node is shutting down.
    #[error("node is shutting down")]
    ShuttingDown,

    /// Generic error.
    #[error("consensus error: {0}")]
    Other(String),
}

impl ConsensusError {
    /// Create a Raft error from any error type.
    pub fn raft<E: std::fmt::Display>(err: E) -> Self {
        Self::Raft(err.to_string())
    }

    /// Create a network error from any error type.
    pub fn network<E: std::fmt::Display>(err: E) -> Self {
        Self::Network(err.to_string())
    }

    /// Create a storage error from any error type.
    pub fn storage<E: std::fmt::Display>(err: E) -> Self {
        Self::Storage(err.to_string())
    }

    /// Check if this error indicates the node is not a leader.
    pub fn is_not_leader(&self) -> bool {
        matches!(self, ConsensusError::NotLeader { .. })
    }
}
