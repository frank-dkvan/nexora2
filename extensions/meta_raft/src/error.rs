//! Error types for RisingWave Meta Raft HA extension.

use std::fmt;

/// Result type alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for the Raft election client.
#[derive(Debug)]
pub enum Error {
    /// Consensus layer error (from nexora-consensus)
    Consensus(String),

    /// Network error (RPC communication)
    Network(String),

    /// Storage error (Raft log persistence)
    Storage(String),

    /// Election error (leader election failures)
    Election(String),

    /// Configuration error
    Config(String),

    /// Internal error
    Internal(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Consensus(msg) => write!(f, "Consensus error: {}", msg),
            Error::Network(msg) => write!(f, "Network error: {}", msg),
            Error::Storage(msg) => write!(f, "Storage error: {}", msg),
            Error::Election(msg) => write!(f, "Election error: {}", msg),
            Error::Config(msg) => write!(f, "Config error: {}", msg),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl From<nexora_consensus::ConsensusError> for Error {
    fn from(err: nexora_consensus::ConsensusError) -> Self {
        Error::Consensus(err.to_string())
    }
}

impl From<nexora_rpc::RpcError> for Error {
    fn from(err: nexora_rpc::RpcError) -> Self {
        Error::Network(err.to_string())
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Internal(err.to_string())
    }
}

// Helper constructors
impl Error {
    pub fn consensus(msg: impl Into<String>) -> Self {
        Error::Consensus(msg.into())
    }

    pub fn network(msg: impl Into<String>) -> Self {
        Error::Network(msg.into())
    }

    pub fn storage(msg: impl Into<String>) -> Self {
        Error::Storage(msg.into())
    }

    pub fn election(msg: impl Into<String>) -> Self {
        Error::Election(msg.into())
    }

    pub fn config(msg: impl Into<String>) -> Self {
        Error::Config(msg.into())
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}
