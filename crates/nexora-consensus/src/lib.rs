//! Consensus abstraction layer for distributed coordination.
//!
//! Provides a unified [`ConsensusClient`] trait that abstracts over different
//! consensus protocols. The primary implementation uses [openraft](https://docs.rs/openraft)
//! for Raft consensus.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  Application Layer (Nexora, RisingWave) │
//! └────────────────┬────────────────────────┘
//!                  │
//!                  v
//! ┌─────────────────────────────────────────┐
//! │     ConsensusClient trait (abstract)    │
//! └────────────────┬────────────────────────┘
//!                  │
//!                  v
//! ┌─────────────────────────────────────────┐
//! │    RaftConsensusClient (openraft 0.9)   │
//! │      ├─ RaftStorage (persistence)       │
//! │      └─ RaftNetwork (communication)     │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use nexora_consensus::{ConsensusClient, RaftConsensusClient, RaftConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a Raft node
//! let config = RaftConfig::new(1, "127.0.0.1:5690".parse()?);
//!
//! let client = RaftConsensusClient::new(config).await?;
//!
//! // Check leadership
//! if client.is_leader().await? {
//!     println!("This node is the leader");
//! }
//!
//! // Commit data
//! let log_index = client.commit(b"data".to_vec().into()).await?;
//! println!("Committed at index: {}", log_index);
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod error;
pub mod network;
pub mod raft_impl;
pub mod storage;
pub mod types;

pub use client::ConsensusClient;
pub use error::{ConsensusError, Result};
pub use network::{NetworkConfig, RaftNetwork};
pub use raft_impl::{RaftConfig, RaftConsensusClient};
pub use storage::{LogEntry, RaftStorage};
pub use types::{LogIndex, NodeId};
