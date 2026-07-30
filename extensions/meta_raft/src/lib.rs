//! RisingWave Meta Raft HA Extension
//!
//! This crate provides a Raft-based leader election implementation for RisingWave Meta nodes,
//! replacing the SQL-based election with embedded Raft consensus.
//!
//! # Architecture
//!
//! ```text
//! RisingWave Meta Node
//!   │
//!   ├─► ElectionClient trait (RisingWave)
//!   │     └─► RaftElectionClient (this crate)
//!   │           └─► ConsensusClient trait (nexora-consensus)
//!   │                 └─► RaftConsensusClient (openraft 0.9)
//!   │
//!   └─► MetaService, CatalogManager, etc.
//! ```
//!
//! # Phase 4 Implementation
//!
//! Phase 4 integrates actual RisingWave components with our Raft implementation:
//! - Implements `ElectionClient` trait using `nexora-consensus`
//! - Provides Raft storage and network layers
//! - Enables 3-5 node Meta cluster with automatic failover
//!
//! # Example
//!
//! ```rust,no_run
//! use extensions_meta_raft::{RaftElectionClient, RaftElectionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure Raft election
//! let config = RaftElectionConfig {
//!     node_id: "meta-node-1".to_string(),
//!     raft_node_id: 1,
//!     peer_node_ids: vec![2, 3],
//!     ..Default::default()
//! };
//!
//! // Create election client
//! let election = RaftElectionClient::new(config).await?;
//!
//! // Use with RisingWave Meta
//! // let meta = MetaService::new(election).await?;
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod network;
mod sqlite_storage;
mod storage;

pub use client::{ElectionMember, RaftElectionClient, RaftElectionConfig};
pub use error::{Error, Result};
pub use network::{RaftNetwork, RaftNetworkConfig};
pub use sqlite_storage::{LogEntry, SqliteStorage, SqliteStorageConfig};
pub use storage::{RaftStorage, RaftStorageConfig};
