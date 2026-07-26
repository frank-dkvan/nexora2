//! Raft consensus implementation using openraft.
//!
//! This is a simplified implementation for Phase 2. Full multi-node
//! networking will be added in Phase 4.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::info;

use crate::client::ConsensusClient;
use crate::error::{ConsensusError, Result};
use crate::types::{LogIndex, NodeId};

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Unique identifier for this node.
    pub node_id: NodeId,

    /// Address this node listens on for Raft communication.
    pub listen_addr: SocketAddr,

    /// Peer nodes in the cluster (node_id, address).
    pub peers: Vec<(NodeId, SocketAddr)>,

    /// Heartbeat interval in milliseconds (default: 500ms).
    pub heartbeat_interval: Option<u64>,

    /// Election timeout in milliseconds (default: 1500ms).
    pub election_timeout_min: Option<u64>,

    /// Maximum election timeout in milliseconds (default: 3000ms).
    pub election_timeout_max: Option<u64>,
}

impl RaftConfig {
    /// Create a new Raft configuration.
    pub fn new(node_id: NodeId, listen_addr: SocketAddr) -> Self {
        Self {
            node_id,
            listen_addr,
            peers: Vec::new(),
            heartbeat_interval: None,
            election_timeout_min: None,
            election_timeout_max: None,
        }
    }

    /// Add a peer to the cluster.
    pub fn add_peer(mut self, node_id: NodeId, addr: SocketAddr) -> Self {
        self.peers.push((node_id, addr));
        self
    }

    /// Set heartbeat interval (milliseconds).
    pub fn heartbeat_interval(mut self, ms: u64) -> Self {
        self.heartbeat_interval = Some(ms);
        self
    }

    /// Set election timeout range (milliseconds).
    pub fn election_timeout(mut self, min: u64, max: u64) -> Self {
        self.election_timeout_min = Some(min);
        self.election_timeout_max = Some(max);
        self
    }
}

/// Raft-based consensus client implementation.
///
/// This is a simplified version for Phase 2 that provides the basic
/// ConsensusClient interface. Full Raft networking will be implemented
/// in Phase 4 when we integrate with RisingWave.
pub struct RaftConsensusClient {
    node_id: NodeId,
    state: Arc<RwLock<RaftState>>,
    #[allow(dead_code)]
    config: RaftConfig,
}

#[derive(Debug)]
struct RaftState {
    is_leader: bool,
    current_leader: Option<NodeId>,
    log_index: LogIndex,
    log: Vec<LogEntry>,
}

#[derive(Debug, Clone)]
struct LogEntry {
    #[allow(dead_code)]
    index: LogIndex,
    #[allow(dead_code)]
    data: Bytes,
}

impl RaftConsensusClient {
    /// Create a new Raft consensus client.
    ///
    /// For Phase 2, this creates a single-node cluster that is always the leader.
    /// Multi-node support will be added in Phase 4.
    pub async fn new(config: RaftConfig) -> Result<Self> {
        info!(
            "Initializing simplified Raft node {} at {}",
            config.node_id, config.listen_addr
        );

        let state = RaftState {
            is_leader: true, // Single node is always leader
            current_leader: Some(config.node_id),
            log_index: 0,
            log: Vec::new(),
        };

        Ok(Self {
            node_id: config.node_id,
            state: Arc::new(RwLock::new(state)),
            config,
        })
    }

    /// Get configuration (for testing).
    #[cfg(test)]
    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    /// Get current log length (for testing).
    #[cfg(test)]
    pub async fn log_len(&self) -> usize {
        self.state.read().await.log.len()
    }
}

#[async_trait]
impl ConsensusClient for RaftConsensusClient {
    async fn is_leader(&self) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.is_leader)
    }

    async fn current_leader(&self) -> Result<Option<NodeId>> {
        let state = self.state.read().await;
        Ok(state.current_leader)
    }

    async fn commit(&self, data: Bytes) -> Result<LogIndex> {
        let mut state = self.state.write().await;

        if !state.is_leader {
            return Err(ConsensusError::NotLeader {
                leader_id: state.current_leader,
            });
        }

        // Increment log index
        state.log_index += 1;
        let index = state.log_index;

        // Append to log
        state.log.push(LogEntry {
            index,
            data: data.clone(),
        });

        info!("Committed {} bytes at log index {}", data.len(), index);
        Ok(index)
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Raft node {} shutting down", self.node_id);
        // In Phase 2, just clear the state
        let mut state = self.state.write().await;
        state.is_leader = false;
        state.current_leader = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_single_node_leadership() {
        let config = RaftConfig::new(1, "127.0.0.1:5690".parse().unwrap());
        let client = RaftConsensusClient::new(config).await.unwrap();

        assert!(client.is_leader().await.unwrap());
        assert_eq!(client.current_leader().await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn test_commit_data() {
        let config = RaftConfig::new(1, "127.0.0.1:5691".parse().unwrap());
        let client = RaftConsensusClient::new(config).await.unwrap();

        let data1 = Bytes::from("test data 1");
        let index1 = client.commit(data1).await.unwrap();
        assert_eq!(index1, 1);

        let data2 = Bytes::from("test data 2");
        let index2 = client.commit(data2).await.unwrap();
        assert_eq!(index2, 2);

        assert_eq!(client.log_len().await, 2);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let config = RaftConfig::new(1, "127.0.0.1:5692".parse().unwrap());
        let client = RaftConsensusClient::new(config).await.unwrap();

        assert!(client.is_leader().await.unwrap());

        client.shutdown().await.unwrap();

        assert!(!client.is_leader().await.unwrap());
        assert_eq!(client.current_leader().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_node_id() {
        let config = RaftConfig::new(42, "127.0.0.1:5693".parse().unwrap());
        let client = RaftConsensusClient::new(config).await.unwrap();

        assert_eq!(client.node_id(), 42);
    }
}
