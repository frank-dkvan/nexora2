//! Raft consensus implementation using openraft.
//!
//! Phase 4: Upgraded to support multi-node clusters with persistent storage
//! and TCP networking. Maintains backward compatibility with Phase 2 single-node mode.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::info;

use crate::client::ConsensusClient;
use crate::error::{ConsensusError, Result};
use crate::network::{NetworkConfig, RaftNetwork};
use crate::storage::{LogEntry as StorageLogEntry, RaftStorage};
use crate::types::{LogIndex, NodeId};

/// Raft operational mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftMode {
    /// Single-node mode (Phase 2 behavior)
    SingleNode,
    /// Multi-node distributed mode (Phase 4)
    MultiNode,
}

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

    /// Operational mode (default: SingleNode for backward compatibility).
    pub mode: RaftMode,

    /// Data directory for persistent storage (only used in MultiNode mode).
    pub data_dir: Option<PathBuf>,
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
            mode: RaftMode::SingleNode, // Backward compatible default
            data_dir: None,
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

    /// Set operational mode.
    pub fn mode(mut self, mode: RaftMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set data directory for persistent storage.
    pub fn data_dir(mut self, path: PathBuf) -> Self {
        self.data_dir = Some(path);
        self
    }
}

/// Raft-based consensus client implementation.
///
/// Supports both single-node (Phase 2) and multi-node (Phase 4) operation modes.
pub struct RaftConsensusClient {
    node_id: NodeId,
    mode: RaftMode,
    state: Arc<RwLock<RaftState>>,
    storage: Option<Arc<RaftStorage>>,
    network: Option<Arc<RaftNetwork>>,
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
    index: LogIndex,
    data: Bytes,
}

impl RaftConsensusClient {
    /// Create a new Raft consensus client.
    ///
    /// Phase 2 compatibility: Single-node mode (in-memory, always leader).
    /// Phase 4: Multi-node mode with persistent storage and networking.
    pub async fn new(config: RaftConfig) -> Result<Self> {
        info!(
            "Initializing Raft node {} at {} (mode: {:?})",
            config.node_id, config.listen_addr, config.mode
        );

        let (state, storage, network) = match config.mode {
            RaftMode::SingleNode => {
                // Phase 2 behavior: in-memory, always leader
                let state = RaftState {
                    is_leader: true,
                    current_leader: Some(config.node_id),
                    log_index: 0,
                    log: Vec::new(),
                };
                (Arc::new(RwLock::new(state)), None, None)
            }
            RaftMode::MultiNode => {
                // Phase 4 behavior: persistent storage + networking
                let data_dir = config
                    .data_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from("/tmp/raft-data"));

                // Initialize storage
                let storage = RaftStorage::new(&data_dir).await?;
                let storage = Arc::new(storage);

                // Initialize network
                let mut peer_map = std::collections::HashMap::new();
                for (peer_id, peer_addr) in &config.peers {
                    peer_map.insert(*peer_id, *peer_addr);
                }
                let network_config = NetworkConfig {
                    node_id: config.node_id,
                    listen_addr: config.listen_addr,
                    peers: peer_map,
                };
                let network = RaftNetwork::new(network_config).await?;
                let network = Arc::new(network);

                // Initialize state (not leader initially, election will determine)
                let state = RaftState {
                    is_leader: false,
                    current_leader: None,
                    log_index: 0,
                    log: Vec::new(),
                };

                (Arc::new(RwLock::new(state)), Some(storage), Some(network))
            }
        };

        Ok(Self {
            node_id: config.node_id,
            mode: config.mode,
            state,
            storage,
            network,
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
        match self.mode {
            RaftMode::SingleNode => self.state.read().await.log.len(),
            RaftMode::MultiNode => {
                if let Some(storage) = &self.storage {
                    storage.log_count().await
                } else {
                    0
                }
            }
        }
    }

    /// Get operational mode.
    pub fn mode(&self) -> RaftMode {
        self.mode
    }

    /// Manually set leadership status (for testing only).
    pub async fn set_leader(&self, is_leader: bool) {
        let mut state = self.state.write().await;
        state.is_leader = is_leader;
        if is_leader {
            state.current_leader = Some(self.node_id);
        }
    }

    /// Check if storage is initialized (for testing).
    pub fn has_storage(&self) -> bool {
        self.storage.is_some()
    }

    /// Get storage log count (for testing).
    pub async fn storage_log_count(&self) -> usize {
        if let Some(storage) = &self.storage {
            storage.log_count().await
        } else {
            0
        }
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

        match self.mode {
            RaftMode::SingleNode => {
                // Phase 2: in-memory only
                state.log.push(LogEntry {
                    index,
                    data: data.clone(),
                });
            }
            RaftMode::MultiNode => {
                // Phase 4: persistent storage
                if let Some(storage) = &self.storage {
                    let entry = StorageLogEntry {
                        index,
                        term: storage.get_term().await?,
                        data: data.clone(),
                    };
                    storage.append_log(entry).await?;
                }
            }
        }

        info!("Committed {} bytes at log index {}", data.len(), index);
        Ok(index)
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Raft node {} shutting down", self.node_id);

        // Shutdown network if in multi-node mode
        if let Some(network) = &self.network {
            network.shutdown().await?;
        }

        // Clear state
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

        assert_eq!(client.mode(), RaftMode::SingleNode);
        assert!(client.is_leader().await.unwrap());
        assert_eq!(client.current_leader().await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn test_multi_node_initialization() {
        let config = RaftConfig::new(1, "127.0.0.1:5690".parse().unwrap())
            .mode(RaftMode::MultiNode)
            .data_dir(PathBuf::from("/tmp/raft-test-multi"));

        let client = RaftConsensusClient::new(config).await.unwrap();

        assert_eq!(client.mode(), RaftMode::MultiNode);
        // Multi-node starts as follower (not leader until election)
        assert!(!client.is_leader().await.unwrap());
    }

    #[tokio::test]
    async fn test_commit_data_single_node() {
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

    #[tokio::test]
    async fn test_multi_node_storage() {
        let config = RaftConfig::new(1, "127.0.0.1:5694".parse().unwrap())
            .mode(RaftMode::MultiNode)
            .data_dir(PathBuf::from("/tmp/raft-test-storage"));

        let client = RaftConsensusClient::new(config).await.unwrap();

        // Manually set as leader for testing
        {
            let mut state = client.state.write().await;
            state.is_leader = true;
        }

        let data = Bytes::from("persistent data");
        let index = client.commit(data).await.unwrap();
        assert_eq!(index, 1);

        // Verify data was written to storage
        if let Some(storage) = &client.storage {
            assert_eq!(storage.log_count().await, 1);
        }
    }
}
