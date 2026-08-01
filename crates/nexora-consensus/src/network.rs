//! TCP-based network layer for Raft communication.
//!
//! This module provides the network transport for Raft nodes to communicate
//! with each other, using nexora-rpc for RPC calls.

use crate::error::{ConsensusError, Result};
use crate::types::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Network configuration for Raft.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// This node's ID
    pub node_id: NodeId,
    /// This node's listen address
    pub listen_addr: SocketAddr,
    /// Peer addresses (node_id -> address)
    pub peers: HashMap<NodeId, SocketAddr>,
}

/// Raft network layer.
///
/// Phase 4: Simplified implementation. Full RPC integration with nexora-rpc
/// will be added when we implement actual message passing.
pub struct RaftNetwork {
    config: NetworkConfig,
    /// Connected peers (Phase 4: will use actual RPC clients)
    peers: Arc<RwLock<HashMap<NodeId, SocketAddr>>>,
}

impl RaftNetwork {
    /// Create a new Raft network.
    pub async fn new(config: NetworkConfig) -> Result<Self> {
        debug!(
            "Creating Raft network for node {} at {}",
            config.node_id, config.listen_addr
        );

        let peers = Arc::new(RwLock::new(config.peers.clone()));

        Ok(Self { config, peers })
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Get this node's listen address.
    pub fn listen_addr(&self) -> SocketAddr {
        self.config.listen_addr
    }

    /// Add a peer to the network.
    pub async fn add_peer(&self, node_id: NodeId, addr: SocketAddr) -> Result<()> {
        debug!("Adding peer {} at {}", node_id, addr);
        let mut peers = self.peers.write().await;
        peers.insert(node_id, addr);
        Ok(())
    }

    /// Remove a peer from the network.
    pub async fn remove_peer(&self, node_id: NodeId) -> Result<()> {
        debug!("Removing peer {}", node_id);
        let mut peers = self.peers.write().await;
        peers.remove(&node_id);
        Ok(())
    }

    /// Get all peer IDs.
    pub async fn peer_ids(&self) -> Vec<NodeId> {
        let peers = self.peers.read().await;
        peers.keys().copied().collect()
    }

    /// Get peer address by node ID.
    pub async fn peer_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        let peers = self.peers.read().await;
        peers.get(&node_id).copied()
    }

    /// Send a message to a peer.
    ///
    /// Phase 4: Placeholder implementation. Full RPC implementation will use
    /// nexora-rpc to send actual Raft messages (AppendEntries, RequestVote).
    pub async fn send_message(&self, target: NodeId, _message: &[u8]) -> Result<()> {
        let addr = self.peer_addr(target).await;
        if addr.is_none() {
            warn!("Peer {} not found in network", target);
            return Err(ConsensusError::Network(format!(
                "Peer {} not found",
                target
            )));
        }

        // Phase 4: Actual RPC call will be implemented here
        debug!("Would send message to peer {} at {:?}", target, addr);
        Ok(())
    }

    /// Broadcast a message to all peers.
    pub async fn broadcast(&self, message: &[u8]) -> Result<()> {
        let peer_ids = self.peer_ids().await;
        for peer_id in peer_ids {
            if let Err(e) = self.send_message(peer_id, message).await {
                warn!("Failed to send to peer {}: {}", peer_id, e);
            }
        }
        Ok(())
    }

    /// Shutdown the network.
    pub async fn shutdown(&self) -> Result<()> {
        debug!("Shutting down Raft network");
        let mut peers = self.peers.write().await;
        peers.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_lifecycle() {
        let config = NetworkConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:5690".parse().unwrap(),
            peers: HashMap::new(),
        };

        let network = RaftNetwork::new(config).await.unwrap();
        assert_eq!(network.node_id(), 1);
        assert_eq!(network.listen_addr(), "127.0.0.1:5690".parse().unwrap());

        network.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_peer_management() {
        let config = NetworkConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:5690".parse().unwrap(),
            peers: HashMap::new(),
        };

        let network = RaftNetwork::new(config).await.unwrap();

        // Add peers
        network
            .add_peer(2, "127.0.0.1:5691".parse().unwrap())
            .await
            .unwrap();
        network
            .add_peer(3, "127.0.0.1:5692".parse().unwrap())
            .await
            .unwrap();

        // Check peers
        let peer_ids = network.peer_ids().await;
        assert_eq!(peer_ids.len(), 2);
        assert!(peer_ids.contains(&2));
        assert!(peer_ids.contains(&3));

        // Get peer address
        let addr = network.peer_addr(2).await.unwrap();
        assert_eq!(addr, "127.0.0.1:5691".parse().unwrap());

        // Remove peer
        network.remove_peer(2).await.unwrap();
        let peer_ids = network.peer_ids().await;
        assert_eq!(peer_ids.len(), 1);
        assert!(!peer_ids.contains(&2));
    }

    #[tokio::test]
    async fn test_send_message() {
        let mut peers = HashMap::new();
        peers.insert(2, "127.0.0.1:5691".parse().unwrap());

        let config = NetworkConfig {
            node_id: 1,
            listen_addr: "127.0.0.1:5690".parse().unwrap(),
            peers,
        };

        let network = RaftNetwork::new(config).await.unwrap();

        // Send to existing peer (should succeed)
        let result = network.send_message(2, b"test message").await;
        assert!(result.is_ok());

        // Send to non-existent peer (should fail)
        let result = network.send_message(99, b"test message").await;
        assert!(result.is_err());
    }
}
