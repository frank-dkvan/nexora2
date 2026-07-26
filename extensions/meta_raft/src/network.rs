//! Raft network layer for RisingWave Meta election.
//!
//! This module provides network communication for Raft nodes using nexora-rpc.

use crate::{Error, Result};
use bytes::Bytes;
use nexora_rpc::{RpcClient, RpcServer, TonicRpcClient, TonicRpcServer};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Network configuration for Raft.
#[derive(Debug, Clone)]
pub struct RaftNetworkConfig {
    /// Local node address
    pub local_addr: SocketAddr,

    /// Peer node addresses
    pub peer_addrs: Vec<SocketAddr>,

    /// RPC timeout in milliseconds
    pub rpc_timeout_ms: u64,
}

impl Default for RaftNetworkConfig {
    fn default() -> Self {
        Self {
            local_addr: "127.0.0.1:5690".parse().unwrap(),
            peer_addrs: vec![],
            rpc_timeout_ms: 5_000,
        }
    }
}

/// Raft network layer for Meta nodes.
///
/// Phase 4: Simplified implementation using nexora-rpc.
/// Future: Add connection pooling, retry logic, and monitoring.
///
/// # Example
///
/// ```rust,no_run
/// use extensions_meta_raft::{RaftNetwork, RaftNetworkConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RaftNetworkConfig::default();
/// let network = RaftNetwork::new(config).await?;
/// network.start().await?;
/// # Ok(())
/// # }
/// ```
pub struct RaftNetwork {
    config: RaftNetworkConfig,
    server: Arc<TonicRpcServer>,
    clients: Arc<RwLock<Vec<TonicRpcClient>>>,
}

impl RaftNetwork {
    /// Create a new Raft network layer.
    ///
    /// # Arguments
    ///
    /// - `config`: Network configuration
    ///
    /// # Errors
    ///
    /// Returns an error if network initialization fails.
    pub async fn new(config: RaftNetworkConfig) -> Result<Self> {
        info!("Initializing Raft network at {}", config.local_addr);

        // Create RPC server
        let server = Arc::new(TonicRpcServer::new(config.local_addr));

        // Create RPC clients for peers
        let clients = Arc::new(RwLock::new(Vec::new()));

        Ok(Self {
            config,
            server,
            clients,
        })
    }

    /// Start the network layer.
    ///
    /// This starts the RPC server and connects to peer nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if server startup or peer connection fails.
    pub async fn start(&self) -> Result<()> {
        info!("Starting Raft network");

        // Start RPC server
        self.server.start().await?;

        // Connect to peers
        let mut clients = self.clients.write().await;
        for peer_addr in &self.config.peer_addrs {
            debug!("Connecting to peer: {}", peer_addr);
            let client = TonicRpcClient::connect(format!("http://{}", peer_addr)).await?;
            clients.push(client);
        }

        info!("Raft network started with {} peers", clients.len());

        Ok(())
    }

    /// Stop the network layer.
    ///
    /// This stops the RPC server and closes peer connections.
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping Raft network");

        // Close peer connections
        let mut clients = self.clients.write().await;
        for client in clients.iter() {
            if let Err(e) = client.close().await {
                debug!("Error closing client connection: {}", e);
            }
        }
        clients.clear();

        // Stop RPC server
        self.server.stop().await?;

        info!("Raft network stopped");

        Ok(())
    }

    /// Send a message to a peer node.
    ///
    /// Phase 4: Simplified implementation.
    /// Future: Add retry logic, connection pooling, and timeout handling.
    ///
    /// # Arguments
    ///
    /// - `peer_index`: Index of the peer node
    /// - `message`: Message bytes to send
    ///
    /// # Returns
    ///
    /// Response bytes from the peer.
    pub async fn send_to_peer(&self, peer_index: usize, message: &[u8]) -> Result<Vec<u8>> {
        debug!("Sending message to peer {}", peer_index);

        let clients = self.clients.read().await;
        let client = clients
            .get(peer_index)
            .ok_or_else(|| Error::network(format!("Peer {} not found", peer_index)))?;

        let response = client
            .call("raft.message", Bytes::copy_from_slice(message))
            .await?;

        Ok(response.to_vec())
    }

    /// Broadcast a message to all peers.
    ///
    /// Phase 4: Sequential broadcast.
    /// Future: Implement parallel broadcast with timeout.
    ///
    /// # Arguments
    ///
    /// - `message`: Message bytes to broadcast
    ///
    /// # Returns
    ///
    /// Vector of responses from all peers (may contain errors).
    pub async fn broadcast(&self, message: &[u8]) -> Vec<Result<Vec<u8>>> {
        debug!("Broadcasting message to all peers");

        let clients = self.clients.read().await;
        let mut responses = Vec::new();

        for client in clients.iter() {
            let result = match client
                .call("raft.message", Bytes::copy_from_slice(message))
                .await
            {
                Ok(response) => Ok(response.to_vec()),
                Err(e) => Err(Error::from(e)),
            };
            responses.push(result);
        }

        responses
    }

    /// Get the number of connected peers.
    pub async fn peer_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Check if the network is running.
    pub fn is_running(&self) -> bool {
        self.server.is_running()
    }

    /// Get local address.
    pub fn local_addr(&self) -> SocketAddr {
        self.config.local_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_lifecycle() {
        let config = RaftNetworkConfig {
            local_addr: "127.0.0.1:15690".parse().unwrap(),
            peer_addrs: vec![],
            rpc_timeout_ms: 5_000,
        };

        let network = RaftNetwork::new(config).await.unwrap();

        assert!(!network.is_running());
        assert_eq!(network.peer_count().await, 0);

        network.start().await.unwrap();
        assert!(network.is_running());

        network.stop().await.unwrap();
        assert!(!network.is_running());
    }

    #[tokio::test]
    async fn test_network_with_peers() {
        let config = RaftNetworkConfig {
            local_addr: "127.0.0.1:15691".parse().unwrap(),
            peer_addrs: vec![
                "127.0.0.1:15692".parse().unwrap(),
                "127.0.0.1:15693".parse().unwrap(),
            ],
            rpc_timeout_ms: 5_000,
        };

        let network = RaftNetwork::new(config).await.unwrap();

        // Note: start() will fail to connect to non-existent peers in this test
        // In real scenarios, peers would be running
        assert_eq!(network.peer_count().await, 0);
    }
}
