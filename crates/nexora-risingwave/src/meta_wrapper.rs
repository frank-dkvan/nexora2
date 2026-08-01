//! RisingWave Meta node wrapper.
//!
//! Phase 3: Simplified implementation with placeholder logic.
//! Phase 4: Full integration with Raft-based HA election.

use crate::error::{EventStreamingError, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Wrapper around RisingWave Meta node.
///
/// The Meta node is responsible for:
/// - Cluster metadata management
/// - DDL execution coordination
/// - Catalog management
/// - Leader election (in HA mode)
pub struct MetaNode {
    addr: SocketAddr,
    state: Arc<RwLock<MetaState>>,
    election_client: Option<Arc<dyn ElectionClientTrait>>,
}

#[derive(Debug)]
struct MetaState {
    running: bool,
    is_leader: bool,
}

/// Trait for election clients to enable HA mode.
///
/// This trait abstracts the election mechanism, allowing different
/// implementations (Raft, etcd, SQL-based, etc.).
#[async_trait::async_trait]
pub trait ElectionClientTrait: Send + Sync {
    /// Initialize the election client.
    async fn init(&self) -> Result<()>;

    /// Check if this node is the leader.
    fn is_leader(&self) -> bool;

    /// Get the node ID.
    fn id(&self) -> Result<String>;

    /// Shutdown the election client.
    async fn shutdown(&self) -> Result<()>;
}

impl MetaNode {
    /// Create a new Meta node wrapper in single-node mode.
    ///
    /// # Arguments
    ///
    /// - `addr`: The address to bind the Meta node to
    ///
    /// # Example
    ///
    /// ```rust
    /// use nexora_risingwave::meta_wrapper::MetaNode;
    ///
    /// let meta = MetaNode::new("127.0.0.1:5690".parse().unwrap());
    /// ```
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: Arc::new(RwLock::new(MetaState {
                running: false,
                is_leader: true, // Single node is always leader
            })),
            election_client: None,
        }
    }

    /// Create a new Meta node wrapper with HA election support.
    ///
    /// # Arguments
    ///
    /// - `addr`: The address to bind the Meta node to
    /// - `election_client`: Election client for leader election
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use nexora_risingwave::meta_wrapper::MetaNode;
    /// use extensions_meta_raft::{RaftElectionClient, RaftElectionConfig};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let election_config = RaftElectionConfig {
    ///     node_id: "meta-1".to_string(),
    ///     raft_node_id: 1,
    ///     peer_node_ids: vec![2, 3],
    ///     ..Default::default()
    /// };
    ///
    /// let election = Arc::new(RaftElectionClient::new(election_config).await?);
    /// let meta = MetaNode::with_election(
    ///     "127.0.0.1:5690".parse()?,
    ///     election
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_election(addr: SocketAddr, election_client: Arc<dyn ElectionClientTrait>) -> Self {
        Self {
            addr,
            state: Arc::new(RwLock::new(MetaState {
                running: false,
                is_leader: false, // HA mode: leader determined by election
            })),
            election_client: Some(election_client),
        }
    }

    /// Start the Meta node.
    ///
    /// In single-node mode, this marks the node as running and leader.
    /// In HA mode, this initializes the election client and starts leader election.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the Meta node started successfully
    /// - `Err(_)` if startup failed
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.running {
            return Err(EventStreamingError::MetaStartFailed(
                "meta node already running".to_string(),
            ));
        }

        info!("Starting RisingWave Meta node on {}", self.addr);

        // Initialize election client if in HA mode
        if let Some(election) = &self.election_client {
            info!("Initializing Raft election for HA mode");
            election.init().await?;

            // Update leader status from election
            state.is_leader = election.is_leader();
            info!("Meta node leader status: {}", state.is_leader);
        }

        // Phase 3: Placeholder
        // Phase 4: Will start actual Meta node:
        //   - Initialize RisingWave MetaService
        //   - Start gRPC server
        //   - Initialize catalog
        //   - Election client already initialized above

        state.running = true;
        Ok(())
    }

    /// Stop the Meta node gracefully.
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if !state.running {
            return Ok(());
        }

        info!("Stopping RisingWave Meta node");

        // Shutdown election client if in HA mode
        if let Some(election) = &self.election_client {
            info!("Shutting down Raft election");
            election.shutdown().await?;
        }

        // Phase 3: Placeholder
        // Phase 4: Will stop actual Meta node

        state.running = false;
        Ok(())
    }

    /// Check if this Meta node is the leader.
    ///
    /// In single-node mode, always returns true.
    /// In HA mode, returns true only if this node won the election.
    pub async fn is_leader(&self) -> bool {
        if let Some(election) = &self.election_client {
            // Query election client for current leader status
            election.is_leader()
        } else {
            // Single-node mode: always leader
            self.state.read().await.is_leader
        }
    }

    /// Get the Meta node address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Check if the Meta node is running.
    pub async fn is_running(&self) -> bool {
        self.state.read().await.running
    }

    /// Get the election client (if in HA mode).
    pub fn election_client(&self) -> Option<&Arc<dyn ElectionClientTrait>> {
        self.election_client.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_meta_lifecycle() {
        let meta = MetaNode::new("127.0.0.1:15690".parse().unwrap());

        assert!(!meta.is_running().await);
        assert_eq!(meta.addr().port(), 15690);

        meta.start().await.unwrap();
        assert!(meta.is_running().await);
        assert!(meta.is_leader().await);

        meta.stop().await.unwrap();
        assert!(!meta.is_running().await);
    }

    #[tokio::test]
    async fn test_meta_double_start() {
        let meta = MetaNode::new("127.0.0.1:15691".parse().unwrap());

        meta.start().await.unwrap();
        let result = meta.start().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_meta_single_node_always_leader() {
        let meta = MetaNode::new("127.0.0.1:15692".parse().unwrap());
        meta.start().await.unwrap();

        // Single-node mode: always leader
        assert!(meta.is_leader().await);
        assert!(meta.election_client().is_none());

        meta.stop().await.unwrap();
    }
}
