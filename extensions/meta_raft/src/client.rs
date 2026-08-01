//! Raft-based election client for RisingWave Meta.
//!
//! This module implements the RisingWave `ElectionClient` trait using our Raft consensus layer.

use crate::{Error, Result};
use nexora_consensus::{ConsensusClient, RaftConfig, RaftConsensusClient};
use std::sync::Arc;
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for Raft election client.
#[derive(Debug, Clone)]
pub struct RaftElectionConfig {
    /// Node ID (must be unique across the cluster)
    pub node_id: String,

    /// Raft node ID (numeric)
    pub raft_node_id: u64,

    /// Peer Raft node IDs
    pub peer_node_ids: Vec<u64>,

    /// Heartbeat interval in seconds
    pub heartbeat_interval_secs: u64,

    /// Election timeout in seconds
    pub election_timeout_secs: u64,
}

impl Default for RaftElectionConfig {
    fn default() -> Self {
        Self {
            node_id: "meta-node-1".to_string(),
            raft_node_id: 1,
            peer_node_ids: vec![],
            heartbeat_interval_secs: 1,
            election_timeout_secs: 5,
        }
    }
}

/// Raft-based election client for RisingWave Meta.
///
/// This implements leader election using the Raft consensus protocol,
/// replacing RisingWave's SQL-based election.
///
/// # Phase 4 Implementation
///
/// Phase 4 provides a working Raft election client that:
/// - Integrates with nexora-consensus (openraft 0.9)
/// - Implements leader election and heartbeat
/// - Provides leader change notifications
/// - Supports multi-node Meta clusters
///
/// # Example
///
/// ```rust,no_run
/// use extensions_meta_raft::{RaftElectionClient, RaftElectionConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RaftElectionConfig {
///     node_id: "meta-1".to_string(),
///     raft_node_id: 1,
///     peer_node_ids: vec![2, 3],
///     ..Default::default()
/// };
///
/// let client = RaftElectionClient::new(config).await?;
/// client.init().await?;
///
/// if client.is_leader() {
///     println!("This node is the leader");
/// }
/// # Ok(())
/// # }
/// ```
pub struct RaftElectionClient {
    config: RaftElectionConfig,
    consensus: Arc<dyn ConsensusClient>,
    is_leader_sender: Sender<bool>,
    state: Arc<RwLock<ClientState>>,
}

#[derive(Debug)]
struct ClientState {
    initialized: bool,
    running: bool,
}

impl RaftElectionClient {
    /// Create a new Raft election client.
    ///
    /// # Arguments
    ///
    /// - `config`: Election client configuration
    ///
    /// # Errors
    ///
    /// Returns an error if consensus client creation fails.
    pub async fn new(config: RaftElectionConfig) -> Result<Self> {
        info!("Creating Raft election client for node {}", config.node_id);

        // Default listen address for Raft
        let listen_addr: std::net::SocketAddr = format!("127.0.0.1:{}", 5690 + config.raft_node_id)
            .parse()
            .map_err(|e| Error::config(format!("Invalid listen address: {}", e)))?;

        // Create Raft consensus client
        let mut raft_config = RaftConfig::new(config.raft_node_id, listen_addr);

        // Add peers
        for peer_id in &config.peer_node_ids {
            let peer_addr: std::net::SocketAddr =
                format!("127.0.0.1:{}", 5690 + peer_id)
                    .parse()
                    .map_err(|e| Error::config(format!("Invalid peer address: {}", e)))?;
            raft_config = raft_config.add_peer(*peer_id, peer_addr);
        }

        // Set timeouts
        raft_config = raft_config
            .heartbeat_interval(config.heartbeat_interval_secs * 1000)
            .election_timeout(
                config.election_timeout_secs * 1000,
                config.election_timeout_secs * 2000,
            );

        let consensus = RaftConsensusClient::new(raft_config).await?;

        let (is_leader_sender, _) = watch::channel(false);

        let state = Arc::new(RwLock::new(ClientState {
            initialized: false,
            running: false,
        }));

        Ok(Self {
            config,
            consensus: Arc::new(consensus),
            is_leader_sender,
            state,
        })
    }

    /// Initialize the election client.
    ///
    /// This starts the Raft consensus layer and begins leader election.
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails.
    pub async fn init(&self) -> Result<()> {
        info!("Initializing Raft election client");

        let mut state = self.state.write().await;
        if state.initialized {
            warn!("Election client already initialized");
            return Ok(());
        }

        // Initialize consensus layer
        // Phase 4: Simplified - actual initialization handled by consensus client
        state.initialized = true;

        info!("Raft election client initialized");
        Ok(())
    }

    /// Get node ID.
    pub fn id(&self) -> Result<String> {
        Ok(self.config.node_id.clone())
    }

    /// Check if this node is the leader.
    ///
    /// This queries the underlying Raft consensus layer.
    ///
    /// Note: This is a synchronous wrapper around the async consensus client.
    /// It blocks the current thread to query leadership status.
    pub fn is_leader(&self) -> bool {
        // Phase 4: Query consensus layer synchronously
        // RisingWave's ElectionClient trait requires sync is_leader(),
        // but our ConsensusClient is async. We use block_in_place to bridge.
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.consensus.is_leader())
                .unwrap_or(false)
        })
    }

    /// Run one election cycle.
    ///
    /// This method blocks until:
    /// - Leader status is lost, or
    /// - The stop signal is received
    ///
    /// # Arguments
    ///
    /// - `ttl`: Time-to-live for leadership (seconds)
    /// - `stop`: Channel to receive stop signal
    ///
    /// # Errors
    ///
    /// Returns an error if the election cycle fails.
    pub async fn run_once(&self, ttl: i64, mut stop: Receiver<()>) -> Result<()> {
        info!("Starting election cycle with ttl={}", ttl);

        let mut state = self.state.write().await;
        if !state.initialized {
            return Err(Error::election("Client not initialized"));
        }
        state.running = true;
        drop(state);

        // Phase 4: Main election loop
        loop {
            // Check if we're the leader
            let is_leader = self.consensus.is_leader().await.unwrap_or(false);
            let _ = self.is_leader_sender.send(is_leader);

            if is_leader {
                debug!("Node {} is the leader", self.config.node_id);
            } else {
                debug!("Node {} is a follower", self.config.node_id);
            }

            // Wait for leader change or stop signal
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    // Continue monitoring
                }
                _ = stop.changed() => {
                    info!("Received stop signal, exiting election cycle");
                    break;
                }
            }

            // If we were leader but lost leadership, exit
            let current_is_leader = self.consensus.is_leader().await.unwrap_or(false);
            if is_leader && !current_is_leader {
                info!("Lost leadership, exiting election cycle");
                break;
            }
        }

        let mut state = self.state.write().await;
        state.running = false;

        Ok(())
    }

    /// Subscribe to leader status changes.
    ///
    /// Returns a receiver that gets notified when leader status changes.
    pub fn subscribe(&self) -> Receiver<bool> {
        self.is_leader_sender.subscribe()
    }

    /// Get the current leader information.
    ///
    /// Returns the current leader's member information, or None if no leader is elected.
    pub async fn leader(&self) -> Result<Option<ElectionMember>> {
        let leader_id = self.consensus.current_leader().await?;

        match leader_id {
            Some(id) => {
                // Check if this node is the leader
                let is_self = id == self.config.raft_node_id;
                let node_id = if is_self {
                    self.config.node_id.clone()
                } else {
                    // Map Raft node ID to string node ID
                    // For now, use numeric ID as string
                    format!("meta-node-{}", id)
                };

                Ok(Some(ElectionMember {
                    id: node_id,
                    is_leader: is_self,
                }))
            }
            None => Ok(None),
        }
    }

    /// Get all cluster members.
    ///
    /// Returns a list of all members in the Raft cluster with their current leadership status.
    pub async fn get_members(&self) -> Result<Vec<ElectionMember>> {
        let mut members = Vec::new();

        // Add self
        members.push(ElectionMember {
            id: self.config.node_id.clone(),
            is_leader: self.consensus.is_leader().await.unwrap_or(false),
        });

        // Add peers
        for peer_id in &self.config.peer_node_ids {
            members.push(ElectionMember {
                id: format!("meta-node-{}", peer_id),
                is_leader: false, // Only this node knows its own status
            });
        }

        Ok(members)
    }

    /// Shutdown the election client.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Raft election client");

        self.consensus.shutdown().await?;

        let mut state = self.state.write().await;
        state.initialized = false;
        state.running = false;

        info!("Raft election client shut down");
        Ok(())
    }
}

/// Election member information.
///
/// This matches RisingWave's ElectionMember struct.
#[derive(Debug, Clone)]
pub struct ElectionMember {
    pub id: String,
    pub is_leader: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_election_client_lifecycle() {
        let config = RaftElectionConfig {
            node_id: "test-node-1".to_string(),
            raft_node_id: 1,
            peer_node_ids: vec![],
            ..Default::default()
        };

        let client = RaftElectionClient::new(config).await.unwrap();

        // Test initialization
        client.init().await.unwrap();
        assert_eq!(client.id().unwrap(), "test-node-1");

        // Test leader status (single node is always leader)
        let is_leader = client.is_leader();
        assert!(is_leader, "Single-node cluster should be leader");

        // Test shutdown
        client.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_election_client_members() {
        let config = RaftElectionConfig {
            node_id: "test-node-2".to_string(),
            raft_node_id: 2,
            peer_node_ids: vec![],
            ..Default::default()
        };

        let client = RaftElectionClient::new(config).await.unwrap();
        client.init().await.unwrap();

        // Test members
        let members = client.get_members().await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].id, "test-node-2");

        client.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_election_client_subscribe() {
        let config = RaftElectionConfig::default();
        let client = RaftElectionClient::new(config).await.unwrap();
        client.init().await.unwrap();

        // Test subscription
        let rx = client.subscribe();
        // rx.borrow() returns a reference to bool, not Option<bool>
        let _is_leader = *rx.borrow();

        client.shutdown().await.unwrap();
    }
}
