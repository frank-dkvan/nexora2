//! Distributed Meta cluster coordination for library mode.
//!
//! Manages a multi-node Meta cluster with Raft consensus. Each Nexora process
//! runs one Meta node in-process, and they coordinate via Raft for catalog
//! consistency.

use crate::distributed_library_config::DistributedLibraryConfig;
use crate::error::{EventStreamingError, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Distributed Meta cluster state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaClusterState {
    /// Cluster is starting up, waiting for quorum.
    Starting,
    /// This node is a Raft follower.
    Follower,
    /// This node is the Raft leader.
    Leader,
    /// Cluster is shutting down.
    ShuttingDown,
}

/// Raft state for the Meta cluster.
///
/// Placeholder for Day 2 implementation. Will integrate with RisingWave's
/// internal Raft coordination or use openraft for external election.
#[derive(Debug)]
pub struct RaftState {
    current_term: std::sync::atomic::AtomicU64,
    voted_for: RwLock<Option<String>>,
    commit_index: std::sync::atomic::AtomicU64,
    last_applied: std::sync::atomic::AtomicU64,
}

impl RaftState {
    fn new() -> Self {
        Self {
            current_term: std::sync::atomic::AtomicU64::new(1),
            voted_for: RwLock::new(None),
            commit_index: std::sync::atomic::AtomicU64::new(0),
            last_applied: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn current_term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::SeqCst)
    }
}

/// Distributed Meta cluster managing Raft consensus and catalog coordination.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │     DistributedMetaCluster (this)       │
/// ├─────────────────────────────────────────┤
/// │  ┌─────────────────────────────────┐   │
/// │  │   RisingWave Meta Node          │   │
/// │  │   (embedded in-process)         │   │
/// │  └─────────────────────────────────┘   │
/// │  ┌─────────────────────────────────┐   │
/// │  │   Raft State Machine            │   │
/// │  │   (election, log replication)   │   │
/// │  └─────────────────────────────────┘   │
/// │  ┌─────────────────────────────────┐   │
/// │  │   Meta Backend                  │   │
/// │  │   (Etcd / SQLite)               │   │
/// │  └─────────────────────────────────┘   │
/// └─────────────────────────────────────────┘
/// ```
///
/// # Example
///
/// ```rust,no_run
/// use nexora_risingwave::{DistributedMetaCluster, DistributedLibraryConfig};
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
///
/// // Start Meta cluster
/// let meta = DistributedMetaCluster::start(config).await?;
///
/// // Wait for leader election
/// let leader_id = meta.wait_for_leader(Duration::from_secs(10)).await?;
/// println!("Leader elected: {}", leader_id);
///
/// // Check if this node is leader
/// if meta.is_leader().await {
///     println!("This node is the leader");
/// }
///
/// // Shutdown
/// meta.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct DistributedMetaCluster {
    /// Node identifier in the cluster.
    node_id: String,

    /// Raft state machine.
    raft_state: Arc<RaftState>,

    /// Whether this node is currently the Raft leader.
    is_leader: Arc<AtomicBool>,

    /// Current cluster state.
    state: Arc<RwLock<MetaClusterState>>,

    /// Configuration snapshot.
    config: DistributedLibraryConfig,
}

impl DistributedMetaCluster {
    /// Start a distributed Meta cluster node.
    ///
    /// This initializes the Meta node, connects to peers, and participates in
    /// Raft leader election. Blocks until the node joins the cluster (quorum
    /// reached) or times out.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Configuration validation fails
    /// - Cannot reach quorum within startup timeout
    /// - Backend (etcd/SQLite) connection fails
    pub async fn start(config: DistributedLibraryConfig) -> Result<Self> {
        // Validate configuration
        config.validate().map_err(|e| {
            EventStreamingError::Config(format!("Invalid distributed config: {}", e))
        })?;

        tracing::info!(
            node_id = %config.node_id,
            peers = ?config.meta.raft_peers,
            "Starting distributed Meta cluster node"
        );

        let raft_state = Arc::new(RaftState::new());
        let is_leader = Arc::new(AtomicBool::new(false));
        let state = Arc::new(RwLock::new(MetaClusterState::Starting));

        // Day 2: Initialize Raft election client
        let raft_client = Self::init_raft_client(&config, raft_state.clone()).await?;

        let cluster = Self {
            node_id: config.node_id.clone(),
            raft_state,
            is_leader,
            state,
            config,
        };

        // Start Raft election
        raft_client
            .init()
            .await
            .map_err(|e| EventStreamingError::Internal(format!("Raft init failed: {}", e)))?;

        // Transition to Follower state (will become Leader after election)
        *cluster.state.write().await = MetaClusterState::Follower;

        tracing::info!(
            node_id = %cluster.node_id,
            "Meta cluster node started (follower)"
        );

        Ok(cluster)
    }

    /// Initialize Raft election client using extensions-meta-raft.
    ///
    /// Connects this Meta node to the Raft cluster for leader election.
    async fn init_raft_client(
        config: &DistributedLibraryConfig,
        raft_state: Arc<RaftState>,
    ) -> Result<Arc<extensions_meta_raft::RaftElectionClient>> {
        use extensions_meta_raft::{RaftElectionClient, RaftElectionConfig};

        // Parse node ID as numeric Raft ID (use hash or sequential mapping)
        let raft_node_id = Self::node_id_to_raft_id(&config.node_id);

        // Parse peer IDs from "node_id@addr" format
        let peer_node_ids: Vec<u64> = config
            .meta
            .raft_peers
            .iter()
            .filter_map(|peer| {
                peer.split('@')
                    .next()
                    .map(|id| Self::node_id_to_raft_id(id))
            })
            .collect();

        let raft_config = RaftElectionConfig {
            node_id: config.node_id.clone(),
            raft_node_id,
            peer_node_ids,
            heartbeat_interval_secs: config.meta.heartbeat_interval_ms / 1000,
            election_timeout_secs: config.meta.election_timeout_ms / 1000,
        };

        let client = RaftElectionClient::new(raft_config).await.map_err(|e| {
            EventStreamingError::Internal(format!("Raft client creation failed: {}", e))
        })?;

        // Spawn background task to monitor leadership
        let is_leader = Arc::new(AtomicBool::new(false));
        let is_leader_clone = is_leader.clone();
        let raft_state_clone = raft_state.clone();
        let mut rx = client.subscribe();

        tokio::spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let leader = *rx.borrow();
                is_leader_clone.store(leader, Ordering::SeqCst);

                if leader {
                    // Update term when becoming leader
                    let current = raft_state_clone.current_term();
                    raft_state_clone.current_term.fetch_add(1, Ordering::SeqCst);
                    tracing::info!("Node became leader, term: {} -> {}", current, current + 1);
                }
            }
        });

        Ok(Arc::new(client))
    }

    /// Convert string node ID to numeric Raft ID.
    ///
    /// Maps node IDs to small sequential numbers suitable for port allocation.
    /// The Raft client constructs ports as `5690 + raft_node_id`, so IDs must be < 60000.
    fn node_id_to_raft_id(node_id: &str) -> u64 {
        // For standard test node IDs, use sequential mapping
        match node_id {
            "meta-1" => 1,
            "meta-2" => 2,
            "meta-3" => 3,
            "node-1" => 1,
            "node-2" => 2,
            "node-3" => 3,
            // For other IDs, hash to a small range (0-999)
            _ => {
                let hash = node_id
                    .bytes()
                    .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
                (hash % 1000) + 1
            }
        }
    }

    /// Check if this node is currently the Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Wait for a leader to be elected in the cluster.
    ///
    /// Blocks until leader election completes or timeout expires.
    ///
    /// # Returns
    ///
    /// The node ID of the elected leader.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Timeout expires before leader is elected
    /// - Cluster is shutting down
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<String> {
        let start = std::time::Instant::now();

        loop {
            // Check if we are the leader
            if self.is_leader().await {
                return Ok(self.node_id.clone());
            }

            // TODO (Day 2): Query Raft state for current leader
            // For now, simulate leader election after a short delay
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Check timeout
            if start.elapsed() > timeout {
                return Err(EventStreamingError::Timeout(format!(
                    "Leader election timeout after {:?}",
                    timeout
                )));
            }

            // Check if shutting down
            if matches!(*self.state.read().await, MetaClusterState::ShuttingDown) {
                return Err(EventStreamingError::Internal(
                    "Cluster shutting down".to_string(),
                ));
            }
        }
    }

    /// Get the current Raft term.
    pub fn current_term(&self) -> u64 {
        self.raft_state.current_term()
    }

    /// Get the current Raft commit index.
    pub fn commit_index(&self) -> u64 {
        self.raft_state.commit_index()
    }

    /// Get the current cluster state.
    pub async fn cluster_state(&self) -> MetaClusterState {
        *self.state.read().await
    }

    /// Shutdown the Meta cluster node gracefully.
    ///
    /// Stops accepting new catalog operations, drains in-flight requests,
    /// and leaves the Raft cluster.
    pub async fn shutdown(&self) -> Result<()> {
        tracing::info!(node_id = %self.node_id, "Shutting down Meta cluster node");

        *self.state.write().await = MetaClusterState::ShuttingDown;

        // TODO (Day 2): Graceful shutdown
        // - Stop election timer
        // - Step down if leader
        // - Drain in-flight catalog operations
        // - Close backend connection
        // - Leave Raft cluster

        tracing::info!(node_id = %self.node_id, "Meta cluster node shut down");
        Ok(())
    }

    /// Get node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get configuration reference.
    pub fn config(&self) -> &DistributedLibraryConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_meta_cluster_start() {
        let config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        let cluster = DistributedMetaCluster::start(config).await.unwrap();

        assert_eq!(cluster.node_id(), "meta-1");
        assert_eq!(cluster.cluster_state().await, MetaClusterState::Follower);
    }

    #[tokio::test]
    async fn test_meta_cluster_shutdown() {
        let config = DistributedLibraryConfig::test_3node_memory("meta-1", 5690);
        let cluster = DistributedMetaCluster::start(config).await.unwrap();

        cluster.shutdown().await.unwrap();
        assert_eq!(
            cluster.cluster_state().await,
            MetaClusterState::ShuttingDown
        );
    }

    #[tokio::test]
    async fn test_raft_state() {
        let state = RaftState::new();
        assert_eq!(state.current_term(), 1);
        assert_eq!(state.commit_index(), 0);
    }
}
