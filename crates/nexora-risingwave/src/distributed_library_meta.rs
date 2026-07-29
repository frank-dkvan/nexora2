//! Distributed Meta cluster coordination for library mode.
//!
//! Manages a multi-node Meta cluster with Raft consensus. Each Nexora process
//! runs one Meta node in-process, and they coordinate via Raft for catalog
//! consistency.

use crate::distributed_library_config::{DistributedLibraryConfig, MetaBackend};
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
    current_term: AtomicU64,
    voted_for: RwLock<Option<String>>,
    commit_index: AtomicU64,
    last_applied: AtomicU64,
}

use std::sync::atomic::AtomicU64;

impl RaftState {
    fn new() -> Self {
        Self {
            current_term: AtomicU64::new(1),
            voted_for: RwLock::new(None),
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
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

        // TODO (Day 2): Initialize RisingWave Meta node with Raft
        // - Create Meta service handle
        // - Configure backend (etcd/SQLite)
        // - Join Raft cluster (connect to peers)
        // - Start election timer
        // - Wait for quorum

        let cluster = Self {
            node_id: config.node_id.clone(),
            raft_state,
            is_leader,
            state,
            config,
        };

        // Transition to Follower state (will become Leader after election)
        *cluster.state.write().await = MetaClusterState::Follower;

        tracing::info!(
            node_id = %cluster.node_id,
            "Meta cluster node started (follower)"
        );

        Ok(cluster)
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
        assert_eq!(
            cluster.cluster_state().await,
            MetaClusterState::Follower
        );
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
