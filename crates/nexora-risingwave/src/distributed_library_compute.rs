//! Distributed Compute node management for stream processing workload.
//!
//! Manages Compute nodes in distributed library mode, handling registration,
//! heartbeat, and work distribution across the cluster.

use crate::distributed_library_config::DistributedLibraryConfig;
use crate::error::{EventStreamingError, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Compute node health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputeHealth {
    /// Compute node is healthy and processing work.
    Healthy,
    /// Compute node is degraded (high load, slow response).
    Degraded,
    /// Compute node is unavailable (crashed, network issue).
    Unavailable,
}

/// Compute node descriptor.
#[derive(Debug, Clone)]
pub struct ComputeNode {
    /// Node identifier.
    pub node_id: String,

    /// Compute listen address.
    pub listen_addr: SocketAddr,

    /// Worker parallelism (number of worker threads).
    pub parallelism: usize,

    /// Current health status.
    pub health: ComputeHealth,

    /// Last heartbeat timestamp.
    pub last_heartbeat: std::time::Instant,

    /// Current workload (number of active fragments).
    pub active_fragments: u32,
}

/// Distributed Compute cluster manager.
///
/// # Architecture
///
/// ```text
/// DistributedComputeCluster
/// ├─ Compute Node 1 (local, 8 workers)
/// ├─ Compute Node 2 (local, 8 workers)
/// └─ Compute Node 3 (local, 8 workers)
///      │
///      └─> Fragment scheduler (work distribution)
/// ```
///
/// # Example
///
/// ```rust,no_run
/// use nexora_risingwave::{DistributedComputeCluster, DistributedLibraryConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
///
/// // Create Compute cluster
/// let cluster = DistributedComputeCluster::new(config).await?;
///
/// // Register local Compute node
/// cluster.register_compute_node().await?;
///
/// // Get available worker count
/// let workers = cluster.total_parallelism().await;
/// println!("Total workers: {}", workers);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DistributedComputeCluster {
    /// All Compute nodes in the cluster.
    nodes: Arc<RwLock<HashMap<String, ComputeNode>>>,

    /// Configuration snapshot.
    config: DistributedLibraryConfig,
}

impl DistributedComputeCluster {
    /// Create a new distributed Compute cluster.
    ///
    /// # Errors
    ///
    /// Returns error if initialization fails.
    pub async fn new(config: DistributedLibraryConfig) -> Result<Self> {
        info!(
            node_id = %config.node_id,
            "Creating distributed Compute cluster"
        );

        let cluster = Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            config,
        };

        info!("Distributed Compute cluster created");
        Ok(cluster)
    }

    /// Register the local Compute node with the Meta cluster.
    ///
    /// Reports node capacity and begins sending heartbeats.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Meta cluster is unreachable
    /// - Registration fails
    pub async fn register_compute_node(&self) -> Result<()> {
        let parallelism = self
            .config
            .compute
            .parallelism
            .unwrap_or_else(num_cpus::get);

        info!(
            node_id = %self.config.node_id,
            listen_addr = %self.config.compute.listen_addr,
            parallelism = parallelism,
            "Registering local Compute node"
        );

        let node = ComputeNode {
            node_id: self.config.node_id.clone(),
            listen_addr: self.config.compute.listen_addr.clone(),
            parallelism,
            health: ComputeHealth::Healthy,
            last_heartbeat: std::time::Instant::now(),
            active_fragments: 0,
        };

        self.nodes.write().await.insert(node.node_id.clone(), node);

        info!("Local Compute node registered");
        Ok(())
    }

    /// Send heartbeat to Meta cluster.
    ///
    /// Reports current health status and workload.
    pub async fn send_heartbeat(&self) -> Result<()> {
        debug!(node_id = %self.config.node_id, "Sending Compute heartbeat");

        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&self.config.node_id) {
            node.last_heartbeat = std::time::Instant::now();
        }

        Ok(())
    }

    /// Get total worker parallelism across all healthy nodes.
    pub async fn total_parallelism(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.health == ComputeHealth::Healthy)
            .map(|n| n.parallelism)
            .sum()
    }

    /// Get the number of healthy Compute nodes.
    pub async fn healthy_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .filter(|n| n.health == ComputeHealth::Healthy)
            .count()
    }

    /// Start heartbeat background task.
    ///
    /// Sends periodic heartbeats to Meta cluster and monitors node health.
    pub fn start_heartbeat(&self) -> tokio::task::JoinHandle<()> {
        let cluster = self.clone_handle();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;

                if let Err(e) = cluster.send_heartbeat().await {
                    warn!(error = %e, "Failed to send Compute heartbeat");
                }

                // Check for stale nodes (no heartbeat in 30s)
                cluster.check_stale_nodes().await;
            }
        })
    }

    /// Check for stale Compute nodes and mark as unavailable.
    async fn check_stale_nodes(&self) {
        let mut nodes = self.nodes.write().await;
        let now = std::time::Instant::now();
        let stale_threshold = std::time::Duration::from_secs(30);

        for (node_id, node) in nodes.iter_mut() {
            if now.duration_since(node.last_heartbeat) > stale_threshold
                && node.health != ComputeHealth::Unavailable
            {
                warn!(
                    node_id = %node_id,
                    "Compute node marked as unavailable (stale heartbeat)"
                );
                node.health = ComputeHealth::Unavailable;
            }
        }
    }

    /// Clone a handle for background tasks.
    fn clone_handle(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            config: self.config.clone(),
        }
    }

    /// Get node status by ID.
    pub async fn get_node_status(&self, node_id: &str) -> Option<ComputeNode> {
        self.nodes.read().await.get(node_id).cloned()
    }

    /// List all Compute nodes.
    pub async fn list_nodes(&self) -> Vec<ComputeNode> {
        self.nodes.read().await.values().cloned().collect()
    }

    /// Shutdown the Compute cluster.
    ///
    /// Deregisters all nodes and stops background tasks.
    pub async fn shutdown(&self) -> Result<()> {
        info!(node_id = %self.config.node_id, "Shutting down Compute cluster");

        // Day 3 placeholder: Graceful shutdown
        // - Stop accepting new work
        // - Drain in-flight fragments
        // - Deregister from Meta

        self.nodes.write().await.clear();

        info!("Compute cluster shut down");
        Ok(())
    }
}

/// Fragment assignment for work distribution.
///
/// Day 3 placeholder: Will be expanded for actual fragment scheduling.
#[derive(Debug, Clone)]
pub struct FragmentAssignment {
    /// Fragment ID (stream processing unit).
    pub fragment_id: u64,

    /// Assigned Compute node ID.
    pub node_id: String,

    /// Parallelism (number of workers).
    pub parallelism: u32,
}

/// Fragment scheduler for work distribution.
///
/// Day 3 placeholder: Simple round-robin scheduler.
pub struct FragmentScheduler {
    /// Reference to Compute cluster.
    cluster: Arc<DistributedComputeCluster>,

    /// Next node index for round-robin.
    next_idx: Arc<RwLock<usize>>,
}

impl FragmentScheduler {
    /// Create a new fragment scheduler.
    pub fn new(cluster: Arc<DistributedComputeCluster>) -> Self {
        Self {
            cluster,
            next_idx: Arc::new(RwLock::new(0)),
        }
    }

    /// Schedule a fragment to a Compute node.
    ///
    /// Uses round-robin load balancing across healthy nodes.
    ///
    /// # Arguments
    ///
    /// - `fragment_id`: Fragment to schedule
    /// - `parallelism`: Required parallelism
    ///
    /// # Errors
    ///
    /// Returns error if no healthy Compute nodes available.
    pub async fn schedule_fragment(
        &self,
        fragment_id: u64,
        parallelism: u32,
    ) -> Result<FragmentAssignment> {
        let nodes = self.cluster.list_nodes().await;
        let healthy: Vec<_> = nodes
            .into_iter()
            .filter(|n| n.health == ComputeHealth::Healthy)
            .collect();

        if healthy.is_empty() {
            return Err(EventStreamingError::Internal(
                "No healthy Compute nodes available".to_string(),
            ));
        }

        // Round-robin selection
        let mut idx = self.next_idx.write().await;
        let node = &healthy[*idx % healthy.len()];
        *idx += 1;

        debug!(
            fragment_id = %fragment_id,
            node_id = %node.node_id,
            parallelism = %parallelism,
            "Scheduled fragment to Compute node"
        );

        Ok(FragmentAssignment {
            fragment_id,
            node_id: node.node_id.clone(),
            parallelism,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compute_cluster_creation() {
        let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
        let cluster = DistributedComputeCluster::new(config).await.unwrap();

        assert_eq!(cluster.healthy_count().await, 0);
    }

    #[tokio::test]
    async fn test_compute_node_registration() {
        let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
        let cluster = DistributedComputeCluster::new(config).await.unwrap();

        cluster.register_compute_node().await.unwrap();

        assert_eq!(cluster.healthy_count().await, 1);
        assert_eq!(cluster.total_parallelism().await, 2); // test_3node_memory sets parallelism to 2
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
        let cluster = DistributedComputeCluster::new(config).await.unwrap();

        cluster.register_compute_node().await.unwrap();

        let handle = cluster.start_heartbeat();

        // Wait for one heartbeat cycle
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        handle.abort();
    }

    #[tokio::test]
    async fn test_fragment_scheduler() {
        let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
        let cluster = Arc::new(DistributedComputeCluster::new(config).await.unwrap());

        cluster.register_compute_node().await.unwrap();

        let scheduler = FragmentScheduler::new(cluster);

        let assignment = scheduler.schedule_fragment(1, 4).await.unwrap();
        assert_eq!(assignment.fragment_id, 1);
        assert_eq!(assignment.parallelism, 4);
    }
}
