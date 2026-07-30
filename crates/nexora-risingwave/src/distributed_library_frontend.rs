//! Distributed Frontend pool for load balancing query workload.
//!
//! Manages multiple Frontend nodes in distributed library mode, routing queries
//! to available Frontends using round-robin load balancing.

use crate::distributed_library_config::DistributedLibraryConfig;
use crate::error::{EventStreamingError, Result};
use crate::frontend_wrapper::FrontendWrapper;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Frontend node health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontendHealth {
    /// Frontend is healthy and accepting queries.
    Healthy,
    /// Frontend is degraded but still operational.
    Degraded,
    /// Frontend is unavailable.
    Unavailable,
}

/// Frontend node descriptor.
#[derive(Debug)]
pub struct FrontendNode {
    /// Node identifier.
    pub node_id: String,

    /// Frontend listen address.
    pub listen_addr: SocketAddr,

    /// Frontend wrapper (in-process).
    pub frontend: Arc<FrontendWrapper>,

    /// Current health status.
    pub health: Arc<RwLock<FrontendHealth>>,

    /// Last health check timestamp.
    pub last_check: Arc<RwLock<std::time::Instant>>,
}

/// Distributed Frontend pool with load balancing.
///
/// # Architecture
///
/// ```text
/// DistributedFrontendPool
/// ├─ Frontend Node 1 (local)
/// ├─ Frontend Node 2 (local)
/// └─ Frontend Node 3 (local)
///      │
///      └─> Round-robin load balancer
/// ```
///
/// # Example
///
/// ```rust,no_run
/// use nexora_risingwave::{DistributedFrontendPool, DistributedLibraryConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
///
/// // Create Frontend pool
/// let pool = DistributedFrontendPool::new(config).await?;
///
/// // Execute query (routed to healthy Frontend)
/// let result = pool.query("SELECT * FROM users LIMIT 10").await?;
///
/// // Execute DDL (routed to leader Frontend)
/// pool.execute_ddl("CREATE SOURCE events ...").await?;
/// # Ok(())
/// # }
/// ```
pub struct DistributedFrontendPool {
    /// All Frontend nodes in the pool.
    nodes: Arc<RwLock<Vec<Arc<FrontendNode>>>>,

    /// Round-robin index for load balancing.
    round_robin_idx: Arc<AtomicUsize>,

    /// Configuration snapshot.
    config: DistributedLibraryConfig,
}

impl DistributedFrontendPool {
    /// Create a new distributed Frontend pool.
    ///
    /// Initializes all Frontend nodes in the cluster. Each Frontend runs
    /// in-process as part of the Nexora node.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Frontend initialization fails
    /// - No healthy Frontends available
    pub async fn new(config: DistributedLibraryConfig) -> Result<Self> {
        info!(
            node_id = %config.node_id,
            "Creating distributed Frontend pool"
        );

        let pool = Self {
            nodes: Arc::new(RwLock::new(Vec::new())),
            round_robin_idx: Arc::new(AtomicUsize::new(0)),
            config,
        };

        // Day 3: Initialize local Frontend node
        pool.init_local_frontend().await?;

        info!("Distributed Frontend pool created");
        Ok(pool)
    }

    /// Initialize the local Frontend node.
    async fn init_local_frontend(&self) -> Result<()> {
        info!("Initializing local Frontend node");

        // Day 3 placeholder: Create FrontendWrapper
        // Will be integrated with Meta cluster in actual implementation
        let frontend = Arc::new(FrontendWrapper::new_placeholder()?);

        let node = Arc::new(FrontendNode {
            node_id: self.config.node_id.clone(),
            listen_addr: self.config.frontend.listen_addr.clone(),
            frontend,
            health: Arc::new(RwLock::new(FrontendHealth::Healthy)),
            last_check: Arc::new(RwLock::new(std::time::Instant::now())),
        });

        self.nodes.write().await.push(node);

        info!("Local Frontend node initialized");
        Ok(())
    }

    /// Execute a query on an available Frontend node.
    ///
    /// Uses round-robin load balancing to distribute queries across healthy
    /// Frontends. Automatically retries on next Frontend if one fails.
    ///
    /// # Arguments
    ///
    /// - `sql`: SQL query to execute
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No healthy Frontends available
    /// - Query execution fails on all Frontends
    pub async fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        debug!("Executing query via Frontend pool: {}", sql);

        let nodes = self.nodes.read().await;
        if nodes.is_empty() {
            return Err(EventStreamingError::Internal(
                "No Frontend nodes available".to_string(),
            ));
        }

        // Round-robin selection
        let start_idx = self.round_robin_idx.fetch_add(1, Ordering::SeqCst) % nodes.len();

        // Try each node in round-robin order
        for i in 0..nodes.len() {
            let idx = (start_idx + i) % nodes.len();
            let node = &nodes[idx];

            // Check health
            if *node.health.read().await != FrontendHealth::Healthy {
                debug!(
                    node_id = %node.node_id,
                    "Skipping unhealthy Frontend node"
                );
                continue;
            }

            // Execute query
            match node.frontend.query(sql).await {
                Ok(result) => {
                    debug!(
                        node_id = %node.node_id,
                        "Query executed successfully"
                    );
                    return Ok(result);
                }
                Err(e) => {
                    warn!(
                        node_id = %node.node_id,
                        error = %e,
                        "Query failed on Frontend node, trying next"
                    );
                    // Mark as degraded
                    *node.health.write().await = FrontendHealth::Degraded;
                }
            }
        }

        Err(EventStreamingError::Internal(
            "Query failed on all Frontend nodes".to_string(),
        ))
    }

    /// Execute a DDL statement on the leader Frontend.
    ///
    /// DDL statements must go through the Meta leader to ensure consistency.
    /// This method routes to the Frontend node on the same Nexora process as
    /// the Meta leader.
    ///
    /// # Arguments
    ///
    /// - `sql`: DDL SQL statement
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - No Meta leader available
    /// - DDL execution fails
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        debug!("Executing DDL via Frontend pool: {}", sql);

        // Day 3 placeholder: Route to local Frontend
        // Will be enhanced to detect Meta leader in actual implementation
        let nodes = self.nodes.read().await;
        if nodes.is_empty() {
            return Err(EventStreamingError::Internal(
                "No Frontend nodes available".to_string(),
            ));
        }

        let node = &nodes[0];
        node.frontend.execute_ddl(sql).await?;

        debug!("DDL executed successfully");
        Ok(())
    }

    /// Get the number of healthy Frontend nodes.
    pub async fn healthy_count(&self) -> usize {
        let nodes = self.nodes.read().await;
        let mut count = 0;
        for node in nodes.iter() {
            if *node.health.read().await == FrontendHealth::Healthy {
                count += 1;
            }
        }
        count
    }

    /// Start health check background task.
    ///
    /// Periodically checks Frontend node health and updates status.
    pub fn start_health_check(&self) -> tokio::task::JoinHandle<()> {
        let nodes = self.nodes.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                interval.tick().await;

                let nodes = nodes.read().await;
                for node in nodes.iter() {
                    // Day 3 placeholder: Simple health check
                    // Will be enhanced with actual Frontend status query
                    *node.last_check.write().await = std::time::Instant::now();

                    // For now, assume all nodes are healthy
                    let mut health = node.health.write().await;
                    if *health == FrontendHealth::Degraded {
                        debug!(
                            node_id = %node.node_id,
                            "Frontend node recovered"
                        );
                        *health = FrontendHealth::Healthy;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_frontend_pool_creation() {
        let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
        let pool = DistributedFrontendPool::new(config).await.unwrap();

        assert_eq!(pool.healthy_count().await, 1);
    }

    #[tokio::test]
    async fn test_frontend_health_check() {
        let config = DistributedLibraryConfig::test_3node_memory("node-1", 5690);
        let pool = DistributedFrontendPool::new(config).await.unwrap();

        let handle = pool.start_health_check();

        // Wait for one health check cycle
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        handle.abort();
    }
}
