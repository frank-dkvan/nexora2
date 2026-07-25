//! Zenoh-based cluster manager.
//!
//! Combines Zenoh session, liveliness discovery, HybridRouter, and ControlPlane
//! into a unified cluster manager. Uses:
//!
//! - **Zenoh query/reply**: for graph operations (via ZenohGraphServer + ZenohRemoteClient)
//! - **Zenoh liveliness**: for node discovery and failure detection (replaces TCP heartbeat)
//! - **Zenoh pub/sub**: for ShardMap propagation (replaces manual gossip)
//!
//! Key expressions:
//!   - `graph/node/{node_id}` — queryable for graph operations
//!   - `cluster/node/{node_id}/alive` — liveliness token per node
//!   - `cluster/shardmap` — pub/sub for ShardMap updates

#![cfg(feature = "zenoh")]

use crate::control::ControlPlane;
use crate::discovery::ClusterRegistry;
use crate::router::HybridRouter;
use crate::shard_map::ShardMap;
use crate::tcp_transport::GraphHandler;
use crate::zenoh_discovery::{FailoverCallback, ZenohDiscovery};
use crate::zenoh_transport::{ZenohGraphServer, ZenohRemoteClient};
use crate::RouterError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use zenoh::key_expr::KeyExpr;
use zenoh::sample::Sample;
use zenoh::Session;

/// Key expression for ShardMap pub/sub.
const SHARDMAP_KEY: &str = "cluster/shardmap";

/// Configuration for Zenoh cluster mode.
#[derive(Clone, Debug)]
pub struct ZenohClusterConfig {
    /// This node's unique ID
    pub node_id: String,
    /// Total number of logical shards
    pub total_shards: usize,
    /// Operation timeout for remote graph operations
    pub op_timeout: Duration,
}

impl Default for ZenohClusterConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".into(),
            total_shards: 256,
            op_timeout: Duration::from_secs(10),
        }
    }
}

/// Cluster statistics (same as TCP version but for Zenoh).
#[derive(Clone, Debug, serde::Serialize)]
pub struct ZenohClusterStats {
    pub node_id: String,
    pub alive_nodes: usize,
    pub uptime_secs: u64,
    pub shard_map_version: u64,
    pub total_shards: usize,
    pub local_shards: usize,
}

/// Zenoh-based cluster manager.
///
/// Orchestrates all distributed components using Eclipse Zenoh:
/// - Opens a Zenoh session in peer mode
/// - Starts ZenohGraphServer to accept graph operations
/// - Creates ZenohRemoteClient for outgoing operations
/// - Uses ZenohDiscovery for node presence/failure detection
/// - Propagates ShardMap updates via Zenoh pub/sub
/// - Triggers failover on node failure
pub struct ZenohClusterManager {
    config: ZenohClusterConfig,
    session: Arc<Session>,
    registry: Arc<ClusterRegistry>,
    control_plane: Arc<ControlPlane>,
    router: Arc<HybridRouter>,
    remote_client: Arc<ZenohRemoteClient>,
    graph_server: Option<ZenohGraphServer>,
    discovery: Option<ZenohDiscovery>,
    /// ShardMap subscriber handle
    _shardmap_sub: Option<tokio::task::JoinHandle<()>>,
    start_time: Instant,
}

impl ZenohClusterManager {
    /// Create a new Zenoh cluster manager.
    ///
    /// Opens a Zenoh session with default peer-mode config.
    pub async fn new(config: ZenohClusterConfig) -> Result<Self, ZenohClusterError> {
        let zenoh_config = zenoh::Config::default();
        let session = Arc::new(
            zenoh::open(zenoh_config)
                .await
                .map_err(|e| ZenohClusterError::Session(e.to_string()))?,
        );

        let registry = Arc::new(ClusterRegistry::new());
        let control_plane = Arc::new(ControlPlane::with_local_node(
            config.total_shards,
            vec![],
            config.node_id.clone(),
        ));

        let remote_client =
            Arc::new(ZenohRemoteClient::new(session.clone()).with_timeout(config.op_timeout));

        let shard_map = ShardMap::new_local_with_node(config.total_shards, config.node_id.clone());
        let router = Arc::new(HybridRouter::new_clustered_no_local(
            shard_map,
            remote_client.clone(),
        ));

        Ok(Self {
            config,
            session,
            registry,
            control_plane,
            router,
            remote_client,
            graph_server: None,
            discovery: None,
            _shardmap_sub: None,
            start_time: Instant::now(),
        })
    }

    /// Start the cluster manager.
    pub async fn start(&mut self, handler: Arc<dyn GraphHandler>) -> Result<(), ZenohClusterError> {
        // 1. Start graph server (declare queryable)
        let mut graph_server =
            ZenohGraphServer::new(self.session.clone(), self.config.node_id.clone());
        graph_server
            .start(handler)
            .await
            .map_err(|e| ZenohClusterError::Transport(e.to_string()))?;
        self.graph_server = Some(graph_server);

        // 2. Start liveliness discovery with failover callback
        let control_plane = self.control_plane.clone();
        let router = self.router.clone();
        let session = self.session.clone();
        let self_node_id = self.config.node_id.clone();

        let on_failure: FailoverCallback = Arc::new(move |failed_node_id: String| {
            let cp = control_plane.clone();
            let rt = router.clone();
            let sn = session.clone();
            let own_id = self_node_id.clone();
            tokio::spawn(async move {
                tracing::warn!("Failover triggered for node {failed_node_id}");

                let failed_shards = cp.mark_node_failed(&failed_node_id).await;

                for shard_id in &failed_shards {
                    if let Ok(token) = cp.failover_shard(*shard_id, own_id.clone()).await {
                        tracing::info!(
                            "Shard {} failed over to {} (epoch {})",
                            shard_id,
                            own_id,
                            token.epoch.value()
                        );
                    }
                }

                if !failed_shards.is_empty() {
                    let map = cp.get_shard_map().await;
                    rt.update_shard_map(map.clone()).await;

                    // Publish updated ShardMap to cluster
                    let payload = serde_json::to_vec(&map).unwrap_or_default();
                    let _ = sn
                        .put(SHARDMAP_KEY, zenoh::bytes::ZBytes::from(payload))
                        .await;
                }
            });
        });

        let mut discovery = ZenohDiscovery::new(self.session.clone(), self.config.node_id.clone());
        discovery
            .start(self.registry.clone(), on_failure)
            .await
            .map_err(|e: RouterError| ZenohClusterError::Discovery(e.to_string()))?;
        self.discovery = Some(discovery);

        // 3. Subscribe to ShardMap updates from other nodes
        self.start_shardmap_subscriber().await;

        // 4. Give Zenoh time to establish connections
        tokio::time::sleep(Duration::from_millis(300)).await;

        tracing::info!(
            "ZenohClusterManager started for node {} ({} shards)",
            self.config.node_id,
            self.config.total_shards
        );

        Ok(())
    }

    /// Subscribe to ShardMap pub/sub updates.
    async fn start_shardmap_subscriber(&mut self) {
        let key = KeyExpr::try_from(SHARDMAP_KEY).unwrap();
        let subscriber = self
            .session
            .declare_subscriber(key)
            .await
            .expect("failed to declare ShardMap subscriber");

        let router = self.router.clone();
        let self_node_id = self.config.node_id.clone();

        self._shardmap_sub = Some(tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                process_shardmap_update(&sample, &router, &self_node_id).await;
            }
        }));
    }

    /// Get a reference to the hybrid router.
    pub fn router(&self) -> &HybridRouter {
        &self.router
    }

    /// Get the remote client for manual operations.
    pub fn remote_client(&self) -> &ZenohRemoteClient {
        &self.remote_client
    }

    /// Get cluster stats.
    pub async fn stats(&self) -> ZenohClusterStats {
        let alive = self.registry.alive_count().await;
        let uptime_secs = self.start_time.elapsed().as_secs();
        let shard_map = self.router.shard_map_snapshot().await;

        ZenohClusterStats {
            node_id: self.config.node_id.clone(),
            alive_nodes: alive,
            uptime_secs,
            shard_map_version: shard_map.version,
            total_shards: shard_map.total_shards,
            local_shards: shard_map.local_shard_count(),
        }
    }

    /// Publish the current ShardMap to the cluster.
    pub async fn publish_shardmap(&self) {
        let map = self.control_plane.get_shard_map().await;
        let payload = serde_json::to_vec(&map).unwrap_or_default();
        let _ = self
            .session
            .put(SHARDMAP_KEY, zenoh::bytes::ZBytes::from(payload))
            .await;
        tracing::debug!("Published ShardMap version {}", map.version);
    }

    /// Shutdown the cluster manager.
    pub async fn shutdown(&self) {
        // Closing the session will drop all declarations
        let _ = self.session.close().await;
        tracing::info!("ZenohClusterManager shut down");
    }
}

/// Process a ShardMap update received via Zenoh pub/sub.
async fn process_shardmap_update(sample: &Sample, router: &Arc<HybridRouter>, self_node_id: &str) {
    let bytes = sample.payload().to_bytes();
    let map: ShardMap = match serde_json::from_slice(&bytes) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("Failed to deserialize ShardMap update: {e}");
            return;
        }
    };

    // Only accept newer ShardMap versions
    let current = router.shard_map_snapshot().await;
    if map.version <= current.version {
        return;
    }

    tracing::info!(
        "Received ShardMap update v{} (local was v{}) — {} shards, local node: {}",
        map.version,
        current.version,
        map.total_shards,
        self_node_id
    );

    router.update_shard_map(map).await;
}

#[derive(Debug, thiserror::Error)]
pub enum ZenohClusterError {
    #[error("session error: {0}")]
    Session(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("discovery error: {0}")]
    Discovery(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler;

    #[async_trait::async_trait]
    impl GraphHandler for TestHandler {
        async fn handle(
            &self,
            _target: &str,
            _op: crate::GraphOperation,
        ) -> Result<crate::GraphResult, crate::RouterError> {
            Ok(crate::GraphResult::Status {
                ok: true,
                message: "test".into(),
            })
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_cluster_single_node() {
        let config = ZenohClusterConfig {
            node_id: "zenoh-test-1".into(),
            total_shards: 4,
            op_timeout: Duration::from_secs(5),
        };

        let mut manager = ZenohClusterManager::new(config).await.unwrap();
        manager.start(Arc::new(TestHandler)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let stats = manager.stats().await;
        assert_eq!(stats.node_id, "zenoh-test-1");
        assert!(stats.alive_nodes >= 1);

        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_cluster_graph_operation() {
        let config = ZenohClusterConfig {
            node_id: "zenoh-test-2".into(),
            total_shards: 4,
            op_timeout: Duration::from_secs(5),
        };

        let mut manager = ZenohClusterManager::new(config).await.unwrap();
        manager.start(Arc::new(TestHandler)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Execute a graph operation through the router
        let qid = nexora_id::NexoraId::from_bytes(b"test-node".to_vec());
        let result = manager
            .router()
            .route(
                &qid,
                crate::GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: "name".into(),
                },
            )
            .await;

        // Should route to local handler (single node owns all shards)
        assert!(result.is_ok());
        match result.unwrap() {
            crate::GraphResult::Status { ok, .. } => assert!(ok),
            other => panic!("expected Status, got {other:?}"),
        }

        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zenoh_cluster_two_nodes() {
        // Start node A
        let config_a = ZenohClusterConfig {
            node_id: "zenoh-node-a".into(),
            total_shards: 8,
            op_timeout: Duration::from_secs(5),
        };
        let mut manager_a = ZenohClusterManager::new(config_a).await.unwrap();
        manager_a.start(Arc::new(TestHandler)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start node B
        let config_b = ZenohClusterConfig {
            node_id: "zenoh-node-b".into(),
            total_shards: 8,
            op_timeout: Duration::from_secs(5),
        };
        let mut manager_b = ZenohClusterManager::new(config_b).await.unwrap();
        manager_b.start(Arc::new(TestHandler)).await.unwrap();

        // Wait for discovery
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let stats_a = manager_a.stats().await;
        let stats_b = manager_b.stats().await;

        // Both should see at least 2 nodes (themselves + the other)
        assert!(
            stats_a.alive_nodes >= 2,
            "Node A should see at least 2 nodes, got {}",
            stats_a.alive_nodes
        );
        assert!(
            stats_b.alive_nodes >= 2,
            "Node B should see at least 2 nodes, got {}",
            stats_b.alive_nodes
        );

        manager_a.shutdown().await;
        manager_b.shutdown().await;
    }
}
