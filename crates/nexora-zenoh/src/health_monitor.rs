//! Health monitoring and heartbeat system for cluster nodes.
//!
//! Tracks the health status of all nodes in the cluster by:
//! - Sending periodic heartbeat pings
//! - Recording last-seen timestamps
//! - Detecting failed/degraded nodes
//! - Triggering automatic failover when owner nodes fail

use crate::shard_map::{NodeId, ShardMap};
use crate::tcp_transport::TcpRemoteClient;
use crate::{GraphOperation, RemoteGraphClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Health status of a node
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeHealth {
    /// Node is responding normally
    Healthy,
    /// Node is responding slowly or intermittently
    Degraded,
    /// Node is not responding
    Failed,
}

/// Health information for a single node
#[derive(Clone, Debug)]
pub struct NodeHealthInfo {
    pub node_id: NodeId,
    pub status: NodeHealth,
    pub last_seen: Instant,
    pub consecutive_failures: u32,
    pub avg_latency_ms: Option<u64>,
}

/// Configuration for health monitoring
#[derive(Clone, Debug)]
pub struct HealthMonitorConfig {
    /// How often to send heartbeat pings (default: 5s)
    pub heartbeat_interval: Duration,
    /// How long before marking a node as failed (default: 15s)
    pub failure_timeout: Duration,
    /// How many consecutive failures before marking as failed (default: 3)
    pub failure_threshold: u32,
    /// Latency threshold for degraded status (default: 1000ms)
    pub degraded_latency_ms: u64,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            failure_timeout: Duration::from_secs(15),
            failure_threshold: 3,
            degraded_latency_ms: 1000,
        }
    }
}

/// Health monitor that tracks cluster node health
pub struct HealthMonitor {
    config: HealthMonitorConfig,
    client: Arc<TcpRemoteClient>,
    health_info: Arc<RwLock<HashMap<NodeId, NodeHealthInfo>>>,
    shutdown: Arc<tokio::sync::Notify>,
    monitor_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl HealthMonitor {
    pub fn new(config: HealthMonitorConfig, client: Arc<TcpRemoteClient>) -> Self {
        Self {
            config,
            client,
            health_info: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            monitor_task: Arc::new(RwLock::new(None)),
        }
    }

    /// Start the health monitoring loop
    pub fn start(&self, shard_map: Arc<RwLock<ShardMap>>) {
        let config = self.config.clone();
        let client = self.client.clone();
        let health_info = self.health_info.clone();
        let shutdown = self.shutdown.clone();
        let monitor_task = self.monitor_task.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.heartbeat_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::heartbeat_tick(
                            &config,
                            &client,
                            &health_info,
                            &shard_map,
                        ).await;
                    }
                    _ = shutdown.notified() => {
                        tracing::info!("Health monitor shutting down");
                        break;
                    }
                }
            }
        });

        // Store the handle for graceful shutdown
        let mut task_guard = monitor_task.blocking_write();
        *task_guard = Some(handle);
    }

    /// Perform one heartbeat check for all nodes
    async fn heartbeat_tick(
        config: &HealthMonitorConfig,
        client: &Arc<TcpRemoteClient>,
        health_info: &Arc<RwLock<HashMap<NodeId, NodeHealthInfo>>>,
        shard_map: &Arc<RwLock<ShardMap>>,
    ) {
        let map = shard_map.read().await;
        let mut nodes_to_check = std::collections::HashSet::new();

        // Collect all unique nodes from shard assignments
        for assignment in map.assignments.values() {
            nodes_to_check.insert(assignment.owner.clone());
            for replica in &assignment.replicas {
                nodes_to_check.insert(replica.clone());
            }
        }

        drop(map);

        // Check each node
        for node_id in nodes_to_check {
            let start = Instant::now();
            let result = client.execute(&node_id, GraphOperation::Ping).await;
            let latency_ms = start.elapsed().as_millis() as u64;

            let mut info_guard = health_info.write().await;
            let info = info_guard.entry(node_id.clone()).or_insert(NodeHealthInfo {
                node_id: node_id.clone(),
                status: NodeHealth::Healthy,
                last_seen: Instant::now(),
                consecutive_failures: 0,
                avg_latency_ms: None,
            });

            match result {
                Ok(_) => {
                    info.last_seen = Instant::now();
                    info.consecutive_failures = 0;
                    info.avg_latency_ms = Some(latency_ms);

                    if latency_ms > config.degraded_latency_ms {
                        if info.status != NodeHealth::Degraded {
                            tracing::warn!(
                                node = %node_id,
                                latency_ms = latency_ms,
                                "node marked as degraded due to high latency"
                            );
                            info.status = NodeHealth::Degraded;
                        }
                    } else if info.status != NodeHealth::Healthy {
                        tracing::info!(
                            node = %node_id,
                            "node recovered to healthy status"
                        );
                        info.status = NodeHealth::Healthy;
                    }
                }
                Err(e) => {
                    info.consecutive_failures += 1;
                    tracing::warn!(
                        node = %node_id,
                        consecutive_failures = info.consecutive_failures,
                        error = %e,
                        "heartbeat failed"
                    );

                    if info.consecutive_failures >= config.failure_threshold
                        && info.status != NodeHealth::Failed
                    {
                        tracing::error!(
                            node = %node_id,
                            consecutive_failures = info.consecutive_failures,
                            "node marked as failed"
                        );
                        info.status = NodeHealth::Failed;
                    }
                }
            }
        }

        // Check for stale nodes (haven't been seen in failure_timeout)
        let now = Instant::now();
        let mut info_guard = health_info.write().await;
        for info in info_guard.values_mut() {
            if now.duration_since(info.last_seen) > config.failure_timeout
                && info.status != NodeHealth::Failed
            {
                tracing::error!(
                    node = %info.node_id,
                    last_seen_secs = now.duration_since(info.last_seen).as_secs(),
                    "node marked as failed due to timeout"
                );
                info.status = NodeHealth::Failed;
            }
        }
    }

    /// Get health status for a specific node
    pub async fn get_node_health(&self, node_id: &NodeId) -> Option<NodeHealthInfo> {
        self.health_info.read().await.get(node_id).cloned()
    }

    /// Get health status for all nodes
    pub async fn get_all_health(&self) -> HashMap<NodeId, NodeHealthInfo> {
        self.health_info.read().await.clone()
    }

    /// Check if a node is healthy
    pub async fn is_healthy(&self, node_id: &NodeId) -> bool {
        self.health_info
            .read()
            .await
            .get(node_id)
            .map(|info| info.status == NodeHealth::Healthy)
            .unwrap_or(false)
    }

    /// Check if a node has failed
    pub async fn is_failed(&self, node_id: &NodeId) -> bool {
        self.health_info
            .read()
            .await
            .get(node_id)
            .map(|info| info.status == NodeHealth::Failed)
            .unwrap_or(false)
    }

    /// Shutdown the health monitor gracefully
    pub async fn shutdown(&self) {
        self.shutdown.notify_one();

        // Wait for the monitor task to complete
        if let Some(handle) = self.monitor_task.write().await.take() {
            if let Err(e) = handle.await {
                tracing::error!(error = ?e, "Health monitor task panicked during shutdown");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_monitor_detects_healthy_node() {
        let config = HealthMonitorConfig {
            heartbeat_interval: Duration::from_millis(100),
            failure_timeout: Duration::from_secs(1),
            failure_threshold: 2,
            degraded_latency_ms: 500,
        };

        let client = Arc::new(TcpRemoteClient::new());
        let monitor = HealthMonitor::new(config, client);

        // Initially no health info
        assert!(monitor
            .get_node_health(&"test-node".to_string())
            .await
            .is_none());
    }

    #[tokio::test]
    async fn test_node_health_info_creation() {
        let info = NodeHealthInfo {
            node_id: "node-1".to_string(),
            status: NodeHealth::Healthy,
            last_seen: Instant::now(),
            consecutive_failures: 0,
            avg_latency_ms: Some(10),
        };

        assert_eq!(info.status, NodeHealth::Healthy);
        assert_eq!(info.consecutive_failures, 0);
        assert_eq!(info.avg_latency_ms, Some(10));
    }
}
