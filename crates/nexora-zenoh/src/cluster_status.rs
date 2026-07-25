//! Cluster status queries and monitoring API.
//!
//! Provides structured information about cluster health, shard assignments,
//! replication status, and node states.

use crate::health_monitor::{HealthMonitor, NodeHealth};
use crate::shard_map::{NodeId, ShardMap};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Complete cluster status snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// Total number of shards
    pub total_shards: usize,
    /// Current shard map version
    pub shard_map_version: u64,
    /// Per-node health information
    pub nodes: HashMap<NodeId, NodeStatus>,
    /// Per-shard assignment and health
    pub shards: HashMap<usize, ShardStatus>,
    /// Overall cluster health summary
    pub summary: ClusterSummary,
}

/// Status of a single node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node_id: NodeId,
    pub health: NodeHealth,
    pub last_seen_secs: Option<u64>,
    pub consecutive_failures: u32,
    pub avg_latency_ms: Option<u64>,
    pub roles: Vec<ShardRole>,
}

/// Role a node plays for a shard
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardRole {
    pub shard_id: usize,
    pub role: Role,
    pub epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    Owner,
    Replica,
}

/// Status of a single shard
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardStatus {
    pub shard_id: usize,
    pub owner: NodeId,
    pub owner_health: NodeHealth,
    pub replicas: Vec<ReplicaInfo>,
    pub epoch: u64,
    pub is_healthy: bool,
}

/// Information about a replica node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: NodeId,
    pub health: NodeHealth,
}

/// High-level cluster health summary
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub degraded_nodes: usize,
    pub failed_nodes: usize,
    pub healthy_shards: usize,
    pub at_risk_shards: usize,
    pub unavailable_shards: usize,
}

/// Cluster status query service
pub struct ClusterStatusQuery {
    shard_map: Arc<RwLock<ShardMap>>,
    health_monitor: Arc<HealthMonitor>,
}

impl ClusterStatusQuery {
    pub fn new(shard_map: Arc<RwLock<ShardMap>>, health_monitor: Arc<HealthMonitor>) -> Self {
        Self {
            shard_map,
            health_monitor,
        }
    }

    /// Get complete cluster status
    pub async fn get_status(&self) -> ClusterStatus {
        let map = self.shard_map.read().await;
        let all_health = self.health_monitor.get_all_health().await;

        // Build node status map
        let mut nodes = HashMap::new();
        for (node_id, health_info) in &all_health {
            let roles = self.collect_node_roles(&map, node_id);
            nodes.insert(
                node_id.clone(),
                NodeStatus {
                    node_id: node_id.clone(),
                    health: health_info.status.clone(),
                    last_seen_secs: Some(health_info.last_seen.elapsed().as_secs()),
                    consecutive_failures: health_info.consecutive_failures,
                    avg_latency_ms: health_info.avg_latency_ms,
                    roles,
                },
            );
        }

        // Build shard status map
        let mut shards = HashMap::new();
        for (shard_id, assignment) in &map.assignments {
            let owner_health = all_health
                .get(&assignment.owner)
                .map(|h| h.status.clone())
                .unwrap_or(NodeHealth::Failed);

            let replicas: Vec<ReplicaInfo> = assignment
                .replicas
                .iter()
                .map(|replica_id| {
                    let health = all_health
                        .get(replica_id)
                        .map(|h| h.status.clone())
                        .unwrap_or(NodeHealth::Failed);
                    ReplicaInfo {
                        node_id: replica_id.clone(),
                        health,
                    }
                })
                .collect();

            let is_healthy = owner_health == NodeHealth::Healthy
                && replicas.iter().any(|r| r.health == NodeHealth::Healthy);

            shards.insert(
                *shard_id,
                ShardStatus {
                    shard_id: *shard_id,
                    owner: assignment.owner.clone(),
                    owner_health,
                    replicas,
                    epoch: assignment.epoch.value(),
                    is_healthy,
                },
            );
        }

        // Build summary
        let summary = self.build_summary(&nodes, &shards);

        ClusterStatus {
            total_shards: map.assignments.len(),
            shard_map_version: map.version,
            nodes,
            shards,
            summary,
        }
    }

    /// Get status for a specific shard
    pub async fn get_shard_status(&self, shard_id: usize) -> Option<ShardStatus> {
        let status = self.get_status().await;
        status.shards.get(&shard_id).cloned()
    }

    /// Get status for a specific node
    pub async fn get_node_status(&self, node_id: &NodeId) -> Option<NodeStatus> {
        let status = self.get_status().await;
        status.nodes.get(node_id).cloned()
    }

    fn collect_node_roles(&self, map: &ShardMap, node_id: &NodeId) -> Vec<ShardRole> {
        let mut roles = Vec::new();
        for (shard_id, assignment) in &map.assignments {
            if &assignment.owner == node_id {
                roles.push(ShardRole {
                    shard_id: *shard_id,
                    role: Role::Owner,
                    epoch: assignment.epoch.value(),
                });
            } else if assignment.replicas.contains(node_id) {
                roles.push(ShardRole {
                    shard_id: *shard_id,
                    role: Role::Replica,
                    epoch: assignment.epoch.value(),
                });
            }
        }
        roles
    }

    fn build_summary(
        &self,
        nodes: &HashMap<NodeId, NodeStatus>,
        shards: &HashMap<usize, ShardStatus>,
    ) -> ClusterSummary {
        let total_nodes = nodes.len();
        let mut healthy_nodes = 0;
        let mut degraded_nodes = 0;
        let mut failed_nodes = 0;

        for node in nodes.values() {
            match node.health {
                NodeHealth::Healthy => healthy_nodes += 1,
                NodeHealth::Degraded => degraded_nodes += 1,
                NodeHealth::Failed => failed_nodes += 1,
            }
        }

        let mut healthy_shards = 0;
        let mut at_risk_shards = 0;
        let mut unavailable_shards = 0;

        for shard in shards.values() {
            if shard.is_healthy {
                healthy_shards += 1;
            } else if shard.owner_health == NodeHealth::Failed
                && shard
                    .replicas
                    .iter()
                    .all(|r| r.health == NodeHealth::Failed)
            {
                unavailable_shards += 1;
            } else {
                at_risk_shards += 1;
            }
        }

        ClusterSummary {
            total_nodes,
            healthy_nodes,
            degraded_nodes,
            failed_nodes,
            healthy_shards,
            at_risk_shards,
            unavailable_shards,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health_monitor::HealthMonitorConfig;
    use crate::tcp_transport::TcpRemoteClient;

    #[tokio::test]
    async fn test_cluster_status_query_creation() {
        let shard_map = Arc::new(RwLock::new(ShardMap::new_local(3)));
        let client = Arc::new(TcpRemoteClient::new());
        let health_monitor = Arc::new(HealthMonitor::new(HealthMonitorConfig::default(), client));

        let query = ClusterStatusQuery::new(shard_map, health_monitor);
        let status = query.get_status().await;

        // ShardMap has 3 shards, but no health info yet, so nodes will be empty
        assert_eq!(status.total_shards, 3);
        assert_eq!(status.summary.total_nodes, 0);
    }
}
