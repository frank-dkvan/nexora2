//! Node discovery and cluster membership.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Information about a cluster node.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    /// Unique node identifier
    pub id: String,
    /// Network address (host:port)
    pub address: String,
    /// Node roles (e.g., "compute", "storage", "meta")
    pub roles: Vec<String>,
    /// Last heartbeat timestamp
    pub last_heartbeat_ms: u64,
    /// Whether the node is currently alive
    pub alive: bool,
}

/// Cluster membership registry.
pub struct ClusterRegistry {
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
}

impl Default for ClusterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterRegistry {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register or update a node.
    pub async fn register(&self, node: NodeInfo) {
        self.nodes.write().await.insert(node.id.clone(), node);
    }

    /// Mark a node as alive with heartbeat.
    pub async fn heartbeat(&self, node_id: &str, timestamp_ms: u64) {
        if let Some(node) = self.nodes.write().await.get_mut(node_id) {
            node.last_heartbeat_ms = timestamp_ms;
            node.alive = true;
        }
    }

    /// Get alive nodes.
    pub async fn alive_nodes(&self) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|n| n.alive)
            .cloned()
            .collect()
    }

    /// Get nodes by role.
    pub async fn nodes_by_role(&self, role: &str) -> Vec<NodeInfo> {
        self.nodes
            .read()
            .await
            .values()
            .filter(|n| n.roles.contains(&role.to_string()) && n.alive)
            .cloned()
            .collect()
    }

    /// Number of alive nodes.
    pub async fn alive_count(&self) -> usize {
        self.nodes.read().await.values().filter(|n| n.alive).count()
    }

    /// Mark a node as dead (failed).
    pub async fn mark_dead(&self, node_id: &str) {
        if let Some(node) = self.nodes.write().await.get_mut(node_id) {
            node.alive = false;
        }
    }

    /// Remove a node from the registry entirely (for dynamic cluster shrink).
    pub async fn unregister(&self, node_id: &str) {
        self.nodes.write().await.remove(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_registry() {
        let registry = ClusterRegistry::new();

        registry
            .register(NodeInfo {
                id: "node-1".into(),
                address: "10.0.0.1:8080".into(),
                roles: vec!["compute".into(), "storage".into()],
                last_heartbeat_ms: 1000,
                alive: true,
            })
            .await;

        registry
            .register(NodeInfo {
                id: "node-2".into(),
                address: "10.0.0.2:8080".into(),
                roles: vec!["compute".into()],
                last_heartbeat_ms: 1000,
                alive: true,
            })
            .await;

        assert_eq!(registry.alive_count().await, 2);
        assert_eq!(registry.nodes_by_role("compute").await.len(), 2);
        assert_eq!(registry.nodes_by_role("storage").await.len(), 1);
    }
}
