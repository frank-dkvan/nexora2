//! Common types for consensus operations.

use serde::{Deserialize, Serialize};

/// Node identifier in the consensus cluster.
pub type NodeId = u64;

/// Log index in the replicated log.
pub type LogIndex = u64;

/// Network address for a consensus node.
pub type NodeAddr = String;

/// Cluster membership configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// All nodes in the cluster (including self).
    pub nodes: Vec<(NodeId, NodeAddr)>,
}

impl ClusterConfig {
    /// Create a single-node cluster (for testing).
    pub fn single_node(node_id: NodeId, addr: NodeAddr) -> Self {
        Self {
            nodes: vec![(node_id, addr)],
        }
    }

    /// Create a three-node cluster.
    pub fn three_nodes(addrs: [(NodeId, NodeAddr); 3]) -> Self {
        Self {
            nodes: addrs.to_vec(),
        }
    }
}
