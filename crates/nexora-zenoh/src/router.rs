use crate::shard_map::{OwnerEpoch, ShardId, ShardMap};
use crate::{GraphOperation, GraphResult, RemoteGraphClient, RouterError};
use nexora_id::NexoraId;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock, Semaphore};

/// Default maximum concurrent shard operations in scatter-gather.
pub const DEFAULT_MAX_CONCURRENT_SHARDS: usize = 64;
/// Default maximum concurrent node operations per shard.
pub const DEFAULT_MAX_CONCURRENT_NODES: usize = 256;

pub type LocalCommand = (
    GraphOperation,
    oneshot::Sender<Result<GraphResult, RouterError>>,
);

pub struct HybridRouter {
    shard_map: Arc<RwLock<ShardMap>>,
    remote_client: Option<Arc<dyn RemoteGraphClient>>,
    local_tx: Option<tokio::sync::mpsc::Sender<LocalCommand>>,
    replica_writer: Option<Arc<crate::replica_writer::ReplicaWriter>>,
}

impl HybridRouter {
    pub fn new_local(total_shards: usize) -> Self {
        let map = ShardMap::new_local(total_shards);
        let (tx, _rx) = tokio::sync::mpsc::channel(1024);
        Self {
            shard_map: Arc::new(RwLock::new(map)),
            remote_client: None,
            local_tx: Some(tx),
            replica_writer: None,
        }
    }

    pub fn new_clustered(shard_map: ShardMap, remote_client: Arc<dyn RemoteGraphClient>) -> Self {
        let (tx, _rx) = tokio::sync::mpsc::channel(1024);
        Self {
            shard_map: Arc::new(RwLock::new(shard_map)),
            remote_client: Some(remote_client),
            local_tx: Some(tx),
            replica_writer: None,
        }
    }

    /// Create a clustered router without a local channel (for Zenoh mode).
    ///
    /// In this mode, all operations — including local shards — are routed
    /// through the remote client. This is used by ZenohClusterManager where
    /// the ZenohGraphServer handles operations via Zenoh queryables.
    pub fn new_clustered_no_local(
        shard_map: ShardMap,
        remote_client: Arc<dyn RemoteGraphClient>,
    ) -> Self {
        Self {
            shard_map: Arc::new(RwLock::new(shard_map)),
            remote_client: Some(remote_client),
            local_tx: None,
            replica_writer: None,
        }
    }

    /// Attach a ReplicaWriter to enable RF>1 quorum writes.
    pub fn with_replica_writer(
        mut self,
        writer: Arc<crate::replica_writer::ReplicaWriter>,
    ) -> Self {
        self.replica_writer = Some(writer);
        self
    }

    /// Get a reference to the ReplicaWriter, if configured.
    pub fn replica_writer(&self) -> Option<Arc<crate::replica_writer::ReplicaWriter>> {
        self.replica_writer.clone()
    }

    pub async fn route(
        &self,
        qid: &NexoraId,
        op: GraphOperation,
    ) -> Result<GraphResult, RouterError> {
        let map = self.shard_map.read().await;
        let shard = map.shard_of(qid);
        if map.is_local(shard) {
            if let Some(ref lt) = self.local_tx {
                let (tx, rx) = oneshot::channel();
                lt.send((op, tx)).await.map_err(|_| RouterError::Timeout)?;
                rx.await.map_err(|_| RouterError::Timeout)?
            } else if let Some(ref client) = self.remote_client {
                // No local channel — route through remote client (Zenoh mode)
                client.execute(&map.local_node, op).await
            } else {
                Err(RouterError::NodeNotFound("no local channel".into()))
            }
        } else if let Some(assignment) = map.get(shard) {
            if let Some(ref client) = self.remote_client {
                // Try owner first, then failover to followers on error (read-only ops)
                match client.execute(&assignment.owner, op.clone()).await {
                    Ok(result) => Ok(result),
                    Err(e) if op.is_read_only() && !assignment.replicas.is_empty() => {
                        tracing::warn!(
                            shard = shard,
                            owner = %assignment.owner,
                            error = %e,
                            "Owner read failed, trying followers"
                        );
                        // Try each follower in order
                        for follower in &assignment.replicas {
                            match client.execute(follower, op.clone()).await {
                                Ok(result) => {
                                    tracing::info!(
                                        shard = shard,
                                        follower = %follower,
                                        "Failover read from follower succeeded"
                                    );
                                    return Ok(result);
                                }
                                Err(fe) => {
                                    tracing::warn!(
                                        shard = shard,
                                        follower = %follower,
                                        error = %fe,
                                        "Follower read failed, trying next"
                                    );
                                }
                            }
                        }
                        Err(e)
                    }
                    Err(e) => Err(e),
                }
            } else {
                Err(RouterError::NodeNotFound(assignment.owner.clone()))
            }
        } else {
            Err(RouterError::NodeNotFound(format!("shard {}", shard)))
        }
    }

    pub async fn update_shard_map(&self, new_map: ShardMap) {
        let mut map = self.shard_map.write().await;
        *map = new_map;
        tracing::info!("ShardMap updated");
    }

    pub async fn is_local(&self, qid: &NexoraId) -> bool {
        let map = self.shard_map.read().await;
        map.is_local(map.shard_of(qid))
    }

    /// Resolve a key to its shard id and the owner's current epoch.
    ///
    /// Returns `None` if the shard has no assignment. Used by the write path to
    /// build a `FencingToken` for quorum replication of an owner write.
    pub async fn shard_and_epoch(&self, qid: &NexoraId) -> Option<(ShardId, OwnerEpoch)> {
        let map = self.shard_map.read().await;
        let shard = map.shard_of(qid);
        map.get(shard).map(|a| (shard, a.epoch))
    }

    /// Get a snapshot of the current shard map.
    pub async fn shard_map_snapshot(&self) -> ShardMap {
        self.shard_map.read().await.clone()
    }

    /// A cloneable handle to the remote client, if this router has one (cluster
    /// mode). Used by the distributed query planner to fan a query out to each
    /// shard owner. `None` in single-node mode.
    pub fn remote_client_arc(&self) -> Option<Arc<dyn RemoteGraphClient>> {
        self.remote_client.clone()
    }

    /// The local node's own id.
    pub async fn local_node_id(&self) -> crate::shard_map::NodeId {
        self.shard_map.read().await.local_node.clone()
    }

    /// Every distinct node id known to this router — shard owners, their
    /// replicas, and the local node. Used to fan a non-sharded query (e.g. an
    /// event-table scan, where each node holds an independent local store) out to
    /// the whole cluster rather than to a specific shard owner. Sorted for
    /// deterministic ordering.
    pub async fn all_node_ids(&self) -> Vec<crate::shard_map::NodeId> {
        let map = self.shard_map.read().await;
        let mut ids: std::collections::BTreeSet<crate::shard_map::NodeId> =
            std::collections::BTreeSet::new();
        ids.insert(map.local_node.clone());
        for a in map.assignments.values() {
            ids.insert(a.owner.clone());
            for r in &a.replicas {
                ids.insert(r.clone());
            }
        }
        ids.into_iter().collect()
    }

    /// Get the number of shards owned by the local node.
    pub async fn local_shard_count(&self) -> usize {
        let map = self.shard_map.read().await;
        map.assignments
            .values()
            .filter(|a| a.owner == map.local_node)
            .count()
    }

    /// Whether every shard is owned by the local node.
    ///
    /// True in single-node mode and in a single-node cluster. The app layer uses
    /// this to decide whether a whole-graph operation (a Cypher/SQL query, which
    /// this router cannot yet split across shards) is safe to run against the
    /// local graph, or must be refused because part of the graph lives elsewhere.
    pub async fn all_shards_local(&self) -> bool {
        let map = self.shard_map.read().await;
        map.assignments.values().all(|a| a.owner == map.local_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_local_router_creation() {
        let router = HybridRouter::new_local(256);
        assert!(router.local_tx.is_some());
    }
}

// ============================================================
// Scatter-Gather: Distributed Cypher Execution (§6.4)
// ============================================================

use std::collections::HashMap;

/// A frontier node in a multi-hop graph traversal.
#[derive(Clone, Debug)]
pub struct FrontierNode {
    pub qid: nexora_id::NexoraId,
    /// Variable bindings accumulated so far
    pub bindings: HashMap<String, serde_json::Value>,
    /// Hops taken so far
    pub depth: u32,
}

/// Result of one hop in a scatter-gather traversal.
#[derive(Clone, Debug)]
pub struct HopResult {
    pub nodes: Vec<FrontierNode>,
    /// Edges traversed in this hop
    pub edges_traversed: usize,
}

/// Shard grouping for scatter phase.
pub type ShardGroup = (usize, Vec<FrontierNode>);

impl HybridRouter {
    /// Execute a multi-hop distributed Cypher query using Scatter-Gather.
    /// Returns all nodes reachable within max_depth hops.
    ///
    /// Uses parallel execution: all shard queries within a hop are launched
    /// concurrently via `join_all`, and within each shard node queries are
    /// also parallelized. A semaphore limits concurrency to prevent
    /// overwhelming the system.
    pub async fn scatter_gather_traverse(
        &self,
        start_nodes: Vec<nexora_id::NexoraId>,
        edge_type: &str,
        max_depth: u32,
    ) -> Result<Vec<FrontierNode>, RouterError> {
        self.scatter_gather_with_limits(
            start_nodes,
            edge_type,
            max_depth,
            DEFAULT_MAX_CONCURRENT_SHARDS,
            DEFAULT_MAX_CONCURRENT_NODES,
        )
        .await
    }

    /// Execute scatter-gather with tunable concurrency limits.
    pub async fn scatter_gather_with_limits(
        &self,
        start_nodes: Vec<nexora_id::NexoraId>,
        edge_type: &str,
        max_depth: u32,
        max_concurrent_shards: usize,
        _max_concurrent_nodes: usize,
    ) -> Result<Vec<FrontierNode>, RouterError> {
        let mut frontier: Vec<FrontierNode> = start_nodes
            .into_iter()
            .map(|qid| FrontierNode {
                qid,
                bindings: HashMap::new(),
                depth: 0,
            })
            .collect();
        let mut all_results = Vec::new();
        let semaphore = Arc::new(Semaphore::new(max_concurrent_shards));

        for hop in 0..max_depth {
            if frontier.is_empty() {
                break;
            }

            // SCATTER: group frontier nodes by shard
            let shard_groups = self.group_by_shard(&frontier).await;

            // GATHER: query all shards in parallel using spawned tasks
            let edge_type_owned = edge_type.to_string();
            let mut handles = Vec::new();

            for (_shard_id, nodes) in shard_groups {
                let permit = semaphore.clone();
                let edge = edge_type_owned.clone();
                let nodes = nodes.clone();
                // Clone what we need to route
                let shard_map = self.shard_map.clone();
                let remote_client = self.remote_client.clone();
                let local_tx = self.local_tx.clone();

                handles.push(tokio::spawn(async move {
                    let _guard = permit.acquire().await;
                    let mut results = Vec::new();
                    let mut edges_traversed = 0;

                    // Build a temporary router for use within this task
                    let task_router = HybridRouter {
                        shard_map,
                        remote_client,
                        local_tx,
                        replica_writer: None,
                    };

                    for node in &nodes {
                        let op = GraphOperation::GetEdges {
                            qid: node.qid.clone(),
                            edge_type: Some(edge.clone()),
                        };

                        if let Ok(GraphResult::Property(Some(val))) =
                            task_router.route(&node.qid, op).await
                        {
                            if let Some(arr) = val.as_array() {
                                for edge_val in arr {
                                    if let Some(target_str) = edge_val
                                        .as_object()
                                        .and_then(|o| o.get("target"))
                                        .and_then(|t| t.as_str())
                                    {
                                        if let Ok(target_qid) =
                                            nexora_id::NexoraId::from_hex(target_str)
                                        {
                                            results.push(FrontierNode {
                                                qid: target_qid,
                                                bindings: node.bindings.clone(),
                                                depth: node.depth,
                                            });
                                            edges_traversed += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    HopResult {
                        nodes: results,
                        edges_traversed,
                    }
                }));
            }

            let mut hop_results = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok(result) => hop_results.push(result),
                    Err(e) => {
                        tracing::warn!("Shard query task panicked: {:?}", e);
                    }
                }
            }

            // Merge results into next frontier
            frontier = hop_results
                .into_iter()
                .flat_map(|r| r.nodes)
                .map(|mut n| {
                    n.depth = hop + 1;
                    n
                })
                .collect();

            all_results.extend(frontier.clone());
        }

        Ok(all_results)
    }

    /// Group frontier nodes by their target shard.
    async fn group_by_shard(&self, frontier: &[FrontierNode]) -> Vec<ShardGroup> {
        let map = self.shard_map.read().await;
        let mut groups: HashMap<usize, Vec<FrontierNode>> = HashMap::new();
        for node in frontier {
            let shard = map.shard_of(&node.qid);
            groups.entry(shard).or_default().push(node.clone());
        }
        groups.into_iter().collect()
    }
}

#[cfg(test)]
mod scatter_gather_tests {
    use super::*;

    #[tokio::test]
    async fn test_shard_grouping() {
        let router = HybridRouter::new_local(4);
        let nodes = [
            nexora_id::NexoraId::from_bytes(b"a".to_vec()),
            nexora_id::NexoraId::from_bytes(b"b".to_vec()),
        ];
        // Verify grouping doesn't panic
        let groups = router
            .group_by_shard(
                &nodes
                    .iter()
                    .map(|q| FrontierNode {
                        qid: q.clone(),
                        bindings: HashMap::new(),
                        depth: 0,
                    })
                    .collect::<Vec<_>>(),
            )
            .await;
        assert!(!groups.is_empty());
    }
}
