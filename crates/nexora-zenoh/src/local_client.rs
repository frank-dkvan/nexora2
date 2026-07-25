//! Local mock client — a RemoteGraphClient implementation for testing
//! and single-node deployments. Routes all operations through an
//! in-memory property store.

use crate::{GraphOperation, GraphResult, RemoteGraphClient, RouterError};
use nexora_id::NexoraId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A local in-memory graph store that implements RemoteGraphClient.
/// Useful for testing distributed routing without actual network calls.
type NodeStore = HashMap<Vec<u8>, HashMap<String, serde_json::Value>>;
type EdgeStore = HashMap<Vec<u8>, Vec<(String, String, Vec<u8>)>>;

pub struct LocalGraphClient {
    /// Node properties: qid_bytes -> (key -> value)
    nodes: Arc<RwLock<NodeStore>>,
    /// Node edges: qid_bytes -> Vec<(edge_type, direction, target_bytes)>
    edges: Arc<RwLock<EdgeStore>>,
}

impl Default for LocalGraphClient {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalGraphClient {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Pre-populate a node property.
    pub async fn set_property(&self, qid: &NexoraId, key: &str, value: serde_json::Value) {
        let mut nodes = self.nodes.write().await;
        nodes
            .entry(qid.as_bytes().to_vec())
            .or_default()
            .insert(key.to_string(), value);
    }

    /// Pre-populate an edge.
    pub async fn add_edge(
        &self,
        source: &NexoraId,
        edge_type: &str,
        direction: &str,
        target: &NexoraId,
    ) {
        let mut edges = self.edges.write().await;
        edges.entry(source.as_bytes().to_vec()).or_default().push((
            edge_type.to_string(),
            direction.to_string(),
            target.as_bytes().to_vec(),
        ));
    }

    /// Get a property value.
    pub async fn get_property(&self, qid: &NexoraId, key: &str) -> Option<serde_json::Value> {
        let nodes = self.nodes.read().await;
        nodes
            .get(qid.as_bytes())
            .and_then(|props| props.get(key))
            .cloned()
    }

    /// Get edges of a node.
    pub async fn get_edges(&self, qid: &NexoraId) -> Vec<(String, String, NexoraId)> {
        let edges = self.edges.read().await;
        edges
            .get(qid.as_bytes())
            .map(|v| {
                v.iter()
                    .map(|(et, dir, target)| {
                        (
                            et.clone(),
                            dir.clone(),
                            NexoraId::from_bytes(target.clone()),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl RemoteGraphClient for LocalGraphClient {
    fn execute<'a>(
        &'a self,
        _target_node: &'a str,
        op: GraphOperation,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match op {
                // In-process client (tests / single-node): no failover, so no
                // fence — just unwrap and apply the inner mutation.
                GraphOperation::FencedWrite { inner, .. } => {
                    self.execute(_target_node, *inner).await
                }
                // In-process client has no shard-scoped export; return an empty
                // snapshot so callers get a well-formed (if empty) response.
                GraphOperation::ExportShard { shard_id, .. } => {
                    let snap = crate::migration::ShardSnapshot {
                        shard_id,
                        epoch: crate::shard_map::OwnerEpoch::new(),
                        nodes: vec![],
                        edges: vec![],
                        created_at_ms: 0,
                    };
                    let json = serde_json::to_value(&snap)
                        .map_err(|e| RouterError::Serialization(e.to_string()))?;
                    Ok(GraphResult::Property(Some(json)))
                }
                // In-process client keeps no replication log → report TooOld so a
                // caller falls back to a (empty) full snapshot.
                GraphOperation::ExportDelta { .. } => {
                    let resp = crate::state_transfer::DeltaResponse::TooOld;
                    let json = serde_json::to_value(&resp)
                        .map_err(|e| RouterError::Serialization(e.to_string()))?;
                    Ok(GraphResult::Property(Some(json)))
                }
                // In-process client keeps no replication log → empty digest.
                GraphOperation::ExportDigest { shard_id } => {
                    let digest = crate::anti_entropy::ShardDigest {
                        shard_id: shard_id as u32,
                        seq_range: (0, 0),
                        root_hash: [0u8; 32],
                    };
                    let json = serde_json::to_value(&digest)
                        .map_err(|e| RouterError::Serialization(e.to_string()))?;
                    Ok(GraphResult::Property(Some(json)))
                }
                GraphOperation::GetProperty { qid, key } => {
                    // Special key: _edges_<type> returns edge targets as JSON array
                    if let Some(edge_type) = key.strip_prefix("_edges_") {
                        let edges = self.edges.read().await;
                        let targets: Vec<serde_json::Value> = edges
                            .get(qid.as_bytes())
                            .map(|v| {
                                v.iter()
                                    .filter(|(et, _, _)| et == edge_type)
                                    .map(|(_, _, target)| {
                                        serde_json::Value::String(
                                            NexoraId::from_bytes(target.clone()).to_hex(),
                                        )
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        Ok(GraphResult::Property(Some(serde_json::Value::Array(
                            targets,
                        ))))
                    } else {
                        let nodes = self.nodes.read().await;
                        let val = nodes
                            .get(qid.as_bytes())
                            .and_then(|props| props.get(&key))
                            .cloned();
                        Ok(GraphResult::Property(val))
                    }
                }
                GraphOperation::SetProperty { qid, key, value } => {
                    let mut nodes = self.nodes.write().await;
                    nodes
                        .entry(qid.as_bytes().to_vec())
                        .or_default()
                        .insert(key, value);
                    Ok(GraphResult::Status {
                        ok: true,
                        message: "property set".into(),
                    })
                }
                GraphOperation::AddEdge {
                    source,
                    edge_type,
                    target,
                    direction,
                } => {
                    let mut edges = self.edges.write().await;
                    edges.entry(source.as_bytes().to_vec()).or_default().push((
                        edge_type,
                        direction,
                        target.as_bytes().to_vec(),
                    ));
                    Ok(GraphResult::Status {
                        ok: true,
                        message: "edge added".into(),
                    })
                }
                GraphOperation::ExecuteCypher { query: _ } => {
                    // Local client doesn't support Cypher execution
                    Ok(GraphResult::Status {
                        ok: false,
                        message: "Cypher not supported on local client".into(),
                    })
                }
                GraphOperation::GetEdges { qid, edge_type } => {
                    let edges = self.edges.read().await;
                    let edge_list: Vec<serde_json::Value> = edges
                        .get(qid.as_bytes())
                        .map(|v| {
                            v.iter()
                                .filter(|(et, _, _)| edge_type.as_ref().is_none_or(|t| t == et))
                                .map(|(et, dir, target)| {
                                    serde_json::json!({
                                        "edge_type": et,
                                        "direction": dir,
                                        "target": NexoraId::from_bytes(target.clone()).to_hex(),
                                    })
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    Ok(GraphResult::Property(Some(serde_json::Value::Array(
                        edge_list,
                    ))))
                }
                GraphOperation::GetAllProperties { qid } => {
                    let nodes = self.nodes.read().await;
                    let map: serde_json::Map<String, serde_json::Value> = nodes
                        .get(qid.as_bytes())
                        .map(|props| props.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default();
                    Ok(GraphResult::Property(Some(serde_json::Value::Object(map))))
                }
                GraphOperation::Ping => Ok(GraphResult::Status {
                    ok: true,
                    message: "pong".to_string(),
                }),
                // In-process client (single-node/tests) has no separate event
                // store to scan; return an empty (hex) payload so callers get a
                // well-formed "no rows from this node" response.
                GraphOperation::ScanEventTable { .. } => {
                    Ok(GraphResult::Property(Some(serde_json::Value::String(
                        String::new(),
                    ))))
                }
                // In-process client (single-node/tests): no peers to broadcast
                // to, so applying/removing an ontology is a local no-op ack.
                GraphOperation::ApplyOntology { .. } => Ok(GraphResult::Status {
                    ok: true,
                    message: "ontology applied (local)".to_string(),
                }),
                GraphOperation::RemoveOntology { .. } => Ok(GraphResult::Status {
                    ok: true,
                    message: "ontology removed (local)".to_string(),
                }),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::HybridRouter;

    #[tokio::test]
    async fn test_local_client_get_set_property() {
        let client = Arc::new(LocalGraphClient::new());
        let qid = NexoraId::from_bytes(b"test-node".to_vec());

        // Set property via RemoteGraphClient trait
        client
            .execute(
                "local",
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: "speed".into(),
                    value: serde_json::json!(80),
                },
            )
            .await
            .unwrap();

        // Get property via RemoteGraphClient trait
        let result = client
            .execute(
                "local",
                GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: "speed".into(),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!(80)),
            _ => panic!("expected Property(Some(80))"),
        }
    }

    #[tokio::test]
    async fn test_local_client_add_edge() {
        let client = Arc::new(LocalGraphClient::new());
        let src = NexoraId::from_bytes(b"src".to_vec());
        let dst = NexoraId::from_bytes(b"dst".to_vec());

        client
            .execute(
                "local",
                GraphOperation::AddEdge {
                    source: src.clone(),
                    edge_type: "KNOWS".into(),
                    target: dst.clone(),
                    direction: "out".into(),
                },
            )
            .await
            .unwrap();

        // Query edges via _edges_KNOWS
        let result = client
            .execute(
                "local",
                GraphOperation::GetProperty {
                    qid: src,
                    key: "_edges_KNOWS".into(),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::Property(Some(serde_json::Value::Array(arr))) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], serde_json::Value::String(dst.to_hex()));
            }
            _ => panic!("expected Property with array"),
        }
    }

    #[tokio::test]
    async fn test_router_with_local_client() {
        let client = Arc::new(LocalGraphClient::new());
        let qid = NexoraId::from_bytes(b"routed-node".to_vec());

        // Pre-populate data
        client
            .set_property(&qid, "name", serde_json::json!("Alice"))
            .await;

        // Create a clustered router with the local client
        // Use a shard map where all shards are owned by "remote-node" so
        // the router routes through the RemoteGraphClient
        let mut shard_map = crate::shard_map::ShardMap::new_local(4);
        shard_map.local_node = "this-node".to_string();
        for (_shard_id, assignment) in shard_map.assignments.iter_mut() {
            assignment.owner = "remote-node".to_string();
        }
        let router = HybridRouter::new_clustered(shard_map, client);

        // Route a GetProperty operation
        let result = router
            .route(
                &qid,
                GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: "name".into(),
                },
            )
            .await;

        // Should succeed and return "Alice"
        assert!(result.is_ok());
        match result.unwrap() {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("Alice")),
            other => panic!("expected Property(Some(\"Alice\")), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_scatter_gather_with_edges() {
        let client = Arc::new(LocalGraphClient::new());

        // Build a small graph: A -> B -> C
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());

        client.add_edge(&a, "NEXT", "out", &b).await;
        client.add_edge(&b, "NEXT", "out", &c).await;

        let mut shard_map = crate::shard_map::ShardMap::new_local(4);
        shard_map.local_node = "this-node".to_string();
        for (_shard_id, assignment) in shard_map.assignments.iter_mut() {
            assignment.owner = "remote-node".to_string();
        }
        let router = HybridRouter::new_clustered(shard_map, client);

        // Traverse from A with max_depth=2
        let results: Vec<crate::router::FrontierNode> = router
            .scatter_gather_traverse(vec![a.clone()], "NEXT", 2)
            .await
            .unwrap();

        // Should discover B and C (A is the start, also included in results)
        let discovered: Vec<Vec<u8>> = results.iter().map(|n| n.qid.as_bytes().to_vec()).collect();
        assert!(
            discovered.contains(&b.as_bytes().to_vec()),
            "Should discover B"
        );
        assert!(
            discovered.contains(&c.as_bytes().to_vec()),
            "Should discover C"
        );
    }
}
