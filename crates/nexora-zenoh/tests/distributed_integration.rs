//! Integration tests for distributed cluster mode.
//!
//! These tests exercise the full TCP transport stack:
//! - Multi-node cluster with real TCP connections
//! - Cross-node graph operations via HybridRouter
//! - Heartbeat-based membership and failure detection
//! - Replica quorum writes
//! - Distributed scatter-gather traversal

use nexora_id::NexoraId;
use nexora_zenoh::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Re-export for convenience
use nexora_zenoh::OwnerEpoch;

/// A test handler that stores properties in memory, simulating a real GraphService.
type NodeStore = Arc<
    RwLock<
        std::collections::HashMap<Vec<u8>, std::collections::HashMap<String, serde_json::Value>>,
    >,
>;
type EdgeStore = Arc<RwLock<std::collections::HashMap<Vec<u8>, Vec<(String, String, Vec<u8>)>>>>;

struct TestGraphHandler {
    nodes: NodeStore,
    edges: EdgeStore,
}

impl TestGraphHandler {
    fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(std::collections::HashMap::new())),
            edges: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl tcp_transport::GraphHandler for TestGraphHandler {
    async fn handle(&self, _target: &str, op: GraphOperation) -> Result<GraphResult, RouterError> {
        match op {
            // Test handler has no fence: unwrap and apply the inner mutation.
            GraphOperation::FencedWrite { inner, .. } => self.handle(_target, *inner).await,
            GraphOperation::GetProperty { qid, key } => {
                if let Some(et) = key.strip_prefix("_edges_") {
                    let edges = self.edges.read().await;
                    let targets: Vec<serde_json::Value> = edges
                        .get(qid.as_bytes())
                        .map(|v| {
                            v.iter()
                                .filter(|(t, _, _)| t == et)
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
                    let val = nodes.get(qid.as_bytes()).and_then(|p| p.get(&key)).cloned();
                    Ok(GraphResult::Property(val))
                }
            }
            GraphOperation::SetProperty { qid, key, value } => {
                self.nodes
                    .write()
                    .await
                    .entry(qid.as_bytes().to_vec())
                    .or_default()
                    .insert(key, value);
                Ok(GraphResult::Status {
                    ok: true,
                    message: "ok".into(),
                })
            }
            GraphOperation::AddEdge {
                source,
                edge_type,
                target,
                direction,
            } => {
                self.edges
                    .write()
                    .await
                    .entry(source.as_bytes().to_vec())
                    .or_default()
                    .push((edge_type, direction, target.as_bytes().to_vec()));
                Ok(GraphResult::Status {
                    ok: true,
                    message: "ok".into(),
                })
            }
            GraphOperation::GetEdges { qid, edge_type } => {
                let edges = self.edges.read().await;
                let list: Vec<serde_json::Value> = edges
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
                Ok(GraphResult::Property(Some(serde_json::Value::Array(list))))
            }
            GraphOperation::GetAllProperties { qid } => {
                let nodes = self.nodes.read().await;
                let map: serde_json::Map<String, serde_json::Value> = nodes
                    .get(qid.as_bytes())
                    .map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default();
                Ok(GraphResult::Property(Some(serde_json::Value::Object(map))))
            }
            GraphOperation::ExecuteCypher { query } => Ok(GraphResult::CypherRows {
                columns: vec!["echo".into()],
                rows: vec![vec![serde_json::json!(query)]],
            }),
            GraphOperation::ExportShard { .. } => {
                Ok(GraphResult::Property(Some(serde_json::json!(null))))
            }
            GraphOperation::ExportDelta { .. } => {
                Ok(GraphResult::Property(Some(serde_json::json!(null))))
            }
            GraphOperation::ExportDigest { .. } => {
                Ok(GraphResult::Property(Some(serde_json::json!(null))))
            }
            GraphOperation::Ping => Ok(GraphResult::Status {
                ok: true,
                message: "pong".into(),
            }),
        }
    }
}

// ============================================================
// Test 1: TCP transport — basic get/set over network
// ============================================================

#[tokio::test]
async fn test_tcp_transport_basic_operations() {
    let handler = Arc::new(TestGraphHandler::new());
    let server = TcpGraphServer::new(handler.clone(), "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let client = TcpRemoteClient::new();
    client.register_node("node-1", &addr).await;

    let qid = NexoraId::from_bytes(b"test-node".to_vec());

    // Set property
    let result = client
        .execute(
            "node-1",
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "name".into(),
                value: serde_json::json!("Alice"),
            },
        )
        .await
        .unwrap();
    assert!(matches!(result, GraphResult::Status { ok: true, .. }));

    // Get property
    let result = client
        .execute(
            "node-1",
            GraphOperation::GetProperty {
                qid: qid.clone(),
                key: "name".into(),
            },
        )
        .await
        .unwrap();
    match result {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("Alice")),
        other => panic!("expected Property(Some(\"Alice\")), got {other:?}"),
    }

    // Verify it was actually stored in the handler
    let stored = handler.nodes.read().await;
    let val = stored
        .get(qid.as_bytes())
        .and_then(|p| p.get("name"))
        .cloned();
    assert_eq!(val, Some(serde_json::json!("Alice")));
}

// ============================================================
// Test 2: HybridRouter routes to remote node correctly
// ============================================================

#[tokio::test]
async fn test_hybrid_router_remote_routing() {
    // Set up a remote node with data
    let handler = Arc::new(TestGraphHandler::new());
    let server = TcpGraphServer::new(handler.clone(), "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("remote-node", &addr).await;

    // Pre-populate data on the remote node
    let qid = NexoraId::from_bytes(b"remote-data".to_vec());
    handler
        .nodes
        .write()
        .await
        .entry(qid.as_bytes().to_vec())
        .or_default()
        .insert("value".into(), serde_json::json!(42));

    // Create a clustered router where all shards are on "remote-node"
    let mut shard_map = shard_map::ShardMap::new_local(4);
    shard_map.local_node = "this-node".to_string();
    for a in shard_map.assignments.values_mut() {
        a.owner = "remote-node".to_string();
    }
    let router = router::HybridRouter::new_clustered(shard_map, client);

    // Route a GetProperty — should go to remote node via TCP
    let result = router
        .route(
            &qid,
            GraphOperation::GetProperty {
                qid: qid.clone(),
                key: "value".into(),
            },
        )
        .await
        .unwrap();

    match result {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!(42)),
        other => panic!("expected Property(Some(42)), got {other:?}"),
    }
}

// ============================================================
// Test 3: Two-node cluster with cross-node operations
// ============================================================

#[tokio::test]
async fn test_two_node_cluster_cross_operations() {
    // Node A: owns shards 0 and 1
    let handler_a = Arc::new(TestGraphHandler::new());
    let server_a = TcpGraphServer::new(handler_a.clone(), "127.0.0.1:0".to_string());
    let addr_a = server_a.start().await.unwrap();

    // Node B: owns shards 2 and 3
    let handler_b = Arc::new(TestGraphHandler::new());
    let server_b = TcpGraphServer::new(handler_b.clone(), "127.0.0.1:0".to_string());
    let addr_b = server_b.start().await.unwrap();

    // Create a client that knows both nodes
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;

    // Build a shard map where shards 0,1 → node-a and 2,3 → node-b
    let mut shard_map = shard_map::ShardMap::new_local(4);
    shard_map.local_node = "this-node".to_string();
    shard_map.assignments.get_mut(&0).unwrap().owner = "node-a".to_string();
    shard_map.assignments.get_mut(&1).unwrap().owner = "node-a".to_string();
    shard_map.assignments.get_mut(&2).unwrap().owner = "node-b".to_string();
    shard_map.assignments.get_mut(&3).unwrap().owner = "node-b".to_string();

    let router = router::HybridRouter::new_clustered(shard_map, client);

    // Write data to a node — we'll verify routing based on actual shard assignment
    let qid_a = NexoraId::from_bytes(b"node-a-data".to_vec());

    // Determine which shard each QID belongs to
    let total_shards = 4;
    let shard_of_a = (qid_a.shard_key() as usize) % total_shards;
    let qid_b = NexoraId::from_bytes(b"node-b-data".to_vec());
    let shard_of_b = (qid_b.shard_key() as usize) % total_shards;

    // Write data to node A
    router
        .route(
            &qid_a,
            GraphOperation::SetProperty {
                qid: qid_a.clone(),
                key: "location".into(),
                value: serde_json::json!("node-a"),
            },
        )
        .await
        .unwrap();

    // Write data to node B
    router
        .route(
            &qid_b,
            GraphOperation::SetProperty {
                qid: qid_b.clone(),
                key: "location".into(),
                value: serde_json::json!("node-b"),
            },
        )
        .await
        .unwrap();

    // Read back from node-a
    let result_a = router
        .route(
            &qid_a,
            GraphOperation::GetProperty {
                qid: qid_a.clone(),
                key: "location".into(),
            },
        )
        .await
        .unwrap();
    match result_a {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("node-a")),
        other => panic!("expected node-a, got {other:?}"),
    }

    // Read back from node-b
    let result_b = router
        .route(
            &qid_b,
            GraphOperation::GetProperty {
                qid: qid_b.clone(),
                key: "location".into(),
            },
        )
        .await
        .unwrap();
    match result_b {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("node-b")),
        other => panic!("expected node-b, got {other:?}"),
    }

    // Verify data ended up on the correct physical nodes based on shard routing
    let stored_a = handler_a.nodes.read().await;
    let stored_b = handler_b.nodes.read().await;

    // qid_a should be on the node that owns its shard
    let owner_of_a = if shard_of_a < 2 { "node-a" } else { "node-b" };
    let owner_of_b = if shard_of_b < 2 { "node-a" } else { "node-b" };

    if owner_of_a == "node-a" {
        assert!(
            stored_a.contains_key(qid_a.as_bytes()),
            "qid_a should be on node-a"
        );
    } else {
        assert!(
            stored_b.contains_key(qid_a.as_bytes()),
            "qid_a should be on node-b"
        );
    }
    if owner_of_b == "node-a" {
        assert!(
            stored_a.contains_key(qid_b.as_bytes()),
            "qid_b should be on node-a"
        );
    } else {
        assert!(
            stored_b.contains_key(qid_b.as_bytes()),
            "qid_b should be on node-b"
        );
    }
}

// ============================================================
// Test 4: Scatter-gather traversal across remote nodes
// ============================================================

#[tokio::test]
async fn test_scatter_gather_distributed_traversal() {
    let handler = Arc::new(TestGraphHandler::new());
    let server = TcpGraphServer::new(handler.clone(), "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("remote", &addr).await;

    // Build graph: A → B → C → D (chain via "NEXT" edges)
    let a = NexoraId::from_bytes(b"A".to_vec());
    let b = NexoraId::from_bytes(b"B".to_vec());
    let c = NexoraId::from_bytes(b"C".to_vec());
    let d = NexoraId::from_bytes(b"D".to_vec());

    // Add edges via the handler directly
    {
        let mut edges = handler.edges.write().await;
        edges.entry(a.as_bytes().to_vec()).or_default().push((
            "NEXT".into(),
            "out".into(),
            b.as_bytes().to_vec(),
        ));
        edges.entry(b.as_bytes().to_vec()).or_default().push((
            "NEXT".into(),
            "out".into(),
            c.as_bytes().to_vec(),
        ));
        edges.entry(c.as_bytes().to_vec()).or_default().push((
            "NEXT".into(),
            "out".into(),
            d.as_bytes().to_vec(),
        ));
    }

    // All shards on remote node
    let mut shard_map = shard_map::ShardMap::new_local(4);
    shard_map.local_node = "local".to_string();
    for a in shard_map.assignments.values_mut() {
        a.owner = "remote".to_string();
    }
    let router = router::HybridRouter::new_clustered(shard_map, client);

    // Traverse from A with max_depth=3
    let results = router
        .scatter_gather_traverse(vec![a.clone()], "NEXT", 3)
        .await
        .unwrap();

    // Should discover B, C, and D
    let discovered: Vec<Vec<u8>> = results.iter().map(|n| n.qid.as_bytes().to_vec()).collect();
    assert!(
        discovered.contains(&b.as_bytes().to_vec()),
        "Should discover B"
    );
    assert!(
        discovered.contains(&c.as_bytes().to_vec()),
        "Should discover C"
    );
    assert!(
        discovered.contains(&d.as_bytes().to_vec()),
        "Should discover D"
    );
}

// ============================================================
// Test 5: Replica quorum write — 3-node replica set
// ============================================================

#[tokio::test]
async fn test_replica_quorum_write_3_nodes() {
    // Set up 3 handler nodes
    let h1 = Arc::new(TestGraphHandler::new());
    let h2 = Arc::new(TestGraphHandler::new());
    let h3 = Arc::new(TestGraphHandler::new());

    let s1 = TcpGraphServer::new(h1.clone(), "127.0.0.1:0".to_string());
    let s2 = TcpGraphServer::new(h2.clone(), "127.0.0.1:0".to_string());
    let s3 = TcpGraphServer::new(h3.clone(), "127.0.0.1:0".to_string());

    let addr1 = s1.start().await.unwrap();
    let addr2 = s2.start().await.unwrap();
    let addr3 = s3.start().await.unwrap();

    // Create client with all 3 nodes registered
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("owner", &addr1).await;
    client.register_node("follower-1", &addr2).await;
    client.register_node("follower-2", &addr3).await;

    let writer = ReplicaWriter::new(client);
    writer.register_replica_set(replication::ReplicaSet::new(
        0,
        "owner".into(),
        vec!["follower-1".into(), "follower-2".into()],
    ));

    let qid = NexoraId::from_bytes(b"replicated".to_vec());
    let token = replication::FencingToken::new(0, OwnerEpoch::new());

    // First, write to the owner (simulating local write that already succeeded)
    h1.nodes
        .write()
        .await
        .entry(qid.as_bytes().to_vec())
        .or_default()
        .insert("data".into(), serde_json::json!("replicated-value"));

    // Now replicate to followers via quorum write
    let status = writer
        .quorum_write(
            0,
            &token,
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "data".into(),
                value: serde_json::json!("replicated-value"),
            },
        )
        .await
        .unwrap();

    match status {
        replication::WriteStatus::CommittedQuorum { acked, total } => {
            assert_eq!(total, 3);
            assert!(
                acked >= 2,
                "should have at least 2 acks (owner + 1 follower)"
            );
        }
        other => panic!("expected CommittedQuorum, got {other:?}"),
    }

    // Verify data was replicated to all nodes
    let v1 = h1
        .nodes
        .read()
        .await
        .get(qid.as_bytes())
        .and_then(|p| p.get("data"))
        .cloned();
    let v2 = h2
        .nodes
        .read()
        .await
        .get(qid.as_bytes())
        .and_then(|p| p.get("data"))
        .cloned();
    let v3 = h3
        .nodes
        .read()
        .await
        .get(qid.as_bytes())
        .and_then(|p| p.get("data"))
        .cloned();

    assert_eq!(v1, Some(serde_json::json!("replicated-value")));
    assert_eq!(v2, Some(serde_json::json!("replicated-value")));
    assert_eq!(v3, Some(serde_json::json!("replicated-value")));
}

// ============================================================
// Test 6: Replica write failure — quorum not reached
// ============================================================

#[tokio::test]
async fn test_replica_quorum_write_failure() {
    // Only 1 follower reachable (not enough for quorum of 3)
    let h1 = Arc::new(TestGraphHandler::new());
    let s1 = TcpGraphServer::new(h1.clone(), "127.0.0.1:0".to_string());
    let addr1 = s1.start().await.unwrap();

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("owner", &addr1).await;
    // follower-1 and follower-2 are NOT registered (unreachable)

    let writer = ReplicaWriter::new(client).with_timeout(Duration::from_millis(200));

    writer.register_replica_set(replication::ReplicaSet::new(
        0,
        "owner".into(),
        vec!["follower-1".into(), "follower-2".into()],
    ));

    let qid = NexoraId::from_bytes(b"fail-test".to_vec());
    let token = replication::FencingToken::new(0, OwnerEpoch::new());

    let result = writer
        .quorum_write(
            0,
            &token,
            GraphOperation::SetProperty {
                qid,
                key: "x".into(),
                value: serde_json::json!(1),
            },
        )
        .await;

    // Only owner acked (1), but quorum requires 2. Quorum failure now surfaces
    // as Err(RouterError::QuorumFailed { acked, required }) rather than Ok(Failed).
    match result {
        Err(RouterError::QuorumFailed { acked, required }) => {
            assert_eq!(acked, 1);
            assert_eq!(required, 2);
        }
        other => panic!("expected Err(QuorumFailed), got {other:?}"),
    }
}

// ============================================================
// Test 7: ShardMap failover increments epoch
// ============================================================

#[tokio::test]
async fn test_shard_failover() {
    use control::ControlPlane;

    let cp = ControlPlane::new(
        4,
        vec!["voter-1".into(), "voter-2".into(), "voter-3".into()],
    );

    // Failover now fails closed unless a voter majority is reachable, so record
    // voter liveness first (a healthy majority is the precondition for failover).
    cp.mark_node_alive("voter-1").await;
    cp.mark_node_alive("voter-2").await;

    // Initial state: all shards owned by "local-node"
    let map = cp.get_shard_map().await;
    assert_eq!(map.assignments.get(&0).unwrap().owner, "local-node");
    assert_eq!(map.assignments.get(&0).unwrap().epoch.value(), 1);

    // Failover shard 0 to "new-owner"
    let token = cp.failover_shard(0, "new-owner".into()).await.unwrap();
    assert_eq!(token.epoch.value(), 2);
    assert_eq!(token.shard_id, 0);

    // Verify in the map
    let map = cp.get_shard_map().await;
    assert_eq!(map.assignments.get(&0).unwrap().owner, "new-owner");
    assert_eq!(map.assignments.get(&0).unwrap().epoch.value(), 2);

    // Other shards should be unchanged
    assert_eq!(map.assignments.get(&1).unwrap().owner, "local-node");
    assert_eq!(map.assignments.get(&1).unwrap().epoch.value(), 1);
}

// ============================================================
// Test 8: ControlPlane mark_node_failed returns affected shards
// ============================================================

#[tokio::test]
async fn test_mark_node_failed_returns_shards() {
    use control::ControlPlane;

    let cp = ControlPlane::new(4, vec!["voter-1".into()]);

    // Single voter must be recorded alive to form a quorum before failover.
    cp.mark_node_alive("voter-1").await;

    // Failover shard 1 to "node-2"
    cp.failover_shard(1, "node-2".into()).await.unwrap();

    // Mark node-2 as failed
    let failed_shards = cp.mark_node_failed("node-2").await;

    // Should return shard 1 (owned by node-2)
    assert!(failed_shards.contains(&1));
    // Should NOT return shard 0 (still owned by local-node)
    assert!(!failed_shards.contains(&0));
}

// ============================================================
// Test 9: ControlPlane rebalance redistributes shards
// ============================================================

#[tokio::test]
async fn test_rebalance_shards() {
    use control::ControlPlane;

    let cp = ControlPlane::new(8, vec![]);

    // Initially all shards owned by "local-node"
    let map = cp.get_shard_map().await;
    for a in map.assignments.values() {
        assert_eq!(a.owner, "local-node");
    }

    // Rebalance across 3 nodes
    let new_map = cp
        .rebalance_shards(&["node-a".into(), "node-b".into(), "node-c".into()])
        .await
        .unwrap();

    // Shards should be distributed round-robin
    let owners: Vec<&str> = (0..8)
        .map(|i| new_map.assignments.get(&i).unwrap().owner.as_str())
        .collect();

    assert_eq!(owners[0], "node-a"); // 0 % 3 = 0
    assert_eq!(owners[1], "node-b"); // 1 % 3 = 1
    assert_eq!(owners[2], "node-c"); // 2 % 3 = 2
    assert_eq!(owners[3], "node-a"); // 3 % 3 = 0

    // Version should have incremented
    assert!(new_map.version > 1);
}

// ============================================================
// Test 10: ClusterManager single-node startup and stats
// ============================================================

#[tokio::test]
async fn test_cluster_manager_single_node_stats() {
    let handler = Arc::new(TestGraphHandler::new());
    let config = cluster::ClusterConfig {
        node_id: "solo-node".into(),
        listen_addr: "127.0.0.1:0".into(),
        heartbeat_addr: "127.0.0.1:0".into(),
        total_shards: 8,
        peers: vec![],
        heartbeat_interval: Duration::from_secs(2),
        failure_timeout: Duration::from_secs(10),
        replication_factor: 1,
        replication_log_dir: None,
        shard_map_dir: None,
        anti_entropy_interval: None,
    };
    let mut cm = ClusterManager::new(config);
    cm.start(handler).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let stats = cm.stats().await;
    assert_eq!(stats.node_id, "solo-node");
    assert_eq!(stats.total_shards, 8);
    assert!(stats.alive_nodes >= 1);
    assert!(stats.local_shards > 0);

    cm.shutdown();
}

// ============================================================
// Test 11: Concurrent operations through TCP transport
// ============================================================

#[tokio::test]
async fn test_concurrent_operations_through_tcp() {
    let handler = Arc::new(TestGraphHandler::new());
    let server = TcpGraphServer::new(handler.clone(), "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-1", &addr).await;

    // Spawn 20 concurrent set operations
    let mut handles = Vec::new();
    for i in 0..20 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let qid = NexoraId::from_bytes(format!("n{i}").into_bytes());
            c.execute(
                "node-1",
                GraphOperation::SetProperty {
                    qid,
                    key: "index".into(),
                    value: serde_json::json!(i),
                },
            )
            .await
            .unwrap()
        }));
    }

    let results = futures::future::join_all(handles).await;
    for r in results {
        assert!(matches!(r.unwrap(), GraphResult::Status { ok: true, .. }));
    }

    // Verify all 20 nodes were stored
    let nodes = handler.nodes.read().await;
    assert_eq!(nodes.len(), 20);
}

// ============================================================
// Test 12: FencingToken rejects stale epoch writes
// ============================================================

#[tokio::test]
async fn test_fencing_token_stale_rejection() {
    use replication::FencingToken;
    let current_epoch = OwnerEpoch::new().next(); // epoch = 2
    let token = FencingToken::new(0, current_epoch);

    // Same epoch should NOT be allowed (allows_write checks strictly >)
    assert!(!token.allows_write(OwnerEpoch::new().next()));

    // Higher epoch should be allowed
    assert!(token.allows_write(OwnerEpoch::new().next().next()));

    // Lower epoch should NOT be allowed
    assert!(!token.allows_write(OwnerEpoch::new()));
}

// ============================================================
// Test 13: GetEdges operation over TCP
// ============================================================

#[tokio::test]
async fn test_get_edges_over_tcp() {
    let handler = Arc::new(TestGraphHandler::new());
    let server = TcpGraphServer::new(handler.clone(), "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let client = TcpRemoteClient::new();
    client.register_node("node-1", &addr).await;

    let src = NexoraId::from_bytes(b"src".to_vec());
    let dst = NexoraId::from_bytes(b"dst".to_vec());

    // Add edge
    client
        .execute(
            "node-1",
            GraphOperation::AddEdge {
                source: src.clone(),
                edge_type: "KNOWS".into(),
                target: dst.clone(),
                direction: "out".into(),
            },
        )
        .await
        .unwrap();

    // Get edges
    let result = client
        .execute(
            "node-1",
            GraphOperation::GetEdges {
                qid: src,
                edge_type: Some("KNOWS".into()),
            },
        )
        .await
        .unwrap();

    match result {
        GraphResult::Property(Some(serde_json::Value::Array(arr))) => {
            assert_eq!(arr.len(), 1);
            let edge = &arr[0];
            assert_eq!(edge["edge_type"], serde_json::json!("KNOWS"));
            assert_eq!(edge["direction"], serde_json::json!("out"));
            assert_eq!(edge["target"], serde_json::json!(dst.to_hex()));
        }
        other => panic!("expected Property with array, got {other:?}"),
    }
}

// ============================================================
// Test 14: Connection pool reuse for multiple requests
// ============================================================

#[tokio::test]
async fn test_connection_pool_reuse() {
    let handler = Arc::new(TestGraphHandler::new());
    let server = TcpGraphServer::new(handler, "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let client = TcpRemoteClient::new();
    client.register_node("node-1", &addr).await;

    let qid = NexoraId::from_bytes(b"pool-test".to_vec());

    // Make 10 sequential requests — should reuse connections
    for i in 0..10 {
        let result = client
            .execute(
                "node-1",
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: format!("k{i}"),
                    value: serde_json::json!(i),
                },
            )
            .await
            .unwrap();
        assert!(matches!(result, GraphResult::Status { ok: true, .. }));
    }

    // Verify all properties were set
    let result = client
        .execute("node-1", GraphOperation::GetAllProperties { qid })
        .await
        .unwrap();

    match result {
        GraphResult::Property(Some(serde_json::Value::Object(map))) => {
            assert_eq!(map.len(), 10);
            for i in 0..10 {
                assert!(map.contains_key(&format!("k{i}")));
            }
        }
        other => panic!("expected Property with object, got {other:?}"),
    }
}
