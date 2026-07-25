//! Integration tests for Zenoh-based distributed mode.
//!
//! These tests exercise the full Zenoh transport stack:
//! - Zenoh query/reply for graph operations
//! - Zenoh liveliness for node discovery and failure detection
//! - Zenoh pub/sub for ShardMap propagation
//! - Multi-node cluster with cross-node operations
//! - Distributed scatter-gather traversal

#![cfg(feature = "zenoh")]

use nexora_id::NexoraId;
use nexora_zenoh::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use nexora_zenoh::tcp_transport::GraphHandler;

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
impl GraphHandler for TestGraphHandler {
    async fn handle(&self, _target: &str, op: GraphOperation) -> Result<GraphResult, RouterError> {
        match op {
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
        }
    }
}

// ============================================================
// Test 1: Zenoh transport — basic get/set
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_transport_basic_get_set() {
    let handler = Arc::new(TestGraphHandler::new());

    let session = Arc::new(zenoh::open(zenoh::Config::default()).await.unwrap());
    let mut server = zenoh_transport::ZenohGraphServer::new(session.clone(), "zenoh-it-1".into());
    server.start(handler.clone()).await.unwrap();

    let client =
        zenoh_transport::ZenohRemoteClient::new(session).with_timeout(Duration::from_secs(5));

    let qid = NexoraId::from_bytes(b"zenoh-node-1".to_vec());

    // Set property
    let result = client
        .execute(
            "zenoh-it-1",
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "name".into(),
                value: serde_json::json!("Alice"),
            },
        )
        .await
        .unwrap();
    assert!(matches!(result, GraphResult::Status { ok: true, .. }));

    // Get property back
    let result = client
        .execute(
            "zenoh-it-1",
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

    // Verify it was stored in the handler
    let stored = handler.nodes.read().await;
    let val = stored
        .get(qid.as_bytes())
        .and_then(|p| p.get("name"))
        .cloned();
    assert_eq!(val, Some(serde_json::json!("Alice")));
}

// ============================================================
// Test 2: Zenoh transport — get edges
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_transport_get_edges() {
    let handler = Arc::new(TestGraphHandler::new());

    let session = Arc::new(zenoh::open(zenoh::Config::default()).await.unwrap());
    let mut server = zenoh_transport::ZenohGraphServer::new(session.clone(), "zenoh-it-2".into());
    server.start(handler.clone()).await.unwrap();

    let client =
        zenoh_transport::ZenohRemoteClient::new(session).with_timeout(Duration::from_secs(5));

    let source = NexoraId::from_bytes(b"source".to_vec());
    let target = NexoraId::from_bytes(b"target".to_vec());

    // Add edge
    client
        .execute(
            "zenoh-it-2",
            GraphOperation::AddEdge {
                source: source.clone(),
                edge_type: "KNOWS".into(),
                target: target.clone(),
                direction: "out".into(),
            },
        )
        .await
        .unwrap();

    // Query edges
    let result = client
        .execute(
            "zenoh-it-2",
            GraphOperation::GetEdges {
                qid: source.clone(),
                edge_type: Some("KNOWS".into()),
            },
        )
        .await
        .unwrap();

    match result {
        GraphResult::Property(Some(v)) => {
            let arr = v.as_array().expect("expected array");
            assert_eq!(arr.len(), 1);
            let edge = &arr[0];
            assert_eq!(edge["edge_type"], serde_json::json!("KNOWS"));
            assert_eq!(edge["target"], serde_json::json!(target.to_hex()));
        }
        other => panic!("expected Property(Some(array)), got {other:?}"),
    }
}

// ============================================================
// Test 3: Zenoh transport — concurrent operations
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_transport_concurrent_operations() {
    let handler = Arc::new(TestGraphHandler::new());

    let session = Arc::new(zenoh::open(zenoh::Config::default()).await.unwrap());
    let mut server = zenoh_transport::ZenohGraphServer::new(session.clone(), "zenoh-it-3".into());
    server.start(handler.clone()).await.unwrap();

    let client = Arc::new(
        zenoh_transport::ZenohRemoteClient::new(session).with_timeout(Duration::from_secs(10)),
    );

    // Launch 20 concurrent set operations
    let mut handles = Vec::new();
    for i in 0..20 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let qid = NexoraId::from_bytes(format!("conc-{i}").into_bytes());
            c.execute(
                "zenoh-it-3",
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: "index".into(),
                    value: serde_json::json!(i),
                },
            )
            .await
        }));
    }

    // All should succeed
    for handle in handles {
        let result = handle.await.unwrap().unwrap();
        assert!(matches!(result, GraphResult::Status { ok: true, .. }));
    }

    // Verify all stored
    let stored = handler.nodes.read().await;
    assert_eq!(stored.len(), 20);
}

// ============================================================
// Test 4: Zenoh transport — execute cypher
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_transport_execute_cypher() {
    let handler = Arc::new(TestGraphHandler::new());

    let session = Arc::new(zenoh::open(zenoh::Config::default()).await.unwrap());
    let mut server = zenoh_transport::ZenohGraphServer::new(session.clone(), "zenoh-it-4".into());
    server.start(handler).await.unwrap();

    let client =
        zenoh_transport::ZenohRemoteClient::new(session).with_timeout(Duration::from_secs(5));

    let result = client
        .execute(
            "zenoh-it-4",
            GraphOperation::ExecuteCypher {
                query: "MATCH (n) RETURN n".into(),
            },
        )
        .await
        .unwrap();

    match result {
        GraphResult::CypherRows { columns, rows } => {
            assert_eq!(columns, vec!["echo"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!("MATCH (n) RETURN n"));
        }
        other => panic!("expected CypherRows, got {other:?}"),
    }
}

// ============================================================
// Test 5: Zenoh cluster — single node stats
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_cluster_single_node_stats() {
    let config = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-cluster-1".into(),
        total_shards: 8,
        op_timeout: Duration::from_secs(5),
    };

    let mut manager = zenoh_cluster::ZenohClusterManager::new(config)
        .await
        .unwrap();
    manager
        .start(Arc::new(TestGraphHandler::new()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let stats = manager.stats().await;
    assert_eq!(stats.node_id, "zenoh-cluster-1");
    assert!(stats.alive_nodes >= 1);
    assert_eq!(stats.total_shards, 8);
    assert_eq!(stats.local_shards, 8); // Single node owns all

    manager.shutdown().await;
}

// ============================================================
// Test 6: Zenoh cluster — graph operation through router
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_cluster_router_operation() {
    let config = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-cluster-2".into(),
        total_shards: 4,
        op_timeout: Duration::from_secs(5),
    };

    let handler = Arc::new(TestGraphHandler::new());
    let mut manager = zenoh_cluster::ZenohClusterManager::new(config)
        .await
        .unwrap();
    manager.start(handler.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let qid = NexoraId::from_bytes(b"router-test".to_vec());

    // Set property through router
    let result = manager
        .router()
        .route(
            &qid,
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "name".into(),
                value: serde_json::json!("Bob"),
            },
        )
        .await;
    assert!(result.is_ok());

    // Get property through router
    let result = manager
        .router()
        .route(
            &qid,
            GraphOperation::GetProperty {
                qid: qid.clone(),
                key: "name".into(),
            },
        )
        .await
        .unwrap();
    match result {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("Bob")),
        other => panic!("expected Property(Some(\"Bob\")), got {other:?}"),
    }

    manager.shutdown().await;
}

// ============================================================
// Test 7: Zenoh cluster — two nodes cross operations
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_cluster_two_nodes_cross_ops() {
    // Start node A
    let config_a = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-cn-a".into(),
        total_shards: 4,
        op_timeout: Duration::from_secs(5),
    };
    let handler_a = Arc::new(TestGraphHandler::new());
    let mut manager_a = zenoh_cluster::ZenohClusterManager::new(config_a)
        .await
        .unwrap();
    manager_a.start(handler_a.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start node B
    let config_b = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-cn-b".into(),
        total_shards: 4,
        op_timeout: Duration::from_secs(5),
    };
    let handler_b = Arc::new(TestGraphHandler::new());
    let mut manager_b = zenoh_cluster::ZenohClusterManager::new(config_b)
        .await
        .unwrap();
    manager_b.start(handler_b.clone()).await.unwrap();

    // Wait for discovery
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Node A writes to a node
    let qid_a = NexoraId::from_bytes(b"node-a-data".to_vec());
    manager_a
        .router()
        .route(
            &qid_a,
            GraphOperation::SetProperty {
                qid: qid_a.clone(),
                key: "owner".into(),
                value: serde_json::json!("A"),
            },
        )
        .await
        .unwrap();

    // Node A reads it back
    let result = manager_a
        .router()
        .route(
            &qid_a,
            GraphOperation::GetProperty {
                qid: qid_a.clone(),
                key: "owner".into(),
            },
        )
        .await
        .unwrap();
    match result {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("A")),
        other => panic!("expected Property(Some(\"A\")), got {other:?}"),
    }

    // Node B writes to a different node
    let qid_b = NexoraId::from_bytes(b"node-b-data".to_vec());
    manager_b
        .router()
        .route(
            &qid_b,
            GraphOperation::SetProperty {
                qid: qid_b.clone(),
                key: "owner".into(),
                value: serde_json::json!("B"),
            },
        )
        .await
        .unwrap();

    // Node B reads it back
    let result = manager_b
        .router()
        .route(
            &qid_b,
            GraphOperation::GetProperty {
                qid: qid_b.clone(),
                key: "owner".into(),
            },
        )
        .await
        .unwrap();
    match result {
        GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("B")),
        other => panic!("expected Property(Some(\"B\")), got {other:?}"),
    }

    manager_a.shutdown().await;
    manager_b.shutdown().await;
}

// ============================================================
// Test 8: Zenoh cluster — get all properties
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_cluster_get_all_properties() {
    let config = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-gap-1".into(),
        total_shards: 4,
        op_timeout: Duration::from_secs(5),
    };

    let handler = Arc::new(TestGraphHandler::new());
    let mut manager = zenoh_cluster::ZenohClusterManager::new(config)
        .await
        .unwrap();
    manager.start(handler.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let qid = NexoraId::from_bytes(b"multi-prop".to_vec());

    // Set multiple properties
    for (key, val) in [("name", "Alice"), ("age", "30"), ("city", "NYC")] {
        manager
            .router()
            .route(
                &qid,
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: key.into(),
                    value: serde_json::json!(val),
                },
            )
            .await
            .unwrap();
    }

    // Get all properties
    let result = manager
        .router()
        .route(&qid, GraphOperation::GetAllProperties { qid: qid.clone() })
        .await
        .unwrap();

    match result {
        GraphResult::Property(Some(v)) => {
            let obj = v.as_object().expect("expected object");
            assert_eq!(obj.len(), 3);
            assert_eq!(obj["name"], serde_json::json!("Alice"));
            assert_eq!(obj["age"], serde_json::json!("30"));
            assert_eq!(obj["city"], serde_json::json!("NYC"));
        }
        other => panic!("expected Property(Some(object)), got {other:?}"),
    }

    manager.shutdown().await;
}

// ============================================================
// Test 9: Zenoh discovery — node alive and dead detection
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_discovery_alive_dead() {
    let session = Arc::new(zenoh::open(zenoh::Config::default()).await.unwrap());
    let registry = Arc::new(discovery::ClusterRegistry::new());

    // Use unique IDs to avoid interference from parallel tests
    let node_a_id = format!("disc-alive-a-{}", std::process::id());
    let node_b_id = format!("disc-alive-b-{}", std::process::id());

    // Start discovery for node-A
    let mut discovery_a = zenoh_discovery::ZenohDiscovery::new(session.clone(), node_a_id.clone());
    discovery1_start(registry.clone(), &mut discovery_a).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node-A should be alive
    let alive = registry.alive_nodes().await;
    let has_a = alive.iter().any(|n| n.id == node_a_id);
    assert!(has_a, "Node A should be discovered as alive");

    // Start discovery for node-B
    let mut discovery_b = zenoh_discovery::ZenohDiscovery::new(session.clone(), node_b_id.clone());
    discovery1_start(registry.clone(), &mut discovery_b).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both should be alive
    let alive = registry.alive_nodes().await;
    let has_b = alive.iter().any(|n| n.id == node_b_id);
    assert!(has_b, "Node B should be discovered as alive");

    // Stop node-B's discovery (simulate death)
    discovery_b.stop().await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node-B should be dead, Node-A should still be alive
    let alive = registry.alive_nodes().await;
    let a_still_alive = alive.iter().any(|n| n.id == node_a_id);
    let b_dead = !alive.iter().any(|n| n.id == node_b_id);
    assert!(a_still_alive, "Node A should still be alive");
    assert!(b_dead, "Node B should be dead after stop()");

    discovery_a.stop().await;
}

/// Helper: start a ZenohDiscovery with a no-op failover callback.
async fn discovery1_start(
    registry: Arc<discovery::ClusterRegistry>,
    discovery: &mut zenoh_discovery::ZenohDiscovery,
) {
    discovery.start(registry, Arc::new(|_| {})).await.unwrap();
}

// ============================================================
// Test 10: Zenoh scatter-gather — distributed edge traversal
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_scatter_gather_traversal() {
    let config = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-sg-1".into(),
        total_shards: 4,
        op_timeout: Duration::from_secs(5),
    };

    let handler = Arc::new(TestGraphHandler::new());
    let mut manager = zenoh_cluster::ZenohClusterManager::new(config)
        .await
        .unwrap();
    manager.start(handler.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Build a chain: A → B → C → D
    let node_a = NexoraId::from_bytes(b"sg-a".to_vec());
    let node_b = NexoraId::from_bytes(b"sg-b".to_vec());
    let node_c = NexoraId::from_bytes(b"sg-c".to_vec());
    let node_d = NexoraId::from_bytes(b"sg-d".to_vec());

    for (src, tgt) in [
        (node_a.clone(), node_b.clone()),
        (node_b.clone(), node_c.clone()),
        (node_c.clone(), node_d.clone()),
    ] {
        manager
            .router()
            .route(
                &src,
                GraphOperation::AddEdge {
                    source: src.clone(),
                    edge_type: "NEXT".into(),
                    target: tgt.clone(),
                    direction: "out".into(),
                },
            )
            .await
            .unwrap();
    }

    // Traverse from A with max_depth=3
    let results = manager
        .router()
        .scatter_gather_traverse(vec![node_a.clone()], "NEXT", 3)
        .await
        .unwrap();

    // Should find B, C, D (3 nodes, excluding start)
    assert!(
        results.len() >= 3,
        "expected at least 3 results from traversal, got {}",
        results.len()
    );

    manager.shutdown().await;
}

// ============================================================
// Test 11: Zenoh cluster — shard map snapshot
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_cluster_shard_map_snapshot() {
    let config = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-sm-1".into(),
        total_shards: 16,
        op_timeout: Duration::from_secs(5),
    };

    let mut manager = zenoh_cluster::ZenohClusterManager::new(config)
        .await
        .unwrap();
    manager
        .start(Arc::new(TestGraphHandler::new()))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let snapshot = manager.router().shard_map_snapshot().await;
    assert_eq!(snapshot.total_shards, 16);
    assert_eq!(snapshot.assignments.len(), 16);
    assert_eq!(snapshot.local_node, "zenoh-sm-1");

    // All shards should be owned by this node
    for (shard_id, assignment) in &snapshot.assignments {
        assert_eq!(
            assignment.owner, "zenoh-sm-1",
            "shard {shard_id} should be owned by zenoh-sm-1"
        );
    }

    let count = manager.router().local_shard_count().await;
    assert_eq!(count, 16);

    manager.shutdown().await;
}

// ============================================================
// Test 12: Zenoh cluster — property persistence across operations
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_cluster_property_persistence() {
    let config = zenoh_cluster::ZenohClusterConfig {
        node_id: "zenoh-pp-1".into(),
        total_shards: 4,
        op_timeout: Duration::from_secs(5),
    };

    let handler = Arc::new(TestGraphHandler::new());
    let mut manager = zenoh_cluster::ZenohClusterManager::new(config)
        .await
        .unwrap();
    manager.start(handler.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let qid = NexoraId::from_bytes(b"persist-test".to_vec());

    // Set multiple properties on the same node
    let props = [
        ("name", serde_json::json!("Charlie")),
        ("age", serde_json::json!(42)),
        ("active", serde_json::json!(true)),
        ("score", serde_json::json!(2.5)),
    ];

    for (key, val) in &props {
        manager
            .router()
            .route(
                &qid,
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: (*key).into(),
                    value: val.clone(),
                },
            )
            .await
            .unwrap();
    }

    // Read each back individually
    for (key, expected) in &props {
        let result = manager
            .router()
            .route(
                &qid,
                GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: (*key).into(),
                },
            )
            .await
            .unwrap();
        match result {
            GraphResult::Property(Some(v)) => assert_eq!(&v, expected, "mismatch for key {key}"),
            other => panic!("expected Property for key {key}, got {other:?}"),
        }
    }

    manager.shutdown().await;
}
