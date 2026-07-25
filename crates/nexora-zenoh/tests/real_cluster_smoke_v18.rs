//! V18 distributed MERGE test.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use std::sync::Arc;
use std::time::Duration;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

struct Node {
    #[allow(dead_code)]
    graph: Arc<GraphService>,
    manager: ClusterManager,
}

async fn start_cluster(n: usize, total_shards: usize, rf: usize) -> Vec<Node> {
    let ids: Vec<String> = (0..n).map(|i| format!("node-{i}")).collect();
    let graph_addrs: Vec<String> = (0..n)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();
    let hb_addrs: Vec<String> = (0..n)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();

    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let peers: Vec<PeerConfig> = (0..n)
            .filter(|&j| j != i)
            .map(|j| PeerConfig {
                node_id: ids[j].clone(),
                graph_addr: graph_addrs[j].clone(),
                heartbeat_addr: hb_addrs[j].clone(),
            })
            .collect();

        let config = ClusterConfig {
            node_id: ids[i].clone(),
            listen_addr: graph_addrs[i].clone(),
            heartbeat_addr: hb_addrs[i].clone(),
            total_shards,
            peers,
            heartbeat_interval: Duration::from_millis(200),
            failure_timeout: Duration::from_secs(5),
            replication_factor: rf,
            replication_log_dir: None,
            shard_map_dir: None,
            anti_entropy_interval: None,
        };

        let graph = Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 1000,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ));
        let mut manager = ClusterManager::new(config);
        let adapter = Arc::new(GraphServiceAdapter::with_fence_and_log(
            graph.clone(),
            manager.fence(),
            manager.replication_log(),
        ));
        manager.start(adapter).await.unwrap();

        nodes.push(Node { graph, manager });
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    nodes
}

/// V18 — distributed MERGE across the real cluster: two-phase match+create.
///
/// MERGE first scatter-gathers a MATCH across all owners to find existing nodes.
/// If no match is found, the coordinator creates the node on the qid's target
/// owner and applies ON CREATE SET clauses. If a match is found, ON MATCH SET
/// clauses are applied to the matched nodes. Verified on the real cluster to
/// ensure no duplicate creation across owners.
#[tokio::test]
async fn v18_distributed_merge_across_owners() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    // Helper: plan + execute a distributed MERGE against the started cluster.
    async fn run_merge(
        router: &std::sync::Arc<nexora_zenoh::router::HybridRouter>,
        q: &str,
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let plan = nexora_zenoh::distributed_query::plan(q)
            .unwrap_or_else(|| panic!("query must be distributable: {q}"));
        nexora_zenoh::distributed_query::execute(router, &plan, None, None, None)
            .await
            .unwrap_or_else(|e| panic!("distributed MERGE failed ({q}): {e}"))
    }

    // Test 1: MERGE when no node exists → CREATE branch with ON CREATE SET.
    let (cols, rows) = run_merge(
        &router0,
        "MERGE (n:Person {id: 1}) ON CREATE SET n.created = true",
    )
    .await;
    assert_eq!(cols[0], "nodes_created");
    assert_eq!(rows[0][0].as_i64(), Some(1), "MERGE created 1 node");
    let props_set_idx = cols.iter().position(|c| c == "properties_set").unwrap();
    // CREATE sets 2 properties: id (from pattern) + created (from ON CREATE SET).
    assert_eq!(
        rows[0][props_set_idx].as_i64(),
        Some(2),
        "CREATE pattern + ON CREATE SET applied 2 properties (id + created)"
    );

    // Verify the node physically landed on the correct owner and has the property.
    // We can't predict which owner without the qid, but a count query should find it.
    let (_, rows) = run_merge(&router0, "MATCH (n:Person {id: 1}) RETURN n.created").await;
    assert_eq!(rows.len(), 1, "MERGE-created node must exist");
    assert_eq!(rows[0][0], serde_json::json!(true), "ON CREATE SET applied");

    // Test 2: MERGE when node exists → ON MATCH SET branch (no creation).
    let (cols, rows) = run_merge(
        &router0,
        "MERGE (n:Person {id: 1}) ON MATCH SET n.matched = true",
    )
    .await;
    assert_eq!(
        rows[0][0].as_i64(),
        Some(0),
        "MERGE did not create (node already exists)"
    );
    let props_set_idx = cols.iter().position(|c| c == "properties_set").unwrap();
    assert_eq!(
        rows[0][props_set_idx].as_i64(),
        Some(1),
        "ON MATCH SET applied 1 property"
    );

    // Verify the ON MATCH SET was applied.
    let (_, rows) = run_merge(
        &router0,
        "MATCH (n:Person {id: 1}) RETURN n.matched, n.created",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], serde_json::json!(true), "ON MATCH SET applied");
    assert_eq!(
        rows[0][1],
        serde_json::json!(true),
        "ON CREATE SET still present"
    );

    // Test 3: MERGE multiple nodes across owners (ensure no duplicate creation).
    // MERGE twice with the same pattern: first creates, second matches.
    let (_, rows) = run_merge(&router0, "MERGE (n:Robot {id: 2})").await;
    assert_eq!(rows[0][0].as_i64(), Some(1), "first MERGE created 1 node");
    let (_, rows) = run_merge(&router0, "MERGE (n:Robot {id: 2})").await;
    assert_eq!(
        rows[0][0].as_i64(),
        Some(0),
        "second MERGE did not create (matched)"
    );

    // Verify only one Robot with id=2 exists (no duplicate creation across owners).
    let (_, rows) = run_merge(&router0, "MATCH (n:Robot {id: 2}) RETURN count(*)").await;
    assert_eq!(
        rows[0][0].as_i64(),
        Some(1),
        "MERGE must not duplicate nodes across owners"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}
