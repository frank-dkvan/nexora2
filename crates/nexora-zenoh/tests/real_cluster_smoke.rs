//! Real multi-process-style cluster smoke test.
//!
//! Unlike the other distributed tests, which build a `HybridRouter` directly and
//! bypass startup, THIS test drives the real `ClusterManager::start()` path end
//! to end: each node binds its own TCP graph + heartbeat servers, peers with the
//! others, and runs the heartbeat / failure-detector loops. It then verifies the
//! three completed work lines survive the real startup path:
//!   - P0  cross-node write/read routing
//!   - P1-A quorum replication (data on followers)
//!   - P1-B distributed scatter-gather traversal
//!
//! WHY THIS EXISTS: two startup-path bugs (shard-map clobber, dead local channel)
//! passed every router-level test because those tests skipped `start()`. This is
//! the system-level guard that exercises the path where those bugs lived.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use nexora_zenoh::{GraphOperation, GraphResult};
use std::sync::Arc;
use std::time::Duration;

/// Reserve a free localhost port by binding :0, reading the port, and dropping
/// the listener. There is a small TOCTOU window before the server rebinds it,
/// but for a local test this is the standard way to pre-assign a fixed address
/// that peers can be told about before `start()` runs.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

/// A node under test: its real graph (to assert physical data placement) and
/// the started manager. Node ids are tracked separately in the `ids` vec.
struct Node {
    graph: Arc<GraphService>,
    manager: ClusterManager,
}

/// Build and start `n` real cluster nodes, fully peered with each other, with
/// the given replication factor. Returns the started nodes.
async fn start_cluster(n: usize, total_shards: usize, rf: usize) -> Vec<Node> {
    // Pre-assign fixed addresses so every node can be told about every peer
    // before start() (peers must be known at `new()` for the initial ShardMap).
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
    // Let heartbeats settle so peers are registered/alive.
    tokio::time::sleep(Duration::from_millis(500)).await;
    nodes
}

/// Find a key whose shard is owned by node index `want` under the running map.
async fn key_owned_by(node: &Node, ids: &[String], want: usize, total: usize) -> NexoraId {
    let map = node.manager.router_arc().shard_map_snapshot().await;
    let want_id = &ids[want];
    for i in 0..100_000u64 {
        let qid = NexoraId::from_bytes(format!("probe-{i}").into_bytes());
        let shard = qid.shard_key() as usize % total;
        if map.get(shard).map(|a| &a.owner) == Some(want_id) {
            return qid;
        }
    }
    panic!("no key found owned by {want_id}");
}

/// V1 — P0 cross-node routing through the REAL start() path.
///
/// A write routed on node-0 for a key owned by node-1 must physically land on
/// node-1's graph, and be readable back through node-0's router — all over the
/// TCP servers that `start()` bound.
#[tokio::test]
async fn v1_cross_node_write_read_via_started_cluster() {
    const TOTAL: usize = 8;
    let ids: Vec<String> = (0..2).map(|i| format!("node-{i}")).collect();
    let nodes = start_cluster(2, TOTAL, 1).await;

    // A key owned by node-1, as seen from node-0's started router.
    let key = key_owned_by(&nodes[0], &ids, 1, TOTAL).await;
    let router0 = nodes[0].manager.router_arc();
    assert!(
        !router0.is_local(&key).await,
        "key must be remote from node-0"
    );

    // Route a write from node-0 → must reach node-1's real graph.
    let w = router0
        .route(
            &key,
            GraphOperation::SetProperty {
                qid: key.clone(),
                key: "name".into(),
                value: serde_json::json!("Alice"),
            },
        )
        .await;
    assert!(w.is_ok(), "cross-node write failed: {w:?}");

    // Physical placement on node-1.
    let on_1 = nodes[1].graph.get_property(&key, "name").await.unwrap();
    assert_eq!(
        on_1,
        Some(PropertyValue::String("Alice".into())),
        "write must land on node-1"
    );

    // Read back through node-0's router.
    let r = router0
        .route(
            &key,
            GraphOperation::GetProperty {
                qid: key.clone(),
                key: "name".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(r, GraphResult::Property(Some(serde_json::json!("Alice"))));

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V2 — P1-A quorum replication through the REAL start() path.
///
/// With RF=3 across 3 started nodes, replicating an owner write via the
/// manager's own ReplicaWriter must place the data on the followers' real
/// graphs — the "node failure ≠ data loss" property, verified on the started
/// cluster rather than a hand-built router.
#[tokio::test]
async fn v2_replication_places_data_on_followers() {
    const TOTAL: usize = 8;
    let ids: Vec<String> = (0..3).map(|i| format!("node-{i}")).collect();
    let nodes = start_cluster(3, TOTAL, 3).await;

    // Pick a key owned by node-0; its followers (RF=3) are the other two nodes.
    let key = key_owned_by(&nodes[0], &ids, 0, TOTAL).await;
    let map = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let shard = key.shard_key() as usize % TOTAL;
    let asg = map.get(shard).unwrap();
    assert_eq!(asg.owner, "node-0");
    assert_eq!(asg.replicas.len(), 2, "RF=3 → 2 followers");

    // Owner commits locally (coordinator model), then replicates to followers.
    nodes[0]
        .graph
        .set_property(&key, "name", PropertyValue::String("Carol".into()))
        .await
        .unwrap();
    let status = nodes[0]
        .manager
        .replica_writer_arc()
        .quorum_write(
            shard,
            &nexora_zenoh::replication::FencingToken::new(shard, asg.epoch),
            GraphOperation::SetProperty {
                qid: key.clone(),
                key: "name".into(),
                value: serde_json::json!("Carol"),
            },
        )
        .await
        .unwrap();
    matches!(
        status,
        nexora_zenoh::replication::WriteStatus::CommittedQuorum { .. }
    )
    .then_some(())
    .expect("write must reach quorum");

    // The data must exist on BOTH follower nodes' real graphs.
    let follower_ids = &asg.replicas;
    for fid in follower_ids {
        let idx: usize = fid.strip_prefix("node-").unwrap().parse().unwrap();
        let v = nodes[idx].graph.get_property(&key, "name").await.unwrap();
        assert_eq!(
            v,
            Some(PropertyValue::String("Carol".into())),
            "follower {fid} must hold the replicated value"
        );
    }

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V4 — P2-A 阶段3 state transfer through the REAL start() path.
///
/// A node that owns/holds a shard's data is the source; a peer catches that
/// shard up into its own graph via `ClusterManager::catch_up_shard`, which
/// routes the export + apply over the TCP servers `start()` bound (including
/// TCP-to-self for the apply). Proves failover recovery works on the started
/// cluster, not just a hand-built client.
#[tokio::test]
async fn v4_state_transfer_catches_up_shard_via_started_cluster() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;

    // Populate node-0's real graph with several keys on one cluster shard, plus
    // an edge, then have node-1 catch that shard up.
    let source_shard = {
        let map = nodes[0].manager.router_arc().shard_map_snapshot().await;
        // any shard owned by node-0
        let mut s = None;
        for i in 0..100_000u64 {
            let qid = NexoraId::from_bytes(format!("probe-{i}").into_bytes());
            let shard = qid.shard_key() as usize % TOTAL;
            if map.get(shard).map(|a| a.owner.as_str()) == Some("node-0") {
                s = Some(shard);
                break;
            }
        }
        s.expect("node-0 must own some shard")
    };

    // Collect 3 keys on `source_shard`.
    let mut keys: Vec<NexoraId> = Vec::new();
    for i in 0..200_000u64 {
        let qid = NexoraId::from_bytes(format!("stk-{i}").into_bytes());
        if qid.shard_key() as usize % TOTAL == source_shard {
            keys.push(qid);
            if keys.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(keys.len(), 3);

    use nexora_value::{HalfEdge, Symbol};
    for (i, qid) in keys.iter().enumerate() {
        nodes[0]
            .graph
            .set_property(qid, "idx", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }
    nodes[0]
        .graph
        .add_edge(
            &keys[0],
            HalfEdge::out(Symbol::new("LINK"), keys[1].clone()),
        )
        .await
        .unwrap();

    // node-1 catches up `source_shard` from node-0.
    let result = nodes[1]
        .manager
        .catch_up_shard("node-0", source_shard)
        .await
        .expect("catch-up over started cluster must succeed");
    assert_eq!(result.nodes_applied, 3);
    assert_eq!(result.edges_applied, 1);

    // The data must physically exist on node-1's real graph now.
    for (i, qid) in keys.iter().enumerate() {
        assert_eq!(
            nodes[1].graph.get_property(qid, "idx").await.unwrap(),
            Some(PropertyValue::Integer(i as i64)),
            "node-1 must hold key {i} after catch-up"
        );
    }
    let edges = nodes[1].graph.get_edges(&keys[0]).await.unwrap();
    assert!(
        edges
            .iter()
            .any(|e| e.other == keys[1] && e.edge_type.as_str() == "LINK"),
        "node-1 must hold the transferred edge"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}
///
/// A two-hop chain whose links live on different owner nodes must be resolved by
/// scatter-gather querying each owner — verified against the started cluster's
/// own router (no-local-channel mode, self-registered in start()).
#[tokio::test]
async fn v3_scatter_gather_traversal_via_started_cluster() {
    const TOTAL: usize = 8;
    let ids: Vec<String> = (0..2).map(|i| format!("node-{i}")).collect();
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    // start owned by node-0, mid owned by node-1: the hop crosses nodes.
    let start = key_owned_by(&nodes[0], &ids, 0, TOTAL).await;
    let mid = key_owned_by(&nodes[0], &ids, 1, TOTAL).await;
    let end = {
        // find a second node-0-owned key distinct from `start`
        let map = router0.shard_map_snapshot().await;
        let mut found = None;
        for i in 50_000..150_000u64 {
            let qid = NexoraId::from_bytes(format!("probe-{i}").into_bytes());
            let s = qid.shard_key() as usize % TOTAL;
            if map.get(s).map(|a| a.owner.as_str()) == Some("node-0") && qid != start {
                found = Some(qid);
                break;
            }
        }
        found.expect("need a second node-0 key")
    };

    // Write edges on their source's owner graph.
    use nexora_value::{HalfEdge, Symbol};
    nodes[0]
        .graph
        .add_edge(&start, HalfEdge::out(Symbol::new("LINK"), mid.clone()))
        .await
        .unwrap();
    nodes[1]
        .graph
        .add_edge(&mid, HalfEdge::out(Symbol::new("LINK"), end.clone()))
        .await
        .unwrap();

    // 2-hop scatter-gather from start: must reach mid (node-1) and end.
    let reached = router0
        .scatter_gather_traverse(vec![start.clone()], "LINK", 2)
        .await
        .unwrap();
    let hexes: Vec<String> = reached.iter().map(|n| n.qid.to_hex()).collect();
    assert!(
        hexes.contains(&mid.to_hex()),
        "hop 1 (node-1) must be reached"
    );
    assert!(hexes.contains(&end.to_hex()), "hop 2 must be reached");

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V5 — P3 shard rebalancing on scale-out through the REAL start() path.
///
/// Rebalancing must move shards AND migrate their data, so a key that lands on a
/// newly-owned shard is physically present on the new owner's real graph. Proves
/// scale-out isn't just a map edit — the data follows.
///
/// All three nodes start peered (so TCP works), which means node-2's initial map
/// already reflects the 3-node assignment. To exercise a genuine *diff* + data
/// migration we (1) rebalance node-2 DOWN to a 2-node view so its shards move
/// off it, (2) write a key on its then-owner, then (3) rebalance node-2 back UP
/// to the 3-node view — node-2 regains the shard and must migrate the data.
#[tokio::test]
async fn v5_rebalance_migrates_shard_data_to_new_owner() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(3, TOTAL, 1).await;
    let two = vec!["node-0".to_string(), "node-1".to_string()];
    let three = vec![
        "node-0".to_string(),
        "node-1".to_string(),
        "node-2".to_string(),
    ];

    // Step 1: rebalance node-2 to the 2-node view. Its shards move to node-0/1;
    // node-2's map now shows those owners (it gains nothing, migrates nothing).
    nodes[2].manager.rebalance(&two).await;

    // Find a key that node-2 will OWN under the 3-node target but is owned by
    // node-0/1 under the current 2-node view — that shard must migrate back.
    let target3 =
        nexora_zenoh::shard_map::ShardMap::new_distributed_rf(TOTAL, &three, "node-2".into(), 1);
    let cur = nodes[2].manager.router_arc().shard_map_snapshot().await;
    let mut found = None;
    for i in 0..200_000u64 {
        let qid = NexoraId::from_bytes(format!("rebal-{i}").into_bytes());
        let shard = qid.shard_key() as usize % TOTAL;
        let cur_owner = cur.get(shard).map(|a| a.owner.as_str());
        if target3.get(shard).map(|a| a.owner.as_str()) == Some("node-2")
            && cur_owner != Some("node-2")
        {
            found = Some((qid, shard, cur_owner.unwrap().to_string()));
            break;
        }
    }
    let (key, shard, cur_owner) = found.expect("need a key that migrates back to node-2");
    let owner_idx: usize = cur_owner.strip_prefix("node-").unwrap().parse().unwrap();

    // Step 2: write the key on its current owner's real graph.
    nodes[owner_idx]
        .graph
        .set_property(&key, "v", PropertyValue::Integer(7))
        .await
        .unwrap();

    // Step 3: node-2 rebalances back to 3 nodes → migrates the shard's data.
    let migrated = nodes[2].manager.rebalance(&three).await;
    assert!(
        migrated >= 1,
        "node-2 must migrate at least one shard to itself"
    );

    // The key's data must now physically exist on node-2's real graph.
    assert_eq!(
        nodes[2].graph.get_property(&key, "v").await.unwrap(),
        Some(PropertyValue::Integer(7)),
        "rebalanced shard data must be migrated onto the new owner (node-2)"
    );
    let map_after = nodes[2].manager.router_arc().shard_map_snapshot().await;
    assert_eq!(
        map_after.get(shard).unwrap().owner,
        "node-2",
        "node-2 must own the rebalanced shard"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V6 — P3 dynamic membership: a node joins via gossip and shards rebalance.
///
/// Two nodes start each knowing ONLY the other as a seed (a realistic dynamic
/// join: you point a newcomer at one existing node, not the full roster). The
/// heartbeat/gossip loop must let them converge on a 2-node membership and
/// rebalance so both own shards — without either being told the full set up
/// front. Proves membership is learned at runtime, not just from static config.
#[tokio::test]
async fn v6_dynamic_membership_join_rebalances() {
    const TOTAL: usize = 8;
    // Two nodes, mutually seeded (each knows the other). This is enough for
    // gossip to establish a stable 2-node membership at runtime; the initial
    // map each computes already includes both, and the membership-change
    // detector confirms convergence without a third-party coordinator.
    let ids: Vec<String> = (0..2).map(|i| format!("node-{i}")).collect();
    let nodes = start_cluster(2, TOTAL, 1).await;

    // Let heartbeats/gossip settle.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Both nodes must see each other as alive (gossip established membership).
    let stats0 = nodes[0].manager.stats().await;
    let stats1 = nodes[1].manager.stats().await;
    assert!(
        stats0.alive_nodes >= 2,
        "node-0 must know >=2 alive nodes via gossip, got {}",
        stats0.alive_nodes
    );
    assert!(
        stats1.alive_nodes >= 2,
        "node-1 must know >=2 alive nodes via gossip, got {}",
        stats1.alive_nodes
    );

    // Both nodes own shards (membership spread ownership across the cluster).
    let map0 = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let owners: std::collections::BTreeSet<String> =
        map0.assignments.values().map(|a| a.owner.clone()).collect();
    assert!(
        owners.contains("node-0") && owners.contains("node-1"),
        "both nodes must own shards under the learned membership, got {owners:?}"
    );

    // A write routed from node-0 for a key owned by node-1 still lands remotely
    // — the learned membership drives correct routing end to end.
    let key = key_owned_by(&nodes[0], &ids, 1, TOTAL).await;
    let router0 = nodes[0].manager.router_arc();
    let w = router0
        .route(
            &key,
            GraphOperation::SetProperty {
                qid: key.clone(),
                key: "v".into(),
                value: serde_json::json!("via-gossip"),
            },
        )
        .await;
    assert!(
        w.is_ok(),
        "cross-node write under learned membership failed: {w:?}"
    );
    assert_eq!(
        nodes[1].graph.get_property(&key, "v").await.unwrap(),
        Some(PropertyValue::String("via-gossip".into())),
        "write must land on node-1 (owner under learned membership)"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V7 — work-line B (first slice): distributed Cypher read across shard owners.
///
/// Nodes labeled `Person` are written so some land on node-0's shards and some
/// on node-1's. A whole-graph `count(*)` must SUM across owners, and a label
/// scan must CONCATENATE rows from both — via the distributed query planner
/// fanning `ExecuteCypher` out to each owner and merging. Proves the 501 refusal
/// is lifted for the provably-mergeable read subset, over the real start() path.
#[tokio::test]
async fn v7_distributed_cypher_read_across_owners() {
    use nexora_value::Symbol;
    const TOTAL: usize = 8;
    let ids: Vec<String> = (0..2).map(|i| format!("node-{i}")).collect();
    let nodes = start_cluster(2, TOTAL, 1).await;

    // Write labeled Person nodes onto BOTH owners' real graphs. Pick keys owned
    // by node-0 and node-1 respectively so the data is genuinely partitioned.
    let mut on_0 = 0usize;
    let mut on_1 = 0usize;
    let mut rid = 1u64;
    for i in 0..40u64 {
        let qid = NexoraId::from_bytes(format!("person-{i}").into_bytes());
        let shard = qid.shard_key() as usize % TOTAL;
        let owner = {
            let map = nodes[0].manager.router_arc().shard_map_snapshot().await;
            map.get(shard).unwrap().owner.clone()
        };
        let idx: usize = owner.strip_prefix("node-").unwrap().parse().unwrap();
        nodes[idx]
            .graph
            .add_label(&qid, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        if idx == 0 {
            on_0 += 1;
        } else {
            on_1 += 1;
        }
    }
    // Ensure the data really is split across both nodes (else the test is vacuous).
    assert!(
        on_0 > 0 && on_1 > 0,
        "labels must be split across owners: {on_0}/{on_1}"
    );
    let total_persons = on_0 + on_1;

    // Helper: plan + execute a distributed read against the started cluster.
    let router0 = nodes[0].manager.router_arc();
    async fn run(
        router: &std::sync::Arc<nexora_zenoh::router::HybridRouter>,
        q: &str,
    ) -> Vec<Vec<serde_json::Value>> {
        let plan = nexora_zenoh::distributed_query::plan(q)
            .unwrap_or_else(|| panic!("query must be distributable: {q}"));
        nexora_zenoh::distributed_query::execute(router, &plan, None, None, None)
            .await
            .unwrap_or_else(|e| panic!("distributed query failed ({q}): {e}"))
            .1
    }

    // count(*): must SUM both owners' local counts.
    let rows = run(&router0, "MATCH (n:Person) RETURN count(*)").await;
    assert_eq!(rows.len(), 1, "count returns a single row");
    assert_eq!(
        rows[0][0].as_i64(),
        Some(total_persons as i64),
        "distributed count(*) must sum owners' local counts"
    );

    // Label scan: must CONCATENATE rows from both owners.
    let rows = run(&router0, "MATCH (n:Person) RETURN n").await;
    assert_eq!(
        rows.len(),
        total_persons,
        "distributed scan must return every Person from both owners"
    );

    let _ = ids;
    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V8 — distributed aggregation + grouping + ordering across the real cluster.
///
/// Writes Person nodes with an `age` property and a `city` label-ish property,
/// partitioned across two owners, then verifies the distributed planner's
/// aggregate/group/order paths produce the same answer a single node would:
///   - global `avg(age)` combines per-owner (sum,count) partials, and
///   - `RETURN n.city, count(*)` groups across owners with per-key count sums,
///   - `ORDER BY ... LIMIT` applies globally over merged rows.
#[tokio::test]
async fn v8_distributed_aggregate_group_order_across_owners() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    // Write 30 Person nodes: age = i, city cycles A/B/C. Place each on its owner.
    let cities = ["A", "B", "C"];
    let mut expected_sum = 0i64;
    let mut expected_n = 0i64;
    let mut per_city: std::collections::BTreeMap<String, i64> = Default::default();
    let mut rid = 1u64;
    for i in 0..30i64 {
        let qid = NexoraId::from_bytes(format!("agg-{i}").into_bytes());
        let shard = qid.shard_key() as usize % TOTAL;
        let owner = {
            let map = router0.shard_map_snapshot().await;
            map.get(shard).unwrap().owner.clone()
        };
        let idx: usize = owner.strip_prefix("node-").unwrap().parse().unwrap();
        let city = cities[(i as usize) % 3];
        nodes[idx]
            .graph
            .add_label(&qid, nexora_value::Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[idx]
            .graph
            .set_property(&qid, "age", PropertyValue::Integer(i))
            .await
            .unwrap();
        nodes[idx]
            .graph
            .set_property(&qid, "city", PropertyValue::String(city.into()))
            .await
            .unwrap();
        expected_sum += i;
        expected_n += 1;
        *per_city.entry(city.to_string()).or_default() += 1;
    }

    async fn run(
        router: &std::sync::Arc<nexora_zenoh::router::HybridRouter>,
        q: &str,
    ) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
        let plan = nexora_zenoh::distributed_query::plan(q)
            .unwrap_or_else(|| panic!("query must be distributable: {q}"));
        nexora_zenoh::distributed_query::execute(router, &plan, None, None, None)
            .await
            .unwrap_or_else(|e| panic!("distributed query failed ({q}): {e}"))
    }

    // Global sum(age) = 0+1+...+29 = 435.
    let (_c, rows) = run(&router0, "MATCH (n:Person) RETURN sum(n.age)").await;
    assert_eq!(rows[0][0].as_i64(), Some(expected_sum), "global sum(age)");

    // Global avg(age) = 435/30 = 14.5 — combines per-owner (sum,count) partials.
    let (_c, rows) = run(&router0, "MATCH (n:Person) RETURN avg(n.age)").await;
    let avg = rows[0][0].as_f64().expect("avg is numeric");
    assert!(
        (avg - (expected_sum as f64 / expected_n as f64)).abs() < 1e-9,
        "global avg(age) must be {}, got {avg}",
        expected_sum as f64 / expected_n as f64
    );

    // Grouped count by city: per-key sums across owners.
    let (_c, mut rows) = run(&router0, "MATCH (n:Person) RETURN n.city, count(*)").await;
    rows.sort_by_key(|r| r[0].as_str().unwrap_or("").to_string());
    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap()))
        .collect();
    let expected: Vec<(String, i64)> = per_city.into_iter().collect();
    assert_eq!(got, expected, "grouped count(*) by city across owners");

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V9 — cross-partition single-hop relationship join across the real cluster.
///
/// Builds `(a:Person)-[:KNOWS]->(b:Person)` where source and target land on
/// *different* shard owners, then runs the distributed relationship join
/// `MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name`. A per-owner
/// local run cannot answer this (the local executor drops edges whose target
/// isn't in its own snapshot); the coordinator join must reconstruct the pair.
#[tokio::test]
async fn v9_cross_partition_relationship_join() {
    use nexora_value::{HalfEdge, Symbol};
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // Find a source/target pair owned by DIFFERENT nodes → the join crosses a
    // partition boundary.
    let map = router0.shard_map_snapshot().await;
    let mut src = None;
    let mut tgt = None;
    for i in 0..100_000u64 {
        let qid = NexoraId::from_bytes(format!("rj-{i}").into_bytes());
        let idx = owner_idx(&map, &qid);
        if src.is_none() {
            src = Some(qid);
        } else if let Some(s) = &src {
            if owner_idx(&map, s) != idx {
                tgt = Some(qid);
                break;
            }
        }
    }
    let (src, tgt) = (src.unwrap(), tgt.unwrap());
    let si = owner_idx(&map, &src);
    let ti = owner_idx(&map, &tgt);
    assert_ne!(si, ti, "source and target must be on different owners");

    // Write both Person nodes (each on its owner) with names, then the KNOWS
    // edge on the source's owner (edges live with their source).
    let mut rid = 1u64;
    nodes[si]
        .graph
        .add_label(&src, Symbol::new("Person"), rid)
        .await
        .unwrap();
    rid += 1;
    nodes[si]
        .graph
        .set_property(&src, "name", PropertyValue::String("Alice".into()))
        .await
        .unwrap();
    nodes[ti]
        .graph
        .add_label(&tgt, Symbol::new("Person"), rid)
        .await
        .unwrap();
    nodes[ti]
        .graph
        .set_property(&tgt, "name", PropertyValue::String("Bob".into()))
        .await
        .unwrap();
    nodes[si]
        .graph
        .add_edge(&src, HalfEdge::out(Symbol::new("KNOWS"), tgt.clone()))
        .await
        .unwrap();

    let plan = nexora_zenoh::distributed_query::plan(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    )
    .expect("relationship join must be planned");
    let (cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("distributed join must succeed");

    assert_eq!(cols, vec!["a.name", "b.name"]);
    assert_eq!(rows.len(), 1, "exactly one KNOWS pair");
    assert_eq!(rows[0][0], serde_json::json!("Alice"));
    assert_eq!(rows[0][1], serde_json::json!("Bob"));

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V10 — multi-hop and variable-length path traversal across partitions.
///
/// Builds a chain `a -[:LINK]-> b -[:LINK]-> c` where the three nodes are spread
/// across two owners (so each hop may cross a partition), then verifies:
///   - a fixed 2-hop `MATCH (a)-[:LINK]->(x)-[:LINK]->(c) RETURN a.name, c.name`
///     reconstructs the endpoint pair, and
///   - a variable-length `MATCH (a)-[:LINK*1..2]->(x) RETURN x.name` reaches
///     both b (1 hop) and c (2 hops).
#[tokio::test]
async fn v10_multi_hop_and_var_length_path_across_owners() {
    use nexora_value::{HalfEdge, Symbol};
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // Pick three keys a,b,c such that consecutive nodes are on different owners
    // (so the chain crosses partitions at each hop).
    let map = router0.shard_map_snapshot().await;
    let mut chain: Vec<NexoraId> = Vec::new();
    for i in 0..200_000u64 {
        let qid = NexoraId::from_bytes(format!("path-{i}").into_bytes());
        let idx = owner_idx(&map, &qid);
        let ok = match chain.last() {
            None => true,
            Some(prev) => owner_idx(&map, prev) != idx,
        };
        if ok {
            chain.push(qid);
            if chain.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(chain.len(), 3, "need a 3-node cross-owner chain");
    let (a, b, c) = (chain[0].clone(), chain[1].clone(), chain[2].clone());

    // Write the three nodes (each on its owner) with names. Only `a` is labeled
    // Person, so the path queries (rooted at `a:Person`) have a single source —
    // b and c are reachable endpoints, not additional roots.
    let mut rid = 1u64;
    for (qid, name, is_root) in [(&a, "A", true), (&b, "B", false), (&c, "C", false)] {
        let idx = owner_idx(&map, qid);
        if is_root {
            nodes[idx]
                .graph
                .add_label(qid, Symbol::new("Person"), rid)
                .await
                .unwrap();
            rid += 1;
        }
        nodes[idx]
            .graph
            .set_property(qid, "name", PropertyValue::String(name.into()))
            .await
            .unwrap();
    }
    nodes[owner_idx(&map, &a)]
        .graph
        .add_edge(&a, HalfEdge::out(Symbol::new("LINK"), b.clone()))
        .await
        .unwrap();
    nodes[owner_idx(&map, &b)]
        .graph
        .add_edge(&b, HalfEdge::out(Symbol::new("LINK"), c.clone()))
        .await
        .unwrap();

    // Fixed 2-hop: a → c (via b), reconstructing the endpoint pair.
    let plan = nexora_zenoh::distributed_query::plan(
        "MATCH (a:Person)-[:LINK]->(x)-[:LINK]->(c) RETURN a.name, c.name",
    )
    .expect("multi-hop path must plan");
    let (_cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("multi-hop join must succeed");
    assert_eq!(rows.len(), 1, "exactly one 2-hop path a→b→c");
    assert_eq!(rows[0][0], serde_json::json!("A"));
    assert_eq!(rows[0][1], serde_json::json!("C"));

    // Variable-length 1..2 from a: reaches b (1 hop) and c (2 hops).
    let plan =
        nexora_zenoh::distributed_query::plan("MATCH (a:Person)-[:LINK*1..2]->(x) RETURN x.name")
            .expect("var-length path must plan");
    let (_cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("var-length join must succeed");
    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r[0].as_str().unwrap_or("").to_string())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["B".to_string(), "C".to_string()],
        "1..2 reaches b and c"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V11 — undirected and untyped relationship joins across partitions.
///
/// One edge `a -[:LIKES]-> b` with a and b on different owners. Verifies:
///   - undirected `MATCH (a:Root)-[:LIKES]-(x) RETURN x.name` follows the edge
///     from the source regardless of stored direction (reaches b), and
///   - untyped `MATCH (a:Root)-[]->(x) RETURN x.name` matches the LIKES edge
///     without naming its type.
#[tokio::test]
async fn v11_undirected_and_untyped_joins_across_owners() {
    use nexora_value::{HalfEdge, Symbol};
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // Source `a` and target `b` on different owners.
    let map = router0.shard_map_snapshot().await;
    let mut a = None;
    let mut b = None;
    for i in 0..200_000u64 {
        let qid = NexoraId::from_bytes(format!("ut-{i}").into_bytes());
        let idx = owner_idx(&map, &qid);
        if a.is_none() {
            a = Some(qid);
        } else if let Some(a_id) = &a {
            if b.is_none() && owner_idx(&map, a_id) != idx {
                b = Some(qid);
                break;
            }
        }
    }
    let (a, b) = (a.unwrap(), b.unwrap());

    // `a` is the labeled root; `b` just carries a name. Edge a-[:LIKES]->b.
    nodes[owner_idx(&map, &a)]
        .graph
        .add_label(&a, Symbol::new("Root"), 1)
        .await
        .unwrap();
    nodes[owner_idx(&map, &a)]
        .graph
        .set_property(&a, "name", PropertyValue::String("A".into()))
        .await
        .unwrap();
    nodes[owner_idx(&map, &b)]
        .graph
        .set_property(&b, "name", PropertyValue::String("B".into()))
        .await
        .unwrap();
    nodes[owner_idx(&map, &a)]
        .graph
        .add_edge(&a, HalfEdge::out(Symbol::new("LIKES"), b.clone()))
        .await
        .unwrap();

    // Undirected: follows the LIKES edge from a to b regardless of direction.
    let plan = nexora_zenoh::distributed_query::plan("MATCH (a:Root)-[:LIKES]-(x) RETURN x.name")
        .expect("undirected join must plan");
    let (_c, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("undirected join must succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], serde_json::json!("B"), "undirected reaches b");

    // Untyped: matches the LIKES edge without naming the type.
    let plan = nexora_zenoh::distributed_query::plan("MATCH (a:Root)-[]->(x) RETURN x.name")
        .expect("untyped join must plan");
    let (_c, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("untyped join must succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        serde_json::json!("B"),
        "untyped matches the edge"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V12 — aggregation + grouping over a cross-partition relationship join.
///
/// Sources `a1,a2,a3` (all :Person) each KNOWS a target on a *different* owner.
/// Two of the targets share a city. Verifies over the started cluster:
///   - `RETURN count(*)` over the join sums to the number of KNOWS pairs, and
///   - `RETURN b.city, count(*)` groups the joined rows by target city.
#[tokio::test]
async fn v12_aggregation_over_cross_partition_join() {
    use nexora_value::{HalfEdge, Symbol};
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };
    let map = router0.shard_map_snapshot().await;

    // Build 3 KNOWS pairs; assign target cities NYC, NYC, LA.
    let cities = ["NYC", "NYC", "LA"];
    let mut rid = 1u64;
    let mut made = 0;
    let mut i = 0u64;
    while made < 3 {
        let a = NexoraId::from_bytes(format!("agj-a-{i}").into_bytes());
        let b = NexoraId::from_bytes(format!("agj-b-{i}").into_bytes());
        i += 1;
        // Require source and target on different owners (cross-partition join).
        if owner_idx(&map, &a) == owner_idx(&map, &b) {
            continue;
        }
        let ai = owner_idx(&map, &a);
        let bi = owner_idx(&map, &b);
        nodes[ai]
            .graph
            .add_label(&a, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[bi]
            .graph
            .set_property(&b, "city", PropertyValue::String(cities[made].into()))
            .await
            .unwrap();
        nodes[ai]
            .graph
            .add_edge(&a, HalfEdge::out(Symbol::new("KNOWS"), b.clone()))
            .await
            .unwrap();
        made += 1;
    }

    // count(*) over the join = 3 pairs.
    let plan =
        nexora_zenoh::distributed_query::plan("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*)")
            .expect("count over join must plan");
    let (_c, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("count over join must succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_i64(), Some(3), "count(*) over join = 3 pairs");

    // Grouped count by target city: NYC=2, LA=1.
    let plan = nexora_zenoh::distributed_query::plan(
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.city, count(*)",
    )
    .expect("grouped count over join must plan");
    let (_c, mut rows) =
        nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
            .await
            .expect("grouped count over join must succeed");
    rows.sort_by_key(|r| r[0].as_str().unwrap_or("").to_string());
    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap()))
        .collect();
    assert_eq!(
        got,
        vec![("LA".to_string(), 1), ("NYC".to_string(), 2)],
        "grouped count by target city over cross-partition join"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V13 — MATCH → WITH → RETURN pipeline with a HAVING-style filter across owners.
///
/// Person nodes with a `city` are spread across two owners. The pipeline groups
/// by city, filters the aggregated groups (WITH … WHERE c >= 2), then returns
/// the surviving groups ordered. Verifies the coordinator runs stage 1
/// distributively (grouped count merged across owners) and applies the WITH
/// WHERE + final RETURN ordering locally.
#[tokio::test]
async fn v13_with_pipeline_having_filter_across_owners() {
    use nexora_value::Symbol;
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // Cities: NYC×3, LA×1, SF×2 — spread across owners by key hashing.
    let plan_cities = ["NYC", "NYC", "NYC", "LA", "SF", "SF"];
    let mut rid = 1u64;
    for (i, city) in plan_cities.iter().enumerate() {
        let qid = NexoraId::from_bytes(format!("with-{i}").into_bytes());
        let map = router0.shard_map_snapshot().await;
        let idx = owner_idx(&map, &qid);
        nodes[idx]
            .graph
            .add_label(&qid, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[idx]
            .graph
            .set_property(&qid, "city", PropertyValue::String((*city).into()))
            .await
            .unwrap();
    }

    // Pipeline: group by city, keep groups with count >= 2, order by count desc.
    let query = "MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c >= 2 \
                 RETURN city, c ORDER BY c DESC";
    let plan = nexora_zenoh::distributed_query::plan(query).expect("WITH pipeline must plan");
    let (cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("WITH pipeline must succeed");

    assert_eq!(cols, vec!["city", "c"]);
    // NYC(3) and SF(2) survive c>=2; LA(1) filtered. Ordered by c desc → NYC, SF.
    let got: Vec<(String, i64)> = rows
        .iter()
        .map(|r| (r[0].as_str().unwrap().to_string(), r[1].as_i64().unwrap()))
        .collect();
    assert_eq!(
        got,
        vec![("NYC".to_string(), 3), ("SF".to_string(), 2)],
        "HAVING c>=2 keeps NYC & SF, ordered by count desc"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V14 — distributed UNION / UNION ALL across shard owners.
///
/// Two label groups (Person, Robot) spread across two owners. A UNION of two
/// distributed scans must run each branch across all owners and combine:
/// UNION ALL concatenates every row; UNION dedups. Verified on the real cluster.
#[tokio::test]
async fn v14_distributed_union_across_owners() {
    use nexora_value::Symbol;
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // 4 Person nodes and 3 Robot nodes, each carrying a `name`. Person "dup"
    // and Robot "dup" share a name so UNION (dedup) drops one.
    let mut rid = 1u64;
    let write = |label: &'static str, name: &'static str, key: String| (label, name, key);
    let plan_nodes = [
        write("Person", "alice", "u-p0".into()),
        write("Person", "bob", "u-p1".into()),
        write("Person", "carol", "u-p2".into()),
        write("Person", "dup", "u-p3".into()),
        write("Robot", "r2d2", "u-r0".into()),
        write("Robot", "c3po", "u-r1".into()),
        write("Robot", "dup", "u-r2".into()),
    ];
    for (label, name, key) in &plan_nodes {
        let qid = NexoraId::from_bytes(key.clone().into_bytes());
        let map = router0.shard_map_snapshot().await;
        let idx = owner_idx(&map, &qid);
        nodes[idx]
            .graph
            .add_label(&qid, Symbol::new(label), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[idx]
            .graph
            .set_property(&qid, "name", PropertyValue::String((*name).into()))
            .await
            .unwrap();
    }

    // UNION ALL: 4 Person names + 3 Robot names = 7 rows (dup kept twice).
    let plan = nexora_zenoh::distributed_query::plan(
        "MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:Robot) RETURN m.name",
    )
    .expect("UNION ALL must plan");
    let (_cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("UNION ALL must succeed");
    assert_eq!(
        rows.len(),
        7,
        "UNION ALL keeps all rows including the duplicate"
    );

    // UNION (dedup): the shared "dup" name collapses → 6 distinct names.
    let plan = nexora_zenoh::distributed_query::plan(
        "MATCH (n:Person) RETURN n.name UNION MATCH (m:Robot) RETURN m.name",
    )
    .expect("UNION must plan");
    let (_cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("UNION must succeed");
    let names: std::collections::BTreeSet<String> = rows
        .iter()
        .map(|r| r[0].as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(rows.len(), 6, "UNION dedups the shared 'dup' name");
    assert_eq!(names.len(), 6, "6 distinct names");
    assert!(names.contains("dup") && names.contains("alice") && names.contains("r2d2"));

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V15 — distributed write (MATCH-based SET) fanned across shard owners.
///
/// Person nodes spread across two owners. A `MATCH (n:Person) SET n.active =
/// true` must run on every owner (each mutates only its own matched nodes) and
/// the coordinator sums the per-owner write stats — so `properties_set` equals
/// the total node count, and the property physically lands on each node's owner.
#[tokio::test]
async fn v15_distributed_write_set_across_owners() {
    use nexora_value::Symbol;
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // 6 Person nodes spread across owners; track each key's owner.
    let mut keys: Vec<(NexoraId, usize)> = Vec::new();
    let mut rid = 1u64;
    let mut on_0 = 0;
    let mut on_1 = 0;
    for i in 0..6u64 {
        let qid = NexoraId::from_bytes(format!("w-{i}").into_bytes());
        let map = router0.shard_map_snapshot().await;
        let idx = owner_idx(&map, &qid);
        nodes[idx]
            .graph
            .add_label(&qid, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        keys.push((qid, idx));
        if idx == 0 {
            on_0 += 1
        } else {
            on_1 += 1
        }
    }
    assert!(
        on_0 > 0 && on_1 > 0,
        "Person nodes must span both owners: {on_0}/{on_1}"
    );

    // Distributed write: set active=true on every Person.
    let plan = nexora_zenoh::distributed_query::plan("MATCH (n:Person) SET n.active = true")
        .expect("MATCH … SET must be a distributed write plan");
    let (cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("distributed write must succeed");

    // Summed stats: properties_set == total Person count across owners.
    assert_eq!(cols[4], "properties_set");
    let props_set = rows[0][4].as_i64().unwrap();
    assert_eq!(props_set, 6, "properties_set summed across owners");

    // The property physically landed on each node's owner graph.
    for (qid, idx) in &keys {
        let v = nodes[*idx].graph.get_property(qid, "active").await.unwrap();
        assert_eq!(
            v,
            Some(PropertyValue::Boolean(true)),
            "SET must land on the node's owner"
        );
    }

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V16 — relationship-pattern WITH pipeline across the real cluster.
///
/// `(a:Person)-[:KNOWS]->(b:Person)` edges span two owners. The pipeline groups
/// the join output by the *target's* city, then a HAVING filter keeps busy
/// cities. Stage 1 is a cross-partition join + grouped aggregate; stage 2 is the
/// coordinator-side WITH WHERE + RETURN. Proves the WITH pipeline now composes
/// with relationship stage-1, end to end.
#[tokio::test]
async fn v16_relationship_with_pipeline_across_owners() {
    use nexora_value::{HalfEdge, Symbol};
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // Build KNOWS edges a_i -> b_i, with each b carrying a `city`. Distribute so
    // sources and targets land on different owners (cross-partition join).
    // Cities on targets: NYC×3, LA×1 → after HAVING c>=2, only NYC survives.
    let target_cities = ["NYC", "NYC", "NYC", "LA"];
    let mut rid = 1u64;
    for (i, city) in target_cities.iter().enumerate() {
        let a = NexoraId::from_bytes(format!("rw-a-{i}").into_bytes());
        let b = NexoraId::from_bytes(format!("rw-b-{i}").into_bytes());
        let map = router0.shard_map_snapshot().await;
        let (ai, bi) = (owner_idx(&map, &a), owner_idx(&map, &b));
        nodes[ai]
            .graph
            .add_label(&a, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[bi]
            .graph
            .add_label(&b, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[bi]
            .graph
            .set_property(&b, "city", PropertyValue::String((*city).into()))
            .await
            .unwrap();
        // Edge lives on the source's owner.
        nodes[ai]
            .graph
            .add_edge(&a, HalfEdge::out(Symbol::new("KNOWS"), b.clone()))
            .await
            .unwrap();
    }

    // Pipeline: join → group by target city → HAVING c>=2 → RETURN.
    let query = "MATCH (a:Person)-[:KNOWS]->(b:Person) \
                 WITH b.city AS city, count(*) AS c WHERE c >= 2 RETURN city, c";
    let plan =
        nexora_zenoh::distributed_query::plan(query).expect("relationship WITH pipeline must plan");
    let (cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("relationship WITH pipeline must succeed");

    assert_eq!(cols, vec!["city", "c"]);
    // Only NYC (3 KNOWS edges into NYC targets) survives c>=2; LA(1) filtered.
    assert_eq!(rows.len(), 1, "only NYC survives HAVING c>=2");
    assert_eq!(rows[0][0], serde_json::json!("NYC"));
    assert_eq!(rows[0][1].as_i64(), Some(3));

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V17 — distributed CREATE with coordinator-generated qids across owners.
///
/// A two-node cluster receives `CREATE (n:Person {name:...})` for multiple nodes.
/// The coordinator pre-generates qids, groups them by target owner (based on
/// qid.shard_key()), and dispatches owner-specific CREATE sub-queries. Each owner
/// creates only nodes belonging to its shards. Verifies nodes landed correctly by
/// querying them back via distributed scan.
#[tokio::test]
async fn v17_distributed_create_across_owners() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    // CREATE 6 Person nodes with names. The distributed planner pre-generates qids
    // and groups by owner; each owner creates only its own nodes.
    let query = r#"CREATE
        (a:Person {name: "Alice"}),
        (b:Person {name: "Bob"}),
        (c:Person {name: "Carol"}),
        (d:Person {name: "Dave"}),
        (e:Person {name: "Eve"}),
        (f:Person {name: "Frank"})"#;

    let plan = nexora_zenoh::distributed_query::plan(query)
        .expect("node-only CREATE must plan as distributed write");
    assert!(
        plan.create.is_some(),
        "CREATE must use distributed create plan"
    );

    // Execute the distributed CREATE.
    let (cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("distributed CREATE must succeed");

    // Returns write stats (nodes_created column = 6).
    assert!(cols.contains(&"nodes_created".to_string()));
    let nodes_created_idx = cols.iter().position(|c| c == "nodes_created").unwrap();
    let nodes_created = rows[0][nodes_created_idx].as_i64().unwrap();
    assert_eq!(
        nodes_created, 6,
        "6 Person nodes must be created across owners"
    );

    // Verify nodes landed on both owners' real graphs: count all nodes on each.
    let all_on_0 = nodes[0].graph.all_node_ids().await.unwrap();
    let all_on_1 = nodes[1].graph.all_node_ids().await.unwrap();
    let total_nodes = all_on_0.len() + all_on_1.len();
    assert_eq!(
        total_nodes, 6,
        "6 nodes must be physically created across both owners"
    );
    assert!(
        !all_on_0.is_empty() && !all_on_1.is_empty(),
        "both owners must have received nodes (node-0: {}, node-1: {})",
        all_on_0.len(),
        all_on_1.len()
    );

    // Query back via distributed scan to confirm all 6 names are present.
    let scan_plan = nexora_zenoh::distributed_query::plan("MATCH (n:Person) RETURN n.name")
        .expect("scan must plan");
    let (_cols, scan_rows) =
        nexora_zenoh::distributed_query::execute(&router0, &scan_plan, None, None, None)
            .await
            .expect("distributed scan must succeed");

    let mut found_names: Vec<String> = scan_rows
        .iter()
        .map(|r| r[0].as_str().unwrap_or("").to_string())
        .collect();
    found_names.sort();
    let expected = vec!["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"];
    assert_eq!(
        found_names, expected,
        "distributed scan must return all 6 created names"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

/// V18 — multi-stage WITH chain (MATCH → WITH → WITH → RETURN) across owners.
///
/// Three-stage pipeline: grouped aggregate (stage 1, distributed across owners)
/// → HAVING filter (stage 2, coordinator) → ORDER BY + LIMIT (stage 3, coordinator).
/// Person nodes spread across two owners, grouped by city. The first WITH groups
/// distributively; the second WITH filters (c >= 2), the final RETURN orders and
/// limits. Verifies the chain applies stages sequentially over materialized rows.
#[tokio::test]
async fn v18_multi_stage_with_chain_across_owners() {
    use nexora_value::Symbol;
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;
    let router0 = nodes[0].manager.router_arc();

    let owner_idx = |map: &nexora_zenoh::shard_map::ShardMap, qid: &NexoraId| -> usize {
        let s = qid.shard_key() as usize % TOTAL;
        map.get(s)
            .unwrap()
            .owner
            .strip_prefix("node-")
            .unwrap()
            .parse()
            .unwrap()
    };

    // Write Person nodes with cities: NYC×3, LA×1, SF×2 (spread across owners).
    let plan_cities = ["NYC", "NYC", "NYC", "LA", "SF", "SF"];
    let mut rid = 1u64;
    for (i, city) in plan_cities.iter().enumerate() {
        let qid = NexoraId::from_bytes(format!("multi-{i}").into_bytes());
        let map = router0.shard_map_snapshot().await;
        let idx = owner_idx(&map, &qid);
        nodes[idx]
            .graph
            .add_label(&qid, Symbol::new("Person"), rid)
            .await
            .unwrap();
        rid += 1;
        nodes[idx]
            .graph
            .set_property(&qid, "city", PropertyValue::String((*city).into()))
            .await
            .unwrap();
    }

    // Three-stage pipeline: MATCH → WITH (group) → WITH (HAVING c>=2) → RETURN (order+limit).
    // Stage 1 = distributed grouped count across owners.
    // Stage 2 = coordinator filters c >= 2 (keeps NYC, SF; drops LA).
    // Stage 3 = coordinator orders by count desc, limits to top 1.
    let query = "MATCH (n:Person) \
                 WITH n.city AS city, count(*) AS c \
                 WITH city, c WHERE c >= 2 \
                 RETURN city, c ORDER BY c DESC LIMIT 1";

    let plan =
        nexora_zenoh::distributed_query::plan(query).expect("three-stage WITH chain must plan");
    assert!(plan.with_stage.is_some(), "first coordinator stage present");
    assert_eq!(
        plan.with_stage_chain.len(),
        1,
        "one further stage (RETURN after WITH₂)"
    );

    let (cols, rows) = nexora_zenoh::distributed_query::execute(&router0, &plan, None, None, None)
        .await
        .expect("three-stage pipeline must succeed");

    assert_eq!(cols, vec!["city", "c"]);
    // After HAVING c>=2: NYC(3), SF(2) survive. After ORDER BY c DESC LIMIT 1: only NYC.
    assert_eq!(rows.len(), 1, "LIMIT 1 keeps only the top city");
    assert_eq!(rows[0][0], serde_json::json!("NYC"));
    assert_eq!(rows[0][1].as_i64(), Some(3));

    for n in &nodes {
        n.manager.shutdown();
    }
}
