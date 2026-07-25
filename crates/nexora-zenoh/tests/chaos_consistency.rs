//! P2-A 阶段5 — fault-injection chaos + Merkle consistency.
//!
//! These tests drive the REAL `ClusterManager::start()` path (like
//! `real_cluster_smoke.rs`) and then inject faults — killing the shard owner,
//! partitioning a node, restarting nodes — and assert that the surviving /
//! recovered replicas hold **byte-identical** shard data, verified by comparing
//! Merkle-tree root hashes (reusing `nexora_raft::MerkleTree`).
//!
//! The discipline (see TASKS.md §关键纪律 #4): distributed correctness must be
//! shown on a real multi-node cluster under fault injection, not on isolated
//! components. A matching Merkle root over a replica set is the consistency
//! oracle — a single divergent key changes the root.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_raft::MerkleTree;
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use nexora_zenoh::GraphOperation;
use std::sync::Arc;
use std::time::Duration;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

struct Node {
    graph: Arc<GraphService>,
    manager: ClusterManager,
    id: String,
    graph_addr: String,
    hb_addr: String,
}

/// Build+start `n` fully-peered real nodes with replication factor `rf`.
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
            heartbeat_interval: Duration::from_millis(150),
            failure_timeout: Duration::from_secs(1),
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

        nodes.push(Node {
            graph,
            manager,
            id: ids[i].clone(),
            graph_addr: graph_addrs[i].clone(),
            hb_addr: hb_addrs[i].clone(),
        });
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    nodes
}

/// Compute a Merkle root over a node's data for a single cluster shard. Keys are
/// filtered by `shard_key() % total == shard`, sorted, and each entry hashes its
/// full property map + outgoing edges — so any divergence in properties or edges
/// changes the root. Returns `None` for an empty shard (no data).
async fn shard_merkle_root(graph: &GraphService, shard: usize, total: usize) -> Option<[u8; 32]> {
    let mut ids: Vec<NexoraId> = graph
        .all_node_ids()
        .await
        .unwrap()
        .into_iter()
        .filter(|q| (q.shard_key() as usize) % total == shard)
        .collect();
    ids.sort_by_key(|q| q.to_hex());

    let mut kv: Vec<(String, serde_json::Value)> = Vec::new();
    for qid in &ids {
        let props = graph.get_all_properties(qid).await.unwrap();
        // Canonical string of sorted props.
        let mut prop_pairs: Vec<(String, String)> = props
            .into_iter()
            .map(|(k, v)| (k.to_string(), format!("{v:?}")))
            .collect();
        prop_pairs.sort();

        let mut edges: Vec<String> = graph
            .get_edges(qid)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.direction == nexora_value::EdgeDirection::Out)
            .map(|e| format!("{}->{}", e.edge_type.as_str(), e.other.to_hex()))
            .collect();
        edges.sort();

        kv.push((
            qid.to_hex(),
            serde_json::json!({ "props": prop_pairs, "edges": edges }),
        ));
    }

    if kv.is_empty() {
        return None;
    }
    MerkleTree::build(&kv, 16).root_hash().copied()
}

// ── Scenario 1: kill the owner → failover promotes a live replica, and the
//    promoted replica's shard data matches the other survivor (Merkle) ───────

/// RF=3 across 3 nodes. Replicate several writes for a shard owned by node-0,
/// then KILL node-0. The failure detector on a surviving node must fail the
/// shard over to a live replica. Both survivors must hold identical shard data
/// (matching Merkle root) — the failover exposed no divergence or data loss.
#[tokio::test]
async fn chaos_kill_owner_failover_preserves_consistency() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(3, TOTAL, 3).await;

    // Find a key/shard owned by node-0 with node-1 & node-2 as replicas.
    let map0 = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let (key, shard) = {
        let mut found = None;
        for i in 0..100_000u64 {
            let qid = NexoraId::from_bytes(format!("chaos-{i}").into_bytes());
            let s = qid.shard_key() as usize % TOTAL;
            if map0.get(s).map(|a| a.owner.as_str()) == Some("node-0") {
                found = Some((qid, s));
                break;
            }
        }
        found.expect("node-0 must own a shard")
    };
    let asg = map0.get(shard).unwrap().clone();
    assert_eq!(asg.owner, "node-0");
    assert_eq!(asg.replicas.len(), 2, "RF=3 → 2 followers");

    // Write several keys on this shard: owner commits locally + quorum-replicates.
    let mut shard_keys = vec![key.clone()];
    for i in 0..200_000u64 {
        if shard_keys.len() >= 5 {
            break;
        }
        let qid = NexoraId::from_bytes(format!("chaos-k-{i}").into_bytes());
        if qid.shard_key() as usize % TOTAL == shard && qid != key {
            shard_keys.push(qid);
        }
    }
    let writer = nodes[0].manager.replica_writer_arc();
    for (i, qid) in shard_keys.iter().enumerate() {
        nodes[0]
            .graph
            .set_property(qid, "v", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        let status = writer
            .quorum_write(
                shard,
                &nexora_zenoh::replication::FencingToken::new(shard, asg.epoch),
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: "v".into(),
                    value: serde_json::json!(i),
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(
                status,
                nexora_zenoh::replication::WriteStatus::CommittedQuorum { .. }
            ),
            "write {i} must reach quorum, got {status:?}"
        );
    }

    // Both followers should hold identical shard data BEFORE the fault.
    let f1: usize = asg.replicas[0]
        .strip_prefix("node-")
        .unwrap()
        .parse()
        .unwrap();
    let f2: usize = asg.replicas[1]
        .strip_prefix("node-")
        .unwrap()
        .parse()
        .unwrap();
    let root_f1 = shard_merkle_root(&nodes[f1].graph, shard, TOTAL).await;
    let root_f2 = shard_merkle_root(&nodes[f2].graph, shard, TOTAL).await;
    assert!(root_f1.is_some(), "follower must hold shard data");
    assert_eq!(root_f1, root_f2, "followers must be consistent pre-fault");

    // ── FAULT: kill node-0 (the owner). ──
    nodes[0].manager.shutdown();

    // Wait for the failure detector to notice (failure_timeout=1s) and fail over.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // A surviving node's control plane must have promoted a live replica: the
    // shard's owner is no longer node-0, and its epoch was bumped.
    let map_after = nodes[f1].manager.router_arc().shard_map_snapshot().await;
    let asg_after = map_after.get(shard).unwrap();
    assert_ne!(asg_after.owner, "node-0", "owner must have failed over");
    assert!(
        asg_after.epoch.value() > asg.epoch.value(),
        "failover must bump the shard epoch ({} -> {})",
        asg.epoch.value(),
        asg_after.epoch.value()
    );

    // Consistency oracle: the two survivors still hold identical shard data — no
    // data loss, no divergence introduced by the failover.
    let root_f1_after = shard_merkle_root(&nodes[f1].graph, shard, TOTAL).await;
    let root_f2_after = shard_merkle_root(&nodes[f2].graph, shard, TOTAL).await;
    assert_eq!(
        root_f1_after, root_f2_after,
        "survivors must remain consistent after owner death"
    );
    assert_eq!(
        root_f1_after, root_f1,
        "surviving data must equal the pre-fault replicated state (no loss)"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}

// ── Scenario 2: rejoin catch-up after restart → restarted node re-attains
//    consistency with a survivor via state transfer (Merkle) ────────────────

/// A node that restarts loses its in-memory graph (InMemoryPersistor). After
/// rejoin it must catch the shard up from a survivor and re-attain an identical
/// Merkle root — the state-transfer recovery path under a restart fault.
#[tokio::test]
async fn chaos_restart_then_catch_up_restores_consistency() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(2, TOTAL, 1).await;

    // Populate a shard owned by node-0 with several keys + an edge.
    let map0 = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let shard = {
        let mut s = None;
        for i in 0..100_000u64 {
            let qid = NexoraId::from_bytes(format!("rs-{i}").into_bytes());
            let sh = qid.shard_key() as usize % TOTAL;
            if map0.get(sh).map(|a| a.owner.as_str()) == Some("node-0") {
                s = Some(sh);
                break;
            }
        }
        s.unwrap()
    };
    let mut keys: Vec<NexoraId> = Vec::new();
    for i in 0..200_000u64 {
        let qid = NexoraId::from_bytes(format!("rs-k-{i}").into_bytes());
        if qid.shard_key() as usize % TOTAL == shard {
            keys.push(qid);
            if keys.len() == 4 {
                break;
            }
        }
    }
    use nexora_value::{HalfEdge, Symbol};
    for (i, qid) in keys.iter().enumerate() {
        nodes[0]
            .graph
            .set_property(qid, "v", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }
    nodes[0]
        .graph
        .add_edge(&keys[0], HalfEdge::out(Symbol::new("REL"), keys[1].clone()))
        .await
        .unwrap();

    let source_root = shard_merkle_root(&nodes[0].graph, shard, TOTAL).await;
    assert!(source_root.is_some());

    // ── FAULT: restart node-1 (fresh graph, new manager on the SAME address so
    //    node-0 can still reach it). ──
    nodes[1].manager.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let n1_id = nodes[1].id.clone();
    let n1_graph_addr = nodes[1].graph_addr.clone();
    let n1_hb_addr = nodes[1].hb_addr.clone();
    let fresh_graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));
    let config = ClusterConfig {
        node_id: n1_id.clone(),
        listen_addr: n1_graph_addr.clone(),
        heartbeat_addr: n1_hb_addr.clone(),
        total_shards: TOTAL,
        peers: vec![PeerConfig {
            node_id: nodes[0].id.clone(),
            graph_addr: nodes[0].graph_addr.clone(),
            heartbeat_addr: nodes[0].hb_addr.clone(),
        }],
        heartbeat_interval: Duration::from_millis(150),
        failure_timeout: Duration::from_secs(1),
        replication_factor: 1,
        replication_log_dir: None,
        shard_map_dir: None,
        anti_entropy_interval: None,
    };
    let mut fresh_mgr = ClusterManager::new(config);
    let fresh_adapter = Arc::new(GraphServiceAdapter::with_fence_and_log(
        fresh_graph.clone(),
        fresh_mgr.fence(),
        fresh_mgr.replication_log(),
    ));
    fresh_mgr.start(fresh_adapter).await.unwrap();

    // The restarted node starts empty.
    assert_eq!(
        shard_merkle_root(&fresh_graph, shard, TOTAL).await,
        None,
        "restarted node must start with no shard data"
    );

    // Recovery: catch the shard up from node-0.
    let result = fresh_mgr
        .catch_up_shard(&nodes[0].id, shard)
        .await
        .expect("catch-up after restart must succeed");
    assert_eq!(result.nodes_applied, 4);
    assert_eq!(result.edges_applied, 1);

    // Consistency oracle: restarted node's shard now matches the source exactly.
    let recovered_root = shard_merkle_root(&fresh_graph, shard, TOTAL).await;
    assert_eq!(
        recovered_root, source_root,
        "restarted node must re-attain identical shard state after catch-up"
    );

    fresh_mgr.shutdown();
    for n in &nodes {
        n.manager.shutdown();
    }
}

// ── Scenario 3: divergence detection — the Merkle oracle actually catches a
//    single differing key (guards against a vacuous "always equal" oracle) ───

/// Sanity guard: if two replicas differ by a single key/value, their Merkle
/// roots must differ. Without this, the consistency assertions above could pass
/// vacuously (e.g. if the root were always `None` or a constant).
#[tokio::test]
async fn chaos_merkle_oracle_detects_single_key_divergence() {
    const TOTAL: usize = 8;
    let a = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));
    let b = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    // Same shard, same keys, same values → identical roots.
    let shard = {
        let probe = NexoraId::from_bytes(b"div-0".to_vec());
        probe.shard_key() as usize % TOTAL
    };
    let mut keys = Vec::new();
    for i in 0..200_000u64 {
        let qid = NexoraId::from_bytes(format!("div-{i}").into_bytes());
        if qid.shard_key() as usize % TOTAL == shard {
            keys.push(qid);
            if keys.len() == 3 {
                break;
            }
        }
    }
    for (i, qid) in keys.iter().enumerate() {
        a.set_property(qid, "v", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        b.set_property(qid, "v", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }
    let ra = shard_merkle_root(&a, shard, TOTAL).await;
    let rb = shard_merkle_root(&b, shard, TOTAL).await;
    assert_eq!(ra, rb, "identical replicas must have equal Merkle roots");

    // Diverge one key on b only.
    b.set_property(&keys[1], "v", PropertyValue::Integer(999))
        .await
        .unwrap();
    let rb2 = shard_merkle_root(&b, shard, TOTAL).await;
    assert_ne!(
        ra, rb2,
        "a single divergent key MUST change the Merkle root (oracle is not vacuous)"
    );
}

// ── Scenario 4: failover AUTO-triggers catch-up → a lagging replica that gets
//    promoted reconciles the writes it missed, without manual intervention ────

/// A follower can lag (miss a write that other replicas got). When the owner
/// dies and the failure detector promotes that lagging follower, it must
/// AUTOMATICALLY catch up from a surviving replica — the new owner ends up with
/// the missed write in its own graph, with no manual `catch_up_shard` call.
///
/// Construction: RF=3 (node-0 owner, node-1 & node-2 followers). We replicate a
/// baseline write to all three, then a SECOND write only to node-0 + node-2
/// (node-1 lags, missing it). Killing node-0 promotes node-1 (first alive
/// replica). node-1's detector must auto-catch-up from node-2 and gain the
/// missed key — verified by a matching Merkle root against node-2.
#[tokio::test]
async fn chaos_failover_auto_catch_up_reconciles_lagging_replica() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(3, TOTAL, 3).await;

    // Shard owned by node-0 with node-1 & node-2 as followers.
    let map0 = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let (baseline_key, shard) = {
        let mut found = None;
        for i in 0..100_000u64 {
            let qid = NexoraId::from_bytes(format!("acu-{i}").into_bytes());
            let s = qid.shard_key() as usize % TOTAL;
            if map0.get(s).map(|a| a.owner.as_str()) == Some("node-0") {
                found = Some((qid, s));
                break;
            }
        }
        found.expect("node-0 must own a shard")
    };
    let asg = map0.get(shard).unwrap().clone();
    assert_eq!(asg.owner, "node-0");
    // Failover promotes the FIRST alive replica; make that node-1 by ordering.
    let promoted = asg.replicas[0].clone();
    let other_replica = asg.replicas[1].clone();
    let promoted_idx: usize = promoted.strip_prefix("node-").unwrap().parse().unwrap();
    let other_idx: usize = other_replica
        .strip_prefix("node-")
        .unwrap()
        .parse()
        .unwrap();

    // We drive replication with RAW FencedWrites via node-0's remote client, so
    // we control exactly which follower records which seq. This faithfully
    // models a laggard that was briefly unreachable for one replicated write —
    // the missed write lands in the OTHER replica's log (as a real replicated
    // op), which is precisely what incremental catch-up must ship. (Writing
    // directly to graphs would bypass the replication log and mis-model lag.)
    use nexora_zenoh::RemoteGraphClient;
    let rc = nodes[0].manager.remote_client();
    let epoch = asg.epoch;
    let send_fenced = |target: String, seq: u64, key: NexoraId, val: i64| {
        let op = GraphOperation::FencedWrite {
            shard_id: shard,
            epoch,
            seq,
            inner: Box::new(GraphOperation::SetProperty {
                qid: key,
                key: "v".into(),
                value: serde_json::json!(val),
            }),
        };
        async move { rc.execute(&target, op).await }
    };

    // Baseline (seq 1): replicate to BOTH followers → both record high_water=1.
    send_fenced(promoted.clone(), 1, baseline_key.clone(), 0)
        .await
        .unwrap();
    send_fenced(other_replica.clone(), 1, baseline_key.clone(), 0)
        .await
        .unwrap();

    // Missed write (seq 2): replicate ONLY to the OTHER replica → it records
    // high_water=2 with the op; the promoted replica never sees it and stays at
    // high_water=1 (it lags by exactly one replicated write).
    let missed_key = {
        let mut k = None;
        for i in 0..300_000u64 {
            let qid = NexoraId::from_bytes(format!("acu-missed-{i}").into_bytes());
            if qid.shard_key() as usize % TOTAL == shard {
                k = Some(qid);
                break;
            }
        }
        k.unwrap()
    };
    send_fenced(other_replica.clone(), 2, missed_key.clone(), 42)
        .await
        .unwrap();

    // Pre-fault: the promoted (lagging) replica diverges from the up-to-date one.
    let root_promoted_pre = shard_merkle_root(&nodes[promoted_idx].graph, shard, TOTAL).await;
    let root_other_pre = shard_merkle_root(&nodes[other_idx].graph, shard, TOTAL).await;
    assert_ne!(
        root_promoted_pre, root_other_pre,
        "the promoted replica must be lagging (missing a key) pre-fault"
    );
    assert_eq!(
        nodes[promoted_idx]
            .graph
            .get_property(&missed_key, "v")
            .await
            .unwrap(),
        None,
        "promoted replica must not yet have the missed key"
    );

    // ── FAULT: kill node-0. Detector on survivors promotes `promoted` and it
    //    must AUTO-catch-up from `other_replica`. ──
    nodes[0].manager.shutdown();

    // Wait for detection (1s timeout) + failover + auto catch-up to settle.
    tokio::time::sleep(Duration::from_millis(3500)).await;

    // The promoted node is the new owner.
    let map_after = nodes[other_idx]
        .manager
        .router_arc()
        .shard_map_snapshot()
        .await;
    let asg_after = map_after.get(shard).unwrap();
    assert_eq!(
        asg_after.owner, promoted,
        "the first alive replica must have been promoted"
    );

    // AUTO catch-up: the promoted node must now hold the previously-missed key,
    // WITHOUT any manual catch_up_shard call.
    assert_eq!(
        nodes[promoted_idx]
            .graph
            .get_property(&missed_key, "v")
            .await
            .unwrap(),
        Some(PropertyValue::Integer(42)),
        "promoted node must have auto-caught-up the missed key from a surviving replica"
    );

    // Consistency oracle: promoted node's shard now matches the up-to-date replica.
    let root_promoted_after = shard_merkle_root(&nodes[promoted_idx].graph, shard, TOTAL).await;
    let root_other_after = shard_merkle_root(&nodes[other_idx].graph, shard, TOTAL).await;
    assert_eq!(
        root_promoted_after, root_other_after,
        "after auto catch-up, the new owner must be consistent with the surviving replica"
    );

    for n in &nodes {
        n.manager.shutdown();
    }
}
