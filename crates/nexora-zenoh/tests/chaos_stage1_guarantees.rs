//! E1: Chaos tests for Stage 1 correctness guarantees.
//!
//! These tests verify the distributed consistency guarantees added in Stage 1:
//! - A1.1: Two-phase commit (quorum before owner write)
//! - A1.2: Strict W+R>N (Majority reads see latest committed)
//! - A1.3: Failover catch-up (already covered by chaos_consistency.rs)
//! - A4: WAL torn-write repair (already covered by core/tests/chaos.rs)
//!
//! Infrastructure reused from `chaos_consistency.rs`: start_cluster, shard_merkle_root.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_raft::MerkleTree;
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use nexora_zenoh::{GraphOperation, ReadConcern};
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
    #[allow(dead_code)] // E1: Chaos test fixture, id reserved for future assertions
    id: String,
    _graph_addr: String,
    _hb_addr: String,
}

/// Build+start `n` fully-peered real nodes with replication factor `rf`.
/// Reused from chaos_consistency.rs.
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
            _graph_addr: graph_addrs[i].clone(),
            _hb_addr: hb_addrs[i].clone(),
        });
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    nodes
}

/// Compute a Merkle root over a node's data for a single cluster shard.
/// Reused from chaos_consistency.rs.
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

// ──────────────────────────────────────────────────────────────────────────
// A1.2: Strict W+R>N — Majority read sees latest committed write immediately
// ──────────────────────────────────────────────────────────────────────────

/// RF=3 cluster. Write a key with quorum commit, then IMMEDIATELY read it back
/// with ReadConcern::Majority from a different node. The read MUST see the
/// latest committed value (W+R>N guarantee: if W=2 and R=2, at least one
/// replica in the read quorum must overlap with the write quorum).
///
/// This is the foundational linearizability test for distributed reads.
#[tokio::test]
#[ignore = "E1: A1.2 Majority read sees latest committed (multi-node)"]
async fn majority_read_sees_latest_committed() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(3, TOTAL, 3).await;

    // Find a shard owned by node-0 with node-1 and node-2 as replicas.
    let map0 = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let (key, shard) = {
        let mut found = None;
        for i in 0..100_000u64 {
            let qid = NexoraId::from_bytes(format!("read-{i}").into_bytes());
            let s = qid.shard_key() as usize % TOTAL;
            if map0.get(s).map(|a| a.owner.as_str()) == Some("node-0") {
                found = Some((qid, s));
                break;
            }
        }
        found.expect("node-0 must own a shard")
    };
    let asg = map0.get(shard).unwrap().clone();
    assert_eq!(asg.replicas.len(), 2, "RF=3 → 2 followers");

    // Write a value from node-0 (owner) with quorum replication.
    // The owner applies locally FIRST (quorum_write only handles follower
    // replication — it assumes the owner's local write already succeeded, per
    // the replica_writer contract), then quorum-replicates to followers.
    nodes[0]
        .graph
        .set_property(&key, "v", PropertyValue::Integer(42))
        .await
        .unwrap();
    let writer = nodes[0].manager.replica_writer_arc();
    let write_result = writer
        .quorum_write(
            shard,
            &nexora_zenoh::replication::FencingToken::new(shard, asg.epoch),
            GraphOperation::SetProperty {
                qid: key.clone(),
                key: "v".into(),
                value: serde_json::json!(42),
            },
        )
        .await
        .unwrap();

    // Verify write reached quorum (committed).
    assert!(
        matches!(
            write_result,
            nexora_zenoh::replication::WriteStatus::CommittedQuorum { .. }
        ),
        "write must reach quorum, got {write_result:?}"
    );

    // IMMEDIATELY read from node-1 (a follower, not the owner) with Majority concern.
    // The W+R>N guarantee (W=2, R=2, N=3 → 2+2>3) ensures node-1's quorum read
    // overlaps with the write quorum and MUST see the committed value.
    let read_op = GraphOperation::GetProperty {
        qid: key.clone(),
        key: "v".into(),
    };
    let router1 = nodes[1].manager.router_arc();
    let read_result =
        nexora_zenoh::read_with_concern(&router1, &key, read_op, ReadConcern::Majority, None, None)
            .await;

    // The read must succeed and return the latest committed value (42).
    let graph_result = read_result.expect("Majority read must succeed");

    // GraphResult::Property wraps an Option<serde_json::Value>. GetProperty
    // returns the raw JSON scalar (e.g. `42`), not a tagged PropertyValue, so
    // compare against the JSON value directly.
    match graph_result {
        nexora_zenoh::GraphResult::Property(Some(json_val)) => {
            assert_eq!(
                json_val,
                serde_json::json!(42),
                "Majority read MUST see the latest committed write (W+R>N guarantee)"
            );
        }
        other => panic!("Expected Property(Some(42)), got {:?}", other),
    }

    for n in &nodes {
        n.manager.shutdown();
    }
}

// ──────────────────────────────────────────────────────────────────────────
// A1.1: Two-Phase Commit — Quorum loss leaves owner clean (no dirty data)
// ──────────────────────────────────────────────────────────────────────────

/// RF=3 cluster. Attempt a two-phase write but kill 2 followers BEFORE the
/// write starts (quorum impossible). The write must fail, and the owner's
/// local graph must NOT contain the uncommitted data (two-phase guarantee:
/// owner write happens AFTER quorum, so quorum failure = owner stays clean).
///
/// **IMPLEMENTATION NOTE:** This test assumes `quorum_write_two_phase` is
/// wired into the production write path. If the router currently uses the
/// best-effort `quorum_write` (which writes owner first), this test documents
/// the gap and will fail with a different assertion message. The test is left
/// as-is to verify the INTENDED behavior once two-phase is fully integrated.
#[tokio::test]
#[ignore = "E1: A1.1 Two-phase commit no partial on quorum loss (multi-node)"]
async fn two_phase_commit_no_partial_on_quorum_loss() {
    const TOTAL: usize = 8;
    let nodes = start_cluster(3, TOTAL, 3).await;

    // Find a shard owned by node-0.
    let map0 = nodes[0].manager.router_arc().shard_map_snapshot().await;
    let (key, shard) = {
        let mut found = None;
        for i in 0..100_000u64 {
            let qid = NexoraId::from_bytes(format!("2pc-{i}").into_bytes());
            let s = qid.shard_key() as usize % TOTAL;
            if map0.get(s).map(|a| a.owner.as_str()) == Some("node-0") {
                found = Some((qid, s));
                break;
            }
        }
        found.expect("node-0 must own a shard")
    };
    let asg = map0.get(shard).unwrap().clone();

    // ── FAULT: Kill 2 followers before the write → quorum impossible (1/3 alive). ──
    nodes[1].manager.shutdown();
    nodes[2].manager.shutdown();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Attempt a two-phase write from node-0. It must fail (no quorum).
    let writer = nodes[0].manager.replica_writer_arc();
    let write_result = writer
        .quorum_write_two_phase(
            shard,
            &nexora_zenoh::replication::FencingToken::new(shard, asg.epoch),
            GraphOperation::SetProperty {
                qid: key.clone(),
                key: "v".into(),
                value: serde_json::json!(99),
            },
        )
        .await;

    // The write should fail (quorum not reached).
    match write_result {
        Ok(status) => {
            assert!(
                matches!(
                    status,
                    nexora_zenoh::replication::WriteStatus::Failed { .. }
                ),
                "two-phase write with no quorum must fail, got {status:?}"
            );
        }
        Err(e) => {
            // Also acceptable: immediate error due to no reachable followers.
            println!("two-phase write failed with error (acceptable): {e:?}");
        }
    }

    // The CORE two-phase guarantee: owner's graph must NOT contain the key
    // (quorum was never reached, so the owner write in phase 2 never happened).
    let owner_value = nodes[0].graph.get_property(&key, "v").await.unwrap();
    assert_eq!(
        owner_value, None,
        "two-phase commit MUST NOT write to owner if quorum fails (owner must stay clean)"
    );

    // IMPLEMENTATION GAP MARKER: If the test fails here with owner_value = Some(99),
    // it means the production write path is still using best-effort `quorum_write`
    // (which writes owner first) rather than `quorum_write_two_phase`. Document
    // this in the final report as "A1.1 two-phase not yet wired into write path".

    nodes[0].manager.shutdown();
}

// ──────────────────────────────────────────────────────────────────────────
// A1.3: Failover catch-up for lagging followers
// ──────────────────────────────────────────────────────────────────────────

// Already fully covered by `chaos_consistency.rs::chaos_failover_auto_catch_up_reconciles_lagging_replica`.
// No additional test needed here — that test verifies:
// - A follower misses a replicated write (lags behind)
// - Owner dies → lagging follower gets promoted
// - Promoted node AUTO catches up from surviving replica
// - Final state matches (Merkle consistency)

// ──────────────────────────────────────────────────────────────────────────
// Consistency sanity: Verify the Merkle oracle works in a Stage 1 context
// ──────────────────────────────────────────────────────────────────────────

/// Sanity check that our Merkle consistency oracle works correctly for Stage 1
/// tests. Two replicas with identical writes must have matching Merkle roots;
/// a single differing property must cause divergence.
#[tokio::test]
async fn stage1_merkle_oracle_sanity() {
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

    // Identical writes → identical roots.
    let shard = {
        let probe = NexoraId::from_bytes(b"stg1-0".to_vec());
        probe.shard_key() as usize % TOTAL
    };
    let mut keys = Vec::new();
    for i in 0..50_000u64 {
        let qid = NexoraId::from_bytes(format!("stg1-{i}").into_bytes());
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

    // Diverge one key → roots differ.
    b.set_property(&keys[1], "v", PropertyValue::Integer(999))
        .await
        .unwrap();
    let rb2 = shard_merkle_root(&b, shard, TOTAL).await;
    assert_ne!(
        ra, rb2,
        "a single divergent key MUST change the Merkle root"
    );
}
