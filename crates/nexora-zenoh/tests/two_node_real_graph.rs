//! Phase-1 acceptance: cross-node routing over TWO REAL `GraphService` nodes.
//!
//! Prior "distributed" tests used an in-memory mock `GraphHandler`. This test
//! wires the real engine end to end:
//!   node-A / node-B each = real GraphService → GraphServiceAdapter → TcpGraphServer
//!   a HybridRouter on A, with a distributed ShardMap, routes writes/reads to
//!   whichever node owns the target key's shard.
//!
//! It proves the T1.4/T1.5/T1.7 wiring: a write issued at A for a key owned by
//! B actually lands on B (and vice versa), which is the whole point of Phase 1.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::NexoraId;
use nexora_zenoh::fencing::ShardFence;
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use nexora_zenoh::replica_writer::ReplicaWriter;
use nexora_zenoh::replication::{FencingToken, ReplicaSet, WriteStatus};
use nexora_zenoh::router::HybridRouter;
use nexora_zenoh::shard_map::ShardMap;
use nexora_zenoh::state_transfer::StateTransfer;
use nexora_zenoh::{GraphOperation, GraphResult, OwnerEpoch, TcpGraphServer, TcpRemoteClient};
use std::sync::Arc;

/// Build a real GraphService fronted by a TCP server; return (graph, addr, server).
/// The `TcpGraphServer` is returned so the caller can keep it bound for the test's
/// lifetime — its accept loop is a detached task, but holding the handle keeps the
/// intent explicit and allows an orderly `shutdown()`.
async fn spawn_node() -> (Arc<GraphService>, String, TcpGraphServer) {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));
    let adapter = Arc::new(GraphServiceAdapter::new(graph.clone()));
    let server = TcpGraphServer::new(adapter, "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();
    (graph, addr, server)
}

/// Like [`spawn_node`], but the adapter enforces epoch fencing against the
/// returned [`ShardFence`] — used to prove stale-epoch replicated writes from a
/// deposed owner are rejected receiver-side.
async fn spawn_fenced_node() -> (Arc<GraphService>, String, TcpGraphServer, ShardFence) {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));
    let fence = ShardFence::new();
    let adapter = Arc::new(GraphServiceAdapter::with_fence(
        graph.clone(),
        fence.clone(),
    ));
    let server = TcpGraphServer::new(adapter, "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();
    (graph, addr, server, fence)
}

/// Find a key whose shard (mod `total`) is owned by a given node under a
/// round-robin distributed map over [a, b]. Returns a NexoraId hashing there.
fn key_for_owner(want_owner: &str, a: &str, b: &str, total: usize) -> NexoraId {
    let map = ShardMap::new_distributed(total, &[a.to_string(), b.to_string()], a.to_string());
    for i in 0..10_000u64 {
        let qid = NexoraId::from_bytes(format!("probe-{i}").into_bytes());
        let shard = map.shard_of(&qid);
        if map.get(shard).unwrap().owner == want_owner {
            return qid;
        }
    }
    panic!("no key found for owner {want_owner}");
}

/// A write issued at node-A for a key owned by node-B must land on B's real
/// GraphService, and be readable back through the same router.
#[tokio::test]
async fn write_routes_to_remote_owner_and_reads_back() {
    const TOTAL: usize = 4;
    let (_graph_a, addr_a, _srv_a) = spawn_node().await;
    let (graph_b, addr_b, _srv_b) = spawn_node().await;

    // Router lives on node-A: local shards → A, remote → B via TCP client.
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;

    let shard_map =
        ShardMap::new_distributed(TOTAL, &["node-a".into(), "node-b".into()], "node-a".into());
    let router = HybridRouter::new_clustered(shard_map, client);

    // Pick a key that the distributed map assigns to node-B.
    let key_on_b = key_for_owner("node-b", "node-a", "node-b", TOTAL);
    assert!(
        !router.is_local(&key_on_b).await,
        "test key must be remote (owned by node-b) from node-a's view"
    );

    // Write via the router (from A) — it must be forwarded to B.
    let write = router
        .route(
            &key_on_b,
            GraphOperation::SetProperty {
                qid: key_on_b.clone(),
                key: "name".into(),
                value: serde_json::json!("Alice"),
            },
        )
        .await;
    assert!(write.is_ok(), "remote write failed: {write:?}");

    // The data must physically exist on node-B's real GraphService.
    let on_b = graph_b.get_property(&key_on_b, "name").await.unwrap();
    assert_eq!(
        on_b,
        Some(nexora_id::PropertyValue::String("Alice".into())),
        "the write must land on node-B's local graph"
    );

    // And a read through the router returns it too.
    let read = router
        .route(
            &key_on_b,
            GraphOperation::GetProperty {
                qid: key_on_b.clone(),
                key: "name".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        read,
        GraphResult::Property(Some(serde_json::json!("Alice")))
    );
}

/// The complementary direction: a key owned by node-A stays local and lands on
/// A's graph — confirms the router isn't blindly forwarding everything.
#[tokio::test]
async fn local_owned_key_stays_on_local_node() {
    const TOTAL: usize = 4;
    let (graph_a, addr_a, _srv_a) = spawn_node().await;
    let (graph_b, addr_b, _srv_b) = spawn_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map =
        ShardMap::new_distributed(TOTAL, &["node-a".into(), "node-b".into()], "node-a".into());
    let router = HybridRouter::new_clustered(shard_map, client);

    let key_on_a = key_for_owner("node-a", "node-a", "node-b", TOTAL);
    assert!(
        router.is_local(&key_on_a).await,
        "key must be local to node-a"
    );

    // Writing a local-owned key through the router should NOT appear on B.
    // (The app layer runs local writes on state.graph directly; here we assert
    // the routing decision, then apply locally as the app would.)
    graph_a
        .set_property(
            &key_on_a,
            "name",
            nexora_id::PropertyValue::String("Bob".into()),
        )
        .await
        .unwrap();

    let on_a = graph_a.get_property(&key_on_a, "name").await.unwrap();
    assert_eq!(on_a, Some(nexora_id::PropertyValue::String("Bob".into())));
    let on_b = graph_b.get_property(&key_on_a, "name").await.unwrap();
    assert_eq!(on_b, None, "a local-owned key must not leak onto node-B");
}

// ── Phase 2: quorum replication over real GraphService nodes ──────────────

/// RF=3: an owner write replicated via ReplicaWriter reaches quorum, and the
/// data physically exists on the followers' real graphs — so killing the owner
/// does not lose it. This is the core "node failure ≠ data loss" proof.
#[tokio::test]
async fn replicated_write_survives_owner_death() {
    // Three real nodes: owner + 2 followers.
    let (_g_owner, addr_owner, srv_owner) = spawn_node().await;
    let (g_f1, addr_f1, _srv_f1) = spawn_node().await;
    let (g_f2, addr_f2, _srv_f2) = spawn_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("owner", &addr_owner).await;
    client.register_node("f1", &addr_f1).await;
    client.register_node("f2", &addr_f2).await;

    // ReplicaWriter with a 3-node replica set for shard 0.
    let writer = ReplicaWriter::new(client);
    writer.register_replica_set(ReplicaSet::new(
        0,
        "owner".into(),
        vec!["f1".into(), "f2".into()],
    ));

    let qid = NexoraId::from_bytes(b"replicated-key".to_vec());
    let token = FencingToken::new(0, OwnerEpoch::new());

    // Replicate a SetProperty to the followers (owner is counted as pre-acked,
    // matching the coordinator model where the owner already committed locally).
    let status = writer
        .quorum_write(
            0,
            &token,
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "name".into(),
                value: serde_json::json!("Carol"),
            },
        )
        .await
        .unwrap();
    match status {
        WriteStatus::CommittedQuorum { acked, total } => {
            assert_eq!(total, 3);
            assert!(acked >= 2, "quorum needs >= 2 acks, got {acked}");
        }
        other => panic!("expected CommittedQuorum, got {other:?}"),
    }

    // The data must physically exist on BOTH followers' real graphs.
    let on_f1 = g_f1.get_property(&qid, "name").await.unwrap();
    let on_f2 = g_f2.get_property(&qid, "name").await.unwrap();
    assert_eq!(
        on_f1,
        Some(nexora_id::PropertyValue::String("Carol".into()))
    );
    assert_eq!(
        on_f2,
        Some(nexora_id::PropertyValue::String("Carol".into()))
    );

    // Kill the owner. The data is still readable from a follower → no data loss.
    srv_owner.shutdown();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let still_there = g_f1.get_property(&qid, "name").await.unwrap();
    assert_eq!(
        still_there,
        Some(nexora_id::PropertyValue::String("Carol".into())),
        "data must survive owner death on the follower"
    );
}

/// When followers are unreachable, quorum is NOT reached and the write reports
/// `Failed` — the write path must surface this, never silently "succeed".
#[tokio::test]
async fn quorum_fails_when_followers_unreachable() {
    // Only the owner exists; followers f1/f2 point at dead addresses.
    let (_g_owner, addr_owner, _srv_owner) = spawn_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("owner", &addr_owner).await;
    // Unroutable follower addresses (nothing listening).
    client.register_node("f1", "127.0.0.1:1").await;
    client.register_node("f2", "127.0.0.1:1").await;

    let writer = ReplicaWriter::new(client).with_timeout(std::time::Duration::from_millis(200));
    writer.register_replica_set(ReplicaSet::new(
        0,
        "owner".into(),
        vec!["f1".into(), "f2".into()],
    ));

    let qid = NexoraId::from_bytes(b"lonely-key".to_vec());
    let token = FencingToken::new(0, OwnerEpoch::new());
    let result = writer
        .quorum_write(
            0,
            &token,
            GraphOperation::SetProperty {
                qid,
                key: "name".into(),
                value: serde_json::json!("Dave"),
            },
        )
        .await;

    // Quorum unreachable → the write path surfaces a loud error (RouterError::
    // QuorumFailed) rather than a silent Ok, so callers never mistake a
    // sub-quorum write for success.
    match result {
        Err(nexora_zenoh::RouterError::QuorumFailed { acked, required }) => {
            assert_eq!(acked, 1, "only the owner acked");
            assert_eq!(required, 2, "majority of 3 is 2");
        }
        other => panic!("expected Err(QuorumFailed), got {other:?}"),
    }
}

// ── P1-B: distributed traversal (scatter-gather) over real GraphService ───

/// A multi-hop traversal whose chain crosses shard boundaries must be resolved
/// by scatter-gather querying whichever node owns each hop's shard — proving the
/// traversal executes across both real nodes, not just locally.
#[tokio::test]
async fn scatter_gather_traverses_across_two_real_nodes() {
    const TOTAL: usize = 4;
    let (graph_a, addr_a, _srv_a) = spawn_node().await;
    let (graph_b, addr_b, _srv_b) = spawn_node().await;

    // no_local router (matches production cluster wiring): local shards also
    // route through the client, so BOTH nodes — including "self" — are registered.
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("node-a", &addr_a).await;
    client.register_node("node-b", &addr_b).await;
    let shard_map =
        ShardMap::new_distributed(TOTAL, &["node-a".into(), "node-b".into()], "node-a".into());
    let router = HybridRouter::new_clustered_no_local(shard_map.clone(), client);

    // Build a chain start → mid → end where consecutive nodes land on different
    // shards/owners, so the traversal must hop across nodes. Probe for such ids.
    let start = key_for_owner("node-a", "node-a", "node-b", TOTAL);
    let mid = key_for_owner("node-b", "node-a", "node-b", TOTAL);
    let end = key_for_owner("node-a", "node-a", "node-b", TOTAL);
    // Ensure distinct ids (probe returns the first match; nudge if collided).
    assert_ne!(start, mid);

    // Write the edges on their OWNERS' real graphs directly (edge lives on the
    // source node's shard owner): start-[LINK]->mid on node-a, mid-[LINK]->end
    // on node-b.
    use nexora_value::{HalfEdge, Symbol};
    let owner_of = |qid: &NexoraId| {
        let s = shard_map.shard_of(qid);
        shard_map.get(s).unwrap().owner.clone()
    };
    // start is on node-a
    assert_eq!(owner_of(&start), "node-a");
    graph_a
        .add_edge(&start, HalfEdge::out(Symbol::new("LINK"), mid.clone()))
        .await
        .unwrap();
    // mid is on node-b
    assert_eq!(owner_of(&mid), "node-b");
    graph_b
        .add_edge(&mid, HalfEdge::out(Symbol::new("LINK"), end.clone()))
        .await
        .unwrap();

    // Traverse 2 hops from start: must discover mid (via node-a) and end (via
    // node-b) — crossing the node boundary.
    let reached = router
        .scatter_gather_traverse(vec![start.clone()], "LINK", 2)
        .await
        .unwrap();
    let reached_ids: Vec<Vec<u8>> = reached.iter().map(|n| n.qid.as_bytes().to_vec()).collect();
    assert!(
        reached_ids.contains(&mid.as_bytes().to_vec()),
        "hop 1 (mid, owned by node-b) must be reached"
    );
    assert!(
        reached_ids.contains(&end.as_bytes().to_vec()),
        "hop 2 (end, discovered via node-b's edges) must be reached"
    );
}

// ── P2-A 阶段4-a: epoch fencing on the replicated write path ───────────────

/// A follower must REJECT a replicated write stamped with an epoch older than
/// the highest it has seen for that shard — i.e. a write from an owner that
/// failover has since deposed. Without this, a slow/partitioned old owner would
/// silently corrupt the shard behind the new owner's back.
///
/// Timeline on one real follower node (fence-enforcing):
///   1. New owner (epoch 2) replicates `name=New`  → admitted, lands on graph.
///   2. Deposed owner (epoch 1) replicates `name=Stale` → FENCED OUT, rejected.
///   3. Follower's graph still holds `New` (the stale write never applied).
#[tokio::test]
async fn stale_epoch_write_is_fenced_on_real_follower() {
    let (g_follower, addr_follower, _srv, _fence) = spawn_fenced_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("follower", &addr_follower).await;
    let writer = ReplicaWriter::new(client);
    // One follower for shard 0 (min_ack becomes 2 = owner + this follower).
    writer.register_replica_set(ReplicaSet::new(0, "owner".into(), vec!["follower".into()]));

    let qid = NexoraId::from_bytes(b"fenced-key".to_vec());
    let e1 = OwnerEpoch::new(); // deposed owner's epoch
    let e2 = e1.next(); // post-failover owner's epoch

    // 1. Current owner replicates at epoch 2 — must reach quorum and land.
    let ok = writer
        .quorum_write(
            0,
            &FencingToken::new(0, e2),
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "name".into(),
                value: serde_json::json!("New"),
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(ok, WriteStatus::CommittedQuorum { .. }),
        "current-epoch write must reach quorum, got {ok:?}"
    );
    assert_eq!(
        g_follower.get_property(&qid, "name").await.unwrap(),
        Some(nexora_id::PropertyValue::String("New".into())),
        "epoch-2 write must land on the follower's real graph"
    );

    // 2. Deposed owner replicates a straggler at epoch 1 — the follower fences it
    //    out, so the follower ack fails and quorum is NOT reached. The write path
    //    surfaces this as a loud RouterError::QuorumFailed (not a silent Ok).
    let stale = writer
        .quorum_write(
            0,
            &FencingToken::new(0, e1),
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "name".into(),
                value: serde_json::json!("Stale"),
            },
        )
        .await;
    assert!(
        matches!(stale, Err(nexora_zenoh::RouterError::QuorumFailed { .. })),
        "stale-epoch write must fail quorum (follower rejects it), got {stale:?}"
    );

    // 3. The follower's real graph must still hold the epoch-2 value — the stale
    //    write never applied.
    assert_eq!(
        g_follower.get_property(&qid, "name").await.unwrap(),
        Some(nexora_id::PropertyValue::String("New".into())),
        "stale write must NOT have overwritten the current value"
    );
}

/// Same-epoch and higher-epoch replicated writes are admitted (the fence rejects
/// only strictly-older epochs), so normal replication and a clean failover
/// hand-off both keep working.
#[tokio::test]
async fn current_and_newer_epoch_writes_are_admitted() {
    let (g_follower, addr_follower, _srv, _fence) = spawn_fenced_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("follower", &addr_follower).await;
    let writer = ReplicaWriter::new(client);
    writer.register_replica_set(ReplicaSet::new(0, "owner".into(), vec!["follower".into()]));

    let qid = NexoraId::from_bytes(b"progressing-key".to_vec());
    let e1 = OwnerEpoch::new();
    let e2 = e1.next();

    // Two writes at the same epoch, then one at a higher epoch — all admitted.
    for (epoch, val) in [(e1, "v1"), (e1, "v1b"), (e2, "v2")] {
        let status = writer
            .quorum_write(
                0,
                &FencingToken::new(0, epoch),
                GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: "name".into(),
                    value: serde_json::json!(val),
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(status, WriteStatus::CommittedQuorum { .. }),
            "epoch {} write must reach quorum, got {status:?}",
            epoch.value()
        );
    }
    assert_eq!(
        g_follower.get_property(&qid, "name").await.unwrap(),
        Some(nexora_id::PropertyValue::String("v2".into())),
        "the last (highest-epoch) write wins on the follower"
    );
}

// ── P2-A 阶段3: operation-based state transfer over real GraphService ───────

/// A recovering node must be able to pull a shard's real data from a surviving
/// source and materialize it in its own graph. This is the failover-recovery
/// primitive: after promotion (or a replica rejoin) the new owner catches up
/// before serving.
///
/// Setup: `source` holds several nodes + an edge for cluster shard S. A fresh
/// `recovering` node starts empty. It runs `catch_up_shard` against the source
/// and must end up with the same nodes, properties, and edge in its real graph.
#[tokio::test]
async fn state_transfer_recovers_shard_into_empty_node() {
    use nexora_value::{HalfEdge, Symbol};

    const TOTAL: usize = 4;
    // Source node holds the data; recovering node starts empty.
    let (g_source, addr_source, _srv_source) = spawn_node().await;
    let (g_recovering, addr_recovering, _srv_recovering) = spawn_node().await;

    // Cluster-shard function: qid.shard_key() % TOTAL. Find keys landing on the
    // same shard S so the export filter selects them.
    let map = ShardMap::new_distributed(TOTAL, &["s".into(), "r".into()], "s".into());
    let mut keys: Vec<NexoraId> = Vec::new();
    let target_shard = {
        // Pick the shard of the first probe key and collect keys on that shard.
        let first = NexoraId::from_bytes(b"st-probe-0".to_vec());
        map.shard_of(&first)
    };
    for i in 0..5_000u64 {
        let qid = NexoraId::from_bytes(format!("st-probe-{i}").into_bytes());
        if map.shard_of(&qid) == target_shard {
            keys.push(qid);
            if keys.len() == 3 {
                break;
            }
        }
    }
    assert_eq!(keys.len(), 3, "need 3 keys on the same cluster shard");

    // Populate the source's real graph: properties on all three, one edge
    // key0 -[LINK]-> key1 (source key0 also lands on the target shard).
    for (i, qid) in keys.iter().enumerate() {
        g_source
            .set_property(qid, "idx", nexora_id::PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }
    g_source
        .add_edge(
            &keys[0],
            HalfEdge::out(Symbol::new("LINK"), keys[1].clone()),
        )
        .await
        .unwrap();

    // The recovering node runs state transfer: fetch shard `target_shard` from
    // the source, apply into its own graph (apply_target = recovering node).
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("source", &addr_source).await;
    client.register_node("recovering", &addr_recovering).await;
    let st = StateTransfer::new(client);

    let result = st
        .catch_up_shard("source", "recovering", target_shard, TOTAL)
        .await
        .expect("catch-up must succeed");

    assert_eq!(result.nodes_applied, 3, "all three nodes transferred");
    assert_eq!(result.edges_applied, 1, "the one out-edge transferred");

    // Verify the data physically landed in the recovering node's real graph.
    for (i, qid) in keys.iter().enumerate() {
        let got = g_recovering.get_property(qid, "idx").await.unwrap();
        assert_eq!(
            got,
            Some(nexora_id::PropertyValue::Integer(i as i64)),
            "recovering node must hold property for key {i}"
        );
    }
    let edges = g_recovering.get_edges(&keys[0]).await.unwrap();
    assert!(
        edges
            .iter()
            .any(|e| e.other == keys[1] && e.edge_type.as_str() == "LINK"),
        "recovering node must hold the transferred edge"
    );

    // Sanity: a node on a DIFFERENT shard was never on the source, so it must
    // not appear on the recovering node either (export is shard-scoped).
    let other_shard_key = (0..5_000u64)
        .map(|i| NexoraId::from_bytes(format!("st-other-{i}").into_bytes()))
        .find(|q| map.shard_of(q) != target_shard)
        .unwrap();
    assert_eq!(
        g_recovering
            .get_property(&other_shard_key, "idx")
            .await
            .unwrap(),
        None,
        "keys outside the transferred shard must not appear"
    );
}

/// State transfer against an unreachable source must fail loudly (not silently
/// leave the recovering node with partial/empty data believing it succeeded).
#[tokio::test]
async fn state_transfer_fails_when_source_unreachable() {
    const TOTAL: usize = 4;
    let (_g_recovering, addr_recovering, _srv) = spawn_node().await;

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node("recovering", &addr_recovering).await;
    // Source points at a dead address.
    client.register_node("source", "127.0.0.1:1").await;
    let st = StateTransfer::new(client).with_fetch_timeout(std::time::Duration::from_millis(300));

    let err = st
        .catch_up_shard("source", "recovering", 0, TOTAL)
        .await
        .expect_err("catch-up must fail against a dead source");
    // Either an unreachable-source error or a timeout — both are loud failures.
    let msg = err.to_string();
    assert!(
        msg.contains("unreachable") || msg.contains("timed out"),
        "expected a loud failure, got: {msg}"
    );
}
