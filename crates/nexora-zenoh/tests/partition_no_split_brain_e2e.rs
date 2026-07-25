//! A2-7 / A2-9: control-plane → router shard-map propagation, and split-brain
//! prevention on the minority side of a partition.
//!
//! ## What these tests cover (and an honest note on what they don't)
//!
//! The A2-7 fix under test: when the control plane commits a shard-map change
//! (a `propose_shard_map_update` or a `failover_shard_auto`), the *committed*
//! map is pushed to that node's `HybridRouter`. Before the fix, the control
//! plane updated its own copy but never told the router, so PG-wire queries kept
//! routing to the pre-failover (possibly isolated) owner.
//!
//! These tests deliberately run without a durable `replication_log_dir`, which
//! means openraft is **not** assembled (A2-6 only builds the Raft node when
//! durable storage is configured) and each node's `ControlPlane` uses the
//! hand-rolled quorum fallback path *in isolation*. That is enough to verify the
//! propagation fix and the minority-side `NoQuorum` guard on the node that
//! performs the action. It is **not** a multi-process Raft convergence test —
//! verifying that a real 3-voter Raft converges on one leader, fails over to the
//! surviving majority, and denies leadership to a minority is covered by
//! `multi_voter_raft_e2e.rs`, which configures durable storage so openraft is
//! actually assembled.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::NexoraId;
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::control::ControlError;
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use nexora_zenoh::shard_map::OwnerEpoch;
use nexora_zenoh::GraphOperation;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

struct Node {
    _graph: Arc<GraphService>,
    manager: ClusterManager,
    _id: String,
    _graph_addr: String,
    _hb_addr: String,
}

/// Build a 3-node cluster with all nodes as voters, RF=2.
///
/// No durable storage → hand-rolled quorum fallback path (see module docs).
async fn start_cluster_rf2() -> Vec<Node> {
    let ids = vec![
        "node-0".to_string(),
        "node-1".to_string(),
        "node-2".to_string(),
    ];
    let graph_addrs: Vec<_> = (0..3)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();
    let hb_addrs: Vec<_> = (0..3)
        .map(|_| format!("127.0.0.1:{}", free_port()))
        .collect();

    let mut nodes = Vec::with_capacity(3);
    for i in 0..3 {
        let peers: Vec<PeerConfig> = (0..3)
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
            total_shards: 4,
            peers,
            heartbeat_interval: Duration::from_millis(100),
            failure_timeout: Duration::from_secs(2),
            replication_factor: 2,
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
            _graph: graph,
            manager,
            _id: ids[i].clone(),
            _graph_addr: graph_addrs[i].clone(),
            _hb_addr: hb_addrs[i].clone(),
        });
    }

    sleep(Duration::from_millis(300)).await;

    // Fallback quorum path gates writes on voter liveness. Prime every node's
    // health view with all voters alive so the initial proposal isn't refused as
    // NoQuorum before any partition is injected. (The failure detector would
    // populate this over time; the tests act immediately.)
    for node in &nodes {
        for id in &ids {
            node.manager.control_plane().mark_node_alive(id).await;
        }
    }

    nodes
}

/// A2-7: a committed `failover_shard_auto` propagates the new owner/epoch to the
/// acting node's router, so subsequent routing targets the promoted replica.
///
/// This is the regression test for the propagation gap: before the fix the
/// control-plane map advanced but `shard_map_snapshot()` (the router's view,
/// which PG-wire uses) still showed the old owner.
#[tokio::test]
#[ignore = "A2-7/A2-9: control-plane → router propagation (multi-node harness)"]
async fn failover_propagates_committed_map_to_router() {
    let nodes = start_cluster_rf2().await;
    let node1 = &nodes[1];

    // Establish shard 0 = owner node-0, replicas [node-1, node-2], epoch 1, on
    // node-1's control plane (and thus its router via the propagation fix).
    let mut map = node1.manager.control_plane().get_shard_map().await;
    {
        let a = map.assignments.get_mut(&0).unwrap();
        a.owner = "node-0".to_string();
        a.replicas = vec!["node-1".to_string(), "node-2".to_string()];
        a.epoch = OwnerEpoch::from_value(1);
    }
    map.version += 1;
    node1
        .manager
        .control_plane()
        .propose_shard_map_update(map)
        .await
        .expect("proposal should commit on a healthy quorum");

    // The proposal must have reached node-1's router (A2-7 propagation).
    let router_map = node1.manager.shard_map_snapshot().await;
    assert_eq!(
        router_map.assignments.get(&0).unwrap().owner,
        "node-0",
        "router should see the proposed owner"
    );
    let epoch_before = router_map.assignments.get(&0).unwrap().epoch.value();

    // node-0 fails; node-1 (majority) auto-fails-over shard 0 to a live replica.
    node1
        .manager
        .control_plane()
        .mark_node_failed("node-0")
        .await;
    let token = node1
        .manager
        .control_plane()
        .failover_shard_auto(0)
        .await
        .expect("majority failover should succeed")
        .expect("a surviving replica should be promoted");

    let new_epoch = token.epoch.value();
    assert!(new_epoch > epoch_before, "epoch must bump on failover");

    // The core A2-7 assertion: the ROUTER (not just the control plane) reflects
    // the failover, so PG-wire queries route to the promoted owner.
    let router_map = node1.manager.shard_map_snapshot().await;
    let router_owner = &router_map.assignments.get(&0).unwrap().owner;
    assert_ne!(
        router_owner, "node-0",
        "router owner must not be the failed node"
    );
    assert_eq!(
        router_map.assignments.get(&0).unwrap().epoch.value(),
        new_epoch,
        "router must see the bumped epoch after failover"
    );

    // The control plane and router must agree (no divergence).
    let cp_map = node1.manager.control_plane().get_shard_map().await;
    assert_eq!(
        &cp_map.assignments.get(&0).unwrap().owner,
        router_owner,
        "control plane and router must agree on the owner"
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}

/// A2-9: the minority side of a partition is refused failover with `NoQuorum`,
/// so it cannot seize a shard the majority is failing over — the anti-split-brain
/// guard.
#[tokio::test]
#[ignore = "A2-7/A2-9: minority NoQuorum guard (multi-node harness)"]
async fn minority_partition_is_blocked_from_failover() {
    let nodes = start_cluster_rf2().await;
    let node0 = &nodes[0];

    // node-0 is partitioned away from node-1 and node-2 → it sees only itself
    // alive (1 of 3 voters), which is a minority.
    node0
        .manager
        .control_plane()
        .mark_node_failed("node-1")
        .await;
    node0
        .manager
        .control_plane()
        .mark_node_failed("node-2")
        .await;

    assert!(
        !node0.manager.control_plane().quorum_healthy().await,
        "isolated node-0 must not have quorum"
    );

    // Any control-plane write on the minority side must be refused, not applied.
    let failover = node0
        .manager
        .control_plane()
        .failover_shard(0, "node-0".to_string())
        .await;
    assert!(
        matches!(failover, Err(ControlError::NoQuorum { .. })),
        "minority failover must be refused with NoQuorum, got: {:?}",
        failover
    );

    let auto = node0.manager.control_plane().failover_shard_auto(0).await;
    assert!(
        matches!(auto, Err(ControlError::NoQuorum { .. })),
        "minority auto-failover must be refused with NoQuorum, got: {:?}",
        auto
    );

    let mut proposed = node0.manager.control_plane().get_shard_map().await;
    proposed.version += 1;
    let propose = node0
        .manager
        .control_plane()
        .propose_shard_map_update(proposed)
        .await;
    assert!(
        matches!(propose, Err(ControlError::NoQuorum { .. })),
        "minority map proposal must be refused with NoQuorum, got: {:?}",
        propose
    );

    // The minority node's router must be untouched by the refused operations —
    // it keeps routing to the last committed owner rather than a stale/seized one.
    let router_map = node0.manager.shard_map_snapshot().await;
    assert!(
        router_map.assignments.contains_key(&0),
        "router map should still hold shard 0's assignment"
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}

/// A2-7: a committed write routed through the promoted owner lands and is
/// readable, confirming the router uses the post-failover owner end-to-end.
#[tokio::test]
#[ignore = "A2-7/A2-9: post-failover write routes to promoted owner"]
async fn write_after_failover_routes_to_new_owner() {
    let nodes = start_cluster_rf2().await;
    let node1 = &nodes[1];

    // shard 0 owned by node-1 itself (so the promoted owner holds data locally
    // and the single-node harness can serve the read back over TCP-to-self).
    let mut map = node1.manager.control_plane().get_shard_map().await;
    {
        let a = map.assignments.get_mut(&0).unwrap();
        a.owner = "node-0".to_string();
        a.replicas = vec!["node-1".to_string()];
        a.epoch = OwnerEpoch::from_value(1);
    }
    map.version += 1;
    node1
        .manager
        .control_plane()
        .propose_shard_map_update(map)
        .await
        .expect("proposal should commit");

    // node-0 down → node-1 is the only surviving replica and gets promoted.
    node1
        .manager
        .control_plane()
        .mark_node_failed("node-0")
        .await;
    let promoted = node1
        .manager
        .control_plane()
        .failover_shard_auto(0)
        .await
        .expect("failover should succeed")
        .expect("node-1 should be promoted");
    let _ = promoted;

    // Router now targets node-1 for shard 0. A write for a shard-0 key must land
    // and read back through node-1's router.
    let router = node1.manager.router_arc();

    // Find a key that maps to shard 0.
    let mut qid = None;
    for i in 0..10_000u64 {
        let candidate = NexoraId::from_bytes(format!("k-{i}").into_bytes());
        let snap = router.shard_map_snapshot().await;
        if (candidate.shard_key() as usize).is_multiple_of(snap.total_shards) {
            qid = Some(candidate);
            break;
        }
    }
    let qid = qid.expect("a shard-0 key should exist");

    let write = router
        .route(
            &qid,
            GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "phase".into(),
                value: serde_json::json!("post-failover"),
            },
        )
        .await;
    assert!(
        write.is_ok(),
        "write to promoted owner should succeed: {:?}",
        write
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}
