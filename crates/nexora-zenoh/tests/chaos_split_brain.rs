//! C2: Anti-split-brain validation — data-plane quorum guard.
//!
//! These tests exercise the `quorum_guard` path at the `ControlPlane` level:
//! when a node is partitioned to the minority side (sees fewer than ⌈N/2⌉+1
//! voters alive), every control-plane write (`propose_shard_map_update`,
//! `failover_shard`, `failover_shard_auto`, `add_node`, `remove_node`) must
//! return `Err(ControlError::NoQuorum)` — the cluster must refuse, not split.
//!
//! Unlike `multi_voter_raft_e2e.rs` (which tests the openraft leadership path),
//! these tests use the hand-rolled quorum fallback (no `replication_log_dir`) so
//! they run fast without network Raft election timeouts.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::control::ControlError;
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
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
    manager: ClusterManager,
    #[allow(dead_code)] // E1: Chaos test fixture, id reserved for future assertions
    id: String,
}

/// Build a 3-node cluster (hand-rolled quorum path, no openraft).
/// All nodes start healthy; tests inject failures before asserting.
async fn start_3node_cluster() -> Vec<Node> {
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
            manager,
            id: ids[i].clone(),
        });
    }

    sleep(Duration::from_millis(200)).await;

    // Prime health state: all nodes alive on all nodes, so subsequent writes
    // on the majority nodes succeed and partition simulation is clean.
    for node in &nodes {
        for id in &ids {
            node.manager.control_plane().mark_node_alive(id).await;
        }
    }

    nodes
}

// ---------------------------------------------------------------------------
// C2-1: minority node cannot commit propose_shard_map_update (direct API)
// ---------------------------------------------------------------------------

/// C2: A minority-partitioned node (1 of 3 voters) is refused `propose_shard_map_update`
/// with `NoQuorum`.  This validates the data-plane write guard fail-closed semantic.
#[tokio::test]
async fn minority_node_shard_map_update_refused() {
    let nodes = start_3node_cluster().await;
    let minority = &nodes[0];

    // Partition node-0: it only sees itself alive.
    minority
        .manager
        .control_plane()
        .mark_node_failed("node-1")
        .await;
    minority
        .manager
        .control_plane()
        .mark_node_failed("node-2")
        .await;

    assert!(
        !minority.manager.control_plane().quorum_healthy().await,
        "node-0 should lose quorum after both peers fail"
    );

    // Any shard-map write must be refused.
    let mut proposed = minority.manager.control_plane().get_shard_map().await;
    proposed.version += 1;
    let result = minority
        .manager
        .control_plane()
        .propose_shard_map_update(proposed)
        .await;

    assert!(
        matches!(result, Err(ControlError::NoQuorum { .. })),
        "minority shard-map update must return NoQuorum, got: {:?}",
        result
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}

// ---------------------------------------------------------------------------
// C2-2: minority node cannot commit failover_shard
// ---------------------------------------------------------------------------

/// C2: Minority-side `failover_shard` returns `NoQuorum` — the node must not
/// seize ownership of a shard while the majority is simultaneously doing the
/// same failover (split-brain prevention).
#[tokio::test]
async fn minority_node_failover_refused() {
    let nodes = start_3node_cluster().await;
    let minority = &nodes[0];

    minority
        .manager
        .control_plane()
        .mark_node_failed("node-1")
        .await;
    minority
        .manager
        .control_plane()
        .mark_node_failed("node-2")
        .await;

    let failover = minority
        .manager
        .control_plane()
        .failover_shard(0, "node-0".to_string())
        .await;

    assert!(
        matches!(failover, Err(ControlError::NoQuorum { .. })),
        "minority failover_shard must return NoQuorum, got: {:?}",
        failover
    );

    let auto = minority
        .manager
        .control_plane()
        .failover_shard_auto(0)
        .await;

    assert!(
        matches!(auto, Err(ControlError::NoQuorum { .. })),
        "minority failover_shard_auto must return NoQuorum, got: {:?}",
        auto
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}

// ---------------------------------------------------------------------------
// C2-3: minority node cannot commit add_node / remove_node membership changes
// ---------------------------------------------------------------------------

/// C2: Membership changes (`add_node`, `remove_node`) on a minority-partitioned
/// node return `NoQuorum`.  Without this guard, both sides of a partition could
/// each reconfigure membership independently, causing permanent divergence.
#[tokio::test]
async fn minority_node_membership_changes_refused() {
    let nodes = start_3node_cluster().await;
    let minority = &nodes[0];

    minority
        .manager
        .control_plane()
        .mark_node_failed("node-1")
        .await;
    minority
        .manager
        .control_plane()
        .mark_node_failed("node-2")
        .await;

    let add_result = minority
        .manager
        .control_plane()
        .add_node("node-99".to_string())
        .await;

    assert!(
        matches!(add_result, Err(ControlError::NoQuorum { .. })),
        "minority add_node must return NoQuorum, got: {:?}",
        add_result
    );

    let remove_result = minority.manager.control_plane().remove_node("node-1").await;

    assert!(
        matches!(remove_result, Err(ControlError::NoQuorum { .. })),
        "minority remove_node must return NoQuorum, got: {:?}",
        remove_result
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}

// ---------------------------------------------------------------------------
// C2-4: majority side succeeds while minority is blocked
// ---------------------------------------------------------------------------

/// C2: While the minority (node-0) is blocked, the majority (node-1, node-2)
/// can still commit shard-map changes — the guard only blocks the minority.
/// This confirms the guard is fail-closed on the correct side.
#[tokio::test]
async fn majority_side_still_commits_during_partition() {
    let nodes = start_3node_cluster().await;

    let ids = ["node-0", "node-1", "node-2"];

    // node-0 sees itself alone → minority.
    nodes[0]
        .manager
        .control_plane()
        .mark_node_failed("node-1")
        .await;
    nodes[0]
        .manager
        .control_plane()
        .mark_node_failed("node-2")
        .await;

    // node-1 still sees node-1 + node-2 → majority.
    for id in &ids {
        nodes[1].manager.control_plane().mark_node_alive(id).await;
    }

    // Majority write must succeed.
    let mut proposed = nodes[1].manager.control_plane().get_shard_map().await;
    proposed.version += 1;
    let majority_result = nodes[1]
        .manager
        .control_plane()
        .propose_shard_map_update(proposed)
        .await;

    assert!(
        majority_result.is_ok(),
        "majority shard-map update must succeed, got: {:?}",
        majority_result
    );

    // Minority write is still blocked.
    let mut minority_proposed = nodes[0].manager.control_plane().get_shard_map().await;
    minority_proposed.version += 999;
    let minority_result = nodes[0]
        .manager
        .control_plane()
        .propose_shard_map_update(minority_proposed)
        .await;

    assert!(
        matches!(minority_result, Err(ControlError::NoQuorum { .. })),
        "minority must still be blocked, got: {:?}",
        minority_result
    );

    for node in &nodes {
        node.manager.shutdown();
    }
}
