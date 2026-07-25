//! A2-8: control-plane degradation — the data plane keeps serving reads/writes
//! even when the control plane has lost quorum.
//!
//! The metadata-write-refusal and failover/anti-split-brain scenarios live in
//! `partition_no_split_brain_e2e.rs`; this file covers the complementary
//! guarantee: losing control-plane quorum must NOT block ordinary graph
//! reads/writes. User data is replayable and best-effort, so the data plane is
//! deliberately decoupled from control-plane consensus (design doc §6.3:
//! "数据平面 best-effort + 可回灌，普通数据读写不必被拖住").
//!
//! Like the partition tests, this runs without durable storage, so the control
//! plane uses the hand-rolled quorum fallback in isolation — sufficient to show
//! that a `quorum_healthy() == false` control plane does not gate `GraphService`
//! data operations, which never consult the control plane at all.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

/// A single-node cluster manager whose control plane can be driven into a
/// no-quorum state, paired with the underlying graph for direct data ops.
async fn start_node() -> (Arc<GraphService>, ClusterManager) {
    let id = "node-0".to_string();
    let graph_addr = format!("127.0.0.1:{}", free_port());
    let hb_addr = format!("127.0.0.1:{}", free_port());

    // Two configured peers that never come up, so the control plane's voter set
    // is {node-0, node-1, node-2} and marking the peers dead drops node-0 into a
    // 1-of-3 minority (no quorum).
    let peers = vec![
        PeerConfig {
            node_id: "node-1".to_string(),
            graph_addr: format!("127.0.0.1:{}", free_port()),
            heartbeat_addr: format!("127.0.0.1:{}", free_port()),
        },
        PeerConfig {
            node_id: "node-2".to_string(),
            graph_addr: format!("127.0.0.1:{}", free_port()),
            heartbeat_addr: format!("127.0.0.1:{}", free_port()),
        },
    ];

    let config = ClusterConfig {
        node_id: id,
        listen_addr: graph_addr,
        heartbeat_addr: hb_addr,
        total_shards: 4,
        peers,
        heartbeat_interval: Duration::from_millis(100),
        failure_timeout: Duration::from_secs(2),
        replication_factor: 1,
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
    let adapter =
        Arc::new(nexora_zenoh::graph_service_adapter::GraphServiceAdapter::new(graph.clone()));
    manager.start(adapter).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    (graph, manager)
}

/// Losing control-plane quorum must not block data-plane reads/writes.
///
/// The graph engine does not consult the control plane for ordinary
/// `set_property`/`get_property`, so a no-quorum control plane leaves data ops
/// fully available (best-effort data plane, design doc §6.3).
#[tokio::test]
#[ignore = "A2-8: data-plane independence from control-plane quorum"]
async fn data_plane_serves_reads_and_writes_without_control_quorum() {
    let (graph, manager) = start_node().await;

    // Baseline: a write succeeds while the control plane is healthy.
    let qid_a = NexoraId::from_bytes(b"device-a".to_vec());
    graph
        .set_property(&qid_a, "phase", PropertyValue::String("healthy".into()))
        .await
        .expect("write should succeed while quorum healthy");

    // Drive the control plane into a no-quorum minority: node-0 sees itself
    // alive but both peers dead (1 of 3 voters).
    manager.control_plane().mark_node_alive("node-0").await;
    manager.control_plane().mark_node_failed("node-1").await;
    manager.control_plane().mark_node_failed("node-2").await;
    assert!(
        !manager.control_plane().quorum_healthy().await,
        "control plane must be in a no-quorum state for this test"
    );

    // Data-plane write must STILL succeed — it never touches the control plane.
    let qid_b = NexoraId::from_bytes(b"device-b".to_vec());
    graph
        .set_property(&qid_b, "phase", PropertyValue::String("no-quorum".into()))
        .await
        .expect("data write must succeed despite control-plane NoQuorum");

    // Data-plane read of both the pre- and post-partition writes must succeed.
    let read_a = graph.get_property(&qid_a, "phase").await.unwrap();
    assert_eq!(read_a, Some(PropertyValue::String("healthy".into())));

    let read_b = graph.get_property(&qid_b, "phase").await.unwrap();
    assert_eq!(read_b, Some(PropertyValue::String("no-quorum".into())));

    manager.shutdown();
}
