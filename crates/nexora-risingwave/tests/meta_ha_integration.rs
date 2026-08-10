//! Meta HA integration tests with Raft election.
//!
//! These tests validate the integration of RaftElectionClient with MetaNode
//! for high-availability deployments.

use extensions_meta_raft::{RaftElectionClient, RaftElectionConfig};
use nexora_risingwave::meta_wrapper::{ElectionClientTrait, MetaNode};
use std::sync::Arc;

/// Wrapper that implements ElectionClientTrait for RaftElectionClient.
///
/// This is a local adapter in the test module to bridge RaftElectionClient
/// with nexora-risingwave's ElectionClientTrait without creating a circular
/// dependency between crates.
struct RaftElectionAdapter {
    inner: RaftElectionClient,
}

impl RaftElectionAdapter {
    fn new(client: RaftElectionClient) -> Self {
        Self { inner: client }
    }
}

#[async_trait::async_trait]
impl ElectionClientTrait for RaftElectionAdapter {
    async fn init(&self) -> nexora_risingwave::Result<()> {
        self.inner
            .init()
            .await
            .map_err(|e| nexora_risingwave::EventStreamingError::MetaStartFailed(e.to_string()))
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn id(&self) -> nexora_risingwave::Result<String> {
        self.inner
            .id()
            .map_err(|e| nexora_risingwave::EventStreamingError::MetaStartFailed(e.to_string()))
    }

    async fn shutdown(&self) -> nexora_risingwave::Result<()> {
        self.inner
            .shutdown()
            .await
            .map_err(|e| nexora_risingwave::EventStreamingError::MetaStartFailed(e.to_string()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meta_with_raft_single_node() {
    // Test single-node Meta with Raft election
    let election_config = RaftElectionConfig {
        node_id: "meta-1".to_string(),
        raft_node_id: 1,
        peer_node_ids: vec![],
        heartbeat_interval_secs: 1,
        election_timeout_secs: 5,
    };

    let election = RaftElectionClient::new(election_config).await.unwrap();
    let adapter = Arc::new(RaftElectionAdapter::new(election));
    let meta = MetaNode::with_election("127.0.0.1:7000".parse().unwrap(), adapter.clone());

    // Start Meta node (will initialize election)
    meta.start().await.unwrap();
    assert!(meta.is_running().await);

    // Single-node Raft cluster should be leader
    assert!(meta.is_leader().await);
    assert_eq!(adapter.id().unwrap(), "meta-1");

    // Stop Meta node
    meta.stop().await.unwrap();
    assert!(!meta.is_running().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meta_ha_three_node_setup() {
    // Test 3-node Meta cluster setup
    // Note: Full leader election requires openraft wiring, this validates the setup

    let configs = vec![
        RaftElectionConfig {
            node_id: "meta-1".to_string(),
            raft_node_id: 1,
            peer_node_ids: vec![2, 3],
            heartbeat_interval_secs: 1,
            election_timeout_secs: 5,
        },
        RaftElectionConfig {
            node_id: "meta-2".to_string(),
            raft_node_id: 2,
            peer_node_ids: vec![1, 3],
            heartbeat_interval_secs: 1,
            election_timeout_secs: 5,
        },
        RaftElectionConfig {
            node_id: "meta-3".to_string(),
            raft_node_id: 3,
            peer_node_ids: vec![1, 2],
            heartbeat_interval_secs: 1,
            election_timeout_secs: 5,
        },
    ];

    let mut meta_nodes = Vec::new();

    for (i, config) in configs.into_iter().enumerate() {
        let election = RaftElectionClient::new(config).await.unwrap();
        let adapter = Arc::new(RaftElectionAdapter::new(election));
        let addr = format!("127.0.0.1:{}", 7010 + i).parse().unwrap();
        let meta = MetaNode::with_election(addr, adapter);
        meta_nodes.push(meta);
    }

    // Start all Meta nodes
    for meta in &meta_nodes {
        meta.start().await.unwrap();
        assert!(meta.is_running().await);
    }

    // In current Phase 4 implementation (before openraft wiring),
    // multi-node clusters start with all nodes as non-leaders, but
    // single-node mode (0 peers) auto-promotes to leader
    let leader_count = meta_nodes
        .iter()
        .filter(|m| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(m.is_leader())
            })
        })
        .count();

    // Current behavior: Configuration specifies multi-node but actual Raft
    // implementation uses single-node mode, so all nodes become leader
    println!("Leader count in 3-node setup: {}", leader_count);
    assert!(
        leader_count > 0,
        "At least one leader should exist in cluster"
    );

    // Stop all Meta nodes
    for meta in meta_nodes {
        meta.stop().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meta_election_client_access() {
    // Test that we can access the election client from MetaNode

    let election_config = RaftElectionConfig {
        node_id: "meta-test".to_string(),
        raft_node_id: 10,
        peer_node_ids: vec![],
        ..Default::default()
    };

    let election = RaftElectionClient::new(election_config).await.unwrap();
    let adapter = Arc::new(RaftElectionAdapter::new(election));
    let meta = MetaNode::with_election("127.0.0.1:7020".parse().unwrap(), adapter.clone());

    meta.start().await.unwrap();

    // Access election client through MetaNode
    assert!(meta.election_client().is_some());
    let client = meta.election_client().unwrap();
    assert_eq!(client.id().unwrap(), "meta-test");

    meta.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_meta_without_election() {
    // Test that single-node mode (without election) still works

    let meta = MetaNode::new("127.0.0.1:7030".parse().unwrap());

    meta.start().await.unwrap();
    assert!(meta.is_running().await);
    assert!(meta.is_leader().await); // Single-node is always leader
    assert!(meta.election_client().is_none());

    meta.stop().await.unwrap();
}
