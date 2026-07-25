//! A2-9: real multi-voter openraft convergence and no-split-brain.
//!
//! Unlike `partition_no_split_brain_e2e.rs` (which runs the hand-rolled quorum
//! fallback in isolation, no Raft assembled), this harness configures a durable
//! `replication_log_dir` on every node so `ClusterManager::start` actually
//! assembles openraft (A2-6) and the nodes form a real Raft over the shared TCP
//! transport (A2-4). The bootstrap node initializes with the *full* voter set,
//! so a genuine 3-voter cluster forms rather than a self-quoruming single node.
//!
//! What this proves:
//!   1. Convergence — exactly one leader emerges across three fresh voters.
//!   2. Majority failover — killing the leader lets the surviving 2/3 majority
//!      elect a new leader (progress continues with a tolerated failure).
//!   3. No split brain — reducing the cluster to a single reachable node (1/3, a
//!      minority) leaves it unable to hold or win leadership. A minority can
//!      never elect a leader, so it can never commit a conflicting shard-map.
//!
//! "Partition" here is induced by `shutdown()`: to the survivors, a shut-down
//! node is indistinguishable from one severed by a network partition — no votes,
//! no heartbeats. That is exactly the condition the quorum rule must survive.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_zenoh::cluster::{ClusterConfig, ClusterManager, PeerConfig};
use nexora_zenoh::control_raft::node_id_to_raft;
use nexora_zenoh::graph_service_adapter::GraphServiceAdapter;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

struct Node {
    manager: ClusterManager,
    id: String,
    dir: std::path::PathBuf,
    alive: bool,
}

impl Node {
    fn raft_id(&self) -> u64 {
        node_id_to_raft(&self.id)
    }
}

/// Build and start a 3-node cluster with durable Raft storage so openraft is
/// assembled on every node. Returns the nodes once storage/servers are up;
/// leadership convergence is awaited separately by the test.
async fn start_raft_cluster() -> Vec<Node> {
    let ids = ["node-0", "node-1", "node-2"];
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
                node_id: ids[j].to_string(),
                graph_addr: graph_addrs[j].clone(),
                heartbeat_addr: hb_addrs[j].clone(),
            })
            .collect();

        let dir = {
            let mut p = std::env::temp_dir();
            p.push(format!("nexora-a2-9-{}-{}", ids[i], uuid::Uuid::new_v4()));
            p
        };

        let config = ClusterConfig {
            node_id: ids[i].to_string(),
            listen_addr: graph_addrs[i].clone(),
            heartbeat_addr: hb_addrs[i].clone(),
            total_shards: 4,
            peers,
            heartbeat_interval: Duration::from_millis(100),
            failure_timeout: Duration::from_secs(2),
            replication_factor: 2,
            replication_log_dir: Some(dir.clone()),
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
            id: ids[i].to_string(),
            dir,
            alive: true,
        });
    }

    nodes
}

/// Poll the live nodes until exactly one of them reports itself as Raft leader,
/// and every live node agrees on that same leader id. Returns the leader's raft
/// id. Fails the test if convergence doesn't happen within `deadline`.
async fn await_single_leader(nodes: &[Node], deadline: Duration) -> u64 {
    let converged = timeout(deadline, async {
        loop {
            let live: Vec<&Node> = nodes.iter().filter(|n| n.alive).collect();

            // Collect each live node's believed leader.
            let mut leaders = Vec::new();
            for n in &live {
                leaders.push(n.manager.control_plane().raft_leader().await);
            }

            // All live nodes must see the *same* Some(leader), and exactly one
            // live node must claim leadership itself.
            let first = leaders.first().copied().flatten();
            let all_agree = first.is_some() && leaders.iter().all(|l| *l == first);

            let mut self_claims = 0usize;
            for n in &live {
                if n.manager.control_plane().is_raft_leader().await {
                    self_claims += 1;
                }
            }

            if all_agree && self_claims == 1 {
                return first.unwrap();
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    converged.expect("cluster did not converge on a single leader in time")
}

fn cleanup(nodes: &[Node]) {
    for n in nodes {
        let _ = std::fs::remove_dir_all(&n.dir);
    }
}

/// A fresh 3-voter cluster converges on exactly one leader.
#[tokio::test]
async fn three_voters_converge_on_single_leader() {
    let nodes = start_raft_cluster().await;

    let leader = await_single_leader(&nodes, Duration::from_secs(10)).await;

    // The leader id must be one of the three real voter ids.
    let voter_ids: Vec<u64> = nodes.iter().map(|n| n.raft_id()).collect();
    assert!(
        voter_ids.contains(&leader),
        "elected leader {leader} must be a configured voter {voter_ids:?}"
    );

    for n in &nodes {
        n.manager.shutdown_async().await;
    }
    cleanup(&nodes);
}

/// Killing the leader lets the surviving 2/3 majority elect a new leader —
/// progress continues across a single tolerated failure.
#[tokio::test]
async fn majority_reelects_after_leader_loss() {
    let mut nodes = start_raft_cluster().await;

    let first_leader = await_single_leader(&nodes, Duration::from_secs(10)).await;

    // Shut down (partition away) the current leader, including its Raft node so
    // it stops issuing append-entries to the survivors.
    let leader_idx = nodes
        .iter()
        .position(|n| n.raft_id() == first_leader)
        .expect("leader must be a known node");
    nodes[leader_idx].manager.shutdown_async().await;
    nodes[leader_idx].alive = false;

    // The remaining two nodes are a majority (2 of 3) and must elect a new
    // leader, distinct from the one we just killed.
    let new_leader = await_single_leader(&nodes, Duration::from_secs(10)).await;
    assert_ne!(
        new_leader, first_leader,
        "a new leader must be elected after the old one is partitioned away"
    );

    for n in &nodes {
        if n.alive {
            n.manager.shutdown_async().await;
        }
    }
    cleanup(&nodes);
}

/// No split brain: reducing the cluster to a single reachable node (1 of 3, a
/// minority) leaves it unable to hold leadership — a minority can never elect a
/// leader, so it can never commit a conflicting shard-map.
#[tokio::test]
async fn minority_of_one_cannot_hold_leadership() {
    let mut nodes = start_raft_cluster().await;

    let _ = await_single_leader(&nodes, Duration::from_secs(10)).await;

    // Partition away two of the three voters (Raft included), leaving a lone
    // survivor (1/3).
    let survivor_idx = 2usize;
    for (i, n) in nodes.iter_mut().enumerate() {
        if i != survivor_idx {
            n.manager.shutdown_async().await;
            n.alive = false;
        }
    }

    // Give the survivor ample time to notice it lost contact and step down (an
    // election timeout plus slack). A correct Raft node in a minority cannot win
    // an election, so it must NOT believe itself leader.
    sleep(Duration::from_secs(4)).await;

    let survivor = &nodes[survivor_idx];
    assert!(
        !survivor.manager.control_plane().is_raft_leader().await,
        "a lone minority node must not hold leadership (split-brain guard)"
    );
    // With no reachable majority, it also cannot see any established leader.
    assert_eq!(
        survivor.manager.control_plane().raft_leader().await,
        None,
        "a minority node must not observe a live leader"
    );

    survivor.manager.shutdown_async().await;
    cleanup(&nodes);
}
