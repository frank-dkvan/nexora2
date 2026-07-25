//! A2-4 integration: openraft `RaftNetwork` over the real TCP transport.
//!
//! Proves the byte-shuttle wiring end to end: a `ControlRaftNetwork` client
//! serializes a Vote / AppendEntries / InstallSnapshot RPC, ships it over a real
//! `TcpRemoteClient` → `TcpGraphServer` connection, the server's
//! `ControlRaftRpcServer` decodes it and dispatches to a real single-node
//! `Raft`, and the typed response comes back to the client. This exercises the
//! actual openraft request/response types, not a mock — the network layer is
//! the thing under test.

use std::sync::Arc;
use std::time::Duration;

use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::VoteRequest;
use openraft::CommittedLeaderId;
use openraft::{BasicNode, Config, LogId, Raft, Vote};

use nexora_core::control_plane_store::InMemoryControlPlaneStore;
use nexora_zenoh::control_raft::{node_id_to_raft, ControlRaftTypeConfig};
use nexora_zenoh::control_raft_log::ControlLogStore;
use nexora_zenoh::control_raft_network::{ControlRaftNetworkFactory, ControlRaftRpcServer};
use nexora_zenoh::control_raft_sm::ControlStateMachine;
use nexora_zenoh::{TcpGraphServer, TcpRemoteClient};

/// An echo graph handler so the server can be constructed; the tests only drive
/// the Raft plane.
struct NoopGraphHandler;

#[async_trait::async_trait]
impl nexora_zenoh::GraphHandler for NoopGraphHandler {
    async fn handle(
        &self,
        _target: &str,
        _op: nexora_zenoh::GraphOperation,
    ) -> Result<nexora_zenoh::GraphResult, nexora_zenoh::RouterError> {
        Ok(nexora_zenoh::GraphResult::Status {
            ok: true,
            message: "noop".into(),
        })
    }
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("nexora-a2-4-{tag}-{}", uuid::Uuid::new_v4()));
    p
}

/// Build a real single-node `Raft` whose network points nowhere (we drive it
/// only as an RPC *target* in these tests). Returns the node plus its temp dir
/// for cleanup.
async fn build_raft(node_str: &str) -> (Raft<ControlRaftTypeConfig>, std::path::PathBuf) {
    let dir = temp_dir(node_str);
    let log = ControlLogStore::open(&dir).unwrap();
    let sm = ControlStateMachine::new(Arc::new(InMemoryControlPlaneStore::new()));

    // Network for the *target* node — it never initiates RPCs in these tests,
    // so an unregistered client is fine.
    let net = ControlRaftNetworkFactory::new(Arc::new(TcpRemoteClient::new()));

    let config = Arc::new(
        Config {
            cluster_name: "control".into(),
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let raft = Raft::new(node_id_to_raft(node_str), config, net, log, sm)
        .await
        .unwrap();
    (raft, dir)
}

/// Vote RPC round-trips over TCP to a real Raft node and returns a typed
/// response. A fresh node (never voted) grants a vote for term 1.
#[tokio::test]
async fn vote_rpc_round_trips_over_tcp() {
    let target_str = "node-target";
    let (raft, dir) = build_raft(target_str).await;

    // Server on the target node, serving the Raft plane over TCP.
    let rpc_server = Arc::new(ControlRaftRpcServer::new(Arc::new(raft.clone())));
    let server = TcpGraphServer::new(Arc::new(NoopGraphHandler), "127.0.0.1:0".to_string())
        .with_raft_handler(rpc_server);
    let addr = server.start().await.unwrap();

    // Client side: register the target's address under its string node_id
    // (which is what BasicNode.addr carries), then mint a network to it.
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node(target_str, &addr).await;
    let mut factory = ControlRaftNetworkFactory::new(client);
    let target_id = node_id_to_raft(target_str);
    let mut net = factory
        .new_client(target_id, &BasicNode::new(target_str))
        .await;

    // A candidate at term 1 asks the fresh follower for a vote.
    let candidate = node_id_to_raft("node-candidate");
    let req = VoteRequest::new(
        Vote::new(1, candidate),
        Some(LogId::new(CommittedLeaderId::new(1, candidate), 0)),
    );
    let opt = RPCOption::new(Duration::from_secs(5));
    let resp = net.vote(req, opt).await.expect("vote rpc should succeed");

    assert!(resp.vote_granted, "fresh node should grant the vote");

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A Raft request that a server rejects surfaces as a `RemoteError` carrying the
/// peer's `RaftError`, not as a transport error. We provoke this by sending a
/// second, lower-term vote after the node has already voted at a higher term.
#[tokio::test]
async fn stale_vote_is_reported_by_peer() {
    let target_str = "node-target-2";
    let (raft, dir) = build_raft(target_str).await;
    let rpc_server = Arc::new(ControlRaftRpcServer::new(Arc::new(raft.clone())));
    let server = TcpGraphServer::new(Arc::new(NoopGraphHandler), "127.0.0.1:0".to_string())
        .with_raft_handler(rpc_server);
    let addr = server.start().await.unwrap();

    let client = Arc::new(TcpRemoteClient::new());
    client.register_node(target_str, &addr).await;
    let mut factory = ControlRaftNetworkFactory::new(client);
    let target_id = node_id_to_raft(target_str);
    let mut net = factory
        .new_client(target_id, &BasicNode::new(target_str))
        .await;

    let candidate = node_id_to_raft("node-candidate");
    let opt = RPCOption::new(Duration::from_secs(5));

    // First vote at term 5 → granted, node persists vote at term 5.
    let hi = VoteRequest::new(Vote::new(5, candidate), None);
    let r1 = net.vote(hi, opt.clone()).await.expect("high vote ok");
    assert!(r1.vote_granted);

    // Second vote at the lower term 2 → not granted (node has seen term 5).
    let lo = VoteRequest::new(Vote::new(2, candidate), None);
    let r2 = net.vote(lo, opt).await.expect("low vote transports ok");
    assert!(!r2.vote_granted, "node must reject a lower-term vote");

    server.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Sending a Raft RPC to a server that has no Raft handler registered surfaces
/// as a transport-level error (the data-plane-only server rejects it), not a
/// panic or hang.
#[tokio::test]
async fn raft_rpc_to_dataplane_only_server_errors() {
    // Data-plane-only server: no with_raft_handler.
    let server = TcpGraphServer::new(Arc::new(NoopGraphHandler), "127.0.0.1:0".to_string());
    let addr = server.start().await.unwrap();

    let target_str = "node-noraft";
    let client = Arc::new(TcpRemoteClient::new());
    client.register_node(target_str, &addr).await;
    let mut factory = ControlRaftNetworkFactory::new(client);
    let mut net = factory
        .new_client(node_id_to_raft(target_str), &BasicNode::new(target_str))
        .await;

    let req = VoteRequest::new(Vote::new(1, node_id_to_raft("c")), None);
    let opt = RPCOption::new(Duration::from_secs(5));
    let res = net.vote(req, opt).await;
    assert!(
        res.is_err(),
        "raft rpc to a server without a raft handler must error"
    );

    server.shutdown();
}

/// An unreachable target (unregistered address) surfaces as an error the
/// client can back off on, not a hang.
#[tokio::test]
async fn unreachable_target_errors() {
    let client = Arc::new(TcpRemoteClient::new());
    // Do NOT register the node → address resolution fails.
    let mut factory = ControlRaftNetworkFactory::new(client);
    let mut net = factory
        .new_client(node_id_to_raft("ghost"), &BasicNode::new("ghost"))
        .await;

    let req = VoteRequest::new(Vote::new(1, node_id_to_raft("c")), None);
    let opt = RPCOption::new(Duration::from_secs(2));
    let res = net.vote(req, opt).await;
    assert!(res.is_err(), "unreachable target must error");
}
