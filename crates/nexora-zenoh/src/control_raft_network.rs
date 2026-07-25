//! Control-plane Raft network (A2-4) — openraft `RaftNetwork` over the existing
//! TCP transport.
//!
//! openraft requires a [`RaftNetworkFactory`] that mints a [`RaftNetwork`] per
//! target node; that network sends AppendEntries/Vote/InstallSnapshot RPCs to
//! the peer. Rather than open a second port or protocol stack, we reuse
//! [`TcpRemoteClient`]/[`TcpGraphServer`]: the transport already multiplexes a
//! Raft byte-shuttle variant ([`WireRequest::Raft`]) over the same socket the
//! data plane uses. This file owns the *codec* — turning openraft's typed RPCs
//! into the opaque bytes the transport carries — so the transport layer never
//! depends on openraft, and openraft never appears on the data-plane
//! `GraphOperation` enum.
//!
//! ## Node id resolution
//!
//! openraft addresses peers by [`ControlNodeId`] (`u64`), but the TCP client
//! routes by the nexora string `node_id`. The [`ControlRaftTypeConfig`]'s
//! `Node = BasicNode` carries that string in its `addr` field (set at
//! membership time, see A2-6), so [`new_client`] reads `node.addr` as the
//! routing key. The `TcpRemoteClient` must already have that node's network
//! address registered (via `register_node`) for the send to resolve.
//!
//! ## Error mapping
//!
//! - transport unreachable/timeout → [`RPCError::Unreachable`] so openraft
//!   backs off before retrying (a peer that's down shouldn't be hammered).
//! - the remote node returning a `RaftError` (e.g. it's not leader) →
//!   [`RPCError::RemoteError`], carrying the peer's error verbatim.
//! - encode/decode failures → [`RPCError::Network`] (retry immediately;
//!   they're our-side faults, not peer-down signals).

use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::control_raft::{ControlNodeId, ControlRaftTypeConfig};
use crate::tcp_transport::{RaftRpcHandler, TcpRemoteClient};

/// A serialized control-plane Raft RPC request. This is exactly the opaque
/// payload the transport's [`WireRequest::Raft`] carries; both peers agree on
/// this enum as the Raft wire codec.
#[derive(Serialize, Deserialize)]
pub enum RaftRpc {
    AppendEntries(AppendEntriesRequest<ControlRaftTypeConfig>),
    Vote(VoteRequest<ControlNodeId>),
    InstallSnapshot(InstallSnapshotRequest<ControlRaftTypeConfig>),
}

/// A serialized Raft RPC response. Each variant pairs with a [`RaftRpc`] kind.
/// The inner `Result` carries the peer's `RaftError` when its local handler
/// rejected the RPC (e.g. higher vote seen), so the caller can surface it as a
/// [`RPCError::RemoteError`].
#[derive(Serialize, Deserialize)]
pub enum RaftRpcResp {
    AppendEntries(Result<AppendEntriesResponse<ControlNodeId>, RaftError<ControlNodeId>>),
    Vote(Result<VoteResponse<ControlNodeId>, RaftError<ControlNodeId>>),
    InstallSnapshot(
        Result<
            InstallSnapshotResponse<ControlNodeId>,
            RaftError<ControlNodeId, openraft::error::InstallSnapshotError>,
        >,
    ),
}

/// Factory that mints a [`ControlRaftNetwork`] per target node, sharing one
/// [`TcpRemoteClient`] connection pool across all of them.
///
/// The same `TcpRemoteClient` also serves the data plane, so control-plane
/// consensus and user graph ops share a single pool of connections to each
/// peer — no extra sockets, no extra port.
#[derive(Clone)]
pub struct ControlRaftNetworkFactory {
    client: Arc<TcpRemoteClient>,
}

impl ControlRaftNetworkFactory {
    pub fn new(client: Arc<TcpRemoteClient>) -> Self {
        Self { client }
    }
}

impl RaftNetworkFactory<ControlRaftTypeConfig> for ControlRaftNetworkFactory {
    type Network = ControlRaftNetwork;

    async fn new_client(&mut self, target: ControlNodeId, node: &BasicNode) -> Self::Network {
        // `node.addr` holds the nexora string node_id (set at membership time),
        // which is the routing key the TcpRemoteClient understands.
        ControlRaftNetwork {
            client: self.client.clone(),
            target,
            target_node_id: node.addr.clone(),
        }
    }
}

/// A [`RaftNetwork`] bound to a single target node. Serializes each RPC and
/// ships it as opaque bytes over the shared TCP transport.
pub struct ControlRaftNetwork {
    client: Arc<TcpRemoteClient>,
    /// openraft's numeric id for the peer — used only to stamp errors.
    target: ControlNodeId,
    /// The nexora string node_id, i.e. the transport routing key.
    target_node_id: String,
}

impl ControlRaftNetwork {
    /// Encode a request, send it over the transport, and decode the response
    /// frame. Transport-level faults map to network/unreachable errors; the
    /// caller destructures the returned [`RaftRpcResp`] for the typed result.
    async fn round_trip(
        &self,
        rpc: RaftRpc,
        option: &RPCOption,
    ) -> Result<RaftRpcResp, RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId>>> {
        let payload = serde_json::to_vec(&rpc)
            .map_err(|e| RPCError::Network(NetworkError::new(&SerdeError(e.to_string()))))?;

        let bytes = self
            .client
            .send_raft(&self.target_node_id, payload, option.hard_ttl())
            .await
            .map_err(|e| {
                // A peer that's down/timed-out should trigger openraft's backoff,
                // not an immediate hot retry, so map transport faults to
                // Unreachable.
                RPCError::Unreachable(Unreachable::new(&TransportError(e.to_string())))
            })?;

        serde_json::from_slice(&bytes)
            .map_err(|e| RPCError::Network(NetworkError::new(&SerdeError(e.to_string()))))
    }
}

/// A response of the wrong variant for the request kind — a protocol bug or a
/// version mismatch between peers, not a peer-down condition.
fn variant_mismatch<N, E: std::error::Error>(kind: &str) -> RPCError<ControlNodeId, N, E>
where
    N: openraft::Node,
{
    RPCError::Network(NetworkError::new(&ProtocolError(format!(
        "expected {kind} raft response variant"
    ))))
}

impl RaftNetwork<ControlRaftTypeConfig> for ControlRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ControlRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<ControlNodeId>,
        RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId>>,
    > {
        match self
            .round_trip(RaftRpc::AppendEntries(rpc), &option)
            .await?
        {
            RaftRpcResp::AppendEntries(res) => {
                res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(variant_mismatch("append_entries")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<ControlNodeId>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<ControlNodeId>,
        RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId>>,
    > {
        match self.round_trip(RaftRpc::Vote(rpc), &option).await? {
            RaftRpcResp::Vote(res) => {
                res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(variant_mismatch("vote")),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ControlRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<ControlNodeId>,
        RPCError<
            ControlNodeId,
            BasicNode,
            RaftError<ControlNodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        // install_snapshot has its own error generic; round_trip is typed to the
        // plain RaftError, so send/decode faults are re-wrapped here to the
        // InstallSnapshot error family.
        let resp = self
            .round_trip(RaftRpc::InstallSnapshot(rpc), &option)
            .await
            .map_err(reframe_rpc_error)?;
        match resp {
            RaftRpcResp::InstallSnapshot(res) => {
                res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(variant_mismatch("install_snapshot")),
        }
    }
}

/// Re-wrap a transport/decode `RPCError` (typed to the plain `RaftError`) into
/// the `install_snapshot` error family. Only the non-remote variants can occur
/// here — `round_trip` never produces a `RemoteError` — so the `RemoteError`
/// arm is unreachable in practice and mapped to a network error defensively.
fn reframe_rpc_error(
    e: RPCError<ControlNodeId, BasicNode, RaftError<ControlNodeId>>,
) -> RPCError<
    ControlNodeId,
    BasicNode,
    RaftError<ControlNodeId, openraft::error::InstallSnapshotError>,
> {
    match e {
        RPCError::Timeout(t) => RPCError::Timeout(t),
        RPCError::Unreachable(u) => RPCError::Unreachable(u),
        RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
        RPCError::Network(n) => RPCError::Network(n),
        RPCError::RemoteError(_) => RPCError::Network(NetworkError::new(&ProtocolError(
            "unexpected remote error on install_snapshot transport".into(),
        ))),
    }
}

/// Helper error types so transport/serde/protocol failures satisfy the
/// `std::error::Error + 'static` bound that `NetworkError::new`/`Unreachable::new`
/// require, while carrying a readable message.
#[derive(Debug, thiserror::Error)]
#[error("raft transport: {0}")]
struct TransportError(String);

#[derive(Debug, thiserror::Error)]
#[error("raft codec: {0}")]
struct SerdeError(String);

#[derive(Debug, thiserror::Error)]
#[error("raft protocol: {0}")]
struct ProtocolError(String);

/// Server side of A2-4: decodes an incoming [`RaftRpc`] and dispatches it to
/// the local openraft [`Raft`] node, then re-encodes the [`RaftRpcResp`].
/// Registered on the [`TcpGraphServer`] via `with_raft_handler` (A2-6).
///
/// A2-7: Holds an Arc<Raft> to allow sharing the handle with ControlPlane.
#[derive(Clone)]
pub struct ControlRaftRpcServer {
    raft: Arc<openraft::Raft<ControlRaftTypeConfig>>,
}

impl ControlRaftRpcServer {
    pub fn new(raft: Arc<openraft::Raft<ControlRaftTypeConfig>>) -> Self {
        Self { raft }
    }
}

#[async_trait::async_trait]
impl RaftRpcHandler for ControlRaftRpcServer {
    async fn handle_raft(&self, payload: Vec<u8>) -> Result<Vec<u8>, String> {
        let rpc: RaftRpc =
            serde_json::from_slice(&payload).map_err(|e| format!("decode raft rpc: {e}"))?;

        let resp = match rpc {
            RaftRpc::AppendEntries(req) => {
                RaftRpcResp::AppendEntries(self.raft.append_entries(req).await)
            }
            RaftRpc::Vote(req) => RaftRpcResp::Vote(self.raft.vote(req).await),
            RaftRpc::InstallSnapshot(req) => {
                RaftRpcResp::InstallSnapshot(self.raft.install_snapshot(req).await)
            }
        };

        serde_json::to_vec(&resp).map_err(|e| format!("encode raft resp: {e}"))
    }
}
