//! Control-plane consensus (A2) — in-process openraft over the unified
//! [`ControlPlaneStore`].
//!
//! This is the *control plane*: it replicates and agrees on nexora's own
//! metadata (shard map, MV/SQ/schema definitions), which is non-replayable and
//! must stay consistent across the cluster with a single elected owner per
//! shard. It is deliberately separate from `nexora-raft`, which is the
//! *data-plane* WAL write-through replicator — different concern, different
//! lifecycle. To keep the two from being confused, everything here is prefixed
//! `Control*`.
//!
//! ## Why openraft, in-process
//!
//! The cluster runs as N nexora processes and must not require a separate
//! consensus daemon (etcd/Consul). openraft is a library compiled into the
//! nexora binary; the Raft voters *are* nexora nodes (co-located), so the
//! deployment topology is unchanged — still just "N nexora processes".
//!
//! ## Type config (A2-1)
//!
//! - `NodeId = u64` — openraft requires the NodeId to be `Copy` (its
//!   `NodeIdEssential` bound), so nexora's `String` node_id can't be used
//!   directly. We map `node_id -> u64` with a stable hash ([`node_id_to_raft`])
//!   and keep the original string as the node's address in `BasicNode`, so the
//!   human/network identity is preserved while Raft gets its cheap `Copy` id.
//! - `Node = BasicNode` — carries the node's `node_id`/graph address for the
//!   network layer to resolve where to send RPCs.
//! - `D = ControlCommand` — a metadata mutation (put/delete on a namespaced key).
//!   Every committed log entry is applied to the [`ControlPlaneStore`].
//! - `R = ControlResponse` — the apply result returned to the proposer.
//! - `SnapshotData = Cursor<Vec<u8>>` — the store's `snapshot()` blob.
//!
//! Later A2 steps implement the storage (A2-3), network (A2-4), and state
//! machine (A2-2) against this config, then assemble the node (A2-6).

use std::io::Cursor;

use openraft::TokioRuntime;
use serde::{Deserialize, Serialize};

use nexora_core::control_plane_store::Namespace;

/// A single control-plane metadata mutation — the app-data (`D`) that a Raft
/// log entry carries. Applying it writes through to the [`ControlPlaneStore`].
///
/// Reads never become commands: only writes go through consensus. The namespace
/// is stored as its stable string key so the command serializes without
/// depending on the `Namespace` enum's repr.
///
/// A2-7: Extended with high-level ShardMap operations so the state machine can
/// apply them atomically (failover = read current map + bump epoch + reassign +
/// write back, all in one apply).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlCommand {
    /// Write `value` at `(namespace, key)`.
    Put {
        namespace: String,
        key: String,
        value: Vec<u8>,
    },
    /// Delete `(namespace, key)`.
    Delete { namespace: String, key: String },
    /// Failover a shard to a specific new owner, bumping its epoch.
    /// Applied by reading the current ShardMap, mutating the assignment, and
    /// writing it back. Returns the new fencing token (shard_id + new epoch).
    FailoverShard { shard_id: u32, new_owner: String },
    /// Failover a shard to the first surviving replica (auto-select from the
    /// assignment's replica list). If no replica is alive, mark the shard
    /// non-writable. Returns `Some(token)` if a replica was promoted, `None` if
    /// the shard is now unavailable.
    FailoverShardAuto { shard_id: u32 },
    /// Propose a complete ShardMap update (version must be newer than current).
    ProposeShardMap {
        proposed: Vec<u8>, // serialized ShardMap
    },
}

impl ControlCommand {
    /// Build a `Put` from a typed namespace.
    pub fn put(namespace: Namespace, key: impl Into<String>, value: Vec<u8>) -> Self {
        Self::Put {
            namespace: namespace.as_str().to_string(),
            key: key.into(),
            value,
        }
    }

    /// Build a `Delete` from a typed namespace.
    pub fn delete(namespace: Namespace, key: impl Into<String>) -> Self {
        Self::Delete {
            namespace: namespace.as_str().to_string(),
            key: key.into(),
        }
    }

    /// Resolve the stored namespace string back to a [`Namespace`], if known.
    /// An unrecognized namespace (e.g. from a future version) returns `None`;
    /// the state machine treats that as a no-op rather than panicking.
    pub fn resolve_namespace(ns: &str) -> Option<Namespace> {
        Namespace::all().into_iter().find(|n| n.as_str() == ns)
    }
}

/// The apply result (`R`) returned to a proposer once its command commits.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResponse {
    /// Generic success (Put/Delete).
    #[default]
    Applied,
    /// Failover succeeded; returns the new fencing token as (shard_id, epoch).
    FailoverToken { shard_id: u32, epoch: u64 },
    /// Auto-failover promoted a replica.
    FailoverPromoted { shard_id: u32, epoch: u64 },
    /// Auto-failover found no surviving replica; shard marked unavailable.
    FailoverNoReplica { shard_id: u32 },
    /// ShardMap proposal accepted (new version).
    MapAccepted { version: u64 },
    /// Command failed (e.g., stale proposal, shard not found).
    Error { message: String },
}

/// The Raft node id type. openraft requires `Copy`, so nexora's string
/// `node_id` is mapped onto a `u64`.
pub type ControlNodeId = u64;

/// Map a nexora string `node_id` to its Raft [`ControlNodeId`] via a stable
/// 64-bit hash (FNV-1a). Deterministic across nodes and restarts, so every node
/// derives the same Raft id for a given `node_id`. The original string is
/// retained as the `BasicNode` address for the network layer.
///
/// Collision note: FNV-1a over the small, operator-controlled set of node ids in
/// a cluster makes a 64-bit collision astronomically unlikely; membership config
/// is validated (A2-5) so a collision would be caught as a duplicate voter.
pub fn node_id_to_raft(node_id: &str) -> ControlNodeId {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in node_id.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

openraft::declare_raft_types!(
    /// openraft type configuration for the control plane (A2-1).
    pub ControlRaftTypeConfig:
        D = ControlCommand,
        R = ControlResponse,
        NodeId = ControlNodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<ControlRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_namespace_roundtrips() {
        let cmd = ControlCommand::put(Namespace::ShardMap, "current", b"map".to_vec());
        match &cmd {
            ControlCommand::Put {
                namespace,
                key,
                value,
            } => {
                assert_eq!(namespace, "shardmap");
                assert_eq!(key, "current");
                assert_eq!(value, b"map");
                assert_eq!(
                    ControlCommand::resolve_namespace(namespace),
                    Some(Namespace::ShardMap)
                );
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn failover_command_roundtrips() {
        let cmd = ControlCommand::FailoverShard {
            shard_id: 42,
            new_owner: "node-2".into(),
        };
        let bytes = serde_json::to_vec(&cmd).unwrap();
        let back: ControlCommand = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn unknown_namespace_resolves_none() {
        assert_eq!(ControlCommand::resolve_namespace("bogus"), None);
    }

    #[test]
    fn command_serde_roundtrips() {
        let cmd = ControlCommand::delete(Namespace::MvDef, "v1");
        let bytes = serde_json::to_vec(&cmd).unwrap();
        let back: ControlCommand = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn node_id_mapping_is_stable_and_distinct() {
        // Deterministic: same input → same id (survives restart / matches peers).
        assert_eq!(node_id_to_raft("node-a"), node_id_to_raft("node-a"));
        // Distinct node ids map to distinct Raft ids (no trivial collision).
        assert_ne!(node_id_to_raft("node-a"), node_id_to_raft("node-b"));
        assert_ne!(node_id_to_raft("node-1"), node_id_to_raft("node-2"));
    }
}
