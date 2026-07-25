//! Shared read-only projection of node state (#1 read-snapshot bypass).
//!
//! Every node task publishes an immutable [`NodeReadState`] snapshot into its
//! shard's projection map on each commit. Reads (`get_property`, full scans,
//! …) consult the projection directly instead of routing a command to the node
//! task's mailbox and awaiting a reply — turning a per-read actor round-trip
//! into a lock-free `DashMap` lookup plus an `Arc` clone.
//!
//! # Consistency
//!
//! A node task processes its commands serially and publishes the new snapshot
//! *inside* the commit, before acknowledging the write. So a reader that
//! observes a committed write's receipt is guaranteed to see that write in the
//! projection (**read-your-writes**); a concurrent reader racing an in-flight
//! commit sees the last committed snapshot (**read-committed**). Under group
//! commit the snapshot becomes visible one durability window before the write
//! is fsynced — reads are committed-visible, not durable-visible. A crash
//! discards both the un-fsynced write and its (never-acknowledged) snapshot.
//!
//! # Residency
//!
//! The projection only holds **resident** nodes: entries are removed when a
//! node is evicted/slept (see `GraphShard`). Reads for a non-resident node miss
//! the projection and fall back to the wake + mailbox slow path.

use crate::graph::node_task::TombstoneRecord;
use dashmap::DashMap;
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Immutable snapshot of a single node's read-visible state.
///
/// Published as `Arc<NodeReadState>` so readers clone a cheap pointer rather
/// than the underlying maps. A new value is built on every commit; the old one
/// lives as long as any reader still holds it.
#[derive(Debug, Clone, Default)]
pub struct NodeReadState {
    /// First-class labels.
    pub labels: HashSet<Symbol>,
    /// Node properties.
    pub properties: BTreeMap<Symbol, PropertyValue>,
    /// Per-property event time (event-time last-writer-wins). A key absent here
    /// is treated as `EventTime::MIN`. Exposed so readers/queries can reason
    /// about the freshness of each property value.
    pub property_times: BTreeMap<Symbol, EventTime>,
    /// Outbound + inbound half-edges.
    pub edges: HashSet<HalfEdge>,
    /// Edge properties keyed by `(edge_type, target)`.
    pub edge_properties: HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
    /// Soft-delete marker, `Some` when the node is logically deleted.
    pub tombstone: Option<TombstoneRecord>,
}

/// Per-shard projection map: node id → latest published read snapshot.
///
/// `DashMap` is used (rather than `RwLock<HashMap>`) so a full-graph scan can
/// iterate all resident nodes without blocking concurrent single-node writes,
/// and per-key writes contend only on their shard-internal segment lock.
pub type ShardProjection = Arc<DashMap<NexoraId, Arc<NodeReadState>>>;

/// Create an empty shard projection.
pub fn new_shard_projection() -> ShardProjection {
    Arc::new(DashMap::new())
}
