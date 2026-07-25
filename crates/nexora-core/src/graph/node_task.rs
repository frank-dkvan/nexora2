//! NodeTask — the async task that represents a single graph node.
//!
//! Each node in the Nexora graph is an independent async task that owns
//! its state exclusively. No other task can directly read or write a
//! node's properties or edges — all access is via `NodeCommand` messages
//! sent through the node's `mpsc` channel.
//!
//! This design ensures:
//! 1. No data races (single owner of mutable state)
//! 2. Natural backpressure (bounded channel)
//! 3. Clean shutdown (drop the sender, task exits)
//! 4. Easy migration (task state is self-contained)

use crate::event::{NodeChangeEvent, TimedEvent};
use crate::graph::projection::{NodeReadState, ShardProjection};
use crate::wal::{WalOperation, WriteAheadLog};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

// ============================================================
// P0.1 Task 1.1: Extended Graph Model Types
// ============================================================

/// Tombstone record for soft-delete semantics.
///
/// When a node is deleted, it is not immediately removed from storage.
/// Instead, a tombstone is recorded, allowing:
/// - Time-travel queries (view state before deletion)
/// - Audit trails (who deleted what and why)
/// - Undelete operations (restore accidentally deleted nodes)
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TombstoneRecord {
    /// Timestamp when the node was deleted (microsecond precision)
    pub deleted_at: EventTime,

    /// Optional: who performed the deletion (user ID, service account, etc.)
    pub deleted_by: Option<String>,

    /// Optional: reason for deletion (audit trail)
    pub reason: Option<String>,
}

pub(crate) type SharedWal = Arc<Mutex<WriteAheadLog>>;

/// Result of spawning a node task: command sender and task handle.
#[derive(Debug)]
pub struct NodeTaskHandle {
    /// Command sender for the node task.
    pub tx: mpsc::Sender<NodeCommand>,
    /// JoinHandle for detecting task panic.
    pub handle: JoinHandle<()>,
}

// ============================================================
// Commit Path Types
// ============================================================

/// A single mutation operation within a commit batch.
#[derive(Clone, Debug)]
pub enum MutationOp {
    SetProperty {
        key: Symbol,
        value: PropertyValue,
    },
    RemoveProperty {
        key: Symbol,
    },
    AddEdge {
        edge: HalfEdge,
    },
    RemoveEdge {
        edge: HalfEdge,
    },
    // P0.1 Task 1.3 Step 2: Label mutations (dual-write during migration)
    AddLabel {
        label: Symbol,
    },
    RemoveLabel {
        label: Symbol,
    },
    // P0.1: Edge property mutation
    EdgePropertySet {
        edge_type: Symbol,
        dst: NexoraId,
        key: Symbol,
        value: PropertyValue,
    },
    // P0.5: Soft-delete a node by setting a tombstone
    DeleteNode {
        tombstone: TombstoneRecord,
    },
}

/// A batch of mutations to be committed atomically.
///
/// Carries a `request_id` for idempotency: if the same request_id
/// is seen again (e.g., due to a retry), the cached result is returned
/// without re-executing the mutations.
#[derive(Debug)]
pub struct MutationRequest {
    /// Unique idempotency key. Same request_id → same result.
    pub request_id: u64,
    /// The mutations to apply, each with an optional per-operation event time.
    ///
    /// When an op carries `Some(t)`, that timestamp is used for event-time LWW;
    /// when `None`, the request-level `event_time` is used as a fallback (and if
    /// that is also `None`, the node's internal clock is used). Per-op times let
    /// a single batch coalesce records with different event times while still
    /// resolving each property write correctly by its own timestamp.
    pub operations: Vec<(MutationOp, Option<EventTime>)>,
    /// Fallback event time for operations without their own timestamp.
    ///
    /// `Some(t)` when an upstream source supplied a batch-level event time;
    /// `None` (all single-write and legacy paths) falls back to the node's
    /// internal monotonic clock, keeping the previous arrival-order behavior.
    pub event_time: Option<EventTime>,
    /// Reply channel.
    pub reply: oneshot::Sender<Result<CommitReceipt, NodeError>>,
    /// Durability barrier: when `true` (the default for all single-write and
    /// legacy paths), the commit blocks until the WAL group-commit flusher has
    /// fsynced this write before replying — so a returned receipt implies the
    /// write is on disk. When `false` (relaxed batch ingest), the commit replies
    /// as soon as the record is buffered; durability is reached asynchronously
    /// by the flusher, and a crash may lose the last un-fsynced batch (the
    /// ingest source replays from its upstream offset to recover).
    pub await_durable: bool,
}

/// Receipt returned after a successful commit.
#[derive(Clone, Debug)]
pub struct CommitReceipt {
    /// The event time assigned to this commit.
    pub event_time: EventTime,
    /// Number of events generated.
    pub event_count: usize,
    /// Number of property writes/removals dropped by event-time LWW because a
    /// newer value already existed (a late/out-of-order arrival). Surfaced so
    /// the ingest layer can report it as an observability metric (F4).
    pub late_dropped: usize,
}

/// Commands sent to a `NodeTask` via its channel.
pub enum NodeCommand {
    // ====== Commit Path (unified mutation) ======
    Mutate(MutationRequest),

    // ====== Read operations ======
    GetProperty {
        key: Symbol,
        reply: oneshot::Sender<Result<Option<PropertyValue>, NodeError>>,
    },
    GetAllProperties {
        reply: oneshot::Sender<Result<BTreeMap<Symbol, PropertyValue>, NodeError>>,
    },
    GetEdges {
        edge_type: Option<Symbol>,
        reply: oneshot::Sender<Result<Vec<HalfEdge>, NodeError>>,
    },

    // ====== Legacy mutation (convenience, bypasses WAL) ======
    SetProperty {
        key: Symbol,
        value: PropertyValue,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
    RemoveProperty {
        key: Symbol,
        reply: oneshot::Sender<Result<Option<PropertyValue>, NodeError>>,
    },
    AddEdge {
        edge: HalfEdge,
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
    RemoveEdge {
        edge: HalfEdge,
        reply: oneshot::Sender<Result<bool, NodeError>>,
    },

    // ====== Lifecycle ======
    DrainJournal {
        reply: oneshot::Sender<Vec<TimedEvent<NodeChangeEvent>>>,
    },
    SnapshotState {
        reply: oneshot::Sender<NodeStateSnapshot>,
    },
    GoToSleep {
        reply: oneshot::Sender<Result<(), NodeError>>,
    },
    MemorySize {
        reply: oneshot::Sender<usize>,
    },
}

/// A consistent copy of a node's state used while persisting it.
///
/// P0.1: Extended to include labels, namespace, tenant_id, and tombstone.
pub struct NodeStateSnapshot {
    // P0.1: New first-class fields
    pub labels: HashSet<Symbol>,
    pub namespace: Option<Symbol>,
    pub tenant_id: Option<Symbol>,
    pub tombstone: Option<TombstoneRecord>,

    // Existing fields
    pub properties: BTreeMap<Symbol, PropertyValue>,
    /// Per-property event time (event-time last-writer-wins). Absent keys are
    /// treated as `EventTime::MIN`.
    pub property_times: BTreeMap<Symbol, EventTime>,
    pub edges: HashSet<HalfEdge>,
    pub edge_properties: HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
    pub journal: Vec<TimedEvent<NodeChangeEvent>>,
}

/// Errors that can occur in node operations.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("node is shutting down")]
    ShuttingDown,
    #[error("internal error: {0}")]
    Internal(String),
    #[error("duplicate request_id: {0}")]
    DuplicateRequest(u64),
}

/// The node task — runs as a `tokio::spawn` task, owns all node state.
///
/// P0.1 Task 1.1: Extended with production-grade graph model fields.
pub struct NodeTask {
    pub id: NexoraId,

    // ====== P0.1: New first-class fields ======
    /// Labels: first-class citizen — the canonical storage for node labels.
    pub labels: HashSet<Symbol>,

    /// Tombstone: soft-delete marker
    /// When set, the node is logically deleted but still queryable for audit/time-travel.
    pub tombstone: Option<TombstoneRecord>,

    /// Namespace: logical isolation (e.g., "airport_cargo", "manufacturing")
    /// Enables multi-domain deployments within a single graph instance.
    pub namespace: Option<Symbol>,

    /// Tenant ID: multi-tenant isolation
    /// Ensures customer data is segregated at the graph level.
    pub tenant_id: Option<Symbol>,

    // ====== Existing fields ======
    properties: BTreeMap<Symbol, PropertyValue>,

    /// Per-property event time for event-time last-writer-wins.
    ///
    /// Records the event time of the write that produced each property's
    /// current value. A late-arriving write whose event time is older than the
    /// stored one is discarded rather than overwriting a newer value, so
    /// out-of-order ingestion converges to the event-time-latest state instead
    /// of the arrival-order-latest one. A key absent here is treated as
    /// `EventTime::MIN` (any write wins) — this keeps snapshots written by older
    /// builds, which carry no per-property times, correct after recovery.
    property_times: BTreeMap<Symbol, EventTime>,

    edges: HashSet<HalfEdge>,

    /// Edge property storage — one entry per outbound edge identity.
    ///
    /// Outbound edges are keyed by (edge_type, other).  Properties belong to
    /// the edge/relationship itself, not to the HalfEdge pointer.  Storing
    /// them in a separate map keeps HalfEdge lightweight (three-tuple
    /// identity + derive(Hash/Eq)) and avoids duplicating properties on
    /// inbound HalfEdges.
    edge_properties: HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,

    journal: Vec<TimedEvent<NodeChangeEvent>>,
    cmd_rx: mpsc::Receiver<NodeCommand>,
    next_event_time: u64,

    /// WAL writer for crash recovery.
    wal: Option<SharedWal>,

    /// Shared read-only projection this node publishes into on each commit.
    /// `None` for standalone tasks (unit tests); `Some` under a `GraphShard`,
    /// which uses it to serve reads without a mailbox round-trip. The entry is
    /// removed by the shard when the node is slept/evicted.
    projection: Option<ShardProjection>,

    /// FIX P1-2: LRU dedup cache using BTreeMap for ordered eviction.
    /// Key: (access_time, request_id) for LRU ordering.
    /// Value: CommitReceipt.
    dedup_cache: BTreeMap<(u64, u64), CommitReceipt>,

    /// Counter for access time ordering.
    dedup_access_counter: u64,
    dedup_capacity: usize,
}

impl NodeTask {
    /// Create a new node task. Returns the task handle and the command sender.
    /// FIX P1-1: Now returns JoinHandle for panic detection.
    pub fn spawn(id: NexoraId, buffer_size: usize) -> NodeTaskHandle {
        Self::spawn_with_state_and_wal(
            id,
            BTreeMap::new(),
            HashSet::new(),
            buffer_size,
            None,
            None,
            None,
        )
    }

    /// Create a node task with pre-loaded state (for recovery from persistence).
    pub fn spawn_with_state(
        id: NexoraId,
        properties: BTreeMap<Symbol, PropertyValue>,
        edges: HashSet<HalfEdge>,
        buffer_size: usize,
    ) -> NodeTaskHandle {
        Self::spawn_with_state_and_wal(id, properties, edges, buffer_size, None, None, None)
    }

    /// P0.1: Create a node task with full state including labels and namespace.
    ///
    /// This is the new production constructor that supports all first-class fields.
    /// Used for:
    /// - WAL replay with recovered labels
    /// - Snapshot restoration
    /// - Multi-tenant deployments
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_full_state(
        id: NexoraId,
        labels: HashSet<Symbol>,
        properties: BTreeMap<Symbol, PropertyValue>,
        edges: HashSet<HalfEdge>,
        edge_properties: HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
        namespace: Option<Symbol>,
        tenant_id: Option<Symbol>,
        tombstone: Option<TombstoneRecord>,
        property_times: BTreeMap<Symbol, EventTime>,
        buffer_size: usize,
        wal: Option<SharedWal>,
        recovered_max_event_time: Option<EventTime>,
        projection: Option<ShardProjection>,
    ) -> NodeTaskHandle {
        let (cmd_tx, cmd_rx) = mpsc::channel(buffer_size);

        let now = EventTime::now();
        let next_event_time = match recovered_max_event_time {
            Some(max_recovered) => {
                let min_next = max_recovered.as_micros().saturating_add(1);
                std::cmp::max(now.as_micros(), min_next)
            }
            None => now.as_micros(),
        };

        let task = Self {
            id,
            labels,
            tombstone,
            namespace,
            tenant_id,
            properties,
            property_times,
            edges,
            edge_properties,
            journal: Vec::new(),
            cmd_rx,
            next_event_time,
            wal,
            projection,
            dedup_cache: BTreeMap::new(),
            dedup_access_counter: 0,
            dedup_capacity: 10_000,
        };

        // Publish the initial (recovered) state so reads see a resident node's
        // full state immediately after wake, before its first new commit.
        task.publish_projection();

        let handle = tokio::spawn(task.run());
        NodeTaskHandle { tx: cmd_tx, handle }
    }

    pub(crate) fn spawn_with_state_and_wal(
        id: NexoraId,
        properties: BTreeMap<Symbol, PropertyValue>,
        edges: HashSet<HalfEdge>,
        buffer_size: usize,
        wal: Option<SharedWal>,
        recovered_max_event_time: Option<EventTime>,
        projection: Option<ShardProjection>,
    ) -> NodeTaskHandle {
        let (cmd_tx, cmd_rx) = mpsc::channel(buffer_size);

        // Fix P0-4: Ensure next_event_time is at least max(now, recovered_max + 1)
        // to prevent timestamp regression after WAL replay.
        let now = EventTime::now();
        let next_event_time = match recovered_max_event_time {
            Some(max_recovered) => {
                let min_next = max_recovered.as_micros().saturating_add(1);
                std::cmp::max(now.as_micros(), min_next)
            }
            None => now.as_micros(),
        };

        let task = Self {
            id,
            // P0.1: Initialize new first-class fields with safe defaults
            labels: HashSet::new(),
            tombstone: None,
            namespace: None,
            tenant_id: None,
            // Existing fields
            properties,
            property_times: BTreeMap::new(),
            edges,
            edge_properties: HashMap::new(),
            journal: Vec::new(),
            cmd_rx,
            next_event_time,
            wal,
            projection,
            dedup_cache: BTreeMap::new(),
            dedup_access_counter: 0,
            dedup_capacity: 10_000,
        };

        task.publish_projection();

        // FIX P1-1: Capture JoinHandle to detect task panic
        let handle = tokio::spawn(task.run());

        NodeTaskHandle { tx: cmd_tx, handle }
    }

    /// Main event loop — process commands until the channel closes.
    async fn run(mut self) {
        tracing::debug!(node = %self.id, "NodeTask started");

        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                // ====== Commit Path: unified mutation with WAL + dedup ======
                NodeCommand::Mutate(req) => {
                    let result = self
                        .handle_mutate(
                            req.request_id,
                            req.operations,
                            req.event_time,
                            req.await_durable,
                        )
                        .await;
                    let _ = req.reply.send(result);
                }
                NodeCommand::SetProperty { key, value, reply } => {
                    let result = self
                        .commit_operations(
                            vec![(MutationOp::SetProperty { key, value }, None)],
                            None,
                            true,
                        )
                        .await
                        .map(|_| ());
                    let _ = reply.send(result);
                }
                NodeCommand::GetProperty { key, reply } => {
                    let result = self.handle_get_property(key);
                    let _ = reply.send(result);
                }
                NodeCommand::RemoveProperty { key, reply } => {
                    let previous = self.properties.get(&key).cloned();
                    let result = self
                        .commit_operations(
                            vec![(MutationOp::RemoveProperty { key }, None)],
                            None,
                            true,
                        )
                        .await
                        .map(|_| previous);
                    let _ = reply.send(result);
                }
                NodeCommand::GetAllProperties { reply } => {
                    let result = Ok(self.properties.clone());
                    let _ = reply.send(result);
                }
                NodeCommand::AddEdge { edge, reply } => {
                    let result = self
                        .commit_operations(vec![(MutationOp::AddEdge { edge }, None)], None, true)
                        .await
                        .map(|_| ());
                    let _ = reply.send(result);
                }
                NodeCommand::RemoveEdge { edge, reply } => {
                    let existed = self.edges.contains(&edge);
                    let result = self
                        .commit_operations(
                            vec![(MutationOp::RemoveEdge { edge }, None)],
                            None,
                            true,
                        )
                        .await
                        .map(|_| existed);
                    let _ = reply.send(result);
                }
                NodeCommand::GetEdges { edge_type, reply } => {
                    let result = self.handle_get_edges(edge_type);
                    let _ = reply.send(result);
                }
                NodeCommand::GoToSleep { reply } => {
                    // In the full implementation: persist state, then exit
                    tracing::debug!(node = %self.id, "NodeTask going to sleep");
                    let _ = reply.send(Ok(()));
                    break;
                }
                NodeCommand::DrainJournal { reply } => {
                    // Drain accumulated journal events
                    let events: Vec<_> = self.journal.drain(..).collect();
                    let _ = reply.send(events);
                }
                NodeCommand::SnapshotState { reply } => {
                    let _ = reply.send(NodeStateSnapshot {
                        labels: self.labels.clone(),
                        namespace: self.namespace.clone(),
                        tenant_id: self.tenant_id.clone(),
                        tombstone: self.tombstone.clone(),
                        properties: self.properties.clone(),
                        property_times: self.property_times.clone(),
                        edges: self.edges.clone(),
                        edge_properties: self.edge_properties.clone(),
                        journal: self.journal.clone(),
                    });
                }
                NodeCommand::MemorySize { reply } => {
                    let size = self.estimate_memory_size();
                    let _ = reply.send(size);
                }
            }
        }

        tracing::debug!(node = %self.id, "NodeTask exited");
    }

    fn next_time(&mut self) -> EventTime {
        self.next_event_time += 1;
        EventTime::from_micros(self.next_event_time)
    }

    /// Commit Path: atomically apply a batch of mutations.
    ///
    /// Flow: dedup check → WAL write → memory apply → journal update
    async fn handle_mutate(
        &mut self,
        request_id: u64,
        operations: Vec<(MutationOp, Option<EventTime>)>,
        event_time: Option<EventTime>,
        await_durable: bool,
    ) -> Result<CommitReceipt, NodeError> {
        tracing::debug!(node = %self.id, op_count = operations.len(), "NodeTask::handle_mutate");
        for (op, _) in &operations {
            tracing::trace!(node = %self.id, ?op, "handle_mutate op");
        }
        // Step 1: Dedup check — O(log n) BTreeMap lookup with LRU ordering
        // Try to find an existing entry for this request_id
        let existing_key = self
            .dedup_cache
            .iter()
            .find(|((_, rid), _)| *rid == request_id)
            .map(|(k, v)| (*k, v.clone()));

        if let Some((key, receipt)) = existing_key {
            tracing::trace!(request_id, "handle_mutate: dedup cache hit");
            // Update access time (move to end for LRU)
            self.dedup_cache.remove(&key);
            self.dedup_access_counter += 1;
            self.dedup_cache
                .insert((self.dedup_access_counter, request_id), receipt.clone());
            return Ok(receipt);
        }

        tracing::trace!(request_id, "handle_mutate: calling commit_operations");
        let receipt = self
            .commit_operations(operations, event_time, await_durable)
            .await?;

        // Step 2: Cache receipt for dedup with LRU ordering
        self.dedup_access_counter += 1;
        self.dedup_cache
            .insert((self.dedup_access_counter, request_id), receipt.clone());

        // FIX P1-2: Evict oldest 50% of entries instead of clearing entire cache
        if self.dedup_cache.len() > self.dedup_capacity {
            let evict_count = self.dedup_cache.len() / 2;
            let keys_to_evict: Vec<_> =
                self.dedup_cache.keys().take(evict_count).cloned().collect();
            for key in keys_to_evict {
                self.dedup_cache.remove(&key);
            }
        }

        Ok(receipt)
    }

    async fn commit_operations(
        &mut self,
        operations: Vec<(MutationOp, Option<EventTime>)>,
        fallback_event_time: Option<EventTime>,
        await_durable: bool,
    ) -> Result<CommitReceipt, NodeError> {
        let receipt_time = self.next_time();
        let mut next_properties = self.properties.clone();
        let mut next_property_times = self.property_times.clone();
        let mut next_edges = self.edges.clone();
        let mut next_labels = self.labels.clone();
        let mut next_edge_properties = self.edge_properties.clone();
        let mut events = Vec::new();
        // F4: count property writes/removals dropped by event-time LWW (late
        // arrivals). Surfaced in the receipt so the ingest layer can report it.
        let mut late_dropped: usize = 0;
        // The None path assigns the first op `receipt_time` and later ops a fresh
        // monotonic tick, reproducing the pre-LWW arrival-order timestamps.
        let mut receipt_time_used = false;

        for (op, op_event_time) in operations {
            tracing::trace!(?op, "commit_operations: processing op");
            // Time assigned to this op's event: per-op time (if present) →
            // fallback batch-level time → node's monotonic clock. This lets a
            // single batch coalesce records with different event times while
            // still resolving each property write correctly by its own timestamp.
            let op_time = match op_event_time.or(fallback_event_time) {
                Some(t) => t,
                None if !receipt_time_used => {
                    receipt_time_used = true;
                    receipt_time
                }
                None => self.next_time(),
            };
            let event = match op {
                MutationOp::SetProperty { key, value } => {
                    // Event-time LWW: overwrite only when this write is at least
                    // as new as the stored value's event time. A late arrival
                    // (older event time) is dropped and emits no event, so it
                    // never reaches the WAL/journal and cannot resurrect a stale
                    // value on replay. A key with no recorded time behaves as
                    // `EventTime::MIN`, so the first write always wins.
                    let wins = match next_property_times.get(&key) {
                        Some(&existing) => op_time >= existing,
                        None => true,
                    };
                    if wins {
                        next_properties.insert(key.clone(), value.clone());
                        next_property_times.insert(key.clone(), op_time);
                        Some(NodeChangeEvent::PropertySet { key, value })
                    } else {
                        late_dropped += 1;
                        tracing::trace!(
                            %key,
                            "commit_operations: dropping late SetProperty (event-time LWW)"
                        );
                        None
                    }
                }
                MutationOp::RemoveProperty { key } => {
                    // Event-time LWW for removals (F5): a removal is itself a
                    // timestamped event competing on the same per-property clock
                    // as writes. It wins only when its event time is >= the
                    // stored one; a late removal (older than the current value's
                    // write) is dropped and emits no event, so it can neither
                    // delete a newer value nor resurrect on replay. On a winning
                    // removal we RETAIN the event time as a tombstone marker
                    // (rather than clearing it), so a subsequently-arriving older
                    // write is correctly rejected while a newer write (>= the
                    // tombstone time) still wins. A key with no recorded time
                    // behaves as `EventTime::MIN`, so a first-ever removal wins.
                    let wins = match next_property_times.get(&key) {
                        Some(&existing) => op_time >= existing,
                        None => true,
                    };
                    if wins {
                        next_property_times.insert(key.clone(), op_time);
                        // A winning removal ALWAYS emits an event, even when the
                        // key was absent (e.g. a removal that arrived before its
                        // set): the event is what persists the tombstone time to
                        // the WAL/journal, so replay rebuilds the same
                        // `property_times` entry and a later out-of-order set is
                        // still rejected. `previous_value` is audit-only (replay
                        // ignores it), so `Null` stands in when there was nothing
                        // to remove.
                        let previous_value =
                            next_properties.remove(&key).unwrap_or(PropertyValue::Null);
                        Some(NodeChangeEvent::PropertyRemoved {
                            key,
                            previous_value,
                        })
                    } else {
                        late_dropped += 1;
                        tracing::trace!(
                            %key,
                            "commit_operations: dropping late RemoveProperty (event-time LWW)"
                        );
                        None
                    }
                }
                MutationOp::AddEdge { edge } => next_edges
                    .insert(edge.clone())
                    .then_some(NodeChangeEvent::EdgeAdded { edge }),
                MutationOp::RemoveEdge { edge } => next_edges
                    .remove(&edge)
                    .then_some(NodeChangeEvent::EdgeRemoved { edge }),
                MutationOp::AddLabel { label } => next_labels
                    .insert(label.clone())
                    .then_some(NodeChangeEvent::LabelAdded { label }),
                MutationOp::RemoveLabel { label } => next_labels
                    .remove(&label)
                    .then_some(NodeChangeEvent::LabelRemoved { label }),
                MutationOp::EdgePropertySet {
                    edge_type,
                    dst,
                    key,
                    value,
                } => {
                    // Write into edge_properties using (edge_type, other) key.
                    // Properties belong to the edge relationship, not the
                    // HalfEdge pointer — this keeps HalfEdge a pure three-tuple
                    // identity while edge data lives in a separate store.
                    let ekey = (edge_type.clone(), dst.clone());
                    tracing::trace!(?edge_type, ?dst, ?key, "NodeTask: inserting edge property");
                    next_edge_properties
                        .entry(ekey)
                        .or_default()
                        .insert(key.clone(), value.clone());
                    tracing::trace!(
                        edges = next_edge_properties.len(),
                        "NodeTask: next_edge_properties updated"
                    );
                    Some(NodeChangeEvent::EdgePropertySet {
                        edge_type,
                        target: dst,
                        key,
                        value,
                    })
                }
                MutationOp::DeleteNode { tombstone } => {
                    self.tombstone = Some(tombstone.clone());
                    Some(NodeChangeEvent::NodeDeleted { tombstone })
                }
            };

            if let Some(event) = event {
                events.push(TimedEvent::new(event, op_time));
            }
        }

        if let Some(wal) = &self.wal {
            // Append under the lock. Under group commit this only buffers the
            // record and returns its sequence number; the background flusher
            // performs the fsync. We grab the durable-watermark receiver while
            // still holding the lock so there is no lost-wakeup window between
            // append and subscribe.
            let (seq, durable_rx) = {
                let mut guard = wal.lock().await;
                let seq = guard
                    .append(WalOperation::NodeEvents {
                        qid: self.id.clone(),
                        events: events.clone(),
                    })
                    .map_err(|e| NodeError::Internal(format!("WAL write failed: {e}")))?;
                (seq, guard.durable_receiver())
            };

            // Group commit: block the receipt until the flusher has fsynced up
            // to our sequence. This is the durability barrier — a returned
            // receipt guarantees the write survives a crash. Under
            // Always/EveryN the append already synced (or intentionally did
            // not), so there is no receiver and nothing to await.
            //
            // `await_durable == false` (relaxed batch ingest) skips the barrier:
            // the record is buffered and will be fsynced by the flusher, but the
            // receipt returns immediately. A crash can lose the last un-fsynced
            // batch; the ingest source replays from its upstream offset. The
            // record is still durably ordered in the WAL once the flusher runs.
            if await_durable {
                if let Some(mut rx) = durable_rx {
                    while *rx.borrow() < seq {
                        if rx.changed().await.is_err() {
                            // Flusher gone (shutdown/panic). Fall back to a direct
                            // sync so we never ack an un-synced write.
                            wal.lock().await.sync().map_err(|e| {
                                NodeError::Internal(format!("WAL sync failed: {e}"))
                            })?;
                            break;
                        }
                    }
                }
            }
        }

        self.properties = next_properties;
        self.property_times = next_property_times;
        self.edges = next_edges;
        self.edge_properties = next_edge_properties;
        tracing::trace!(
            edges = self.edge_properties.len(),
            "NodeTask: committed edge_properties"
        );
        self.labels = next_labels;
        self.journal.extend(events.clone());

        // #1: Publish the new state into the shard projection *before* returning
        // the receipt. Because the node task is single-threaded and this runs
        // synchronously within the commit, any caller that observes the receipt
        // is guaranteed to see this write in the projection (read-your-writes).
        self.publish_projection();

        Ok(CommitReceipt {
            event_time: receipt_time,
            event_count: events.len(),
            late_dropped,
        })
    }

    /// Publish the current node state as an immutable snapshot into the shard
    /// projection, if one is attached. Builds a fresh `Arc<NodeReadState>` on
    /// each commit; readers hold the `Arc`, so an in-flight read keeps its
    /// snapshot alive even as this replaces the map entry. No-op for standalone
    /// tasks (no projection).
    fn publish_projection(&self) {
        if let Some(projection) = &self.projection {
            let state = Arc::new(NodeReadState {
                labels: self.labels.clone(),
                properties: self.properties.clone(),
                property_times: self.property_times.clone(),
                edges: self.edges.clone(),
                edge_properties: self.edge_properties.clone(),
                tombstone: self.tombstone.clone(),
            });
            projection.insert(self.id.clone(), state);
        }
    }

    fn handle_get_property(&self, key: Symbol) -> Result<Option<PropertyValue>, NodeError> {
        Ok(self.properties.get(&key).cloned())
    }

    fn handle_get_edges(&self, edge_type: Option<Symbol>) -> Result<Vec<HalfEdge>, NodeError> {
        let edges: Vec<_> = match edge_type {
            Some(ref typ) => self
                .edges
                .iter()
                .filter(|e| e.edge_type == *typ)
                .cloned()
                .collect(),
            None => self.edges.iter().cloned().collect(),
        };
        Ok(edges)
    }

    fn estimate_memory_size(&self) -> usize {
        let prop_size: usize = self
            .properties
            .iter()
            .map(|(k, v)| 24 + k.len() + v.memory_size())
            .sum();
        let edge_size: usize = self.edges.iter().map(|e| e.memory_size()).sum();
        let journal_size = self.journal.len() * 64; // rough estimate
        prop_size + edge_size + journal_size + 128 // base overhead
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_set_get_property() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        // Set property
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::SetProperty {
            key: Symbol::new("name"),
            value: PropertyValue::String("Alice".into()),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // Get property
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new("name"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        let result = reply_rx.await.unwrap().unwrap();
        assert_eq!(result, Some(PropertyValue::String("Alice".into())));
    }

    #[tokio::test]
    async fn test_node_edge_operations() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        let target = NexoraId::new_random();
        let edge = HalfEdge::out(Symbol::new("KNOWS"), target);

        // Add edge
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::AddEdge {
            edge: edge.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // Get edges
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetEdges {
            edge_type: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
        let edges = reply_rx.await.unwrap().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], edge);
    }

    #[tokio::test]
    async fn test_node_remove_property() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        // Set then remove
        let (reply_tx, _) = oneshot::channel();
        tx.send(NodeCommand::SetProperty {
            key: Symbol::new("temp"),
            value: PropertyValue::Float(99.9),
            reply: reply_tx,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::RemoveProperty {
            key: Symbol::new("temp"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        let removed = reply_rx.await.unwrap().unwrap();
        assert_eq!(removed, Some(PropertyValue::Float(99.9)));

        // Should be gone now
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new("temp"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(reply_rx.await.unwrap().unwrap(), None);
    }

    #[tokio::test]
    async fn test_node_memory_size() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid.clone(), 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::MemorySize { reply: reply_tx })
            .await
            .unwrap();
        let size = reply_rx.await.unwrap();
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_node_spawn_with_state() {
        let qid = NexoraId::new_random();
        let mut props = BTreeMap::new();
        props.insert(Symbol::new("name"), PropertyValue::String("Bob".into()));

        let NodeTaskHandle { tx, .. } = NodeTask::spawn_with_state(qid, props, HashSet::new(), 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new("name"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap(),
            Some(PropertyValue::String("Bob".into()))
        );
    }

    // ====== NT-004: RemoveProperty 不存在的 key ======
    #[tokio::test]
    async fn test_remove_nonexistent_property() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::RemoveProperty {
            key: Symbol::new("nonexistent"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(reply_rx.await.unwrap().unwrap(), None);
    }

    // ====== NT-005: GetAllProperties ======
    #[tokio::test]
    async fn test_get_all_properties() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        tx.send(NodeCommand::SetProperty {
            key: Symbol::new("a"),
            value: PropertyValue::Integer(1),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();
        tx.send(NodeCommand::SetProperty {
            key: Symbol::new("b"),
            value: PropertyValue::Integer(2),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetAllProperties { reply: reply_tx })
            .await
            .unwrap();
        let props = reply_rx.await.unwrap().unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(
            props.get(&Symbol::new("a")),
            Some(&PropertyValue::Integer(1))
        );
        assert_eq!(
            props.get(&Symbol::new("b")),
            Some(&PropertyValue::Integer(2))
        );
    }

    // ====== NT-007: RemoveEdge 存在的边 ======
    #[tokio::test]
    async fn test_remove_existing_edge() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);
        let edge = HalfEdge::out(Symbol::new("KNOWS"), NexoraId::new_random());

        tx.send(NodeCommand::AddEdge {
            edge: edge.clone(),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::RemoveEdge {
            edge: edge.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().unwrap());
    }

    // ====== NT-008: RemoveEdge 不存在的边 ======
    #[tokio::test]
    async fn test_remove_nonexistent_edge() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);
        let edge = HalfEdge::out(Symbol::new("KNOWS"), NexoraId::new_random());

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::RemoveEdge {
            edge,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(!reply_rx.await.unwrap().unwrap());
    }

    // ====== NT-010: GetEdges 按类型过滤 ======
    #[tokio::test]
    async fn test_get_edges_by_type() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        tx.send(NodeCommand::AddEdge {
            edge: HalfEdge::out(Symbol::new("KNOWS"), NexoraId::new_random()),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();
        tx.send(NodeCommand::AddEdge {
            edge: HalfEdge::out(Symbol::new("IN_ZONE"), NexoraId::new_random()),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetEdges {
            edge_type: Some(Symbol::new("KNOWS")),
            reply: reply_tx,
        })
        .await
        .unwrap();
        let edges = reply_rx.await.unwrap().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].edge_type.as_str(), "KNOWS");
    }

    // ====== NT-012: DrainJournal ======
    #[tokio::test]
    async fn test_drain_journal() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        // 写入 3 个属性
        for i in 0..3 {
            tx.send(NodeCommand::SetProperty {
                key: Symbol::new("x"),
                value: PropertyValue::Integer(i),
                reply: oneshot::channel().0,
            })
            .await
            .unwrap();
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: reply_tx })
            .await
            .unwrap();
        let events = reply_rx.await.unwrap();
        assert_eq!(events.len(), 3);

        // 再次 drain 应为空
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: reply_tx })
            .await
            .unwrap();
        let events = reply_rx.await.unwrap();
        assert!(events.is_empty());
    }

    // ====== NT-013: GoToSleep 导致 channel 断开 ======
    #[tokio::test]
    async fn test_go_to_sleep_disconnects() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GoToSleep { reply: reply_tx })
            .await
            .unwrap();
        assert!(reply_rx.await.unwrap().is_ok());

        // 后续发送应失败（channel 已关闭）
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let result = tx
            .send(NodeCommand::MemorySize {
                reply: oneshot::channel().0,
            })
            .await;
        assert!(result.is_err(), "channel should be closed after GoToSleep");
    }

    // ====== NT-015: 属性覆盖写 ======
    #[tokio::test]
    async fn test_property_overwrite() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        for i in 0..10 {
            tx.send(NodeCommand::SetProperty {
                key: Symbol::new("x"),
                value: PropertyValue::Integer(i),
                reply: oneshot::channel().0,
            })
            .await
            .unwrap();
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new("x"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap(),
            Some(PropertyValue::Integer(9))
        );
    }

    // ====== NT-016: Journal 事件顺序和数量 ======
    #[tokio::test]
    async fn test_journal_event_ordering() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        tx.send(NodeCommand::SetProperty {
            key: Symbol::new("a"),
            value: PropertyValue::Integer(1),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();
        tx.send(NodeCommand::AddEdge {
            edge: HalfEdge::out(Symbol::new("X"), NexoraId::new_random()),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();
        tx.send(NodeCommand::SetProperty {
            key: Symbol::new("b"),
            value: PropertyValue::Integer(2),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: reply_tx })
            .await
            .unwrap();
        let events = reply_rx.await.unwrap();
        assert_eq!(events.len(), 3);

        // 验证事件顺序：PropertySet, EdgeAdded, PropertySet
        assert!(
            matches!(&events[0].event, NodeChangeEvent::PropertySet { key, .. } if key.as_str() == "a")
        );
        assert!(matches!(
            &events[1].event,
            NodeChangeEvent::EdgeAdded { .. }
        ));
        assert!(
            matches!(&events[2].event, NodeChangeEvent::PropertySet { key, .. } if key.as_str() == "b")
        );

        // 验证时间单调递增
        assert!(events[0].time < events[1].time);
        assert!(events[1].time < events[2].time);
    }

    // ====== A-01: EdgeRemoved 产生 journal 事件 ======
    #[tokio::test]
    async fn test_edge_removed_produces_journal_event() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);
        let edge = HalfEdge::out(Symbol::new("KNOWS"), NexoraId::new_random());

        // 添加边
        tx.send(NodeCommand::AddEdge {
            edge: edge.clone(),
            reply: oneshot::channel().0,
        })
        .await
        .unwrap();

        // 删除边
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::RemoveEdge {
            edge: edge.clone(),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(reply_rx.await.unwrap().unwrap());

        // 验证 journal 包含 EdgeAdded + EdgeRemoved
        let (drain_tx, drain_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: drain_tx })
            .await
            .unwrap();
        let events = drain_rx.await.unwrap();
        assert_eq!(events.len(), 2, "Should have EdgeAdded + EdgeRemoved");
        assert!(matches!(
            &events[0].event,
            NodeChangeEvent::EdgeAdded { .. }
        ));
        assert!(matches!(
            &events[1].event,
            NodeChangeEvent::EdgeRemoved { .. }
        ));
    }

    // ====== 删除不存在的边不产生 journal 事件 ======
    #[tokio::test]
    async fn test_remove_nonexistent_edge_no_journal() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);
        let edge = HalfEdge::out(Symbol::new("KNOWS"), NexoraId::new_random());

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::RemoveEdge {
            edge,
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert!(!reply_rx.await.unwrap().unwrap());

        // Journal 应为空（删除不存在的边不产生事件）
        let (drain_tx, drain_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: drain_tx })
            .await
            .unwrap();
        assert!(drain_rx.await.unwrap().is_empty());
    }

    // ====== A-04: Commit Path — Mutate 基础 ======
    #[tokio::test]
    async fn test_mutate_single_property() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 1,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new("speed"),
                    value: PropertyValue::Float(12.5),
                },
                None,
            )],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();

        let receipt = reply_rx.await.unwrap().unwrap();
        assert_eq!(receipt.event_count, 1);
        assert!(receipt.event_time.as_micros() > 0);

        // 验证属性已设置
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new("speed"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap(),
            Some(PropertyValue::Float(12.5))
        );
    }

    // ====== A-04: Commit Path — 批量 Mutate ======
    #[tokio::test]
    async fn test_mutate_batch() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 2,
            operations: vec![
                (
                    MutationOp::SetProperty {
                        key: Symbol::new("name"),
                        value: PropertyValue::String("FL-042".into()),
                    },
                    None,
                ),
                (
                    MutationOp::SetProperty {
                        key: Symbol::new("speed"),
                        value: PropertyValue::Float(12.5),
                    },
                    None,
                ),
                (
                    MutationOp::AddEdge {
                        edge: HalfEdge::out(Symbol::new("IN_ZONE"), NexoraId::new_random()),
                    },
                    None,
                ),
            ],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();

        let receipt = reply_rx.await.unwrap().unwrap();
        assert_eq!(receipt.event_count, 3);
    }

    // ====== A-04: Commit Path — request_id 幂等 ======
    #[tokio::test]
    async fn test_mutate_idempotency() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        // 第一次提交
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 42,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new("x"),
                    value: PropertyValue::Integer(1),
                },
                None,
            )],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();
        let receipt1 = reply_rx.await.unwrap().unwrap();

        // 第二次提交相同 request_id（幂等）
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 42,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new("x"),
                    value: PropertyValue::Integer(999), // 不同的值
                },
                None,
            )],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();
        let receipt2 = reply_rx.await.unwrap().unwrap();

        // 幂等：应返回相同 receipt，不应修改值
        assert_eq!(receipt1.event_time, receipt2.event_time);
        assert_eq!(receipt1.event_count, receipt2.event_count);

        // 值应为第一次写入的 1，不是 999
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new("x"),
            reply: reply_tx,
        })
        .await
        .unwrap();
        assert_eq!(
            reply_rx.await.unwrap().unwrap(),
            Some(PropertyValue::Integer(1))
        );
    }

    // ====== A-04: Commit Path — Journal 包含 Mutate 事件 ======
    #[tokio::test]
    async fn test_mutate_journal_events() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 5,
            operations: vec![
                (
                    MutationOp::SetProperty {
                        key: Symbol::new("a"),
                        value: PropertyValue::Integer(1),
                    },
                    None,
                ),
                (
                    MutationOp::AddEdge {
                        edge: HalfEdge::out(Symbol::new("X"), NexoraId::new_random()),
                    },
                    None,
                ),
            ],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();
        let _ = reply_rx.await.unwrap().unwrap();

        // Drain journal
        let (drain_tx, drain_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: drain_tx })
            .await
            .unwrap();
        let events = drain_rx.await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0].event, NodeChangeEvent::PropertySet { key, .. } if key.as_str() == "a")
        );
        assert!(matches!(
            &events[1].event,
            NodeChangeEvent::EdgeAdded { .. }
        ));
    }

    // ====== Event-time LWW: out-of-order writes ======

    /// Send a single SetProperty at a given event time and await the receipt.
    async fn set_prop_at(
        tx: &mpsc::Sender<NodeCommand>,
        request_id: u64,
        key: &str,
        value: PropertyValue,
        event_time: Option<EventTime>,
    ) {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new(key),
                    value,
                },
                event_time,
            )],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap();
    }

    async fn get_prop(tx: &mpsc::Sender<NodeCommand>, key: &str) -> Option<PropertyValue> {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::GetProperty {
            key: Symbol::new(key),
            reply: reply_tx,
        })
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn late_write_does_not_overwrite_newer_value() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        // Newer event time arrives first.
        set_prop_at(
            &tx,
            1,
            "temp",
            PropertyValue::Integer(20),
            Some(EventTime::from_micros(10_000)),
        )
        .await;
        // Older (late) event arrives second — must be dropped by event-time LWW.
        set_prop_at(
            &tx,
            2,
            "temp",
            PropertyValue::Integer(5),
            Some(EventTime::from_micros(5_000)),
        )
        .await;

        assert_eq!(
            get_prop(&tx, "temp").await,
            Some(PropertyValue::Integer(20)),
            "late lower-event-time write must not overwrite the newer value"
        );
    }

    #[tokio::test]
    async fn newer_event_time_wins_regardless_of_arrival() {
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        set_prop_at(
            &tx,
            1,
            "temp",
            PropertyValue::Integer(5),
            Some(EventTime::from_micros(5_000)),
        )
        .await;
        set_prop_at(
            &tx,
            2,
            "temp",
            PropertyValue::Integer(20),
            Some(EventTime::from_micros(10_000)),
        )
        .await;

        assert_eq!(
            get_prop(&tx, "temp").await,
            Some(PropertyValue::Integer(20)),
            "the highest-event-time write wins"
        );
    }

    #[tokio::test]
    async fn late_write_emits_no_journal_event() {
        // A dropped late write must not reach the journal/WAL, or replay would
        // re-evaluate it. The winning write's event is the only one recorded.
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        set_prop_at(
            &tx,
            1,
            "temp",
            PropertyValue::Integer(20),
            Some(EventTime::from_micros(10_000)),
        )
        .await;
        set_prop_at(
            &tx,
            2,
            "temp",
            PropertyValue::Integer(5),
            Some(EventTime::from_micros(5_000)),
        )
        .await;

        let (drain_tx, drain_rx) = oneshot::channel();
        tx.send(NodeCommand::DrainJournal { reply: drain_tx })
            .await
            .unwrap();
        let events = drain_rx.await.unwrap();
        assert_eq!(
            events.len(),
            1,
            "only the winning write is journaled; the late write is dropped"
        );
        assert_eq!(events[0].time, EventTime::from_micros(10_000));
    }

    #[tokio::test]
    async fn none_event_time_preserves_arrival_order() {
        // Without event times (legacy/single-write paths), behavior is unchanged:
        // last arrival wins.
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        set_prop_at(&tx, 1, "temp", PropertyValue::Integer(1), None).await;
        set_prop_at(&tx, 2, "temp", PropertyValue::Integer(2), None).await;

        assert_eq!(
            get_prop(&tx, "temp").await,
            Some(PropertyValue::Integer(2)),
            "arrival-order LWW preserved when no event time is supplied"
        );
    }

    /// Send a single RemoveProperty at a given event time and await the receipt.
    async fn remove_prop_at(
        tx: &mpsc::Sender<NodeCommand>,
        request_id: u64,
        key: &str,
        event_time: Option<EventTime>,
    ) {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::RemoveProperty {
                    key: Symbol::new(key),
                },
                event_time,
            )],
            event_time: None,
            reply: reply_tx,
            await_durable: true,
        }))
        .await
        .unwrap();
        reply_rx.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn late_remove_does_not_delete_newer_value() {
        // set@t=10 then a late remove@t=5 must NOT delete the newer value.
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        set_prop_at(
            &tx,
            1,
            "temp",
            PropertyValue::Integer(20),
            Some(EventTime::from_micros(10_000)),
        )
        .await;
        remove_prop_at(&tx, 2, "temp", Some(EventTime::from_micros(5_000))).await;

        assert_eq!(
            get_prop(&tx, "temp").await,
            Some(PropertyValue::Integer(20)),
            "a late (older-event-time) remove must not delete a newer value"
        );
    }

    #[tokio::test]
    async fn receipt_reports_late_dropped_count() {
        // F4: a late (older-event-time) write is dropped by LWW and the receipt
        // must report late_dropped=1 so the ingest layer can meter it.
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        // First write at t=10 wins (no prior value).
        let (rtx, rrx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 1,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new("temp"),
                    value: PropertyValue::Integer(20),
                },
                Some(EventTime::from_micros(10_000)),
            )],
            event_time: None,
            reply: rtx,
            await_durable: true,
        }))
        .await
        .unwrap();
        let first = rrx.await.unwrap().unwrap();
        assert_eq!(first.late_dropped, 0, "first write is not late");
        assert_eq!(first.event_count, 1, "first write emits an event");

        // Late write at t=5 is dropped by LWW → late_dropped=1, no event.
        let (rtx, rrx) = oneshot::channel();
        tx.send(NodeCommand::Mutate(MutationRequest {
            request_id: 2,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new("temp"),
                    value: PropertyValue::Integer(5),
                },
                Some(EventTime::from_micros(5_000)),
            )],
            event_time: None,
            reply: rtx,
            await_durable: true,
        }))
        .await
        .unwrap();
        let late = rrx.await.unwrap().unwrap();
        assert_eq!(
            late.late_dropped, 1,
            "late write must be counted as dropped"
        );
        assert_eq!(late.event_count, 0, "dropped write emits no event");
    }

    #[tokio::test]
    async fn newer_remove_wins_over_older_set() {
        // set@t=5 then remove@t=10 → deleted.
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        set_prop_at(
            &tx,
            1,
            "temp",
            PropertyValue::Integer(5),
            Some(EventTime::from_micros(5_000)),
        )
        .await;
        remove_prop_at(&tx, 2, "temp", Some(EventTime::from_micros(10_000))).await;

        assert_eq!(
            get_prop(&tx, "temp").await,
            None,
            "a newer remove wins over an older set"
        );
    }

    #[tokio::test]
    async fn stale_set_after_remove_is_not_resurrected() {
        // The resurrection case the tombstone fixes: remove@t=10, then a late
        // set@t=5 arrives. Without a tombstone-with-time the stale set would win
        // (no recorded time → treated as MIN). With it, the set loses.
        let qid = NexoraId::new_random();
        let NodeTaskHandle { tx, .. } = NodeTask::spawn(qid, 16);

        set_prop_at(
            &tx,
            1,
            "temp",
            PropertyValue::Integer(20),
            Some(EventTime::from_micros(8_000)),
        )
        .await;
        remove_prop_at(&tx, 2, "temp", Some(EventTime::from_micros(10_000))).await;
        // Late set with an event time below the tombstone must be dropped.
        set_prop_at(
            &tx,
            3,
            "temp",
            PropertyValue::Integer(5),
            Some(EventTime::from_micros(5_000)),
        )
        .await;

        assert_eq!(
            get_prop(&tx, "temp").await,
            None,
            "a stale set below the remove tombstone must not resurrect the value"
        );
    }
}
