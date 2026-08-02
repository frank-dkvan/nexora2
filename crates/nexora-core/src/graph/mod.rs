//! Graph engine — NodeTask, GraphShard, GraphService.
//!
//! This module implements the Actor-per-Node model:
//! - Each graph node runs as an independent `tokio::spawn` task
//! - Communication is via `mpsc` channels (local) or Zenoh (remote)
//! - The `GraphShard` manages a collection of nodes with lifecycle control
//! - The `GraphService` orchestrates all shards

pub mod node_task;
pub mod projection;
pub mod shard;

pub use node_task::{CommitReceipt, MutationOp, MutationRequest, NodeCommand, NodeError, NodeTask};
pub use projection::{NodeReadState, ShardProjection};
pub use shard::{GraphShard, ShardError};

use crate::edge_index::EdgeIndex;
use crate::event::NodeChangeEvent;
use crate::graph::node_task::TombstoneRecord;
use crate::index::PropertyIndex;
use crate::persistor::NamespacedPersistenceAgent;
use crate::LabelIndex;
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};

/// Configuration for the graph service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphServiceConfig {
    /// Number of shards (logical partitions of the node space).
    pub num_shards: usize,
    /// Maximum number of nodes kept in memory per shard before LRU eviction.
    pub max_nodes_per_shard: usize,
    /// Channel buffer size for each node task.
    pub node_channel_size: usize,
}

impl Default for GraphServiceConfig {
    fn default() -> Self {
        Self {
            num_shards: 256,
            max_nodes_per_shard: 10_000,
            node_channel_size: 64,
        }
    }
}

/// Outcome of an idle-node eviction sweep across all shards.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvictionStats {
    /// Nodes put to sleep (state persisted, memory released) this sweep.
    pub evicted: usize,
    /// Nodes examined that were still within the TTL (left resident).
    pub retained: usize,
}

/// The top-level graph service — manages shards and routes operations.
///
/// This is the central orchestrator. All graph operations flow through here.
/// In single-node mode, all operations route to local shards.
/// In distributed mode, the HybridRouter wraps this to add Zenoh routing.
/// Callback invoked when a property changes on any node.
/// Used to trigger Standing Query evaluation.
pub type PropertyChangeCallback = Arc<
    dyn Fn(
            NexoraId,
            String,
            PropertyValue,
            std::collections::HashMap<String, PropertyValue>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// P0.5: Callback invoked on any graph mutation (labels, edges, properties,
/// tombstone). Used to trigger Standing Query re-evaluation for events beyond
/// just property changes.
pub type GraphMutationCallback = Arc<
    dyn Fn(
            NexoraId,
            NodeChangeEvent,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// Durability mode for a batch write (see [`WriteBatchOptions`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchDurability {
    /// Each write blocks until the WAL group-commit flusher has fsynced it
    /// before its receipt returns — a completed batch implies every write in it
    /// is on disk (`ack = durable`). Use for CDC / financial ingest where loss
    /// is unacceptable.
    WaitDurable,
    /// Writes return once buffered; the flusher fsyncs asynchronously. A crash
    /// may lose the last un-fsynced writes, which the ingest source recovers by
    /// replaying from its upstream offset (`ack = buffered`). Higher throughput;
    /// use for replayable stream/file ingest that tolerates bounded loss.
    Relaxed,
}

/// Options controlling a [`GraphService::write_batch`] call.
#[derive(Clone, Copy, Debug)]
pub struct WriteBatchOptions {
    /// Max number of per-node commits issued concurrently. Bounds the in-flight
    /// fan-out so a huge batch can't spawn unbounded work; also the width over
    /// which one group-commit fsync amortizes. A sensible default is a small
    /// multiple of the shard count.
    pub concurrency: usize,
    /// Durability mode for every write in the batch.
    pub durability: BatchDurability,
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            concurrency: 64,
            durability: BatchDurability::WaitDurable,
        }
    }
}

/// One node's slice of a write batch: its id, the ops to apply (each with an
/// optional per-op event time for event-time LWW), and an optional node-level
/// fallback event time used for ops that carry none.
pub type EventTimedWriteItem = (
    NexoraId,
    Vec<(node_task::MutationOp, Option<EventTime>)>,
    Option<EventTime>,
);

pub struct GraphService {
    config: GraphServiceConfig,
    shards: Vec<Arc<RwLock<GraphShard>>>,
    persistor: Arc<dyn NamespacedPersistenceAgent>,
    /// Optional callback fired on property changes (for SQ trigger)
    sq_callback: Option<PropertyChangeCallback>,
    /// P0.5: Optional callback fired on all mutation types (for SQ trigger)
    mutation_callback: Option<GraphMutationCallback>,
    /// Registered standing queries: id -> (name, pattern_json)
    standing_queries: RwLock<HashMap<String, (String, String)>>,
    /// P0.4: Label index for O(1) label-to-node-id lookups
    pub label_index: LabelIndex,
    /// P0.4: Property index for exact-match and range property queries
    pub property_index: PropertyIndex,
    /// P0.4: Edge index for O(1) edge-type lookups (forward + reverse)
    pub edge_index: EdgeIndex,
    /// Monotonic source of per-commit idempotency keys for internally-issued
    /// mutations (e.g. `write_batch`). Each node dedups on `request_id`, so
    /// batch writes must carry distinct ids or the dedup cache would silently
    /// skip a batch that happened to reuse one.
    next_request_id: std::sync::atomic::AtomicU64,
}

// ============================================================
// Graph Capability Traits (design section 5.4)
// ============================================================

/// Literal property and edge operations.
#[async_trait::async_trait]
pub trait LiteralOpsGraph: Send + Sync {
    async fn set_property(
        &self,
        qid: &NexoraId,
        key: &str,
        value: PropertyValue,
    ) -> Result<(), GraphError>;
    async fn get_property(
        &self,
        qid: &NexoraId,
        key: &str,
    ) -> Result<Option<PropertyValue>, GraphError>;
    async fn add_edge(
        &self,
        qid: &NexoraId,
        edge: nexora_value::HalfEdge,
    ) -> Result<(), GraphError>;
    async fn get_edges(&self, qid: &NexoraId) -> Result<Vec<nexora_value::HalfEdge>, GraphError>;
    async fn sleep_node(&self, qid: &NexoraId) -> Result<(), GraphError>;
}

/// Standing Query operations (placeholder — implemented in Phase 4).
#[async_trait::async_trait]
pub trait StandingQueryOpsGraph: Send + Sync {
    async fn register_standing_query(
        &self,
        _name: &str,
        _query: &str,
    ) -> Result<String, GraphError> {
        Err(GraphError::Internal(
            "Standing queries not yet implemented".into(),
        ))
    }
    async fn remove_standing_query(&self, _sq_id: &str) -> Result<(), GraphError> {
        Err(GraphError::Internal(
            "Standing queries not yet implemented".into(),
        ))
    }
}

/// Cypher query execution (placeholder — implemented in Phase 3).
#[async_trait::async_trait]
pub trait CypherOpsGraph: Send + Sync {
    async fn execute_cypher(&self, _query: &str) -> Result<String, GraphError> {
        Err(GraphError::Internal(
            "Cypher execution not yet implemented".into(),
        ))
    }
}

/// Full graph service trait — combines all capabilities.
pub trait FullGraphService: LiteralOpsGraph + StandingQueryOpsGraph + CypherOpsGraph {
    fn shard_count(&self) -> usize;
    fn persistor(&self) -> &dyn NamespacedPersistenceAgent;
}

/// Handle to the Standing Query engine for triggering evaluations.
pub type StandingQueryHandle = Arc<
    dyn Fn(
            NexoraId,
            String,
            PropertyValue,
            HashMap<String, PropertyValue>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

impl GraphService {
    /// Create a new graph service with persistence backend (no WAL).
    pub fn new(config: GraphServiceConfig, persistor: Arc<dyn NamespacedPersistenceAgent>) -> Self {
        assert!(
            config.num_shards > 0,
            "num_shards must be greater than zero"
        );
        assert!(
            config.max_nodes_per_shard > 0,
            "max_nodes_per_shard must be greater than zero"
        );
        assert!(
            config.node_channel_size > 0,
            "node_channel_size must be greater than zero"
        );
        let shards: Vec<_> = (0..config.num_shards)
            .map(|id| {
                Arc::new(RwLock::new(GraphShard::new(
                    id,
                    config.max_nodes_per_shard,
                    config.node_channel_size,
                    persistor.clone(),
                )))
            })
            .collect();
        Self {
            config,
            shards,
            persistor,
            sq_callback: None,
            mutation_callback: None,
            standing_queries: RwLock::new(HashMap::new()),
            label_index: LabelIndex::new(),
            property_index: PropertyIndex::new(),
            edge_index: EdgeIndex::new(),
            next_request_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Create a new graph service with WAL enabled for crash recovery.
    ///
    /// Each shard gets its own WAL file under `wal_dir/shard_{id}/`.
    /// On startup, call `replay_all_wals()` to recover uncommitted state.
    ///
    /// If `wal_encryption_key` is provided, WAL payloads are encrypted with AES-256-GCM.
    pub fn new_with_wal(
        config: GraphServiceConfig,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
        wal_dir: PathBuf,
        wal_encryption_key: Option<[u8; 32]>,
    ) -> std::io::Result<Self> {
        Self::new_with_wal_policy(
            config,
            persistor,
            wal_dir,
            wal_encryption_key,
            crate::wal::WalSyncPolicy::Group {
                max_ops: 256,
                max_delay: std::time::Duration::from_micros(500),
                max_bytes: None,
            },
        )
    }

    /// Like [`Self::new_with_wal`] but with an explicit WAL sync policy.
    ///
    /// Lets operators (and benchmarks) pick the durability/throughput tradeoff:
    /// `Always` fsyncs every write (lowest single-write latency floor, worst
    /// throughput under load); `Group { .. }` amortizes one fsync across
    /// concurrent committers (best throughput, bounded added latency).
    #[allow(unused_variables)]
    pub fn new_with_wal_policy(
        config: GraphServiceConfig,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
        wal_dir: PathBuf,
        wal_encryption_key: Option<[u8; 32]>,
        wal_sync_policy: crate::wal::WalSyncPolicy,
    ) -> std::io::Result<Self> {
        if config.num_shards == 0
            || config.max_nodes_per_shard == 0
            || config.node_channel_size == 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "num_shards, max_nodes_per_shard, and node_channel_size must be greater than zero",
            ));
        }
        let mut shards = Vec::with_capacity(config.num_shards);
        for id in 0..config.num_shards {
            let shard_wal_dir = wal_dir.join(format!("shard_{id}"));
            let shard = GraphShard::with_wal_policy(
                id,
                config.max_nodes_per_shard,
                config.node_channel_size,
                persistor.clone(),
                shard_wal_dir,
                wal_encryption_key,
                wal_sync_policy,
            )?;
            shards.push(Arc::new(RwLock::new(shard)));
        }
        Ok(Self {
            config,
            shards,
            persistor,
            sq_callback: None,
            mutation_callback: None,
            standing_queries: RwLock::new(HashMap::new()),
            label_index: LabelIndex::new(),
            property_index: PropertyIndex::new(),
            edge_index: EdgeIndex::new(),
            next_request_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Replay all shard WALs on startup.
    /// Should be called once after `new_with_wal()` to recover uncommitted state.
    /// Returns total number of WAL records replayed.
    pub async fn replay_all_wals(&self) -> Result<usize, GraphError> {
        let mut total = 0;
        for shard in &self.shards {
            let mut shard = shard.write().await;
            total += shard.replay_wal().await?;
        }
        if total > 0 {
            tracing::info!(
                "WAL recovery complete: {} records replayed across {} shards",
                total,
                self.shards.len()
            );
        }
        // The label index is GraphService-level in-memory state; shard WAL replay
        // restores each node's own labels (in its journal/snapshot) but never
        // repopulates this cross-node index. Rebuild it from persisted state so
        // label scans (`MATCH (n:Forklift)`, the pg-wire table catalog, Standing
        // Query LabelFilters) work after a restart instead of silently returning
        // nothing.
        self.rebuild_label_index().await?;
        Ok(total)
    }

    /// Rebuild the in-memory label index from persisted nodes.
    ///
    /// Enumerates every persisted node id and re-registers its labels. Called
    /// after WAL replay on startup. Best-effort per node: a node that can't be
    /// read is logged and skipped rather than failing the whole recovery.
    pub async fn rebuild_label_index(&self) -> Result<usize, GraphError> {
        let map_persist = |e: crate::persistor::PersistenceError| {
            GraphError::Internal(format!("enumerate nodes for label rebuild: {e}"))
        };
        let mut seen: std::collections::HashSet<NexoraId> = std::collections::HashSet::new();
        for qid in self
            .persistor
            .enumerate_journal_node_ids()
            .await
            .map_err(map_persist)?
        {
            seen.insert(qid);
        }
        for qid in self
            .persistor
            .enumerate_snapshot_node_ids()
            .await
            .map_err(map_persist)?
        {
            seen.insert(qid);
        }

        let mut labelled = 0usize;
        for qid in seen {
            match self.get_labels(&qid).await {
                Ok(labels) => {
                    for label in labels {
                        self.label_index
                            .add_label(label.as_str(), qid.clone())
                            .await;
                        labelled += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(node = %qid, error = %e, "rebuild_label_index: skip node");
                }
            }
        }
        if labelled > 0 {
            tracing::info!("Label index rebuilt: {labelled} label assignments restored");
        }
        Ok(labelled)
    }

    /// Route a command to the appropriate shard for a given NexoraId.
    ///
    /// Uses double-checked locking to avoid deadlock and race conditions:
    /// 1. Try a non-blocking read first (fast path — node is in memory).
    /// 2. If that misses, acquire a write lock and check again before waking
    ///    (another task may have woken the node between steps 1 and 2).
    pub async fn route(&self, qid: &NexoraId, cmd: NodeCommand) -> Result<(), GraphError> {
        let shard_id = self.shard_of(qid);
        let shard = &self.shards[shard_id];

        // Fast path: try non-blocking read lock first.
        // `try_read()` returns immediately — no `.await` so we never yield while
        // holding a read lock, eliminating the deadlock window entirely.
        if let Ok(shard_guard) = shard.try_read() {
            if let Some(entry) = shard_guard.get_node(qid) {
                let tx = entry.tx.clone();
                drop(shard_guard); // release read lock before sending
                tx.send(cmd)
                    .await
                    .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?;
                return Ok(());
            }
            // Node not in this shard yet; drop read lock and fall through to write path.
            drop(shard_guard);
        }

        // Slow path: acquire write lock to wake the node.
        // IMPORTANT: after acquiring the write lock, check AGAIN whether the node
        // is now awake — another task may have raced ahead and created it.
        let tx = {
            let mut shard_guard = shard.write().await;
            // Double-check: did another task wake the node while we waited for the write lock?
            if let Some(entry) = shard_guard.get_node(qid) {
                entry.tx.clone()
            } else {
                shard_guard.ensure_node_awake(qid).await?;
                shard_guard
                    .get_node(qid)
                    .ok_or_else(|| GraphError::NodeUnavailable(qid.clone()))?
                    .tx
                    .clone()
            }
        };
        // Lock is released here; send happens outside the lock to avoid blocking the shard.
        tx.send(cmd)
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?;
        Ok(())
    }

    /// Set a property on a node (convenience method).
    pub async fn set_property(
        &self,
        qid: &NexoraId,
        key: &str,
        value: PropertyValue,
    ) -> Result<(), GraphError> {
        // Clone value for SQ callback before it's moved into the command
        let value_for_callback = value.clone();
        let key_for_callback = key.to_string();
        let qid_for_callback = qid.clone();

        let (tx, rx) = oneshot::channel();
        self.route(
            qid,
            NodeCommand::SetProperty {
                key: Symbol::new(key),
                value,
                reply: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // P0.4: Update the property index
        self.property_index
            .insert(key, value_for_callback.clone(), qid.clone())
            .await
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // Fire SQ callback if registered (for ingest pipeline integration)
        if let Some(cb) = &self.sq_callback {
            // Fetch ALL node properties for SQ evaluation
            let all_props = match self.get_all_properties(&qid_for_callback).await {
                Ok(props) => props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
                Err(_) => {
                    let mut props = HashMap::new();
                    props.insert(key_for_callback.clone(), value_for_callback.clone());
                    props
                }
            };
            cb(
                qid_for_callback.clone(),
                key_for_callback.clone(),
                value_for_callback.clone(),
                all_props,
            )
            .await;
        }

        // P0.5: Notify the mutation callback with a PropertySet event. The
        // mutation callback observes *all* mutation types (labels/edges already
        // fire it); a plain property set must too, so downstream consumers —
        // Standing Queries over properties, and the fragment sealing pipeline —
        // see it. Previously only the sq_callback fired here, so property writes
        // were invisible to the mutation stream.
        if let Some(cb) = &self.mutation_callback {
            cb(
                qid_for_callback,
                NodeChangeEvent::PropertySet {
                    key: Symbol::new(&key_for_callback),
                    value: value_for_callback,
                },
            )
            .await;
        }
        Ok(())
    }

    /// Remove a property from a node.
    pub async fn remove_property(&self, qid: &NexoraId, key: &str) -> Result<(), GraphError> {
        let (tx, rx) = oneshot::channel();
        self.route(
            qid,
            NodeCommand::RemoveProperty {
                key: Symbol::new(key),
                reply: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;
        Ok(())
    }

    /// Mutate a property via the commit path (WAL → memory → journal).
    /// This is the production path that guarantees durability.
    pub async fn mutate_set_property(
        &self,
        qid: &NexoraId,
        key: &str,
        value: PropertyValue,
        request_id: u64,
    ) -> Result<CommitReceipt, GraphError> {
        use crate::graph::node_task::{MutationOp, MutationRequest};

        let (tx, rx) = oneshot::channel();
        let req = MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::SetProperty {
                    key: Symbol::new(key),
                    value,
                },
                None,
            )],
            event_time: None,
            reply: tx,
            await_durable: true,
        };
        self.route(qid, NodeCommand::Mutate(req)).await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))
    }

    /// P0.4: Add a label to a node via the commit path.
    ///
    /// Uses MutationOp::AddLabel to emit a `LabelAdded` event and update
    /// the label index. Also updates the LabelIndex for O(1) lookups.
    /// No-op if the label is already present on the node.
    pub async fn add_label(
        &self,
        qid: &NexoraId,
        label: Symbol,
        request_id: u64,
    ) -> Result<CommitReceipt, GraphError> {
        use crate::graph::node_task::{MutationOp, MutationRequest};

        let (tx, rx) = oneshot::channel();
        let req = MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::AddLabel {
                    label: label.clone(),
                },
                None,
            )],
            event_time: None,
            reply: tx,
            await_durable: true,
        };
        self.route(qid, NodeCommand::Mutate(req)).await?;
        let receipt = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // Update the label index
        self.label_index
            .add_label(label.as_str(), qid.clone())
            .await;

        // P0.5: Notify mutation callback (for Standing Query re-evaluation)
        if let Some(cb) = &self.mutation_callback {
            cb(
                qid.clone(),
                NodeChangeEvent::LabelAdded {
                    label: label.clone(),
                },
            )
            .await;
        }

        Ok(receipt)
    }

    /// P0.4: Remove a label from a node via the commit path.
    ///
    /// Uses MutationOp::RemoveLabel to emit a `LabelRemoved` event and
    /// update the label index. No-op if the label is not present.
    pub async fn remove_label(
        &self,
        qid: &NexoraId,
        label: Symbol,
        request_id: u64,
    ) -> Result<CommitReceipt, GraphError> {
        use crate::graph::node_task::{MutationOp, MutationRequest};

        let (tx, rx) = oneshot::channel();
        let req = MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::RemoveLabel {
                    label: label.clone(),
                },
                None,
            )],
            event_time: None,
            reply: tx,
            await_durable: true,
        };
        self.route(qid, NodeCommand::Mutate(req)).await?;
        let receipt = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // Update the label index
        self.label_index.remove_label(label.as_str(), qid).await;

        // P0.5: Notify mutation callback
        if let Some(cb) = &self.mutation_callback {
            cb(
                qid.clone(),
                NodeChangeEvent::LabelRemoved {
                    label: label.clone(),
                },
            )
            .await;
        }

        Ok(receipt)
    }

    /// P0.5: Mark a node as soft-deleted by setting its tombstone.
    ///
    /// Uses MutationOp to emit a `NodeDeleted` event via the commit path.
    /// After this call the node is still queryable for audit but behaves as
    /// deleted for most graph operations.
    pub async fn delete_node(
        &self,
        qid: &NexoraId,
        request_id: u64,
        deleted_by: Option<&str>,
        reason: Option<&str>,
    ) -> Result<CommitReceipt, GraphError> {
        use crate::graph::node_task::{MutationOp, MutationRequest, TombstoneRecord};

        let (tx, rx) = oneshot::channel();
        let req = MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::DeleteNode {
                    tombstone: TombstoneRecord {
                        deleted_at: EventTime::now(),
                        deleted_by: deleted_by.map(|s| s.to_string()),
                        reason: reason.map(|s| s.to_string()),
                    },
                },
                None,
            )],
            event_time: None,
            reply: tx,
            await_durable: true,
        };
        self.route(qid, NodeCommand::Mutate(req)).await?;
        let receipt = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // P0.5: Notify mutation callback
        if let Some(cb) = &self.mutation_callback {
            cb(
                qid.clone(),
                NodeChangeEvent::NodeDeleted {
                    tombstone: TombstoneRecord {
                        deleted_at: EventTime::now(),
                        deleted_by: deleted_by.map(|s| s.to_string()),
                        reason: reason.map(|s| s.to_string()),
                    },
                },
            )
            .await;
        }

        // C-12 FIX: Remove all edges involving this node from edge index
        // to prevent unbounded memory growth from deleted nodes' orphaned edges
        self.edge_index.remove_node(qid).await;

        Ok(receipt)
    }

    /// Concurrent batch write: apply many per-node mutation groups in one call,
    /// fanning out across shards so the WAL group-commit flusher can amortize a
    /// single fsync over the whole in-flight batch.
    ///
    /// `items` is a list of `(node, ops)`: each entry's `ops` are committed
    /// together as one atomic per-node commit (one WAL record, one journal
    /// entry). Distinct nodes commit concurrently, bounded by
    /// `opts.concurrency`. Ordering across different nodes is not guaranteed;
    /// ops within a single entry apply in order.
    ///
    /// This is the shared write path for high-throughput producers — stream/file
    /// ingest runners and the REST/CLI bulk-write entry points — replacing a
    /// serial `for … { set_property().await }` loop, which under group commit
    /// pays one fsync per write instead of one per batch.
    ///
    /// `opts.durability` picks `ack = durable` (each commit awaits its fsync)
    /// vs `ack = buffered` (returns once buffered, flusher fsyncs async). See
    /// [`BatchDurability`].
    ///
    /// Returns the per-item [`CommitReceipt`]s in the **same order as `items`**.
    /// An empty batch is a no-op returning an empty vec. Index maintenance and
    /// mutation callbacks fire per applied op, mirroring the single-write paths.
    pub async fn write_batch(
        &self,
        items: Vec<(NexoraId, Vec<node_task::MutationOp>)>,
        opts: WriteBatchOptions,
    ) -> Result<Vec<CommitReceipt>, GraphError> {
        // Arrival-order batch: no upstream event times, so every op defers to
        // the node's internal clock (pre-LWW behavior). Event-time-aware ingest
        // uses `write_batch_with_event_times`.
        let timed = items
            .into_iter()
            .map(|(qid, ops)| {
                let timed_ops = ops.into_iter().map(|op| (op, None)).collect();
                (qid, timed_ops, None)
            })
            .collect();
        self.write_batch_with_event_times(timed, opts).await
    }

    /// Like [`write_batch`], but each item carries an optional event time used
    /// for event-time last-writer-wins. `Some(t)` makes every property write in
    /// that item's ops resolve conflicts against the stored per-property event
    /// time (out-of-order-safe); `None` falls back to the node's internal clock.
    pub async fn write_batch_with_event_times(
        &self,
        items: Vec<EventTimedWriteItem>,
        opts: WriteBatchOptions,
    ) -> Result<Vec<CommitReceipt>, GraphError> {
        use crate::graph::node_task::MutationRequest;
        use futures::stream::{FuturesOrdered, StreamExt};
        use std::sync::atomic::Ordering;

        if items.is_empty() {
            return Ok(Vec::new());
        }
        let await_durable = matches!(opts.durability, BatchDurability::WaitDurable);
        let concurrency = opts.concurrency.max(1);

        // Each item becomes a future that routes a Mutate to its node and awaits
        // the receipt. FuturesOrdered preserves input order in the results while
        // running up to `concurrency` commits in flight; buffer() caps the
        // fan-out so a large batch can't spawn unbounded work.
        let mut pending = FuturesOrdered::new();
        let mut results: Vec<CommitReceipt> = Vec::with_capacity(items.len());

        for (qid, ops, event_time) in items {
            let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            let bare_ops: Vec<_> = ops.iter().map(|(op, _)| op.clone()).collect();
            let fut = async move {
                let (tx, rx) = oneshot::channel();
                let req = MutationRequest {
                    request_id,
                    operations: ops.clone(),
                    event_time,
                    reply: tx,
                    await_durable,
                };
                self.route(&qid, NodeCommand::Mutate(req)).await?;
                let receipt = rx
                    .await
                    .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
                    .map_err(|e| GraphError::Internal(e.to_string()))?;
                // Keep secondary indexes and SQ callbacks consistent with the
                // single-write paths (add_edge/add_label/set_edge_property do
                // this inline; the batch path applies the same side effects).
                self.apply_batch_side_effects(&qid, &bare_ops).await;
                Ok::<CommitReceipt, GraphError>(receipt)
            };
            pending.push_back(fut);

            // Drain to keep at most `concurrency` futures in flight.
            while pending.len() >= concurrency {
                if let Some(res) = pending.next().await {
                    results.push(res?);
                }
            }
        }
        while let Some(res) = pending.next().await {
            results.push(res?);
        }
        Ok(results)
    }

    /// Apply the index + mutation-callback side effects for a committed batch
    /// item, mirroring what the single-write convenience methods do inline.
    async fn apply_batch_side_effects(&self, qid: &NexoraId, ops: &[node_task::MutationOp]) {
        use crate::graph::node_task::MutationOp;
        for op in ops {
            match op {
                MutationOp::AddEdge { edge } => {
                    self.edge_index.add_halfedge(qid, edge).await;
                    if let Some(cb) = &self.mutation_callback {
                        cb(
                            qid.clone(),
                            NodeChangeEvent::EdgeAdded { edge: edge.clone() },
                        )
                        .await;
                    }
                }
                MutationOp::RemoveEdge { edge } => {
                    self.edge_index
                        .remove_edge(edge.edge_type.as_str(), qid, &edge.other)
                        .await;
                    if let Some(cb) = &self.mutation_callback {
                        cb(
                            qid.clone(),
                            NodeChangeEvent::EdgeRemoved { edge: edge.clone() },
                        )
                        .await;
                    }
                }
                MutationOp::AddLabel { label } => {
                    self.label_index
                        .add_label(label.as_str(), qid.clone())
                        .await;
                    if let Some(cb) = &self.mutation_callback {
                        cb(
                            qid.clone(),
                            NodeChangeEvent::LabelAdded {
                                label: label.clone(),
                            },
                        )
                        .await;
                    }
                }
                MutationOp::RemoveLabel { label } => {
                    self.label_index.remove_label(label.as_str(), qid).await;
                    if let Some(cb) = &self.mutation_callback {
                        cb(
                            qid.clone(),
                            NodeChangeEvent::LabelRemoved {
                                label: label.clone(),
                            },
                        )
                        .await;
                    }
                }
                MutationOp::SetProperty { key, value } => {
                    // Mirror the single-write path (set_property): a batched
                    // property write must maintain the SAME side effects, or
                    // ingest via write_batch (bulk_ingest / stream sources) would
                    // silently skip them while single writes don't.
                    //
                    // 1) Property index — otherwise indexed lookups
                    //    (MATCH (n {k: v})) miss batch-written nodes.
                    let _ = self
                        .property_index
                        .insert(key.as_str(), value.clone(), qid.clone())
                        .await;

                    // 2) sq_callback — the property-Standing-Query trigger. The
                    //    single-write path fires this; the batch path previously
                    //    fired only mutation_callback, whose PropertySet arm is a
                    //    no-op, so property SQs never matched on batched ingest.
                    //    Fetch the full property set so the SQ sees the node's
                    //    complete state (patterns may reference other keys).
                    if let Some(cb) = &self.sq_callback {
                        let all_props = match self.get_all_properties(qid).await {
                            Ok(props) => {
                                props.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
                            }
                            Err(_) => {
                                let mut props = HashMap::new();
                                props.insert(key.as_str().to_string(), value.clone());
                                props
                            }
                        };
                        cb(
                            qid.clone(),
                            key.as_str().to_string(),
                            value.clone(),
                            all_props,
                        )
                        .await;
                    }

                    // 3) mutation_callback — sealing pipeline + non-property SQ
                    //    observers. Kept for parity with the single-write path.
                    if let Some(cb) = &self.mutation_callback {
                        cb(
                            qid.clone(),
                            NodeChangeEvent::PropertySet {
                                key: key.clone(),
                                value: value.clone(),
                            },
                        )
                        .await;
                    }
                }
                // Remaining ops (RemoveProperty/EdgePropertySet/DeleteNode) have
                // no secondary index to maintain here; their events are already
                // journaled by the commit. Callback wiring for them can be added
                // if a consumer needs it.
                _ => {}
            }
        }
    }

    /// P0.4: Query the property index for nodes matching a property value.
    /// Used by the Cypher executor for indexed property lookups like
    /// MATCH (n {id: 'X'}) without scanning all properties.
    pub async fn query_property_index(
        &self,
        property: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NexoraId>, GraphError> {
        self.property_index
            .query(property, value)
            .await
            .map_err(|e| GraphError::Internal(e.to_string()))
    }

    /// P0.4: Query the edge index for outgoing neighbors by edge type.
    /// Used by the Cypher executor for indexed edge traversal like
    /// MATCH (a)-[:EXECUTING]->(t) without scanning all edges.
    pub async fn query_edge_index_outgoing(&self, edge_type: &str) -> Vec<(NexoraId, NexoraId)> {
        self.edge_index.query_outgoing(edge_type).await
    }

    /// P0.4: Query the edge index for incoming neighbors by edge type.
    pub async fn query_edge_index_incoming(&self, edge_type: &str) -> Vec<(NexoraId, NexoraId)> {
        self.edge_index.query_incoming(edge_type).await
    }

    /// P0.1: Get a node's labels via a snapshot.
    ///
    /// Returns the first-class labels field from NodeTask.
    pub async fn get_labels(&self, qid: &NexoraId) -> Result<HashSet<Symbol>, GraphError> {
        // #1: projection fast path — resident node, no mailbox round-trip.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state.labels.clone());
        }
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::SnapshotState { reply: tx })
            .await?;
        let snapshot = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?;
        Ok(snapshot.labels)
    }

    /// GAP-1: Get a node's edge properties via a snapshot.
    ///
    /// Returns the `edge_properties` map keyed by `(edge_type, target)`.
    /// Exposed so callers (and crash-recovery tests) can observe edge data.
    pub async fn get_edge_properties(
        &self,
        qid: &NexoraId,
    ) -> Result<HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>, GraphError> {
        // #1: projection fast path — resident node, no mailbox round-trip.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state.edge_properties.clone());
        }
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::SnapshotState { reply: tx })
            .await?;
        let snapshot = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?;
        Ok(snapshot.edge_properties)
    }

    /// GAP-1: Get a node's tombstone (soft-delete state) via a snapshot.
    ///
    /// Returns `Some` if the node is soft-deleted, `None` otherwise. Exposed so
    /// callers can query delete state and crash-recovery tests can assert it.
    pub async fn get_tombstone(
        &self,
        qid: &NexoraId,
    ) -> Result<Option<TombstoneRecord>, GraphError> {
        // #1: projection fast path — resident node, no mailbox round-trip.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state.tombstone.clone());
        }
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::SnapshotState { reply: tx })
            .await?;
        let snapshot = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?;
        Ok(snapshot.tombstone)
    }

    /// P0.1: Set an edge property via the commit path.
    ///
    /// Specifies an edge by (src, edge_type, dst), and sets a property on it
    /// via MutationOp::EdgePropertySet event.
    pub async fn set_edge_property(
        &self,
        src: &NexoraId,
        edge_type: Symbol,
        dst: &NexoraId,
        key: Symbol,
        value: PropertyValue,
        request_id: u64,
    ) -> Result<CommitReceipt, GraphError> {
        use crate::graph::node_task::{MutationOp, MutationRequest};

        let (tx, rx) = oneshot::channel();
        let req = MutationRequest {
            request_id,
            operations: vec![(
                MutationOp::EdgePropertySet {
                    edge_type,
                    dst: dst.clone(),
                    key,
                    value,
                },
                None,
            )],
            event_time: None,
            reply: tx,
            await_durable: true,
        };
        self.route(src, NodeCommand::Mutate(req)).await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(src.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))
    }

    /// Get a property from a node (convenience method).
    pub async fn get_property(
        &self,
        qid: &NexoraId,
        key: &str,
    ) -> Result<Option<PropertyValue>, GraphError> {
        // #1: projection fast path — resident node, no mailbox round-trip.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state.properties.get(&Symbol::new(key)).cloned());
        }
        let (tx, rx) = oneshot::channel();
        self.route(
            qid,
            NodeCommand::GetProperty {
                key: Symbol::new(key),
                reply: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))
    }

    /// Add an edge (convenience method).
    pub async fn add_edge(
        &self,
        qid: &NexoraId,
        edge: nexora_value::HalfEdge,
    ) -> Result<(), GraphError> {
        let edge_clone = edge.clone();
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::AddEdge { edge, reply: tx })
            .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // P0.4: Update edge index
        self.edge_index.add_halfedge(qid, &edge_clone).await;

        // P0.5: Notify mutation callback
        if let Some(cb) = &self.mutation_callback {
            cb(qid.clone(), NodeChangeEvent::EdgeAdded { edge: edge_clone }).await;
        }

        Ok(())
    }

    /// Remove an edge (convenience method).
    pub async fn remove_edge(
        &self,
        qid: &NexoraId,
        edge: nexora_value::HalfEdge,
    ) -> Result<(), GraphError> {
        let edge_clone = edge.clone();
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::RemoveEdge { edge, reply: tx })
            .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;

        // P0.5: Notify mutation callback
        if let Some(cb) = &self.mutation_callback {
            cb(
                qid.clone(),
                NodeChangeEvent::EdgeRemoved {
                    edge: edge_clone.clone(),
                },
            )
            .await;
        }

        // P0.4: Remove from edge index
        self.edge_index
            .remove_edge(edge_clone.edge_type.as_str(), qid, &edge_clone.other)
            .await;

        Ok(())
    }

    /// Get all edges of a node (convenience method).
    pub async fn get_edges(
        &self,
        qid: &NexoraId,
    ) -> Result<Vec<nexora_value::HalfEdge>, GraphError> {
        // #1: projection fast path — resident node, no mailbox round-trip.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state.edges.iter().cloned().collect());
        }
        let (tx, rx) = oneshot::channel();
        self.route(
            qid,
            NodeCommand::GetEdges {
                edge_type: None,
                reply: tx,
            },
        )
        .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))
    }

    /// D2: outgoing neighbors of `qid` along a single edge type.
    ///
    /// A typed-traversal fast path for BFS/multi-hop queries. Unlike
    /// [`Self::get_edges`] (which clones every `HalfEdge` of the node and leaves
    /// the caller to filter), this filters by `edge_type` + outgoing direction
    /// and returns only the target ids — no full-edge-set clone per hop, which
    /// is the dominant cost in a wide traversal. For a resident node it reads
    /// the lock-free projection directly; for a cold node it uses the node
    /// task's server-side `edge_type` filter (only matching edges cross the
    /// mailbox), then wakes it.
    pub async fn outgoing_neighbors(
        &self,
        qid: &NexoraId,
        edge_type: &str,
    ) -> Result<Vec<NexoraId>, GraphError> {
        // Projection fast path — resident node, no mailbox round-trip, and we
        // collect only target ids rather than cloning whole HalfEdges.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state
                .edges
                .iter()
                .filter(|e| e.direction.is_out() && e.edge_type.as_str() == edge_type)
                .map(|e| e.other.clone())
                .collect());
        }
        // Cold node: server-side type filter keeps the mailbox payload minimal.
        let (tx, rx) = oneshot::channel();
        self.route(
            qid,
            NodeCommand::GetEdges {
                edge_type: Some(nexora_value::Symbol::new(edge_type)),
                reply: tx,
            },
        )
        .await?;
        let edges = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))?;
        Ok(edges
            .into_iter()
            .filter(|e| e.direction.is_out())
            .map(|e| e.other)
            .collect())
    }

    /// Get all properties of a node (convenience method).
    pub async fn get_all_properties(
        &self,
        qid: &NexoraId,
    ) -> Result<BTreeMap<nexora_value::Symbol, PropertyValue>, GraphError> {
        // #1: projection fast path — resident node, no mailbox round-trip.
        if let Some(state) = self.read_projection(qid) {
            return Ok(state.properties.clone());
        }
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::GetAllProperties { reply: tx })
            .await?;
        rx.await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?
            .map_err(|e| GraphError::Internal(e.to_string()))
    }

    /// Evict (sleep) every node idle for longer than `ttl`, across all shards,
    /// releasing hot memory. Returns `(evicted, retained)` totals.
    ///
    /// This is the time-based counterpart to the capacity-based LRU eviction
    /// (`enforce_memory_limit`): a background sweep can call it periodically so a
    /// large but mostly-cold graph doesn't pin memory for nodes no one touches.
    /// `sleep_node` persists full state, so an evicted node wakes identically on
    /// its next access — eviction reclaims memory, it never drops data.
    pub async fn evict_idle_nodes(&self, ttl: std::time::Duration) -> (usize, usize) {
        let mut evicted = 0;
        let mut retained = 0;
        for shard in &self.shards {
            let (e, r) = shard.write().await.evict_idle_nodes(ttl).await;
            evicted += e;
            retained += r;
        }
        (evicted, retained)
    }

    /// Put a node to sleep (persist state, release memory).
    pub async fn sleep_node(&self, qid: &NexoraId) -> Result<(), GraphError> {
        let shard_id = self.shard_of(qid);
        let mut shard = self.shards[shard_id].write().await;
        shard.sleep_node(qid).await.map_err(GraphError::from)
    }

    /// The shard index for a given NexoraId.
    pub fn shard_of(&self, qid: &NexoraId) -> usize {
        (qid.shard_key() as usize) % self.config.num_shards
    }

    /// #1: Try to read a node's published snapshot from its shard projection,
    /// bypassing the node-task mailbox. Returns `None` when the node is not
    /// resident (never woken, or slept/evicted) or the shard lock is momentarily
    /// contended — callers then fall back to the mailbox slow path.
    ///
    /// Uses `try_read()` so the read never blocks behind a shard write lock (the
    /// same non-blocking discipline as `route`'s fast path); a miss is cheap and
    /// simply routes through the actor instead.
    fn read_projection(&self, qid: &NexoraId) -> Option<Arc<projection::NodeReadState>> {
        let shard = &self.shards[self.shard_of(qid)];
        let guard = shard.try_read().ok()?;
        guard.read_projection(qid)
    }

    /// #1b: Read a node's full read-visible state in one call.
    ///
    /// Projection fast path for resident nodes — a single lock-free snapshot
    /// clone, no mailbox round-trip. Falls back to the `SnapshotState` mailbox
    /// command (which also wakes the node from persistence) for cold nodes.
    ///
    /// This is the bulk-read primitive the Cypher executor uses to build query
    /// snapshots: one call per node instead of three separate getters
    /// (`get_all_properties` + `get_labels` + `get_edges`).
    pub async fn read_node_state(
        &self,
        qid: &NexoraId,
    ) -> Result<Arc<projection::NodeReadState>, GraphError> {
        if let Some(state) = self.read_projection(qid) {
            return Ok(state);
        }
        // Cold node: route a snapshot request (wakes it) and adapt the reply.
        let (tx, rx) = oneshot::channel();
        self.route(qid, NodeCommand::SnapshotState { reply: tx })
            .await?;
        let snap = rx
            .await
            .map_err(|_| GraphError::NodeUnavailable(qid.clone()))?;
        Ok(Arc::new(projection::NodeReadState {
            labels: snap.labels,
            properties: snap.properties,
            property_times: snap.property_times,
            edges: snap.edges,
            edge_properties: snap.edge_properties,
            tombstone: snap.tombstone,
        }))
    }

    /// Total number of active (in-memory) nodes across all shards.
    pub async fn active_node_count(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.read().await.active_node_count();
        }
        total
    }

    /// Return every known node ID, including active nodes and nodes represented
    /// only by persisted journals or snapshots.
    pub async fn all_node_ids(&self) -> Result<Vec<NexoraId>, GraphError> {
        let mut ids = HashSet::new();
        for shard in &self.shards {
            ids.extend(shard.read().await.active_node_ids());
        }
        ids.extend(
            self.persistor
                .enumerate_journal_node_ids()
                .await
                .map_err(|e| GraphError::Persistence(e.to_string()))?,
        );
        ids.extend(
            self.persistor
                .enumerate_snapshot_node_ids()
                .await
                .map_err(|e| GraphError::Persistence(e.to_string()))?,
        );
        Ok(ids.into_iter().collect())
    }

    /// Persist and stop every active node, then flush the persistence backend.
    pub async fn shutdown(&self) -> Result<(), GraphError> {
        for shard in &self.shards {
            let mut guard = shard.write().await;
            guard.sleep_all_nodes().await?;
            // Drain the group-commit flusher after sleeping nodes so the final
            // fsync covers every buffered record (including snapshot
            // checkpoints), then stop the background task.
            guard.shutdown_flusher().await;
        }
        self.persistor
            .shutdown()
            .await
            .map_err(|e| GraphError::Persistence(e.to_string()))
    }

    /// Register a callback that fires on every property change.
    /// Used to trigger Standing Query evaluation from any mutation path.
    pub fn with_sq_callback(mut self, cb: PropertyChangeCallback) -> Self {
        self.sq_callback = Some(cb);
        self
    }

    /// P0.5: Register a callback that fires on every graph mutation
    /// (labels, edges, properties, tombstone). Used to trigger Standing
    /// Query re-evaluation for events beyond just property changes.
    pub fn with_mutation_callback(mut self, cb: GraphMutationCallback) -> Self {
        self.mutation_callback = Some(cb);
        self
    }

    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.config.num_shards
    }

    /// Access the persistence backend.
    pub fn persistor(&self) -> &dyn NamespacedPersistenceAgent {
        self.persistor.as_ref()
    }

    /// Clone a shared handle to the persistence backend.
    ///
    /// Unlike [`Self::persistor`], which borrows, this returns an owned `Arc`
    /// so other subsystems (e.g. the Standing Query manager's definition
    /// persistence) can hold the same backend for write-through and restore.
    pub fn persistor_arc(&self) -> Arc<dyn NamespacedPersistenceAgent> {
        self.persistor.clone()
    }

    /// Flush all active nodes to persistence (for graceful shutdown).
    /// Iterates all shards and sleeps each active node, persisting its journal and snapshot.
    pub async fn flush_all_nodes(&self) -> Result<usize, GraphError> {
        // B6: flush shards CONCURRENTLY. Each shard sits behind its own RwLock,
        // so flushing them in parallel is safe and turns a serial 256-shard
        // sweep (which at scale meant millions of nodes slept one shard at a
        // time — minutes of wall-clock) into a bounded fan-out. Concurrency is
        // capped so a huge shard count can't spawn unbounded lock-holding tasks;
        // the whole set is still covered across batches.
        use futures::stream::StreamExt;
        const FLUSH_CONCURRENCY: usize = 16;

        let flushed: usize = futures::stream::iter(0..self.shards.len())
            .map(|shard_id| async move {
                let mut shard = self.shards[shard_id].write().await;
                let active_nodes: Vec<NexoraId> = shard.active_node_ids();
                let mut n = 0;
                for qid in active_nodes {
                    if let Err(e) = shard.sleep_node(&qid).await {
                        tracing::warn!("Failed to flush node {}: {}", qid, e);
                    } else {
                        n += 1;
                    }
                }
                n
            })
            .buffer_unordered(FLUSH_CONCURRENCY)
            .fold(0usize, |acc, n| async move { acc + n })
            .await;

        tracing::info!("Flushed {} active nodes to persistence", flushed);
        Ok(flushed)
    }

    /// Flush (persist + sleep) every active node in a single shard, returning
    /// the number of nodes flushed.
    ///
    /// This is the per-shard counterpart to [`Self::flush_all_nodes`], used by
    /// offset-aligned checkpoints (B2/F1.2) to drive a barrier per shard and
    /// report the real flushed node count for each. Semantics match
    /// `flush_all_nodes` restricted to one shard: each resident node's journal
    /// and snapshot are persisted, then the node is slept (memory released), so
    /// it wakes identically on next access. Non-destructive.
    ///
    /// `shard_id` must be `< shard_count()`; out-of-range returns
    /// [`GraphError::InvalidArgument`].
    pub async fn flush_shard(&self, shard_id: usize) -> Result<usize, GraphError> {
        if shard_id >= self.shards.len() {
            return Err(GraphError::ShardNotFound(shard_id));
        }
        let mut flushed = 0;
        let mut shard = self.shards[shard_id].write().await;
        let active_nodes: Vec<NexoraId> = shard.active_node_ids();
        for qid in active_nodes {
            if let Err(e) = shard.sleep_node(&qid).await {
                tracing::warn!("Failed to flush node {} on shard {}: {}", qid, shard_id, e);
            } else {
                flushed += 1;
            }
        }
        tracing::debug!("Flushed {} active nodes on shard {}", flushed, shard_id);
        Ok(flushed)
    }
}

// ============================================================
// Trait Implementations for GraphService
// ============================================================

#[async_trait::async_trait]
impl LiteralOpsGraph for GraphService {
    async fn set_property(
        &self,
        qid: &NexoraId,
        key: &str,
        value: PropertyValue,
    ) -> Result<(), GraphError> {
        GraphService::set_property(self, qid, key, value).await
    }
    async fn get_property(
        &self,
        qid: &NexoraId,
        key: &str,
    ) -> Result<Option<PropertyValue>, GraphError> {
        GraphService::get_property(self, qid, key).await
    }
    async fn add_edge(
        &self,
        qid: &NexoraId,
        edge: nexora_value::HalfEdge,
    ) -> Result<(), GraphError> {
        GraphService::add_edge(self, qid, edge).await
    }
    async fn get_edges(&self, qid: &NexoraId) -> Result<Vec<nexora_value::HalfEdge>, GraphError> {
        GraphService::get_edges(self, qid).await
    }
    async fn sleep_node(&self, qid: &NexoraId) -> Result<(), GraphError> {
        GraphService::sleep_node(self, qid).await
    }
}

#[async_trait::async_trait]
impl StandingQueryOpsGraph for GraphService {
    async fn register_standing_query(&self, name: &str, query: &str) -> Result<String, GraphError> {
        let id = uuid::Uuid::new_v4().to_string();
        let mut queries = self.standing_queries.write().await;
        queries.insert(id.clone(), (name.to_string(), query.to_string()));
        tracing::info!(id = %id, name = name, "Registered standing query");
        Ok(id)
    }

    async fn remove_standing_query(&self, sq_id: &str) -> Result<(), GraphError> {
        let mut queries = self.standing_queries.write().await;
        if queries.remove(sq_id).is_some() {
            tracing::info!(id = sq_id, "Removed standing query");
            Ok(())
        } else {
            Err(GraphError::Internal(format!(
                "Standing query not found: {}",
                sq_id
            )))
        }
    }
}

/// Get registered standing queries from a graph service.
/// Separate trait to avoid coupling with full SQ manager.
#[async_trait::async_trait]
pub trait StandingQueryRegistry: Send + Sync {
    async fn list_standing_queries(&self) -> Vec<(String, String, String)>; // (id, name, pattern)
    async fn get_standing_query(&self, sq_id: &str) -> Option<(String, String)>; // (name, pattern)
}

#[async_trait::async_trait]
impl StandingQueryRegistry for GraphService {
    async fn list_standing_queries(&self) -> Vec<(String, String, String)> {
        let queries = self.standing_queries.read().await;
        queries
            .iter()
            .map(|(id, (name, pattern))| (id.clone(), name.clone(), pattern.clone()))
            .collect()
    }

    async fn get_standing_query(&self, sq_id: &str) -> Option<(String, String)> {
        let queries = self.standing_queries.read().await;
        queries.get(sq_id).cloned()
    }
}

#[async_trait::async_trait]
impl CypherOpsGraph for GraphService {}

impl FullGraphService for GraphService {
    fn shard_count(&self) -> usize {
        self.config.num_shards
    }
    fn persistor(&self) -> &dyn NamespacedPersistenceAgent {
        self.persistor.as_ref()
    }
}

/// Errors that can occur in graph operations.
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("node not found: {0}")]
    NodeNotFound(NexoraId),
    #[error("node unavailable (task exited): {0}")]
    NodeUnavailable(NexoraId),
    #[error("shard not found: {0}")]
    ShardNotFound(usize),
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error("operation timed out")]
    Timeout,
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<ShardError> for GraphError {
    fn from(e: ShardError) -> Self {
        match e {
            ShardError::NodeNotFound(qid) => GraphError::NodeNotFound(qid),
            ShardError::NodeUnavailable(qid) => GraphError::NodeUnavailable(qid),
            ShardError::Persistence(msg) => GraphError::Persistence(msg),
            ShardError::Internal(msg) => GraphError::Internal(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryPersistor;
    use nexora_value::HalfEdge;
    use std::time::Duration;

    fn test_service() -> GraphService {
        let config = GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 16,
        };
        let persistor = Arc::new(InMemoryPersistor::new());
        GraphService::new(config, persistor)
    }

    #[tokio::test]
    async fn test_set_and_get_property() {
        let svc = test_service();
        let qid = NexoraId::new_random();

        svc.set_property(&qid, "speed", PropertyValue::Float(12.5))
            .await
            .unwrap();

        let val = svc.get_property(&qid, "speed").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Float(12.5)));
    }

    #[tokio::test]
    async fn test_add_and_get_edges() {
        let svc = test_service();
        let qid = NexoraId::new_random();
        let target = NexoraId::new_random();

        let edge = HalfEdge::out(Symbol::new("KNOWS"), target);
        svc.add_edge(&qid, edge.clone()).await.unwrap();

        let edges = svc.get_edges(&qid).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], edge);
    }

    /// D2: typed neighbor accessor returns only matching outgoing targets,
    /// on both the resident (projection) and cold (mailbox filter) paths.
    #[tokio::test]
    async fn test_outgoing_neighbors_typed_traversal() {
        let svc = test_service();
        let a = NexoraId::new_random();
        let knows1 = NexoraId::new_random();
        let knows2 = NexoraId::new_random();
        let follows = NexoraId::new_random();

        svc.add_edge(&a, HalfEdge::out(Symbol::new("KNOWS"), knows1.clone()))
            .await
            .unwrap();
        svc.add_edge(&a, HalfEdge::out(Symbol::new("KNOWS"), knows2.clone()))
            .await
            .unwrap();
        svc.add_edge(&a, HalfEdge::out(Symbol::new("FOLLOWS"), follows.clone()))
            .await
            .unwrap();

        // Compare as hex-id sets (NexoraId isn't Ord, and order is unspecified).
        let hexset = |ids: Vec<NexoraId>| -> std::collections::HashSet<String> {
            ids.iter().map(|q| q.to_hex()).collect()
        };
        let expected: std::collections::HashSet<String> =
            [&knows1, &knows2].iter().map(|q| q.to_hex()).collect();

        // Resident path: only the two KNOWS targets, FOLLOWS excluded.
        assert_eq!(
            hexset(svc.outgoing_neighbors(&a, "KNOWS").await.unwrap()),
            expected,
            "resident: only KNOWS targets"
        );
        assert_eq!(
            hexset(svc.outgoing_neighbors(&a, "FOLLOWS").await.unwrap()),
            [&follows].iter().map(|q| q.to_hex()).collect(),
            "resident: only FOLLOWS target"
        );
        assert!(
            svc.outgoing_neighbors(&a, "MISSING")
                .await
                .unwrap()
                .is_empty(),
            "resident: unknown edge type → empty"
        );

        // Cold path: sleep the node, then the same query must go through the
        // mailbox server-side filter and return identical results.
        svc.sleep_node(&a).await.unwrap();
        assert_eq!(
            hexset(svc.outgoing_neighbors(&a, "KNOWS").await.unwrap()),
            expected,
            "cold: same KNOWS targets after wake"
        );
    }

    #[tokio::test]
    async fn test_sleep_and_wake_preserves_state() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let config = GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 16,
        };
        let qid = NexoraId::new_random();
        let target = NexoraId::new_random();

        // Phase 1: Write data, then sleep
        {
            let svc = GraphService::new(config.clone(), persistor.clone());
            svc.set_property(&qid, "name", PropertyValue::String("Forklift-042".into()))
                .await
                .unwrap();
            svc.set_property(&qid, "speed", PropertyValue::Float(15.0))
                .await
                .unwrap();
            svc.add_edge(&qid, HalfEdge::out(Symbol::new("IN_ZONE"), target.clone()))
                .await
                .unwrap();

            // Sleep the node (persists to InMemoryPersistor)
            svc.sleep_node(&qid).await.unwrap();
            assert_eq!(svc.active_node_count().await, 0);
        }
        // Service dropped, but persistor survives (Arc)

        // Phase 2: New service, same persistor — wake should restore state
        {
            let svc = GraphService::new(config, persistor);

            // Access the node — should auto-wake from persistence
            let name = svc.get_property(&qid, "name").await.unwrap();
            assert_eq!(name, Some(PropertyValue::String("Forklift-042".into())));

            let speed = svc.get_property(&qid, "speed").await.unwrap();
            assert_eq!(speed, Some(PropertyValue::Float(15.0)));

            let edges = svc.get_edges(&qid).await.unwrap();
            assert_eq!(edges.len(), 1);
            assert_eq!(edges[0].edge_type.as_str(), "IN_ZONE");
            assert_eq!(edges[0].other, target);
        }
    }

    #[tokio::test]
    async fn test_multiple_properties() {
        let svc = test_service();
        let qid = NexoraId::new_random();

        svc.set_property(&qid, "a", PropertyValue::Integer(1))
            .await
            .unwrap();
        svc.set_property(&qid, "b", PropertyValue::Integer(2))
            .await
            .unwrap();
        svc.set_property(&qid, "c", PropertyValue::Integer(3))
            .await
            .unwrap();

        assert_eq!(
            svc.get_property(&qid, "a").await.unwrap(),
            Some(PropertyValue::Integer(1))
        );
        assert_eq!(
            svc.get_property(&qid, "b").await.unwrap(),
            Some(PropertyValue::Integer(2))
        );
        assert_eq!(
            svc.get_property(&qid, "c").await.unwrap(),
            Some(PropertyValue::Integer(3))
        );
    }

    #[tokio::test]
    async fn test_shard_distribution() {
        let svc = test_service();
        // Different NexoraIds should land on different shards
        let qid1 = NexoraId::from_bytes(vec![0, 0, 0, 1]);
        let qid2 = NexoraId::from_bytes(vec![0, 0, 0, 2]);
        let qid3 = NexoraId::from_bytes(vec![0, 0, 0, 3]);
        let qid4 = NexoraId::from_bytes(vec![0, 0, 0, 4]);

        // At least some should be on different shards (with 4 shards)
        let s1 = svc.shard_of(&qid1);
        let s2 = svc.shard_of(&qid2);
        let s3 = svc.shard_of(&qid3);
        let s4 = svc.shard_of(&qid4);

        // Not all on the same shard (probabilistically guaranteed with 4 shards)
        let all_same = s1 == s2 && s2 == s3 && s3 == s4;
        assert!(
            !all_same,
            "Expected different shards: {s1}, {s2}, {s3}, {s4}"
        );
    }

    #[tokio::test]
    async fn test_property_overwrite() {
        let svc = test_service();
        let qid = NexoraId::new_random();

        svc.set_property(&qid, "x", PropertyValue::Integer(1))
            .await
            .unwrap();
        svc.set_property(&qid, "x", PropertyValue::Integer(42))
            .await
            .unwrap();

        let val = svc.get_property(&qid, "x").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(42)));
    }

    #[tokio::test]
    async fn test_evict_idle_nodes_end_to_end() {
        let svc = test_service();
        let qid = NexoraId::new_random();

        // Write a property so the node is resident.
        svc.set_property(&qid, "name", PropertyValue::String("evict-me".into()))
            .await
            .unwrap();

        // TTL far in the future → nothing evicted.
        let (evicted, retained) = svc.evict_idle_nodes(Duration::from_secs(3600)).await;
        assert_eq!((evicted, retained), (0, 1));

        // Age past a tiny TTL, then sweep → node is evicted.
        tokio::time::sleep(Duration::from_millis(15)).await;
        let (evicted, retained) = svc.evict_idle_nodes(Duration::from_millis(10)).await;
        assert_eq!((evicted, retained), (1, 0));

        // Reading wakes it with the same state (sleep persisted full state).
        let val = svc.get_property(&qid, "name").await.unwrap();
        assert_eq!(val, Some(PropertyValue::String("evict-me".into())));
    }

    #[tokio::test]
    async fn test_node_not_found() {
        let svc = test_service();
        let qid = NexoraId::from_bytes(vec![99, 99, 99]);

        // Should create node on first access (auto-wake with empty state)
        let val = svc.get_property(&qid, "nonexistent").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn write_batch_fires_sq_callback_for_properties() {
        // Regression: batched property writes (bulk_ingest / stream sources) must
        // fire the sq_callback exactly like the single-write path, or property
        // Standing Queries never match on batched ingest. Before the fix,
        // apply_batch_side_effects only called mutation_callback (whose PropertySet
        // arm is a no-op for property SQs), so this callback never fired.
        use std::sync::{Arc as StdArc, Mutex};

        let config = GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 16,
        };
        let persistor = Arc::new(InMemoryPersistor::new());

        // Record every (qid, key, value, all_props) the sq_callback observes.
        #[allow(clippy::type_complexity)]
        let seen: StdArc<Mutex<Vec<(NexoraId, String, PropertyValue, usize)>>> =
            StdArc::new(Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let cb: PropertyChangeCallback = Arc::new(move |qid, key, value, all_props| {
            let seen = seen_cb.clone();
            Box::pin(async move {
                seen.lock()
                    .unwrap()
                    .push((qid, key, value, all_props.len()));
            })
        });

        let svc = GraphService::new(config, persistor).with_sq_callback(cb);

        // Batch-write two properties on the same node.
        let qid = NexoraId::new_random();
        svc.write_batch(
            vec![(
                qid.clone(),
                vec![
                    node_task::MutationOp::SetProperty {
                        key: nexora_value::Symbol::new("speed"),
                        value: PropertyValue::Float(120.0),
                    },
                    node_task::MutationOp::SetProperty {
                        key: nexora_value::Symbol::new("status"),
                        value: PropertyValue::String("active".into()),
                    },
                ],
            )],
            WriteBatchOptions::default(),
        )
        .await
        .unwrap();

        let calls = seen.lock().unwrap().clone();
        assert_eq!(
            calls.len(),
            2,
            "sq_callback must fire once per batched property write, got {}",
            calls.len()
        );
        // Both writes are for the same node; the fix fetches the FULL property
        // set, so the second call must see both keys (complete node state).
        let last = calls.last().unwrap();
        assert!(
            last.3 >= 2,
            "sq_callback must see the node's full property set (>=2 keys), got {}",
            last.3
        );

        // And the batch-written properties must be indexed (property_index side
        // effect), so an indexed lookup finds the node.
        let hits = svc
            .query_property_index("speed", &PropertyValue::Float(120.0))
            .await
            .unwrap();
        assert!(
            hits.contains(&qid),
            "batch-written property must be in the property index"
        );
    }
}
