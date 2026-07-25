//! GraphShard — manages a collection of node tasks with lifecycle control.
//!
//! Each shard owns a subset of graph nodes (determined by consistent hashing).
//! The shard handles:
//! - Creating node tasks on demand (lazy initialization)
//! - Waking sleeping nodes from persistence
//! - Putting inactive nodes to sleep (with snapshot)
//! - Memory pressure management (evict LRU nodes when limit reached)
//! - Routing commands to the correct node task

use crate::event::{NodeChangeEvent, TimedEvent};
use crate::graph::node_task::{NodeCommand, NodeTask, NodeTaskHandle, SharedWal, TombstoneRecord};
use crate::graph::projection::{new_shard_projection, NodeReadState, ShardProjection};
use crate::persistor::NamespacedPersistenceAgent;
use crate::snapshot_manifest::{ChecksumKind, SnapshotKind, SnapshotManifest};
use crate::wal::{spawn_group_flusher, FlusherHandle, WalOperation, WalSyncPolicy, WriteAheadLog};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

/// Default group-commit batch cap: force an inline fsync once this many records
/// have buffered without a flush. Bounds buffer growth and tail latency while
/// still letting one fsync amortize a full batch of concurrent writes.
const GROUP_COMMIT_MAX_OPS: usize = 256;

/// Default group-commit delay bound: the flusher syncs a non-empty buffer at
/// least this often even under light load, capping single-write latency.
const GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(500);

/// The production WAL sync policy for graph shards. Group commit preserves the
/// "ack implies durable" contract (committers await the durable watermark)
/// while amortizing fsync cost across concurrent writes.
fn shard_wal_policy() -> WalSyncPolicy {
    WalSyncPolicy::Group {
        max_ops: GROUP_COMMIT_MAX_OPS,
        max_delay: GROUP_COMMIT_MAX_DELAY,
        max_bytes: None,
    }
}

/// A shard — manages a collection of node tasks.
pub struct GraphShard {
    /// Shard identifier (index in the shard array).
    pub id: usize,
    /// Active node tasks: NexoraId → command sender.
    nodes: HashMap<NexoraId, NodeEntry>,
    /// Maximum nodes before LRU eviction kicks in.
    max_nodes: usize,
    /// Channel buffer size for each node task.
    channel_size: usize,
    /// Persistence backend for loading/saving node state.
    persistor: Arc<dyn NamespacedPersistenceAgent>,
    /// Write-Ahead Log for crash recovery.
    /// Each shard has its own WAL file.
    wal: Option<SharedWal>,
    /// Background group-commit flusher handle. Present when the WAL runs under
    /// `WalSyncPolicy::Group`; drained on shutdown so no acked write is lost.
    flusher: Option<FlusherHandle>,
    /// #1: Shared read-only projection for this shard. Node tasks publish their
    /// state here on each commit; reads consult it directly instead of a
    /// mailbox round-trip. Entries are removed when a node is slept/evicted, so
    /// it only ever holds resident nodes.
    projection: ShardProjection,
}

pub(crate) struct NodeEntry {
    pub(crate) tx: mpsc::Sender<NodeCommand>,
    /// FIX P1-1: JoinHandle for detecting task panic.
    pub(crate) handle: JoinHandle<()>,
    pub(crate) last_access: Instant,
}

impl GraphShard {
    pub fn new(
        id: usize,
        max_nodes: usize,
        channel_size: usize,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
    ) -> Self {
        Self {
            id,
            nodes: HashMap::new(),
            max_nodes,
            channel_size,
            persistor,
            wal: None,
            flusher: None,
            projection: new_shard_projection(),
        }
    }

    /// Create a shard with WAL enabled for crash recovery, using the default
    /// production sync policy (group commit).
    /// If `encryption_key` is provided, WAL payloads are encrypted with AES-256-GCM.
    #[cfg(feature = "encrypt")]
    pub fn with_wal(
        id: usize,
        max_nodes: usize,
        channel_size: usize,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
        wal_dir: PathBuf,
        encryption_key: Option<[u8; 32]>,
    ) -> std::io::Result<Self> {
        Self::with_wal_policy(
            id,
            max_nodes,
            channel_size,
            persistor,
            wal_dir,
            encryption_key,
            shard_wal_policy(),
        )
    }

    /// Create a shard with WAL enabled and an explicit sync policy. Exposed so
    /// operators (and benchmarks) can trade durability latency for throughput,
    /// e.g. `Always` for strict per-write fsync vs `Group` for amortized fsync.
    #[cfg(feature = "encrypt")]
    pub fn with_wal_policy(
        id: usize,
        max_nodes: usize,
        channel_size: usize,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
        wal_dir: PathBuf,
        encryption_key: Option<[u8; 32]>,
        policy: WalSyncPolicy,
    ) -> std::io::Result<Self> {
        let wal = if let Some(key) = encryption_key {
            Arc::new(Mutex::new(WriteAheadLog::open_with_encryption(
                &wal_dir, policy, key,
            )?))
        } else {
            Arc::new(Mutex::new(WriteAheadLog::open_with_policy(
                &wal_dir, policy,
            )?))
        };
        let flusher = spawn_group_flusher(wal.clone());
        Ok(Self {
            id,
            nodes: HashMap::new(),
            max_nodes,
            channel_size,
            persistor,
            wal: Some(wal),
            flusher,
            projection: new_shard_projection(),
        })
    }

    /// Create a shard with WAL enabled for crash recovery (no encryption
    /// feature), using the default production sync policy (group commit).
    #[cfg(not(feature = "encrypt"))]
    pub fn with_wal(
        id: usize,
        max_nodes: usize,
        channel_size: usize,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
        wal_dir: PathBuf,
        encryption_key: Option<[u8; 32]>,
    ) -> std::io::Result<Self> {
        Self::with_wal_policy(
            id,
            max_nodes,
            channel_size,
            persistor,
            wal_dir,
            encryption_key,
            shard_wal_policy(),
        )
    }

    /// Create a shard with WAL enabled and an explicit sync policy. Exposed so
    /// operators (and benchmarks) can trade durability latency for throughput,
    /// e.g. `Always` for strict per-write fsync vs `Group` for amortized fsync.
    #[cfg(not(feature = "encrypt"))]
    pub fn with_wal_policy(
        id: usize,
        max_nodes: usize,
        channel_size: usize,
        persistor: Arc<dyn NamespacedPersistenceAgent>,
        wal_dir: PathBuf,
        _encryption_key: Option<[u8; 32]>,
        policy: WalSyncPolicy,
    ) -> std::io::Result<Self> {
        let wal = Arc::new(Mutex::new(WriteAheadLog::open_with_policy(
            &wal_dir, policy,
        )?));
        let flusher = spawn_group_flusher(wal.clone());
        Ok(Self {
            id,
            nodes: HashMap::new(),
            max_nodes,
            channel_size,
            persistor,
            wal: Some(wal),
            flusher,
            projection: new_shard_projection(),
        })
    }

    /// Replay WAL on startup — apply any uncommitted events.
    /// Returns the number of records replayed.
    pub async fn replay_wal(&mut self) -> Result<usize, ShardError> {
        let replay_result = if let Some(wal) = &self.wal {
            wal.lock()
                .await
                .replay()
                .map_err(|e| ShardError::Persistence(e.to_string()))?
        } else {
            return Ok(0);
        };

        let count = replay_result.records.len();

        // Group WAL records by NexoraId
        let mut by_node: HashMap<NexoraId, Vec<WalOperation>> = HashMap::new();
        for record in &replay_result.records {
            if let Some(qid) = record.operation.nexora_id() {
                by_node
                    .entry(qid.clone())
                    .or_default()
                    .push(record.operation.clone());
            }
        }

        // For each node with WAL records, check if they need recovery
        for (qid, operations) in &by_node {
            let replay_from = operations
                .iter()
                .rposition(|op| matches!(op, WalOperation::SnapshotCheckpoint { .. }))
                .map_or(0, |idx| idx + 1);
            let events: Vec<TimedEvent<NodeChangeEvent>> = operations[replay_from..]
                .iter()
                .flat_map(|op| match op {
                    WalOperation::NodeEvent { event, .. } => vec![event.clone()],
                    WalOperation::NodeEvents { events, .. } => events.clone(),
                    _ => Vec::new(),
                })
                .collect();

            if !events.is_empty() {
                self.persistor
                    .persist_node_change_events(qid.clone(), events.clone())
                    .await
                    .map_err(|e| ShardError::Persistence(e.to_string()))?;

                let recovered_through = events
                    .iter()
                    .map(|event| event.time)
                    .max()
                    .unwrap_or(EventTime::MIN);
                if let Some(wal) = &self.wal {
                    wal.lock()
                        .await
                        .append(WalOperation::SnapshotCheckpoint {
                            qid: qid.clone(),
                            snapshot_time: recovered_through,
                        })
                        .map_err(|e| ShardError::Persistence(e.to_string()))?;
                }
            }
        }

        tracing::info!(
            shard = self.id,
            records = count,
            nodes = by_node.len(),
            "WAL replay complete"
        );

        Ok(count)
    }

    /// Send a command to a node, waking it from persistence if needed.
    pub async fn send(&mut self, qid: &NexoraId, cmd: NodeCommand) -> Result<(), ShardError> {
        self.touch(qid).await?;
        if let Some(entry) = self.nodes.get(qid) {
            entry
                .tx
                .send(cmd)
                .await
                .map_err(|_| ShardError::NodeUnavailable(qid.clone()))
        } else {
            Err(ShardError::NodeNotFound(qid.clone()))
        }
    }

    /// Get a reference to a node entry if it's in memory (for read-lock fast path).
    pub(crate) fn get_node(&self, qid: &NexoraId) -> Option<&NodeEntry> {
        if let Some(entry) = self.nodes.get(qid) {
            // Note: last_access update requires &mut, skip in read path
            return Some(entry);
        }
        None
    }

    /// #1: Read a resident node's published snapshot from the projection without
    /// a mailbox round-trip. Returns `None` if the node is not resident (never
    /// woken, or slept/evicted) — the caller then falls back to the wake path.
    pub(crate) fn read_projection(&self, qid: &NexoraId) -> Option<Arc<NodeReadState>> {
        self.projection.get(qid).map(|entry| entry.value().clone())
    }

    /// Ensure a node is awake, loading from persistence if needed.
    /// Used by the route() fast/slow path after the write lock is acquired.
    pub async fn ensure_node_awake(&mut self, qid: &NexoraId) -> Result<(), ShardError> {
        self.touch(qid).await
    }

    /// Ensure a node is awake, loading from persistence if needed.
    async fn touch(&mut self, qid: &NexoraId) -> Result<(), ShardError> {
        if let Some(entry) = self.nodes.get_mut(qid) {
            entry.last_access = Instant::now();
            return Ok(());
        }

        // Node not in memory — wake from persistence
        self.wake_node(qid.clone()).await
    }

    /// Load a node from persistence and create its task.
    async fn wake_node(&mut self, qid: NexoraId) -> Result<(), ShardError> {
        // Enforce memory limit before waking
        self.enforce_memory_limit().await?;

        // Load snapshot
        let snapshot = self
            .persistor
            .get_latest_snapshot(qid.clone(), EventTime::MAX)
            .await
            .map_err(|e| ShardError::Persistence(e.to_string()))?;

        let snapshot_time = snapshot.as_ref().map(|(t, _)| *t);
        let recovered = match snapshot {
            Some((_time, data)) => deserialize_snapshot(&data)
                .map_err(|e| ShardError::Persistence(format!("invalid snapshot: {e}")))?,
            None => RecoveredSnapshot {
                properties: BTreeMap::new(),
                property_times: BTreeMap::new(),
                edges: HashSet::new(),
                labels: HashSet::new(),
                edge_properties: HashMap::new(),
                tombstone: None,
            },
        };

        // Load journal events after snapshot time
        let events = self
            .persistor
            .get_node_change_events(
                qid.clone(),
                snapshot_time
                    .map(|time| EventTime::from_micros(time.as_micros().saturating_add(1))),
                None,
            )
            .await
            .map_err(|e| ShardError::Persistence(e.to_string()))?;

        // Seed reconstruction from the snapshot's full state, then apply any
        // journal events recorded after the snapshot. GAP-1: labels, edge
        // properties, and tombstone now start from the snapshot rather than
        // empty, so sleep/wake (where all events predate the snapshot) recovers
        // them correctly; post-snapshot journal events layer on top.
        let mut properties = recovered.properties;
        // Per-property event time for event-time LWW. Seeded from the snapshot
        // (empty for snapshots written by older builds — those keys then behave
        // as `EventTime::MIN`, so the first replayed write always wins, matching
        // the pre-LWW arrival-order recovery). Post-snapshot journal events
        // layer on with the same event-time comparison used on the live path.
        let mut property_times: BTreeMap<Symbol, EventTime> = recovered.property_times;
        let mut edges = recovered.edges;
        let mut labels: HashSet<Symbol> = recovered.labels;
        // Edge properties keyed by (edge_type, target) — mirrors NodeTask's
        // edge_properties store so recovered state matches the live layout.
        let mut edge_properties: HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>> =
            recovered.edge_properties;
        // Tombstone tracks soft-delete state. The last NodeDeleted/NodeRestored
        // event in the stream wins.
        let mut tombstone: Option<TombstoneRecord> = recovered.tombstone;
        let mut max_event_time: Option<EventTime> = None;
        for event in &events {
            // Track the maximum event time for timestamp regression fix
            if max_event_time.is_none() || event.time > max_event_time.unwrap() {
                max_event_time = Some(event.time);
            }
            match &event.event {
                NodeChangeEvent::PropertySet { key, value } => {
                    // Event-time LWW: a replayed write only wins if its event
                    // time is >= the stored one. Journal events are appended in
                    // arrival order, so a late write recorded after a newer one
                    // must not resurrect the stale value on recovery — mirrors
                    // the live commit path in node_task.rs.
                    let wins = match property_times.get(key) {
                        Some(&existing) => event.time >= existing,
                        None => true,
                    };
                    if wins {
                        properties.insert(key.clone(), value.clone());
                        property_times.insert(key.clone(), event.time);
                    }
                }
                NodeChangeEvent::PropertyRemoved { key, .. } => {
                    // Event-time LWW for removals (F5): mirror the live commit
                    // path. A removal wins only if its event time is >= the
                    // stored one; the winning time is RETAINED as a tombstone so
                    // a later older write stays rejected and a newer write wins.
                    // Journal events are appended in arrival order, so this guard
                    // stops a stale removal recorded after a newer write from
                    // deleting the live value on recovery.
                    let wins = match property_times.get(key) {
                        Some(&existing) => event.time >= existing,
                        None => true,
                    };
                    if wins {
                        properties.remove(key);
                        property_times.insert(key.clone(), event.time);
                    }
                }
                NodeChangeEvent::EdgeAdded { edge } => {
                    edges.insert(edge.clone());
                }
                NodeChangeEvent::EdgeRemoved { edge } => {
                    edges.remove(edge);
                }
                // P0.1: Reconstruct labels from the event stream.
                // Labels are rebuilt from LabelAdded/LabelRemoved events
                // during replay so they survive crash recovery.
                NodeChangeEvent::LabelAdded { label } => {
                    labels.insert(label.clone());
                }
                NodeChangeEvent::LabelRemoved { label } => {
                    labels.remove(label);
                }
                // GAP-1: Reconstruct edge properties from the event stream so
                // they survive crash recovery. Key is (edge_type, target),
                // matching NodeTask's live edge_properties layout.
                NodeChangeEvent::EdgePropertySet {
                    edge_type,
                    target,
                    key,
                    value,
                } => {
                    edge_properties
                        .entry((edge_type.clone(), target.clone()))
                        .or_default()
                        .insert(key.clone(), value.clone());
                }
                NodeChangeEvent::EdgePropertyRemoved {
                    edge_type,
                    target,
                    key,
                } => {
                    let ekey = (edge_type.clone(), target.clone());
                    if let Some(props) = edge_properties.get_mut(&ekey) {
                        props.remove(key);
                        // Drop the entry entirely once its last property is gone,
                        // so recovered state matches a node that never had it.
                        if props.is_empty() {
                            edge_properties.remove(&ekey);
                        }
                    }
                }
                // GAP-1: Reconstruct soft-delete state. A NodeDeleted sets the
                // tombstone; a later NodeRestored clears it. Without this, a
                // deleted node would "resurrect" after crash recovery.
                NodeChangeEvent::NodeDeleted { tombstone: ts } => {
                    tombstone = Some(ts.clone());
                }
                NodeChangeEvent::NodeRestored => {
                    tombstone = None;
                }
            }
        }

        // Spawn node task with recovered state
        // FIX P1-1: Capture JoinHandle for panic detection
        // P0.1 Task 1.3: use full-state constructor so recovered labels survive.
        // GAP-1: edge_properties and tombstone are now reconstructed and passed
        // through so edge data and soft-delete state survive crash recovery.
        let NodeTaskHandle { tx, handle } = NodeTask::spawn_with_full_state(
            qid.clone(),
            labels,
            properties,
            edges,
            edge_properties,
            None,
            None,
            tombstone,
            property_times,
            self.channel_size,
            self.wal.clone(),
            max_event_time,
            Some(self.projection.clone()),
        );

        self.nodes.insert(
            qid.clone(),
            NodeEntry {
                tx,
                handle,
                last_access: Instant::now(),
            },
        );

        tracing::debug!(shard = self.id, node = %qid, "Node woken from persistence");
        Ok(())
    }

    /// Put a node to sleep: drain journal, snapshot state, persist, release memory.
    pub async fn sleep_node(&mut self, qid: &NexoraId) -> Result<(), ShardError> {
        let entry = self
            .nodes
            .get(qid)
            .ok_or_else(|| ShardError::NodeNotFound(qid.clone()))?;
        let tx = entry.tx.clone();

        // Step 1: Copy a consistent state. The journal remains in memory until
        // persistence succeeds, so a transient backend failure cannot lose it.
        let (state_tx, state_rx) = oneshot::channel();
        tx.send(NodeCommand::SnapshotState { reply: state_tx })
            .await
            .map_err(|_| ShardError::NodeUnavailable(qid.clone()))?;

        let state = state_rx
            .await
            .map_err(|_| ShardError::NodeUnavailable(qid.clone()))?;

        if !state.journal.is_empty() {
            self.persistor
                .persist_node_change_events(qid.clone(), state.journal.clone())
                .await
                .map_err(|e| ShardError::Persistence(e.to_string()))?;
        }

        // Step 2: Serialize and persist snapshot.
        // GAP-1: persist the FULL state (labels/edge_properties/tombstone too),
        // otherwise sleep/wake silently drops everything except props+edges.
        let snapshot_data = serialize_snapshot(
            &state.properties,
            &state.property_times,
            &state.edges,
            &state.labels,
            &state.edge_properties,
            &state.tombstone,
        )
        .map_err(|e| ShardError::Persistence(format!("snapshot serialization failed: {e}")))?;
        let now = state
            .journal
            .iter()
            .map(|event| event.time)
            .max()
            .map_or_else(EventTime::now, |latest| latest.max(EventTime::now()));

        self.persistor
            .persist_snapshot(qid.clone(), now, snapshot_data)
            .await
            .map_err(|e| ShardError::Persistence(e.to_string()))?;

        // Step 4.4: Durability barrier BEFORE the checkpoint. The journal-event
        // and snapshot writes above use unsynced WriteOptions on the durable
        // backend, so they may still sit in an OS buffer. The SnapshotCheckpoint
        // we write next lets recovery *skip* every event up to `now` — so if we
        // crashed after the checkpoint reached disk but before this data did,
        // recovery would skip events that were never persisted → silent loss.
        // Force the data durable first so the checkpoint can only be crossed
        // once the state it subsumes is truly on disk.
        self.persistor
            .flush_durable()
            .await
            .map_err(|e| ShardError::Persistence(format!("durability barrier failed: {e}")))?;

        // Step 4.5: Write SnapshotCheckpoint to WAL
        // This marks that all events up to `now` are safely persisted.
        // On recovery, events before this checkpoint can be skipped.
        if let Some(wal) = &self.wal {
            wal.lock()
                .await
                .append(WalOperation::SnapshotCheckpoint {
                    qid: qid.clone(),
                    snapshot_time: now,
                })
                .map_err(|e| ShardError::Persistence(format!("WAL checkpoint failed: {e}")))?;
        }

        // Step 5: Tell node to shut down
        let (sleep_tx, sleep_rx) = oneshot::channel();
        let _ = tx.send(NodeCommand::GoToSleep { reply: sleep_tx }).await;
        let _ = sleep_rx.await;

        // Step 6: Remove from active nodes and drop its read projection entry.
        // #1: the projection only holds resident nodes; a slept node's reads
        // must fall back to the wake path, so its snapshot is removed here.
        self.nodes.remove(qid);
        self.projection.remove(qid);

        // FIX B: Trigger WAL truncation after every sleep to prevent unbounded
        // growth. `truncate()` discards records before the earliest unsubsumed
        // SnapshotCheckpoint, so sleeping a node (which just wrote its checkpoint)
        // immediately makes its old WAL records eligible for cleanup. Without this,
        // the WAL grows indefinitely even though all nodes are snapshotted.
        if let Some(wal) = &self.wal {
            if let Err(e) = wal.lock().await.truncate() {
                // Non-fatal: log the failure but don't block the sleep. WAL will
                // grow larger than necessary until the next successful truncate.
                tracing::warn!(
                    shard = self.id,
                    node = %qid,
                    error = %e,
                    "WAL truncate failed after sleep (non-fatal; WAL will grow until next truncate)"
                );
            }
        }

        tracing::debug!(shard = self.id, node = %qid, "Node put to sleep (journal persisted)");
        Ok(())
    }

    /// Enforce memory limit by sleeping the coldest node — D4: topology-aware,
    /// not pure LRU.
    ///
    /// Pure LRU evicts by `last_access` alone, which repeatedly pages out
    /// high-degree hub nodes that a traversal is about to revisit (a hub is
    /// touched between bursts, so its recency looks stale even though it's a
    /// hot traversal target). D4 scores each candidate by *effective idle time*
    /// = real idle time minus a degree bonus, so a well-connected hub must be
    /// idle substantially longer than a leaf before it's chosen. This keeps
    /// traversal hotspots resident, lifting projection hit-rate on graph
    /// queries, while leaves (degree 0) still evict on plain LRU order.
    async fn enforce_memory_limit(&mut self) -> Result<(), ShardError> {
        while self.nodes.len() >= self.max_nodes {
            let now = Instant::now();
            // Pick the node with the largest effective idle time (coldest after
            // the degree discount). Degree is read from the lock-free projection
            // (outbound+inbound half-edges); absent projection → degree 0 (leaf).
            let victim = self
                .nodes
                .iter()
                .max_by_key(|(qid, entry)| {
                    let idle_ms = now.duration_since(entry.last_access).as_millis() as u64;
                    let degree = self
                        .projection
                        .get(*qid)
                        .map(|s| s.edges.len() as u64)
                        .unwrap_or(0);
                    // Each unit of degree buys DEGREE_PROTECT_MS of extra
                    // residency, capped so a mega-hub can't become unevictable
                    // (which would let the shard exceed its memory bound).
                    const DEGREE_PROTECT_MS: u64 = 500;
                    const MAX_PROTECT_MS: u64 = 30_000;
                    let protect = (degree.saturating_mul(DEGREE_PROTECT_MS)).min(MAX_PROTECT_MS);
                    let effective_idle = idle_ms.saturating_sub(protect);
                    // Composite key: evict the largest effective idle; break ties
                    // (common when many nodes were touched close together, so
                    // their idle times all round near zero) by preferring the
                    // LOWEST-degree node — `u64::MAX - degree` is larger for a
                    // leaf than a hub, so the leaf wins the max and is evicted.
                    (effective_idle, u64::MAX - degree.min(u64::MAX - 1))
                })
                .map(|(qid, _)| qid.clone());
            if let Some(victim) = victim {
                self.sleep_node(&victim).await?;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Sleep every node whose last access is older than `ttl`, releasing its
    /// memory. Returns `(evicted, retained)`. State is persisted by `sleep_node`
    /// (full journal checkpoint), so an evicted node wakes with identical state
    /// on its next access — this only reclaims hot memory, never loses data.
    pub async fn evict_idle_nodes(&mut self, ttl: std::time::Duration) -> (usize, usize) {
        let now = Instant::now();
        let stale: Vec<NexoraId> = self
            .nodes
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.last_access) >= ttl)
            .map(|(qid, _)| qid.clone())
            .collect();
        let retained = self.nodes.len() - stale.len();
        let mut evicted = 0;
        for qid in stale {
            match self.sleep_node(&qid).await {
                Ok(()) => evicted += 1,
                Err(e) => tracing::warn!(
                    shard = self.id,
                    node = %qid,
                    "idle eviction: sleep_node failed (left resident): {e}"
                ),
            }
        }
        (evicted, retained)
    }

    pub async fn sleep_all_nodes(&mut self) -> Result<(), ShardError> {
        let node_ids: Vec<_> = self.nodes.keys().cloned().collect();
        for qid in node_ids {
            self.sleep_node(&qid).await?;
        }
        Ok(())
    }

    /// Graceful shutdown of the group-commit flusher: drain any buffered WAL
    /// records with a final fsync, then join the flusher task.
    ///
    /// Call this after `sleep_all_nodes` (which buffers snapshot checkpoints)
    /// so nothing acked or checkpointed is left un-synced. A no-op when the WAL
    /// is not under group commit. Idempotent — the handle is taken on first use.
    pub async fn shutdown_flusher(&mut self) {
        if let Some(flusher) = self.flusher.take() {
            flusher.shutdown().await;
        }
    }

    pub fn active_node_ids(&self) -> Vec<NexoraId> {
        self.nodes.keys().cloned().collect()
    }

    /// Number of active (awake) nodes in this shard.
    pub fn active_node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Check if a node is currently awake.
    pub fn is_awake(&self, qid: &NexoraId) -> bool {
        self.nodes.contains_key(qid)
    }

    /// FIX P1-1: Check for zombie nodes (tasks that have panicked).
    /// Returns the list of NexoraIds for nodes whose tasks have terminated unexpectedly.
    ///
    /// Note: We can only detect completed tasks, not panics directly.
    /// A completed task with Ok result is a graceful shutdown.
    /// A completed task with Err result indicates a panic.
    pub async fn check_zombie_nodes(&mut self) -> Vec<NexoraId> {
        let mut zombies = Vec::new();

        // Collect finished handles first
        let finished: Vec<(NexoraId, JoinHandle<()>)> = self
            .nodes
            .iter_mut()
            .filter_map(|(qid, entry)| {
                if entry.handle.is_finished() {
                    // Take the handle out of the entry
                    let handle = std::mem::replace(
                        &mut entry.handle,
                        tokio::spawn(async {}), // Dummy handle to satisfy type
                    );
                    Some((qid.clone(), handle))
                } else {
                    None
                }
            })
            .collect();

        for (qid, handle) in finished {
            match handle.await {
                Ok(()) => {
                    // Task completed normally (channel closed gracefully)
                    tracing::debug!(shard = self.id, node = %qid, "Node task completed normally");
                    zombies.push(qid);
                }
                Err(panic_err) => {
                    // Task panicked
                    tracing::error!(shard = self.id, node = %qid, "Node task panicked: {:?}", panic_err);
                    zombies.push(qid);
                }
            }
        }

        // Remove zombie nodes from active set and drop their projection entries
        // so reads for a dead node fall back to the wake path rather than
        // serving stale snapshots.
        for qid in &zombies {
            self.nodes.remove(qid);
            self.projection.remove(qid);
        }

        zombies
    }
}

/// Serialize node state into a snapshot byte vector.
///
/// Full recovered node state produced by [`deserialize_snapshot`].
struct RecoveredSnapshot {
    properties: BTreeMap<Symbol, PropertyValue>,
    /// Per-property event time (event-time LWW). Empty for snapshots written by
    /// older builds; missing keys behave as `EventTime::MIN`.
    property_times: BTreeMap<Symbol, EventTime>,
    edges: HashSet<HalfEdge>,
    labels: HashSet<Symbol>,
    edge_properties: HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
    tombstone: Option<TombstoneRecord>,
}

/// B4: snapshot codec error covering both the new MessagePack path and the
/// legacy JSON fallback read path.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotCodecError {
    #[error("msgpack encode error: {0}")]
    MsgpackEncode(#[from] rmp_serde::encode::Error),
    #[error("msgpack decode error: {0}")]
    MsgpackDecode(#[from] rmp_serde::decode::Error),
    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("snapshot integrity check failed: {0}")]
    IntegrityCheckFailed(String),
}

/// B4: format tag for the compact MessagePack snapshot encoding.
///
/// Legacy JSON snapshots always begin with `{` (`0x7B`). MessagePack maps also
/// start in the `0x80..=0x8f`/`0xde`/`0xdf` range, so we prepend an unambiguous
/// magic byte and dispatch on it during decode. `0xF5` is not a valid leading
/// byte for a JSON document, keeping the two formats disjoint.
const SNAPSHOT_MAGIC_MSGPACK: u8 = 0xF5;

#[allow(clippy::too_many_arguments)]
fn serialize_snapshot(
    properties: &BTreeMap<Symbol, PropertyValue>,
    property_times: &BTreeMap<Symbol, EventTime>,
    edges: &HashSet<HalfEdge>,
    labels: &HashSet<Symbol>,
    edge_properties: &HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
    tombstone: &Option<TombstoneRecord>,
) -> Result<Vec<u8>, SnapshotCodecError> {
    // GAP-1: The snapshot must carry the FULL node state, not just
    // properties+edges. On sleep/wake, journal events at or before the snapshot
    // time are filtered out during replay, so any state not in the snapshot is
    // lost (previously: labels, edge properties, and tombstone).
    let snapshot = SnapshotData {
        properties: properties.clone(),
        property_times: property_times.clone(),
        edges: edges.iter().cloned().collect(),
        labels: labels.iter().cloned().collect(),
        edge_properties: edge_properties
            .iter()
            .map(|((et, tgt), props)| SerEdgeProps {
                edge_type: et.clone(),
                target: tgt.clone(),
                properties: props.clone(),
            })
            .collect(),
        tombstone: tombstone.clone(),
    };
    // B4: write the compact MessagePack encoding behind a magic byte. This drops
    // the JSON parse/format overhead on the sleep/wake cold path while keeping
    // the exact same serde structure (zero schema maintenance).
    let mut payload = Vec::with_capacity(64);
    payload.push(SNAPSHOT_MAGIC_MSGPACK);
    rmp_serde::encode::write(&mut payload, &snapshot)?;

    // FIX A: Append SnapshotManifest with CRC32 checksum for integrity protection.
    // Format: <payload_bytes><newline><manifest_json_line>
    // The manifest is JSON (not MessagePack) so it's human-readable and self-
    // describing; separating it from the payload keeps decode logic simple.
    let manifest = SnapshotManifest::for_payload(
        SnapshotKind::Node,
        0, // last_tx_id unused for node snapshots (EventTime in persistor tracks ordering)
        &payload,
        ChecksumKind::Crc32,
    );
    let manifest_json = serde_json::to_string(&manifest).map_err(SnapshotCodecError::Json)?;

    let mut out = payload;
    out.push(b'\n');
    out.extend_from_slice(manifest_json.as_bytes());
    Ok(out)
}

/// Deserialize a snapshot back into full node state.
///
/// B4: reads either the new MessagePack format (leading `SNAPSHOT_MAGIC_MSGPACK`
/// byte) or the legacy JSON format written by older builds. Backward-compatible
/// reads are a hard requirement — production restarts will still encounter JSON
/// snapshots written before this change.
///
/// New fields carry `#[serde(default)]` so snapshots written by older builds
/// (properties+edges only) still deserialize — their missing fields recover as
/// empty, matching the previous behavior for those nodes.
///
/// FIX A: Snapshots written by the new `serialize_snapshot` carry a trailing
/// SnapshotManifest (after a newline). We verify the checksum before decoding
/// the payload. Legacy snapshots (no manifest) skip verification gracefully.
fn deserialize_snapshot(data: &[u8]) -> Result<RecoveredSnapshot, SnapshotCodecError> {
    // Split payload and manifest (if present). New format: <payload>\n<manifest_json>
    let (payload, manifest_opt) = if let Some(newline_pos) = data.iter().rposition(|&b| b == b'\n')
    {
        let payload_bytes = &data[..newline_pos];
        let manifest_bytes = &data[newline_pos + 1..];
        // Attempt to parse the manifest; if it fails (legacy snapshot with a
        // coincidental newline in the data), treat the whole blob as payload.
        match serde_json::from_slice::<SnapshotManifest>(manifest_bytes) {
            Ok(m) => (payload_bytes, Some(m)),
            Err(_) => (data, None), // legacy snapshot
        }
    } else {
        (data, None) // no newline → legacy snapshot
    };

    // Verify checksum if a manifest is present.
    if let Some(ref manifest) = manifest_opt {
        manifest
            .verify(payload)
            .map_err(SnapshotCodecError::IntegrityCheckFailed)?;
    }

    // Decode the payload (MessagePack or JSON).
    let snapshot: SnapshotData = match payload.first() {
        Some(&SNAPSHOT_MAGIC_MSGPACK) => rmp_serde::from_slice(&payload[1..])?,
        // Legacy path: JSON snapshots (start with `{`) and any other bytes fall
        // through to the JSON decoder, which reports an error for corrupt data.
        _ => serde_json::from_slice(payload)?,
    };
    let edge_properties = snapshot
        .edge_properties
        .into_iter()
        .map(|e| ((e.edge_type, e.target), e.properties))
        .collect();
    Ok(RecoveredSnapshot {
        properties: snapshot.properties,
        property_times: snapshot.property_times,
        edges: snapshot.edges.into_iter().collect(),
        labels: snapshot.labels.into_iter().collect(),
        edge_properties,
        tombstone: snapshot.tombstone,
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotData {
    properties: BTreeMap<Symbol, PropertyValue>,
    edges: Vec<HalfEdge>,
    // GAP-1: full-state fields. `default` keeps old snapshots readable.
    #[serde(default)]
    labels: Vec<Symbol>,
    #[serde(default)]
    edge_properties: Vec<SerEdgeProps>,
    #[serde(default)]
    tombstone: Option<TombstoneRecord>,
    // Event-time LWW: per-property event time. MUST stay LAST — rmp_serde encodes
    // this struct as a positional array, and `#[serde(default)]` only backfills
    // TRAILING missing elements, so a new field is only backward-compatible with
    // pre-existing MessagePack snapshots when appended at the end. Empty default
    // makes those keys behave as `EventTime::MIN` (first post-recovery write wins).
    #[serde(default)]
    property_times: BTreeMap<Symbol, EventTime>,
}

/// Serializable edge-property entry. The live store keys edge properties by a
/// `(Symbol, NexoraId)` tuple, which JSON can't use as a map key, so we flatten
/// it to a list of records for serialization.
#[derive(serde::Serialize, serde::Deserialize)]
struct SerEdgeProps {
    edge_type: Symbol,
    target: NexoraId,
    properties: BTreeMap<Symbol, PropertyValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("node not found: {0}")]
    NodeNotFound(NexoraId),
    #[error("node unavailable (task exited): {0}")]
    NodeUnavailable(NexoraId),
    #[error("persistence error: {0}")]
    Persistence(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryPersistor;

    fn test_shard(max_nodes: usize) -> GraphShard {
        let persistor = Arc::new(InMemoryPersistor::new());
        GraphShard::new(0, max_nodes, 16, persistor)
    }

    // ====== GS-001: send 自动唤醒不存在的节点 ======
    #[tokio::test]
    async fn test_send_auto_wakes_node() {
        let mut shard = test_shard(100);
        let qid = NexoraId::new_random();

        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetProperty {
                    key: Symbol::new("x"),
                    reply: tx,
                },
            )
            .await
            .unwrap();

        // 应自动唤醒（空状态），返回 None
        let result = rx.await.unwrap().unwrap();
        assert_eq!(result, None);
        assert!(shard.is_awake(&qid));
    }

    // ====== GS-002: sleep_node 后节点不再活跃 ======
    #[tokio::test]
    async fn test_sleep_node_removes_from_active() {
        let mut shard = test_shard(100);
        let qid = NexoraId::new_random();

        // 先唤醒
        let (tx, _) = oneshot::channel();
        let _ = shard
            .send(&qid, NodeCommand::MemorySize { reply: tx })
            .await;
        assert!(shard.is_awake(&qid));

        // 休眠
        shard.sleep_node(&qid).await.unwrap();
        assert!(!shard.is_awake(&qid));
        assert_eq!(shard.active_node_count(), 0);
    }

    // ====== GS-003: sleep_node 刷写 journal 到持久化层 ======
    #[tokio::test]
    async fn test_sleep_node_persists_journal() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();

        // 写入属性（产生 journal 事件）
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::SetProperty {
                    key: Symbol::new("speed"),
                    value: PropertyValue::Float(12.5),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // 休眠
        shard.sleep_node(&qid).await.unwrap();

        // 验证事件已持久化
        let events = persistor
            .get_node_change_events(qid, None, None)
            .await
            .unwrap();
        assert!(
            !events.is_empty(),
            "journal events should be persisted on sleep"
        );
    }

    // ====== GS-004: LRU 淘汰超过 max_nodes ======
    #[tokio::test]
    async fn test_lru_eviction() {
        let mut shard = test_shard(3); // 最多 3 个节点

        let qids: Vec<_> = (0..5).map(|_| NexoraId::new_random()).collect();

        // 唤醒 5 个节点（超过限制）
        for qid in &qids {
            let (tx, rx) = oneshot::channel();
            shard
                .send(qid, NodeCommand::MemorySize { reply: tx })
                .await
                .unwrap();
            let _ = rx.await;
        }

        // 活跃节点数不应超过限制
        assert!(
            shard.active_node_count() <= 3,
            "active nodes {} should be <= 3",
            shard.active_node_count()
        );
    }

    // ====== D4: 拓扑感知常驻 — 高入度 hub 抵抗淘汰 ======
    /// A high-degree hub, made resident and idle FIRST (oldest last_access),
    /// must survive eviction pressure while lower-degree leaves — accessed
    /// later but with no edges — are chosen instead. Pure LRU would evict the
    /// hub (it's the oldest); the degree bonus keeps it resident.
    #[tokio::test]
    async fn test_topology_aware_eviction_protects_hub() {
        let mut shard = test_shard(3); // room for 3 resident nodes

        // Hub: wake first (oldest), give it many edges (high degree).
        let hub = NexoraId::new_random();
        for _ in 0..10 {
            let (tx, rx) = oneshot::channel();
            shard
                .send(
                    &hub,
                    NodeCommand::AddEdge {
                        edge: HalfEdge::out(Symbol::new("KNOWS"), NexoraId::new_random()),
                        reply: tx,
                    },
                )
                .await
                .unwrap();
            rx.await.unwrap().unwrap();
        }
        // Ensure the hub is the oldest by access time.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Two leaves (no edges), accessed AFTER the hub.
        let leaf1 = NexoraId::new_random();
        let leaf2 = NexoraId::new_random();
        for leaf in [&leaf1, &leaf2] {
            let (tx, rx) = oneshot::channel();
            shard
                .send(leaf, NodeCommand::MemorySize { reply: tx })
                .await
                .unwrap();
            let _ = rx.await;
        }

        // A 4th node forces eviction (limit is 3). With pure LRU the hub (oldest)
        // would go; topology-aware eviction discounts the hub's idle time by its
        // degree, so a leaf is chosen and the hub stays resident.
        let trigger = NexoraId::new_random();
        let (tx, rx) = oneshot::channel();
        shard
            .send(&trigger, NodeCommand::MemorySize { reply: tx })
            .await
            .unwrap();
        let _ = rx.await;

        assert!(
            shard.active_node_count() <= 3,
            "eviction must keep the shard within its node limit"
        );
        assert!(
            shard.read_projection(&hub).is_some(),
            "high-degree hub must resist eviction (topology-aware, not pure LRU)"
        );
    }

    // ====== GS-006: TTL 空闲驱逐 ======
    #[tokio::test]
    async fn test_evict_idle_nodes() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();

        // Wake a node with a property, so it is resident.
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::SetProperty {
                    key: Symbol::new("name"),
                    value: PropertyValue::String("idle-1".into()),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
        assert_eq!(shard.active_node_count(), 1);

        // TTL far in the future → nothing is stale, node stays resident.
        let (evicted, retained) = shard.evict_idle_nodes(Duration::from_secs(3600)).await;
        assert_eq!((evicted, retained), (0, 1));
        assert!(shard.is_awake(&qid));

        // Let it age past a tiny TTL, then sweep → node is evicted (slept).
        tokio::time::sleep(Duration::from_millis(15)).await;
        let (evicted, retained) = shard.evict_idle_nodes(Duration::from_millis(10)).await;
        assert_eq!((evicted, retained), (1, 0));
        assert!(!shard.is_awake(&qid), "idle node must be slept");

        // Its state survives eviction: reading wakes it with the same value.
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetProperty {
                    key: Symbol::new("name"),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        let val = rx.await.unwrap().unwrap();
        assert_eq!(val, Some(PropertyValue::String("idle-1".into())));
    }

    // ====== GS-005: 唤醒后恢复属性 ======
    #[tokio::test]
    async fn test_wake_restores_properties() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();

        // 写入属性
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::SetProperty {
                    key: Symbol::new("name"),
                    value: PropertyValue::String("FL-042".into()),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // 休眠
        shard.sleep_node(&qid).await.unwrap();

        // 重新唤醒
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetProperty {
                    key: Symbol::new("name"),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        let result = rx.await.unwrap().unwrap();
        assert_eq!(result, Some(PropertyValue::String("FL-042".into())));
    }

    #[tokio::test]
    async fn wake_replay_applies_event_time_lww() {
        // Journal holds an out-of-order sequence: a newer-event-time write
        // followed by an older (late) one for the same key. Recovery must apply
        // event-time LWW and keep the newer value, not the stream-order-last one.
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();

        let events = vec![
            TimedEvent::new(
                NodeChangeEvent::PropertySet {
                    key: Symbol::new("temp"),
                    value: PropertyValue::Integer(20),
                },
                EventTime::from_micros(10_000),
            ),
            // Late arrival recorded after the newer one, but with a lower event
            // time — must lose under event-time LWW on replay.
            TimedEvent::new(
                NodeChangeEvent::PropertySet {
                    key: Symbol::new("temp"),
                    value: PropertyValue::Integer(5),
                },
                EventTime::from_micros(5_000),
            ),
        ];
        persistor
            .persist_node_change_events(qid.clone(), events)
            .await
            .unwrap();

        // Wake the node (cold: no snapshot, replays the journal) and read back.
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetProperty {
                    key: Symbol::new("temp"),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            rx.await.unwrap().unwrap(),
            Some(PropertyValue::Integer(20)),
            "replay must keep the higher-event-time value despite stream order"
        );
    }

    #[tokio::test]
    async fn wake_replay_applies_remove_event_time_lww() {
        // F5: journal holds a newer-event-time write followed by an older (late)
        // removal for the same key. Recovery must apply event-time LWW to the
        // removal too — the stale removal loses and the value survives, mirroring
        // the live commit path. Without delete-LWW on replay, the stream-order-
        // last removal would wrongly delete the live value on recovery.
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();

        let events = vec![
            TimedEvent::new(
                NodeChangeEvent::PropertySet {
                    key: Symbol::new("temp"),
                    value: PropertyValue::Integer(20),
                },
                EventTime::from_micros(10_000),
            ),
            // Late removal recorded after the newer write, lower event time —
            // must lose under event-time LWW on replay.
            TimedEvent::new(
                NodeChangeEvent::PropertyRemoved {
                    key: Symbol::new("temp"),
                    previous_value: PropertyValue::Integer(20),
                },
                EventTime::from_micros(5_000),
            ),
        ];
        persistor
            .persist_node_change_events(qid.clone(), events)
            .await
            .unwrap();

        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetProperty {
                    key: Symbol::new("temp"),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            rx.await.unwrap().unwrap(),
            Some(PropertyValue::Integer(20)),
            "replay must reject a stale (lower-event-time) removal and keep the value"
        );
    }

    // ====== GS-006: 唤醒后恢复边 ======
    #[tokio::test]
    async fn test_wake_restores_edges() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();
        let target = NexoraId::new_random();
        let edge = HalfEdge::out(Symbol::new("KNOWS"), target);

        // 添加边
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::AddEdge {
                    edge: edge.clone(),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // 休眠
        shard.sleep_node(&qid).await.unwrap();

        // 重新唤醒，验证边
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetEdges {
                    edge_type: None,
                    reply: tx,
                },
            )
            .await
            .unwrap();
        let edges = rx.await.unwrap().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], edge);
    }

    // ====== GS-007: 唤醒后恢复 journal 事件 ======
    #[tokio::test]
    async fn test_wake_replays_journal() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let mut shard = GraphShard::new(0, 100, 16, persistor.clone());
        let qid = NexoraId::new_random();

        // 写入多个属性
        for i in 0..5 {
            let (tx, rx) = oneshot::channel();
            shard
                .send(
                    &qid,
                    NodeCommand::SetProperty {
                        key: Symbol::new("tick"),
                        value: PropertyValue::Integer(i),
                        reply: tx,
                    },
                )
                .await
                .unwrap();
            rx.await.unwrap().unwrap();
        }

        // 休眠
        shard.sleep_node(&qid).await.unwrap();

        // 重新唤醒，验证最终状态
        let (tx, rx) = oneshot::channel();
        shard
            .send(
                &qid,
                NodeCommand::GetProperty {
                    key: Symbol::new("tick"),
                    reply: tx,
                },
            )
            .await
            .unwrap();
        let result = rx.await.unwrap().unwrap();
        assert_eq!(result, Some(PropertyValue::Integer(4)));
    }

    // ====== GS-008: active_node_count ======
    #[tokio::test]
    async fn test_active_node_count() {
        let mut shard = test_shard(100);
        assert_eq!(shard.active_node_count(), 0);

        for _ in 0..3 {
            let qid = NexoraId::new_random();
            let (tx, rx) = oneshot::channel();
            shard
                .send(&qid, NodeCommand::MemorySize { reply: tx })
                .await
                .unwrap();
            let _ = rx.await;
        }

        assert_eq!(shard.active_node_count(), 3);
    }

    // ====== GS-009: is_awake 对活跃节点返回 true ======
    #[tokio::test]
    async fn test_is_awake_active() {
        let mut shard = test_shard(100);
        let qid = NexoraId::new_random();

        let (tx, rx) = oneshot::channel();
        shard
            .send(&qid, NodeCommand::MemorySize { reply: tx })
            .await
            .unwrap();
        let _ = rx.await;

        assert!(shard.is_awake(&qid));
    }

    // ====== GS-010: is_awake 对不存在节点返回 false ======
    #[tokio::test]
    async fn test_is_awake_unknown() {
        let shard = test_shard(100);
        let qid = NexoraId::new_random();
        assert!(!shard.is_awake(&qid));
    }

    // ====== 额外：sleep 不存在的节点应返回错误 ======
    #[tokio::test]
    async fn test_sleep_nonexistent_node_errors() {
        let mut shard = test_shard(100);
        let qid = NexoraId::new_random();
        let result = shard.sleep_node(&qid).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn corrupted_snapshot_is_reported_instead_of_erasing_state() {
        let persistor = Arc::new(InMemoryPersistor::new());
        let qid = NexoraId::new_random();
        persistor
            .persist_snapshot(
                qid.clone(),
                EventTime::from_micros(10),
                b"not-json".to_vec(),
            )
            .await
            .unwrap();
        let mut shard = GraphShard::new(0, 100, 16, persistor);

        let (reply, _) = oneshot::channel();
        let error = shard
            .send(&qid, NodeCommand::GetAllProperties { reply })
            .await
            .unwrap_err();
        assert!(
            matches!(error, ShardError::Persistence(message) if message.contains("invalid snapshot"))
        );
    }

    // ====== B4: snapshot codec (MessagePack + legacy JSON fallback) ======

    /// Build a fully-populated snapshot fixture touching every field so the
    /// round-trip tests catch any silently-dropped state (GAP-1 regression).
    #[allow(clippy::type_complexity)]
    fn full_snapshot_fixture() -> (
        BTreeMap<Symbol, PropertyValue>,
        HashSet<HalfEdge>,
        HashSet<Symbol>,
        HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
        Option<TombstoneRecord>,
    ) {
        use nexora_value::EdgeDirection;

        let mut properties = BTreeMap::new();
        properties.insert(Symbol::new("name"), PropertyValue::String("alice".into()));
        properties.insert(Symbol::new("age"), PropertyValue::Integer(42));
        properties.insert(Symbol::new("score"), PropertyValue::Float(3.5));
        properties.insert(Symbol::new("active"), PropertyValue::Boolean(true));
        properties.insert(
            Symbol::new("tags"),
            PropertyValue::List(vec![
                PropertyValue::String("a".into()),
                PropertyValue::Integer(7),
            ]),
        );

        let tgt1 = NexoraId::new_random();
        let tgt2 = NexoraId::new_random();
        let mut edges = HashSet::new();
        edges.insert(HalfEdge::new(
            Symbol::new("KNOWS"),
            EdgeDirection::Out,
            tgt1.clone(),
        ));
        edges.insert(HalfEdge::new(
            Symbol::new("LIVES_IN"),
            EdgeDirection::In,
            tgt2.clone(),
        ));

        let mut labels = HashSet::new();
        labels.insert(Symbol::new("Person"));
        labels.insert(Symbol::new("Employee"));

        let mut edge_props_inner = BTreeMap::new();
        edge_props_inner.insert(Symbol::new("since"), PropertyValue::Integer(2020));
        let mut edge_properties = HashMap::new();
        edge_properties.insert((Symbol::new("KNOWS"), tgt1.clone()), edge_props_inner);

        let tombstone = Some(TombstoneRecord {
            deleted_at: EventTime::from_micros(12345),
            deleted_by: Some("svc-account".into()),
            reason: Some("gdpr".into()),
        });

        (properties, edges, labels, edge_properties, tombstone)
    }

    /// Assert a recovered snapshot matches the source state field-by-field.
    #[allow(clippy::type_complexity)]
    fn assert_recovered_eq(
        recovered: &RecoveredSnapshot,
        properties: &BTreeMap<Symbol, PropertyValue>,
        edges: &HashSet<HalfEdge>,
        labels: &HashSet<Symbol>,
        edge_properties: &HashMap<(Symbol, NexoraId), BTreeMap<Symbol, PropertyValue>>,
        tombstone: &Option<TombstoneRecord>,
    ) {
        assert_eq!(&recovered.properties, properties, "properties mismatch");
        assert_eq!(&recovered.edges, edges, "edges mismatch");
        assert_eq!(&recovered.labels, labels, "labels mismatch");
        assert_eq!(
            &recovered.edge_properties, edge_properties,
            "edge_properties mismatch"
        );
        assert_eq!(
            recovered.tombstone.as_ref().map(|t| (
                t.deleted_at,
                t.deleted_by.clone(),
                t.reason.clone()
            )),
            tombstone
                .as_ref()
                .map(|t| (t.deleted_at, t.deleted_by.clone(), t.reason.clone())),
            "tombstone mismatch"
        );
    }

    #[test]
    fn snapshot_msgpack_roundtrip_all_fields() {
        let (properties, edges, labels, edge_properties, tombstone) = full_snapshot_fixture();
        let bytes = serialize_snapshot(
            &properties,
            &BTreeMap::new(),
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        )
        .unwrap();
        // New format must carry the MessagePack magic byte.
        assert_eq!(bytes.first(), Some(&SNAPSHOT_MAGIC_MSGPACK));
        let recovered = deserialize_snapshot(&bytes).unwrap();
        assert_recovered_eq(
            &recovered,
            &properties,
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        );
    }

    #[test]
    fn snapshot_reads_legacy_json() {
        use nexora_value::EdgeDirection;

        // Hand-build a legacy JSON snapshot exactly as older builds wrote it.
        let (properties, edges, labels, edge_properties, tombstone) = full_snapshot_fixture();
        let legacy = SnapshotData {
            properties: properties.clone(),
            property_times: BTreeMap::new(),
            edges: edges.iter().cloned().collect(),
            labels: labels.iter().cloned().collect(),
            edge_properties: edge_properties
                .iter()
                .map(|((et, tgt), props)| SerEdgeProps {
                    edge_type: et.clone(),
                    target: tgt.clone(),
                    properties: props.clone(),
                })
                .collect(),
            tombstone: tombstone.clone(),
        };
        let json_bytes = serde_json::to_vec(&legacy).unwrap();
        // Legacy bytes have no magic prefix and start with `{`.
        assert_eq!(json_bytes.first(), Some(&b'{'));

        let recovered = deserialize_snapshot(&json_bytes).unwrap();
        assert_recovered_eq(
            &recovered,
            &properties,
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        );

        // Also cover the oldest JSON shape: only properties+edges (missing fields
        // recover as empty via serde defaults).
        let mut minimal_edges = HashSet::new();
        minimal_edges.insert(HalfEdge::new(
            Symbol::new("REL"),
            EdgeDirection::Out,
            NexoraId::new_random(),
        ));
        let minimal_json = serde_json::json!({
            "properties": {},
            "edges": minimal_edges.iter().collect::<Vec<_>>(),
        });
        let minimal_bytes = serde_json::to_vec(&minimal_json).unwrap();
        let recovered_min = deserialize_snapshot(&minimal_bytes).unwrap();
        assert_eq!(recovered_min.edges, minimal_edges);
        assert!(recovered_min.labels.is_empty());
        assert!(recovered_min.edge_properties.is_empty());
        assert!(recovered_min.tombstone.is_none());
    }

    #[test]
    fn snapshot_empty_roundtrip() {
        let properties = BTreeMap::new();
        let edges = HashSet::new();
        let labels = HashSet::new();
        let edge_properties = HashMap::new();
        let tombstone = None;
        let bytes = serialize_snapshot(
            &properties,
            &BTreeMap::new(),
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        )
        .unwrap();
        let recovered = deserialize_snapshot(&bytes).unwrap();
        assert_recovered_eq(
            &recovered,
            &properties,
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        );
    }

    #[test]
    fn snapshot_tombstone_preserved() {
        // A tombstoned node must not "resurrect" after a codec round-trip.
        let properties = BTreeMap::new();
        let edges = HashSet::new();
        let labels = HashSet::new();
        let edge_properties = HashMap::new();
        let tombstone = Some(TombstoneRecord {
            deleted_at: EventTime::from_micros(999),
            deleted_by: None,
            reason: None,
        });
        let bytes = serialize_snapshot(
            &properties,
            &BTreeMap::new(),
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        )
        .unwrap();
        let recovered = deserialize_snapshot(&bytes).unwrap();
        assert!(
            recovered.tombstone.is_some(),
            "tombstone dropped — deleted node would resurrect on wake"
        );
        assert_eq!(
            recovered.tombstone.unwrap().deleted_at,
            EventTime::from_micros(999)
        );
    }

    #[test]
    fn snapshot_property_times_roundtrip() {
        // Per-property event times must survive a codec round-trip so event-time
        // LWW is preserved across sleep/wake.
        let mut properties = BTreeMap::new();
        properties.insert(Symbol::new("temp"), PropertyValue::Integer(20));
        let mut property_times = BTreeMap::new();
        property_times.insert(Symbol::new("temp"), EventTime::from_micros(10_000));
        let edges = HashSet::new();
        let labels = HashSet::new();
        let edge_properties = HashMap::new();
        let tombstone = None;

        let bytes = serialize_snapshot(
            &properties,
            &property_times,
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        )
        .unwrap();
        let recovered = deserialize_snapshot(&bytes).unwrap();
        assert_eq!(
            recovered.property_times.get(&Symbol::new("temp")),
            Some(&EventTime::from_micros(10_000)),
            "per-property event time must round-trip"
        );
    }

    #[test]
    fn legacy_snapshot_recovers_empty_property_times() {
        // Snapshots written before property_times existed must still decode,
        // with an empty map (keys then behave as EventTime::MIN on replay).
        let minimal_json = serde_json::json!({
            "properties": {},
            "edges": Vec::<HalfEdge>::new(),
        });
        let bytes = serde_json::to_vec(&minimal_json).unwrap();
        let recovered = deserialize_snapshot(&bytes).unwrap();
        assert!(
            recovered.property_times.is_empty(),
            "legacy snapshot must recover empty property_times via serde default"
        );
    }

    /// Struct mirroring the pre-`property_times` `SnapshotData` field layout, used
    /// to synthesize a genuine old-format MessagePack snapshot.
    #[derive(serde::Serialize)]
    struct LegacySnapshotData {
        properties: BTreeMap<Symbol, PropertyValue>,
        edges: Vec<HalfEdge>,
        #[serde(default)]
        labels: Vec<Symbol>,
        #[serde(default)]
        edge_properties: Vec<SerEdgeProps>,
        #[serde(default)]
        tombstone: Option<TombstoneRecord>,
    }

    #[test]
    fn legacy_msgpack_snapshot_without_property_times_still_decodes() {
        // Regression: rmp_serde encodes SnapshotData as a positional array, and
        // #[serde(default)] only backfills TRAILING missing elements. A snapshot
        // written by an older build (5 fields, no property_times) must still wake
        // — property_times MUST be the last field for this to hold. If someone
        // reorders it into the middle, this test fails with a msgpack decode error
        // (exactly the data-loss-on-restart bug this guards against).
        let (properties, edges, labels, edge_properties, tombstone) = full_snapshot_fixture();
        let legacy = LegacySnapshotData {
            properties: properties.clone(),
            edges: edges.iter().cloned().collect(),
            labels: labels.iter().cloned().collect(),
            edge_properties: edge_properties
                .iter()
                .map(|((et, tgt), props)| SerEdgeProps {
                    edge_type: et.clone(),
                    target: tgt.clone(),
                    properties: props.clone(),
                })
                .collect(),
            tombstone: tombstone.clone(),
        };
        // Reproduce the exact on-disk format: magic byte + rmp_serde encoding.
        let mut bytes = Vec::new();
        bytes.push(SNAPSHOT_MAGIC_MSGPACK);
        rmp_serde::encode::write(&mut bytes, &legacy).unwrap();

        let recovered = deserialize_snapshot(&bytes).unwrap();
        assert_recovered_eq(
            &recovered,
            &properties,
            &edges,
            &labels,
            &edge_properties,
            &tombstone,
        );
        assert!(
            recovered.property_times.is_empty(),
            "old msgpack snapshot recovers empty property_times"
        );
    }
}
