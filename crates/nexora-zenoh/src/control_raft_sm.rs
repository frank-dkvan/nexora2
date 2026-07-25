//! Control-plane Raft state machine (A2-2).
//!
//! The state machine *is* the unified [`ControlPlaneStore`]: applying a
//! committed [`ControlCommand`] writes through to the store, so metadata (shard
//! map, MV/SQ/schema defs) is the replicated state. This is the single apply
//! target the A1 design set up for.
//!
//! Persistence model: **persistent state machine**. The store is RocksDB-backed
//! and already survives restart, so `apply()` persists directly and no snapshot
//! is needed for durability. We additionally persist Raft's own last-applied log
//! id and last membership under [`Namespace::RaftMeta`] so `applied_state()` can
//! report them on startup without replaying the whole log.
//!
//! Snapshots (for catching up a lagging/new follower) serialize only the
//! application namespaces via [`ControlPlaneStore::snapshot`]; `RaftMeta` is
//! carried in [`SnapshotMeta`], not the blob.

// openraft's `StorageError` is a large enum fixed by the `RaftStateMachine`
// trait for every method here; boxing it would force unwrapping at each trait
// boundary. The lint is a false positive against a trait-constrained API.
#![allow(clippy::result_large_err)]

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use nexora_core::control_plane_store::{ControlPlaneStore, Namespace};

use crate::control_raft::{ControlCommand, ControlNodeId, ControlRaftTypeConfig, ControlResponse};

/// Well-known keys under [`Namespace::RaftMeta`].
const APPLIED_KEY: &str = "last_applied";
const SNAPSHOT_KEY: &str = "current_snapshot";

/// A committed change to an ontology (`Namespace::DomainDef`), emitted by the
/// state machine's `apply()` AFTER the durable write-through so the app layer can
/// activate it (create event tables, update the topic router, schedule views) or
/// deactivate it (drop routing rules). Emitted on every node in Raft commit
/// order, so activation is consensus-consistent across the cluster.
#[derive(Clone, Debug)]
pub enum OntologyActivation {
    /// A domain package was put; `pkg_json` is its serialized `DomainPackage`.
    Put { domain: String, pkg_json: Vec<u8> },
    /// A domain was deleted.
    Delete { domain: String },
}

/// Raft state machine over the unified control-plane store.
#[derive(Clone)]
pub struct ControlStateMachine {
    store: Arc<dyn ControlPlaneStore>,
    /// A6: namespace-level quarantine for apply failures.
    quarantine: QuarantineTracker,
    /// When set, committed `DomainDef` changes are emitted here (after the store
    /// write-through) so the app layer can activate/deactivate the ontology. The
    /// send is non-blocking (unbounded) so it never stalls the Raft apply loop.
    /// `None` in tests / non-event-first builds → ontology commits still persist,
    /// they just aren't activated.
    ontology_tx: Option<tokio::sync::mpsc::UnboundedSender<OntologyActivation>>,
}

/// Persisted `(last_applied, membership)` bookkeeping for `applied_state()`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct AppliedState {
    last_applied: Option<LogId<ControlNodeId>>,
    membership: StoredMembership<ControlNodeId, openraft::BasicNode>,
}

/// A stored snapshot: the metadata plus the store's serialized blob.
#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<ControlNodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

/// A6: per-namespace divergence isolation. When applying a control command to
/// one namespace fails (deserialize error, store write error), quarantine just
/// that namespace instead of halting the whole state machine — other namespaces
/// keep serving. Only when quarantined namespaces exceed a threshold does the
/// node halt (too much divergence = unsafe to continue).
///
/// **Raft consistency note:** Quarantine is a local apply-layer isolation
/// mechanism. It does NOT affect Raft log replication — all nodes still receive
/// and store the same log entries. The `last_applied` index advances even when
/// a command is quarantined/skipped, because Raft requires monotonic progress
/// to avoid infinite retries. A quarantined namespace simply stops executing
/// new commands locally until cleared (e.g., after a snapshot repair).
#[derive(Clone)]
pub struct QuarantineTracker {
    /// Namespaces currently quarantined due to apply failures.
    quarantined: Arc<RwLock<HashSet<Namespace>>>,
    /// How many quarantined namespaces trigger a node-wide halt.
    halt_threshold: usize,
}

impl QuarantineTracker {
    pub fn new(halt_threshold: usize) -> Self {
        Self {
            quarantined: Arc::new(RwLock::new(HashSet::new())),
            halt_threshold,
        }
    }

    /// Quarantine a namespace after an apply failure. Returns true if the node
    /// should now halt (threshold exceeded).
    pub fn quarantine(&self, ns: Namespace, reason: &str) -> bool {
        let mut q = self.quarantined.write().unwrap();
        if q.insert(ns) {
            tracing::error!(
                namespace = ?ns,
                reason = %reason,
                quarantined_count = q.len(),
                halt_threshold = self.halt_threshold,
                "Namespace quarantined due to apply failure"
            );
        }
        q.len() >= self.halt_threshold
    }

    pub fn is_quarantined(&self, ns: Namespace) -> bool {
        self.quarantined.read().unwrap().contains(&ns)
    }

    pub fn quarantined_count(&self) -> usize {
        self.quarantined.read().unwrap().len()
    }

    /// Clear a namespace from quarantine (e.g., after snapshot repair).
    pub fn clear(&self, ns: Namespace) {
        let mut q = self.quarantined.write().unwrap();
        if q.remove(&ns) {
            tracing::info!(namespace = ?ns, "Namespace cleared from quarantine");
        }
    }

    #[cfg(test)]
    pub fn clear_all(&self) {
        self.quarantined.write().unwrap().clear();
    }
}

impl ControlStateMachine {
    pub fn new(store: Arc<dyn ControlPlaneStore>) -> Self {
        // Default threshold: quarantine up to 2 namespaces before halting.
        // With 4 application namespaces (ShardMap, MvDef, MvData, SqState),
        // losing 2 means half the control plane is diverged — unsafe to continue.
        Self::new_with_quarantine_threshold(store, 2)
    }

    pub fn new_with_quarantine_threshold(
        store: Arc<dyn ControlPlaneStore>,
        halt_threshold: usize,
    ) -> Self {
        Self {
            store,
            quarantine: QuarantineTracker::new(halt_threshold),
            ontology_tx: None,
        }
    }

    /// Attach an ontology-activation channel. Committed `DomainDef` Put/Delete
    /// commands are emitted here after their store write-through so the app layer
    /// can activate them. Builder-style.
    pub fn with_ontology_activation(
        mut self,
        tx: tokio::sync::mpsc::UnboundedSender<OntologyActivation>,
    ) -> Self {
        self.ontology_tx = Some(tx);
        self
    }

    fn load_applied(&self) -> AppliedState {
        match self.store.get(Namespace::RaftMeta, APPLIED_KEY) {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap_or_default(),
            _ => AppliedState::default(),
        }
    }

    fn store_applied(&self, state: &AppliedState) -> Result<(), StorageError<ControlNodeId>> {
        let bytes = serde_json::to_vec(state).map_err(sm_write_err)?;
        self.store
            .put(Namespace::RaftMeta, APPLIED_KEY, &bytes)
            .map_err(sm_write_err)
    }

    /// Apply a single control command to the store. A command with an
    /// unrecognized namespace is a no-op rather than a hard error, so a newer
    /// node's command doesn't crash an older applier.
    ///
    /// A2-7: Extended to handle high-level ShardMap operations — failover reads
    /// the current map from the store, mutates it, and writes it back, all
    /// atomically within this apply call.
    ///
    /// A6: Commands targeting a quarantined namespace are skipped with a warning.
    /// Apply failures quarantine the namespace; if the quarantine count exceeds
    /// the threshold, returns Err(StorageError) to halt the state machine.
    fn apply_command(
        &self,
        cmd: &ControlCommand,
    ) -> Result<ControlResponse, StorageError<ControlNodeId>> {
        match cmd {
            ControlCommand::Put {
                namespace,
                key,
                value,
            } => {
                match ControlCommand::resolve_namespace(namespace) {
                    Some(ns) => {
                        // A6: Skip if already quarantined.
                        if self.quarantine.is_quarantined(ns) {
                            tracing::warn!(
                                namespace = ?ns,
                                key = %key,
                                "Skipping Put on quarantined namespace"
                            );
                            return Ok(ControlResponse::Error {
                                message: format!("namespace {:?} is quarantined", ns),
                            });
                        }
                        match self.store.put(ns, key, value) {
                            Ok(_) => {
                                // Emit ontology activation after the durable write.
                                if ns == Namespace::DomainDef {
                                    if let Some(tx) = &self.ontology_tx {
                                        let _ = tx.send(OntologyActivation::Put {
                                            domain: key.clone(),
                                            pkg_json: value.clone(),
                                        });
                                    }
                                }
                                Ok(ControlResponse::Applied)
                            }
                            Err(e) => {
                                let reason = format!("Put failed: {}", e);
                                tracing::error!(error = %e, namespace = ?ns, "Put failed");
                                // A6: Quarantine on failure.
                                if self.quarantine.quarantine(ns, &reason) {
                                    return Err(sm_write_err(format!(
                                        "Quarantine threshold exceeded ({} namespaces), halting state machine",
                                        self.quarantine.quarantined_count()
                                    )));
                                }
                                Ok(ControlResponse::Error {
                                    message: e.to_string(),
                                })
                            }
                        }
                    }
                    None => Ok(ControlResponse::Error {
                        message: format!("unknown namespace: {}", namespace),
                    }),
                }
            }
            ControlCommand::Delete { namespace, key } => {
                match ControlCommand::resolve_namespace(namespace) {
                    Some(ns) => {
                        // A6: Skip if already quarantined.
                        if self.quarantine.is_quarantined(ns) {
                            tracing::warn!(
                                namespace = ?ns,
                                key = %key,
                                "Skipping Delete on quarantined namespace"
                            );
                            return Ok(ControlResponse::Error {
                                message: format!("namespace {:?} is quarantined", ns),
                            });
                        }
                        match self.store.delete(ns, key) {
                            Ok(_) => {
                                if ns == Namespace::DomainDef {
                                    if let Some(tx) = &self.ontology_tx {
                                        let _ = tx.send(OntologyActivation::Delete {
                                            domain: key.clone(),
                                        });
                                    }
                                }
                                Ok(ControlResponse::Applied)
                            }
                            Err(e) => {
                                let reason = format!("Delete failed: {}", e);
                                tracing::error!(error = %e, namespace = ?ns, "Delete failed");
                                // A6: Quarantine on failure.
                                if self.quarantine.quarantine(ns, &reason) {
                                    return Err(sm_write_err(format!(
                                        "Quarantine threshold exceeded ({} namespaces), halting state machine",
                                        self.quarantine.quarantined_count()
                                    )));
                                }
                                Ok(ControlResponse::Error {
                                    message: e.to_string(),
                                })
                            }
                        }
                    }
                    None => Ok(ControlResponse::Error {
                        message: format!("unknown namespace: {}", namespace),
                    }),
                }
            }
            ControlCommand::FailoverShard {
                shard_id,
                new_owner,
            } => Ok(self.apply_failover_shard(*shard_id, new_owner.clone())),
            ControlCommand::FailoverShardAuto { shard_id } => {
                Ok(self.apply_failover_shard_auto(*shard_id))
            }
            ControlCommand::ProposeShardMap { proposed } => {
                Ok(self.apply_propose_shard_map(proposed))
            }
        }
    }

    /// Apply a FailoverShard command: read current ShardMap, bump the shard's
    /// epoch, reassign owner, write back.
    fn apply_failover_shard(&self, shard_id: u32, new_owner: String) -> ControlResponse {
        use crate::shard_map::ShardMap;
        const MAP_KEY: &str = "current";

        let mut map: ShardMap = match self.load_shard_map(MAP_KEY) {
            Ok(m) => m,
            Err(msg) => return ControlResponse::Error { message: msg },
        };

        let Some(assignment) = map.assignments.get_mut(&(shard_id as usize)) else {
            return ControlResponse::Error {
                message: format!("shard {} not found", shard_id),
            };
        };

        let new_epoch = assignment.epoch.next();
        assignment.owner = new_owner.clone();
        assignment.epoch = new_epoch;
        assignment.writable = true;
        map.version += 1;

        if let Err(e) = self.save_shard_map(MAP_KEY, &map) {
            return ControlResponse::Error { message: e };
        }

        tracing::warn!(
            "Shard {} failed over to {} (epoch {})",
            shard_id,
            new_owner,
            new_epoch.value()
        );
        ControlResponse::FailoverToken {
            shard_id,
            epoch: new_epoch.value(),
        }
    }

    /// Apply a FailoverShardAuto command: promote the first surviving replica,
    /// or mark the shard unavailable if no replica is alive. The health check
    /// is NOT performed here (the state machine has no health map); the caller
    /// must have already filtered the replica list before submitting the command.
    fn apply_failover_shard_auto(&self, shard_id: u32) -> ControlResponse {
        use crate::shard_map::ShardMap;
        const MAP_KEY: &str = "current";

        let mut map: ShardMap = match self.load_shard_map(MAP_KEY) {
            Ok(m) => m,
            Err(msg) => return ControlResponse::Error { message: msg },
        };

        let Some(assignment) = map.assignments.get_mut(&(shard_id as usize)) else {
            return ControlResponse::Error {
                message: format!("shard {} not found", shard_id),
            };
        };

        // The caller (ControlPlane) has already filtered replicas; if the list
        // is non-empty, promote the first. If empty, mark unavailable.
        let response = if let Some(new_owner) = assignment.replicas.first().cloned() {
            let new_epoch = assignment.epoch.next();
            assignment.replicas.retain(|r| r != &new_owner);
            assignment.owner = new_owner.clone();
            assignment.epoch = new_epoch;
            assignment.writable = true;
            map.version += 1;
            tracing::warn!(
                "Shard {} auto-failed over to replica {} (epoch {})",
                shard_id,
                new_owner,
                new_epoch.value()
            );
            ControlResponse::FailoverPromoted {
                shard_id,
                epoch: new_epoch.value(),
            }
        } else {
            assignment.writable = false;
            map.version += 1;
            tracing::error!(
                "Shard {} has no surviving replica; marked unavailable",
                shard_id
            );
            ControlResponse::FailoverNoReplica { shard_id }
        };

        if let Err(e) = self.save_shard_map(MAP_KEY, &map) {
            return ControlResponse::Error { message: e };
        }

        response
    }

    /// Apply a ProposeShardMap command: deserialize and validate the proposed
    /// map, accept it if the version is newer.
    fn apply_propose_shard_map(&self, proposed_bytes: &[u8]) -> ControlResponse {
        use crate::shard_map::ShardMap;
        const MAP_KEY: &str = "current";

        let proposed: ShardMap = match serde_json::from_slice(proposed_bytes) {
            Ok(m) => m,
            Err(e) => {
                return ControlResponse::Error {
                    message: format!("invalid ShardMap: {}", e),
                }
            }
        };

        let current = match self.load_shard_map(MAP_KEY) {
            Ok(m) => m,
            Err(msg) => return ControlResponse::Error { message: msg },
        };

        if proposed.version <= current.version {
            return ControlResponse::Error {
                message: format!(
                    "stale proposal: version {} <= current {}",
                    proposed.version, current.version
                ),
            };
        }

        if let Err(e) = self.save_shard_map(MAP_KEY, &proposed) {
            return ControlResponse::Error { message: e };
        }

        tracing::info!("ShardMap committed at version {}", proposed.version);
        ControlResponse::MapAccepted {
            version: proposed.version,
        }
    }

    fn load_shard_map(&self, key: &str) -> Result<crate::shard_map::ShardMap, String> {
        let bytes = self
            .store
            .get(Namespace::ShardMap, key)
            .map_err(|e| format!("failed to read ShardMap: {}", e))?
            .ok_or_else(|| "ShardMap not found".to_string())?;
        serde_json::from_slice(&bytes).map_err(|e| format!("invalid ShardMap: {}", e))
    }

    fn save_shard_map(&self, key: &str, map: &crate::shard_map::ShardMap) -> Result<(), String> {
        let bytes = serde_json::to_vec(map).map_err(|e| format!("ShardMap serialize: {}", e))?;
        self.store
            .put(Namespace::ShardMap, key, &bytes)
            .map_err(|e| format!("ShardMap write: {}", e))
    }
}

/// Map a store write failure to an openraft `StorageError` on the write path.
fn sm_write_err(e: impl std::fmt::Display) -> StorageError<ControlNodeId> {
    openraft::StorageIOError::write_state_machine(openraft::AnyError::error(e)).into()
}

impl RaftSnapshotBuilder<ControlRaftTypeConfig> for ControlStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ControlRaftTypeConfig>, StorageError<ControlNodeId>> {
        let applied = self.load_applied();
        let data = self.store.snapshot().map_err(sm_read_err)?;

        let snapshot_id = match &applied.last_applied {
            Some(log_id) => format!("{}-{}", log_id.leader_id, log_id.index),
            None => "0-0".to_string(),
        };
        let meta = SnapshotMeta {
            last_log_id: applied.last_applied,
            last_membership: applied.membership.clone(),
            snapshot_id,
        };

        // Persist the snapshot so get_current_snapshot returns it after restart.
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let stored_bytes = serde_json::to_vec(&stored).map_err(sm_write_err)?;
        self.store
            .put(Namespace::RaftMeta, SNAPSHOT_KEY, &stored_bytes)
            .map_err(sm_write_err)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// Map a store read failure to an openraft `StorageError` on the read path.
fn sm_read_err(e: impl std::fmt::Display) -> StorageError<ControlNodeId> {
    openraft::StorageIOError::read_state_machine(openraft::AnyError::error(e)).into()
}

impl RaftStateMachine<ControlRaftTypeConfig> for ControlStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<ControlNodeId>>,
            StoredMembership<ControlNodeId, openraft::BasicNode>,
        ),
        StorageError<ControlNodeId>,
    > {
        let applied = self.load_applied();
        Ok((applied.last_applied, applied.membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ControlResponse>, StorageError<ControlNodeId>>
    where
        I: IntoIterator<Item = Entry<ControlRaftTypeConfig>>,
    {
        let mut applied = self.load_applied();
        let mut responses = Vec::new();

        for entry in entries {
            // A6: Advance last_applied BEFORE executing the command. This is
            // critical for Raft consistency — even if the command is quarantined
            // or fails, the index must advance so Raft doesn't retry infinitely.
            // Quarantine is a local apply-layer concern; it does NOT block log
            // replication or index progression.
            applied.last_applied = Some(entry.log_id);

            match entry.payload {
                // Blank entries (new-leader markers) and membership entries carry
                // no business data — record the membership, respond with a
                // default (no-op) result.
                EntryPayload::Blank => responses.push(ControlResponse::default()),
                EntryPayload::Membership(m) => {
                    applied.membership = StoredMembership::new(Some(entry.log_id), m);
                    responses.push(ControlResponse::default());
                }
                EntryPayload::Normal(cmd) => {
                    // A6: apply_command can now return Err if quarantine threshold
                    // is exceeded, which halts the state machine.
                    match self.apply_command(&cmd) {
                        Ok(response) => responses.push(response),
                        Err(e) => {
                            // Halt threshold exceeded. Persist applied-tracking for
                            // the entries we successfully processed, then propagate
                            // the error to stop the state machine.
                            self.store_applied(&applied)?;
                            return Err(e);
                        }
                    }
                }
            }
        }

        // Persist applied-tracking after the batch. The store's own writes are
        // fsync'd; this records how far we've applied for restart recovery.
        self.store_applied(&applied)?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<ControlNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<ControlNodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<ControlNodeId>> {
        let data = snapshot.into_inner();

        // Replace application state with the snapshot's contents.
        self.store.restore(&data).map_err(sm_write_err)?;

        // Adopt the snapshot's applied log id + membership as our tracking.
        let applied = AppliedState {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
        };
        self.store_applied(&applied)?;

        // Persist the installed snapshot so get_current_snapshot returns it.
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        let stored_bytes = serde_json::to_vec(&stored).map_err(sm_write_err)?;
        self.store
            .put(Namespace::RaftMeta, SNAPSHOT_KEY, &stored_bytes)
            .map_err(sm_write_err)?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ControlRaftTypeConfig>>, StorageError<ControlNodeId>> {
        match self
            .store
            .get(Namespace::RaftMeta, SNAPSHOT_KEY)
            .map_err(sm_read_err)?
        {
            Some(bytes) => {
                let stored: StoredSnapshot = serde_json::from_slice(&bytes).map_err(sm_read_err)?;
                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_core::control_plane_store::InMemoryControlPlaneStore;

    fn sm() -> ControlStateMachine {
        ControlStateMachine::new(Arc::new(InMemoryControlPlaneStore::new()))
    }

    fn entry(index: u64, cmd: ControlCommand) -> Entry<ControlRaftTypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn apply_put_writes_through_to_store() {
        let mut m = sm();
        let resp = m
            .apply([entry(
                1,
                ControlCommand::put(Namespace::ShardMap, "current", b"map".to_vec()),
            )])
            .await
            .unwrap();
        assert_eq!(resp.len(), 1);
        assert!(matches!(resp[0], ControlResponse::Applied));
        // Value is now readable through the store.
        assert_eq!(
            m.store.get(Namespace::ShardMap, "current").unwrap(),
            Some(b"map".to_vec())
        );
        // applied_state advanced.
        let (last, _) = m.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);
    }

    #[tokio::test]
    async fn apply_delete_removes_key() {
        let mut m = sm();
        m.apply([entry(
            1,
            ControlCommand::put(Namespace::MvDef, "v1", b"def".to_vec()),
        )])
        .await
        .unwrap();
        m.apply([entry(2, ControlCommand::delete(Namespace::MvDef, "v1"))])
            .await
            .unwrap();
        assert_eq!(m.store.get(Namespace::MvDef, "v1").unwrap(), None);
    }

    #[tokio::test]
    async fn snapshot_then_restore_into_fresh_sm() {
        let mut src = sm();
        src.apply([
            entry(
                1,
                ControlCommand::put(Namespace::ShardMap, "current", b"map-v7".to_vec()),
            ),
            entry(
                2,
                ControlCommand::put(Namespace::SqState, "state", b"sq".to_vec()),
            ),
        ])
        .await
        .unwrap();

        let snap = src.build_snapshot().await.unwrap();

        // Install into a fresh state machine.
        let mut dst = sm();
        dst.install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();
        assert_eq!(
            dst.store.get(Namespace::ShardMap, "current").unwrap(),
            Some(b"map-v7".to_vec())
        );
        assert_eq!(
            dst.store.get(Namespace::SqState, "state").unwrap(),
            Some(b"sq".to_vec())
        );
        // Applied log id carried across.
        let (last, _) = dst.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 2);
    }

    #[tokio::test]
    async fn get_current_snapshot_returns_built_snapshot() {
        let mut m = sm();
        m.apply([entry(
            1,
            ControlCommand::put(Namespace::ShardMap, "current", b"x".to_vec()),
        )])
        .await
        .unwrap();
        assert!(m.get_current_snapshot().await.unwrap().is_none());
        m.build_snapshot().await.unwrap();
        assert!(m.get_current_snapshot().await.unwrap().is_some());
    }

    // A6 quarantine tests
    use nexora_core::control_plane_store::ControlStoreError;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A mock store that fails writes to specific namespaces on demand.
    struct FailingStore {
        inner: InMemoryControlPlaneStore,
        fail_mvdef: Arc<AtomicBool>,
        fail_sqstate: Arc<AtomicBool>,
    }

    impl FailingStore {
        fn new() -> Self {
            Self {
                inner: InMemoryControlPlaneStore::new(),
                fail_mvdef: Arc::new(AtomicBool::new(false)),
                fail_sqstate: Arc::new(AtomicBool::new(false)),
            }
        }

        fn set_fail_mvdef(&self, fail: bool) {
            self.fail_mvdef.store(fail, Ordering::SeqCst);
        }

        fn set_fail_sqstate(&self, fail: bool) {
            self.fail_sqstate.store(fail, Ordering::SeqCst);
        }
    }

    impl ControlPlaneStore for FailingStore {
        fn put(
            &self,
            namespace: Namespace,
            key: &str,
            value: &[u8],
        ) -> Result<(), ControlStoreError> {
            if namespace == Namespace::MvDef && self.fail_mvdef.load(Ordering::SeqCst) {
                return Err(ControlStoreError::Backend("simulated MvDef failure".into()));
            }
            if namespace == Namespace::SqState && self.fail_sqstate.load(Ordering::SeqCst) {
                return Err(ControlStoreError::Backend(
                    "simulated SqState failure".into(),
                ));
            }
            self.inner.put(namespace, key, value)
        }

        fn get(
            &self,
            namespace: Namespace,
            key: &str,
        ) -> Result<Option<Vec<u8>>, ControlStoreError> {
            self.inner.get(namespace, key)
        }

        fn delete(&self, namespace: Namespace, key: &str) -> Result<(), ControlStoreError> {
            if namespace == Namespace::MvDef && self.fail_mvdef.load(Ordering::SeqCst) {
                return Err(ControlStoreError::Backend("simulated MvDef failure".into()));
            }
            if namespace == Namespace::SqState && self.fail_sqstate.load(Ordering::SeqCst) {
                return Err(ControlStoreError::Backend(
                    "simulated SqState failure".into(),
                ));
            }
            self.inner.delete(namespace, key)
        }

        fn list(&self, namespace: Namespace) -> Result<Vec<(String, Vec<u8>)>, ControlStoreError> {
            self.inner.list(namespace)
        }

        fn is_durable(&self) -> bool {
            false
        }

        fn snapshot(&self) -> Result<Vec<u8>, ControlStoreError> {
            self.inner.snapshot()
        }

        fn restore(&self, data: &[u8]) -> Result<(), ControlStoreError> {
            self.inner.restore(data)
        }
    }

    #[tokio::test]
    async fn quarantine_isolates_single_namespace() {
        let store = Arc::new(FailingStore::new());
        let mut m = ControlStateMachine::new_with_quarantine_threshold(store.clone(), 3);

        // MvDef write succeeds initially.
        let resp = m
            .apply([entry(
                1,
                ControlCommand::put(Namespace::MvDef, "v1", b"def".to_vec()),
            )])
            .await
            .unwrap();
        assert!(matches!(resp[0], ControlResponse::Applied));

        // Now make MvDef fail.
        store.set_fail_mvdef(true);
        let resp = m
            .apply([entry(
                2,
                ControlCommand::put(Namespace::MvDef, "v2", b"def2".to_vec()),
            )])
            .await
            .unwrap();
        // Returns error response but doesn't halt (threshold not exceeded).
        assert!(matches!(resp[0], ControlResponse::Error { .. }));
        assert_eq!(m.quarantine.quarantined_count(), 1);

        // MvDef is quarantined: subsequent commands are skipped.
        let resp = m
            .apply([entry(
                3,
                ControlCommand::put(Namespace::MvDef, "v3", b"def3".to_vec()),
            )])
            .await
            .unwrap();
        assert!(matches!(resp[0], ControlResponse::Error { .. }));

        // Other namespaces still work.
        let resp = m
            .apply([entry(
                4,
                ControlCommand::put(Namespace::SqState, "state", b"sq".to_vec()),
            )])
            .await
            .unwrap();
        assert!(matches!(resp[0], ControlResponse::Applied));
        assert_eq!(
            m.store.get(Namespace::SqState, "state").unwrap(),
            Some(b"sq".to_vec())
        );

        // Applied index advanced through all entries (even quarantined ones).
        let (last, _) = m.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 4);
    }

    #[tokio::test]
    async fn quarantine_threshold_triggers_halt() {
        let store = Arc::new(FailingStore::new());
        // Threshold = 2: halts when 2 namespaces are quarantined.
        let mut m = ControlStateMachine::new_with_quarantine_threshold(store.clone(), 2);

        // Fail MvDef.
        store.set_fail_mvdef(true);
        let resp = m
            .apply([entry(
                1,
                ControlCommand::put(Namespace::MvDef, "v1", b"x".to_vec()),
            )])
            .await
            .unwrap();
        assert!(matches!(resp[0], ControlResponse::Error { .. }));
        assert_eq!(m.quarantine.quarantined_count(), 1);

        // Fail SqState — threshold reached, state machine halts.
        store.set_fail_sqstate(true);
        let result = m
            .apply([entry(
                2,
                ControlCommand::put(Namespace::SqState, "s1", b"y".to_vec()),
            )])
            .await;
        assert!(result.is_err());
        assert_eq!(m.quarantine.quarantined_count(), 2);

        // Applied index advanced to entry 2 before the halt.
        let (last, _) = m.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 2);
    }

    #[tokio::test]
    async fn quarantined_namespace_skips_subsequent_applies() {
        let store = Arc::new(FailingStore::new());
        let mut m = ControlStateMachine::new_with_quarantine_threshold(store.clone(), 3);

        // Quarantine MvDef.
        store.set_fail_mvdef(true);
        let resp = m
            .apply([entry(
                1,
                ControlCommand::put(Namespace::MvDef, "v1", b"x".to_vec()),
            )])
            .await
            .unwrap();
        assert!(matches!(resp[0], ControlResponse::Error { .. }));

        // Subsequent MvDef commands are skipped.
        let resp = m
            .apply([
                entry(
                    2,
                    ControlCommand::put(Namespace::MvDef, "v2", b"y".to_vec()),
                ),
                entry(3, ControlCommand::delete(Namespace::MvDef, "v1")),
            ])
            .await
            .unwrap();
        assert_eq!(resp.len(), 2);
        assert!(matches!(resp[0], ControlResponse::Error { .. }));
        assert!(matches!(resp[1], ControlResponse::Error { .. }));
        // Both contain "quarantined" in the message.
        if let ControlResponse::Error { message } = &resp[0] {
            assert!(message.contains("quarantined"));
        }

        // Applied index still advanced.
        let (last, _) = m.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 3);
    }

    #[tokio::test]
    async fn clear_restores_namespace() {
        let store = Arc::new(FailingStore::new());
        let mut m = ControlStateMachine::new_with_quarantine_threshold(store.clone(), 3);

        // Quarantine MvDef.
        store.set_fail_mvdef(true);
        m.apply([entry(
            1,
            ControlCommand::put(Namespace::MvDef, "v1", b"x".to_vec()),
        )])
        .await
        .unwrap();
        assert_eq!(m.quarantine.quarantined_count(), 1);

        // Clear the quarantine and fix the store.
        m.quarantine.clear(Namespace::MvDef);
        store.set_fail_mvdef(false);
        assert_eq!(m.quarantine.quarantined_count(), 0);

        // MvDef now applies successfully.
        let resp = m
            .apply([entry(
                2,
                ControlCommand::put(Namespace::MvDef, "v2", b"restored".to_vec()),
            )])
            .await
            .unwrap();
        assert!(matches!(resp[0], ControlResponse::Applied));
        assert_eq!(
            m.store.get(Namespace::MvDef, "v2").unwrap(),
            Some(b"restored".to_vec())
        );
    }
}
