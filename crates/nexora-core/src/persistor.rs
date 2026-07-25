//! Persistence layer — the `NamespacedPersistenceAgent` trait and supporting types.
//!
//! This module defines the persistence abstraction that all storage backends
//! must implement. The trait hierarchy mirrors the reference implementation:
//!
//! - `NamespacedPersistenceAgent`: Core CRUD for events, snapshots, SQ state
//! - `PersistenceAgent`: Extends above with global metadata and DGN storage
//!
//! Implementations are in separate crates (nexora-persistor-rocksdb, etc.)

use crate::event::{DomainIndexEvent, NodeChangeEvent, TimedEvent};
use nexora_id::{EventTime, NexoraId};
use std::collections::HashMap;

type SnapshotStore = HashMap<NexoraId, Vec<(EventTime, Vec<u8>)>>;

/// Core persistence interface scoped to a single namespace.
///
/// All methods are async. Implementations must be `Send + Sync` to support
/// concurrent access from multiple NodeTasks.
#[async_trait::async_trait]
pub trait NamespacedPersistenceAgent: Send + Sync {
    /// The namespace this agent is scoped to.
    fn namespace(&self) -> &str;

    // ====== Node Change Events (journal) ======

    /// Append events to the journal for a node.
    async fn persist_node_change_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<NodeChangeEvent>>,
    ) -> Result<(), PersistenceError>;

    /// Get events for a node within a time range.
    async fn get_node_change_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<NodeChangeEvent>>, PersistenceError>;

    /// Delete all events for a node.
    async fn delete_node_change_events(&self, qid: NexoraId) -> Result<(), PersistenceError>;

    // ====== Domain Index Events ======

    /// Append domain index events.
    async fn persist_domain_index_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<DomainIndexEvent>>,
    ) -> Result<(), PersistenceError>;

    /// Get domain index events within a time range.
    async fn get_domain_index_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<DomainIndexEvent>>, PersistenceError>;

    // ====== Snapshots ======

    /// Persist a node snapshot (serialized node state).
    async fn persist_snapshot(
        &self,
        qid: NexoraId,
        time: EventTime,
        snapshot: Vec<u8>,
    ) -> Result<(), PersistenceError>;

    /// Get the latest snapshot for a node at or before the given time.
    async fn get_latest_snapshot(
        &self,
        qid: NexoraId,
        up_to: EventTime,
    ) -> Result<Option<(EventTime, Vec<u8>)>, PersistenceError>;

    /// Delete all snapshots for a node.
    async fn delete_snapshots(&self, qid: NexoraId) -> Result<(), PersistenceError>;

    // ====== Enumeration ======

    /// List all node IDs that have journal entries.
    async fn enumerate_journal_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError>;

    /// List all node IDs that have snapshots.
    async fn enumerate_snapshot_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError>;

    // ====== Durability barrier ======

    /// Flush all prior writes durably to stable storage (fsync).
    ///
    /// This is a durability barrier: when it returns `Ok`, every `persist_*`
    /// call that completed before it is guaranteed on disk. Callers that record
    /// an external durability marker — e.g. a WAL `SnapshotCheckpoint` that lets
    /// recovery *skip* the events this snapshot subsumes — MUST call this first,
    /// otherwise a crash between the (unsynced) data write and the marker leaves
    /// recovery skipping events that were never actually persisted.
    ///
    /// The default is a no-op, correct for in-memory backends that have no
    /// separate durable tier. Durable backends must override it.
    async fn flush_durable(&self) -> Result<(), PersistenceError> {
        Ok(())
    }

    // ====== Shutdown ======

    /// Gracefully shut down the persistence agent.
    async fn shutdown(&self) -> Result<(), PersistenceError>;
}

/// Errors that can occur during persistence operations.
///
/// This enum covers the full range of failures: I/O, serialization,
/// not-found, conflict, and backend-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// Underlying I/O error from the storage layer.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Failed to serialize or deserialize event/snapshot data.
    #[error("serialization error: {0}")]
    Serialization(String),
    /// The requested data was not found in the store.
    #[error("not found: {0}")]
    NotFound(String),
    /// A concurrent modification conflict was detected.
    #[error("conflict: {0}")]
    Conflict(String),
    /// An error from the storage backend (e.g., RocksDB, LMDB).
    #[error("backend error: {0}")]
    Backend(String),
}

/// In-memory persistence agent — for testing and Lite mode.
///
/// All data is stored in memory and lost on process exit.
pub struct InMemoryPersistor {
    namespace: String,
    journals: tokio::sync::RwLock<HashMap<NexoraId, Vec<TimedEvent<NodeChangeEvent>>>>,
    domain_events: tokio::sync::RwLock<HashMap<NexoraId, Vec<TimedEvent<DomainIndexEvent>>>>,
    snapshots: tokio::sync::RwLock<SnapshotStore>,
}

impl InMemoryPersistor {
    /// Create a new empty in-memory persistence agent.
    ///
    /// The agent is scoped to the `"default"` namespace. All data is stored
    /// in `HashMap`s behind `RwLock`s and is lost when the process exits.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use nexora_core::persistor::{InMemoryPersistor, NamespacedPersistenceAgent};
    /// let persistor = InMemoryPersistor::new();
    /// assert_eq!(persistor.namespace(), "default");
    /// ```
    pub fn new() -> Self {
        Self {
            namespace: "default".to_string(),
            journals: tokio::sync::RwLock::new(HashMap::new()),
            domain_events: tokio::sync::RwLock::new(HashMap::new()),
            snapshots: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryPersistor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl NamespacedPersistenceAgent for InMemoryPersistor {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    async fn persist_node_change_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<NodeChangeEvent>>,
    ) -> Result<(), PersistenceError> {
        let mut journals = self.journals.write().await;
        journals.entry(qid).or_default().extend(events);
        Ok(())
    }

    async fn get_node_change_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<NodeChangeEvent>>, PersistenceError> {
        let journals = self.journals.read().await;
        Ok(journals
            .get(&qid)
            .map(|events| {
                events
                    .iter()
                    .filter(|e| {
                        start.is_none_or(|s| e.time >= s) && end.is_none_or(|e2| e.time <= e2)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn delete_node_change_events(&self, qid: NexoraId) -> Result<(), PersistenceError> {
        self.journals.write().await.remove(&qid);
        Ok(())
    }

    async fn persist_domain_index_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<DomainIndexEvent>>,
    ) -> Result<(), PersistenceError> {
        self.domain_events
            .write()
            .await
            .entry(qid)
            .or_default()
            .extend(events);
        Ok(())
    }

    async fn get_domain_index_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<DomainIndexEvent>>, PersistenceError> {
        let events = self.domain_events.read().await;
        Ok(events
            .get(&qid)
            .map(|evts| {
                evts.iter()
                    .filter(|e| {
                        start.is_none_or(|s| e.time >= s) && end.is_none_or(|e2| e.time <= e2)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn persist_snapshot(
        &self,
        qid: NexoraId,
        time: EventTime,
        snapshot: Vec<u8>,
    ) -> Result<(), PersistenceError> {
        self.snapshots
            .write()
            .await
            .entry(qid)
            .or_default()
            .push((time, snapshot));
        Ok(())
    }

    async fn get_latest_snapshot(
        &self,
        qid: NexoraId,
        up_to: EventTime,
    ) -> Result<Option<(EventTime, Vec<u8>)>, PersistenceError> {
        let snapshots = self.snapshots.read().await;
        Ok(snapshots.get(&qid).and_then(|snaps| {
            snaps
                .iter()
                .filter(|(t, _)| *t <= up_to)
                .max_by_key(|(t, _)| *t)
                .cloned()
        }))
    }

    async fn delete_snapshots(&self, qid: NexoraId) -> Result<(), PersistenceError> {
        self.snapshots.write().await.remove(&qid);
        Ok(())
    }

    async fn enumerate_journal_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError> {
        Ok(self.journals.read().await.keys().cloned().collect())
    }

    async fn enumerate_snapshot_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError> {
        Ok(self.snapshots.read().await.keys().cloned().collect())
    }

    async fn shutdown(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::NodeChangeEvent;
    use nexora_value::Symbol;

    #[tokio::test]
    async fn test_in_memory_persist_and_retrieve() {
        let persistor = InMemoryPersistor::new();
        let qid = NexoraId::new_random();

        let events = vec![TimedEvent::new(
            NodeChangeEvent::PropertySet {
                key: Symbol::new("speed"),
                value: nexora_id::PropertyValue::Float(12.5),
            },
            EventTime::from_micros(1000),
        )];

        persistor
            .persist_node_change_events(qid.clone(), events)
            .await
            .unwrap();

        let retrieved = persistor
            .get_node_change_events(qid, None, None)
            .await
            .unwrap();
        assert_eq!(retrieved.len(), 1);
    }

    #[tokio::test]
    async fn test_snapshot_roundtrip() {
        let persistor = InMemoryPersistor::new();
        let qid = NexoraId::new_random();

        let snapshot_data = vec![1, 2, 3, 4, 5];
        let time = EventTime::from_micros(5000);

        persistor
            .persist_snapshot(qid.clone(), time, snapshot_data.clone())
            .await
            .unwrap();

        let result = persistor
            .get_latest_snapshot(qid, EventTime::MAX)
            .await
            .unwrap();
        assert!(result.is_some());
        let (t, data) = result.unwrap();
        assert_eq!(t, time);
        assert_eq!(data, snapshot_data);
    }

    #[tokio::test]
    async fn test_snapshot_time_filter() {
        let persistor = InMemoryPersistor::new();
        let qid = NexoraId::new_random();

        persistor
            .persist_snapshot(qid.clone(), EventTime::from_micros(100), vec![1])
            .await
            .unwrap();
        persistor
            .persist_snapshot(qid.clone(), EventTime::from_micros(200), vec![2])
            .await
            .unwrap();

        // Query at time 150 — should get the first snapshot
        let result = persistor
            .get_latest_snapshot(qid.clone(), EventTime::from_micros(150))
            .await
            .unwrap();
        assert_eq!(result.unwrap().1, vec![1]);

        // Query at time 250 — should get the second snapshot
        let result = persistor
            .get_latest_snapshot(qid, EventTime::from_micros(250))
            .await
            .unwrap();
        assert_eq!(result.unwrap().1, vec![2]);
    }

    // ====== PA-002: 时间范围过滤 ======
    #[tokio::test]
    async fn test_time_range_filter() {
        let persistor = InMemoryPersistor::new();
        let qid = NexoraId::new_random();

        for i in 1..=10i64 {
            persistor
                .persist_node_change_events(
                    qid.clone(),
                    vec![TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("tick"),
                            value: nexora_id::PropertyValue::Integer(i),
                        },
                        EventTime::from_micros((i * 1000) as u64),
                    )],
                )
                .await
                .unwrap();
        }

        // 范围 [3000, 7000]
        let result = persistor
            .get_node_change_events(
                qid.clone(),
                Some(EventTime::from_micros(3000)),
                Some(EventTime::from_micros(7000)),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].time, EventTime::from_micros(3000));
        assert_eq!(result[4].time, EventTime::from_micros(7000));
    }

    // ====== PA-003: 删除事件 ======
    #[tokio::test]
    async fn test_delete_node_change_events() {
        let persistor = InMemoryPersistor::new();
        let qid = NexoraId::new_random();

        persistor
            .persist_node_change_events(
                qid.clone(),
                vec![TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: nexora_id::PropertyValue::Integer(1),
                    },
                    EventTime::from_micros(1000),
                )],
            )
            .await
            .unwrap();

        persistor
            .delete_node_change_events(qid.clone())
            .await
            .unwrap();

        let result = persistor
            .get_node_change_events(qid, None, None)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    // ====== PA-006: 枚举节点 ID ======
    #[tokio::test]
    async fn test_enumerate_journal_node_ids() {
        let persistor = InMemoryPersistor::new();
        let qid1 = NexoraId::new_random();
        let qid2 = NexoraId::new_random();

        persistor
            .persist_node_change_events(
                qid1.clone(),
                vec![TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("a"),
                        value: nexora_id::PropertyValue::Integer(1),
                    },
                    EventTime::from_micros(1000),
                )],
            )
            .await
            .unwrap();

        persistor
            .persist_node_change_events(
                qid2.clone(),
                vec![TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("b"),
                        value: nexora_id::PropertyValue::Integer(2),
                    },
                    EventTime::from_micros(2000),
                )],
            )
            .await
            .unwrap();

        let ids = persistor.enumerate_journal_node_ids().await.unwrap();
        assert_eq!(ids.len(), 2);
    }

    // ====== PA-007: namespace 默认值 ======
    #[tokio::test]
    async fn test_default_namespace() {
        let persistor = InMemoryPersistor::new();
        assert_eq!(persistor.namespace(), "default");
    }
}
