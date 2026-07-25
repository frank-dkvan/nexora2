//! Durability-ordering regression test for the sleep/checkpoint path.
//!
//! Fix under test: `sleep_node` must force the durable backend's writes to
//! disk (`flush_durable`) BEFORE it appends the WAL `SnapshotCheckpoint` that
//! lets recovery skip the events the snapshot subsumes. Without that ordering,
//! a crash after the checkpoint reached disk but before the (unsynced) journal
//! and snapshot writes did would make recovery skip events that were never
//! persisted — silent loss of acknowledged data.
//!
//! We cannot simulate real power loss in a unit test (unsynced writes still sit
//! in the OS page cache, which survives a plain process exit), so a naive
//! drop+reopen check would pass even without the fsync barrier and give false
//! confidence. Instead we assert the *observable contract* the fix adds: the
//! `flush_durable` barrier is invoked, and it is invoked AFTER the snapshot /
//! journal writes it is meant to make durable — i.e. the barrier can only be
//! crossed once the state it covers has been handed to the durable layer.

use async_trait::async_trait;
use nexora_core::event::{DomainIndexEvent, NodeChangeEvent, TimedEvent};
use nexora_core::persistor::{NamespacedPersistenceAgent, PersistenceError};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use std::sync::{Arc, Mutex};

/// A recorded persistence call, in the order the shard issued it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    PersistEvents,
    PersistSnapshot,
    FlushDurable,
}

/// Persistor spy: delegates every operation to an inner `InMemoryPersistor`
/// while recording the order of the calls that matter to the durability
/// barrier. Only the three relevant hooks push to the log; the rest just
/// delegate so the graph behaves normally.
struct RecordingPersistor {
    inner: InMemoryPersistor,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl RecordingPersistor {
    fn new(calls: Arc<Mutex<Vec<Call>>>) -> Self {
        Self {
            inner: InMemoryPersistor::new(),
            calls,
        }
    }
}

#[async_trait]
impl NamespacedPersistenceAgent for RecordingPersistor {
    fn namespace(&self) -> &str {
        self.inner.namespace()
    }

    async fn persist_node_change_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<NodeChangeEvent>>,
    ) -> Result<(), PersistenceError> {
        self.calls.lock().unwrap().push(Call::PersistEvents);
        self.inner.persist_node_change_events(qid, events).await
    }

    async fn get_node_change_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<NodeChangeEvent>>, PersistenceError> {
        self.inner.get_node_change_events(qid, start, end).await
    }

    async fn delete_node_change_events(&self, qid: NexoraId) -> Result<(), PersistenceError> {
        self.inner.delete_node_change_events(qid).await
    }

    async fn persist_domain_index_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<DomainIndexEvent>>,
    ) -> Result<(), PersistenceError> {
        self.inner.persist_domain_index_events(qid, events).await
    }

    async fn get_domain_index_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<DomainIndexEvent>>, PersistenceError> {
        self.inner.get_domain_index_events(qid, start, end).await
    }

    async fn persist_snapshot(
        &self,
        qid: NexoraId,
        time: EventTime,
        snapshot: Vec<u8>,
    ) -> Result<(), PersistenceError> {
        self.calls.lock().unwrap().push(Call::PersistSnapshot);
        self.inner.persist_snapshot(qid, time, snapshot).await
    }

    async fn get_latest_snapshot(
        &self,
        qid: NexoraId,
        up_to: EventTime,
    ) -> Result<Option<(EventTime, Vec<u8>)>, PersistenceError> {
        self.inner.get_latest_snapshot(qid, up_to).await
    }

    async fn delete_snapshots(&self, qid: NexoraId) -> Result<(), PersistenceError> {
        self.inner.delete_snapshots(qid).await
    }

    async fn enumerate_journal_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError> {
        self.inner.enumerate_journal_node_ids().await
    }

    async fn enumerate_snapshot_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError> {
        self.inner.enumerate_snapshot_node_ids().await
    }

    async fn flush_durable(&self) -> Result<(), PersistenceError> {
        self.calls.lock().unwrap().push(Call::FlushDurable);
        self.inner.flush_durable().await
    }

    async fn shutdown(&self) -> Result<(), PersistenceError> {
        self.inner.shutdown().await
    }
}

fn config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 1,
        max_nodes_per_shard: 64,
        node_channel_size: 16,
    }
}

/// sleep_node must call `flush_durable` after the snapshot/journal writes it is
/// meant to make durable — proving the WAL checkpoint (written next) can only
/// be crossed once that state has reached the durable layer.
#[tokio::test]
async fn sleep_node_flushes_durable_after_persisting_state() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let persistor = Arc::new(RecordingPersistor::new(calls.clone()));
    let wal_dir = tempfile::tempdir().unwrap();

    // A WAL must be configured, otherwise the SnapshotCheckpoint path (and its
    // durability barrier) is skipped entirely.
    let svc = GraphService::new_with_wal(config(), persistor, wal_dir.path().to_path_buf(), None)
        .expect("WAL-backed GraphService");

    let qid = NexoraId::from_bytes(b"node-1".to_vec());
    svc.set_property(&qid, Symbol::new("k").as_str(), PropertyValue::Integer(1))
        .await
        .unwrap();

    svc.sleep_node(&qid).await.unwrap();

    let recorded = calls.lock().unwrap().clone();

    // The barrier must have run at all.
    assert!(
        recorded.contains(&Call::FlushDurable),
        "sleep_node must invoke flush_durable before the WAL checkpoint; calls: {recorded:?}"
    );

    // The barrier must come AFTER the last state write it is meant to flush:
    // no PersistSnapshot / PersistEvents may appear after the FlushDurable.
    let flush_pos = recorded
        .iter()
        .position(|c| *c == Call::FlushDurable)
        .expect("flush_durable recorded");
    let last_write_pos = recorded
        .iter()
        .rposition(|c| matches!(c, Call::PersistSnapshot | Call::PersistEvents))
        .expect("state was persisted before sleep");
    assert!(
        flush_pos > last_write_pos,
        "flush_durable (idx {flush_pos}) must run AFTER the snapshot/journal writes \
         (last write idx {last_write_pos}) so the checkpoint cannot outrun durable state; \
         calls: {recorded:?}"
    );
}
