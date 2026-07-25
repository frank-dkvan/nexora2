use byteorder::{BigEndian, ByteOrder};
use nexora_core::event::{DomainIndexEvent, NodeChangeEvent, TimedEvent};
use nexora_core::persistor::{NamespacedPersistenceAgent, PersistenceError};
use nexora_id::{EventTime, NexoraId};
use nexora_serialization::EventCodec;
use rust_rocksdb::{ColumnFamilyDescriptor, DBCompressionType, Options, WriteBatch, DB};
use std::path::Path;
use std::sync::Arc;

/// Column family names (matching RocksDbPersistor).
const CF_NODE_EVENTS: &str = "node-events";
const CF_DOMAIN_INDEX_EVENTS: &str = "domain-index-events";
const CF_SNAPSHOTS: &str = "snapshots";
const CF_META_DATA: &str = "meta-data";
const NAMESPACE_KEY_MAGIC: &[u8; 3] = b"QN\x01";

/// All column family names used by this persistor.
const ALL_CF_NAMES: &[&str] = &[
    CF_NODE_EVENTS,
    CF_DOMAIN_INDEX_EVENTS,
    CF_SNAPSHOTS,
    CF_META_DATA,
    rust_rocksdb::DEFAULT_COLUMN_FAMILY_NAME,
];

/// RocksDB persistence backend.
///
/// All blocking RocksDB operations are wrapped in `tokio::task::spawn_blocking`
/// to avoid blocking Tokio worker threads (design section 5.3).
pub struct RocksDbPersistor {
    db: Arc<DB>,
    namespace: String,
}

impl RocksDbPersistor {
    /// Open or create a RocksDB database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PersistenceError> {
        Self::open_with_namespace(path, "default")
    }

    /// Open with a specific namespace.
    pub fn open_with_namespace(
        path: impl AsRef<Path>,
        namespace: &str,
    ) -> Result<Self, PersistenceError> {
        if namespace.len() > u16::MAX as usize {
            return Err(PersistenceError::Backend(
                "namespace is too long (maximum 65535 bytes)".into(),
            ));
        }
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB
        opts.set_max_write_buffer_number(3);
        opts.set_target_file_size_base(64 * 1024 * 1024); // 64 MB

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = ALL_CF_NAMES
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                cf_opts.set_compression_type(DBCompressionType::Lz4);
                ColumnFamilyDescriptor::new(*name, cf_opts)
            })
            .collect();

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| PersistenceError::Backend(format!("RocksDB open failed: {e}")))?;

        Ok(Self {
            db: Arc::new(db),
            namespace: namespace.to_string(),
        })
    }

    fn encode_namespace_prefix(namespace: &str) -> Vec<u8> {
        // Preserve the original on-disk layout for the default namespace.
        if namespace == "default" {
            return Vec::new();
        }
        let mut key = Vec::with_capacity(NAMESPACE_KEY_MAGIC.len() + 2 + namespace.len());
        key.extend_from_slice(NAMESPACE_KEY_MAGIC);
        key.extend_from_slice(&(namespace.len() as u16).to_be_bytes());
        key.extend_from_slice(namespace.as_bytes());
        key
    }

    /// Encode a journal key:
    /// [namespace][QID][8-byte EventTime][4-byte event ordinal]
    fn encode_journal_key(
        namespace: &str,
        qid: &NexoraId,
        time: EventTime,
        ordinal: u32,
    ) -> Result<Vec<u8>, PersistenceError> {
        let mut key = Self::encode_node_time_key(namespace, qid, time)?;
        key.extend_from_slice(&ordinal.to_be_bytes());
        Ok(key)
    }

    /// Encode a snapshot key: [namespace][QID][8-byte EventTime].
    fn encode_snapshot_key(
        namespace: &str,
        qid: &NexoraId,
        time: EventTime,
    ) -> Result<Vec<u8>, PersistenceError> {
        Self::encode_node_time_key(namespace, qid, time)
    }

    fn encode_node_time_key(
        namespace: &str,
        qid: &NexoraId,
        time: EventTime,
    ) -> Result<Vec<u8>, PersistenceError> {
        let qid_bytes = qid.as_bytes();
        if qid_bytes.len() > u16::MAX as usize {
            return Err(PersistenceError::Backend(
                "NexoraId is too long for RocksDB key encoding".into(),
            ));
        }
        let mut key = Self::encode_namespace_prefix(namespace);
        key.reserve(2 + qid_bytes.len() + 8);
        key.extend_from_slice(&(qid_bytes.len() as u16).to_be_bytes());
        key.extend_from_slice(qid_bytes);
        key.extend_from_slice(&time.as_micros().to_be_bytes());
        Ok(key)
    }

    /// Encode a prefix key for range scans.
    fn encode_prefix_key(namespace: &str, qid: &NexoraId) -> Result<Vec<u8>, PersistenceError> {
        let qid_bytes = qid.as_bytes();
        if qid_bytes.len() > u16::MAX as usize {
            return Err(PersistenceError::Backend(
                "NexoraId is too long for RocksDB key encoding".into(),
            ));
        }
        let mut key = Self::encode_namespace_prefix(namespace);
        key.reserve(2 + qid_bytes.len());
        key.extend_from_slice(&(qid_bytes.len() as u16).to_be_bytes());
        key.extend_from_slice(qid_bytes);
        Ok(key)
    }

    fn decode_qid(namespace: &str, key: &[u8]) -> Option<NexoraId> {
        let namespace_prefix = Self::encode_namespace_prefix(namespace);
        if !key.starts_with(&namespace_prefix) {
            return None;
        }
        let offset = namespace_prefix.len();
        if key.len() < offset + 2 {
            return None;
        }
        let qid_len = BigEndian::read_u16(&key[offset..offset + 2]) as usize;
        let qid_start = offset + 2;
        (key.len() >= qid_start + qid_len)
            .then(|| NexoraId::from_bytes(key[qid_start..qid_start + qid_len].to_vec()))
    }

    /// Decode EventTime from both original and namespaced node keys.
    fn decode_event_time(namespace: &str, key: &[u8]) -> Option<EventTime> {
        let namespace_prefix = Self::encode_namespace_prefix(namespace);
        if !key.starts_with(&namespace_prefix) {
            return None;
        }
        let qid_len_offset = namespace_prefix.len();
        if key.len() < qid_len_offset + 2 {
            return None;
        }
        let qid_len = BigEndian::read_u16(&key[qid_len_offset..qid_len_offset + 2]) as usize;
        let offset = qid_len_offset + 2 + qid_len;
        if key.len() < offset + 8 {
            return None;
        }
        Some(EventTime::from_micros(BigEndian::read_u64(
            &key[offset..offset + 8],
        )))
    }
}

#[async_trait::async_trait]
impl NamespacedPersistenceAgent for RocksDbPersistor {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    // ====== Node Change Events ======

    async fn persist_node_change_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<NodeChangeEvent>>,
    ) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_NODE_EVENTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let mut batch = WriteBatch::default();
            for (ordinal, event) in events.iter().enumerate() {
                let ordinal = u32::try_from(ordinal).map_err(|_| {
                    PersistenceError::Backend("too many events in one persistence batch".into())
                })?;
                let key = Self::encode_journal_key(&namespace, &qid, event.time, ordinal)?;
                let value = EventCodec::encode_node_event_json(event)
                    .map_err(|e| PersistenceError::Serialization(e.to_string()))?;
                batch.put_cf(cf, key, value);
            }
            db.write(&batch)
                .map_err(|e| PersistenceError::Backend(format!("Write batch failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    async fn get_node_change_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<NodeChangeEvent>>, PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_NODE_EVENTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let prefix = Self::encode_prefix_key(&namespace, &qid)?;
            let start_key = match start {
                Some(t) => Self::encode_journal_key(&namespace, &qid, t, 0)?,
                None => prefix.clone(),
            };
            let mut iter = db.raw_iterator_cf(cf);
            iter.seek(&start_key);
            let mut results = Vec::new();
            while iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(end_time) = end {
                    if let Some(t) = Self::decode_event_time(&namespace, key) {
                        if t > end_time {
                            break;
                        }
                    }
                }
                if let Some(value) = iter.value() {
                    let event = EventCodec::decode_node_event_json(value).map_err(|error| {
                        PersistenceError::Serialization(format!(
                            "failed to decode node event: {error}"
                        ))
                    })?;
                    results.push(event);
                }
                iter.next();
            }
            Ok(results)
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    async fn delete_node_change_events(&self, qid: NexoraId) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_NODE_EVENTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let prefix = Self::encode_prefix_key(&namespace, &qid)?;
            let mut iter = db.raw_iterator_cf(cf);
            iter.seek(&prefix);
            let mut batch = WriteBatch::default();
            while iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if !key.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(cf, key);
                iter.next();
            }
            db.write(&batch)
                .map_err(|e| PersistenceError::Backend(format!("Delete batch failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    // ====== Domain Index Events ======

    async fn persist_domain_index_events(
        &self,
        qid: NexoraId,
        events: Vec<TimedEvent<DomainIndexEvent>>,
    ) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_DOMAIN_INDEX_EVENTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let mut batch = WriteBatch::default();
            for (ordinal, event) in events.iter().enumerate() {
                let ordinal = u32::try_from(ordinal).map_err(|_| {
                    PersistenceError::Backend("too many events in one persistence batch".into())
                })?;
                let key = Self::encode_journal_key(&namespace, &qid, event.time, ordinal)?;
                let value = EventCodec::encode_domain_event_json(event)
                    .map_err(|e| PersistenceError::Serialization(e.to_string()))?;
                batch.put_cf(cf, key, value);
            }
            db.write(&batch)
                .map_err(|e| PersistenceError::Backend(format!("Write batch failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    async fn get_domain_index_events(
        &self,
        qid: NexoraId,
        start: Option<EventTime>,
        end: Option<EventTime>,
    ) -> Result<Vec<TimedEvent<DomainIndexEvent>>, PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_DOMAIN_INDEX_EVENTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let prefix = Self::encode_prefix_key(&namespace, &qid)?;
            let start_key = match start {
                Some(t) => Self::encode_journal_key(&namespace, &qid, t, 0)?,
                None => prefix.clone(),
            };
            let mut iter = db.raw_iterator_cf(cf);
            iter.seek(&start_key);
            let mut results = Vec::new();
            while iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if !key.starts_with(&prefix) {
                    break;
                }
                if let Some(end_time) = end {
                    if let Some(t) = Self::decode_event_time(&namespace, key) {
                        if t > end_time {
                            break;
                        }
                    }
                }
                if let Some(value) = iter.value() {
                    let event = EventCodec::decode_domain_event_json(value).map_err(|error| {
                        PersistenceError::Serialization(format!(
                            "failed to decode domain event: {error}"
                        ))
                    })?;
                    results.push(event);
                }
                iter.next();
            }
            Ok(results)
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    // ====== Snapshots ======

    async fn persist_snapshot(
        &self,
        qid: NexoraId,
        time: EventTime,
        snapshot: Vec<u8>,
    ) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let key = Self::encode_snapshot_key(&namespace, &qid, time)?;
            db.put_cf(cf, key, snapshot)
                .map_err(|e| PersistenceError::Backend(format!("Snapshot write failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    async fn get_latest_snapshot(
        &self,
        qid: NexoraId,
        up_to: EventTime,
    ) -> Result<Option<(EventTime, Vec<u8>)>, PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let seek_key = Self::encode_snapshot_key(&namespace, &qid, up_to)?;
            let mut iter = db.raw_iterator_cf(cf);
            iter.seek_for_prev(&seek_key);
            let prefix = Self::encode_prefix_key(&namespace, &qid)?;
            if iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if key.starts_with(&prefix) {
                    if let (Some(t), Some(v)) =
                        (Self::decode_event_time(&namespace, key), iter.value())
                    {
                        return Ok(Some((t, v.to_vec())));
                    }
                }
            }
            Ok(None)
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    async fn delete_snapshots(&self, qid: NexoraId) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let prefix = Self::encode_prefix_key(&namespace, &qid)?;
            let mut iter = db.raw_iterator_cf(cf);
            iter.seek(&prefix);
            let mut batch = WriteBatch::default();
            while iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if !key.starts_with(&prefix) {
                    break;
                }
                batch.delete_cf(cf, key);
                iter.next();
            }
            db.write(&batch)
                .map_err(|e| PersistenceError::Backend(format!("Delete snapshots failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    // ====== Enumeration ======

    async fn enumerate_journal_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_NODE_EVENTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let mut iter = db.raw_iterator_cf(cf);
            let namespace_prefix = Self::encode_namespace_prefix(&namespace);
            iter.seek(&namespace_prefix);
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            while iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if !key.starts_with(&namespace_prefix) {
                    break;
                }
                if let Some(qid) = Self::decode_qid(&namespace, key) {
                    if seen.insert(qid.clone()) {
                        result.push(qid);
                    }
                }
                iter.next();
            }
            Ok(result)
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    async fn enumerate_snapshot_node_ids(&self) -> Result<Vec<NexoraId>, PersistenceError> {
        let db = self.db.clone();
        let namespace = self.namespace.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db
                .cf_handle(CF_SNAPSHOTS)
                .ok_or_else(|| PersistenceError::Backend("CF not found".into()))?;
            let mut iter = db.raw_iterator_cf(cf);
            let namespace_prefix = Self::encode_namespace_prefix(&namespace);
            iter.seek(&namespace_prefix);
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            while iter.valid() {
                let key = iter.key().ok_or_else(|| {
                    PersistenceError::Backend("RocksDB iterator key is None".into())
                })?;
                if !key.starts_with(&namespace_prefix) {
                    break;
                }
                if let Some(qid) = Self::decode_qid(&namespace, key) {
                    if seen.insert(qid.clone()) {
                        result.push(qid);
                    }
                }
                iter.next();
            }
            Ok(result)
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    // ====== Durability barrier ======

    async fn flush_durable(&self) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            // Persist the WAL durably (fsync), then flush memtables to SSTs so
            // the data is on disk independent of the WAL. Writes use default
            // (unsynced) WriteOptions on the hot path for throughput; this
            // barrier is what makes a completed batch durable on demand.
            db.flush_wal(true)
                .map_err(|e| PersistenceError::Backend(format!("RocksDB WAL fsync failed: {e}")))?;
            db.flush()
                .map_err(|e| PersistenceError::Backend(format!("RocksDB flush failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))?
    }

    // ====== Shutdown ======

    async fn shutdown(&self) -> Result<(), PersistenceError> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.flush()
                .map_err(|e| PersistenceError::Backend(format!("RocksDB flush failed: {e}")))
        })
        .await
        .map_err(|_| PersistenceError::Backend("spawn_blocking cancelled".into()))??;
        tracing::info!(
            "RocksDB persistor shutting down (namespace: {})",
            self.namespace
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_value::Symbol;

    fn temp_persistor() -> (RocksDbPersistor, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let persistor = RocksDbPersistor::open(dir.path()).unwrap();
        (persistor, dir)
    }

    #[tokio::test]
    async fn test_persist_and_retrieve_events() {
        let (persistor, _dir) = temp_persistor();
        let qid = NexoraId::new_random();

        let events = vec![
            TimedEvent::new(
                NodeChangeEvent::PropertySet {
                    key: Symbol::new("speed"),
                    value: nexora_id::PropertyValue::Float(12.5),
                },
                EventTime::from_micros(1000),
            ),
            TimedEvent::new(
                NodeChangeEvent::PropertySet {
                    key: Symbol::new("name"),
                    value: nexora_id::PropertyValue::String("Forklift-042".into()),
                },
                EventTime::from_micros(2000),
            ),
        ];

        persistor
            .persist_node_change_events(qid.clone(), events)
            .await
            .unwrap();

        let retrieved = persistor
            .get_node_change_events(qid, None, None)
            .await
            .unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].time, EventTime::from_micros(1000));
        assert_eq!(retrieved[1].time, EventTime::from_micros(2000));
    }

    #[tokio::test]
    async fn test_time_range_filter() {
        let (persistor, _dir) = temp_persistor();
        let qid = NexoraId::new_random();

        let events: Vec<_> = (1..=10)
            .map(|i| {
                TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("tick"),
                        value: nexora_id::PropertyValue::Integer(i),
                    },
                    EventTime::from_micros((i * 1000) as u64),
                )
            })
            .collect();

        persistor
            .persist_node_change_events(qid.clone(), events)
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_snapshot_roundtrip() {
        let (persistor, _dir) = temp_persistor();
        let qid = NexoraId::new_random();

        let snapshot_data = b"test snapshot data".to_vec();
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
        let (persistor, _dir) = temp_persistor();
        let qid = NexoraId::new_random();

        persistor
            .persist_snapshot(qid.clone(), EventTime::from_micros(100), vec![1])
            .await
            .unwrap();
        persistor
            .persist_snapshot(qid.clone(), EventTime::from_micros(200), vec![2])
            .await
            .unwrap();
        persistor
            .persist_snapshot(qid.clone(), EventTime::from_micros(300), vec![3])
            .await
            .unwrap();

        let result = persistor
            .get_latest_snapshot(qid.clone(), EventTime::from_micros(200))
            .await
            .unwrap();
        assert_eq!(result.unwrap().1, vec![2]);

        let result = persistor
            .get_latest_snapshot(qid.clone(), EventTime::from_micros(150))
            .await
            .unwrap();
        assert_eq!(result.unwrap().1, vec![1]);
    }

    #[tokio::test]
    async fn test_delete_events() {
        let (persistor, _dir) = temp_persistor();
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

    #[tokio::test]
    async fn test_enumerate_node_ids() {
        let (persistor, _dir) = temp_persistor();
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

    #[tokio::test]
    async fn test_persistence_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();

        {
            let persistor = RocksDbPersistor::open(dir.path()).unwrap();
            persistor
                .persist_node_change_events(
                    qid.clone(),
                    vec![TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("persistent"),
                            value: nexora_id::PropertyValue::Boolean(true),
                        },
                        EventTime::from_micros(42),
                    )],
                )
                .await
                .unwrap();
        }

        {
            let persistor = RocksDbPersistor::open(dir.path()).unwrap();
            let events = persistor
                .get_node_change_events(qid, None, None)
                .await
                .unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].time, EventTime::from_micros(42));
        }
    }

    #[tokio::test]
    async fn test_same_timestamp_events_do_not_overwrite_each_other() {
        let (persistor, _dir) = temp_persistor();
        let qid = NexoraId::new_random();
        let time = EventTime::from_micros(42);
        persistor
            .persist_node_change_events(
                qid.clone(),
                vec![
                    TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("a"),
                            value: nexora_id::PropertyValue::Integer(1),
                        },
                        time,
                    ),
                    TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("b"),
                            value: nexora_id::PropertyValue::Integer(2),
                        },
                        time,
                    ),
                ],
            )
            .await
            .unwrap();

        let events = persistor
            .get_node_change_events(qid, None, None)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_reads_original_default_namespace_keys() {
        let (persistor, _dir) = temp_persistor();
        let qid = NexoraId::new_random();
        let event = TimedEvent::new(
            NodeChangeEvent::PropertySet {
                key: Symbol::new("legacy"),
                value: nexora_id::PropertyValue::Boolean(true),
            },
            EventTime::from_micros(7),
        );
        let key = RocksDbPersistor::encode_node_time_key("default", &qid, event.time).unwrap();
        let value = EventCodec::encode_node_event_json(&event).unwrap();
        let cf = persistor.db.cf_handle(CF_NODE_EVENTS).unwrap();
        persistor.db.put_cf(cf, key, value).unwrap();

        let events = persistor
            .get_node_change_events(qid, None, None)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].time, EventTime::from_micros(7));
    }

    #[tokio::test]
    async fn test_namespaces_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let qid = NexoraId::new_random();
        {
            let alpha = RocksDbPersistor::open_with_namespace(dir.path(), "alpha").unwrap();
            alpha
                .persist_node_change_events(
                    qid.clone(),
                    vec![TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("tenant"),
                            value: nexora_id::PropertyValue::String("alpha".into()),
                        },
                        EventTime::from_micros(1),
                    )],
                )
                .await
                .unwrap();
        }

        {
            let beta = RocksDbPersistor::open_with_namespace(dir.path(), "beta").unwrap();
            assert!(beta
                .get_node_change_events(qid.clone(), None, None)
                .await
                .unwrap()
                .is_empty());
        }

        let alpha = RocksDbPersistor::open_with_namespace(dir.path(), "alpha").unwrap();
        assert_eq!(
            alpha
                .get_node_change_events(qid, None, None)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
