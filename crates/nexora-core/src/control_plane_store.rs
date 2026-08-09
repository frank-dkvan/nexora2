//! Unified control-plane metadata store (A1).
//!
//! A0 gave each kind of built-in metadata its *own* single-node persistence:
//! shard map (JSON snapshot in nexora-zenoh), MV definitions (a dedicated
//! RocksDB in nexora-core), SQ definitions (the graph persistence layer). Three
//! stores, three restore paths, three fsync policies — and each with its own
//! bypass write site (`create_view` writing RocksDB directly, etc.).
//!
//! A1 collapses those onto a single abstraction: a namespaced, byte-oriented
//! key/value store with a durable snapshot. Every piece of control-plane
//! metadata reads and writes through *this* trait, so:
//!
//! - there is one restore path and one fsync policy for all metadata;
//! - A2 (consensus) has exactly one apply target — a Raft log entry becomes a
//!   `put`/`delete` here, and `snapshot`/`restore` back the consensus snapshot.
//!
//! The trait is byte-oriented on purpose. It lives in `nexora-core`, the common
//! dependency of the three metadata owners (nexora-core, nexora-standing-query,
//! nexora-zenoh), so it cannot reference their domain types (`ShardMap`,
//! `MaterializedView`, `StandingQuery`) without a dependency cycle. Each domain
//! owns its own serde and passes bytes; the store only moves bytes durably. A
//! [`Namespace`] separates the domains within one physical store.

use rust_rocksdb as rocksdb;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// Logical partition of the control-plane keyspace. Keeps the metadata domains
/// from colliding within one physical store, and lets a domain `list` only its
/// own keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Namespace {
    /// Cluster shard map (owner/epoch/replicas). Single well-known key.
    ShardMap,
    /// Materialized-view definitions, keyed by view id.
    MvDef,
    /// Materialized-view row data, keyed by `{view_id}:{row_key}`.
    MvData,
    /// Standing-query definitions + match state. Single well-known key.
    SqState,
    /// Control-plane consensus (A2) bookkeeping: the Raft state machine's
    /// last-applied log id and last membership, so a persistent state machine
    /// can report `applied_state()` on restart without replaying from zero.
    RaftMeta,
    /// Domain/ontology package definitions, keyed by domain name (阶段6).
    DomainDef,
}

impl Namespace {
    /// Stable on-disk prefix. Changing these strings is a data migration.
    pub fn as_str(self) -> &'static str {
        match self {
            Namespace::ShardMap => "shardmap",
            Namespace::MvDef => "mv:def",
            Namespace::MvData => "mv:data",
            Namespace::SqState => "sq:state",
            Namespace::RaftMeta => "raft:meta",
            Namespace::DomainDef => "domain:def",
        }
    }

    /// All namespaces, for iteration.
    pub fn all() -> [Namespace; 6] {
        [
            Namespace::ShardMap,
            Namespace::MvDef,
            Namespace::MvData,
            Namespace::SqState,
            Namespace::RaftMeta,
            Namespace::DomainDef,
        ]
    }

    /// Application-data namespaces included in a consensus snapshot — everything
    /// except [`Namespace::RaftMeta`], which is the Raft layer's own
    /// last-applied/membership bookkeeping (carried in `SnapshotMeta`, not the
    /// snapshot blob). Including it would let an installed snapshot clobber a
    /// follower's applied-tracking.
    pub fn snapshot_namespaces() -> [Namespace; 5] {
        [
            Namespace::ShardMap,
            Namespace::MvDef,
            Namespace::MvData,
            Namespace::SqState,
            Namespace::DomainDef,
        ]
    }
}

/// Error from a control-plane store operation.
#[derive(Debug, thiserror::Error)]
pub enum ControlStoreError {
    /// Underlying I/O / backend failure.
    #[error("control-plane store backend error: {0}")]
    Backend(String),
    /// Snapshot payload could not be decoded.
    #[error("control-plane store snapshot decode error: {0}")]
    SnapshotDecode(String),
    /// Lock poisoned (concurrent panic while holding lock).
    #[error("control-plane store lock poisoned: {0}")]
    LockPoisoned(String),
}

/// A namespaced, byte-oriented, durable key/value store for control-plane
/// metadata.
///
/// Durability contract: a successful `put`/`delete` on a durable backend is
/// fsync'd before it returns — control-plane metadata is non-replayable, so it
/// must survive a crash the instant the call succeeds. In-memory backends are a
/// no-op on durability (used for tests and `--no-rocksdb`).
///
/// All methods are synchronous: the backing store is a local embedded KV
/// (RocksDB) or a memory map, and the callers (constructors like
/// `ClusterManager::new`, definition-change handlers) are not on a hot path.
/// Keeping them sync lets them be called from non-async constructors, matching
/// the existing `ShardMapStore`/`ShardReplicationLog::open_durable` pattern.
pub trait ControlPlaneStore: Send + Sync {
    /// Write `value` at `(namespace, key)`, fsync'd on a durable backend.
    fn put(&self, namespace: Namespace, key: &str, value: &[u8]) -> Result<(), ControlStoreError>;

    /// Read the value at `(namespace, key)`, or `None` if absent.
    fn get(&self, namespace: Namespace, key: &str) -> Result<Option<Vec<u8>>, ControlStoreError>;

    /// Delete `(namespace, key)`, fsync'd on a durable backend. Deleting a
    /// missing key is not an error.
    fn delete(&self, namespace: Namespace, key: &str) -> Result<(), ControlStoreError>;

    /// List all `(key, value)` pairs in `namespace`, in key order.
    fn list(&self, namespace: Namespace) -> Result<Vec<(String, Vec<u8>)>, ControlStoreError>;

    /// Whether this store persists to disk (`false` = in-memory no-op).
    fn is_durable(&self) -> bool;

    /// Serialize the entire store (all namespaces) into a single opaque blob,
    /// for a consensus snapshot (A2). The format is the store's own concern;
    /// `restore` must accept exactly what `snapshot` produced.
    fn snapshot(&self) -> Result<Vec<u8>, ControlStoreError> {
        let mut dump: BTreeMap<String, BTreeMap<String, Vec<u8>>> = BTreeMap::new();
        for ns in Namespace::snapshot_namespaces() {
            let entries = self.list(ns)?;
            if entries.is_empty() {
                continue;
            }
            let bucket = dump.entry(ns.as_str().to_string()).or_default();
            for (k, v) in entries {
                bucket.insert(k, v);
            }
        }
        serde_json::to_vec(&dump).map_err(|e| ControlStoreError::Backend(e.to_string()))
    }

    /// Replace the entire store contents with a blob previously produced by
    /// [`snapshot`](Self::snapshot). Used to install a consensus snapshot (A2).
    ///
    /// Each snapshot namespace is fully **cleared** before the blob's keys are
    /// applied, so keys that were deleted before the snapshot was taken do not
    /// survive the install. Without this, a follower whose local store held keys
    /// absent from the snapshot would keep them, diverging from the leader's
    /// state machine and defeating consensus. `snapshot`/`restore` must therefore
    /// agree on the exact namespace set (`Namespace::snapshot_namespaces`).
    fn restore(&self, blob: &[u8]) -> Result<(), ControlStoreError> {
        let dump: BTreeMap<String, BTreeMap<String, Vec<u8>>> = serde_json::from_slice(blob)
            .map_err(|e| ControlStoreError::SnapshotDecode(e.to_string()))?;
        for ns in Namespace::snapshot_namespaces() {
            // Clear the namespace first: delete every existing key so stale
            // entries (deleted before the snapshot) don't linger after install.
            for (k, _) in self.list(ns)? {
                self.delete(ns, &k)?;
            }
            if let Some(bucket) = dump.get(ns.as_str()) {
                for (k, v) in bucket {
                    self.put(ns, k, v)?;
                }
            }
        }
        Ok(())
    }
}

/// In-memory [`ControlPlaneStore`] — for tests and `--no-rocksdb`. All data is
/// lost on process exit; `is_durable` is `false`.
#[derive(Default)]
pub struct InMemoryControlPlaneStore {
    // namespace -> key -> value
    data: std::sync::RwLock<BTreeMap<&'static str, BTreeMap<String, Vec<u8>>>>,
}

impl InMemoryControlPlaneStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlPlaneStore for InMemoryControlPlaneStore {
    fn put(&self, namespace: Namespace, key: &str, value: &[u8]) -> Result<(), ControlStoreError> {
        let mut data = self
            .data
            .write()
            .map_err(|e| ControlStoreError::LockPoisoned(format!("Write lock: {e}")))?;
        data.entry(namespace.as_str())
            .or_default()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn get(&self, namespace: Namespace, key: &str) -> Result<Option<Vec<u8>>, ControlStoreError> {
        let data = self
            .data
            .read()
            .map_err(|e| ControlStoreError::LockPoisoned(format!("Read lock: {e}")))?;
        Ok(data
            .get(namespace.as_str())
            .and_then(|b| b.get(key))
            .cloned())
    }

    fn delete(&self, namespace: Namespace, key: &str) -> Result<(), ControlStoreError> {
        let mut data = self
            .data
            .write()
            .map_err(|e| ControlStoreError::LockPoisoned(format!("Write lock: {e}")))?;
        if let Some(bucket) = data.get_mut(namespace.as_str()) {
            bucket.remove(key);
        }
        Ok(())
    }

    fn list(&self, namespace: Namespace) -> Result<Vec<(String, Vec<u8>)>, ControlStoreError> {
        let data = self
            .data
            .read()
            .map_err(|e| ControlStoreError::LockPoisoned(format!("Read lock: {e}")))?;
        Ok(data
            .get(namespace.as_str())
            .map(|b| b.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    fn is_durable(&self) -> bool {
        false
    }
}

/// Durable [`ControlPlaneStore`] backed by RocksDB.
///
/// Keys are laid out as `{namespace}:{key}` so a single DB holds every metadata
/// domain and a namespace `list` is a prefix scan. Writes are fsync'd
/// (`WriteOptions::set_sync(true)`) so control-plane metadata survives a crash
/// the instant a call returns — the durability contract the trait promises.
pub struct RocksDbControlPlaneStore {
    db: Arc<rocksdb::DB>,
}

impl RocksDbControlPlaneStore {
    /// Open (creating if absent) a durable store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ControlStoreError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, path)
            .map_err(|e| ControlStoreError::Backend(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    fn full_key(namespace: Namespace, key: &str) -> String {
        format!("{}:{}", namespace.as_str(), key)
    }
}

impl ControlPlaneStore for RocksDbControlPlaneStore {
    fn put(&self, namespace: Namespace, key: &str, value: &[u8]) -> Result<(), ControlStoreError> {
        let mut wopts = rocksdb::WriteOptions::default();
        wopts.set_sync(true);
        self.db
            .put_opt(Self::full_key(namespace, key).as_bytes(), value, &wopts)
            .map_err(|e| ControlStoreError::Backend(e.to_string()))
    }

    fn get(&self, namespace: Namespace, key: &str) -> Result<Option<Vec<u8>>, ControlStoreError> {
        self.db
            .get(Self::full_key(namespace, key).as_bytes())
            .map_err(|e| ControlStoreError::Backend(e.to_string()))
    }

    fn delete(&self, namespace: Namespace, key: &str) -> Result<(), ControlStoreError> {
        let mut wopts = rocksdb::WriteOptions::default();
        wopts.set_sync(true);
        self.db
            .delete_opt(Self::full_key(namespace, key).as_bytes(), &wopts)
            .map_err(|e| ControlStoreError::Backend(e.to_string()))
    }

    fn list(&self, namespace: Namespace) -> Result<Vec<(String, Vec<u8>)>, ControlStoreError> {
        let prefix = format!("{}:", namespace.as_str());
        let prefix_bytes = prefix.as_bytes();
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix_bytes) {
            let (k, v) = item.map_err(|e| ControlStoreError::Backend(e.to_string()))?;
            // prefix_iterator can over-scan past the prefix; guard explicitly.
            if !k.starts_with(prefix_bytes) {
                break;
            }
            let key = String::from_utf8_lossy(&k[prefix_bytes.len()..]).to_string();
            out.push((key, v.to_vec()));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn is_durable(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete_list_roundtrip() {
        let store = InMemoryControlPlaneStore::new();
        store.put(Namespace::MvDef, "v1", b"def-1").unwrap();
        store.put(Namespace::MvDef, "v2", b"def-2").unwrap();
        store.put(Namespace::SqState, "state", b"sq").unwrap();

        assert_eq!(
            store.get(Namespace::MvDef, "v1").unwrap(),
            Some(b"def-1".to_vec())
        );
        assert_eq!(store.get(Namespace::MvDef, "missing").unwrap(), None);

        // list is scoped to the namespace and key-ordered.
        let mvs = store.list(Namespace::MvDef).unwrap();
        assert_eq!(mvs.len(), 2);
        assert_eq!(mvs[0].0, "v1");
        assert_eq!(mvs[1].0, "v2");
        assert_eq!(store.list(Namespace::SqState).unwrap().len(), 1);

        store.delete(Namespace::MvDef, "v1").unwrap();
        assert_eq!(store.get(Namespace::MvDef, "v1").unwrap(), None);
        assert_eq!(store.list(Namespace::MvDef).unwrap().len(), 1);
        // deleting a missing key is not an error
        store.delete(Namespace::MvDef, "nope").unwrap();
    }

    #[test]
    fn snapshot_restore_roundtrips_all_namespaces() {
        let src = InMemoryControlPlaneStore::new();
        src.put(Namespace::ShardMap, "map", b"the-map").unwrap();
        src.put(Namespace::MvDef, "v1", b"def-1").unwrap();
        src.put(Namespace::MvData, "v1:row1", b"row").unwrap();
        src.put(Namespace::SqState, "state", b"sq").unwrap();

        let blob = src.snapshot().unwrap();

        let dst = InMemoryControlPlaneStore::new();
        dst.restore(&blob).unwrap();

        assert_eq!(
            dst.get(Namespace::ShardMap, "map").unwrap(),
            Some(b"the-map".to_vec())
        );
        assert_eq!(
            dst.get(Namespace::MvDef, "v1").unwrap(),
            Some(b"def-1".to_vec())
        );
        assert_eq!(
            dst.get(Namespace::MvData, "v1:row1").unwrap(),
            Some(b"row".to_vec())
        );
        assert_eq!(
            dst.get(Namespace::SqState, "state").unwrap(),
            Some(b"sq".to_vec())
        );
    }

    #[test]
    fn restore_clears_stale_keys_absent_from_snapshot() {
        // Leader takes a snapshot holding only v1.
        let leader = InMemoryControlPlaneStore::new();
        leader.put(Namespace::MvDef, "v1", b"def-1").unwrap();
        let blob = leader.snapshot().unwrap();

        // Follower's local store has an extra key (v2) that was deleted before
        // the snapshot was taken. Installing the snapshot must remove it, else
        // the follower diverges from the leader's state machine.
        let follower = InMemoryControlPlaneStore::new();
        follower.put(Namespace::MvDef, "v1", b"stale-old").unwrap();
        follower
            .put(Namespace::MvDef, "v2", b"should-be-gone")
            .unwrap();

        follower.restore(&blob).unwrap();

        assert_eq!(
            follower.get(Namespace::MvDef, "v1").unwrap(),
            Some(b"def-1".to_vec())
        );
        assert_eq!(
            follower.get(Namespace::MvDef, "v2").unwrap(),
            None,
            "stale key not present in the snapshot must be cleared on restore"
        );
        assert_eq!(follower.list(Namespace::MvDef).unwrap().len(), 1);
    }

    #[test]
    fn in_memory_is_not_durable() {
        assert!(!InMemoryControlPlaneStore::new().is_durable());
    }

    fn temp_dir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("nexora-cps-{}", uuid::Uuid::new_v4()));
        p
    }

    #[test]
    fn rocksdb_is_durable_and_survives_reopen() {
        let dir = temp_dir();
        {
            let store = RocksDbControlPlaneStore::open(&dir).unwrap();
            assert!(store.is_durable());
            store.put(Namespace::ShardMap, "map", b"the-map").unwrap();
            store.put(Namespace::MvDef, "v1", b"def-1").unwrap();
            store.put(Namespace::MvDef, "v2", b"def-2").unwrap();
            store.put(Namespace::MvData, "v1:row1", b"row").unwrap();
            store.delete(Namespace::MvDef, "v2").unwrap();
        } // drop closes the DB — simulates a process restart

        let store2 = RocksDbControlPlaneStore::open(&dir).unwrap();
        assert_eq!(
            store2.get(Namespace::ShardMap, "map").unwrap(),
            Some(b"the-map".to_vec())
        );
        assert_eq!(
            store2.get(Namespace::MvDef, "v1").unwrap(),
            Some(b"def-1".to_vec())
        );
        // deleted key stays gone across restart
        assert_eq!(store2.get(Namespace::MvDef, "v2").unwrap(), None);
        // list is namespace-scoped and does not leak MvData into MvDef
        let mvs = store2.list(Namespace::MvDef).unwrap();
        assert_eq!(mvs.len(), 1);
        assert_eq!(mvs[0].0, "v1");
        assert_eq!(store2.list(Namespace::MvData).unwrap().len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rocksdb_snapshot_restore_into_memory() {
        let dir = temp_dir();
        let src = RocksDbControlPlaneStore::open(&dir).unwrap();
        src.put(Namespace::ShardMap, "map", b"m").unwrap();
        src.put(Namespace::SqState, "state", b"s").unwrap();
        let blob = src.snapshot().unwrap();

        // Snapshot from a durable store restores into any backend (A2 will
        // install consensus snapshots this way).
        let dst = InMemoryControlPlaneStore::new();
        dst.restore(&blob).unwrap();
        assert_eq!(
            dst.get(Namespace::ShardMap, "map").unwrap(),
            Some(b"m".to_vec())
        );
        assert_eq!(
            dst.get(Namespace::SqState, "state").unwrap(),
            Some(b"s".to_vec())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
