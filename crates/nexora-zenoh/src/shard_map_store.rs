//! Durable snapshot of the authoritative [`ShardMap`].
//!
//! A0 introduced this store to keep the shard map — the one piece of cluster
//! metadata that drives routing, ownership, and epoch fencing — from being
//! rebuilt cold on every restart (`ClusterManager::new` derives version 1, all
//! epochs 1, from membership). Runtime failover and rebalance change owners and
//! *bump epochs*; without persistence those changes are lost on restart and the
//! reset epochs can no longer fence a deposed owner's stale writes.
//!
//! A1: this store no longer owns its own JSON file. It is now a thin, typed
//! wrapper over the unified [`ControlPlaneStore`] — it serializes the `ShardMap`
//! and writes it under a single well-known key in [`Namespace::ShardMap`]. All
//! control-plane metadata (shard map, MV defs, SQ defs) thus shares one durable
//! backend, one fsync policy, and one restore path — and A2 gets a single apply
//! target for consensus. The public API (`in_memory`/`open`/`save`/`load`/
//! `is_durable`) is unchanged so existing callers (`ControlPlane`,
//! `ClusterManager::new`) need no changes.

use crate::shard_map::ShardMap;
use nexora_core::control_plane_store::{
    ControlPlaneStore, InMemoryControlPlaneStore, Namespace, RocksDbControlPlaneStore,
};
use std::path::Path;
use std::sync::Arc;

/// The single well-known key under which the shard map is stored. There is only
/// ever one shard map per node, so a fixed key suffices.
const SHARD_MAP_KEY: &str = "current";

/// Persists the committed [`ShardMap`] through the unified [`ControlPlaneStore`].
///
/// Cloneable: holds an `Arc` to the backing store. An in-memory backend makes
/// `save` a no-op and `load` return `None`, matching the "durability is opt-in,
/// correctness is unaffected" convention used across this crate.
#[derive(Clone)]
pub struct ShardMapStore {
    store: Arc<dyn ControlPlaneStore>,
}

impl ShardMapStore {
    /// In-memory no-op store: `save` does nothing durable, `load` returns `None`
    /// across a process restart.
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(InMemoryControlPlaneStore::new()),
        }
    }

    /// Durable store backed by a RocksDB control-plane store under `dir`. On
    /// failure to open, falls back to an in-memory no-op (durability is
    /// best-effort; correctness is unaffected).
    pub fn open(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref();
        match RocksDbControlPlaneStore::open(dir) {
            Ok(store) => Self {
                store: Arc::new(store),
            },
            Err(e) => {
                tracing::warn!(
                    "Failed to open shard-map control-plane store at {} ({e}); \
                     shard map will not survive restart",
                    dir.display()
                );
                Self::in_memory()
            }
        }
    }

    /// Wrap an existing shared [`ControlPlaneStore`]. Lets the shard map share
    /// the *same* physical backend as MV/SQ definitions (one store per node),
    /// which is how A2 will feed a single consensus-backed store to every
    /// metadata domain.
    pub fn from_store(store: Arc<dyn ControlPlaneStore>) -> Self {
        Self { store }
    }

    /// The underlying control-plane store, for callers that want to share the
    /// same backend with other metadata domains.
    pub fn inner(&self) -> Arc<dyn ControlPlaneStore> {
        self.store.clone()
    }

    /// Whether this store persists to disk.
    pub fn is_durable(&self) -> bool {
        self.store.is_durable()
    }

    /// Persist the map. Serializes to JSON and writes it under the well-known
    /// shard-map key; the backend fsyncs durable writes before returning. No-op
    /// on an in-memory backend. Errors are returned but should not fail the
    /// originating shard-map commit (the in-memory map is authoritative for the
    /// running process; the snapshot is durability on top).
    pub fn save(&self, map: &ShardMap) -> Result<(), String> {
        let json =
            serde_json::to_vec(map).map_err(|e| format!("shard map serialize failed: {e}"))?;
        self.store
            .put(Namespace::ShardMap, SHARD_MAP_KEY, &json)
            .map_err(|e| format!("shard map persist failed: {e}"))
    }

    /// Load the last persisted map, or `None` if durability is off or no
    /// snapshot exists yet. A corrupt/unparseable snapshot is logged and treated
    /// as absent (the caller falls back to rebuilding from membership) rather
    /// than failing startup.
    pub fn load(&self) -> Option<ShardMap> {
        let bytes = match self.store.get(Namespace::ShardMap, SHARD_MAP_KEY) {
            Ok(Some(b)) => b,
            Ok(None) => return None,
            Err(e) => {
                tracing::warn!("Failed to read shard-map snapshot: {e}");
                return None;
            }
        };
        match serde_json::from_slice::<ShardMap>(&bytes) {
            Ok(map) => Some(map),
            Err(e) => {
                tracing::warn!(
                    "Shard-map snapshot is corrupt ({e}); ignoring and rebuilding from membership"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_map::ShardMap;

    fn temp_dir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("nexora-shardmap-store-{}", uuid::Uuid::new_v4()));
        p
    }

    #[test]
    fn in_memory_save_is_noop_and_load_is_none() {
        let store = ShardMapStore::in_memory();
        let map = ShardMap::new_local(4);
        assert!(store.save(&map).is_ok());
        // In-memory backend keeps it within this instance, but a fresh in-memory
        // store (a restart) has nothing.
        assert!(!store.is_durable());
        assert!(ShardMapStore::in_memory().load().is_none());
    }

    #[test]
    fn durable_roundtrip_preserves_version_and_epochs() {
        let dir = temp_dir();
        let store = ShardMapStore::open(&dir);
        assert!(store.is_durable());

        // A map that has "moved on" from the cold initial state: bumped version
        // and a shard whose owner + epoch changed via failover.
        let mut map = ShardMap::new_distributed_rf(
            4,
            &["node-a".into(), "node-b".into()],
            "node-a".into(),
            2,
        );
        map.version = 7;
        let asg = map.assignments.get_mut(&0).unwrap();
        asg.owner = "node-b".into();
        asg.epoch = asg.epoch.next().next(); // epoch 3
        let expected_owner = asg.owner.clone();
        let expected_epoch = asg.epoch.value();

        store.save(&map).unwrap();
        drop(store);

        // Fresh store over the same dir = simulated restart.
        let store2 = ShardMapStore::open(&dir);
        let loaded = store2.load().expect("snapshot should load after restart");
        assert_eq!(loaded.version, 7, "committed version must survive restart");
        let loaded_asg = loaded.get(0).unwrap();
        assert_eq!(
            loaded_asg.owner, expected_owner,
            "failed-over owner must survive"
        );
        assert_eq!(
            loaded_asg.epoch.value(),
            expected_epoch,
            "bumped epoch must survive so a deposed owner stays fenced"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn latest_save_overwrites_previous() {
        let dir = temp_dir();
        let store = ShardMapStore::open(&dir);
        let mut map = ShardMap::new_local(2);
        map.version = 1;
        store.save(&map).unwrap();
        map.version = 2;
        store.save(&map).unwrap();
        assert_eq!(store.load().unwrap().version, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn from_store_shares_backend() {
        // A shard map saved through one ShardMapStore is visible through another
        // that wraps the same underlying control-plane store.
        let backing = ShardMapStore::in_memory();
        let shared = backing.inner();
        let a = ShardMapStore::from_store(shared.clone());
        let mut map = ShardMap::new_local(2);
        map.version = 5;
        a.save(&map).unwrap();
        let b = ShardMapStore::from_store(shared);
        assert_eq!(
            b.load().unwrap().version,
            5,
            "both wrappers share one backend"
        );
    }
}
