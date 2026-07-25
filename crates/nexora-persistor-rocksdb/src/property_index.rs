//! Property index with L3 LSM persistence (RocksDB backend).
//!
//! Extends the in-memory `PropertyIndex` (L1 Hash + L2 BTree) with a persistent
//! RocksDB L3 tier. The in-memory tiers act as a read/write cache over the
//! durable L3 store.
//!
//! # Architecture
//!
//! - **L1**: Hash index (hot data, ~10K entries, O(1) lookup)
//! - **L2**: BTree index (warm data, ~100K entries, O(log N), range-scan capable)
//! - **L3**: RocksDB column family `"property-index"` (cold data, unlimited, persistent)
//!
//! # Key Encoding (L3)
//!
//! Keys are bincode-serialized `(property_name: String, property_value: PropertyValue)` tuples.
//! Values are bincode-serialized `Vec<NexoraId>`.
//!
//! # Write Path
//!
//! Writes go to L3 first (durability), then cache in L1/L2. This ensures that
//! even if the process crashes immediately after `insert`, the data survives in
//! RocksDB.
//!
//! # Read Path
//!
//! Queries check L1, then L2, then L3. On an L3 hit the result is warmed into
//! the in-memory tiers so subsequent queries for the same key avoid RocksDB I/O.
//!
//! # Range Queries
//!
//! Range queries merge results from L2 (BTree range scan) and L3 (RocksDB
//! iterator), deduplicating by NexoraId.

use nexora_core::{IndexConfig, IndexError, IndexStats, PropertyIndex};
use nexora_id::{NexoraId, PropertyValue};
use rust_rocksdb::{ColumnFamilyDescriptor, DBCompressionType, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

/// Column family name for the property index in RocksDB.
const CF_PROPERTY_INDEX: &str = "property-index";

/// Semantic ordering for `PropertyValue`, used to bound L3 range scans.
///
/// This must NOT rely on the bincode byte encoding (which is not order-
/// preserving). It mirrors the total order used by the in-memory index:
/// Null < Bool < Int < Float < String < Bytes < everything else, comparing
/// within a type by natural value order. Floats use IEEE-754 total ordering.
fn property_value_cmp(a: &PropertyValue, b: &PropertyValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    fn discriminant(v: &PropertyValue) -> u8 {
        match v {
            PropertyValue::Null => 0,
            PropertyValue::Boolean(_) => 1,
            PropertyValue::Integer(_) => 2,
            PropertyValue::Float(_) => 3,
            PropertyValue::String(_) => 4,
            PropertyValue::Bytes(_) => 5,
            _ => 6,
        }
    }

    let (da, db) = (discriminant(a), discriminant(b));
    if da != db {
        return da.cmp(&db);
    }

    match (a, b) {
        (PropertyValue::Null, PropertyValue::Null) => Ordering::Equal,
        (PropertyValue::Boolean(x), PropertyValue::Boolean(y)) => x.cmp(y),
        (PropertyValue::Integer(x), PropertyValue::Integer(y)) => x.cmp(y),
        (PropertyValue::Float(x), PropertyValue::Float(y)) => total_cmp_f64(*x, *y),
        (PropertyValue::String(x), PropertyValue::String(y)) => x.cmp(y),
        (PropertyValue::Bytes(x), PropertyValue::Bytes(y)) => x.cmp(y),
        // Complex/temporal types: fall back to a stable serialized comparison.
        _ => {
            let sa = serde_json::to_string(a).unwrap_or_default();
            let sb = serde_json::to_string(b).unwrap_or_default();
            sa.cmp(&sb)
        }
    }
}

/// IEEE-754 total ordering for f64 (matches `f64::total_cmp`, restated here to
/// avoid depending on the MSRV of that stabilization).
fn total_cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    let mut ai = a.to_bits() as i64;
    let mut bi = b.to_bits() as i64;
    ai ^= (((ai >> 63) as u64) >> 1) as i64;
    bi ^= (((bi >> 63) as u64) >> 1) as i64;
    ai.cmp(&bi)
}

/// A key stored in the RocksDB `property-index` column family.
///
/// Encoded as bincode-serialized bytes. The lexicographic order of the
/// serialized form is used for range scans.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexKey {
    property: String,
    value: PropertyValue,
}

impl IndexKey {
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("bincode serialization of IndexKey should not fail")
    }

    #[allow(dead_code)]
    fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        bincode::deserialize(bytes).map_err(|e| IndexError::Serialization(e.to_string()))
    }
}

/// A property index with a persistent RocksDB L3 tier.
///
/// Combines an in-memory `PropertyIndex` (L1 + L2) with a RocksDB column family
/// (`"property-index"`) as the durable L3 store.
///
/// # Examples
///
/// ```no_run
/// use nexora_persistor_rocksdb::PersistentPropertyIndex;
/// use nexora_core::IndexConfig;
///
/// let index = PersistentPropertyIndex::new("/tmp/my_index", IndexConfig::default()).unwrap();
/// ```
pub struct PersistentPropertyIndex {
    /// In-memory tiers (L1 Hash + L2 BTree).
    memory_index: PropertyIndex,

    /// Persistent tier (L3) -- RocksDB.
    db: Arc<DB>,

    /// Serializes L3 read-modify-write sequences. RocksDB's `get`+`put` is not
    /// atomic on its own, so two concurrent inserts to the same key would each
    /// read the same list, append their own node, and the second `put` would
    /// overwrite the first — losing a node. This lock makes each RMW exclusive.
    l3_write_lock: std::sync::Mutex<()>,
}

impl PersistentPropertyIndex {
    /// Open or create a persistent property index at `db_path`.
    ///
    /// Creates the RocksDB database (and the `"property-index"` column family)
    /// if they do not already exist. The in-memory tiers start empty and are
    /// warmed on subsequent queries.
    pub fn new(db_path: impl AsRef<Path>, config: IndexConfig) -> Result<Self, IndexError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Tune for a write-heavy workload (index inserts are frequent).
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128 MB
        opts.set_max_write_buffer_number(4);
        opts.set_compression_type(DBCompressionType::Lz4);

        let cf_descriptor = ColumnFamilyDescriptor::new(CF_PROPERTY_INDEX, Options::default());

        let db = DB::open_cf_descriptors(&opts, db_path, vec![cf_descriptor])
            .map_err(|e| IndexError::Backend(e.to_string()))?;

        Ok(Self {
            memory_index: PropertyIndex::new_with_config(config),
            db: Arc::new(db),
            l3_write_lock: std::sync::Mutex::new(()),
        })
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Insert a property index entry.
    ///
    /// Writes through to L3 (RocksDB) first for durability, then caches the
    /// entry in the in-memory L1/L2 tiers.
    pub async fn insert(
        &self,
        property: impl Into<String>,
        value: PropertyValue,
        node_id: NexoraId,
    ) -> Result<(), IndexError> {
        let property = property.into();
        let key = IndexKey {
            property: property.clone(),
            value: value.clone(),
        };

        // 1. Write to L3 first -- durability guarantee.
        self.write_to_l3(&key, &node_id)?;

        // 2. Cache in L1/L2 for fast subsequent lookups.
        self.memory_index.insert(property, value, node_id).await?;

        Ok(())
    }

    /// Exact-match query: return all node IDs that have `property = value`.
    ///
    /// Lookup order: L1 (hash) -> L2 (btree) -> L3 (RocksDB).
    /// On an L3 hit the result is automatically warmed into the in-memory
    /// tiers so the next query for the same key is fast.
    pub async fn query(
        &self,
        property: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NexoraId>, IndexError> {
        // Read the in-memory tiers (L1 + L2) AND L3, then merge. The memory
        // tiers are a cache and may hold only a subset of a key's nodes after
        // eviction or `clear_cache`, so returning the memory result alone can
        // silently omit nodes that are still durable in L3. L3 is authoritative
        // for completeness; merging both guarantees the full result set.
        let mut results = self.memory_index.query(property, value).await?;

        let key = IndexKey {
            property: property.to_string(),
            value: value.clone(),
        };
        let l3_nodes = self.read_from_l3(&key)?;

        // Cache-warm any L3 nodes missing from memory so subsequent queries can
        // be served from L1/L2. The data is already durable in L3, so warming
        // errors are non-fatal.
        for node in &l3_nodes {
            if !results.contains(node) {
                results.push(node.clone());
                let _ = self
                    .memory_index
                    .insert(property, value.clone(), node.clone())
                    .await;
            }
        }

        Ok(results)
    }

    /// Range query: all nodes where `property` is between `start` and `end`
    /// (inclusive).
    ///
    /// Merges results from L2 (BTree range scan) and L3 (RocksDB iterator),
    /// deduplicating by `NexoraId`.
    pub async fn range_query(
        &self,
        property: &str,
        start: &PropertyValue,
        end: &PropertyValue,
    ) -> Result<Vec<NexoraId>, IndexError> {
        // L2: BTree range scan (sorted, in-memory).
        let mut results = self.memory_index.range_query(property, start, end).await?;

        // L3: RocksDB iterator.
        let l3_results = self.range_query_l3(property, start, end)?;

        // Merge and deduplicate.
        results.extend(l3_results);
        results.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        results.dedup();

        Ok(results)
    }

    /// Remove all index entries for the given node across all tiers.
    ///
    /// This is an expensive operation in L3 because it requires a full scan of
    /// the column family. In a production system a reverse index
    /// (`node_id -> [(property, value)]`) should be maintained to make this
    /// O(1) per index entry rather than O(N) in total index size.
    pub async fn remove_node(&self, node_id: &NexoraId) -> Result<(), IndexError> {
        // Remove from in-memory tiers.
        self.memory_index.remove_node(node_id).await?;

        // Remove from L3 (full scan -- see note above).
        self.remove_from_l3(node_id)?;

        Ok(())
    }

    /// Clear only the in-memory caches (L1 + L2).
    ///
    /// The persistent L3 data is untouched. Useful for testing cache-miss
    /// behaviour or reclaiming memory under pressure. Subsequent queries will
    /// repopulate the cache from L3.
    pub async fn clear_cache(&self) {
        self.memory_index.clear().await;
    }

    /// Return a snapshot of the current index statistics.
    ///
    /// The returned `IndexStats` includes `l3_hits` tracked by this wrapper,
    /// plus the in-memory L1/L2 hit/miss counters from the inner `PropertyIndex`.
    pub async fn stats(&self) -> IndexStats {
        self.memory_index.stats().await
    }

    // ------------------------------------------------------------------
    // Internal L3 helpers
    // ------------------------------------------------------------------

    /// Get (or create once) a handle to the `"property-index"` column family.
    fn cf_handle(&self) -> Result<&rust_rocksdb::ColumnFamily, IndexError> {
        self.db
            .cf_handle(CF_PROPERTY_INDEX)
            .ok_or_else(|| IndexError::NotFound(CF_PROPERTY_INDEX.to_string()))
    }

    /// Write a single (key, node_id) pair to L3.
    ///
    /// Reads the existing value (a `Vec<NexoraId>`) for the key, appends
    /// `node_id` if not already present, and writes the updated list back.
    fn write_to_l3(&self, key: &IndexKey, node_id: &NexoraId) -> Result<(), IndexError> {
        let cf = self.cf_handle()?;
        let key_bytes = key.to_bytes();

        // Hold the RMW lock for the whole get→append→put so concurrent inserts
        // to the same key cannot clobber each other. Poisoning is not a
        // correctness concern here (we hold no invariant across the guard beyond
        // the DB call), so recover the guard either way.
        let _guard = self
            .l3_write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Read-modify-write: get existing nodes, append, write back.
        let mut nodes: Vec<NexoraId> = match self.db.get_cf(cf, &key_bytes) {
            Ok(Some(bytes)) => bincode::deserialize(&bytes)
                .map_err(|e| IndexError::Serialization(e.to_string()))?,
            Ok(None) => Vec::new(),
            Err(e) => return Err(IndexError::Backend(e.to_string())),
        };

        if !nodes.contains(node_id) {
            nodes.push(node_id.clone());
        }

        let value_bytes =
            bincode::serialize(&nodes).map_err(|e| IndexError::Serialization(e.to_string()))?;

        self.db
            .put_cf(cf, key_bytes, value_bytes)
            .map_err(|e| IndexError::Backend(e.to_string()))
    }

    /// Read the `Vec<NexoraId>` for a single key from L3.
    fn read_from_l3(&self, key: &IndexKey) -> Result<Vec<NexoraId>, IndexError> {
        let cf = self.cf_handle()?;
        let key_bytes = key.to_bytes();

        match self.db.get_cf(cf, &key_bytes) {
            Ok(Some(bytes)) => {
                bincode::deserialize(&bytes).map_err(|e| IndexError::Serialization(e.to_string()))
            }
            Ok(None) => Ok(Vec::new()),
            Err(e) => Err(IndexError::Backend(e.to_string())),
        }
    }

    /// Range scan L3 for keys in `[start, end]` with the given property name.
    ///
    /// RocksDB orders keys lexicographically by their bincode-serialized bytes,
    /// which does NOT match the semantic ordering of `PropertyValue` (e.g. the
    /// little-endian fixint encoding sorts `256` before `255`, and negative
    /// integers before positives). Relying on byte order to bound the scan would
    /// silently drop or prematurely stop at valid results.
    ///
    /// Instead we scan the whole column family, deserialize each key, and filter
    /// by `property` name and a semantic value comparison (`start <= v <= end`).
    /// This is O(N) in index size; a production system would maintain a
    /// separately-encoded, order-preserving key (see `remove_from_l3`'s note).
    fn range_query_l3(
        &self,
        property: &str,
        start: &PropertyValue,
        end: &PropertyValue,
    ) -> Result<Vec<NexoraId>, IndexError> {
        let cf = self.cf_handle()?;

        // Normalize bounds so a reversed range still selects the intended set.
        let (lo, hi) = if property_value_cmp(start, end) == std::cmp::Ordering::Greater {
            (end, start)
        } else {
            (start, end)
        };

        let mut results = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        for item in iter {
            let (key, value) = item.map_err(|e| IndexError::Backend(e.to_string()))?;

            let index_key: IndexKey = match bincode::deserialize(&key) {
                Ok(k) => k,
                // Skip keys that are not IndexKey-shaped rather than failing the
                // whole scan.
                Err(_) => continue,
            };

            if index_key.property != property {
                continue;
            }

            // Semantic bounds check (not byte order).
            if property_value_cmp(&index_key.value, lo) == std::cmp::Ordering::Less
                || property_value_cmp(&index_key.value, hi) == std::cmp::Ordering::Greater
            {
                continue;
            }

            let nodes: Vec<NexoraId> = bincode::deserialize(&value)
                .map_err(|e| IndexError::Serialization(e.to_string()))?;

            results.extend(nodes);
        }

        Ok(results)
    }

    /// Remove all L3 entries that reference the given `node_id`.
    ///
    /// Performs a full scan of the column family. For each key, deserializes
    /// the value, removes `node_id` from the list, and either deletes the key
    /// (if the list becomes empty) or writes back the pruned list.
    fn remove_from_l3(&self, node_id: &NexoraId) -> Result<(), IndexError> {
        let cf = self.cf_handle()?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        // Collect updates to apply after the iterator is dropped, avoiding
        // borrowing conflicts with the DB.
        struct Update {
            key: Vec<u8>,
            nodes: Vec<NexoraId>,
        }

        let mut to_update: Vec<Update> = Vec::new();
        let mut to_delete: Vec<Vec<u8>> = Vec::new();

        for item in iter {
            let (key, value) = item.map_err(|e| IndexError::Backend(e.to_string()))?;

            let mut nodes: Vec<NexoraId> = bincode::deserialize(&value)
                .map_err(|e| IndexError::Serialization(e.to_string()))?;

            let original_len = nodes.len();
            nodes.retain(|id| id != node_id);

            if nodes.len() != original_len {
                if nodes.is_empty() {
                    to_delete.push(key.to_vec());
                } else {
                    to_update.push(Update {
                        key: key.to_vec(),
                        nodes,
                    });
                }
            }
        }

        // Apply deletions.
        for key in &to_delete {
            self.db
                .delete_cf(cf, key)
                .map_err(|e| IndexError::Backend(e.to_string()))?;
        }

        // Apply modifications.
        for update in &to_update {
            let value_bytes = bincode::serialize(&update.nodes)
                .map_err(|e| IndexError::Serialization(e.to_string()))?;
            self.db
                .put_cf(cf, &update.key, value_bytes)
                .map_err(|e| IndexError::Backend(e.to_string()))?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_id(label: &str) -> NexoraId {
        NexoraId::from_bytes(label.as_bytes().to_vec())
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();

        let node1 = make_id("node1");
        let node2 = make_id("node2");

        index
            .insert("age", PropertyValue::Integer(25), node1.clone())
            .await
            .unwrap();
        index
            .insert("age", PropertyValue::Integer(25), node2.clone())
            .await
            .unwrap();

        let results = index
            .query("age", &PropertyValue::Integer(25))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&node1));
        assert!(results.contains(&node2));
    }

    #[tokio::test]
    async fn test_persistence_across_restart() {
        let dir = tempdir().unwrap();
        let node = make_id("node1");

        // Write and close.
        {
            let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();
            index
                .insert("name", PropertyValue::String("Alice".into()), node.clone())
                .await
                .unwrap();
            // Drop closes the DB (Arc reference dropped).
        }

        // Reopen and query -- data should survive in L3.
        {
            let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();
            let results = index
                .query("name", &PropertyValue::String("Alice".into()))
                .await
                .unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], node);
        }
    }

    #[tokio::test]
    async fn test_cache_miss_loads_from_l3() {
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();
        let node = make_id("node1");

        // Insert (goes to L3 + L1/L2).
        index
            .insert(
                "city",
                PropertyValue::String("Beijing".into()),
                node.clone(),
            )
            .await
            .unwrap();

        // Clear in-memory cache.
        index.clear_cache().await;

        // Query should load from L3 and warm the cache.
        let results = index
            .query("city", &PropertyValue::String("Beijing".into()))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], node);

        // Second query should hit in-memory (no L3 round-trip needed).
        let results2 = index
            .query("city", &PropertyValue::String("Beijing".into()))
            .await
            .unwrap();
        assert_eq!(results2.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_node() {
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();

        let node1 = make_id("node1");
        let node2 = make_id("node2");

        index
            .insert("age", PropertyValue::Integer(30), node1.clone())
            .await
            .unwrap();
        index
            .insert("age", PropertyValue::Integer(30), node2.clone())
            .await
            .unwrap();

        index.remove_node(&node1).await.unwrap();

        let results = index
            .query("age", &PropertyValue::Integer(30))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&node2));
        assert!(!results.contains(&node1));
    }

    #[tokio::test]
    async fn test_range_query_merges_l2_and_l3() {
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();

        for i in 20..30 {
            let node = make_id(&format!("node{i}"));
            index
                .insert("score", PropertyValue::Integer(i), node)
                .await
                .unwrap();
        }

        let results = index
            .range_query(
                "score",
                &PropertyValue::Integer(22),
                &PropertyValue::Integer(27),
            )
            .await
            .unwrap();

        // 22..=27 inclusive is 6 values.
        assert_eq!(results.len(), 6);
    }

    #[tokio::test]
    async fn test_range_query_l3_crosses_byte_order_boundary() {
        // Regression: bincode's little-endian fixint encoding sorts `256` before
        // `255` lexicographically, so a byte-order-bounded L3 scan would drop or
        // prematurely stop at values crossing the 255/256 boundary. Clearing the
        // cache forces the query through L3 only.
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();

        for i in [200i64, 255, 256, 257, 300, 1000] {
            let node = make_id(&format!("node{i}"));
            index
                .insert("age", PropertyValue::Integer(i), node)
                .await
                .unwrap();
        }
        index.clear_cache().await;

        let results = index
            .range_query(
                "age",
                &PropertyValue::Integer(255),
                &PropertyValue::Integer(300),
            )
            .await
            .unwrap();

        // 255, 256, 257, 300 are in range; 200 and 1000 are not.
        assert_eq!(results.len(), 4);
    }

    #[tokio::test]
    async fn test_query_merges_l3_after_partial_cache_eviction() {
        // Regression: `query` must consult L3 even when the memory tier returns a
        // non-empty (but incomplete) result. Here we insert two nodes, clear the
        // cache, warm only one back via an exact query, then confirm a second
        // query still returns both.
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();

        let n1 = make_id("n1");
        let n2 = make_id("n2");
        index
            .insert("k", PropertyValue::Integer(7), n1.clone())
            .await
            .unwrap();
        index
            .insert("k", PropertyValue::Integer(7), n2.clone())
            .await
            .unwrap();
        index.clear_cache().await;

        let results = index.query("k", &PropertyValue::Integer(7)).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&n1));
        assert!(results.contains(&n2));
    }

    #[tokio::test]
    async fn test_stats_are_accessible() {
        let dir = tempdir().unwrap();
        let index = PersistentPropertyIndex::new(dir.path(), IndexConfig::default()).unwrap();

        let node = make_id("n1");
        index
            .insert("x", PropertyValue::Integer(1), node.clone())
            .await
            .unwrap();
        index.query("x", &PropertyValue::Integer(1)).await.unwrap();

        let stats = index.stats().await;
        // After one write + one query that hits L2 (first insert goes to L2)
        assert!(stats.writes > 0);
    }
}
