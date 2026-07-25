//! Property index system for fast property-based queries.
//!
//! Implements a two-tier hybrid index (L1 + L2 in-memory):
//! - L1: Hash index (hot data, in-memory, O(1) lookups)
//! - L2: BTree index (warm data, in-memory, range queries)
//!
//! For L3 (persistent LSM storage), use PropertyIndexWithPersistence
//! in the nexora-persistor-rocksdb crate.

use dashmap::DashMap;
use nexora_id::{NexoraId, PropertyValue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Index not found: {0}")]
    NotFound(String),

    #[error("Backend error: {0}")]
    Backend(String),
}

/// Property index key: (property_name, property_value)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexKey {
    property: String,
    value: PropertyValue,
}

impl PartialEq for IndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.property == other.property && self.value == other.value
    }
}

impl Eq for IndexKey {}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare property names
        match self.property.cmp(&other.property) {
            std::cmp::Ordering::Equal => {
                // Compare values using a deterministic binary encoding that
                // preserves ordering and avoids JSON serialization overhead.
                property_value_cmp(&self.value, &other.value)
            }
            other => other,
        }
    }
}

/// Compare two PropertyValues for ordering using a lightweight binary encoding.
/// This avoids the O(n) cost of JSON serialization on every BTree comparison.
fn property_value_cmp(a: &PropertyValue, b: &PropertyValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Type discriminant for ordering: Null(0) < Bool(1) < Int(2) < Float(3)
    // < String(4) < Bytes(5) < Complex types(6+)
    fn discriminant(v: &PropertyValue) -> u8 {
        match v {
            PropertyValue::Null => 0,
            PropertyValue::Boolean(_) => 1,
            PropertyValue::Integer(_) => 2,
            PropertyValue::Float(_) => 3,
            PropertyValue::String(_) => 4,
            PropertyValue::Bytes(_) => 5,
            PropertyValue::List(_)
            | PropertyValue::Map(_)
            | PropertyValue::Node(_)
            | PropertyValue::Relationship(_)
            | PropertyValue::Path(_)
            | PropertyValue::Date(_)
            | PropertyValue::LocalDateTime(_)
            | PropertyValue::ZonedDateTime(_)
            | PropertyValue::Duration(_)
            | PropertyValue::Point(_)
            | PropertyValue::BlobRef(_) => 6,
        }
    }

    let da = discriminant(a);
    let db = discriminant(b);
    if da != db {
        return da.cmp(&db);
    }

    // Same type — compare by value
    match (a, b) {
        (PropertyValue::Null, PropertyValue::Null) => Ordering::Equal,
        (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => a.cmp(b),
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            // IEEE-754 total ordering. A raw `to_bits().cmp()` is WRONG: negative
            // floats have the sign bit set, so their bit patterns compare *greater*
            // than any positive float, and larger-magnitude negatives sort after
            // smaller ones. That makes the BTree key order non-monotonic and makes
            // `range(start..=end)` panic when start > end for a valid numeric range.
            //
            // The standard fix maps f64 bits to a monotonic u64: flip all bits for
            // negatives (sign bit set), and flip only the sign bit for non-negatives.
            fn total_order_key(f: f64) -> u64 {
                let bits = f.to_bits();
                if bits & 0x8000_0000_0000_0000 != 0 {
                    // Negative (including -0.0 and -NaN): invert all bits.
                    !bits
                } else {
                    // Non-negative: set the sign bit so it sorts above negatives.
                    bits | 0x8000_0000_0000_0000
                }
            }
            total_order_key(*a).cmp(&total_order_key(*b))
        }
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        (PropertyValue::Bytes(a), PropertyValue::Bytes(b)) => a.cmp(b),
        // Complex types fall back to JSON serialization (rare in indexing)
        _ => {
            let a_json = serde_json::to_string(a).unwrap_or_default();
            let b_json = serde_json::to_string(b).unwrap_or_default();
            a_json.cmp(&b_json)
        }
    }
}

impl std::hash::Hash for IndexKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.property.hash(state);
        // Custom hash for PropertyValue
        match &self.value {
            PropertyValue::Null => {
                0u8.hash(state);
            }
            PropertyValue::Boolean(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            PropertyValue::Integer(i) => {
                2u8.hash(state);
                i.hash(state);
            }
            PropertyValue::Float(f) => {
                3u8.hash(state);
                // For floats, use bits representation
                f.to_bits().hash(state);
            }
            PropertyValue::String(s) => {
                4u8.hash(state);
                s.hash(state);
            }
            PropertyValue::Bytes(b) => {
                5u8.hash(state);
                b.hash(state);
            }
            PropertyValue::List(_)
            | PropertyValue::Map(_)
            | PropertyValue::Node(_)
            | PropertyValue::Relationship(_)
            | PropertyValue::Path(_)
            | PropertyValue::Date(_)
            | PropertyValue::LocalDateTime(_)
            | PropertyValue::ZonedDateTime(_)
            | PropertyValue::Duration(_)
            | PropertyValue::Point(_)
            | PropertyValue::BlobRef(_) => {
                // For complex types, use serialized form
                6u8.hash(state);
                serde_json::to_string(&self.value)
                    .unwrap_or_default()
                    .hash(state);
            }
        }
    }
}

impl IndexKey {
    fn new(property: impl Into<String>, value: PropertyValue) -> Self {
        Self {
            property: property.into(),
            value,
        }
    }
}

/// Statistics for index performance monitoring
#[derive(Debug, Default, Clone)]
pub struct IndexStats {
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub l3_hits: u64,
    pub misses: u64,
    pub writes: u64,
}

/// Live counters backing [`IndexStats`]. Atomics so the hit/miss/write
/// bookkeeping on the query and insert hot paths never takes a write lock (the
/// old design took `stats.write()` / `try_write()` on every op).
#[derive(Debug, Default)]
struct StatCounters {
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    l3_hits: AtomicU64,
    misses: AtomicU64,
    writes: AtomicU64,
}

/// Two-tier hybrid property index (in-memory only)
///
/// For persistent storage, integrate with RocksDB via the
/// nexora-persistor-rocksdb crate.
///
/// # Sharding
///
/// L1 and the access-count tracker are `DashMap`s (sharded locks). L2 is a
/// `DashMap` keyed by **property name**, each entry a `BTreeMap` over that one
/// property's values — so range queries (always scoped to a single property)
/// keep their efficient ordered scan, while writes to different properties
/// contend only on their segment lock instead of a graph-wide L2 lock.
pub struct PropertyIndex {
    /// L1: Hash index for hot data (most frequently accessed)
    hot_hash: Arc<DashMap<IndexKey, Vec<NexoraId>>>,

    /// L2: per-property BTree index for warm data (sorted for range queries).
    /// Outer key is the property name; the inner `BTreeMap` is keyed by the
    /// full `IndexKey` (all keys in one entry share that property), preserving
    /// the exact ordering semantics of the previous single-BTree design.
    warm_btree: Arc<DashMap<String, BTreeMap<IndexKey, Vec<NexoraId>>>>,

    /// Total distinct keys across all L2 property shards, for global capacity
    /// enforcement without summing every shard on each insert.
    l2_total: Arc<AtomicUsize>,

    /// LRU tracking for cache management
    access_count: Arc<DashMap<IndexKey, u64>>,

    /// Configuration
    config: IndexConfig,

    /// Statistics
    stats: Arc<StatCounters>,
}

#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Maximum entries in L1 hash index
    pub l1_max_entries: usize,

    /// Maximum entries in L2 btree index
    pub l2_max_entries: usize,

    /// Access count threshold for promotion to L1
    pub l1_promotion_threshold: u64,

    /// Maximum entries in access_count tracking (prevents memory leak)
    pub access_count_max_entries: usize,

    /// Enable statistics collection
    pub enable_stats: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            l1_max_entries: 10_000,           // ~10K hot keys
            l2_max_entries: 100_000,          // ~100K warm keys
            l1_promotion_threshold: 10,       // Promote after 10 accesses
            access_count_max_entries: 50_000, // Cap access_count to prevent memory leak
            enable_stats: true,
        }
    }
}

impl PropertyIndex {
    /// Create a new property index with default configuration
    pub fn new() -> Self {
        Self::new_with_config(IndexConfig::default())
    }

    /// Create with custom configuration
    pub fn new_with_config(config: IndexConfig) -> Self {
        Self {
            hot_hash: Arc::new(DashMap::new()),
            warm_btree: Arc::new(DashMap::new()),
            l2_total: Arc::new(AtomicUsize::new(0)),
            access_count: Arc::new(DashMap::new()),
            config,
            stats: Arc::new(StatCounters::default()),
        }
    }

    /// Insert a property index entry
    pub async fn insert(
        &self,
        property: impl Into<String>,
        value: PropertyValue,
        node_id: NexoraId,
    ) -> Result<(), IndexError> {
        let key = IndexKey::new(property, value);

        // Update statistics
        if self.config.enable_stats {
            self.stats.writes.fetch_add(1, Ordering::Relaxed);
        }

        // If this key was promoted to L1, keep L1 authoritative: a query hits L1
        // first and returns immediately, so a node added only to L2 would be
        // invisible. Append to the L1 bucket when present.
        if let Some(mut nodes) = self.hot_hash.get_mut(&key) {
            if !nodes.contains(&node_id) {
                nodes.push(node_id.clone());
            }
            // Mirror into L2 below so eviction from L1 does not lose the node.
        }

        // Add to L2 (warm cache), sharded by property name so writes to
        // different properties don't serialize on a single L2 lock.
        {
            let mut shard = self.warm_btree.entry(key.property.clone()).or_default();

            // Only evict when inserting a genuinely new key would exceed the
            // global L2 capacity. Appending a node to an existing key does not
            // grow the map, so it must not trigger eviction of an unrelated key
            // (which would silently drop that key's entire index entry).
            let is_new_key = !shard.contains_key(&key);
            if is_new_key && self.l2_total.load(Ordering::Relaxed) >= self.config.l2_max_entries {
                // Evict the first entry within this property shard (simple FIFO),
                // but never the key we are about to insert.
                if let Some(first_key) = shard.keys().next().cloned() {
                    if first_key != key {
                        shard.remove(&first_key);
                        self.l2_total.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }

            if is_new_key {
                self.l2_total.fetch_add(1, Ordering::Relaxed);
            }
            let nodes = shard.entry(key).or_default();
            if !nodes.contains(&node_id) {
                nodes.push(node_id);
            }
        }

        Ok(())
    }

    /// Query for nodes with exact property match
    pub async fn query(
        &self,
        property: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NexoraId>, IndexError> {
        let key = IndexKey::new(property, value.clone());

        // L1: Check hash index (hottest data). Clone out and drop the entry ref
        // before any further map access so we never hold a `DashMap` guard
        // across the promotion/access bookkeeping below.
        if let Some(nodes) = self.hot_hash.get(&key).map(|n| n.clone()) {
            self.record_hit(1);
            self.record_access(&key);
            return Ok(nodes);
        }

        // L2: Check BTree index (warm data), in this property's shard.
        let l2_nodes = self
            .warm_btree
            .get(&key.property)
            .and_then(|shard| shard.get(&key).cloned());
        if let Some(nodes) = l2_nodes {
            self.record_hit(2);
            self.record_access(&key);

            // Maybe promote to L1 if accessed frequently
            self.maybe_promote_to_l1(&key, nodes.clone());

            return Ok(nodes);
        }

        // Not found
        self.record_miss();
        Ok(Vec::new())
    }

    /// Range query: property >= start AND property <= end
    pub async fn range_query(
        &self,
        property: &str,
        start: &PropertyValue,
        end: &PropertyValue,
    ) -> Result<Vec<NexoraId>, IndexError> {
        let start_key = IndexKey::new(property, start.clone());
        let end_key = IndexKey::new(property, end.clone());

        // BTreeMap::range panics if start > end. Normalize the bounds so a
        // reversed range (or one that sorts unexpectedly under the total order)
        // yields the correct result instead of crashing the engine.
        let (lo, hi) = if start_key <= end_key {
            (start_key, end_key)
        } else {
            (end_key, start_key)
        };

        let mut results = Vec::new();

        // Check L2. Range scans are always scoped to one property, which is
        // exactly one shard here — so the ordered BTree scan is unchanged.
        if let Some(shard) = self.warm_btree.get(property) {
            for (_, nodes) in shard.range(lo.clone()..=hi.clone()) {
                results.extend(nodes.iter().cloned());
            }
        }

        // Include any keys promoted to L1 that fall within the range: L1 is
        // authoritative for promoted keys, so scanning only L2 could miss nodes.
        for entry in self.hot_hash.iter() {
            let k = entry.key();
            if k.property == property && *k >= lo && *k <= hi {
                results.extend(entry.value().iter().cloned());
            }
        }

        // Deduplicate (NexoraId is not Ord, so we need a different approach)
        results.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        results.dedup();

        Ok(results)
    }

    /// Remove all index entries for a node
    pub async fn remove_node(&self, node_id: &NexoraId) -> Result<(), IndexError> {
        // Remove from L1: drop the node from each bucket, prune emptied keys.
        self.hot_hash.iter_mut().for_each(|mut entry| {
            entry.value_mut().retain(|id| id != node_id);
        });
        self.hot_hash.retain(|_, nodes| !nodes.is_empty());

        // Remove from L2 (per-property shards), keeping the global key counter
        // in step with the keys pruned.
        self.warm_btree.iter_mut().for_each(|mut shard| {
            let before = shard.len();
            shard.retain(|_, nodes| {
                nodes.retain(|id| id != node_id);
                !nodes.is_empty()
            });
            let pruned = before - shard.len();
            if pruned > 0 {
                self.l2_total.fetch_sub(pruned, Ordering::Relaxed);
            }
        });
        // Drop property shards left with no keys at all.
        self.warm_btree.retain(|_, shard| !shard.is_empty());

        Ok(())
    }

    /// Get current statistics
    pub async fn stats(&self) -> IndexStats {
        IndexStats {
            l1_hits: self.stats.l1_hits.load(Ordering::Relaxed),
            l2_hits: self.stats.l2_hits.load(Ordering::Relaxed),
            l3_hits: self.stats.l3_hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            writes: self.stats.writes.load(Ordering::Relaxed),
        }
    }

    /// Clear all caches (L1 and L2)
    pub async fn clear(&self) {
        self.hot_hash.clear();
        self.warm_btree.clear();
        self.l2_total.store(0, Ordering::Relaxed);
        self.access_count.clear();
    }

    /// Get current cache sizes
    pub async fn cache_sizes(&self) -> (usize, usize) {
        let l1_size = self.hot_hash.len();
        let l2_size = self.l2_total.load(Ordering::Relaxed);
        (l1_size, l2_size)
    }

    // Internal methods

    /// Record an access to a key for L1 promotion tracking.
    /// FIX P0-3: Evicts oldest entries when access_count exceeds max capacity.
    fn record_access(&self, key: &IndexKey) {
        // Evict if over capacity (simple: remove ~25% of entries). `DashMap`
        // iteration order is unspecified, which is fine for this approximate
        // LRU shedding.
        if self.access_count.len() >= self.config.access_count_max_entries {
            let evict_count = self.access_count.len() / 4;
            let keys_to_evict: Vec<_> = self
                .access_count
                .iter()
                .take(evict_count)
                .map(|e| e.key().clone())
                .collect();
            for k in keys_to_evict {
                self.access_count.remove(&k);
            }
        }

        *self.access_count.entry(key.clone()).or_insert(0) += 1;
    }

    /// FIX P0-3: Clean up access_count entry after promotion to L1.
    fn maybe_promote_to_l1(&self, key: &IndexKey, nodes: Vec<NexoraId>) {
        let should_promote = self.access_count.get(key).map(|c| *c).unwrap_or(0)
            >= self.config.l1_promotion_threshold;

        if should_promote {
            // Evict if over capacity (simple FIFO). Skip if the key is already
            // resident so we don't churn on repeated promotions of the same key.
            if !self.hot_hash.contains_key(key) && self.hot_hash.len() >= self.config.l1_max_entries
            {
                if let Some(first_key) = self.hot_hash.iter().next().map(|e| e.key().clone()) {
                    self.hot_hash.remove(&first_key);
                }
            }

            self.hot_hash.insert(key.clone(), nodes);

            // FIX P0-3: Remove from access_count after promotion to prevent unbounded growth
            self.access_count.remove(key);
        }
    }

    fn record_hit(&self, level: u8) {
        if !self.config.enable_stats {
            return;
        }
        match level {
            1 => self.stats.l1_hits.fetch_add(1, Ordering::Relaxed),
            2 => self.stats.l2_hits.fetch_add(1, Ordering::Relaxed),
            3 => self.stats.l3_hits.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    fn record_miss(&self) {
        if !self.config.enable_stats {
            return;
        }
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for PropertyIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_insert_and_query() {
        let index = PropertyIndex::new();

        let node1 = NexoraId::from_bytes(b"node1".to_vec());
        let node2 = NexoraId::from_bytes(b"node2".to_vec());

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
    async fn test_range_query() {
        let index = PropertyIndex::new();

        for i in 20..30 {
            let node = NexoraId::from_bytes(format!("node{}", i).into_bytes());
            index
                .insert("age", PropertyValue::Integer(i), node.clone())
                .await
                .unwrap();
        }

        let results = index
            .range_query(
                "age",
                &PropertyValue::Integer(22),
                &PropertyValue::Integer(27),
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 6); // 22, 23, 24, 25, 26, 27
    }

    #[tokio::test]
    async fn test_l1_promotion() {
        let config = IndexConfig {
            l1_promotion_threshold: 3,
            ..Default::default()
        };

        let index = PropertyIndex::new_with_config(config);
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index
            .insert("name", PropertyValue::String("Alice".into()), node.clone())
            .await
            .unwrap();

        // Query multiple times to trigger L1 promotion
        for _ in 0..5 {
            index
                .query("name", &PropertyValue::String("Alice".into()))
                .await
                .unwrap();
        }

        let stats = index.stats().await;
        assert!(stats.l1_hits > 0, "Should have L1 hits after promotion");
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let config = IndexConfig {
            l2_max_entries: 3,
            ..Default::default()
        };

        let index = PropertyIndex::new_with_config(config);

        // Insert 5 entries (should evict 2)
        for i in 0..5 {
            let node = NexoraId::from_bytes(format!("node{}", i).into_bytes());
            index
                .insert("id", PropertyValue::Integer(i), node.clone())
                .await
                .unwrap();
        }

        let (_, l2_size) = index.cache_sizes().await;
        assert!(l2_size <= 3, "L2 should be capped at max_entries");
    }

    #[tokio::test]
    async fn test_remove_node() {
        let index = PropertyIndex::new();

        let node1 = NexoraId::from_bytes(b"node1".to_vec());
        let node2 = NexoraId::from_bytes(b"node2".to_vec());

        index
            .insert("age", PropertyValue::Integer(25), node1.clone())
            .await
            .unwrap();
        index
            .insert("age", PropertyValue::Integer(25), node2.clone())
            .await
            .unwrap();

        index.remove_node(&node1).await.unwrap();

        let results = index
            .query("age", &PropertyValue::Integer(25))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&node2));
        assert!(!results.contains(&node1));
    }

    #[tokio::test]
    async fn test_statistics() {
        let index = PropertyIndex::new();
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index
            .insert("age", PropertyValue::Integer(25), node.clone())
            .await
            .unwrap();
        index
            .query("age", &PropertyValue::Integer(25))
            .await
            .unwrap();
        index
            .query("age", &PropertyValue::Integer(99))
            .await
            .unwrap();

        let stats = index.stats().await;
        assert_eq!(stats.writes, 1);
        assert!(stats.l2_hits > 0);
        assert!(stats.misses > 0);
    }

    /// Many tasks inserting distinct properties concurrently. Under the
    /// property-sharded L2 these contend only on segment locks, not a graph-wide
    /// L2 lock; the assertion is correctness (and lazy stats/size accuracy).
    #[tokio::test]
    async fn test_concurrent_insert_distinct_properties() {
        let index = Arc::new(PropertyIndex::new());
        let mut handles = Vec::new();

        for p in 0..64u32 {
            let index = index.clone();
            handles.push(tokio::spawn(async move {
                for v in 0..25i64 {
                    let node = NexoraId::from_bytes(format!("p{p}-v{v}").into_bytes());
                    index
                        .insert(format!("prop{p}"), PropertyValue::Integer(v), node)
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Each (prop, value) pair holds exactly one node.
        let hits = index
            .query("prop0", &PropertyValue::Integer(0))
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        let (_, l2) = index.cache_sizes().await;
        assert_eq!(l2, 64 * 25, "l2 total must track all distinct keys");
        assert_eq!(index.stats().await.writes, 64 * 25);
    }

    /// Range query must still return the correct ordered span after sharding L2
    /// by property — the range lives entirely within one property's shard.
    #[tokio::test]
    async fn test_range_query_after_promotion_to_l1() {
        // Low promotion threshold so repeated point queries move keys into L1,
        // then a range query must still find those promoted keys (L1 is
        // authoritative) alongside the ones left in L2.
        let config = IndexConfig {
            l1_promotion_threshold: 2,
            ..Default::default()
        };
        let index = PropertyIndex::new_with_config(config);
        for i in 0..10i64 {
            let node = NexoraId::from_bytes(format!("n{i}").into_bytes());
            index
                .insert("age", PropertyValue::Integer(i), node)
                .await
                .unwrap();
        }

        // Promote a couple of keys inside the range into L1.
        for _ in 0..3 {
            index
                .query("age", &PropertyValue::Integer(3))
                .await
                .unwrap();
            index
                .query("age", &PropertyValue::Integer(5))
                .await
                .unwrap();
        }

        let results = index
            .range_query(
                "age",
                &PropertyValue::Integer(2),
                &PropertyValue::Integer(7),
            )
            .await
            .unwrap();
        // 2,3,4,5,6,7 — including the L1-promoted 3 and 5, no duplicates.
        assert_eq!(
            results.len(),
            6,
            "range must union L1-promoted keys without dupes"
        );
    }
}
