//! Label index for fast label-based node lookups.
//!
//! Provides O(1) lookup for nodes by label, supporting common graph queries like:
//! - MATCH (n:Person) RETURN n
//! - MATCH (n:Product:Electronics) RETURN n (intersection of multiple labels)
//!
//! The index maintains a mapping from label → set of node IDs.

use dashmap::DashMap;
use nexora_id::NexoraId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Statistics for label index monitoring
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LabelIndexStats {
    /// Total number of unique labels
    pub total_labels: usize,

    /// Total number of indexed nodes
    pub total_nodes: usize,

    /// Number of label queries
    pub queries: u64,

    /// Number of multi-label (intersection) queries
    pub intersection_queries: u64,
}

/// Label index for fast label-based lookups
///
/// # Examples
///
/// ```
/// use nexora_core::LabelIndex;
/// use nexora_id::NexoraId;
///
/// # #[tokio::main]
/// # async fn main() {
/// let index = LabelIndex::new();
/// let node = NexoraId::from_bytes(b"node1".to_vec());
///
/// // Add labels
/// index.add_label("Person", node.clone()).await;
/// index.add_label("Employee", node.clone()).await;
///
/// // Query by single label
/// let persons = index.query("Person").await;
/// assert_eq!(persons.len(), 1);
///
/// // Query by multiple labels (intersection)
/// let person_employees = index.query_all(&["Person", "Employee"]).await;
/// assert_eq!(person_employees.len(), 1);
/// # }
/// ```
pub struct LabelIndex {
    /// label -> set of node IDs.
    ///
    /// A `DashMap` (sharded lock) rather than a single `RwLock<HashMap>` so
    /// writes to different labels contend only on their internal segment lock
    /// instead of a graph-wide write lock. The inner `HashSet` is guarded by the
    /// per-entry lock `DashMap` already provides.
    index: Arc<DashMap<String, HashSet<NexoraId>>>,

    /// Query counters. Kept as atomics so read-path stat bookkeeping never takes
    /// a write lock (the old design took `stats.write()` on every `query`).
    queries: Arc<AtomicU64>,
    intersection_queries: Arc<AtomicU64>,
}

impl LabelIndex {
    /// Create a new label index
    pub fn new() -> Self {
        Self {
            index: Arc::new(DashMap::new()),
            queries: Arc::new(AtomicU64::new(0)),
            intersection_queries: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Add a label to a node
    pub async fn add_label(&self, label: impl Into<String>, node_id: NexoraId) {
        self.index.entry(label.into()).or_default().insert(node_id);
    }

    /// Add multiple labels to a node
    pub async fn add_labels(&self, labels: &[impl AsRef<str>], node_id: NexoraId) {
        for label in labels {
            self.add_label(label.as_ref(), node_id.clone()).await;
        }
    }

    /// Remove a label from a node
    pub async fn remove_label(&self, label: &str, node_id: &NexoraId) {
        // Remove the empty set entry atomically: `remove_if` holds the entry lock
        // across the emptiness check so a concurrent `add_label` can't insert
        // into a set that's about to be dropped.
        if let Some(mut nodes) = self.index.get_mut(label) {
            nodes.remove(node_id);
        }
        self.index.remove_if(label, |_, nodes| nodes.is_empty());
    }

    /// Remove all labels from a node
    pub async fn remove_node(&self, node_id: &NexoraId) {
        // Drop the node from every label set, then prune sets left empty.
        self.index.iter_mut().for_each(|mut entry| {
            entry.value_mut().remove(node_id);
        });
        self.index.retain(|_, nodes| !nodes.is_empty());
    }

    /// Query nodes by a single label
    ///
    /// Returns all nodes that have the given label.
    /// Time complexity: O(1) average case
    pub async fn query(&self, label: &str) -> Vec<NexoraId> {
        self.queries.fetch_add(1, Ordering::Relaxed);

        self.index
            .get(label)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Query nodes by multiple labels (intersection)
    ///
    /// Returns nodes that have ALL of the given labels.
    /// Time complexity: O(k * n) where k = number of labels, n = smallest label set size
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Note: LabelIndex methods are async.
    /// // let index = LabelIndex::new();
    /// // index.add_label("Person", node.clone()).await;
    /// // let nodes = index.query_all(&["Person", "Employee"]).await;
    /// ```
    pub async fn query_all(&self, labels: &[&str]) -> Vec<NexoraId> {
        if labels.is_empty() {
            return Vec::new();
        }

        self.intersection_queries.fetch_add(1, Ordering::Relaxed);

        // Snapshot each label's set. We clone rather than hold `DashMap` refs
        // across the intersection: holding refs to multiple entries risks a
        // deadlock against a concurrent writer touching the same segments.
        let mut sets: Vec<HashSet<NexoraId>> = Vec::with_capacity(labels.len());
        for label in labels {
            match self.index.get(*label) {
                Some(set) => sets.push(set.clone()),
                None => return Vec::new(), // If any label doesn't exist, intersection is empty
            }
        }

        // Sort by size (smallest first) for efficiency
        sets.sort_by_key(|set| set.len());

        // Start with the smallest set and intersect with others
        let mut iter = sets.into_iter();
        let mut result = iter.next().unwrap_or_default();

        for set in iter {
            result.retain(|node| set.contains(node));

            // Early exit if intersection becomes empty
            if result.is_empty() {
                break;
            }
        }

        result.into_iter().collect()
    }

    /// Query nodes by any of the given labels (union)
    ///
    /// Returns nodes that have ANY of the given labels.
    /// Time complexity: O(k * n) where k = number of labels, n = average label set size
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Note: LabelIndex methods are async.
    /// // let index = LabelIndex::new();
    /// // index.add_label("Person", node.clone()).await;
    /// // let nodes = index.query_any(&["Person", "Product"]).await;
    /// ```
    pub async fn query_any(&self, labels: &[&str]) -> Vec<NexoraId> {
        let mut result = HashSet::new();

        for label in labels {
            if let Some(nodes) = self.index.get(*label) {
                result.extend(nodes.iter().cloned());
            }
        }

        result.into_iter().collect()
    }

    /// Check if a node has a specific label
    pub async fn has_label(&self, label: &str, node_id: &NexoraId) -> bool {
        self.index
            .get(label)
            .map(|set| set.contains(node_id))
            .unwrap_or(false)
    }

    /// Get all labels for a node
    ///
    /// Note: This is O(L * N) where L = total labels, N = avg nodes per label.
    /// For frequent use, maintain a reverse index (node_id -> labels).
    pub async fn get_labels(&self, node_id: &NexoraId) -> Vec<String> {
        self.index
            .iter()
            .filter(|entry| entry.value().contains(node_id))
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get all labels in the index
    pub async fn all_labels(&self) -> Vec<String> {
        self.index.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get statistics
    ///
    /// `total_labels`/`total_nodes` are computed on demand here rather than
    /// maintained on every write. The old design recomputed `total_nodes` (an
    /// O(labels) sum over all sets) inside every `add_label`/`remove_label`
    /// under the write lock — a per-write cost paid whether or not anyone reads
    /// stats. Callers that poll stats absorb the O(L·N) walk instead.
    pub async fn stats(&self) -> LabelIndexStats {
        let mut total_nodes = 0;
        for entry in self.index.iter() {
            total_nodes += entry.value().len();
        }
        LabelIndexStats {
            total_labels: self.index.len(),
            total_nodes,
            queries: self.queries.load(Ordering::Relaxed),
            intersection_queries: self.intersection_queries.load(Ordering::Relaxed),
        }
    }

    /// Clear the entire index
    pub async fn clear(&self) {
        self.index.clear();
        self.queries.store(0, Ordering::Relaxed);
        self.intersection_queries.store(0, Ordering::Relaxed);
    }

    /// Get the number of nodes with a given label
    pub async fn count(&self, label: &str) -> usize {
        self.index.get(label).map(|set| set.len()).unwrap_or(0)
    }

    /// Check if the index contains a label
    pub async fn contains_label(&self, label: &str) -> bool {
        self.index.contains_key(label)
    }
}

impl Default for LabelIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_query_single_label() {
        let index = LabelIndex::new();
        let node1 = NexoraId::from_bytes(b"node1".to_vec());
        let node2 = NexoraId::from_bytes(b"node2".to_vec());

        index.add_label("Person", node1.clone()).await;
        index.add_label("Person", node2.clone()).await;

        let results = index.query("Person").await;
        assert_eq!(results.len(), 2);
        assert!(results.contains(&node1));
        assert!(results.contains(&node2));
    }

    #[tokio::test]
    async fn test_query_multiple_labels_intersection() {
        let index = LabelIndex::new();
        let node1 = NexoraId::from_bytes(b"node1".to_vec());
        let node2 = NexoraId::from_bytes(b"node2".to_vec());

        // node1: Person, Employee
        index.add_label("Person", node1.clone()).await;
        index.add_label("Employee", node1.clone()).await;

        // node2: Person only
        index.add_label("Person", node2.clone()).await;

        // Query intersection
        let results = index.query_all(&["Person", "Employee"]).await;
        assert_eq!(results.len(), 1);
        assert!(results.contains(&node1));
    }

    #[tokio::test]
    async fn test_query_any_labels_union() {
        let index = LabelIndex::new();
        let node1 = NexoraId::from_bytes(b"node1".to_vec());
        let node2 = NexoraId::from_bytes(b"node2".to_vec());

        index.add_label("Person", node1.clone()).await;
        index.add_label("Product", node2.clone()).await;

        let results = index.query_any(&["Person", "Product"]).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_label() {
        let index = LabelIndex::new();
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index.add_label("Person", node.clone()).await;
        assert_eq!(index.query("Person").await.len(), 1);

        index.remove_label("Person", &node).await;
        assert_eq!(index.query("Person").await.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_node() {
        let index = LabelIndex::new();
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index
            .add_labels(&["Person", "Employee", "Manager"], node.clone())
            .await;

        index.remove_node(&node).await;

        assert_eq!(index.query("Person").await.len(), 0);
        assert_eq!(index.query("Employee").await.len(), 0);
        assert_eq!(index.query("Manager").await.len(), 0);
    }

    #[tokio::test]
    async fn test_has_label() {
        let index = LabelIndex::new();
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index.add_label("Person", node.clone()).await;

        assert!(index.has_label("Person", &node).await);
        assert!(!index.has_label("Product", &node).await);
    }

    #[tokio::test]
    async fn test_get_labels() {
        let index = LabelIndex::new();
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index
            .add_labels(&["Person", "Employee"], node.clone())
            .await;

        let labels = index.get_labels(&node).await;
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"Employee".to_string()));
    }

    #[tokio::test]
    async fn test_statistics() {
        let index = LabelIndex::new();
        let node1 = NexoraId::from_bytes(b"node1".to_vec());
        let node2 = NexoraId::from_bytes(b"node2".to_vec());

        index.add_label("Person", node1.clone()).await;
        index.add_label("Product", node2.clone()).await;

        let stats = index.stats().await;
        assert_eq!(stats.total_labels, 2);
        assert_eq!(stats.total_nodes, 2);

        index.query("Person").await;
        index.query_all(&["Person", "Product"]).await;

        let stats = index.stats().await;
        assert_eq!(stats.queries, 1);
        assert_eq!(stats.intersection_queries, 1);
    }

    #[tokio::test]
    async fn test_count() {
        let index = LabelIndex::new();

        for i in 0..10 {
            let node = NexoraId::from_bytes(format!("node{}", i).into_bytes());
            index.add_label("Person", node).await;
        }

        assert_eq!(index.count("Person").await, 10);
        assert_eq!(index.count("NonExistent").await, 0);
    }

    #[tokio::test]
    async fn test_empty_intersection() {
        let index = LabelIndex::new();
        let node = NexoraId::from_bytes(b"node1".to_vec());

        index.add_label("Person", node.clone()).await;

        // Query with non-existent label
        let results = index.query_all(&["Person", "NonExistent"]).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_writes_distinct_labels() {
        // Many tasks writing distinct labels concurrently. With the sharded
        // `DashMap` these contend only on segment locks, not a graph-wide write
        // lock; the assertion here is correctness under that concurrency.
        let index = Arc::new(LabelIndex::new());
        let mut handles = Vec::new();

        for l in 0..64u32 {
            let idx = index.clone();
            handles.push(tokio::spawn(async move {
                for n in 0..50u32 {
                    let node = NexoraId::from_bytes(format!("l{l}-n{n}").into_bytes());
                    idx.add_label(format!("Label{l}"), node).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        for l in 0..64u32 {
            assert_eq!(index.count(&format!("Label{l}")).await, 50);
        }
        let stats = index.stats().await;
        assert_eq!(stats.total_labels, 64);
        assert_eq!(stats.total_nodes, 64 * 50);
    }

    #[tokio::test]
    async fn test_concurrent_add_remove_same_label() {
        // Interleave adds and removes on one label from many tasks to exercise
        // the `remove_if`-based empty-set pruning against concurrent inserts.
        let index = Arc::new(LabelIndex::new());
        let mut handles = Vec::new();

        for t in 0..32u32 {
            let idx = index.clone();
            handles.push(tokio::spawn(async move {
                let node = NexoraId::from_bytes(format!("n{t}").into_bytes());
                idx.add_label("Hot", node.clone()).await;
                idx.remove_label("Hot", &node).await;
                idx.add_label("Hot", node).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Each node ends added exactly once.
        assert_eq!(index.count("Hot").await, 32);
    }
}
