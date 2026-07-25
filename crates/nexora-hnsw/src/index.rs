//! HNSW index — hierarchical navigable small world graph for approximate
//! nearest neighbor (ANN) search on graph node embeddings.
//!
//! Based on: Malkov & Yashunin, "Efficient and robust approximate nearest
//! neighbor search using Hierarchical Navigable Small World graphs" (2016).
//!
//! Architecture:
//! ```text
//!   Layer 2 (sparse):  [A] ────────────────── [D]
//!                      │                        │
//!   Layer 1 (medium):  [A] ─── [B] ─── [C] ── [D]
//!                      │        │        │       │
//!   Layer 0 (dense):   [A]─[B]─[C]─[D]─[E]─[F]─[G]─[H]
//! ```
//!
//! Each node is assigned a random layer (geometric distribution). At each
//! layer, the node connects to its `M` nearest neighbors. Search descends
//! from the top layer, doing greedy best-first search at each layer.

use crate::distance::{cosine_similarity, euclidean_distance, inner_product, Distance};
use nexora_id::NexoraId;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Configuration for the HNSW index.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Maximum number of connections per node per layer (except layer 0).
    pub m: usize,
    /// Maximum number of connections for layer 0 (typically 2*M).
    pub m0: usize,
    /// Maximum number of layers in the hierarchy.
    pub max_layers: usize,
    /// Size of the dynamic candidate list during construction.
    pub ef_construction: usize,
    /// Size of the dynamic candidate list during search.
    pub ef_search: usize,
    /// Vector dimension.
    pub dim: usize,
    /// Distance metric.
    pub distance: Distance,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            max_layers: 5,
            ef_construction: 200,
            ef_search: 100,
            dim: 128,
            distance: Distance::L2,
        }
    }
}

/// A node in the HNSW graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HnswNode {
    id: NexoraId,
    vector: Vec<f32>,
    /// The highest layer this node exists in.
    max_layer: usize,
    /// Neighbors at each layer: layer -> set of neighbor NexoraIds.
    neighbors: HashMap<usize, Vec<NexoraId>>,
}

/// Epsilon for floating-point distance comparisons.
const DIST_EPSILON: f32 = 1e-6;

/// Check if two f32 values are approximately equal.
#[inline]
fn approx_eq_f32(a: f32, b: f32) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    (a - b).abs() < DIST_EPSILON
}

/// Compare two f32 values with epsilon tolerance, returning a deterministic Ordering.
/// NaN is treated as greater than any finite value (sorted last in min-heap).
#[inline]
fn total_cmp_eps(a: f32, b: f32) -> Ordering {
    if a.is_nan() && b.is_nan() {
        return Ordering::Equal;
    }
    if a.is_nan() {
        return Ordering::Greater;
    }
    if b.is_nan() {
        return Ordering::Less;
    }
    if approx_eq_f32(a, b) {
        return Ordering::Equal;
    }
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Candidate for search expansion, ordered by distance (min-heap).
#[derive(Clone, Debug)]
struct Candidate {
    id: NexoraId,
    dist: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        approx_eq_f32(self.dist, other.dist)
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: smaller distance = higher priority
        total_cmp_eps(other.dist, self.dist)
    }
}

/// Max-heap candidate (for tracking worst results to evict).
#[derive(Clone, Debug)]
struct MaxCandidate {
    id: NexoraId,
    dist: f32,
}
impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        approx_eq_f32(self.dist, other.dist)
    }
}
impl Eq for MaxCandidate {}
impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: larger distance = higher priority (for eviction)
        total_cmp_eps(self.dist, other.dist)
    }
}

/// The HNSW index.
pub struct HnswIndex {
    config: HnswConfig,
    nodes: HashMap<NexoraId, HnswNode>,
    /// Entry point at the top layer.
    entry_point: Option<NexoraId>,
    /// Current maximum layer in the index.
    max_level: usize,
}

impl HnswIndex {
    /// Create a new HNSW index.
    pub fn new(config: HnswConfig) -> Self {
        Self {
            config,
            nodes: HashMap::new(),
            entry_point: None,
            max_level: 0,
        }
    }

    /// Insert a node embedding into the index with full multi-layer connection.
    pub fn insert(&mut self, id: NexoraId, vector: Vec<f32>) {
        let mut rng = rand::thread_rng();
        let node_layer = self.random_layer(&mut rng);

        let mut node = HnswNode {
            id: id.clone(),
            vector: vector.clone(),
            max_layer: node_layer,
            neighbors: HashMap::new(),
        };

        if self.entry_point.is_none() {
            // First node: becomes the entry point at all its layers
            for layer in 0..=node_layer {
                node.neighbors.insert(layer, Vec::new());
            }
            self.nodes.insert(id.clone(), node);
            self.entry_point = Some(id);
            self.max_level = node_layer;
            return;
        }

        let entry = self.entry_point.clone().unwrap();

        // Phase 1: Descend from top layer to node_layer+1, greedy search
        // to find the closest entry point for lower layers.
        let mut curr_entry = entry;
        for layer in (node_layer + 1..=self.max_level).rev() {
            curr_entry = self.greedy_search_layer(&vector, &curr_entry, layer);
        }

        // Phase 2: From min(node_layer, max_level) down to 0, connect neighbors
        for layer in (0..=node_layer.min(self.max_level)).rev() {
            let m_max = if layer == 0 {
                self.config.m0
            } else {
                self.config.m
            };

            // Find ef_construction nearest neighbors at this layer
            let candidates =
                self.search_layer(&vector, &curr_entry, layer, self.config.ef_construction);

            // Select M nearest neighbors
            let neighbors: Vec<NexoraId> = candidates
                .iter()
                .take(m_max)
                .map(|c| c.id.clone())
                .collect();

            // Connect new node -> neighbors
            node.neighbors.insert(layer, neighbors.clone());

            // Connect neighbors -> new node (bidirectional)
            for neighbor_id in &neighbors {
                // First, get the neighbor's current connections and vector
                let (neighbor_vec, neighbor_id_owned, conns_to_update) = {
                    if let Some(neighbor) = self.nodes.get(neighbor_id) {
                        let conns = neighbor.neighbors.get(&layer).cloned().unwrap_or_default();
                        (neighbor.vector.clone(), neighbor_id.clone(), conns)
                    } else {
                        continue;
                    }
                };

                // Add new connection
                let mut updated_conns = conns_to_update;
                updated_conns.push(id.clone());

                // Prune: if neighbor has too many connections, keep only M closest
                if updated_conns.len() > m_max {
                    // Compute distances from neighbor to all its connections
                    let mut scored: Vec<(NexoraId, f32)> = updated_conns
                        .iter()
                        .filter(|nid| *nid != &neighbor_id_owned)
                        .filter_map(|nid| {
                            self.nodes
                                .get(nid)
                                .map(|n| (nid.clone(), self.dist(&neighbor_vec, &n.vector)))
                        })
                        .collect();
                    scored.sort_by(|a, b| total_cmp_eps(a.1, b.1));
                    scored.truncate(m_max);
                    updated_conns = scored.into_iter().map(|(nid, _)| nid).collect();
                }

                // Write back the updated connections
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id_owned) {
                    neighbor.neighbors.insert(layer, updated_conns);
                }
            }

            // Update entry for next (lower) layer
            if let Some(best) = candidates.first() {
                curr_entry = best.id.clone();
            }
        }

        // Initialize empty neighbor lists for layers above max_level (if node_layer > max_level)
        for layer in (self.max_level + 1)..=node_layer {
            node.neighbors.insert(layer, Vec::new());
        }

        // Update entry point if new node has a higher layer
        if node_layer > self.max_level {
            self.max_level = node_layer;
            self.entry_point = Some(id.clone());
        }

        self.nodes.insert(id, node);
    }

    /// Search for k nearest neighbors to a query vector.
    pub fn search_knn(&self, query: &[f32], k: usize) -> Vec<(NexoraId, f32)> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }

        let entry = match &self.entry_point {
            Some(id) => id.clone(),
            None => return Vec::new(),
        };

        // Phase 1: Descend from top layer to layer 1, greedy search
        let mut curr_entry = entry;
        for layer in (1..=self.max_level).rev() {
            curr_entry = self.greedy_search_layer(query, &curr_entry, layer);
        }

        // Phase 2: At layer 0, do ef_search-wide search
        let ef = std::cmp::max(self.config.ef_search, k);
        let candidates = self.search_layer(query, &curr_entry, 0, ef);

        // Return top-k
        candidates
            .into_iter()
            .take(k)
            .map(|c| (c.id, c.dist))
            .collect()
    }

    /// Remove a node from the index.
    pub fn remove(&mut self, id: &NexoraId) {
        // Collect all (neighbor_id, layer) pairs to update, avoiding borrow conflicts
        let mut updates: Vec<(NexoraId, usize)> = Vec::new();
        if let Some(node) = self.nodes.get(id) {
            for (layer, layer_neighbors) in &node.neighbors {
                for neighbor_id in layer_neighbors {
                    updates.push((neighbor_id.clone(), *layer));
                }
            }
        }

        // Apply the removal of references
        for (neighbor_id, layer) in &updates {
            if let Some(neighbor) = self.nodes.get_mut(neighbor_id) {
                if let Some(conns) = neighbor.neighbors.get_mut(layer) {
                    conns.retain(|n| n != id);
                }
            }
        }

        self.nodes.remove(id);

        // Update entry point if needed
        if self.entry_point.as_ref() == Some(id) {
            self.entry_point = self.nodes.keys().next().cloned();
            self.max_level = self
                .entry_point
                .as_ref()
                .and_then(|id| self.nodes.get(id))
                .map(|n| n.max_layer)
                .unwrap_or(0);
        }
    }

    /// Number of nodes in the index.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get the vector for a node, if it exists in the index.
    pub fn get(&self, id: &NexoraId) -> Option<&[f32]> {
        self.nodes.get(id).map(|n| n.vector.as_slice())
    }

    /// Total number of connections in the graph.
    pub fn connection_count(&self) -> usize {
        self.nodes
            .values()
            .flat_map(|n| n.neighbors.values())
            .map(|s| s.len())
            .sum()
    }

    // ================================================================
    // Internal algorithms
    // ================================================================

    /// Generate a random layer using geometric distribution.
    fn random_layer(&self, rng: &mut impl Rng) -> usize {
        let ml = 1.0 / (self.config.m as f64).ln();
        let mut layer = 0;
        while rng.gen::<f64>() < ml && layer < self.config.max_layers {
            layer += 1;
        }
        layer
    }

    /// Greedy search at a single layer: find the closest node to the query
    /// starting from `entry`, moving to neighbors if they are closer.
    fn greedy_search_layer(&self, query: &[f32], entry: &NexoraId, layer: usize) -> NexoraId {
        let mut current = entry.clone();
        let mut current_dist = match self.nodes.get(&current) {
            Some(n) => self.dist(query, &n.vector),
            None => return current,
        };

        loop {
            let mut improved = false;
            if let Some(node) = self.nodes.get(&current) {
                if let Some(neighbors) = node.neighbors.get(&layer) {
                    for neighbor_id in neighbors {
                        if let Some(neighbor) = self.nodes.get(neighbor_id) {
                            let d = self.dist(query, &neighbor.vector);
                            if d < current_dist {
                                current_dist = d;
                                current = neighbor_id.clone();
                                improved = true;
                            }
                        }
                    }
                }
            }
            if !improved {
                break;
            }
        }

        current
    }

    /// Search at a single layer with ef-wide candidate list.
    /// Returns sorted candidates (closest first).
    fn search_layer(
        &self,
        query: &[f32],
        entry: &NexoraId,
        layer: usize,
        ef: usize,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        visited.insert(entry.clone());

        let entry_dist = match self.nodes.get(entry) {
            Some(n) => self.dist(query, &n.vector),
            None => return Vec::new(),
        };

        let mut candidates = BinaryHeap::new(); // min-heap
        let mut results = BinaryHeap::new(); // max-heap (for eviction)

        candidates.push(Candidate {
            id: entry.clone(),
            dist: entry_dist,
        });
        results.push(MaxCandidate {
            id: entry.clone(),
            dist: entry_dist,
        });

        while let Some(current) = candidates.pop() {
            // If current is farther than worst result, stop
            if let Some(worst) = results.peek() {
                if current.dist > worst.dist {
                    break;
                }
            }

            if let Some(node) = self.nodes.get(&current.id) {
                if let Some(neighbors) = node.neighbors.get(&layer) {
                    for neighbor_id in neighbors {
                        if visited.contains(neighbor_id) {
                            continue;
                        }
                        visited.insert(neighbor_id.clone());

                        if let Some(neighbor) = self.nodes.get(neighbor_id) {
                            let d = self.dist(query, &neighbor.vector);
                            candidates.push(Candidate {
                                id: neighbor_id.clone(),
                                dist: d,
                            });

                            // Add to results if better than worst or results not full
                            let should_add = match results.peek() {
                                Some(worst) if results.len() >= ef => d < worst.dist,
                                _ => true,
                            };
                            if should_add {
                                results.push(MaxCandidate {
                                    id: neighbor_id.clone(),
                                    dist: d,
                                });
                                if results.len() > ef {
                                    results.pop(); // Evict worst
                                }
                            }
                        }
                    }
                }
            }
        }

        // Convert max-heap to sorted vec (closest first)
        let mut sorted: Vec<Candidate> = results
            .into_iter()
            .map(|mc| Candidate {
                id: mc.id,
                dist: mc.dist,
            })
            .collect();
        sorted.sort_by(|a, b| total_cmp_eps(a.dist, b.dist));
        sorted
    }

    /// Compute distance between two vectors using the configured metric.
    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.config.distance {
            Distance::L2 => euclidean_distance(a, b),
            Distance::Cosine => cosine_similarity(a, b),
            Distance::InnerProduct => inner_product(a, b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(dim: usize) -> HnswConfig {
        HnswConfig {
            dim,
            m: 4,
            m0: 8,
            max_layers: 3,
            ef_construction: 50,
            ef_search: 20,
            distance: Distance::L2,
        }
    }

    #[test]
    fn test_insert_single_node() {
        let mut index = HnswIndex::new(make_config(4));
        let id = NexoraId::from_bytes(b"node-1".to_vec());
        index.insert(id, vec![1.0, 0.0, 0.0, 0.0]);
        assert_eq!(index.len(), 1);
        assert!(index.entry_point.is_some());
    }

    #[test]
    fn test_insert_multiple_and_search() {
        let mut index = HnswIndex::new(make_config(4));

        // Insert 10 nodes with distinct vectors
        let nodes = vec![
            (b"n0".to_vec(), vec![0.0, 0.0, 0.0, 0.0]),
            (b"n1".to_vec(), vec![10.0, 0.0, 0.0, 0.0]),
            (b"n2".to_vec(), vec![0.0, 10.0, 0.0, 0.0]),
            (b"n3".to_vec(), vec![10.0, 10.0, 0.0, 0.0]),
            (b"n4".to_vec(), vec![0.0, 0.0, 10.0, 0.0]),
            (b"n5".to_vec(), vec![10.0, 0.0, 10.0, 0.0]),
            (b"n6".to_vec(), vec![0.0, 10.0, 10.0, 0.0]),
            (b"n7".to_vec(), vec![10.0, 10.0, 10.0, 0.0]),
            (b"n8".to_vec(), vec![5.0, 5.0, 5.0, 0.0]),
            (b"n9".to_vec(), vec![1.0, 1.0, 1.0, 0.0]),
        ];

        for (id_bytes, vec) in &nodes {
            index.insert(NexoraId::from_bytes(id_bytes.clone()), vec.clone());
        }

        assert_eq!(index.len(), 10, "Index should contain 10 nodes");

        // Test basic search functionality
        let query = vec![1.0, 1.0, 1.0, 0.0];
        let results = index.search_knn(&query, 5);

        assert!(!results.is_empty(), "Search should return results");
        assert!(results.len() <= 5, "Should return at most k results");

        // Verify results are sorted by distance (closest first), with tolerance
        for i in 1..results.len() {
            assert!(
                results[i].1 >= results[i - 1].1 - 1e-5,
                "Results should be sorted by distance (ascending), \
                 got {} at index {} before {} at index {}",
                results[i - 1].1,
                i - 1,
                results[i].1,
                i
            );
        }

        // Verify all returned IDs exist in the index
        for (id, _dist) in &results {
            assert!(
                nodes
                    .iter()
                    .any(|(node_id, _)| &NexoraId::from_bytes(node_id.clone()) == id),
                "Returned ID {:?} should exist in the index",
                id
            );
        }

        // The closest result should be reasonably close (not a distant node)
        // With our test data, distances range from ~1.7 (n0) to ~17 (n7)
        assert!(
            results[0].1 < 10.0,
            "Closest result should be reasonably close, got distance: {}",
            results[0].1
        );
    }

    #[test]
    fn test_search_empty_index() {
        let index = HnswIndex::new(make_config(4));
        let results = index.search_knn(&[1.0, 0.0, 0.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_remove() {
        let mut index = HnswIndex::new(make_config(4));
        let id = NexoraId::from_bytes(b"remove-me".to_vec());
        index.insert(id.clone(), vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(index.len(), 1);

        index.remove(&id);
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_connection_count() {
        let mut index = HnswIndex::new(make_config(2));
        for i in 0..5 {
            index.insert(
                NexoraId::from_bytes(format!("n{i}").into_bytes()),
                vec![i as f32, (i * 2) as f32],
            );
        }
        assert!(
            index.connection_count() > 0,
            "Should have connections after insert"
        );
    }

    #[test]
    fn test_cosine_distance() {
        let mut index = HnswIndex::new(HnswConfig {
            dim: 3,
            m: 4,
            m0: 8,
            max_layers: 2,
            ef_construction: 50,
            ef_search: 20,
            distance: Distance::Cosine,
        });

        index.insert(NexoraId::from_bytes(b"a".to_vec()), vec![1.0, 0.0, 0.0]);
        index.insert(NexoraId::from_bytes(b"b".to_vec()), vec![0.9, 0.1, 0.0]);
        index.insert(NexoraId::from_bytes(b"c".to_vec()), vec![0.0, 0.0, 1.0]);

        let results = index.search_knn(&[1.0, 0.0, 0.0], 2);
        assert!(!results.is_empty());
        // Closest to [1,0,0] should be 'a' or 'b' (both near same direction)
        let top_id = results[0].0.clone();
        assert!(
            top_id == NexoraId::from_bytes(b"a".to_vec())
                || top_id == NexoraId::from_bytes(b"b".to_vec()),
            "Closest should be 'a' or 'b'"
        );
    }

    #[test]
    fn test_large_index() {
        let mut index = HnswIndex::new(HnswConfig {
            dim: 8,
            m: 8,
            m0: 16,
            max_layers: 4,
            ef_construction: 100,
            ef_search: 50,
            distance: Distance::L2,
        });

        // Insert 100 nodes
        for i in 0..100u32 {
            let vec: Vec<f32> = (0..8).map(|j| ((i >> j) & 1) as f32).collect();
            index.insert(NexoraId::from_bytes(format!("node-{i}").into_bytes()), vec);
        }

        assert_eq!(index.len(), 100);

        // Search for k=5
        let query: Vec<f32> = vec![1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0];
        let results = index.search_knn(&query, 5);
        assert_eq!(results.len(), 5);

        // Results should be sorted by distance
        for i in 1..results.len() {
            assert!(
                results[i - 1].1 <= results[i].1 + 1e-5,
                "Results should be sorted by distance"
            );
        }
    }

    #[test]
    fn test_is_empty_new_index() {
        let index = HnswIndex::new(make_config(4));
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_get_returns_vector() {
        let mut index = HnswIndex::new(make_config(3));
        let id = NexoraId::from_bytes(b"get-me".to_vec());
        index.insert(id.clone(), vec![1.0, 2.0, 3.0]);

        let vec = index.get(&id).unwrap();
        assert_eq!(vec.len(), 3);
        assert!((vec[0] - 1.0).abs() < 1e-6);
        assert!((vec[1] - 2.0).abs() < 1e-6);
        assert!((vec[2] - 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let index = HnswIndex::new(make_config(3));
        let id = NexoraId::from_bytes(b"missing".to_vec());
        assert!(index.get(&id).is_none());
    }

    #[test]
    fn test_search_k_zero() {
        let mut index = HnswIndex::new(make_config(3));
        index.insert(NexoraId::from_bytes(b"a".to_vec()), vec![1.0, 0.0, 0.0]);
        index.insert(NexoraId::from_bytes(b"b".to_vec()), vec![0.0, 1.0, 0.0]);

        let results = index.search_knn(&[1.0, 0.0, 0.0], 0);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_k_greater_than_index_size() {
        let mut index = HnswIndex::new(make_config(3));
        index.insert(NexoraId::from_bytes(b"a".to_vec()), vec![1.0, 0.0, 0.0]);
        index.insert(NexoraId::from_bytes(b"b".to_vec()), vec![0.0, 1.0, 0.0]);

        // Requesting k=10 with only 2 nodes should return at most 2
        let results = index.search_knn(&[1.0, 0.0, 0.0], 10);
        assert!(results.len() <= 2);
    }

    #[test]
    fn test_remove_nonexistent_node() {
        let mut index = HnswIndex::new(make_config(3));
        index.insert(NexoraId::from_bytes(b"a".to_vec()), vec![1.0, 0.0, 0.0]);

        // Remove a node that doesn't exist — should not panic
        let missing = NexoraId::from_bytes(b"missing".to_vec());
        index.remove(&missing);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_remove_all_nodes() {
        let mut index = HnswIndex::new(make_config(3));
        let id1 = NexoraId::from_bytes(b"a".to_vec());
        let id2 = NexoraId::from_bytes(b"b".to_vec());

        index.insert(id1.clone(), vec![1.0, 0.0, 0.0]);
        index.insert(id2.clone(), vec![0.0, 1.0, 0.0]);

        index.remove(&id1);
        assert_eq!(index.len(), 1);

        index.remove(&id2);
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_inner_product_distance() {
        let mut index = HnswIndex::new(HnswConfig {
            dim: 3,
            m: 4,
            m0: 8,
            max_layers: 2,
            ef_construction: 50,
            ef_search: 20,
            distance: Distance::InnerProduct,
        });

        index.insert(NexoraId::from_bytes(b"a".to_vec()), vec![1.0, 1.0, 0.0]);
        index.insert(NexoraId::from_bytes(b"b".to_vec()), vec![1.0, 0.0, 0.0]);
        index.insert(NexoraId::from_bytes(b"c".to_vec()), vec![0.0, 0.0, 1.0]);

        let results = index.search_knn(&[1.0, 1.0, 0.0], 1);
        assert!(!results.is_empty());
        // With inner product, the node with highest dot product should be closest.
        // [1,1,0] · [1,1,0] = 2 (highest, so negated is lowest = closest)
        // Use approximate comparison for distance to avoid float instability
        let expected_id = NexoraId::from_bytes(b"a".to_vec());
        let expected_dist = -2.0f32; // -(1*1 + 1*1 + 0*0) = -2
        assert!(
            results[0].0 == expected_id || (results[0].1 - expected_dist).abs() < 1e-5,
            "Expected node 'a' with distance ~{}, got {:?} with distance {}",
            expected_dist,
            results[0].0,
            results[0].1
        );
    }

    #[test]
    fn test_search_after_remove() {
        let mut index = HnswIndex::new(make_config(3));
        let id_a = NexoraId::from_bytes(b"a".to_vec());
        let id_b = NexoraId::from_bytes(b"b".to_vec());

        index.insert(id_a.clone(), vec![1.0, 0.0, 0.0]);
        index.insert(id_b.clone(), vec![0.0, 1.0, 0.0]);

        index.remove(&id_a);

        let results = index.search_knn(&[1.0, 0.0, 0.0], 1);
        assert!(!results.is_empty());
        // The removed node should not be in results
        for (id, _) in &results {
            assert_ne!(*id, id_a);
        }
    }

    #[test]
    fn test_config_default() {
        let config = HnswConfig::default();
        assert_eq!(config.m, 16);
        assert_eq!(config.m0, 32);
        assert_eq!(config.max_layers, 5);
        assert_eq!(config.ef_construction, 200);
        assert_eq!(config.ef_search, 100);
        assert_eq!(config.dim, 128);
        assert_eq!(config.distance, Distance::L2);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = HnswConfig {
            dim: 64,
            m: 8,
            m0: 16,
            max_layers: 3,
            ef_construction: 100,
            ef_search: 50,
            distance: Distance::Cosine,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HnswConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.dim, 64);
        assert_eq!(deserialized.m, 8);
        assert_eq!(deserialized.distance, Distance::Cosine);
    }

    #[test]
    fn test_single_node_search() {
        let mut index = HnswIndex::new(make_config(3));
        let id = NexoraId::from_bytes(b"only".to_vec());
        index.insert(id.clone(), vec![1.0, 0.0, 0.0]);

        let results = index.search_knn(&[1.0, 0.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id);
        // Distance to itself should be 0
        assert!((results[0].1 - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_insert_duplicate_id() {
        let mut index = HnswIndex::new(make_config(3));
        let id = NexoraId::from_bytes(b"dup".to_vec());

        index.insert(id.clone(), vec![1.0, 0.0, 0.0]);
        // Insert same ID again with different vector — should overwrite
        index.insert(id.clone(), vec![0.0, 1.0, 0.0]);

        assert_eq!(index.len(), 1);
        let vec = index.get(&id).unwrap();
        // The second insert should have overwritten the vector
        assert!((vec[0] - 0.0).abs() < 1e-6);
        assert!((vec[1] - 1.0).abs() < 1e-6);
    }
}
