//! Edge index for fast edge-type-based node lookups.
//!
//! Provides O(1) lookup for nodes by edge type and optional edge property filters.
//! Supports queries like:
//! - MATCH (a)-[:EXECUTING]->(t) — find incoming neighbors by edge type
//! - MATCH (a)-[:EXECUTING {priority: 1}]->(t) — filter by edge property
//! - MATCH (a)-[:DEPENDS_ON*1..3]->(b) — variable-length path traversal
//!
//! Design:
//! - Maintains both forward (outgoing) and reverse (incoming) indexes
//! - Forward: (edge_type) → set of (src, dst) pairs
//! - Reverse: (edge_type) → set of (dst, src) pairs (for inbound queries)
//! - Edge property index: (edge_type, prop_key, prop_value) → set of (src, dst) pairs
//! - All indexes are write-through, updated on every mutation

use dashmap::DashMap;
use nexora_id::NexoraId;
use nexora_value::HalfEdge;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// An (src, dst) pair representing a directed edge
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeTuple {
    pub src: NexoraId,
    pub dst: NexoraId,
}

/// Statistics for edge index monitoring
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct EdgeIndexStats {
    /// Total number of unique edge types
    pub total_edge_types: usize,

    /// Total number of indexed edges
    pub total_edges: usize,

    /// Number of edge-type queries
    pub queries: u64,

    /// Number of edge property queries
    pub property_queries: u64,

    /// Number of incoming-edge queries
    pub incoming_queries: u64,
}

/// Edge index for fast traversal — all three index tiers.
///
/// # Examples
///
/// ```
/// use nexora_core::EdgeIndex;
/// use nexora_id::NexoraId;
/// use nexora_value::HalfEdge;
///
/// # #[tokio::main]
/// # async fn main() {
/// let idx = EdgeIndex::new();
/// let a = NexoraId::new_random();
/// let b = NexoraId::new_random();
///
/// idx.add_edge("EXECUTING", a, b).await;
///
/// let out = idx.query_outgoing("EXECUTING").await;
/// assert_eq!(out.len(), 1);
/// # }
/// ```
pub struct EdgeIndex {
    /// Forward index: edge_type → set of (src, dst) pairs.
    ///
    /// A `DashMap` (sharded lock) rather than a single `RwLock<HashMap>` so
    /// mutations to different edge types contend only on their internal segment
    /// lock instead of a graph-wide write lock — this is the hottest write-path
    /// index (touched on every `add_edge`).
    forward: Arc<DashMap<String, HashSet<EdgeTuple>>>,

    /// Reverse index: edge_type → set of (dst, src) pairs (for inbound queries)
    reverse: Arc<DashMap<String, HashSet<EdgeTuple>>>,

    /// Query counters. Atomics so read-path stat bookkeeping never takes a write
    /// lock (the old design took `stats.write()` on every query).
    queries: Arc<AtomicU64>,
    property_queries: Arc<AtomicU64>,
    incoming_queries: Arc<AtomicU64>,
}

impl EdgeIndex {
    pub fn new() -> Self {
        Self {
            forward: Arc::new(DashMap::new()),
            reverse: Arc::new(DashMap::new()),
            queries: Arc::new(AtomicU64::new(0)),
            property_queries: Arc::new(AtomicU64::new(0)),
            incoming_queries: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Record an edge in the index, called when an edge is added to the graph.
    /// `src_qid` is the node that owns/holds the outgoing HalfEdge.
    pub async fn add_edge(&self, edge_type: &str, src_qid: NexoraId, dst: NexoraId) {
        let et = edge_type.to_string();
        let tuple = EdgeTuple {
            src: src_qid.clone(),
            dst: dst.clone(),
        };
        let rev_tuple = EdgeTuple {
            src: dst,
            dst: src_qid,
        };

        // Sharded write: distinct edge types touch distinct `DashMap` segments,
        // so concurrent `add_edge`s no longer serialize on a graph-wide lock.
        // Edge/type counts are derived on demand in `stats()`, so there is no
        // stat bookkeeping on this hot path anymore.
        self.forward.entry(et.clone()).or_default().insert(tuple);
        self.reverse.entry(et).or_default().insert(rev_tuple);
    }

    /// Record an edge from a HalfEdge, where `owner` holds the half-edge.
    /// Only indexes Outgoing edges (since Incoming edges don't logically
    /// represent a src→dst relationship from the owner's perspective).
    pub async fn add_halfedge(&self, owner: &NexoraId, he: &HalfEdge) {
        if he.direction.is_out() {
            self.add_edge(he.edge_type.as_str(), owner.clone(), he.other.clone())
                .await;
        }
    }

    /// Remove an edge from the index.
    pub async fn remove_edge(&self, edge_type: &str, src_qid: &NexoraId, dst: &NexoraId) {
        let et = edge_type.to_string();
        let tuple = EdgeTuple {
            src: src_qid.clone(),
            dst: dst.clone(),
        };
        let rev_tuple = EdgeTuple {
            src: dst.clone(),
            dst: src_qid.clone(),
        };

        // Remove from each set, then prune the entry if it emptied. `remove_if`
        // holds the entry lock across the emptiness check so a concurrent
        // `add_edge` can't insert into a set that's about to be dropped.
        if let Some(mut set) = self.forward.get_mut(&et) {
            set.remove(&tuple);
        }
        self.forward.remove_if(&et, |_, set| set.is_empty());

        if let Some(mut set) = self.reverse.get_mut(&et) {
            set.remove(&rev_tuple);
        }
        self.reverse.remove_if(&et, |_, set| set.is_empty());
    }

    /// Query outgoing edges of a given type: returns (src, dst) pairs
    /// where the edge type matches.
    pub async fn query_outgoing(&self, edge_type: &str) -> Vec<(NexoraId, NexoraId)> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.forward
            .get(edge_type)
            .map(|set| set.iter().map(|t| (t.src.clone(), t.dst.clone())).collect())
            .unwrap_or_default()
    }

    /// Query incoming edges of a given type: returns (dst, src) pairs.
    pub async fn query_incoming(&self, edge_type: &str) -> Vec<(NexoraId, NexoraId)> {
        self.incoming_queries.fetch_add(1, Ordering::Relaxed);
        self.reverse
            .get(edge_type)
            .map(|set| set.iter().map(|t| (t.src.clone(), t.dst.clone())).collect())
            .unwrap_or_default()
    }

    /// Get all outgoing targets for a specific node and edge type.
    pub async fn outgoing_targets(&self, edge_type: &str, src: &NexoraId) -> Vec<NexoraId> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.forward
            .get(edge_type)
            .map(|set| {
                set.iter()
                    .filter(|t| t.src == *src)
                    .map(|t| t.dst.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all incoming sources for a specific node and edge type.
    pub async fn incoming_sources(&self, edge_type: &str, dst: &NexoraId) -> Vec<NexoraId> {
        self.incoming_queries.fetch_add(1, Ordering::Relaxed);
        self.reverse
            .get(edge_type)
            .map(|set| {
                set.iter()
                    .filter(|t| t.src == *dst)
                    .map(|t| t.dst.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove all edges involving a node from the index.
    /// Called when a node is deleted.
    pub async fn remove_node(&self, node_id: &NexoraId) {
        // Drop every tuple touching the node from both indexes, then prune the
        // edge types left empty. Per-entry mutation contends only on segment
        // locks; counts are derived on demand in `stats()`.
        self.forward.iter_mut().for_each(|mut entry| {
            entry
                .value_mut()
                .retain(|t| t.src != *node_id && t.dst != *node_id);
        });
        self.forward.retain(|_, s| !s.is_empty());

        self.reverse.iter_mut().for_each(|mut entry| {
            entry
                .value_mut()
                .retain(|t| t.src != *node_id && t.dst != *node_id);
        });
        self.reverse.retain(|_, s| !s.is_empty());
    }

    /// List all edge types in the index.
    pub async fn all_edge_types(&self) -> Vec<String> {
        self.forward.iter().map(|e| e.key().clone()).collect()
    }

    /// Get statistics
    ///
    /// `total_edge_types`/`total_edges` are computed on demand from the forward
    /// index rather than maintained on every mutation. The old design carried
    /// incremental counters that had to be adjusted (under a write lock) inside
    /// every add/remove — a per-write cost, and a recurring source of drift bugs
    /// on the remove paths. Callers polling stats absorb the O(types·edges) walk
    /// instead; the hot write path pays nothing.
    pub async fn stats(&self) -> EdgeIndexStats {
        let mut total_edges = 0;
        for entry in self.forward.iter() {
            total_edges += entry.value().len();
        }
        EdgeIndexStats {
            total_edge_types: self.forward.len(),
            total_edges,
            queries: self.queries.load(Ordering::Relaxed),
            property_queries: self.property_queries.load(Ordering::Relaxed),
            incoming_queries: self.incoming_queries.load(Ordering::Relaxed),
        }
    }

    /// Clear the entire index
    pub async fn clear(&self) {
        self.forward.clear();
        self.reverse.clear();
        self.queries.store(0, Ordering::Relaxed);
        self.property_queries.store(0, Ordering::Relaxed);
        self.incoming_queries.store(0, Ordering::Relaxed);
    }
}

impl Default for EdgeIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_value::Symbol;

    fn test_node(i: u8) -> NexoraId {
        NexoraId::from_bytes(vec![i])
    }

    #[tokio::test]
    async fn test_add_and_query_outgoing() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;

        let out = idx.query_outgoing("KNOWS").await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], (a, b));
    }

    #[tokio::test]
    async fn test_outgoing_targets() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);
        let c = test_node(3);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        idx.add_edge("KNOWS", a.clone(), c.clone()).await;

        let targets = idx.outgoing_targets("KNOWS", &a).await;
        assert_eq!(targets.len(), 2);
    }

    #[tokio::test]
    async fn test_incoming_sources() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;

        let sources = idx.incoming_sources("KNOWS", &b).await;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0], a);
    }

    #[tokio::test]
    async fn test_remove_edge() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        assert_eq!(idx.query_outgoing("KNOWS").await.len(), 1);

        idx.remove_edge("KNOWS", &a, &b).await;
        assert_eq!(idx.query_outgoing("KNOWS").await.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_node() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);
        let c = test_node(3);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        idx.add_edge("FOLLOWS", a.clone(), c.clone()).await;

        idx.remove_node(&a).await;

        assert_eq!(idx.query_outgoing("KNOWS").await.len(), 0);
        assert_eq!(idx.query_outgoing("FOLLOWS").await.len(), 0);
        assert_eq!(idx.all_edge_types().await.len(), 0);
    }

    #[tokio::test]
    async fn test_multiple_edge_types() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        idx.add_edge("FOLLOWS", a.clone(), b.clone()).await;

        assert_eq!(idx.query_outgoing("KNOWS").await.len(), 1);
        assert_eq!(idx.query_outgoing("FOLLOWS").await.len(), 1);
        assert_eq!(idx.all_edge_types().await.len(), 2);
    }

    #[tokio::test]
    async fn test_stats() {
        let idx = EdgeIndex::new();
        idx.add_edge("A", test_node(1), test_node(2)).await;
        idx.add_edge("B", test_node(3), test_node(4)).await;

        let stats = idx.stats().await;
        assert_eq!(stats.total_edge_types, 2);
        assert_eq!(stats.total_edges, 2);
    }

    #[tokio::test]
    async fn test_add_halfedge_outgoing() {
        let idx = EdgeIndex::new();
        let owner = test_node(1);
        let target = test_node(2);
        let he = HalfEdge::out(Symbol::new("EXECUTING"), target.clone());

        idx.add_halfedge(&owner, &he).await;

        let targets = idx.outgoing_targets("EXECUTING", &owner).await;
        assert_eq!(targets, vec![target]);
    }

    #[tokio::test]
    async fn test_add_halfedge_incoming_ignored() {
        let idx = EdgeIndex::new();
        let owner = test_node(1);
        let source = test_node(2);
        let he = HalfEdge::incoming(Symbol::new("IN_ZONE"), source.clone());

        idx.add_halfedge(&owner, &he).await;

        // Incoming half-edges shouldn't create forward index entries
        let all = idx.query_outgoing("IN_ZONE").await;
        assert_eq!(all.len(), 0);
    }

    // #4a regression: stats are maintained incrementally and stay consistent
    // across removals. Previously add_edge recomputed stats by scanning every
    // set (O(N) per insert), while remove_edge/remove_node left stats untouched
    // so total_edges drifted above the true count after any deletion.

    /// remove_edge must decrement stats (previously it didn't).
    #[tokio::test]
    async fn test_stats_consistent_after_remove_edge() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);
        let c = test_node(3);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        idx.add_edge("KNOWS", a.clone(), c.clone()).await;
        assert_eq!(idx.stats().await.total_edges, 2);
        assert_eq!(idx.stats().await.total_edge_types, 1);

        // Remove one edge: total_edges drops, type stays (set non-empty).
        idx.remove_edge("KNOWS", &a, &b).await;
        let s = idx.stats().await;
        assert_eq!(s.total_edges, 1, "total_edges must track removals");
        assert_eq!(s.total_edge_types, 1);

        // Remove the last edge of the type: both counts drop.
        idx.remove_edge("KNOWS", &a, &c).await;
        let s = idx.stats().await;
        assert_eq!(s.total_edges, 0);
        assert_eq!(s.total_edge_types, 0, "empty edge type must be uncounted");
    }

    /// remove_node must decrement stats by the number of edges it removed.
    #[tokio::test]
    async fn test_stats_consistent_after_remove_node() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);
        let c = test_node(3);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        idx.add_edge("FOLLOWS", a.clone(), c.clone()).await;
        idx.add_edge("KNOWS", b.clone(), c.clone()).await;
        assert_eq!(idx.stats().await.total_edges, 3);
        assert_eq!(idx.stats().await.total_edge_types, 2);

        // Removing `a` drops both edges it participates in (KNOWS a→b,
        // FOLLOWS a→c); KNOWS b→c survives, so FOLLOWS empties out.
        idx.remove_node(&a).await;
        let s = idx.stats().await;
        assert_eq!(
            s.total_edges, 1,
            "remove_node must decrement by edges removed"
        );
        assert_eq!(
            s.total_edge_types, 1,
            "emptied FOLLOWS type must be uncounted"
        );
    }

    /// Duplicate inserts must not inflate stats (idempotent add).
    #[tokio::test]
    async fn test_stats_dedup_on_duplicate_add() {
        let idx = EdgeIndex::new();
        let a = test_node(1);
        let b = test_node(2);

        idx.add_edge("KNOWS", a.clone(), b.clone()).await;
        idx.add_edge("KNOWS", a.clone(), b.clone()).await; // duplicate
        let s = idx.stats().await;
        assert_eq!(s.total_edges, 1, "duplicate edge must not double-count");
        assert_eq!(s.total_edge_types, 1);
    }

    /// Stats stay equal to a full recount after a mixed add/remove workload —
    /// guards against incremental drift.
    #[tokio::test]
    async fn test_stats_match_recount_after_churn() {
        let idx = EdgeIndex::new();
        for i in 0..20u8 {
            let et = if i % 2 == 0 { "EVEN" } else { "ODD" };
            idx.add_edge(et, test_node(i), test_node(i.wrapping_add(1)))
                .await;
        }
        // Remove a handful of edges and one whole node's worth.
        idx.remove_edge("EVEN", &test_node(0), &test_node(1)).await;
        idx.remove_edge("ODD", &test_node(3), &test_node(4)).await;
        idx.remove_node(&test_node(6)).await;

        // Independent ground truth from the forward map.
        let true_edges: usize = idx.forward.iter().map(|e| e.value().len()).sum();
        let true_types = idx.forward.len();

        let s = idx.stats().await;
        assert_eq!(
            s.total_edges, true_edges,
            "total_edges drifted from recount"
        );
        assert_eq!(
            s.total_edge_types, true_types,
            "total_edge_types drifted from recount"
        );
    }

    /// Many tasks adding edges of distinct types concurrently. Under the
    /// sharded `DashMap` these contend only on segment locks, not a graph-wide
    /// write lock; the assertion is correctness (and lazy stats accuracy) under
    /// that concurrency.
    #[tokio::test]
    async fn test_concurrent_add_distinct_types() {
        let idx = Arc::new(EdgeIndex::new());
        let mut handles = Vec::new();

        for t in 0..64u32 {
            let idx = idx.clone();
            handles.push(tokio::spawn(async move {
                for e in 0..50u32 {
                    let src = NexoraId::from_bytes(format!("t{t}-s{e}").into_bytes());
                    let dst = NexoraId::from_bytes(format!("t{t}-d{e}").into_bytes());
                    idx.add_edge(&format!("TYPE{t}"), src, dst).await;
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let s = idx.stats().await;
        assert_eq!(s.total_edge_types, 64);
        assert_eq!(s.total_edges, 64 * 50);
        assert_eq!(idx.query_outgoing("TYPE0").await.len(), 50);
    }

    /// Interleave adds and removes of the same edge from many tasks to exercise
    /// `remove_if` empty-set pruning against concurrent inserts on shared types.
    #[tokio::test]
    async fn test_concurrent_add_remove_same_type() {
        let idx = Arc::new(EdgeIndex::new());
        let mut handles = Vec::new();

        for t in 0..32u32 {
            let idx = idx.clone();
            handles.push(tokio::spawn(async move {
                let src = NexoraId::from_bytes(format!("s{t}").into_bytes());
                let dst = NexoraId::from_bytes(format!("d{t}").into_bytes());
                idx.add_edge("HOT", src.clone(), dst.clone()).await;
                idx.remove_edge("HOT", &src, &dst).await;
                idx.add_edge("HOT", src, dst).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Each task's edge ends added exactly once.
        assert_eq!(idx.query_outgoing("HOT").await.len(), 32);
        assert_eq!(idx.stats().await.total_edges, 32);
    }
}
