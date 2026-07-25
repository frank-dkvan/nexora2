//! Streaming fixpoint operator — incremental graph traversal.
//!
//! Computes and maintains transitive closure, reachability, and N-hop
//! neighborhoods as graph edges change in real-time.
//!
//! This is a novel capability that neither RisingWave nor TileDB provide:
//! "When a new edge arrives, recalculate reachability in real-time."

pub mod closure;

use nexora_id::NexoraId;
use std::collections::{HashMap, HashSet};

/// A directed edge in the fixpoint graph.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LabeledEdge {
    pub from: NexoraId,
    pub to: NexoraId,
    pub label: Option<String>,
}

/// Result of a fixpoint computation.
#[derive(Clone, Debug)]
pub struct FixpointState {
    /// Reachability: for each source node, the set of reachable target nodes
    pub reachable: HashMap<NexoraId, HashSet<NexoraId>>,
    /// Shortest path distance (in hops) for each (source, target) pair
    pub distances: HashMap<(NexoraId, NexoraId), u32>,
    /// Number of iterations to converge
    pub iterations: u32,
}

/// Trait for fixpoint operators.
pub trait FixpointOperator: Send + Sync {
    /// Process a new edge and produce delta updates.
    fn apply_edge(&mut self, edge: &LabeledEdge) -> Vec<ReachabilityDelta>;
    /// Process an edge removal and produce delta removals.
    fn remove_edge(&mut self, edge: &LabeledEdge) -> Vec<ReachabilityDelta>;
    /// Get current state.
    fn state(&self) -> &FixpointState;
    /// Check if source can reach target.
    fn is_reachable(&self, from: &NexoraId, to: &NexoraId) -> bool;
    /// Get shortest path distance.
    fn distance(&self, from: &NexoraId, to: &NexoraId) -> Option<u32>;
}

/// A delta produced during incremental fixpoint recomputation.
#[derive(Clone, Debug)]
pub enum ReachabilityDelta {
    /// A new reachable path was discovered
    Added {
        from: NexoraId,
        to: NexoraId,
        distance: u32,
    },
    /// A previously reachable path was removed
    Removed { from: NexoraId, to: NexoraId },
}
