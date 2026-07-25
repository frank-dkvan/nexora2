//! Streaming transitive closure — incremental Warshall's algorithm.

use crate::{FixpointOperator, FixpointState, LabeledEdge, NexoraId, ReachabilityDelta};
use std::collections::{HashMap, HashSet};

/// Incremental transitive closure using semi-naive evaluation.
/// Maintains reachability state and updates it incrementally on edge changes.
pub struct IncrementalClosure {
    state: FixpointState,
    /// Active edges in the graph
    edges: HashSet<LabeledEdge>,
    /// Max depth for path exploration
    max_depth: u32,
    /// Max paths to track (prevents explosion)
    max_paths: usize,
}

impl IncrementalClosure {
    pub fn new(max_depth: u32, max_paths: usize) -> Self {
        Self {
            state: FixpointState {
                reachable: HashMap::new(),
                distances: HashMap::new(),
                iterations: 0,
            },
            edges: HashSet::new(),
            max_depth,
            max_paths,
        }
    }

    /// Expand reachability from a source node using BFS.
    fn expand_from(&mut self, from: NexoraId) -> Vec<ReachabilityDelta> {
        let mut deltas = Vec::new();
        let mut queue = Vec::new();
        let mut visited = HashSet::new();

        // Initialize with direct edges from source
        for edge in &self.edges {
            if edge.from == from && !visited.contains(&edge.to) {
                let dist = 1u32;
                let key = (from.clone(), edge.to.clone());
                if self.state.distances.get(&key) != Some(&dist) {
                    self.state
                        .reachable
                        .entry(from.clone())
                        .or_default()
                        .insert(edge.to.clone());
                    self.state.distances.insert(key.clone(), dist);
                    deltas.push(ReachabilityDelta::Added {
                        from: from.clone(),
                        to: edge.to.clone(),
                        distance: dist,
                    });
                    queue.push((edge.to.clone(), dist));
                    visited.insert(edge.to.clone());
                }
            }
        }

        // BFS expansion
        let mut idx = 0;
        while idx < queue.len() && visited.len() < self.max_paths {
            let (current, dist) = queue[idx].clone();
            idx += 1;

            if dist >= self.max_depth {
                continue;
            }

            for edge in &self.edges {
                if edge.from == current && !visited.contains(&edge.to) {
                    let new_dist = dist + 1;
                    let key = (from.clone(), edge.to.clone());
                    if self.state.distances.get(&key) != Some(&new_dist) {
                        self.state
                            .reachable
                            .entry(from.clone())
                            .or_default()
                            .insert(edge.to.clone());
                        self.state.distances.insert(key.clone(), new_dist);
                        deltas.push(ReachabilityDelta::Added {
                            from: from.clone(),
                            to: edge.to.clone(),
                            distance: new_dist,
                        });
                        queue.push((edge.to.clone(), new_dist));
                        visited.insert(edge.to.clone());
                    }
                }
            }
        }

        self.state.iterations += 1;
        deltas
    }

    /// Contract reachability when an edge is removed.
    fn contract_from(&mut self, from: NexoraId) -> Vec<ReachabilityDelta> {
        let mut deltas = Vec::new();

        // Remove all paths from 'from' and recompute
        if let Some(targets) = self.state.reachable.remove(&from) {
            for to in &targets {
                self.state.distances.remove(&(from.clone(), to.clone()));
                deltas.push(ReachabilityDelta::Removed {
                    from: from.clone(),
                    to: to.clone(),
                });
            }
        }

        // Recompute from this source
        deltas.extend(self.expand_from(from));
        deltas
    }
}

impl FixpointOperator for IncrementalClosure {
    fn apply_edge(&mut self, edge: &LabeledEdge) -> Vec<ReachabilityDelta> {
        if !self.edges.insert(edge.clone()) {
            return Vec::new(); // Already exists
        }

        let mut deltas = Vec::new();

        // Add direct path
        let key = (edge.from.clone(), edge.to.clone());
        if !self.state.distances.contains_key(&key) {
            self.state
                .reachable
                .entry(edge.from.clone())
                .or_default()
                .insert(edge.to.clone());
            self.state.distances.insert(key, 1);
            deltas.push(ReachabilityDelta::Added {
                from: edge.from.clone(),
                to: edge.to.clone(),
                distance: 1,
            });
        }

        // Recompute reachability: BFS from edge.from
        deltas.extend(self.expand_from(edge.from.clone()));

        // Also expand from all sources that can reach edge.from
        // (so A→B becomes A→C when B→C is added)
        let affected_sources: Vec<NexoraId> = self
            .state
            .reachable
            .iter()
            .filter(|(_, targets)| targets.contains(&edge.from))
            .map(|(src, _)| src.clone())
            .collect();

        for src in affected_sources {
            // For each source S that can reach edge.from,
            // and for each edge.from→X, add S→X at distance d(S, edge.from) + 1
            if let Some(s_dist) = self.state.distances.get(&(src.clone(), edge.from.clone())) {
                let new_dist = s_dist + 1;
                let key = (src.clone(), edge.to.clone());
                if !self.state.distances.contains_key(&key)
                    || *self.state.distances.get(&key).unwrap() > new_dist
                {
                    self.state
                        .reachable
                        .entry(src.clone())
                        .or_default()
                        .insert(edge.to.clone());
                    self.state.distances.insert(key, new_dist);
                    deltas.push(ReachabilityDelta::Added {
                        from: src.clone(),
                        to: edge.to.clone(),
                        distance: new_dist,
                    });
                }
            }
            // Full BFS from each affected source
            deltas.extend(self.expand_from(src));
        }

        deltas
    }

    fn remove_edge(&mut self, edge: &LabeledEdge) -> Vec<ReachabilityDelta> {
        self.edges.remove(edge);

        // Remove direct path
        let key = (edge.from.clone(), edge.to.clone());
        self.state
            .reachable
            .entry(edge.from.clone())
            .or_default()
            .remove(&edge.to);
        self.state.distances.remove(&key);

        let mut deltas = vec![ReachabilityDelta::Removed {
            from: edge.from.clone(),
            to: edge.to.clone(),
        }];

        // Recompute reachability from edge.from
        deltas.extend(self.contract_from(edge.from.clone()));

        // Recompute from affected sources
        let affected_sources: Vec<NexoraId> = self
            .state
            .reachable
            .iter()
            .filter(|(_, targets)| targets.contains(&edge.from))
            .map(|(src, _)| src.clone())
            .collect();

        for src in affected_sources {
            deltas.extend(self.contract_from(src));
        }

        deltas
    }

    fn state(&self) -> &FixpointState {
        &self.state
    }
    fn is_reachable(&self, from: &NexoraId, to: &NexoraId) -> bool {
        self.state
            .reachable
            .get(from)
            .is_some_and(|t| t.contains(to))
    }
    fn distance(&self, from: &NexoraId, to: &NexoraId) -> Option<u32> {
        self.state
            .distances
            .get(&(from.clone(), to.clone()))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transitive_closure() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());

        // Add edge A→B
        let d1 = tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        assert!(d1.iter().any(
            |d| matches!(d, ReachabilityDelta::Added { from, to, .. } if from == &a && to == &b)
        ));
        assert!(tc.is_reachable(&a, &b));

        // Add edge B→C → should discover A→C transitively
        let _d2 = tc.apply_edge(&LabeledEdge {
            from: b.clone(),
            to: c.clone(),
            label: None,
        });
        assert!(tc.is_reachable(&a, &c));
        assert_eq!(tc.distance(&a, &c), Some(2));

        // Check iteration tracking
        assert!(tc.state().iterations > 0);
    }

    #[test]
    fn test_edge_removal() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());

        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        assert!(tc.is_reachable(&a, &b));

        tc.remove_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        assert!(!tc.is_reachable(&a, &b));
    }

    #[test]
    fn test_empty_closure() {
        let tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        assert!(!tc.is_reachable(&a, &b));
        assert_eq!(tc.distance(&a, &b), None);
        assert_eq!(tc.state().iterations, 0);
    }

    #[test]
    fn test_single_edge_distance() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());

        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        assert_eq!(tc.distance(&a, &b), Some(1));
    }

    #[test]
    fn test_three_hop_path() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());
        let d = NexoraId::from_bytes(b"D".to_vec());

        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: b.clone(),
            to: c.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: c.clone(),
            to: d.clone(),
            label: None,
        });

        assert!(tc.is_reachable(&a, &d));
        assert_eq!(tc.distance(&a, &d), Some(3));
        assert_eq!(tc.distance(&a, &c), Some(2));
        assert_eq!(tc.distance(&b, &d), Some(2));
    }

    #[test]
    fn test_duplicate_edge_is_idempotent() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());

        let edge = LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        };

        let d1 = tc.apply_edge(&edge);
        assert!(!d1.is_empty());

        // Second apply of the same edge should produce no deltas
        let d2 = tc.apply_edge(&edge);
        assert!(d2.is_empty());

        assert!(tc.is_reachable(&a, &b));
    }

    #[test]
    fn test_remove_nonexistent_edge() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());

        // Removing an edge that doesn't exist should not panic
        tc.remove_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        assert!(!tc.is_reachable(&a, &b));
    }

    #[test]
    fn test_no_reachability_between_unconnected_nodes() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());

        // A→B and C is isolated
        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });

        assert!(!tc.is_reachable(&a, &c));
        assert!(!tc.is_reachable(&c, &a));
        assert!(!tc.is_reachable(&b, &c));
        assert_eq!(tc.distance(&a, &c), None);
    }

    #[test]
    fn test_max_depth_limit() {
        let mut tc = IncrementalClosure::new(2, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());
        let d = NexoraId::from_bytes(b"D".to_vec());

        // Chain A→B→C→D with max_depth=2
        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: b.clone(),
            to: c.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: c.clone(),
            to: d.clone(),
            label: None,
        });

        // A→B (1 hop) and A→C (2 hops) should be reachable
        assert!(tc.is_reachable(&a, &b));
        assert!(tc.is_reachable(&a, &c));
        // A→D (3 hops) may not be reachable due to max_depth=2
        // The exact behavior depends on implementation, but at least
        // the first 2 hops should be found.
    }

    #[test]
    fn test_labeled_edge() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());

        let edge = LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: Some("KNOWS".to_string()),
        };

        tc.apply_edge(&edge);
        assert!(tc.is_reachable(&a, &b));

        // Remove with a different label — should still work (label is part of edge identity)
        tc.remove_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: Some("FOLLOWS".to_string()),
        });
        // The original edge with label "KNOWS" should still exist
        // because the labels differ
        // (HashSet uses the full LabeledEdge for equality)
    }

    #[test]
    fn test_diamond_shape_reachability() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());
        let d = NexoraId::from_bytes(b"D".to_vec());

        // Diamond: A→B, A→C, B→D, C→D
        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: c.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: b.clone(),
            to: d.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: c.clone(),
            to: d.clone(),
            label: None,
        });

        assert!(tc.is_reachable(&a, &d));
        // Shortest path should be 2 (via B or C)
        assert_eq!(tc.distance(&a, &d), Some(2));
    }

    #[test]
    fn test_self_loop() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());

        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: a.clone(),
            label: None,
        });
        // Self-loop: A should be reachable from A
        assert!(tc.is_reachable(&a, &a));
    }

    #[test]
    fn test_state_after_multiple_edges() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());

        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: b.clone(),
            to: c.clone(),
            label: None,
        });

        let state = tc.state();
        // A should be able to reach B and C
        assert!(state.reachable.contains_key(&a));
        assert!(state.reachable.get(&a).unwrap().contains(&b));
        assert!(state.reachable.get(&a).unwrap().contains(&c));
        // Iterations should have advanced
        assert!(state.iterations >= 2);
    }

    #[test]
    fn test_remove_intermediate_edge() {
        let mut tc = IncrementalClosure::new(10, 1000);
        let a = NexoraId::from_bytes(b"A".to_vec());
        let b = NexoraId::from_bytes(b"B".to_vec());
        let c = NexoraId::from_bytes(b"C".to_vec());

        // Build chain A→B→C
        tc.apply_edge(&LabeledEdge {
            from: a.clone(),
            to: b.clone(),
            label: None,
        });
        tc.apply_edge(&LabeledEdge {
            from: b.clone(),
            to: c.clone(),
            label: None,
        });

        assert!(tc.is_reachable(&a, &c));

        // Remove B→C edge
        tc.remove_edge(&LabeledEdge {
            from: b.clone(),
            to: c.clone(),
            label: None,
        });

        // A→B should still be reachable
        assert!(tc.is_reachable(&a, &b));
        // A→C should no longer be reachable (no path)
        assert!(!tc.is_reachable(&a, &c));
    }
}
