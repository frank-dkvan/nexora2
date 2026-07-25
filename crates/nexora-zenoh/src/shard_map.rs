//! Versioned ShardMap — logical shard to physical node assignment.
//!
//! Design doc §6.3: fixed logical shards + versioned ShardMap + Owner epoch.

use std::collections::HashMap;

/// Logical shard identifier (0..total_shards).
pub type ShardId = usize;

/// Physical node identifier (UUID or host:port).
pub type NodeId = String;

/// Epoch for a shard owner — monotonically increasing.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct OwnerEpoch(u64);

impl Default for OwnerEpoch {
    fn default() -> Self {
        Self(1)
    }
}

impl OwnerEpoch {
    pub fn new() -> Self {
        Self(1)
    }
    pub fn from_value(value: u64) -> Self {
        Self(value)
    }
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Assignment of a single logical shard.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ShardAssignment {
    /// Current owner node
    pub owner: NodeId,
    /// Owner epoch (must increase on each change)
    pub epoch: OwnerEpoch,
    /// Replica nodes (for fault tolerance)
    pub replicas: Vec<NodeId>,
    /// Whether this assignment is writable
    pub writable: bool,
}

/// Versioned map of all shard assignments.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ShardMap {
    /// Map version (must increase on each change)
    pub version: u64,
    /// Total number of logical shards
    pub total_shards: usize,
    /// Shard assignments (may not have all shards)
    pub assignments: HashMap<ShardId, ShardAssignment>,
    /// The local node ID
    pub local_node: NodeId,
}

impl ShardMap {
    /// Create a new ShardMap with static local-only assignments.
    pub fn new_local(total_shards: usize) -> Self {
        Self::new_local_with_node(total_shards, "local-node".to_string())
    }

    /// Create a new ShardMap with static local-only assignments for a specific node ID.
    pub fn new_local_with_node(total_shards: usize, node_id: String) -> Self {
        let mut assignments = HashMap::new();
        for shard in 0..total_shards {
            assignments.insert(
                shard,
                ShardAssignment {
                    owner: node_id.clone(),
                    epoch: OwnerEpoch::new(),
                    replicas: vec![],
                    writable: true,
                },
            );
        }
        Self {
            version: 1,
            total_shards,
            assignments,
            local_node: node_id,
        }
    }

    /// Create a ShardMap that distributes shards across a set of nodes, with a
    /// replication factor of 1 (owner only, no followers). Equivalent to
    /// `new_distributed_rf(total_shards, nodes, local_node, 1)`.
    pub fn new_distributed(total_shards: usize, nodes: &[NodeId], local_node: NodeId) -> Self {
        Self::new_distributed_rf(total_shards, nodes, local_node, 1)
    }

    /// Create a ShardMap that distributes shards across a set of nodes with a
    /// given replication factor.
    ///
    /// Shards are assigned round-robin over the sorted node list, so every node
    /// derives the *same* assignment from the same membership (deterministic —
    /// no coordination needed for the initial map). For each shard the owner is
    /// `sorted[shard % n]` and its followers are the next `rf - 1` distinct
    /// nodes clockwise on the ring — so replicas of a shard never collide with
    /// its owner and are spread across different nodes. `local_node` marks which
    /// node this map belongs to (drives `is_local`). Empty `nodes` falls back to
    /// a local-only map.
    ///
    /// `replication_factor` is clamped to `[1, n]`: you cannot have more
    /// replicas than nodes.
    pub fn new_distributed_rf(
        total_shards: usize,
        nodes: &[NodeId],
        local_node: NodeId,
        replication_factor: usize,
    ) -> Self {
        if nodes.is_empty() {
            return Self::new_local_with_node(total_shards, local_node);
        }
        // Sort for determinism: all nodes must agree on the assignment.
        let mut sorted: Vec<NodeId> = nodes.to_vec();
        sorted.sort();
        sorted.dedup();
        let n = sorted.len();
        let rf = replication_factor.clamp(1, n);

        let mut assignments = HashMap::new();
        for shard in 0..total_shards {
            let owner_idx = shard % n;
            let owner = sorted[owner_idx].clone();
            // Followers: the next rf-1 distinct nodes clockwise on the ring.
            let followers: Vec<NodeId> = (1..rf)
                .map(|offset| sorted[(owner_idx + offset) % n].clone())
                .collect();
            assignments.insert(
                shard,
                ShardAssignment {
                    owner,
                    epoch: OwnerEpoch::new(),
                    replicas: followers,
                    writable: true,
                },
            );
        }
        Self {
            version: 1,
            total_shards,
            assignments,
            local_node,
        }
    }

    /// Determine which shard a NexoraId belongs to.
    pub fn shard_of(&self, qid: &nexora_id::NexoraId) -> ShardId {
        qid.shard_key() as usize % self.total_shards
    }

    /// Get the assignment for a shard.
    pub fn get(&self, shard: ShardId) -> Option<&ShardAssignment> {
        self.assignments.get(&shard)
    }

    /// Check if a shard is local.
    pub fn is_local(&self, shard: ShardId) -> bool {
        self.assignments
            .get(&shard)
            .is_some_and(|a| a.owner == self.local_node)
    }

    /// Get owner node for a NexoraId.
    pub fn owner_of(&self, qid: &nexora_id::NexoraId) -> Option<&str> {
        let shard = self.shard_of(qid);
        self.assignments.get(&shard).map(|a| a.owner.as_str())
    }

    /// Count shards owned by the local node.
    pub fn local_shard_count(&self) -> usize {
        self.assignments
            .values()
            .filter(|a| a.owner == self.local_node)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_routing() {
        let map = ShardMap::new_local(256);
        let qid = nexora_id::NexoraId::from_bytes(b"test-node".to_vec());
        let shard = map.shard_of(&qid);
        assert!(shard < 256);
        assert!(map.is_local(shard));
    }

    #[test]
    fn test_distributed_spreads_ownership() {
        let nodes = vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
        ];
        let map = ShardMap::new_distributed(9, &nodes, "node-a".to_string());
        // Each node owns 3 of the 9 shards (round-robin).
        for n in &nodes {
            let owned = map.assignments.values().filter(|a| &a.owner == n).count();
            assert_eq!(owned, 3, "{n} should own 3 shards");
        }
        // From node-a's view, only its own shards are local.
        let local = map
            .assignments
            .values()
            .filter(|a| a.owner == "node-a")
            .count();
        assert_eq!(local, 3);
        assert!(
            !map.is_local(1),
            "shard 1 (node-b) must not be local to node-a"
        );
    }

    #[test]
    fn test_distributed_is_deterministic_across_nodes() {
        // Two nodes given the same (unsorted) membership must derive the same
        // owner for every shard — otherwise routing would disagree.
        let m1 = ShardMap::new_distributed(16, &["z".into(), "a".into(), "m".into()], "a".into());
        let m2 = ShardMap::new_distributed(16, &["m".into(), "z".into(), "a".into()], "z".into());
        for shard in 0..16 {
            assert_eq!(
                m1.get(shard).unwrap().owner,
                m2.get(shard).unwrap().owner,
                "shard {shard} owner must agree regardless of input order / local node"
            );
        }
    }

    #[test]
    fn test_distributed_empty_falls_back_local() {
        let map = ShardMap::new_distributed(4, &[], "solo".into());
        assert!((0..4).all(|s| map.is_local(s)));
    }

    #[test]
    fn test_distributed_rf_assigns_distinct_followers() {
        let nodes = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let map = ShardMap::new_distributed_rf(9, &nodes, "a".into(), 3);
        for shard in 0..9 {
            let asg = map.get(shard).unwrap();
            // RF=3 → 2 followers, all distinct from owner and from each other.
            assert_eq!(asg.replicas.len(), 2, "shard {shard} needs 2 followers");
            assert!(
                !asg.replicas.contains(&asg.owner),
                "owner must not be its own replica"
            );
            assert_ne!(
                asg.replicas[0], asg.replicas[1],
                "followers must be distinct"
            );
            // With 3 nodes and RF=3, owner+followers cover all 3 nodes.
            let mut all = asg.replicas.clone();
            all.push(asg.owner.clone());
            all.sort();
            assert_eq!(
                all,
                vec!["a", "b", "c"],
                "shard {shard} must replicate to every node"
            );
        }
    }

    #[test]
    fn test_distributed_rf_clamped_to_node_count() {
        // RF larger than node count clamps: 2 nodes, RF=5 → 1 follower.
        let map = ShardMap::new_distributed_rf(4, &["a".into(), "b".into()], "a".into(), 5);
        for shard in 0..4 {
            assert_eq!(map.get(shard).unwrap().replicas.len(), 1);
        }
        // RF=1 → no followers.
        let map1 = ShardMap::new_distributed_rf(4, &["a".into(), "b".into()], "a".into(), 1);
        assert!(map1.get(0).unwrap().replicas.is_empty());
    }

    #[test]
    fn test_owner_epoch_monotonic() {
        let epoch = OwnerEpoch::new();
        assert_eq!(epoch.value(), 1);
        let next = epoch.next();
        assert_eq!(next.value(), 2);
        assert!(next > epoch);
    }

    #[test]
    fn test_shard_map_versioning() {
        let map = ShardMap::new_local(4);
        assert_eq!(map.version, 1);
        assert_eq!(map.total_shards, 4);
        assert_eq!(map.assignments.len(), 4);
    }
}
