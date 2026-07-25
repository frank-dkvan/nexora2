//! Per-shard epoch fencing for the replicated write path.
//!
//! Design doc §6.3, §11.2: an owner write carries the shard's owner epoch. After
//! a failover the epoch is bumped, so a *deposed* owner that keeps replicating
//! (a slow node, or one on the losing side of a partition) ships writes stamped
//! with the **old** epoch. Followers must reject those, or the stale owner would
//! silently corrupt the shard behind the new owner's back.
//!
//! [`ShardFence`] is the per-node high-water mark of the largest epoch each shard
//! has been seen at. A replicated write is *admitted* only if its epoch is `>=`
//! the high-water mark; a lower epoch is fenced out. The mark is advanced from
//! two sources:
//! - **Shard-map updates** ([`observe_map`]): authoritative. When failover bumps
//!   a shard's epoch and pushes the new map to the router, we bump the fence too,
//!   so the promoted node rejects the old owner's stragglers immediately.
//! - **Incoming fenced writes** (the admit call itself): a self-healing backstop.
//!   Even a follower that never saw the new map learns the new epoch from the
//!   first write the new owner replicates, and fences the old owner thereafter.
//!
//! [`observe_map`]: ShardFence::observe_map

use crate::shard_map::{OwnerEpoch, ShardId, ShardMap};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Per-shard highest-seen owner epoch. Shared (via `Arc`) between a node's
/// cluster manager (which advances it on shard-map changes) and its graph
/// handler (which consults it to fence stale replicated writes).
#[derive(Clone, Default)]
pub struct ShardFence {
    epochs: Arc<RwLock<HashMap<ShardId, OwnerEpoch>>>,
}

impl ShardFence {
    /// Create an empty fence (no shard has been observed yet).
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the fence from an authoritative shard map. Monotonic: a shard's
    /// high-water mark only ever moves forward, so a stale map delivered late
    /// can never lower it.
    pub async fn observe_map(&self, map: &ShardMap) {
        let mut epochs = self.epochs.write().await;
        for (shard_id, asg) in &map.assignments {
            let entry = epochs.entry(*shard_id).or_insert(asg.epoch);
            if asg.epoch > *entry {
                *entry = asg.epoch;
            }
        }
    }

    /// Admit a replicated write for `shard_id` stamped with `epoch`.
    ///
    /// Returns `true` if the write is allowed — `epoch` is at least the current
    /// high-water mark — and records `epoch` as the new mark. Returns `false`
    /// (fenced out) if `epoch` is strictly below the mark, which means it came
    /// from an owner that has since been superseded by failover.
    pub async fn admit(&self, shard_id: ShardId, epoch: OwnerEpoch) -> bool {
        let mut epochs = self.epochs.write().await;
        match epochs.get(&shard_id).copied() {
            Some(current) if epoch < current => false,
            _ => {
                // epoch >= current (or no entry) — advance and admit.
                epochs.insert(shard_id, epoch);
                true
            }
        }
    }

    /// Current high-water epoch for a shard, if any write/map has been seen.
    pub async fn current(&self, shard_id: ShardId) -> Option<OwnerEpoch> {
        self.epochs.read().await.get(&shard_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admits_first_write_and_records_epoch() {
        let fence = ShardFence::new();
        assert!(fence.admit(5, OwnerEpoch::new()).await); // epoch 1, first sight
        assert_eq!(fence.current(5).await, Some(OwnerEpoch::new()));
    }

    #[tokio::test]
    async fn rejects_stale_epoch_after_failover() {
        let fence = ShardFence::new();
        let e1 = OwnerEpoch::new(); // 1
        let e2 = e1.next(); // 2 (post-failover)

        // New owner replicates at epoch 2 — admitted, high-water becomes 2.
        assert!(fence.admit(5, e2).await);
        // Deposed owner replicates a straggler at epoch 1 — fenced out.
        assert!(!fence.admit(5, e1).await);
        // High-water stays at 2 (the stale write did not lower it).
        assert_eq!(fence.current(5).await, Some(e2));
    }

    #[tokio::test]
    async fn same_epoch_is_admitted() {
        let fence = ShardFence::new();
        let e = OwnerEpoch::new();
        assert!(fence.admit(5, e).await);
        assert!(fence.admit(5, e).await); // repeated same-epoch write is fine
    }

    #[tokio::test]
    async fn observe_map_seeds_and_is_monotonic() {
        use crate::shard_map::{ShardAssignment, ShardMap};

        let fence = ShardFence::new();
        let mut map = ShardMap {
            version: 1,
            total_shards: 1,
            assignments: HashMap::new(),
            local_node: "node-a".into(),
        };
        let e2 = OwnerEpoch::new().next();
        map.assignments.insert(
            0,
            ShardAssignment {
                owner: "node-b".into(),
                epoch: e2,
                replicas: vec![],
                writable: true,
            },
        );
        fence.observe_map(&map).await;
        assert_eq!(fence.current(0).await, Some(e2));

        // A stale map at epoch 1 must not lower the mark.
        let mut stale = map.clone();
        stale.assignments.get_mut(&0).unwrap().epoch = OwnerEpoch::new();
        fence.observe_map(&stale).await;
        assert_eq!(fence.current(0).await, Some(e2));

        // And a straggler write at epoch 1 is fenced.
        assert!(!fence.admit(0, OwnerEpoch::new()).await);
    }
}
