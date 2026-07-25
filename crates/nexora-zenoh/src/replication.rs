//! Replication and fencing token for data consistency.
//!
//! Design doc §6.3, §11.2: Owner epoch fencing, replica quorum writes.

use crate::shard_map::{OwnerEpoch, ShardId};

/// A fencing token that prevents stale writes.
/// Any write with an epoch lower than the current owner epoch is rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FencingToken {
    pub shard_id: ShardId,
    pub epoch: OwnerEpoch,
    /// Unique request ID for deduplication
    pub request_id: Option<String>,
}

impl FencingToken {
    /// Create a new fencing token for a shard.
    pub fn new(shard_id: ShardId, epoch: OwnerEpoch) -> Self {
        Self {
            shard_id,
            epoch,
            request_id: None,
        }
    }

    /// Check if an incoming write is allowed (epoch must be strictly greater).
    /// This prevents split-brain scenarios where old and new owners with the same epoch write concurrently.
    pub fn allows_write(&self, incoming_epoch: OwnerEpoch) -> bool {
        incoming_epoch > self.epoch
    }

    /// Attach a request_id for idempotency.
    pub fn with_request_id(mut self, id: String) -> Self {
        self.request_id = Some(id);
        self
    }
}

/// Replication set: the current owner + follower replicas.
#[derive(Clone, Debug)]
pub struct ReplicaSet {
    pub shard_id: ShardId,
    pub owner: String,
    pub followers: Vec<String>,
    /// Minimum number of replicas that must acknowledge a write
    pub min_ack: usize,
}

impl ReplicaSet {
    /// Create a new replica set.
    pub fn new(shard_id: ShardId, owner: String, followers: Vec<String>) -> Self {
        let min_ack = followers.len().div_ceil(2) + 1; // majority quorum
        Self {
            shard_id,
            owner,
            followers,
            min_ack,
        }
    }

    /// Total nodes in the replica set.
    pub fn total_nodes(&self) -> usize {
        1 + self.followers.len()
    }

    /// Check if enough replicas have acknowledged.
    pub fn quorum_reached(&self, ack_count: usize) -> bool {
        ack_count >= self.min_ack
    }

    /// All nodes in the replica set (owner + followers).
    pub fn all_nodes(&self) -> Vec<&str> {
        let mut nodes: Vec<&str> = vec![&self.owner];
        nodes.extend(self.followers.iter().map(|s| s.as_str()));
        nodes
    }
}

/// Write acknowledgement from a replica.
#[derive(Clone, Debug)]
pub struct WriteAck {
    pub node_id: String,
    pub shard_id: ShardId,
    pub epoch: OwnerEpoch,
    pub success: bool,
}

/// Status of a distributed write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteStatus {
    /// Write committed on owner only (no replicas)
    CommittedLocal,
    /// Write committed with quorum (owner + majority of followers)
    CommittedQuorum { acked: usize, total: usize },
    /// Write failed to reach quorum
    Failed { acked: usize, required: usize },
    /// Request was a duplicate and already processed (P1 idempotency)
    AlreadyCommitted,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fencing_rejects_stale_epoch() {
        let token = FencingToken::new(0, OwnerEpoch::new()); // epoch=1

        // Same epoch REJECTED (prevents split-brain)
        assert!(!token.allows_write(OwnerEpoch::new()));

        // Higher epoch allowed
        assert!(token.allows_write(OwnerEpoch::new().next())); // epoch=2

        // Lower epoch rejected
        let higher_token = FencingToken::new(0, OwnerEpoch::new().next().next()); // epoch=3
        assert!(!higher_token.allows_write(OwnerEpoch::new())); // epoch=1 rejected
    }

    #[test]
    fn test_quorum_3_node_replica_set() {
        let replicas = ReplicaSet::new(0, "node-1".into(), vec!["node-2".into(), "node-3".into()]);
        assert_eq!(replicas.total_nodes(), 3);
        assert_eq!(replicas.min_ack, 2); // majority of 3
        assert!(replicas.quorum_reached(2));
        assert!(!replicas.quorum_reached(1));
    }

    #[test]
    fn test_fencing_token_with_request_id() {
        let token = FencingToken::new(0, OwnerEpoch::new()).with_request_id("req-123".into());
        assert_eq!(token.request_id, Some("req-123".into()));
    }
}
