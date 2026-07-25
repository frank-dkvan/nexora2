//! Replication progress tracking for each shard.
//!
//! Maintains commit_index per shard to track which writes have been
//! successfully replicated to a quorum of replicas.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Per-shard replication progress tracker
#[derive(Clone, Debug)]
pub struct ReplicationProgress {
    /// Per-shard progress state
    state: Arc<RwLock<HashMap<usize, ShardProgress>>>,
}

/// Progress state for a single shard
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardProgress {
    /// Shard ID
    pub shard_id: usize,
    /// Highest sequence number committed to a quorum
    pub commit_index: u64,
    /// Highest sequence number written by the owner
    pub last_written: u64,
    /// Per-replica progress (node_id → last_acked_seq)
    pub replica_progress: HashMap<String, u64>,
}

impl ReplicationProgress {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize a shard's progress tracking
    pub async fn init_shard(&self, shard_id: usize, replicas: Vec<String>) {
        let mut state = self.state.write().await;
        state.insert(
            shard_id,
            ShardProgress {
                shard_id,
                commit_index: 0,
                last_written: 0,
                replica_progress: replicas.into_iter().map(|r| (r, 0)).collect(),
            },
        );
    }

    /// Record a write at the owner
    pub async fn record_write(&self, shard_id: usize, seq: u64) {
        let mut state = self.state.write().await;
        // Lazily create the shard entry: the production writer records writes/acks
        // without a prior `init_shard`, so an `if let Some` guard here would make
        // every call a silent no-op and the read-side C2 gate would never engage.
        let progress = state.entry(shard_id).or_insert_with(|| ShardProgress {
            shard_id,
            commit_index: 0,
            last_written: 0,
            replica_progress: HashMap::new(),
        });
        progress.last_written = progress.last_written.max(seq);
    }

    /// Record a replica acknowledgment
    pub async fn record_ack(&self, shard_id: usize, replica: &str, seq: u64) {
        let mut state = self.state.write().await;
        // Lazily create the shard entry (see record_write) so a production ack
        // without a prior init_shard still advances the commit index.
        let progress = state.entry(shard_id).or_insert_with(|| ShardProgress {
            shard_id,
            commit_index: 0,
            last_written: 0,
            replica_progress: HashMap::new(),
        });
        {
            progress
                .replica_progress
                .entry(replica.to_string())
                .and_modify(|v| *v = (*v).max(seq))
                .or_insert(seq);

            // Update commit_index if we have quorum
            let rf = progress.replica_progress.len();
            let quorum_size = (rf / 2) + 1;

            let mut acked_seqs: Vec<u64> = progress.replica_progress.values().copied().collect();
            acked_seqs.sort_unstable();

            // The commit_index is the seq that at least quorum_size replicas have acked
            if acked_seqs.len() >= quorum_size {
                let new_commit_index = acked_seqs[acked_seqs.len() - quorum_size];
                if new_commit_index > progress.commit_index {
                    tracing::debug!(
                        shard_id = shard_id,
                        old_commit_index = progress.commit_index,
                        new_commit_index = new_commit_index,
                        quorum_size = quorum_size,
                        "advancing commit index"
                    );
                    progress.commit_index = new_commit_index;
                }
            }
        }
    }

    /// Get the commit index for a shard
    pub async fn get_commit_index(&self, shard_id: usize) -> Option<u64> {
        let state = self.state.read().await;
        state.get(&shard_id).map(|p| p.commit_index)
    }

    /// Get the replication lag for a shard (last_written - commit_index)
    pub async fn get_replication_lag(&self, shard_id: usize) -> Option<u64> {
        let state = self.state.read().await;
        state
            .get(&shard_id)
            .map(|p| p.last_written.saturating_sub(p.commit_index))
    }

    /// Get full progress info for a shard
    pub async fn get_shard_progress(&self, shard_id: usize) -> Option<ShardProgress> {
        let state = self.state.read().await;
        state.get(&shard_id).cloned()
    }

    /// Get progress for all shards
    pub async fn get_all_progress(&self) -> HashMap<usize, ShardProgress> {
        let state = self.state.read().await;
        state.clone()
    }

    /// Check if a given sequence number has been committed to quorum
    pub async fn is_committed(&self, shard_id: usize, seq: u64) -> bool {
        let state = self.state.read().await;
        state
            .get(&shard_id)
            .map(|p| seq <= p.commit_index)
            .unwrap_or(false)
    }

    /// Reset progress for a shard (e.g., after failover)
    pub async fn reset_shard(&self, shard_id: usize) {
        let mut state = self.state.write().await;
        if let Some(progress) = state.get_mut(&shard_id) {
            progress.commit_index = 0;
            progress.last_written = 0;
            progress.replica_progress.clear();
        }
    }

    /// C1: has `replica` applied writes up to at least `seq` for this shard?
    ///
    /// This is the follower-read guard for session read-after-write: a read that
    /// may be served from a follower (during owner failover, or a C2 majority
    /// read) consults this to reject a follower that has not yet caught up to the
    /// session's last write, avoiding a stale read of the client's own write.
    ///
    /// Returns `false` if the shard or replica is unknown (fail-safe: treat an
    /// untracked replica as not-caught-up rather than risk a stale read).
    pub async fn replica_caught_up(&self, shard_id: usize, replica: &str, seq: u64) -> bool {
        // seq 0 means "no write to wait for" — any replica trivially satisfies it.
        if seq == 0 {
            return true;
        }
        let state = self.state.read().await;
        state
            .get(&shard_id)
            .and_then(|p| p.replica_progress.get(replica))
            .map(|acked| *acked >= seq)
            .unwrap_or(false)
    }

    /// C1: the owner's last-written seq for a shard — the value a client records
    /// as its session high-water after a write, then carries into later reads.
    pub async fn last_written(&self, shard_id: usize) -> Option<u64> {
        let state = self.state.read().await;
        state.get(&shard_id).map(|p| p.last_written)
    }
}

/// C1: per-session read-after-write tracker.
///
/// A client session records, per shard it has written, the owner-assigned seq of
/// its most recent write (`note_write`). A subsequent read carries these
/// high-water marks (`required_seq`) so the read path can ensure whatever replica
/// serves it has applied at least that seq — guaranteeing the session always sees
/// its own writes even if a read lands on a follower.
///
/// Scope is a single connection/session; it holds only seqs the session itself
/// produced, so it never forces a read to wait on writes from other clients
/// (that would be linearizability, which the freshness SLA does not require).
#[derive(Debug, Default)]
pub struct SessionReadTracker {
    /// shard_id → highest seq this session has written.
    high_water: std::sync::Mutex<HashMap<usize, u64>>,
}

impl SessionReadTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that this session wrote up to `seq` on `shard_id`. Monotonic: a
    /// lower seq never regresses the high-water mark.
    pub fn note_write(&self, shard_id: usize, seq: u64) {
        if seq == 0 {
            return;
        }
        let mut hw = self.high_water.lock().unwrap();
        hw.entry(shard_id)
            .and_modify(|v| *v = (*v).max(seq))
            .or_insert(seq);
    }

    /// The seq a read of `shard_id` must observe to satisfy read-after-write for
    /// this session, or 0 if the session has not written this shard (no constraint).
    pub fn required_seq(&self, shard_id: usize) -> u64 {
        *self.high_water.lock().unwrap().get(&shard_id).unwrap_or(&0)
    }
}

impl Default for ReplicationProgress {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_replication_progress_tracking() {
        let progress = ReplicationProgress::new();

        // Initialize shard with 3 replicas (RF=3)
        progress
            .init_shard(
                0,
                vec![
                    "node-1".to_string(),
                    "node-2".to_string(),
                    "node-3".to_string(),
                ],
            )
            .await;

        // Owner writes seq 1, 2, 3
        progress.record_write(0, 1).await;
        progress.record_write(0, 2).await;
        progress.record_write(0, 3).await;

        // Initially, commit_index is 0
        assert_eq!(progress.get_commit_index(0).await, Some(0));

        // node-1 acks seq 1
        progress.record_ack(0, "node-1", 1).await;
        // Still not quorum (need 2 out of 3)
        assert_eq!(progress.get_commit_index(0).await, Some(0));

        // node-2 acks seq 1
        progress.record_ack(0, "node-2", 1).await;
        // Now we have quorum for seq 1
        assert_eq!(progress.get_commit_index(0).await, Some(1));

        // node-1 acks seq 2, node-2 acks seq 2
        progress.record_ack(0, "node-1", 2).await;
        progress.record_ack(0, "node-2", 2).await;
        // Now we have quorum for seq 2
        assert_eq!(progress.get_commit_index(0).await, Some(2));

        // Check replication lag
        let lag = progress.get_replication_lag(0).await;
        assert_eq!(lag, Some(1)); // last_written=3, commit_index=2
    }

    #[tokio::test]
    async fn test_quorum_calculation() {
        let progress = ReplicationProgress::new();

        progress
            .init_shard(
                0,
                vec![
                    "node-1".to_string(),
                    "node-2".to_string(),
                    "node-3".to_string(),
                ],
            )
            .await;

        progress.record_write(0, 10).await;

        // All 3 replicas ack different seqs
        progress.record_ack(0, "node-1", 10).await;
        progress.record_ack(0, "node-2", 8).await;
        progress.record_ack(0, "node-3", 5).await;

        // Quorum (2 out of 3) is at seq 8
        assert_eq!(progress.get_commit_index(0).await, Some(8));
    }

    /// C1: replica_caught_up gates follower reads on the session's last write.
    #[tokio::test]
    async fn test_replica_caught_up_gates_follower_reads() {
        let progress = ReplicationProgress::new();
        progress
            .init_shard(0, vec!["f1".to_string(), "f2".to_string()])
            .await;
        progress.record_ack(0, "f1", 5).await;
        progress.record_ack(0, "f2", 2).await;

        // f1 (acked 5) satisfies a read requiring seq ≤ 5.
        assert!(progress.replica_caught_up(0, "f1", 5).await);
        assert!(progress.replica_caught_up(0, "f1", 3).await);
        // f1 has NOT applied seq 6 yet → not caught up.
        assert!(!progress.replica_caught_up(0, "f1", 6).await);
        // f2 (acked 2) is behind for a read requiring 5.
        assert!(!progress.replica_caught_up(0, "f2", 5).await);
        // seq 0 = no write to wait for → any replica satisfies it.
        assert!(progress.replica_caught_up(0, "f2", 0).await);
        // Unknown replica / shard → fail-safe false (never a stale read).
        assert!(!progress.replica_caught_up(0, "ghost", 1).await);
        assert!(!progress.replica_caught_up(9, "f1", 1).await);
    }

    /// C1: SessionReadTracker records per-shard write high-water monotonically.
    #[test]
    fn test_session_read_tracker_high_water() {
        let tracker = SessionReadTracker::new();
        // No write yet → no constraint.
        assert_eq!(tracker.required_seq(0), 0);

        tracker.note_write(0, 3);
        assert_eq!(tracker.required_seq(0), 3);

        // A later, higher write advances it.
        tracker.note_write(0, 7);
        assert_eq!(tracker.required_seq(0), 7);

        // An out-of-order lower seq does NOT regress the high-water.
        tracker.note_write(0, 4);
        assert_eq!(tracker.required_seq(0), 7);

        // Independent shards tracked separately.
        tracker.note_write(1, 2);
        assert_eq!(tracker.required_seq(1), 2);
        assert_eq!(tracker.required_seq(0), 7);

        // seq 0 is a no-op (unlogged write path).
        tracker.note_write(2, 0);
        assert_eq!(tracker.required_seq(2), 0);
    }

    /// C1 end-to-end mechanism: a session writes, then a follower read is only
    /// admitted once that follower has applied the session's write seq.
    #[tokio::test]
    async fn test_session_read_after_write_flow() {
        let progress = ReplicationProgress::new();
        progress
            .init_shard(0, vec!["f1".to_string(), "f2".to_string()])
            .await;
        let session = SessionReadTracker::new();

        // Owner writes seq 4; session records it.
        progress.record_write(0, 4).await;
        session.note_write(0, 4);

        let need = session.required_seq(0);
        assert_eq!(need, 4);

        // f1 has only applied up to seq 3 → must NOT serve this session's read.
        progress.record_ack(0, "f1", 3).await;
        assert!(!progress.replica_caught_up(0, "f1", need).await);

        // f1 catches up to seq 4 → now safe to serve the read-after-write.
        progress.record_ack(0, "f1", 4).await;
        assert!(progress.replica_caught_up(0, "f1", need).await);
    }
}
