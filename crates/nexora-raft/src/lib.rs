//! Raft-based WAL write-through replication.
//!
//! This module provides a simplified Raft protocol focused on log replication
//! and quorum commit. It bridges the gap between local WAL writes and
//! distributed replication — ensuring that every WAL entry is replicated to a
//! quorum of followers before the mutation is committed to the in-memory graph.
//!
//! ## Architecture
//!
//! ```text
//! Mutation → WAL.append()  →  RaftLogReplicator.replicate()
//!                              ├→ follower_1: append_to_wal(entry)
//!                              ├→ follower_2: append_to_wal(entry)
//!                              └→ wait for quorum acks
//!                                          ↓
//!                              quorum reached → commit to memory
//! ```
//!
//! ## Key Design Decisions
//!
//! - **No leader election**: In nexora, shard ownership is managed by
//!   ShardMap + OwnerEpoch. A shard owner is the de facto leader for that
//!   shard. Leader election is replaced by the ControlPlane's failover
//!   mechanism.
//! - **Raft log = WAL**: We reuse the existing WAL as the Raft log. Each
//!   WAL entry (with its seq_no) serves as a Raft log entry.
//! - **Quorum commit**: Entries are appended to the local WAL first, then
//!   replicated to followers. Only when quorum acks are received does the
//!   entry become "committed" and applied to the in-memory state.

use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex, RwLock};

/// A Raft log entry representing a WAL record to replicate.
#[derive(Clone, Debug)]
pub struct LogEntry {
    /// Monotonic sequence number (maps to WAL seq_no).
    pub seq_no: u64,
    /// The shard this entry belongs to.
    pub shard_id: usize,
    /// The operation payload (serialized WAL record bytes).
    pub payload: Vec<u8>,
    /// The epoch under which this entry was written.
    pub epoch: u64,
    /// The term for this log entry (Raft term).
    pub term: u64,
}

/// Replication client — sends log entries to remote followers.
#[async_trait::async_trait]
pub trait ReplicationTarget: Send + Sync {
    /// Append a log entry to the follower's WAL. Returns the highest
    /// seq_no the follower has committed.
    async fn append_entries(
        &self,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<AppendEntriesResponse, ReplicationError>;

    /// Request a snapshot transfer starting from seq_no.
    async fn install_snapshot(
        &self,
        shard_id: usize,
        from_seq: u64,
    ) -> Result<SnapshotData, ReplicationError>;
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesResponse {
    /// The follower's current term.
    pub term: u64,
    /// Whether the append was successful.
    pub success: bool,
    /// Last committed seq_no on the follower.
    pub last_committed: u64,
    /// Last log seq_no on the follower.
    pub last_log_seq: u64,
}

#[derive(Clone, Debug)]
pub struct SnapshotData {
    pub shard_id: usize,
    pub last_seq_no: u64,
    pub entries: Vec<Vec<u8>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("follower rejected: term={follower_term}, leader={leader_term}")]
    StaleTerm {
        follower_term: u64,
        leader_term: u64,
    },
    #[error("timeout")]
    Timeout,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("channel closed: {0}")]
    ChannelClosed(String),
}

/// Result of a quorum replication attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuorumResult {
    /// Quorum reached; the entry can be committed.
    Committed { acked: usize, total: usize },
    /// Quorum not reached; the entry may be retried.
    NotCommitted { acked: usize, required: usize },
    /// Follower has a higher term; leader should step down.
    HigherTerm { follower_term: u64 },
}

/// Configuration for the Raft log replicator.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// How many nodes (including leader) must acknowledge before commit.
    pub quorum_size: usize,
    /// Total nodes in the replica set.
    pub total_nodes: usize,
    /// Timeout for replication RPC calls.
    pub rpc_timeout: Duration,
    /// Maximum batch size for AppendEntries.
    pub max_batch_size: usize,
    /// Current Raft term.
    pub current_term: u64,
    /// Local node ID.
    pub node_id: String,
    /// Shard ID this replicator manages.
    pub shard_id: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            quorum_size: 2, // majority of 3
            total_nodes: 3,
            rpc_timeout: Duration::from_secs(5),
            max_batch_size: 100,
            current_term: 1,
            node_id: "local".into(),
            shard_id: 0,
        }
    }
}

/// Tracks per-follower replication progress.
#[derive(Clone, Debug)]
struct FollowerProgress {
    /// Next seq_no to send to this follower.
    next_seq: u64,
    /// Highest seq_no known to be replicated to this follower.
    match_seq: u64,
    /// Last known response from this follower.
    last_response: AppendEntriesResponse,
}

/// Raft log replicator — the core write-through replication engine.
///
/// Manages replication of local WAL entries to follower nodes and tracks
/// quorum commit progress.
pub struct RaftLogReplicator {
    pub config: RaftConfig,
    /// Committed seq_no — all entries up to this are committed.
    commit_index: RwLock<u64>,
    /// Last seq_no appended to the local WAL.
    last_applied: RwLock<u64>,
    /// Per-follower replication progress.
    followers: Mutex<HashMap<String, FollowerProgress>>,
    /// Channels waiting for commit confirmation, keyed by seq_no.
    commit_waiters: Mutex<HashMap<u64, Vec<oneshot::Sender<QuorumResult>>>>,
    /// Directory where commit_index/last_applied are persisted. None = in-memory only.
    state_dir: Option<std::path::PathBuf>,
}

/// Load persisted commit_index/last_applied from files, or return (0, 0) if absent.
fn load_raft_state(state_dir: &std::path::Path, shard_id: usize) -> (u64, u64) {
    let commit_path = state_dir.join(format!("raft_commit_index_shard{}", shard_id));
    let applied_path = state_dir.join(format!("raft_last_applied_shard{}", shard_id));
    let load = |path: &std::path::Path| -> u64 {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    };
    (load(&commit_path), load(&applied_path))
}

/// Persist commit_index or last_applied to disk with fsync + atomic rename.
fn persist_raft_value(state_dir: &std::path::Path, shard_id: usize, name: &str, value: u64) {
    use std::io::Write;
    let path = state_dir.join(format!("raft_{}_shard{}", name, shard_id));
    let tmp = path.with_extension("tmp");
    let write = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(value.to_string().as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)
    })();
    if let Err(e) = write {
        tracing::warn!(error = %e, name = name, shard = shard_id, "failed to persist Raft state");
    }
}

impl RaftLogReplicator {
    pub fn new(config: RaftConfig) -> Self {
        Self {
            config,
            commit_index: RwLock::new(0),
            last_applied: RwLock::new(0),
            followers: Mutex::new(HashMap::new()),
            commit_waiters: Mutex::new(HashMap::new()),
            state_dir: None,
        }
    }

    /// Create a RaftLogReplicator with persistent state (commit_index/last_applied).
    /// State is loaded from `state_dir/raft_{commit_index|last_applied}_shard{shard_id}`.
    pub fn with_persistence(config: RaftConfig, state_dir: std::path::PathBuf) -> Self {
        let (commit, applied) = load_raft_state(&state_dir, config.shard_id);
        if commit > 0 || applied > 0 {
            tracing::info!(
                commit_index = commit,
                last_applied = applied,
                shard = config.shard_id,
                "restored Raft replicator state from disk"
            );
        }
        Self {
            config,
            commit_index: RwLock::new(commit),
            last_applied: RwLock::new(applied),
            followers: Mutex::new(HashMap::new()),
            commit_waiters: Mutex::new(HashMap::new()),
            state_dir: Some(state_dir),
        }
    }

    /// Register a follower with its initial state.
    pub async fn register_follower(&self, node_id: &str, last_seq: u64, last_committed: u64) {
        let mut followers = self.followers.lock().await;
        followers.insert(
            node_id.to_string(),
            FollowerProgress {
                next_seq: last_seq + 1,
                match_seq: last_seq,
                last_response: AppendEntriesResponse {
                    term: self.config.current_term,
                    success: true,
                    last_committed,
                    last_log_seq: last_seq,
                },
            },
        );
        tracing::info!(
            node_id = node_id,
            next_seq = last_seq + 1,
            "Registered follower"
        );
    }

    /// Remove a follower (e.g., on failure detection).
    pub async fn remove_follower(&self, node_id: &str) {
        let mut followers = self.followers.lock().await;
        followers.remove(node_id);
        tracing::warn!(node_id = node_id, "Removed follower");
    }

    /// Notify the replicator of a new local WAL entry.
    /// Returns a channel that resolves when the entry is committed.
    pub async fn append_local(&self, seq_no: u64) -> oneshot::Receiver<QuorumResult> {
        let new_applied = {
            let mut applied = self.last_applied.write().await;
            *applied = (*applied).max(seq_no);
            *applied
        };

        // Persist last_applied durably after updating in-memory state
        if let Some(ref dir) = self.state_dir {
            persist_raft_value(dir, self.config.shard_id, "last_applied", new_applied);
        }

        let (tx, rx) = oneshot::channel();
        let mut waiters = self.commit_waiters.lock().await;
        waiters.entry(seq_no).or_default().push(tx);

        // The leader counts as one vote toward quorum. Only self-commit when the
        // configured quorum can be satisfied by the leader alone (quorum_size <= 1
        // — e.g. a genuine single-node cluster). Otherwise the entry stays pending
        // until enough followers acknowledge it via try_advance_commit().
        //
        // A partitioned leader with zero reachable followers but quorum_size > 1
        // must NOT report the write as committed: doing so would let it durably
        // accept writes a majority never saw, diverging from a quorum elected on
        // the other side of the partition.
        if self.config.quorum_size <= 1 {
            drop(waiters);
            self.advance_commit_index(seq_no).await;
        }

        rx
    }

    /// Replicate entries to a specific follower.
    /// Called periodically or when new entries are appended.
    pub async fn replicate_to(
        &self,
        target: &dyn ReplicationTarget,
        follower_id: &str,
    ) -> Result<AppendEntriesResponse, ReplicationError> {
        // C-1 FIX: Establish lock ordering to prevent deadlock.
        // Always acquire locks in this order: last_applied → followers → commit_index
        // This ensures no circular dependency regardless of concurrent call patterns.

        let last_applied = *self.last_applied.read().await;

        let (entries, _next_seq) = {
            let followers = self.followers.lock().await;
            let progress = match followers.get(follower_id) {
                Some(p) => p.clone(),
                None => {
                    return Err(ReplicationError::Connection(format!(
                        "Unknown follower: {}",
                        follower_id
                    )))
                }
            };

            let mut entries = Vec::new();
            let mut seq = progress.next_seq;

            // Collect entries to send (up to max_batch_size)
            // In a real implementation, we'd read from WAL here.
            // For now, we pass the seq_no range and let the caller fill payloads.
            while seq <= last_applied && entries.len() < self.config.max_batch_size {
                entries.push(LogEntry {
                    seq_no: seq,
                    shard_id: self.config.shard_id,
                    payload: vec![], // Will be filled by WAL reader
                    epoch: 0,        // Will be filled by WAL reader
                    term: self.config.current_term,
                });
                seq += 1;
            }
            (entries, progress.next_seq)
        };

        if entries.is_empty() {
            return Ok(AppendEntriesResponse {
                term: self.config.current_term,
                success: true,
                last_committed: *self.commit_index.read().await,
                last_log_seq: *self.last_applied.read().await,
            });
        }

        // Fetch actual payloads from WAL before sending
        // (caller must fill these in for a real implementation)

        let commit_index = *self.commit_index.read().await;

        // Wrap RPC with timeout to prevent indefinite blocking on network issues
        let result = tokio::time::timeout(
            self.config.rpc_timeout,
            target.append_entries(entries.clone(), commit_index)
        )
        .await
        .map_err(|_| ReplicationError::Timeout)?;

        match &result {
            Ok(resp) if resp.success => {
                let mut followers = self.followers.lock().await;
                if let Some(progress) = followers.get_mut(follower_id) {
                    let last_seq = entries
                        .last()
                        .map(|e| e.seq_no)
                        .unwrap_or(progress.match_seq);
                    progress.next_seq = last_seq + 1;
                    progress.match_seq = last_seq;
                    progress.last_response = resp.clone();

                    tracing::trace!(
                        follower = follower_id,
                        match_seq = last_seq,
                        "Replicated to follower"
                    );
                }
                self.try_advance_commit().await;
            }
            Ok(resp) if resp.term > self.config.current_term => {
                return Err(ReplicationError::StaleTerm {
                    follower_term: resp.term,
                    leader_term: self.config.current_term,
                });
            }
            Ok(_) => {
                // Follower rejected but term is ok — will retry
                tracing::warn!(follower = follower_id, "Follower rejected append");
            }
            Err(_) => {
                tracing::warn!(follower = follower_id, "Replication failed");
            }
        }

        result
    }

    /// Try to advance the commit index based on quorum.
    async fn try_advance_commit(&self) {
        let last_applied = *self.last_applied.read().await;
        let followers = self.followers.lock().await;

        // Collect match_seq from all followers
        let mut matched: Vec<u64> = followers.values().map(|p| p.match_seq).collect();
        // Add leader (local) — all entries up to last_applied are on leader
        matched.push(last_applied);

        // Sort and find the median (quorum position)
        matched.sort_unstable();
        let quorum_pos = matched.len().saturating_sub(self.config.quorum_size);
        let new_commit = matched[quorum_pos];

        // Drop followers lock before acquiring commit_index write lock to avoid deadlock
        drop(followers);

        // Acquire write lock upfront to prevent race condition
        let mut commit = self.commit_index.write().await;
        let old_commit = *commit;

        if new_commit > old_commit {
            *commit = new_commit;
            drop(commit); // Release write lock before I/O

            // Persist commit_index durably after updating in-memory state
            if let Some(ref dir) = self.state_dir {
                persist_raft_value(dir, self.config.shard_id, "commit_index", new_commit);
            }

            // Notify all waiters for seq_no <= new_commit
            let mut waiters = self.commit_waiters.lock().await;
            let keys: Vec<u64> = waiters
                .keys()
                .copied()
                .filter(|&s| s <= new_commit)
                .collect();

            for seq in keys {
                if let Some(channels) = waiters.remove(&seq) {
                    for tx in channels {
                        let _ = tx.send(QuorumResult::Committed {
                            acked: self.config.quorum_size,
                            total: self.config.total_nodes,
                        });
                    }
                }
            }

            tracing::debug!(
                old_commit = old_commit,
                new_commit = new_commit,
                waiters_remaining = waiters.len(),
                "Advanced commit index"
            );
        }
    }

    /// Advance commit index and notify waiters.
    /// Uses atomic compare-and-swap to prevent race conditions.
    async fn advance_commit_index(&self, new_commit: u64) {
        // Acquire write lock upfront to prevent race condition
        let mut commit = self.commit_index.write().await;

        if new_commit <= *commit {
            return;
        }

        let old_commit = *commit;
        *commit = new_commit;
        drop(commit); // Release write lock before I/O

        // Persist commit_index durably after updating in-memory state
        if let Some(ref dir) = self.state_dir {
            persist_raft_value(dir, self.config.shard_id, "commit_index", new_commit);
        }

        // Notify all waiters for seq_no <= new_commit
        let mut waiters = self.commit_waiters.lock().await;
        let keys: Vec<u64> = waiters
            .keys()
            .copied()
            .filter(|&s| s <= new_commit)
            .collect();

        for seq in keys {
            if let Some(channels) = waiters.remove(&seq) {
                for tx in channels {
                    let _ = tx.send(QuorumResult::Committed {
                        acked: self.config.quorum_size,
                        total: self.config.total_nodes,
                    });
                }
            }
        }

        tracing::debug!(
            old_commit = old_commit,
            new_commit = new_commit,
            waiters_remaining = waiters.len(),
            "Advanced commit index"
        );
    }

    /// Get current commit index.
    pub async fn commit_index(&self) -> u64 {
        *self.commit_index.read().await
    }

    /// Get last applied index.
    pub async fn last_applied(&self) -> u64 {
        *self.last_applied.read().await
    }

    /// Get active follower count.
    pub async fn follower_count(&self) -> usize {
        self.followers.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_immediate_commit_no_followers() {
        let config = RaftConfig {
            quorum_size: 1,
            total_nodes: 1,
            ..Default::default()
        };
        let replicator = RaftLogReplicator::new(config);

        let mut rx = replicator.append_local(1).await;
        let result = rx.try_recv().unwrap();
        assert_eq!(result, QuorumResult::Committed { acked: 1, total: 1 });
        assert_eq!(replicator.commit_index().await, 1);
    }

    #[tokio::test]
    async fn test_quorum_advance_with_followers() {
        let config = RaftConfig {
            quorum_size: 2, // need leader + 1 follower
            total_nodes: 3,
            shard_id: 0,
            ..Default::default()
        };
        let replicator = Arc::new(RaftLogReplicator::new(config));

        // Register two followers
        replicator.register_follower("follower-1", 0, 0).await;
        replicator.register_follower("follower-2", 0, 0).await;

        // Append 5 entries locally
        for i in 1..=5 {
            let mut rx = replicator.append_local(i).await;
            // With followers registered but not yet replicated, nothing committed
            if i < 5 {
                assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
            }
        }

        assert_eq!(replicator.last_applied().await, 5);
        assert_eq!(replicator.commit_index().await, 0); // nothing committed yet

        // Simulate follower-1 acks up to seq 5
        {
            let mut followers = replicator.followers.lock().await;
            if let Some(p) = followers.get_mut("follower-1") {
                p.match_seq = 5;
                p.next_seq = 6;
            }
        }
        replicator.try_advance_commit().await;

        // With leader(5) + follower-1(5) = 2 out of 3, quorum(2) reached
        assert_eq!(replicator.commit_index().await, 5);

        // Waiters for seq 1-5 should be resolved
        let _rx = replicator.append_local(1).await; // Already committed
                                                    // commit_index already advanced past seq 1
    }

    #[tokio::test]
    async fn test_register_and_remove_follower() {
        let config = RaftConfig::default();
        let replicator = RaftLogReplicator::new(config);

        replicator.register_follower("f-1", 10, 8).await;
        replicator.register_follower("f-2", 5, 3).await;
        assert_eq!(replicator.follower_count().await, 2);

        replicator.remove_follower("f-1").await;
        assert_eq!(replicator.follower_count().await, 1);

        let followers = replicator.followers.lock().await;
        let p = followers.get("f-2").unwrap();
        assert_eq!(p.match_seq, 5);
        assert_eq!(p.next_seq, 6);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_follower() {
        let replicator = RaftLogReplicator::new(RaftConfig::default());
        // Removing a follower that doesn't exist should not panic
        replicator.remove_follower("ghost").await;
        assert_eq!(replicator.follower_count().await, 0);
    }

    #[tokio::test]
    async fn test_no_followers_immediate_commit() {
        let config = RaftConfig {
            quorum_size: 1,
            total_nodes: 1,
            ..Default::default()
        };
        let replicator = RaftLogReplicator::new(config);

        // With no followers and quorum_size=1, entries should commit immediately
        let mut rx1 = replicator.append_local(1).await;
        let result1 = rx1.try_recv().unwrap();
        assert_eq!(result1, QuorumResult::Committed { acked: 1, total: 1 });

        let mut rx2 = replicator.append_local(2).await;
        let result2 = rx2.try_recv().unwrap();
        assert_eq!(result2, QuorumResult::Committed { acked: 1, total: 1 });

        assert_eq!(replicator.commit_index().await, 2);
        assert_eq!(replicator.last_applied().await, 2);
    }

    #[tokio::test]
    async fn test_append_local_updates_last_applied() {
        let config = RaftConfig {
            quorum_size: 1,
            total_nodes: 1,
            ..Default::default()
        };
        let replicator = RaftLogReplicator::new(config);

        // Append multiple entries
        for i in 1..=10 {
            let mut rx = replicator.append_local(i).await;
            let _ = rx.try_recv().unwrap();
        }

        assert_eq!(replicator.last_applied().await, 10);
        assert_eq!(replicator.commit_index().await, 10);
    }

    #[tokio::test]
    async fn test_quorum_not_reached_with_followers() {
        let config = RaftConfig {
            quorum_size: 3, // need 3 out of 3
            total_nodes: 3,
            ..Default::default()
        };
        let replicator = RaftLogReplicator::new(config);

        replicator.register_follower("f-1", 0, 0).await;
        replicator.register_follower("f-2", 0, 0).await;

        let mut rx = replicator.append_local(1).await;
        // With quorum_size=3, even with both followers acking we'd need all 3
        // But followers haven't acked yet, so commit should be pending
        assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
        assert_eq!(replicator.commit_index().await, 0);
    }

    #[tokio::test]
    async fn test_follower_count_after_multiple_operations() {
        let replicator = RaftLogReplicator::new(RaftConfig::default());

        assert_eq!(replicator.follower_count().await, 0);

        replicator.register_follower("a", 0, 0).await;
        assert_eq!(replicator.follower_count().await, 1);

        replicator.register_follower("b", 0, 0).await;
        replicator.register_follower("c", 0, 0).await;
        assert_eq!(replicator.follower_count().await, 3);

        replicator.remove_follower("b").await;
        assert_eq!(replicator.follower_count().await, 2);

        replicator.remove_follower("a").await;
        replicator.remove_follower("c").await;
        assert_eq!(replicator.follower_count().await, 0);
    }

    #[tokio::test]
    async fn test_re_register_follower() {
        let replicator = RaftLogReplicator::new(RaftConfig::default());

        replicator.register_follower("f-1", 10, 8).await;
        // Re-registering should update the follower's state
        replicator.register_follower("f-1", 20, 15).await;
        assert_eq!(replicator.follower_count().await, 1);

        let followers = replicator.followers.lock().await;
        let p = followers.get("f-1").unwrap();
        assert_eq!(p.match_seq, 20);
        assert_eq!(p.next_seq, 21);
    }

    #[test]
    fn test_raft_config_default() {
        let config = RaftConfig::default();
        assert_eq!(config.quorum_size, 2);
        assert_eq!(config.total_nodes, 3);
        assert_eq!(config.rpc_timeout, Duration::from_secs(5));
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.current_term, 1);
        assert_eq!(config.node_id, "local");
        assert_eq!(config.shard_id, 0);
    }

    #[test]
    fn test_log_entry_clone_debug() {
        let entry = LogEntry {
            seq_no: 42,
            shard_id: 1,
            payload: vec![1, 2, 3],
            epoch: 10,
            term: 5,
        };
        let cloned = entry.clone();
        assert_eq!(cloned.seq_no, 42);
        assert_eq!(cloned.payload, vec![1, 2, 3]);
        // Verify Debug derives
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("seq_no: 42"));
    }

    #[test]
    fn test_append_entries_response_serde() {
        let resp = AppendEntriesResponse {
            term: 5,
            success: true,
            last_committed: 100,
            last_log_seq: 105,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: AppendEntriesResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.term, 5);
        assert!(deserialized.success);
        assert_eq!(deserialized.last_committed, 100);
        assert_eq!(deserialized.last_log_seq, 105);
    }

    #[test]
    fn test_quorum_result_equality() {
        let a = QuorumResult::Committed { acked: 2, total: 3 };
        let b = QuorumResult::Committed { acked: 2, total: 3 };
        assert_eq!(a, b);

        let c = QuorumResult::NotCommitted {
            acked: 1,
            required: 2,
        };
        let d = QuorumResult::NotCommitted {
            acked: 1,
            required: 2,
        };
        assert_eq!(c, d);

        let e = QuorumResult::HigherTerm { follower_term: 10 };
        let f = QuorumResult::HigherTerm { follower_term: 10 };
        assert_eq!(e, f);

        assert_ne!(a, c);
    }

    #[test]
    fn test_replication_error_display() {
        let conn_err = ReplicationError::Connection("network down".into());
        assert_eq!(format!("{conn_err}"), "connection failed: network down");

        let stale_err = ReplicationError::StaleTerm {
            follower_term: 5,
            leader_term: 3,
        };
        assert_eq!(
            format!("{stale_err}"),
            "follower rejected: term=5, leader=3"
        );

        let timeout_err = ReplicationError::Timeout;
        assert_eq!(format!("{timeout_err}"), "timeout");

        let ser_err = ReplicationError::Serialization("bad data".into());
        assert_eq!(format!("{ser_err}"), "serialization error: bad data");
    }

    #[test]
    fn test_snapshot_data_debug() {
        let snap = SnapshotData {
            shard_id: 0,
            last_seq_no: 42,
            entries: vec![vec![1, 2], vec![3, 4]],
        };
        let debug_str = format!("{:?}", snap);
        assert!(debug_str.contains("shard_id: 0"));
        assert!(debug_str.contains("last_seq_no: 42"));
    }

    #[tokio::test]
    async fn raft_state_persistence_survives_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().to_path_buf();
        let config = RaftConfig {
            shard_id: 3,
            ..RaftConfig::default()
        };

        // First session: create replicator with persistence, append entries
        let replicator = RaftLogReplicator::with_persistence(config.clone(), state_dir.clone());
        let _rx1 = replicator.append_local(10).await;
        let _rx2 = replicator.append_local(20).await;
        // Simulate quorum commit advancing to 15
        replicator.advance_commit_index(15).await;

        // Verify in-memory state
        assert_eq!(*replicator.last_applied.read().await, 20);
        assert_eq!(*replicator.commit_index.read().await, 15);

        drop(replicator);

        // Second session: reload from disk
        let replicator2 = RaftLogReplicator::with_persistence(config.clone(), state_dir.clone());
        assert_eq!(
            *replicator2.last_applied.read().await,
            20,
            "last_applied must survive restart"
        );
        assert_eq!(
            *replicator2.commit_index.read().await,
            15,
            "commit_index must survive restart"
        );

        // Verify files exist
        let commit_file = state_dir.join("raft_commit_index_shard3");
        let applied_file = state_dir.join("raft_last_applied_shard3");
        assert!(commit_file.exists());
        assert!(applied_file.exists());
        assert_eq!(std::fs::read_to_string(&commit_file).unwrap().trim(), "15");
        assert_eq!(std::fs::read_to_string(&applied_file).unwrap().trim(), "20");
    }

    #[tokio::test]
    async fn raft_state_new_without_persistence_defaults_zero() {
        let replicator = RaftLogReplicator::new(RaftConfig::default());
        assert_eq!(*replicator.last_applied.read().await, 0);
        assert_eq!(*replicator.commit_index.read().await, 0);
        assert!(replicator.state_dir.is_none());
    }
}

pub mod anti_entropy;
pub mod state_transfer;
pub mod write_through;

pub use anti_entropy::{
    AntiEntropyScheduler, HintDeliveryHandler, HintedHandoffManager, HintedWrite, MerkleTree,
    ReadRepairEngine, ReadRepairStrategy,
};
pub use state_transfer::{
    SnapshotMeta, StateTransferClient, StateTransferManager, StateTransferRequest,
    StateTransferResponse, StateTransferResult,
};
pub use write_through::{CommitGate, NodeCommitState, WalLogReader};
