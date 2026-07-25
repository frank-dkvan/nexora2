//! WAL Write-Through Bridge — connects RaftLogReplicator to the WAL commit path.
//!
//! This module provides the integration layer between `nexora-core`'s
//! `NodeTask::commit_operations()` and the Raft-based replication engine.
//! It ensures that every WAL write is replicated to a quorum of followers
//! before the mutation is committed to in-memory state.

use crate::{LogEntry, QuorumResult, RaftConfig, RaftLogReplicator};
use nexora_core::wal::WriteAheadLog;
use nexora_id::NexoraId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

/// CommitGate — the barrier between WAL write and memory commit.
///
/// Wraps the RaftLogReplicator to provide a simple `wait_for_commit(seq_no)`
/// interface that NodeTask can call between WAL write and memory commit.
pub struct CommitGate {
    replicator: Arc<RaftLogReplicator>,
    /// Shared WAL reference for reading entries during replication.
    wal: Option<Arc<Mutex<WriteAheadLog>>>,
    /// Mapping from NexoraId to shard_id (for multi-shard setups).
    qid_to_shard: Arc<dyn Fn(&NexoraId) -> usize + Send + Sync>,
}

impl CommitGate {
    pub fn new(
        config: RaftConfig,
        qid_to_shard: Arc<dyn Fn(&NexoraId) -> usize + Send + Sync>,
    ) -> Self {
        Self {
            replicator: Arc::new(RaftLogReplicator::new(config)),
            wal: None,
            qid_to_shard,
        }
    }

    /// Set the shared WAL reference for entry reading during replication.
    pub fn with_wal(mut self, wal: Arc<Mutex<WriteAheadLog>>) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Get a reference to the replicator.
    pub fn replicator(&self) -> &Arc<RaftLogReplicator> {
        &self.replicator
    }

    /// Register a follower node.
    pub async fn register_follower(&self, node_id: &str, last_seq: u64, last_committed: u64) {
        self.replicator
            .register_follower(node_id, last_seq, last_committed)
            .await;
    }

    /// Remove a follower node (on failure).
    pub async fn remove_follower(&self, node_id: &str) {
        self.replicator.remove_follower(node_id).await;
    }

    /// Called by NodeTask after WAL append, before memory commit.
    ///
    /// Returns a oneshot receiver that resolves when quorum is reached.
    /// NodeTask should await this before committing to memory.
    ///
    /// # Example (in NodeTask::commit_operations)
    ///
    /// ```ignore
    /// let seq_no = wal.append(op).await?;
    /// if let Some(gate) = &self.commit_gate {
    ///     let commit_rx = gate.after_wal_write(&qid, seq_no).await;
    ///     tokio::select! {
    ///         Ok(QuorumResult::Committed { .. }) = commit_rx => {
    ///             // proceed to memory commit
    ///         }
    ///         _ = tokio::time::sleep(timeout) => {
    ///             // timeout — commit locally only or retry
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn after_wal_write(
        &self,
        qid: &NexoraId,
        seq_no: u64,
    ) -> oneshot::Receiver<QuorumResult> {
        let _shard = (self.qid_to_shard)(qid);
        self.replicator.append_local(seq_no).await
    }

    /// Replicate entries to a specific follower.
    /// Called by a background replication task.
    pub async fn replicate_to(
        &self,
        target: &dyn crate::ReplicationTarget,
        follower_id: &str,
    ) -> Result<crate::AppendEntriesResponse, crate::ReplicationError> {
        self.replicator.replicate_to(target, follower_id).await
    }

    /// Get current commit index (highest seq_no committed by quorum).
    pub async fn commit_index(&self) -> u64 {
        self.replicator.commit_index().await
    }

    /// Get follower count.
    pub async fn follower_count(&self) -> usize {
        self.replicator.follower_count().await
    }
}

/// WAL-sourced LogEntry filler — reads actual payloads from WAL for replication.
pub struct WalLogReader {
    wal: Arc<Mutex<WriteAheadLog>>,
}

impl WalLogReader {
    pub fn new(wal: Arc<Mutex<WriteAheadLog>>) -> Self {
        Self { wal }
    }

    /// Read WAL entries for replication, filling payload fields.
    pub async fn fill_entries(
        &self,
        entries: &mut [LogEntry],
        shard_id: usize,
        qid: &NexoraId,
    ) -> Result<(), String> {
        // In a full implementation, we'd read the WAL to get actual payloads.
        // For now, this is a placeholder structure.
        let _wal = self.wal.lock().await;
        for entry in entries.iter_mut() {
            // Placeholder: encode a minimal payload
            let payload = serde_json::to_vec(&serde_json::json!({
                "seq_no": entry.seq_no,
                "shard_id": shard_id,
                "qid": qid.to_hex(),
            }))
            .map_err(|e| e.to_string())?;
            entry.payload = payload;
            entry.shard_id = shard_id;
        }
        Ok(())
    }
}

/// Node-level commit coordination. Tracks pending commits per node.
pub struct NodeCommitState {
    pub qid: NexoraId,
    /// Pending commit receivers, keyed by request_id.
    pub pending: HashMap<u64, oneshot::Receiver<QuorumResult>>,
    /// Whether this node participates in quorum replication.
    pub quorum_enabled: bool,
}

impl NodeCommitState {
    pub fn new(qid: NexoraId) -> Self {
        Self {
            qid,
            pending: HashMap::new(),
            quorum_enabled: false,
        }
    }

    /// Register a pending commit and get its receiver.
    pub fn register(&mut self, request_id: u64, rx: oneshot::Receiver<QuorumResult>) {
        self.pending.insert(request_id, rx);
    }

    /// Check if a commit has resolved.
    pub fn check_commit(&mut self, request_id: u64) -> Option<QuorumResult> {
        self.pending
            .get_mut(&request_id)
            .and_then(|rx| rx.try_recv().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_commit_gate_no_followers() {
        let config = RaftConfig {
            quorum_size: 1,
            total_nodes: 1,
            ..Default::default()
        };
        let qid_to_shard = Arc::new(|_qid: &NexoraId| 0usize);
        let gate = CommitGate::new(config, qid_to_shard);

        let qid = NexoraId::from_bytes(b"test".to_vec());
        let mut rx = gate.after_wal_write(&qid, 1).await;
        let result = rx.try_recv().unwrap();
        assert_eq!(result, QuorumResult::Committed { acked: 1, total: 1 });
    }

    #[tokio::test]
    async fn test_commit_gate_with_followers_pending() {
        let config = RaftConfig {
            quorum_size: 2,
            total_nodes: 3,
            ..Default::default()
        };
        let qid_to_shard = Arc::new(|_qid: &NexoraId| 0usize);
        let gate = CommitGate::new(config, qid_to_shard);

        // Register followers
        gate.register_follower("f-1", 0, 0).await;
        gate.register_follower("f-2", 0, 0).await;

        let qid = NexoraId::from_bytes(b"test".to_vec());
        let mut rx = gate.after_wal_write(&qid, 1).await;

        // With followers but no acks yet, commit should be pending
        assert_eq!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_node_commit_state() {
        let qid = NexoraId::from_bytes(b"test".to_vec());
        let mut state = NodeCommitState::new(qid);

        let (tx, rx) = oneshot::channel();
        state.register(42, rx);
        assert_eq!(state.pending.len(), 1);

        // Not yet resolved
        assert!(state.check_commit(42).is_none());

        // Resolve
        let _ = tx.send(QuorumResult::Committed { acked: 2, total: 3 });
        let result = state.check_commit(42);
        assert!(result.is_some());
    }
}
