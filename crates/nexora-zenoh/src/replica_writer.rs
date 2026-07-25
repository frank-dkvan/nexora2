//! Replica write path — quorum-based write replication.
//!
//! Design doc §6.3, §11.2: Owner epoch fencing, replica quorum writes.
//!
//! Flow:
//! 1. Owner receives write with FencingToken
//! 2. Owner validates epoch (rejects stale writes)
//! 3. Owner sends write to all followers in parallel
//! 4. Collects acks with timeout
//! 5. If quorum reached (owner + majority of followers), write is committed
//! 6. Returns WriteStatus

use crate::replication::{FencingToken, ReplicaSet, WriteAck, WriteStatus};
use crate::{GraphOperation, GraphResult, RemoteGraphClient, RouterError};
use futures::future::BoxFuture;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A1.1: Type alias for the owner apply callback in two-phase commit — a
/// thread-safe async function that applies a GraphOperation after quorum is reached.
type OwnerApplyCallback =
    Arc<dyn Fn(GraphOperation) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

/// B2: observable replication counters. Replication is best-effort (a follower
/// being unreachable never blocks the owner write), so the only way an operator
/// sees degradation is through these metrics — otherwise a cluster silently
/// running at effective RF=1 looks healthy. All counters are process-lifetime
/// monotonic totals; a scrape computes rates/deltas.
#[derive(Debug, Default)]
pub struct ReplicationMetrics {
    /// Owner writes for which follower replication was attempted (had ≥1 follower).
    pub attempts: AtomicU64,
    /// Writes that reached the configured write concern (quorum committed).
    pub quorum_ok: AtomicU64,
    /// Writes that did NOT reach write concern — the honest degradation signal.
    pub quorum_failed: AtomicU64,
    /// Individual follower ack failures (a single write can contribute several).
    pub follower_nacks: AtomicU64,
    /// Sum of missing acks across failed writes (rough replication-lag proxy).
    pub missing_acks_total: AtomicU64,
}

impl ReplicationMetrics {
    /// Snapshot the counters as plain u64s for a metrics endpoint / log line.
    pub fn snapshot(&self) -> ReplicationMetricsSnapshot {
        ReplicationMetricsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            quorum_ok: self.quorum_ok.load(Ordering::Relaxed),
            quorum_failed: self.quorum_failed.load(Ordering::Relaxed),
            follower_nacks: self.follower_nacks.load(Ordering::Relaxed),
            missing_acks_total: self.missing_acks_total.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of [`ReplicationMetrics`], safe to serialize/log.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct ReplicationMetricsSnapshot {
    pub attempts: u64,
    pub quorum_ok: u64,
    pub quorum_failed: u64,
    pub follower_nacks: u64,
    pub missing_acks_total: u64,
}

/// Write concern: how many replicas must acknowledge a write before it's considered successful.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConcern {
    /// Write must be acknowledged by a majority of replicas (W > N/2).
    /// This guarantees durability if combined with ReadConcern::Majority (W + R > N).
    Majority,
    /// Write must be acknowledged by ALL replicas (W = N).
    /// Highest durability but lowest availability (any single replica failure blocks writes).
    All,
    /// Write must be acknowledged by the owner only (W = 1).
    /// Lowest durability but highest availability (for backward compatibility with RF=1).
    One,
}

impl WriteConcern {
    /// Calculate the minimum number of acks required for this write concern.
    pub fn min_acks(self, total_replicas: usize) -> usize {
        match self {
            WriteConcern::Majority => (total_replicas / 2) + 1,
            WriteConcern::All => total_replicas,
            WriteConcern::One => 1,
        }
    }
}

/// Replica writer — manages quorum writes for a set of replica sets.
pub struct ReplicaWriter {
    /// Remote client for sending writes to followers
    client: Arc<dyn RemoteGraphClient>,
    /// Per-shard replica sets (P0-5 FIX: using parking_lot::RwLock to avoid async lock across await)
    replica_sets: RwLock<std::collections::HashMap<usize, ReplicaSet>>,
    /// Write timeout for replica acks
    write_timeout: Duration,
    /// Optional per-shard replication log. When set, the owner assigns a
    /// monotonic seq per replicated write and stamps it into each `FencedWrite`,
    /// enabling incremental catch-up. `None` → seqs are 0 (unlogged path).
    replication_log: Option<crate::replication_log::ShardReplicationLog>,
    /// Write concern: how many replicas must acknowledge a write
    write_concern: WriteConcern,
    /// B2: observable replication counters (attempts/quorum outcomes/nacks).
    metrics: Arc<ReplicationMetrics>,
    /// C2: per-shard replication progress. When set, `quorum_write` records the
    /// owner's write seq and each follower's ack seq here, so the read path can
    /// gate `ReadConcern::Majority` on a replica having reached the quorum
    /// `commit_index`. `None` → progress is untracked (Majority degrades to
    /// owner-first failover).
    progress: Option<Arc<crate::replication_progress::ReplicationProgress>>,
    /// P1: idempotency tracker to detect and reject duplicate writes from retries
    idempotency: Option<Arc<crate::idempotency::IdempotencyTracker>>,
    /// A1.1: Owner apply callback for two-phase commit. When set, the owner write
    /// is deferred until after quorum is reached on followers.
    owner_apply: Option<OwnerApplyCallback>,
}

impl ReplicaWriter {
    pub fn new(client: Arc<dyn RemoteGraphClient>) -> Self {
        Self {
            client,
            replica_sets: RwLock::new(std::collections::HashMap::new()),
            write_timeout: Duration::from_secs(5),
            replication_log: None,
            write_concern: WriteConcern::Majority,
            metrics: Arc::new(ReplicationMetrics::default()),
            progress: None,
            idempotency: None,
            owner_apply: None,
        }
    }

    /// Construct a writer pre-seeded with replica sets (synchronous — no lock
    /// contention at startup). Used when the initial ShardMap already defines
    /// this node's owned shards and their followers.
    pub fn with_replica_sets(
        client: Arc<dyn RemoteGraphClient>,
        replica_sets: Vec<ReplicaSet>,
    ) -> Self {
        let map = replica_sets
            .into_iter()
            .map(|rs| (rs.shard_id, rs))
            .collect();
        Self {
            client,
            replica_sets: RwLock::new(map),
            write_timeout: Duration::from_secs(5),
            replication_log: None,
            write_concern: WriteConcern::Majority,
            metrics: Arc::new(ReplicationMetrics::default()),
            progress: None,
            idempotency: None,
            owner_apply: None,
        }
    }

    /// B2: shared handle to the replication counters, so the app/monitoring layer
    /// can scrape replication health (attempts, quorum failures, follower nacks).
    pub fn metrics(&self) -> Arc<ReplicationMetrics> {
        self.metrics.clone()
    }

    /// Set the write timeout for replica acks.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Attach a shared per-shard replication log so owner writes are seq-stamped
    /// (enables incremental state transfer). Chainable.
    pub fn with_replication_log(
        mut self,
        log: crate::replication_log::ShardReplicationLog,
    ) -> Self {
        self.replication_log = Some(log);
        self
    }

    /// C2: attach a shared ReplicationProgress tracker so `quorum_write` records
    /// the owner's write seq and each follower's ack seq, enabling the read path
    /// to gate `ReadConcern::Majority` on a replica having reached the quorum
    /// commit_index. Chainable.
    pub fn with_progress(
        mut self,
        progress: Arc<crate::replication_progress::ReplicationProgress>,
    ) -> Self {
        self.progress = Some(progress);
        self
    }

    /// C2: shared handle to the replication progress tracker, so the distributed
    /// read path can check which replicas have caught up to a shard's quorum seq.
    pub fn progress(&self) -> Option<Arc<crate::replication_progress::ReplicationProgress>> {
        self.progress.clone()
    }

    /// P1: attach an idempotency tracker to detect and reject duplicate writes
    /// from client or internal retries. Chainable.
    pub fn with_idempotency(
        mut self,
        tracker: Arc<crate::idempotency::IdempotencyTracker>,
    ) -> Self {
        self.idempotency = Some(tracker);
        self
    }

    /// Set the write concern for this writer. Chainable.
    pub fn with_write_concern(mut self, concern: WriteConcern) -> Self {
        self.write_concern = concern;
        self
    }

    /// A1.1: Attach an owner apply callback for two-phase commit. When set,
    /// `quorum_write_two_phase` will call this after quorum is reached. Chainable.
    pub fn with_owner_apply<F>(mut self, f: F) -> Self
    where
        F: Fn(GraphOperation) -> BoxFuture<'static, Result<(), String>> + Send + Sync + 'static,
    {
        self.owner_apply = Some(Arc::new(f));
        self
    }

    /// Register a replica set for a shard.
    pub fn register_replica_set(&self, replica_set: ReplicaSet) {
        self.replica_sets
            .write()
            .insert(replica_set.shard_id, replica_set);
    }

    /// Execute a quorum write: send to owner + all followers, collect acks.
    ///
    /// The owner write is assumed to have already succeeded (the caller
    /// executed it locally). This function handles the follower replication.
    ///
    /// Returns:
    /// - `WriteStatus::CommittedLocal` if no replicas configured
    /// - `WriteStatus::CommittedQuorum` if enough acks received per write concern
    /// - `WriteStatus::Failed` if quorum not reached
    /// - `WriteStatus::AlreadyCommitted` if this is a duplicate request (P1 idempotency)
    pub async fn quorum_write(
        &self,
        shard_id: usize,
        token: &FencingToken,
        op: GraphOperation,
    ) -> Result<WriteStatus, RouterError> {
        // P1: Check for duplicate request_id (idempotency)
        if let (Some(tracker), Some(request_id)) = (&self.idempotency, &token.request_id) {
            if let Some(cached_result) = tracker.check_duplicate(request_id).await {
                tracing::debug!(
                    request_id = %request_id,
                    shard = shard_id,
                    ?cached_result,
                    "duplicate write detected, returning cached result"
                );
                return Ok(WriteStatus::AlreadyCommitted);
            }
        }

        // P0-5 FIX: Use non-async RwLock to avoid holding lock across await
        let replica_set = {
            let replica_sets = self.replica_sets.read();
            match replica_sets.get(&shard_id) {
                Some(rs) => rs.clone(),
                None => {
                    // No replica set configured — local write is sufficient
                    return Ok(WriteStatus::CommittedLocal);
                }
            }
        }; // Lock released here automatically

        if replica_set.followers.is_empty() {
            // No followers — owner write alone is sufficient
            return Ok(WriteStatus::CommittedLocal);
        }

        // Calculate required acks based on write concern
        let total = replica_set.total_nodes();
        let required_acks = self.write_concern.min_acks(total);

        // Clone epoch for use in spawned tasks (avoids lifetime issues)
        let epoch = token.epoch;

        // Assign an owner replication seq for this write (0 if no log attached).
        // Recording it on the owner keeps the owner able to serve incremental
        // deltas to a lagging follower.
        let seq = match &self.replication_log {
            Some(log) => log.record_owner(shard_id, op.clone()).await,
            None => 0,
        };

        // C2: record the owner's write seq in the replication progress tracker so
        // the read path knows the owner's high-water mark. Later, when a quorum of
        // replicas ack, this seq becomes the shard's commit_index.
        if let Some(prog) = &self.progress {
            if seq > 0 {
                prog.record_write(shard_id, seq).await;
            }
        }

        // Send write to all followers in parallel, each wrapped in a FencedWrite
        // stamped with the owner's current epoch + replication seq. The follower
        // admits it only if the epoch is at least the highest it has seen for
        // this shard; a deposed owner replicating with a stale epoch is rejected
        // receiver-side. (The old self-comparison `token.allows_write(token.epoch)`
        // was a no-op — it never had the follower's high-water mark to compare.)
        let mut handles = Vec::new();
        for follower in &replica_set.followers {
            let client = self.client.clone();
            let follower = follower.clone();
            let fenced = GraphOperation::FencedWrite {
                shard_id,
                epoch,
                seq,
                inner: Box::new(op.clone()),
            };
            handles.push(tokio::spawn(async move {
                let result = client.execute(&follower, fenced).await;
                WriteAck {
                    node_id: follower,
                    shard_id,
                    epoch,
                    success: result.is_ok(),
                }
            }));
        }

        // Collect acks with timeout
        let mut acks = 0usize;

        // Owner already acknowledged (local write succeeded)
        acks += 1;

        let timeout_result = tokio::time::timeout(self.write_timeout, async {
            for handle in handles {
                if let Ok(ack) = handle.await {
                    if ack.success {
                        acks += 1;
                        // C2: record this follower's successful ack at the write seq,
                        // so the read path knows it has caught up to (at least) seq.
                        if let Some(prog) = &self.progress {
                            if seq > 0 {
                                prog.record_ack(shard_id, &ack.node_id, seq).await;
                            }
                        }
                        if acks >= required_acks {
                            return acks;
                        }
                    }
                }
            }
            acks
        })
        .await;

        let final_acks = timeout_result.unwrap_or(acks);

        // B2: record the outcome so best-effort degradation is observable. This
        // path only runs when there is ≥1 follower (early-returned above
        // otherwise), so every increment is a genuine replication attempt.
        self.metrics.attempts.fetch_add(1, Ordering::Relaxed);

        let result = if final_acks >= required_acks {
            self.metrics.quorum_ok.fetch_add(1, Ordering::Relaxed);
            Ok(WriteStatus::CommittedQuorum {
                acked: final_acks,
                total,
            })
        } else {
            let missing = (required_acks - final_acks) as u64;
            self.metrics.quorum_failed.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .missing_acks_total
                .fetch_add(missing, Ordering::Relaxed);
            // follower_nacks: acks we expected from followers but didn't get.
            // required_acks includes the owner's self-ack, so followers expected
            // = required_acks - 1; nacks = that minus follower acks received.
            let follower_acks = final_acks.saturating_sub(1);
            let followers_expected = required_acks.saturating_sub(1);
            self.metrics.follower_nacks.fetch_add(
                followers_expected.saturating_sub(follower_acks) as u64,
                Ordering::Relaxed,
            );
            tracing::warn!(
                shard = shard_id,
                acked = final_acks,
                required = required_acks,
                "quorum write failed"
            );
            Err(RouterError::QuorumFailed {
                acked: final_acks,
                required: required_acks,
            })
        };

        // P1: Record write result in idempotency cache if request_id present
        if let (Some(tracker), Some(request_id)) = (&self.idempotency, &token.request_id) {
            match &result {
                Ok(WriteStatus::CommittedQuorum { .. }) | Ok(WriteStatus::CommittedLocal) => {
                    tracker.record_success(request_id.clone(), seq).await;
                }
                Err(e) => {
                    tracker
                        .record_failure(request_id.clone(), e.to_string())
                        .await;
                }
                _ => {}
            }
        }

        result
    }

    /// Execute a two-phase commit quorum write with rollback support.
    ///
    /// This version first replicates to followers (phase 1), and only applies
    /// to owner if quorum is reached (phase 2). If quorum fails, no data is
    /// written to owner, maintaining consistency.
    ///
    /// Returns:
    /// - `WriteStatus::CommittedLocal` if no replicas configured
    /// - `WriteStatus::CommittedQuorum` if quorum reached and owner applied
    /// - `WriteStatus::Failed` if quorum not reached (no data written anywhere)
    pub async fn quorum_write_two_phase(
        &self,
        shard_id: usize,
        token: &FencingToken,
        op: GraphOperation,
    ) -> Result<WriteStatus, RouterError> {
        // P0-5 FIX: Use non-async RwLock
        let replica_set = {
            let replica_sets = self.replica_sets.read();
            match replica_sets.get(&shard_id) {
                Some(rs) => rs.clone(),
                None => {
                    // No replica set configured — execute owner write directly
                    return Ok(WriteStatus::CommittedLocal);
                }
            }
        }; // Lock released here

        if replica_set.followers.is_empty() {
            // No followers — owner write alone is sufficient
            return Ok(WriteStatus::CommittedLocal);
        }

        let epoch = token.epoch;

        // Phase 1: Replicate to followers FIRST (before owner write)
        let seq = match &self.replication_log {
            Some(log) => log.record_owner(shard_id, op.clone()).await,
            None => 0,
        };

        let mut handles = Vec::new();
        for follower in &replica_set.followers {
            let client = self.client.clone();
            let follower = follower.clone();
            let fenced = GraphOperation::FencedWrite {
                shard_id,
                epoch,
                seq,
                inner: Box::new(op.clone()),
            };
            handles.push(tokio::spawn(async move {
                let result = client.execute(&follower, fenced).await;
                WriteAck {
                    node_id: follower,
                    shard_id,
                    epoch,
                    success: result.is_ok(),
                }
            }));
        }

        // Collect follower acks
        let mut follower_acks = 0usize;
        let total = replica_set.total_nodes();

        let timeout_result = tokio::time::timeout(self.write_timeout, async {
            for handle in handles {
                if let Ok(ack) = handle.await {
                    if ack.success {
                        follower_acks += 1;
                    }
                }
            }
            follower_acks
        })
        .await;

        let final_follower_acks = timeout_result.unwrap_or(follower_acks);

        // Check if we would reach quorum with owner
        let potential_acks = final_follower_acks + 1; // +1 for owner

        if !replica_set.quorum_reached(potential_acks) {
            // Quorum cannot be reached — abort without writing to owner
            return Ok(WriteStatus::Failed {
                acked: final_follower_acks,
                required: replica_set.min_ack,
            });
        }

        // Phase 2: Quorum reached on followers, now apply to owner
        if let Some(apply_fn) = &self.owner_apply {
            match apply_fn(op.clone()).await {
                Ok(()) => {
                    // Owner write succeeded
                    Ok(WriteStatus::CommittedQuorum {
                        acked: potential_acks,
                        total,
                    })
                }
                Err(e) => {
                    // Owner write failed after quorum — this is a critical error
                    tracing::error!(
                        shard = shard_id,
                        error = %e,
                        "owner apply failed after quorum reached"
                    );
                    Err(RouterError::Remote(format!("owner apply failed: {}", e)))
                }
            }
        } else {
            // No owner_apply callback — return success assuming caller will apply
            Ok(WriteStatus::CommittedQuorum {
                acked: potential_acks,
                total,
            })
        }
    }

    /// Execute a read from the replica set (can read from any replica).
    pub async fn replica_read(
        &self,
        _shard_id: usize,
        target_node: &str,
        op: GraphOperation,
    ) -> Result<GraphResult, RouterError> {
        // For reads, just use the owner (could be extended to read from any replica)
        self.client.execute(target_node, op).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_client::LocalGraphClient;
    use crate::OwnerEpoch;

    #[tokio::test]
    async fn test_quorum_write_no_replicas() {
        let client = Arc::new(LocalGraphClient::new());
        let writer = ReplicaWriter::new(client);

        let token = FencingToken::new(0, OwnerEpoch::new());
        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let status = writer.quorum_write(0, &token, op).await.unwrap();
        assert_eq!(status, WriteStatus::CommittedLocal);
    }

    #[tokio::test]
    async fn test_quorum_write_with_replicas_success() {
        let client = Arc::new(LocalGraphClient::new());
        let writer = ReplicaWriter::new(client.clone());

        // Register a 3-node replica set
        let replica_set = ReplicaSet::new(
            0,
            "owner".into(),
            vec!["follower-1".into(), "follower-2".into()],
        );
        writer.register_replica_set(replica_set);

        // Pre-register follower nodes in the client
        // LocalGraphClient ignores target_node, so any node works
        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write(0, &token, op).await.unwrap();

        // Should achieve quorum (owner + at least 1 follower = 2 out of 3)
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert!(acked >= 2);
            }
            other => panic!("expected CommittedQuorum, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_quorum_write_timeout() {
        // Create a client that will timeout (no server listening)
        struct TimeoutClient;

        impl RemoteGraphClient for TimeoutClient {
            fn execute<'a>(
                &'a self,
                _target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                Box::pin(async {
                    // Simulate a long delay that will timeout
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Err(RouterError::Timeout)
                })
            }
        }

        let client = Arc::new(TimeoutClient);
        let writer = ReplicaWriter::new(client).with_timeout(Duration::from_millis(100));

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"t".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(1),
        };
        let token = FencingToken::new(0, OwnerEpoch::new());

        let result = writer.quorum_write(0, &token, op).await;

        // Only owner acked (1), followers timed out
        assert!(result.is_err());
        match result {
            Err(RouterError::QuorumFailed { acked, required }) => {
                assert_eq!(acked, 1); // only owner
                assert_eq!(required, 2); // majority of 3
            }
            other => panic!("expected QuorumFailed error, got {other:?}"),
        }
    }

    /// B2: a best-effort replication failure (unreachable followers) is
    /// observable through the metrics — attempts +1, quorum_failed +1, and the
    /// missing-ack / follower-nack counters reflect the shortfall. The write
    /// itself does NOT error (best-effort): the owner already committed.
    #[tokio::test]
    async fn test_replication_metrics_track_best_effort_failure() {
        struct TimeoutClient;
        impl RemoteGraphClient for TimeoutClient {
            fn execute<'a>(
                &'a self,
                _target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Err(RouterError::Timeout)
                })
            }
        }

        let writer =
            ReplicaWriter::new(Arc::new(TimeoutClient)).with_timeout(Duration::from_millis(50));
        let metrics = writer.metrics();
        writer.register_replica_set(ReplicaSet::new(
            0,
            "owner".into(),
            vec!["f1".into(), "f2".into()],
        ));

        let before = metrics.snapshot();
        assert_eq!(before.attempts, 0);

        let op = GraphOperation::SetProperty {
            qid: nexora_id::NexoraId::from_bytes(b"m".to_vec()),
            key: "k".into(),
            value: serde_json::json!(1),
        };
        // Best-effort: the call now returns Err(QuorumFailed) instead of Ok(Failed)
        let result = writer
            .quorum_write(0, &FencingToken::new(0, OwnerEpoch::new()), op)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(RouterError::QuorumFailed { .. })));

        let after = metrics.snapshot();
        assert_eq!(after.attempts, 1, "one replication attempt recorded");
        assert_eq!(after.quorum_ok, 0);
        assert_eq!(after.quorum_failed, 1, "quorum failure recorded");
        // Majority of 3 = 2 required; owner acked 1 → 1 missing ack.
        assert_eq!(after.missing_acks_total, 1);
        // 1 follower ack expected (required 2 - owner 1), 0 received → 1 nack.
        assert_eq!(after.follower_nacks, 1);
    }

    /// B2: a fully-acked write increments only attempts + quorum_ok, leaving the
    /// failure counters at zero.
    #[tokio::test]
    async fn test_replication_metrics_track_success() {
        let writer = ReplicaWriter::new(Arc::new(LocalGraphClient::new()));
        let metrics = writer.metrics();
        writer.register_replica_set(ReplicaSet::new(
            0,
            "owner".into(),
            vec!["f1".into(), "f2".into()],
        ));

        let op = GraphOperation::SetProperty {
            qid: nexora_id::NexoraId::from_bytes(b"ok".to_vec()),
            key: "k".into(),
            value: serde_json::json!(1),
        };
        writer
            .quorum_write(0, &FencingToken::new(0, OwnerEpoch::new()), op)
            .await
            .unwrap();

        let s = metrics.snapshot();
        assert_eq!(s.attempts, 1);
        assert_eq!(s.quorum_ok, 1);
        assert_eq!(s.quorum_failed, 0);
        assert_eq!(s.follower_nacks, 0);
        assert_eq!(s.missing_acks_total, 0);
    }

    #[tokio::test]
    async fn test_replica_read() {
        let client = Arc::new(LocalGraphClient::new());

        // Pre-populate data
        let qid = nexora_id::NexoraId::from_bytes(b"n1".to_vec());
        client
            .set_property(&qid, "name", serde_json::json!("Alice"))
            .await;

        let writer = ReplicaWriter::new(client);

        let result = writer
            .replica_read(
                0,
                "any",
                GraphOperation::GetProperty {
                    qid,
                    key: "name".into(),
                },
            )
            .await
            .unwrap();

        match result {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("Alice")),
            other => panic!("expected Property(Some(\"Alice\")), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_two_phase_commit_quorum_success() {
        let client = Arc::new(LocalGraphClient::new());
        let writer = ReplicaWriter::new(client.clone());

        // Register a 3-node replica set
        let replica_set = ReplicaSet::new(
            0,
            "owner".into(),
            vec!["follower-1".into(), "follower-2".into()],
        );
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write_two_phase(0, &token, op).await.unwrap();

        // Should achieve quorum (owner + at least 1 follower = 2 out of 3)
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert!(acked >= 2);
            }
            other => panic!("expected CommittedQuorum, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_two_phase_commit_quorum_failure_no_owner_write() {
        // Create a client that will fail for all followers
        struct FailingClient;

        impl RemoteGraphClient for FailingClient {
            fn execute<'a>(
                &'a self,
                _target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                Box::pin(async { Err(RouterError::Timeout) })
            }
        }

        let client = Arc::new(FailingClient);
        let writer = ReplicaWriter::new(client).with_timeout(Duration::from_millis(100));

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"t".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(1),
        };
        let token = FencingToken::new(0, OwnerEpoch::new());

        let status = writer.quorum_write_two_phase(0, &token, op).await.unwrap();

        // No followers acked, so quorum cannot be reached — owner should not write
        match status {
            WriteStatus::Failed { acked, required } => {
                assert_eq!(acked, 0); // no followers acked
                assert_eq!(required, 2); // majority of 3
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_write_concern_majority() {
        let client = Arc::new(LocalGraphClient::new());
        let writer = ReplicaWriter::new(client.clone()).with_write_concern(WriteConcern::Majority);

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write(0, &token, op).await.unwrap();

        // With Majority concern, 2 out of 3 should be sufficient
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert!(acked >= 2);
            }
            other => panic!("expected CommittedQuorum, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_write_concern_all() {
        let client = Arc::new(LocalGraphClient::new());
        let writer = ReplicaWriter::new(client.clone()).with_write_concern(WriteConcern::All);

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write(0, &token, op).await.unwrap();

        // With All concern, all 3 replicas must ack
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert_eq!(acked, 3);
            }
            other => panic!("expected CommittedQuorum with all acks, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_write_concern_one() {
        let client = Arc::new(LocalGraphClient::new());
        let writer = ReplicaWriter::new(client.clone()).with_write_concern(WriteConcern::One);

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write(0, &token, op).await.unwrap();

        // With One concern, owner ack alone is sufficient
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert!(acked >= 1);
            }
            other => panic!("expected CommittedQuorum, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_write_concern_majority_fails_if_majority_unavailable() {
        // Create a client where only owner succeeds
        struct PartiallyFailingClient {
            success_nodes: std::collections::HashSet<String>,
        }

        impl RemoteGraphClient for PartiallyFailingClient {
            fn execute<'a>(
                &'a self,
                target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                let success = self.success_nodes.contains(target);
                Box::pin(async move {
                    if success {
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "ok".to_string(),
                        })
                    } else {
                        Err(RouterError::Timeout)
                    }
                })
            }
        }

        let mut success_nodes = std::collections::HashSet::new();
        success_nodes.insert("owner".to_string());
        // Only owner succeeds, both followers fail
        let client = Arc::new(PartiallyFailingClient { success_nodes });
        let writer = ReplicaWriter::new(client)
            .with_write_concern(WriteConcern::Majority)
            .with_timeout(Duration::from_millis(100));

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let result = writer.quorum_write(0, &token, op).await;

        // Only owner acked (1/3), majority requires 2, so should fail
        assert!(result.is_err());
        match result {
            Err(RouterError::QuorumFailed { acked, required }) => {
                assert_eq!(acked, 1); // only owner
                assert_eq!(required, 2); // majority of 3
            }
            other => panic!("expected QuorumFailed error, got {other:?}"),
        }
    }

    /// A1.1: two-phase commit applies to owner after quorum is reached
    #[tokio::test]
    async fn test_two_phase_applies_to_owner_after_quorum() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let client = Arc::new(LocalGraphClient::new());
        let owner_applied = Arc::new(AtomicBool::new(false));
        let owner_applied_clone = owner_applied.clone();

        let writer = ReplicaWriter::new(client.clone()).with_owner_apply(move |_op| {
            let flag = owner_applied_clone.clone();
            Box::pin(async move {
                flag.store(true, Ordering::SeqCst);
                Ok(())
            })
        });

        // Register a 3-node replica set
        let replica_set = ReplicaSet::new(
            0,
            "owner".into(),
            vec!["follower-1".into(), "follower-2".into()],
        );
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write_two_phase(0, &token, op).await.unwrap();

        // Should achieve quorum and apply to owner
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert!(acked >= 2);
            }
            other => panic!("expected CommittedQuorum, got {other:?}"),
        }

        // Verify owner_apply was called
        assert!(
            owner_applied.load(Ordering::SeqCst),
            "owner_apply should have been called"
        );
    }

    /// A1.1: two-phase commit aborts if quorum not reached (owner not applied)
    #[tokio::test]
    async fn test_two_phase_aborts_if_quorum_not_reached() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // Create a client that will fail for all followers
        struct FailingClient;
        impl RemoteGraphClient for FailingClient {
            fn execute<'a>(
                &'a self,
                _target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                Box::pin(async { Err(RouterError::Timeout) })
            }
        }

        let client = Arc::new(FailingClient);
        let owner_applied = Arc::new(AtomicBool::new(false));
        let owner_applied_clone = owner_applied.clone();

        let writer = ReplicaWriter::new(client)
            .with_timeout(Duration::from_millis(100))
            .with_owner_apply(move |_op| {
                let flag = owner_applied_clone.clone();
                Box::pin(async move {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                })
            });

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"t".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(1),
        };
        let token = FencingToken::new(0, OwnerEpoch::new());

        let status = writer.quorum_write_two_phase(0, &token, op).await.unwrap();

        // No followers acked, so quorum cannot be reached
        match status {
            WriteStatus::Failed { acked, required } => {
                assert_eq!(acked, 0); // no followers acked
                assert_eq!(required, 2); // majority of 3
            }
            other => panic!("expected Failed, got {other:?}"),
        }

        // Verify owner_apply was NOT called
        assert!(
            !owner_applied.load(Ordering::SeqCst),
            "owner_apply should NOT have been called"
        );
    }

    #[tokio::test]
    async fn test_write_concern_majority_succeeds_with_one_follower() {
        // Create a client where owner + 1 follower succeeds
        struct PartiallyFailingClient {
            success_nodes: std::collections::HashSet<String>,
        }

        impl RemoteGraphClient for PartiallyFailingClient {
            fn execute<'a>(
                &'a self,
                target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                let success = self.success_nodes.contains(target);
                Box::pin(async move {
                    if success {
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "ok".to_string(),
                        })
                    } else {
                        Err(RouterError::Timeout)
                    }
                })
            }
        }

        let mut success_nodes = std::collections::HashSet::new();
        success_nodes.insert("owner".to_string());
        success_nodes.insert("f1".to_string());
        // Owner + f1 succeed, f2 fails
        let client = Arc::new(PartiallyFailingClient { success_nodes });
        let writer = ReplicaWriter::new(client)
            .with_write_concern(WriteConcern::Majority)
            .with_timeout(Duration::from_millis(100));

        let replica_set = ReplicaSet::new(0, "owner".into(), vec!["f1".into(), "f2".into()]);
        writer.register_replica_set(replica_set);

        let qid = nexora_id::NexoraId::from_bytes(b"test".to_vec());
        let op = GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(42),
        };

        let token = FencingToken::new(0, OwnerEpoch::new());
        let status = writer.quorum_write(0, &token, op).await.unwrap();

        // Owner + 1 follower = 2/3, majority is satisfied
        match status {
            WriteStatus::CommittedQuorum { acked, total } => {
                assert_eq!(total, 3);
                assert_eq!(acked, 2);
            }
            other => panic!("expected CommittedQuorum, got {other:?}"),
        }
    }
}
