//! Epoch Barrier — global consistency checkpoint for streaming graph state.
//!
//! Inspired by RisingWave's barrier scheduler. Replaces per-shard WAL
//! with a unified epoch-based checkpointing model.
//!
//! Flow:
//!   1. Meta injects Barrier { epoch: N }
//!   2. Barrier flows through all Shards
//!   3. Each Shard flushes memtable → reports completion
//!   4. All Shards complete → epoch N committed
//!   5. Global consistent snapshot at epoch N

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, RwLock};

/// An epoch identifier — monotonically increasing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(u64);

impl Epoch {
    pub fn new(n: u64) -> Self {
        Self(n)
    }
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "epoch-{}", self.0)
    }
}

/// A barrier flowing through the system.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Barrier {
    /// Monotonically increasing epoch number
    pub epoch: Epoch,
    /// When this barrier was created
    pub created_at: DateTime<Utc>,
    /// Kind of checkpoint
    pub kind: BarrierKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BarrierKind {
    /// Full checkpoint — flush all shard state
    Checkpoint,
    /// Light checkpoint — only commit offsets
    LightCheckpoint,
    /// Snapshot barrier — create a named snapshot
    Snapshot(String),
}

/// Status of a shard for a given barrier epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShardStatus {
    /// Shard has not yet received the barrier
    Pending,
    /// Shard has flushed and is ready to commit
    Flushed { node_count: usize, event_count: u64 },
    /// Shard encountered an error
    Failed(String),
}

/// Manages barrier propagation and epoch commits across all shards.
pub struct BarrierScheduler {
    /// Current epoch
    current_epoch: RwLock<Epoch>,
    /// Per-epoch completion status: epoch → (total shards, completed shards)
    epoch_states: RwLock<HashMap<Epoch, EpochState>>,
    /// Notifier when epoch commits
    commit_tx: broadcast::Sender<Epoch>,
    /// Total number of shards
    total_shards: usize,
    /// Last committed epoch
    committed_epoch: RwLock<Epoch>,
}

struct EpochState {
    total: usize,
    completed: HashSet<usize>,
    failed: HashMap<usize, String>,
}

impl BarrierScheduler {
    /// Create a new barrier scheduler.
    pub fn new(total_shards: usize) -> Self {
        let (commit_tx, _) = broadcast::channel(64);
        Self {
            current_epoch: RwLock::new(Epoch::new(1)),
            epoch_states: RwLock::new(HashMap::new()),
            commit_tx,
            total_shards,
            committed_epoch: RwLock::new(Epoch::new(0)),
        }
    }

    /// Create a new barrier and advance the epoch.
    pub async fn create_barrier(&self, kind: BarrierKind) -> Barrier {
        let mut epoch = self.current_epoch.write().await;
        let barrier = Barrier {
            epoch: *epoch,
            created_at: Utc::now(),
            kind,
        };

        self.epoch_states.write().await.insert(
            barrier.epoch,
            EpochState {
                total: self.total_shards,
                completed: HashSet::new(),
                failed: HashMap::new(),
            },
        );

        *epoch = epoch.next();
        tracing::info!(
            "Barrier epoch {} created ({:?})",
            barrier.epoch,
            barrier.kind
        );
        barrier
    }

    /// Report that a shard has completed a barrier.
    pub async fn report_shard(
        &self,
        epoch: Epoch,
        shard_id: usize,
        status: ShardStatus,
    ) -> Result<(), EpochError> {
        let mut states = self.epoch_states.write().await;
        let state = states
            .get_mut(&epoch)
            .ok_or(EpochError::UnknownEpoch(epoch))?;

        match status {
            ShardStatus::Flushed {
                node_count,
                event_count,
            } => {
                state.completed.insert(shard_id);
                tracing::debug!(
                    "Epoch {} shard {} flushed: {} nodes, {} events",
                    epoch,
                    shard_id,
                    node_count,
                    event_count
                );

                // Check if all shards have completed
                if state.completed.len() >= state.total {
                    drop(states); // Release write lock before callback
                    self.commit_epoch(epoch).await?;
                }
            }
            ShardStatus::Failed(err) => {
                state.failed.insert(shard_id, err);
                tracing::warn!("Epoch {} shard {} failed", epoch, shard_id);
            }
            ShardStatus::Pending => {}
        }

        Ok(())
    }

    /// Commit an epoch — all shards have reported completion.
    async fn commit_epoch(&self, epoch: Epoch) -> Result<(), EpochError> {
        let mut committed = self.committed_epoch.write().await;
        if epoch <= *committed {
            return Err(EpochError::AlreadyCommitted(epoch));
        }
        *committed = epoch;

        tracing::info!("Epoch {} committed — global consistency checkpoint", epoch);

        // Notify all subscribers
        let _ = self.commit_tx.send(epoch);

        Ok(())
    }

    /// Subscribe to epoch commit notifications.
    pub fn subscribe_commits(&self) -> broadcast::Receiver<Epoch> {
        self.commit_tx.subscribe()
    }

    /// Get the last committed epoch.
    pub async fn committed_epoch(&self) -> Epoch {
        *self.committed_epoch.read().await
    }

    /// Get the current active epoch.
    pub async fn current_epoch(&self) -> Epoch {
        *self.current_epoch.read().await
    }

    /// Check if a barrier has been fully acknowledged.
    pub async fn is_barrier_complete(&self, epoch: Epoch) -> bool {
        self.epoch_states
            .read()
            .await
            .get(&epoch)
            .is_some_and(|s| s.completed.len() >= s.total)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    #[error("unknown epoch: {0}")]
    UnknownEpoch(Epoch),
    #[error("epoch {0} already committed")]
    AlreadyCommitted(Epoch),
    #[error("barrier timeout: {0}")]
    Timeout(Epoch),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_barrier_basic_flow() {
        let scheduler = BarrierScheduler::new(4);

        let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;
        assert_eq!(barrier.epoch.value(), 1);
        assert_eq!(scheduler.committed_epoch().await.value(), 0);

        // Report 4 shards as flushed
        for i in 0..4 {
            scheduler
                .report_shard(
                    barrier.epoch,
                    i,
                    ShardStatus::Flushed {
                        node_count: 10,
                        event_count: 100,
                    },
                )
                .await
                .unwrap();
        }

        assert_eq!(scheduler.committed_epoch().await.value(), 1);
        assert!(scheduler.is_barrier_complete(barrier.epoch).await);
    }

    #[tokio::test]
    async fn test_epoch_monotonic() {
        let scheduler = BarrierScheduler::new(2);

        let b1 = scheduler.create_barrier(BarrierKind::Checkpoint).await;
        let b2 = scheduler.create_barrier(BarrierKind::Checkpoint).await;

        assert!(b2.epoch > b1.epoch);
        assert_eq!(b1.epoch.value(), 1);
        assert_eq!(b2.epoch.value(), 2);
    }

    #[tokio::test]
    async fn test_partial_report_no_commit() {
        let scheduler = BarrierScheduler::new(4);

        let barrier = scheduler.create_barrier(BarrierKind::Checkpoint).await;

        // Only report 2 out of 4 shards
        for i in 0..2 {
            scheduler
                .report_shard(
                    barrier.epoch,
                    i,
                    ShardStatus::Flushed {
                        node_count: 1,
                        event_count: 1,
                    },
                )
                .await
                .unwrap();
        }

        // Should NOT be committed yet
        assert!(!scheduler.is_barrier_complete(barrier.epoch).await);
        assert_eq!(scheduler.committed_epoch().await.value(), 0);
    }
}
