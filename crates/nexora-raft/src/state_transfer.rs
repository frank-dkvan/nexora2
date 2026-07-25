//! State Transfer — failover data recovery.
//!
//! When a shard owner fails and a new owner is elected (via ControlPlane
//! failover), the new owner must pull WAL entries and the latest snapshot
//! from surviving replicas before it can serve reads/writes.
//!
//! ## Protocol
//!
//! 1. New owner receives `ShardMap` update with it marked as owner.
//! 2. New owner queries replicas for their latest snapshot + WAL seq_no.
//! 3. New owner picks the replica with the highest seq_no.
//! 4. New owner fetches snapshot + WAL entries since snapshot.
//! 5. New owner replays WAL and takes over serving.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::RwLock;

/// Snapshot metadata for state transfer negotiation.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    /// The WAL seq_no at which this snapshot was taken.
    pub seq_no: u64,
    /// Timestamp when the snapshot was created (micros).
    pub timestamp: u64,
    /// Size in bytes of the snapshot payload.
    pub size_bytes: u64,
    /// Optional checksum for integrity verification.
    pub checksum: Option<String>,
}

/// A request to transfer state from a replica.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StateTransferRequest {
    /// Shard being transferred.
    pub shard_id: usize,
    /// Starting seq_no (entries >= this are requested).
    pub from_seq: u64,
    /// Maximum number of WAL entries to include.
    pub max_entries: usize,
}

/// A state transfer response from a replica.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StateTransferResponse {
    /// Latest snapshot metadata.
    pub snapshot: Option<SnapshotMeta>,
    /// Serialized WAL entries (if any).
    pub wal_entries: Vec<Vec<u8>>,
    /// Last seq_no included in this response.
    pub last_seq_no: u64,
    /// Whether there are more entries available.
    pub has_more: bool,
}

/// Result of a completed state transfer.
#[derive(Clone, Debug)]
pub struct StateTransferResult {
    pub shard_id: usize,
    pub snapshot_applied: bool,
    pub wal_entries_replayed: usize,
    pub recovered_to_seq_no: u64,
    pub duration: Duration,
    pub source_node: String,
}

/// State transfer manager — orchestrates failover data recovery.
pub struct StateTransferManager {
    /// Per-shard transfer state.
    transfers: RwLock<HashMap<usize, TransferState>>,
    /// Timeout for state transfer operations.
    pub rpc_timeout: Duration,
    /// Maximum WAL entries per fetch batch.
    pub max_batch_size: usize,
    /// Retry count for failed fetches.
    pub max_retries: u32,
}

#[derive(Clone, Debug)]
pub struct TransferState {
    start_seq: u64,
    target_seq: u64,
    transferred: u64,
    status: TransferStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    FetchingWal,
}

impl StateTransferManager {
    pub fn new() -> Self {
        Self {
            transfers: RwLock::new(HashMap::new()),
            rpc_timeout: Duration::from_secs(30),
            max_batch_size: 1000,
            max_retries: 3,
        }
    }

    /// Start a state transfer from a replica.
    pub async fn start_transfer(&self, shard_id: usize, source_node: &str) {
        let mut transfers = self.transfers.write().await;
        transfers.insert(
            shard_id,
            TransferState {
                start_seq: 0,
                target_seq: 0,
                transferred: 0,
                status: TransferStatus::Pending,
            },
        );
        tracing::info!(
            shard_id = shard_id,
            source = source_node,
            "Starting state transfer"
        );
    }

    /// Report transfer progress.
    pub async fn report_progress(&self, shard_id: usize, seq: u64, status: TransferStatus) {
        let mut transfers = self.transfers.write().await;
        if let Some(state) = transfers.get_mut(&shard_id) {
            state.target_seq = seq;
            state.transferred = seq.saturating_sub(state.start_seq);
            state.status = status;
        }
    }

    /// Mark a transfer as complete.
    pub async fn complete_transfer(&self, shard_id: usize, result: StateTransferResult) {
        let mut transfers = self.transfers.write().await;
        transfers.remove(&shard_id);
        tracing::info!(
            shard_id = shard_id,
            recovered_to = result.recovered_to_seq_no,
            duration_ms = result.duration.as_millis(),
            "State transfer complete"
        );
    }

    /// Check if a transfer is in progress for a shard.
    pub async fn is_transferring(&self, shard_id: usize) -> bool {
        let transfers = self.transfers.read().await;
        transfers.contains_key(&shard_id)
    }

    /// Get current transfer state.
    pub async fn get_transfer(&self, shard_id: usize) -> Option<TransferState> {
        let transfers = self.transfers.read().await;
        transfers.get(&shard_id).cloned()
    }
}

impl Default for StateTransferManager {
    fn default() -> Self {
        Self::new()
    }
}

/// async trait for fetching state from remote replicas.
#[async_trait::async_trait]
pub trait StateTransferClient: Send + Sync {
    /// Ask a node for snapshot metadata.
    async fn get_snapshot_meta(
        &self,
        node: &str,
        shard_id: usize,
    ) -> Result<Option<SnapshotMeta>, String>;

    /// Fetch state from a node.
    async fn fetch_state(
        &self,
        node: &str,
        request: StateTransferRequest,
    ) -> Result<StateTransferResponse, String>;

    /// Apply a received state transfer to local state.
    async fn apply_transfer(
        &self,
        node: &str,
        shard_id: usize,
        response: StateTransferResponse,
    ) -> Result<u64, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transfer_lifecycle() {
        let manager = StateTransferManager::new();

        // Start transfer
        manager.start_transfer(0, "replica-1").await;
        assert!(manager.is_transferring(0).await);

        // Report progress
        manager
            .report_progress(0, 100, TransferStatus::FetchingWal)
            .await;

        let state = manager.get_transfer(0).await.unwrap();
        assert_eq!(state.transferred, 100);

        // Complete
        let result = StateTransferResult {
            shard_id: 0,
            snapshot_applied: true,
            wal_entries_replayed: 50,
            recovered_to_seq_no: 100,
            duration: Duration::from_millis(200),
            source_node: "replica-1".into(),
        };
        manager.complete_transfer(0, result).await;
        assert!(!manager.is_transferring(0).await);
    }

    #[tokio::test]
    async fn test_multiple_transfers() {
        let manager = StateTransferManager::new();

        manager.start_transfer(0, "r1").await;
        manager.start_transfer(1, "r2").await;
        assert!(manager.is_transferring(0).await);
        assert!(manager.is_transferring(1).await);

        // Complete shard 0
        manager
            .complete_transfer(
                0,
                StateTransferResult {
                    shard_id: 0,
                    snapshot_applied: false,
                    wal_entries_replayed: 10,
                    recovered_to_seq_no: 10,
                    duration: Duration::from_millis(50),
                    source_node: "r1".into(),
                },
            )
            .await;

        assert!(!manager.is_transferring(0).await);
        assert!(manager.is_transferring(1).await);
    }

    #[tokio::test]
    async fn test_get_nonexistent_transfer() {
        let manager = StateTransferManager::new();
        assert!(manager.get_transfer(99).await.is_none());
        assert!(!manager.is_transferring(99).await);
    }

    #[tokio::test]
    async fn test_report_progress_nonexistent() {
        let manager = StateTransferManager::new();
        // Reporting progress for a non-existent transfer should not panic
        manager
            .report_progress(99, 100, TransferStatus::FetchingWal)
            .await;
    }

    #[tokio::test]
    async fn test_complete_nonexistent_transfer() {
        let manager = StateTransferManager::new();
        // Completing a non-existent transfer should not panic
        manager
            .complete_transfer(
                99,
                StateTransferResult {
                    shard_id: 99,
                    snapshot_applied: false,
                    wal_entries_replayed: 0,
                    recovered_to_seq_no: 0,
                    duration: Duration::from_millis(0),
                    source_node: "ghost".into(),
                },
            )
            .await;
    }

    #[tokio::test]
    async fn test_transfer_progress_tracking() {
        let manager = StateTransferManager::new();

        manager.start_transfer(0, "r1").await;
        let state = manager.get_transfer(0).await.unwrap();
        assert_eq!(state.status, TransferStatus::Pending);
        assert_eq!(state.transferred, 0);

        manager
            .report_progress(0, 50, TransferStatus::FetchingWal)
            .await;
        let state = manager.get_transfer(0).await.unwrap();
        assert_eq!(state.status, TransferStatus::FetchingWal);
        assert_eq!(state.transferred, 50);
        assert_eq!(state.target_seq, 50);
    }

    #[tokio::test]
    async fn test_default_implementation() {
        let manager1 = StateTransferManager::new();
        let manager2 = StateTransferManager::default();
        // Both should have the same configuration
        assert_eq!(manager1.rpc_timeout, manager2.rpc_timeout);
        assert_eq!(manager1.max_batch_size, manager2.max_batch_size);
        assert_eq!(manager1.max_retries, manager2.max_retries);
    }

    #[test]
    fn test_snapshot_meta_serde() {
        let meta = SnapshotMeta {
            seq_no: 100,
            timestamp: 1234567890,
            size_bytes: 4096,
            checksum: Some("abc123".to_string()),
        };
        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: SnapshotMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.seq_no, 100);
        assert_eq!(deserialized.checksum, Some("abc123".to_string()));
    }

    #[test]
    fn test_snapshot_meta_no_checksum() {
        let meta = SnapshotMeta {
            seq_no: 0,
            timestamp: 0,
            size_bytes: 0,
            checksum: None,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: SnapshotMeta = serde_json::from_str(&json).unwrap();
        assert!(deserialized.checksum.is_none());
    }

    #[test]
    fn test_state_transfer_request_serde() {
        let req = StateTransferRequest {
            shard_id: 3,
            from_seq: 100,
            max_entries: 500,
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: StateTransferRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.shard_id, 3);
        assert_eq!(deserialized.from_seq, 100);
        assert_eq!(deserialized.max_entries, 500);
    }

    #[test]
    fn test_state_transfer_response_serde() {
        let resp = StateTransferResponse {
            snapshot: Some(SnapshotMeta {
                seq_no: 50,
                timestamp: 1000,
                size_bytes: 2048,
                checksum: None,
            }),
            wal_entries: vec![vec![1, 2, 3], vec![4, 5, 6]],
            last_seq_no: 100,
            has_more: false,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: StateTransferResponse = serde_json::from_str(&json).unwrap();
        assert!(deserialized.snapshot.is_some());
        assert_eq!(deserialized.wal_entries.len(), 2);
        assert_eq!(deserialized.last_seq_no, 100);
        assert!(!deserialized.has_more);
    }

    #[test]
    fn test_state_transfer_response_empty() {
        let resp = StateTransferResponse {
            snapshot: None,
            wal_entries: vec![],
            last_seq_no: 0,
            has_more: false,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: StateTransferResponse = serde_json::from_str(&json).unwrap();
        assert!(deserialized.snapshot.is_none());
        assert!(deserialized.wal_entries.is_empty());
    }

    #[test]
    fn test_transfer_status_equality() {
        assert_eq!(TransferStatus::Pending, TransferStatus::Pending);
        assert_eq!(TransferStatus::FetchingWal, TransferStatus::FetchingWal);
        assert_ne!(TransferStatus::Pending, TransferStatus::FetchingWal);
    }

    #[test]
    fn test_state_transfer_result_debug() {
        let result = StateTransferResult {
            shard_id: 0,
            snapshot_applied: true,
            wal_entries_replayed: 42,
            recovered_to_seq_no: 100,
            duration: Duration::from_millis(500),
            source_node: "node-1".into(),
        };
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("shard_id: 0"));
        assert!(debug_str.contains("snapshot_applied: true"));
        assert!(debug_str.contains("wal_entries_replayed: 42"));
    }
}
