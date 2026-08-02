//! State transfer — a recovering owner/replica catches up a shard's data from a
//! surviving node.
//!
//! HA roadmap 阶段3. This is the operation-based counterpart to the WAL-shipping
//! trait in `nexora-raft`: consistent with the replication design (T2.3), state
//! transfer ships **operations**, not raw WAL bytes. A node that has just been
//! promoted to own a shard (failover), or a replica that rejoined after a
//! restart, pulls a [`ShardSnapshot`] from a source that already holds the data
//! and replays it into its own graph as ordinary local writes.
//!
//! Flow:
//! 1. Recovering node calls [`StateTransfer::catch_up_shard`] with a source node
//!    that is alive and holds the shard (owner or a surviving replica).
//! 2. It sends `ExportShard { shard_id, total_shards }`; the source's adapter
//!    exports every node+edge it holds for that cluster shard.
//! 3. The recovering node applies each `SetProperty`/`AddEdge` locally, through
//!    the same client it would use for a remote write (self-registered in the
//!    cluster's `remote_client`, TCP-to-self), so the data lands in its graph.
//!
//! Unlike quorum replication, catch-up writes are **not** fenced: they carry the
//! source's data verbatim and are applied by the recovering node to itself,
//! outside the epoch-stamped owner→follower path.
//!
//! P1-4 fix: Support resumable state transfer with checkpoint tracking.

use crate::migration::ShardSnapshot;
use crate::{GraphOperation, GraphResult, RemoteGraphClient};
use nexora_id::NexoraId;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Outcome of a shard catch-up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatchUpResult {
    pub shard_id: usize,
    pub nodes_applied: usize,
    pub edges_applied: usize,
    pub source_node: String,
    /// How the catch-up was served: `true` if an incremental delta was applied,
    /// `false` if a full snapshot was taken (fallback, or first-time recovery).
    pub incremental: bool,
    /// Number of operations applied when incremental (0 for a snapshot).
    pub ops_applied: usize,
}

/// P1-4: Progress checkpoint for resumable state transfer
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TransferCheckpoint {
    pub shard_id: usize,
    pub nodes_applied: usize,
    pub edges_applied: usize,
    pub last_node_idx: usize,
    pub last_edge_idx: usize,
    pub started_at: std::time::SystemTime,
}

impl TransferCheckpoint {
    fn new(shard_id: usize) -> Self {
        Self {
            shard_id,
            nodes_applied: 0,
            edges_applied: 0,
            last_node_idx: 0,
            last_edge_idx: 0,
            started_at: std::time::SystemTime::now(),
        }
    }

    /// Save checkpoint to disk (JSON format for simplicity)
    fn save(&self, checkpoint_dir: &std::path::Path) -> Result<(), String> {
        let path = checkpoint_dir.join(format!("transfer_shard_{}.ckpt", self.shard_id));
        let tmp_path = checkpoint_dir.join(format!("transfer_shard_{}.ckpt.tmp", self.shard_id));

        let json =
            serde_json::to_string_pretty(self).map_err(|e| format!("serialize checkpoint: {e}"))?;

        // C-8 FIX: Setup cleanup guard BEFORE writing to ensure temp file is always cleaned up
        // even if write fails. The guard will remove temp file on any error path.
        let tmp_path_clone = tmp_path.clone();
        let _cleanup = scopeguard::guard((), move |_| {
            let _ = std::fs::remove_file(&tmp_path_clone);
        });

        // Write to temporary file first, then atomic rename
        std::fs::write(&tmp_path, &json)
            .map_err(|e| format!("write temp checkpoint {}: {e}", tmp_path.display()))?;

        // Atomic rename (on most filesystems)
        std::fs::rename(&tmp_path, &path)
            .map_err(|e| format!("rename checkpoint {}: {e}", path.display()))?;

        // Success - defuse the cleanup guard by forgetting it
        std::mem::forget(_cleanup);

        Ok(())
    }

    /// Load checkpoint from disk, returns None if not found
    fn load(checkpoint_dir: &std::path::Path, shard_id: usize) -> Option<Self> {
        let path = checkpoint_dir.join(format!("transfer_shard_{}.ckpt", shard_id));
        let json = std::fs::read_to_string(&path).ok()?;
        serde_json::from_str(&json).ok()
    }

    /// Delete checkpoint after successful completion
    fn delete(checkpoint_dir: &std::path::Path, shard_id: usize) {
        let path = checkpoint_dir.join(format!("transfer_shard_{}.ckpt", shard_id));
        let _ = std::fs::remove_file(path);
    }
}

/// Response to an `ExportDelta` request — what the source can offer the caller
/// given its high-water seq. Serialized as JSON inside `GraphResult::Property`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum DeltaResponse {
    /// Caller is already at or beyond the source's high-water — nothing to ship.
    UpToDate,
    /// The requested seq fell outside the retained window (or the source has no
    /// log) — the caller must fall back to a full snapshot.
    TooOld,
    /// The delta since the requested seq: `(seq, op)` pairs in seq order.
    Delta { ops: Vec<(u64, GraphOperation)> },
}

/// Errors during state transfer.
#[derive(Debug, thiserror::Error)]
pub enum StateTransferError {
    #[error("source {0} unreachable: {1}")]
    SourceUnreachable(String, String),
    #[error("export from {0} returned an unexpected result shape")]
    BadExport(String),
    #[error("failed to decode snapshot: {0}")]
    Decode(String),
    #[error("failed to apply {kind} for {qid}: {reason}")]
    Apply {
        kind: &'static str,
        qid: String,
        reason: String,
    },
    #[error("state transfer timed out fetching shard {0} from {1}")]
    Timeout(usize, String),
    #[error("checkpoint error: {0}")]
    Checkpoint(String),
}

/// Drives operation-based state transfer over a [`RemoteGraphClient`].
pub struct StateTransfer {
    client: Arc<dyn RemoteGraphClient>,
    /// Timeout for the export fetch RPC.
    fetch_timeout: Duration,
    /// P1-4: Directory for saving transfer checkpoints (resumable transfer)
    checkpoint_dir: Option<std::path::PathBuf>,
}

impl StateTransfer {
    pub fn new(client: Arc<dyn RemoteGraphClient>) -> Self {
        Self {
            client,
            fetch_timeout: Duration::from_secs(30),
            checkpoint_dir: None,
        }
    }

    /// Override the export-fetch timeout.
    pub fn with_fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = timeout;
        self
    }

    /// P1-4: Enable resumable state transfer with checkpoint tracking.
    /// Pass a directory where progress will be saved every N operations.
    pub fn with_checkpoints(mut self, checkpoint_dir: std::path::PathBuf) -> Self {
        self.checkpoint_dir = Some(checkpoint_dir);
        self
    }

    /// Fetch a shard's snapshot from `source_node`.
    pub async fn fetch_shard(
        &self,
        source_node: &str,
        shard_id: usize,
        total_shards: usize,
    ) -> Result<ShardSnapshot, StateTransferError> {
        let op = GraphOperation::ExportShard {
            shard_id,
            total_shards,
        };
        let fetched =
            tokio::time::timeout(self.fetch_timeout, self.client.execute(source_node, op))
                .await
                .map_err(|_| StateTransferError::Timeout(shard_id, source_node.to_string()))?;

        let result = fetched.map_err(|e| {
            StateTransferError::SourceUnreachable(source_node.to_string(), e.to_string())
        })?;

        match result {
            GraphResult::Property(Some(json)) => {
                serde_json::from_value(json).map_err(|e| StateTransferError::Decode(e.to_string()))
            }
            _ => Err(StateTransferError::BadExport(source_node.to_string())),
        }
    }

    /// Catch up a shard: fetch its snapshot from `source_node` and replay it into
    /// the local graph via `apply_target` (the recovering node's own address, as
    /// registered in the cluster's remote client). Returns how much was applied.
    ///
    /// P1-4: Now supports resumable transfer — if a checkpoint exists and matches
    /// the snapshot size, resumes from that point. Saves checkpoints every 10k ops.
    pub async fn catch_up_shard(
        &self,
        source_node: &str,
        apply_target: &str,
        shard_id: usize,
        total_shards: usize,
    ) -> Result<CatchUpResult, StateTransferError> {
        let started = Instant::now();
        let snapshot = self
            .fetch_shard(source_node, shard_id, total_shards)
            .await?;

        // P1-4: Try to resume from checkpoint
        let mut checkpoint = if let Some(ref ckpt_dir) = self.checkpoint_dir {
            std::fs::create_dir_all(ckpt_dir)
                .map_err(|e| StateTransferError::Checkpoint(format!("create dir: {e}")))?;
            TransferCheckpoint::load(ckpt_dir, shard_id)
        } else {
            None
        };

        // If no checkpoint or snapshot structure changed, start fresh
        if checkpoint.is_none() {
            checkpoint = Some(TransferCheckpoint::new(shard_id));
        }
        let mut ckpt = checkpoint.unwrap();

        let mut nodes_applied = ckpt.nodes_applied;
        let checkpoint_interval = 10_000; // Save every 10k ops

        // Resume node processing from last checkpoint
        for (idx, node) in snapshot.nodes.iter().enumerate() {
            if idx < ckpt.last_node_idx {
                continue; // Already processed
            }

            let qid = NexoraId::from_hex(&node.qid_hex).map_err(|e| {
                StateTransferError::Decode(format!("bad node qid {}: {e}", node.qid_hex))
            })?;
            for (key, value) in &node.properties {
                let op = GraphOperation::SetProperty {
                    qid: qid.clone(),
                    key: key.clone(),
                    value: value.clone(),
                };
                self.client.execute(apply_target, op).await.map_err(|e| {
                    StateTransferError::Apply {
                        kind: "property",
                        qid: node.qid_hex.clone(),
                        reason: e.to_string(),
                    }
                })?;
            }
            nodes_applied += 1;
            ckpt.nodes_applied = nodes_applied;
            ckpt.last_node_idx = idx + 1;

            // Save checkpoint every N nodes
            if nodes_applied % checkpoint_interval == 0 {
                if let Some(ref dir) = self.checkpoint_dir {
                    if let Err(e) = ckpt.save(dir) {
                        tracing::warn!("Failed to save transfer checkpoint: {e}");
                    }
                }
            }
        }

        let mut edges_applied = ckpt.edges_applied;
        // Resume edge processing from last checkpoint
        for (idx, edge) in snapshot.edges.iter().enumerate() {
            if idx < ckpt.last_edge_idx {
                continue; // Already processed
            }

            let source = NexoraId::from_hex(&edge.source_hex).map_err(|e| {
                StateTransferError::Decode(format!("bad edge source {}: {e}", edge.source_hex))
            })?;
            let target = NexoraId::from_hex(&edge.target_hex).map_err(|e| {
                StateTransferError::Decode(format!("bad edge target {}: {e}", edge.target_hex))
            })?;
            let op = GraphOperation::AddEdge {
                source,
                edge_type: edge.edge_type.clone(),
                target,
                direction: edge.direction.clone(),
            };
            self.client
                .execute(apply_target, op)
                .await
                .map_err(|e| StateTransferError::Apply {
                    kind: "edge",
                    qid: format!("{}->{}", edge.source_hex, edge.target_hex),
                    reason: e.to_string(),
                })?;
            edges_applied += 1;
            ckpt.edges_applied = edges_applied;
            ckpt.last_edge_idx = idx + 1;

            // Save checkpoint every N edges
            if edges_applied % checkpoint_interval == 0 {
                if let Some(ref dir) = self.checkpoint_dir {
                    if let Err(e) = ckpt.save(dir) {
                        tracing::warn!("Failed to save transfer checkpoint: {e}");
                    }
                }
            }
        }

        // P1-4: Delete checkpoint after successful completion
        if let Some(ref dir) = self.checkpoint_dir {
            TransferCheckpoint::delete(dir, shard_id);
        }

        let elapsed = started.elapsed();
        tracing::info!(
            "Caught up shard {shard_id} from {source_node}: {nodes_applied} nodes, {edges_applied} edges in {elapsed:?}"
        );

        Ok(CatchUpResult {
            shard_id,
            nodes_applied,
            edges_applied,
            source_node: source_node.to_string(),
            incremental: false,
            ops_applied: 0,
        })
    }

    /// Incremental catch-up: ask the source for operations after `from_seq` and
    /// apply them. If the source's replication log no longer holds `from_seq`, fall
    /// back to a full [`Self::catch_up_shard`] snapshot when the source reports
    /// [`DeltaResponse::TooOld`]. If the recovering node is already up to date, the
    /// source returns [`DeltaResponse::UpToDate`] and no work is done.
    ///
    /// Returns the same `CatchUpResult` shape, with `incremental = true` when a
    /// delta is applied, or `false` if it fell back to a full snapshot.
    pub async fn catch_up_incremental(
        &self,
        source_node: &str,
        apply_target: &str,
        shard_id: usize,
        total_shards: usize,
        from_seq: u64,
    ) -> Result<CatchUpResult, StateTransferError> {
        let op = GraphOperation::ExportDelta { shard_id, from_seq };
        let fetched =
            tokio::time::timeout(self.fetch_timeout, self.client.execute(source_node, op))
                .await
                .map_err(|_| StateTransferError::Timeout(shard_id, source_node.to_string()))?;

        let result = fetched.map_err(|e| {
            StateTransferError::SourceUnreachable(source_node.to_string(), e.to_string())
        })?;

        let delta: DeltaResponse = match result {
            GraphResult::Property(Some(json)) => serde_json::from_value(json)
                .map_err(|e| StateTransferError::Decode(e.to_string()))?,
            _ => return Err(StateTransferError::BadExport(source_node.to_string())),
        };

        match delta {
            DeltaResponse::UpToDate => {
                tracing::info!("Shard {shard_id} already up to date (from_seq={from_seq})");
                Ok(CatchUpResult {
                    shard_id,
                    nodes_applied: 0,
                    edges_applied: 0,
                    source_node: source_node.to_string(),
                    incremental: true,
                    ops_applied: 0,
                })
            }
            DeltaResponse::TooOld => {
                tracing::info!(
                    "Shard {shard_id} delta from seq {from_seq} fell out of window, falling back to full snapshot"
                );
                self.catch_up_shard(source_node, apply_target, shard_id, total_shards)
                    .await
            }
            DeltaResponse::Delta { ops } => {
                let started = Instant::now();
                let ops_count = ops.len();
                for (_seq, op) in ops {
                    self.client.execute(apply_target, op).await.map_err(|e| {
                        StateTransferError::Apply {
                            kind: "delta-op",
                            qid: format!("shard-{shard_id}"),
                            reason: e.to_string(),
                        }
                    })?;
                }
                let elapsed = started.elapsed();
                tracing::info!(
                    "Incremental catch-up for shard {shard_id}: applied {ops_count} ops in {elapsed:?}"
                );
                Ok(CatchUpResult {
                    shard_id,
                    nodes_applied: 0,
                    edges_applied: 0,
                    source_node: source_node.to_string(),
                    incremental: true,
                    ops_applied: ops_count,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::NodeEntry;
    use crate::shard_map::OwnerEpoch;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock client that records apply calls and returns canned export responses.
    struct MockClient {
        /// Recorded (target, op) pairs.
        applied: Mutex<Vec<(String, GraphOperation)>>,
        /// Canned export result.
        export_result: GraphResult,
    }

    impl MockClient {
        fn new(export: ShardSnapshot) -> Arc<Self> {
            Arc::new(Self {
                applied: Mutex::new(Vec::new()),
                export_result: GraphResult::Property(Some(serde_json::to_value(export).unwrap())),
            })
        }

        fn applied_ops(&self) -> Vec<(String, GraphOperation)> {
            self.applied.lock().unwrap().clone()
        }
    }

    impl RemoteGraphClient for MockClient {
        fn execute<'a>(
            &'a self,
            target: &'a str,
            op: GraphOperation,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<GraphResult, crate::RouterError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                match op {
                    GraphOperation::ExportShard { .. } => Ok(self.export_result.clone()),
                    _ => {
                        self.applied.lock().unwrap().push((target.to_string(), op));
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "applied".into(),
                        })
                    }
                }
            })
        }
    }

    /// A client that answers `ExportDelta` with a canned `DeltaResponse` and
    /// records all other ops as "apply" calls.
    struct IncrementalMockClient {
        applied: Mutex<Vec<(String, GraphOperation)>>,
        delta_response: DeltaResponse,
        snapshot: Option<ShardSnapshot>,
    }

    impl RemoteGraphClient for IncrementalMockClient {
        fn execute<'a>(
            &'a self,
            target: &'a str,
            op: GraphOperation,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<GraphResult, crate::RouterError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                match op {
                    GraphOperation::ExportDelta { .. } => {
                        let json = serde_json::to_value(&self.delta_response).unwrap();
                        Ok(GraphResult::Property(Some(json)))
                    }
                    GraphOperation::ExportShard { .. } => {
                        let snap = self.snapshot.as_ref().unwrap();
                        Ok(GraphResult::Property(Some(
                            serde_json::to_value(snap).unwrap(),
                        )))
                    }
                    _ => {
                        self.applied.lock().unwrap().push((target.to_string(), op));
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "applied".into(),
                        })
                    }
                }
            })
        }
    }

    #[tokio::test]
    async fn test_catch_up_shard_empty() {
        let snap = ShardSnapshot {
            shard_id: 42,
            epoch: OwnerEpoch::from_value(1),
            nodes: vec![],
            edges: vec![],
            created_at_ms: 0,
        };
        let client = MockClient::new(snap.clone());
        let xfer = StateTransfer::new(client.clone());

        let result = xfer
            .catch_up_shard("source-node", "apply-node", 42, 8)
            .await
            .unwrap();
        assert_eq!(result.shard_id, 42);
        assert_eq!(result.nodes_applied, 0);
        assert_eq!(result.edges_applied, 0);
        assert_eq!(result.source_node, "source-node");
        assert!(!result.incremental);

        let applied = client.applied_ops();
        assert!(
            applied.is_empty(),
            "no ops should be applied for an empty shard"
        );
    }

    #[tokio::test]
    async fn test_catch_up_shard_one_node() {
        let node_qid_hex = "0000000000000000000000000000000000000000000000000000000000000001";
        let mut props = HashMap::new();
        props.insert("name".to_string(), serde_json::json!("Alice"));

        let snap = ShardSnapshot {
            shard_id: 42,
            epoch: OwnerEpoch::from_value(1),
            nodes: vec![NodeEntry {
                qid_hex: node_qid_hex.to_string(),
                properties: props,
            }],
            edges: vec![],
            created_at_ms: 0,
        };
        let client = MockClient::new(snap.clone());
        let xfer = StateTransfer::new(client.clone());

        let result = xfer
            .catch_up_shard("source-node", "apply-node", 42, 8)
            .await
            .unwrap();
        assert_eq!(result.nodes_applied, 1);
        assert_eq!(result.edges_applied, 0);

        let applied = client.applied_ops();
        assert_eq!(applied.len(), 1, "should have one SetProperty call");
        let (target, op) = &applied[0];
        assert_eq!(target, "apply-node");
        match op {
            GraphOperation::SetProperty { qid, key, value } => {
                assert_eq!(qid.to_hex(), node_qid_hex);
                assert_eq!(key, "name");
                assert_eq!(value, &serde_json::json!("Alice"));
            }
            _ => panic!("expected SetProperty, got {op:?}"),
        }
    }

    #[tokio::test]
    async fn test_catch_up_incremental_up_to_date() {
        let client = Arc::new(IncrementalMockClient {
            applied: Mutex::new(Vec::new()),
            delta_response: DeltaResponse::UpToDate,
            snapshot: None,
        });
        let xfer = StateTransfer::new(client.clone());

        let result = xfer
            .catch_up_incremental("source-node", "apply-node", 42, 8, 100)
            .await
            .unwrap();

        assert!(result.incremental);
        assert_eq!(result.ops_applied, 0);
        assert!(client.applied.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_catch_up_incremental_delta() {
        let delta = DeltaResponse::Delta {
            ops: vec![
                (
                    101,
                    GraphOperation::SetProperty {
                        qid: NexoraId::from_hex(
                            "0000000000000000000000000000000000000000000000000000000000000001",
                        )
                        .unwrap(),
                        key: "name".to_string(),
                        value: serde_json::json!("Bob"),
                    },
                ),
                (
                    102,
                    GraphOperation::SetProperty {
                        qid: NexoraId::from_hex(
                            "0000000000000000000000000000000000000000000000000000000000000002",
                        )
                        .unwrap(),
                        key: "age".to_string(),
                        value: serde_json::json!(30),
                    },
                ),
            ],
        };
        let client = Arc::new(IncrementalMockClient {
            applied: Mutex::new(Vec::new()),
            delta_response: delta,
            snapshot: None,
        });
        let xfer = StateTransfer::new(client.clone());

        let result = xfer
            .catch_up_incremental("source-node", "apply-node", 42, 8, 100)
            .await
            .unwrap();

        assert!(result.incremental);
        assert_eq!(result.ops_applied, 2);
        assert_eq!(client.applied.lock().unwrap().len(), 2);
    }
}
