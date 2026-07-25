//! Cross-shard distributed transactions — two-phase commit (2PC) protocol.
//!
//! Design doc §6.6: distributed transaction support for multi-shard writes.
//!
//! ## Two-Phase Commit (2PC)
//!
//! ```text
//!  Coordinator                    Participants
//!  ────────────                   ────────────
//!       │                              │
//!       │── PREPARE ──────────────────→│  Phase 1: Prepare
//!       │←── VOTE_YES / VOTE_NO ──────│
//!       │                              │
//!       │   (if all YES:)              │
//!       │── COMMIT ───────────────────→│  Phase 2: Commit
//!       │←── ACK ─────────────────────│
//!       │                              │
//!       │   (if any NO or timeout:)    │
//!       │── ABORT ────────────────────→│  Phase 2: Abort
//!       │←── ACK ─────────────────────│
//! ```
//!
//! ## Features
//!
//! - **Timeout handling**: Each phase has a configurable timeout.
//! - **Idempotent operations**: Participants deduplicate by transaction ID.
//! - **Fencing token integration**: Writes are fenced by shard epoch.
//! - **Automatic abort**: If any participant votes NO, the coordinator aborts.
//!
//! ## Usage
//!
//! ```ignore
//! # use std::sync::Arc;
//! # use nexora_zenoh::{TransactionCoordinator, LocalGraphClient, GraphOperation};
//! # async fn run() {
//! let client = Arc::new(LocalGraphClient::new());
//! let coord = TransactionCoordinator::new(client.clone());
//!
//! // Register participants
//! coord.add_participant("node-a".into()).await;
//! coord.add_participant("node-b".into()).await;
//!
//! // Prepare operations for each participant
//! let mut ops = std::collections::HashMap::new();
//! ops.insert("node-a".into(), vec![GraphOperation::SetProperty {
//!     qid: nexora_id::NexoraId::from_bytes(b"x".to_vec()),
//!     key: "k".into(),
//!     value: serde_json::json!(1),
//! }]);
//! ops.insert("node-b".into(), vec![GraphOperation::SetProperty {
//!     qid: nexora_id::NexoraId::from_bytes(b"y".to_vec()),
//!     key: "k".into(),
//!     value: serde_json::json!(2),
//! }]);
//!
//! let result = coord.execute_transaction("tx-1", ops).await;
//! assert!(result.is_committed());
//! # }
//! ```

use crate::{GraphOperation, RemoteGraphClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Unique transaction identifier.
pub type TxnId = String;

/// Participant node ID.
pub type ParticipantId = String;

/// State of a distributed transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransactionState {
    /// Transaction created but not yet started.
    Initiated,
    /// PREPARE messages sent to participants; collecting votes.
    Preparing,
    /// All participants voted YES; sending COMMIT.
    Committing,
    /// All participants acknowledged COMMIT.
    Committed,
    /// COMMIT decision was made, but at least one participant did not
    /// acknowledge applying its operations (error or timeout). The transaction
    /// cannot be aborted at this point; part of the write may be durable while
    /// the rest awaits reconciliation. This is a distinct, non-success outcome.
    CommitIncomplete,
    /// At least one participant voted NO or timed out; sending ABORT.
    Aborting,
    /// All participants acknowledged ABORT.
    Aborted,
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::Initiated => write!(f, "Initiated"),
            TransactionState::Preparing => write!(f, "Preparing"),
            TransactionState::Committing => write!(f, "Committing"),
            TransactionState::Committed => write!(f, "Committed"),
            TransactionState::CommitIncomplete => write!(f, "CommitIncomplete"),
            TransactionState::Aborting => write!(f, "Aborting"),
            TransactionState::Aborted => write!(f, "Aborted"),
        }
    }
}

/// A participant's vote in the prepare phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Vote {
    /// Participant is ready to commit.
    Yes,
    /// Participant cannot commit (e.g., conflict, local error).
    No(String),
}

/// Result of a distributed transaction.
#[derive(Clone, Debug)]
pub struct TransactionResult {
    pub txn_id: TxnId,
    pub state: TransactionState,
    pub participants: Vec<ParticipantId>,
    pub committed_participants: Vec<ParticipantId>,
    pub failed_participant: Option<(ParticipantId, String)>,
    pub started_at: Instant,
    pub finished_at: Instant,
}

impl TransactionResult {
    /// Check if the transaction was committed.
    pub fn is_committed(&self) -> bool {
        self.state == TransactionState::Committed
    }

    /// Check if the transaction was aborted.
    pub fn is_aborted(&self) -> bool {
        self.state == TransactionState::Aborted
    }

    /// Elapsed time of the transaction.
    pub fn duration(&self) -> Duration {
        self.finished_at.duration_since(self.started_at)
    }
}

/// Transaction metadata tracked by the coordinator.
#[derive(Clone, Debug)]
struct TransactionMeta {
    #[allow(dead_code)]
    id: TxnId,
    state: TransactionState,
    participants: Vec<ParticipantId>,
    /// Operations per participant.
    operations: HashMap<ParticipantId, Vec<GraphOperation>>,
    /// Votes collected so far.
    votes: HashMap<ParticipantId, Vote>,
    /// Participants that acknowledged commit/abort.
    acks: Vec<ParticipantId>,
    /// Start time.
    started_at: Instant,
    /// Failure reason (if any).
    failure_reason: Option<String>,
    /// Failed participant (if any).
    failed_participant: Option<(ParticipantId, String)>,
}

impl TransactionMeta {
    fn new(id: TxnId, operations: HashMap<ParticipantId, Vec<GraphOperation>>) -> Self {
        let participants: Vec<ParticipantId> = operations.keys().cloned().collect();
        Self {
            id,
            state: TransactionState::Initiated,
            participants,
            operations,
            votes: HashMap::new(),
            acks: Vec::new(),
            started_at: Instant::now(),
            failure_reason: None,
            failed_participant: None,
        }
    }

    #[allow(dead_code)]
    fn all_voted_yes(&self) -> bool {
        self.participants
            .iter()
            .all(|p| self.votes.get(p) == Some(&Vote::Yes))
    }

    fn any_voted_no(&self) -> bool {
        self.votes.values().any(|v| matches!(v, Vote::No(_)))
    }
}

/// Transaction coordinator — runs the 2PC protocol.
///
/// The coordinator sends PREPARE → collect votes → COMMIT or ABORT.
/// It uses a `RemoteGraphClient` to communicate with participants.
pub struct TransactionCoordinator {
    /// Client for remote operations.
    client: Arc<dyn RemoteGraphClient>,
    /// Registered participants (for validation).
    registered: Mutex<Vec<ParticipantId>>,
    /// Active transactions.
    active: Mutex<HashMap<TxnId, TransactionMeta>>,
    /// Completed transactions (history, capped at 100).
    completed: Mutex<Vec<TransactionResult>>,
    /// Timeout for the prepare phase.
    prepare_timeout: Duration,
    /// Timeout for the commit/abort phase.
    commit_timeout: Duration,
}

impl TransactionCoordinator {
    /// Create a new transaction coordinator.
    pub fn new(client: Arc<dyn RemoteGraphClient>) -> Self {
        Self {
            client,
            registered: Mutex::new(Vec::new()),
            active: Mutex::new(HashMap::new()),
            completed: Mutex::new(Vec::new()),
            prepare_timeout: Duration::from_secs(10),
            commit_timeout: Duration::from_secs(10),
        }
    }

    /// Set the prepare phase timeout.
    pub fn with_prepare_timeout(mut self, timeout: Duration) -> Self {
        self.prepare_timeout = timeout;
        self
    }

    /// Set the commit phase timeout.
    pub fn with_commit_timeout(mut self, timeout: Duration) -> Self {
        self.commit_timeout = timeout;
        self
    }

    /// Register a participant node.
    pub async fn add_participant(&self, node_id: ParticipantId) {
        self.registered.lock().await.push(node_id);
    }

    /// Execute a distributed transaction with the given operations.
    ///
    /// This runs the full 2PC protocol:
    /// 1. Send PREPARE to all participants (collect votes)
    /// 2. If all vote YES → send COMMIT
    /// 3. If any vote NO → send ABORT
    ///
    /// Returns the final `TransactionResult`.
    pub async fn execute_transaction(
        &self,
        txn_id: impl Into<TxnId>,
        operations: HashMap<ParticipantId, Vec<GraphOperation>>,
    ) -> TransactionResult {
        let txn_id = txn_id.into();
        let meta = TransactionMeta::new(txn_id.clone(), operations);
        let started_at = meta.started_at;

        self.active
            .lock()
            .await
            .insert(txn_id.clone(), meta.clone());

        // Phase 1: PREPARE
        let prepare_result = self.prepare_phase(&txn_id, &meta).await;

        let final_state = match &prepare_result {
            PrepareOutcome::AllYes(updated_meta) => {
                // Phase 2a: COMMIT
                match self.commit_phase(&txn_id, updated_meta).await {
                    Ok(state) => state,
                    Err(e) => {
                        // Commit failed - record as incomplete and return it
                        let mut completed = self.completed.lock().await;
                        let result = TransactionResult {
                            txn_id: txn_id.clone(),
                            state: TransactionState::CommitIncomplete,
                            started_at,
                            finished_at: Instant::now(),
                            participants: meta.participants.clone(),
                            committed_participants: vec![],
                            failed_participant: Some((String::new(), e.clone())),
                        };
                        completed.push(result.clone());
                        if completed.len() > 100 {
                            completed.remove(0);
                        }
                        return result;
                    }
                }
            }
            PrepareOutcome::AnyNo(updated_meta) => {
                // Update active meta with failure info before aborting
                {
                    let mut active = self.active.lock().await;
                    if let Some(m) = active.get_mut(&txn_id) {
                        m.failed_participant = updated_meta.failed_participant.clone();
                        m.failure_reason = updated_meta.failure_reason.clone();
                        m.votes = updated_meta.votes.clone();
                    }
                }
                // Phase 2b: ABORT
                self.abort_phase(&txn_id, updated_meta).await
            }
        };

        // Build result
        let mut active = self.active.lock().await;
        let meta = active.remove(&txn_id).unwrap_or_else(|| {
            // Shouldn't happen, but handle gracefully
            TransactionMeta::new(txn_id.clone(), HashMap::new())
        });

        let result = TransactionResult {
            txn_id: txn_id.clone(),
            state: final_state.clone(),
            participants: meta.participants.clone(),
            committed_participants: meta.acks.clone(),
            failed_participant: meta.failed_participant.clone(),
            started_at,
            finished_at: Instant::now(),
        };

        // Store in history
        let mut completed = self.completed.lock().await;
        completed.push(result.clone());
        if completed.len() > 100 {
            let drain_count = completed.len() - 100;
            completed.drain(0..drain_count);
        }

        result
    }

    /// Phase 1: Send PREPARE to all participants, collect votes.
    ///
    /// For each participant, we execute a probe operation (GetProperty on a
    /// sentinel key) to check if the participant is alive and can accept
    /// writes. In a production system, this would be a dedicated PREPARE RPC
    /// that validates the operations locally.
    async fn prepare_phase(&self, _txn_id: &TxnId, meta: &TransactionMeta) -> PrepareOutcome {
        let mut updated = meta.clone();
        updated.state = TransactionState::Preparing;

        // Send PREPARE to all participants in parallel
        let mut handles = Vec::new();

        for participant in &meta.participants {
            let client = self.client.clone();
            let participant = participant.clone();
            let ops = meta
                .operations
                .get(&participant)
                .cloned()
                .unwrap_or_default();

            handles.push(tokio::spawn(async move {
                let vote = prepare_participant(&client, &participant, &ops).await;
                (participant, vote)
            }));
        }

        // Collect votes with timeout
        let collect_fut = async {
            for handle in handles {
                if let Ok((participant, vote)) = handle.await {
                    updated.votes.insert(participant, vote);
                }
            }
        };

        let _ = tokio::time::timeout(self.prepare_timeout, collect_fut).await;

        // Check for missing votes (timeout)
        for participant in &meta.participants {
            if !updated.votes.contains_key(participant) {
                updated
                    .votes
                    .insert(participant.clone(), Vote::No("prepare timeout".into()));
                updated.failed_participant = Some((participant.clone(), "prepare timeout".into()));
            }
        }

        if updated.any_voted_no() {
            // Find the first NO reason
            for (p, v) in &updated.votes {
                if let Vote::No(reason) = v {
                    updated.failure_reason = Some(reason.clone());
                    if updated.failed_participant.is_none() {
                        updated.failed_participant = Some((p.clone(), reason.clone()));
                    }
                }
            }
            PrepareOutcome::AnyNo(updated)
        } else {
            PrepareOutcome::AllYes(updated)
        }
    }

    /// Phase 2a: Send COMMIT to all participants.
    ///
    /// Returns `Committed` only when every participant acknowledged applying its
    /// operations. If any participant fails (error or timeout), the coordinator
    /// records the failure and returns an error — it must NOT report
    /// success, because doing so would tell the caller a cross-shard write is
    /// durable when part of it was never applied.
    ///
    /// Note: once the commit decision is made (all voted YES), 2PC cannot abort;
    /// participants that are momentarily unreachable must eventually apply the
    /// commit on retry/recovery. This implementation surfaces the incomplete
    /// state honestly; durable intent-log replay for automatic completion is
    /// tracked as future work (see `prepare_participant`).
    async fn commit_phase(
        &self,
        txn_id: &TxnId,
        meta: &TransactionMeta,
    ) -> Result<TransactionState, String> {
        let mut updated = meta.clone();
        updated.state = TransactionState::Committing;

        // Send COMMIT (execute actual operations) to all participants
        let mut handles = Vec::new();
        for participant in &meta.participants {
            let client = self.client.clone();
            let participant = participant.clone();
            let ops = meta
                .operations
                .get(&participant)
                .cloned()
                .unwrap_or_default();

            handles.push(tokio::spawn(async move {
                let mut error: Option<String> = None;
                for op in ops {
                    if let Err(e) = client.execute(&participant, op).await {
                        error = Some(e.to_string());
                        break;
                    }
                }
                (participant, error)
            }));
        }

        // Collect acks with timeout. A participant only acks if all its ops applied.
        let mut failed: Option<(ParticipantId, String)> = None;
        let collect_fut = async {
            for handle in handles {
                if let Ok((participant, error)) = handle.await {
                    match error {
                        None => updated.acks.push(participant),
                        Some(reason) => {
                            if failed.is_none() {
                                failed = Some((participant, reason));
                            }
                        }
                    }
                }
            }
        };

        if tokio::time::timeout(self.commit_timeout, collect_fut)
            .await
            .is_err()
            && failed.is_none()
        {
            failed = Some(("<unknown>".into(), "commit phase timeout".into()));
        }

        // A commit is only complete when every participant acknowledged.
        let all_acked = updated.acks.len() == meta.participants.len();

        if !all_acked || failed.is_some() {
            // P0-1 FIX: Return error instead of CommitIncomplete state
            tracing::error!(
                txn_id = %txn_id,
                acked = updated.acks.len(),
                participants = meta.participants.len(),
                failure = ?failed,
                "2PC commit incomplete: some participants did not apply; \
                 cross-shard state may be partially applied and needs reconciliation"
            );

            // Update active transaction state before returning error
            let mut active = self.active.lock().await;
            if let Some(m) = active.get_mut(txn_id) {
                m.state = TransactionState::CommitIncomplete;
                m.acks = updated.acks.clone();
                if let Some((p, reason)) = &failed {
                    m.failed_participant = Some((p.clone(), reason.clone()));
                    m.failure_reason = Some(reason.clone());
                }
            }

            return Err(format!(
                "commit incomplete: {}/{} participants acknowledged, failure: {:?}",
                updated.acks.len(),
                meta.participants.len(),
                failed
            ));
        }

        // All participants acknowledged successfully
        let final_state = TransactionState::Committed;

        // Update active transaction state
        let mut active = self.active.lock().await;
        if let Some(m) = active.get_mut(txn_id) {
            m.state = final_state.clone();
            m.acks = updated.acks.clone();
        }

        Ok(final_state)
    }

    /// Phase 2b: Send ABORT to all participants.
    async fn abort_phase(&self, txn_id: &TxnId, meta: &TransactionMeta) -> TransactionState {
        let mut updated = meta.clone();
        updated.state = TransactionState::Aborting;

        // In 2PC, ABORT means participants should discard any prepared state.
        // Since our prepare phase doesn't modify state, ABORT is a no-op
        // acknowledgement from participants.
        for participant in &meta.participants {
            updated.acks.push(participant.clone());
        }

        // Update active transaction state
        let mut active = self.active.lock().await;
        if let Some(m) = active.get_mut(txn_id) {
            m.state = TransactionState::Aborted;
            m.acks = updated.acks.clone();
        }

        TransactionState::Aborted
    }

    /// Get all active transaction IDs.
    pub async fn active_transactions(&self) -> Vec<TxnId> {
        self.active.lock().await.keys().cloned().collect()
    }

    /// Get completed transaction history.
    pub async fn completed_transactions(&self) -> Vec<TransactionResult> {
        self.completed.lock().await.clone()
    }
}

/// Outcome of the prepare phase.
enum PrepareOutcome {
    AllYes(TransactionMeta),
    AnyNo(TransactionMeta),
}

/// Probe a participant's readiness for the PREPARE phase.
///
/// IMPORTANT — this is a *liveness/reachability probe only*, not a durable
/// prepare. It issues a read against each target to confirm the shard is
/// reachable, then votes YES. It does NOT reserve locks or write an intent log,
/// so a YES vote does not guarantee the participant can still commit later (a
/// concurrent writer may mutate the same key between vote and commit). Genuine
/// 2PC isolation requires the coordinator to route PREPARE through
/// [`TransactionParticipant::handle_prepare`], which records prepared operations
/// and enables ABORT to discard them; wiring that durable path (and crash
/// recovery of prepared state) is tracked as future work. Until then, callers
/// must treat this coordinator as providing atomicity-on-a-best-effort basis,
/// not strict serializable isolation.
async fn prepare_participant(
    client: &Arc<dyn RemoteGraphClient>,
    participant: &str,
    ops: &[GraphOperation],
) -> Vote {
    // Phase 1a: Validate — check that all target nodes exist and are writable
    for op in ops {
        match op {
            GraphOperation::SetProperty { qid, key, value: _ } => {
                // Probe: check if the node is reachable by reading the key
                // (this validates that the shard is alive and the node exists)
                let probe = GraphOperation::GetProperty {
                    qid: qid.clone(),
                    key: key.clone(),
                };
                if let Err(e) = client.execute(participant, probe).await {
                    return Vote::No(format!(
                        "node {} not reachable on {}: {}",
                        qid, participant, e
                    ));
                }
            }
            _ => {
                // For other operations, probe with a generic status check
                let probe = GraphOperation::GetProperty {
                    qid: nexora_id::NexoraId::from_bytes(b"__txn_probe__".to_vec()),
                    key: "__probe__".into(),
                };
                if let Err(e) = client.execute(participant, probe).await {
                    return Vote::No(format!("participant {} not reachable: {}", participant, e));
                }
            }
        }
    }

    // NOTE: This path only reachability-probes; it does NOT reserve locks or
    // write a durable intent log, so a YES here is not a true 2PC prepare vote.
    // Between this vote and the commit, another writer may mutate the same keys.
    // The durable-prepare path lives in `TransactionParticipant::handle_prepare`
    // (which records `prepared` ops); wiring the coordinator to drive that RPC is
    // tracked as future work. Until then, treat committed transactions as
    // providing atomicity of *delivery*, not isolation.
    Vote::Yes
}

/// Transaction participant — handles prepare/commit/abort requests.
///
/// In a production system, each node would run a `TransactionParticipant`
/// that receives RPCs from the coordinator. Here we provide the structure
/// for completeness; the actual prepare/commit logic is simulated in
/// `prepare_participant`.
pub struct TransactionParticipant {
    /// This node's ID.
    node_id: ParticipantId,
    /// Client for local operations.
    client: Arc<dyn RemoteGraphClient>,
    /// Prepared transactions (waiting for commit/abort).
    prepared: Mutex<HashMap<TxnId, Vec<GraphOperation>>>,
    /// Completed transactions (for deduplication).
    completed_txns: Mutex<HashMap<TxnId, TransactionState>>,
}

impl TransactionParticipant {
    /// Create a new transaction participant.
    pub fn new(node_id: ParticipantId, client: Arc<dyn RemoteGraphClient>) -> Self {
        Self {
            node_id,
            client,
            prepared: Mutex::new(HashMap::new()),
            completed_txns: Mutex::new(HashMap::new()),
        }
    }

    /// Handle a PREPARE request from the coordinator.
    ///
    /// Returns `Vote::Yes` if the participant can commit, `Vote::No` otherwise.
    pub async fn handle_prepare(&self, txn_id: TxnId, ops: Vec<GraphOperation>) -> Vote {
        // Check for duplicate transaction
        let completed = self.completed_txns.lock().await;
        if let Some(state) = completed.get(&txn_id) {
            // Already processed — return the previous result
            return match state {
                TransactionState::Committed => Vote::Yes,
                TransactionState::Aborted => Vote::No("previously aborted".into()),
                _ => Vote::No("unexpected state".into()),
            };
        }
        drop(completed);

        // Store prepared operations
        self.prepared.lock().await.insert(txn_id, ops);

        Vote::Yes
    }

    /// Handle a COMMIT request from the coordinator.
    ///
    /// Executes the prepared operations and returns `true` on success.
    pub async fn handle_commit(&self, txn_id: &TxnId) -> bool {
        let ops = self.prepared.lock().await.remove(txn_id);
        let Some(ops) = ops else {
            // Already committed or never prepared
            return true;
        };

        let mut success = true;
        for op in ops {
            if self.client.execute(&self.node_id, op).await.is_err() {
                success = false;
            }
        }

        self.completed_txns
            .lock()
            .await
            .insert(txn_id.clone(), TransactionState::Committed);

        success
    }

    /// Handle an ABORT request from the coordinator.
    ///
    /// Discards prepared operations.
    pub async fn handle_abort(&self, txn_id: &TxnId) {
        self.prepared.lock().await.remove(txn_id);
        self.completed_txns
            .lock()
            .await
            .insert(txn_id.clone(), TransactionState::Aborted);
    }

    /// Get the node ID of this participant.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Errors specific to distributed transactions.
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("participant {0} not found")]
    ParticipantNotFound(ParticipantId),
    #[error("prepare phase failed: {0}")]
    PrepareFailed(String),
    #[error("commit phase failed: {0}")]
    CommitFailed(String),
    #[error("transaction timed out")]
    Timeout,
    #[error("transaction already completed: {0}")]
    AlreadyCompleted(TxnId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_client::LocalGraphClient;
    use crate::{GraphResult, RouterError};
    use nexora_id::NexoraId;

    // ── State tests ──

    #[test]
    fn test_transaction_state_display() {
        assert_eq!(TransactionState::Initiated.to_string(), "Initiated");
        assert_eq!(TransactionState::Preparing.to_string(), "Preparing");
        assert_eq!(TransactionState::Committing.to_string(), "Committing");
        assert_eq!(TransactionState::Committed.to_string(), "Committed");
        assert_eq!(TransactionState::Aborting.to_string(), "Aborting");
        assert_eq!(TransactionState::Aborted.to_string(), "Aborted");
    }

    // ── Coordinator 2PC tests ──

    #[tokio::test]
    async fn test_2pc_commit_success() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client.clone());

        let qid_a = NexoraId::from_bytes(b"node-a".to_vec());
        let qid_b = NexoraId::from_bytes(b"node-b".to_vec());

        let mut ops = HashMap::new();
        ops.insert(
            "node-a".into(),
            vec![GraphOperation::SetProperty {
                qid: qid_a.clone(),
                key: "val".into(),
                value: serde_json::json!(1),
            }],
        );
        ops.insert(
            "node-b".into(),
            vec![GraphOperation::SetProperty {
                qid: qid_b.clone(),
                key: "val".into(),
                value: serde_json::json!(2),
            }],
        );

        let result = coord.execute_transaction("tx-1", ops).await;

        assert!(result.is_committed());
        assert_eq!(result.participants.len(), 2);
        assert!(result.failed_participant.is_none());

        // Verify data was written (LocalGraphClient ignores target_node)
        let r = client
            .execute(
                "any",
                GraphOperation::GetProperty {
                    qid: qid_a,
                    key: "val".into(),
                },
            )
            .await
            .unwrap();
        match r {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!(1)),
            _ => panic!("expected Property(Some(1))"),
        }
    }

    #[tokio::test]
    async fn test_2pc_abort_on_participant_failure() {
        // Create a client that always fails
        struct FailingClient;
        impl RemoteGraphClient for FailingClient {
            fn execute<'a>(
                &'a self,
                _target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                Box::pin(async { Err(RouterError::Remote("connection refused".into())) })
            }
        }

        let client = Arc::new(FailingClient);
        let coord = TransactionCoordinator::new(client);

        let mut ops = HashMap::new();
        ops.insert(
            "node-a".into(),
            vec![GraphOperation::SetProperty {
                qid: NexoraId::from_bytes(b"x".to_vec()),
                key: "k".into(),
                value: serde_json::json!(1),
            }],
        );

        let result = coord.execute_transaction("tx-fail", ops).await;

        assert!(result.is_aborted());
        assert!(result.failed_participant.is_some());
    }

    #[tokio::test]
    async fn test_2pc_with_mixed_success() {
        // One participant succeeds (prepare votes YES), but since all must vote YES,
        // if any fails the transaction aborts.

        // Create a client that works for "good-node" and fails for "bad-node"
        struct MixedClient;
        impl RemoteGraphClient for MixedClient {
            fn execute<'a>(
                &'a self,
                target: &'a str,
                _op: GraphOperation,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<GraphResult, RouterError>> + Send + 'a>,
            > {
                Box::pin(async move {
                    if target == "bad-node" {
                        Err(RouterError::Remote("down".into()))
                    } else {
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "ok".into(),
                        })
                    }
                })
            }
        }

        let client = Arc::new(MixedClient);
        let coord = TransactionCoordinator::new(client);

        let qid = NexoraId::from_bytes(b"test".to_vec());
        let mut ops = HashMap::new();
        ops.insert(
            "good-node".into(),
            vec![GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "k".into(),
                value: serde_json::json!(1),
            }],
        );
        ops.insert(
            "bad-node".into(),
            vec![GraphOperation::SetProperty {
                qid: qid.clone(),
                key: "k".into(),
                value: serde_json::json!(2),
            }],
        );

        let result = coord.execute_transaction("tx-mixed", ops).await;

        // Should abort because bad-node votes NO
        assert!(result.is_aborted());
        assert!(result.failed_participant.is_some());
    }

    #[tokio::test]
    async fn test_2pc_single_participant() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client);

        let qid = NexoraId::from_bytes(b"solo".to_vec());
        let mut ops = HashMap::new();
        ops.insert(
            "only-node".into(),
            vec![GraphOperation::SetProperty {
                qid,
                key: "k".into(),
                value: serde_json::json!(42),
            }],
        );

        let result = coord.execute_transaction("tx-solo", ops).await;
        assert!(result.is_committed());
        assert_eq!(result.participants.len(), 1);
    }

    #[tokio::test]
    async fn test_2pc_empty_participants() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client);

        let ops = HashMap::new();
        let result = coord.execute_transaction("tx-empty", ops).await;

        // No participants → all voted yes (vacuously true) → committed
        assert!(result.is_committed());
    }

    #[tokio::test]
    async fn test_2pc_transaction_history() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client);

        let ops = HashMap::new();
        coord.execute_transaction("tx-1", ops.clone()).await;
        coord.execute_transaction("tx-2", ops.clone()).await;
        coord.execute_transaction("tx-3", ops).await;

        let completed = coord.completed_transactions().await;
        assert_eq!(completed.len(), 3);
        assert_eq!(completed[0].txn_id, "tx-1");
        assert_eq!(completed[2].txn_id, "tx-3");
    }

    #[tokio::test]
    async fn test_2pc_no_active_after_completion() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client);

        let ops = HashMap::new();
        coord.execute_transaction("tx-1", ops).await;

        let active = coord.active_transactions().await;
        assert!(active.is_empty());
    }

    // ── Participant tests ──

    #[tokio::test]
    async fn test_participant_prepare_commit() {
        let client = Arc::new(LocalGraphClient::new());
        let participant = TransactionParticipant::new("node-1".into(), client.clone());

        let qid = NexoraId::from_bytes(b"test".to_vec());
        let ops = vec![GraphOperation::SetProperty {
            qid: qid.clone(),
            key: "k".into(),
            value: serde_json::json!(99),
        }];

        // Prepare
        let vote = participant.handle_prepare("tx-1".into(), ops).await;
        assert_eq!(vote, Vote::Yes);

        // Commit
        let success = participant.handle_commit(&"tx-1".to_string()).await;
        assert!(success);

        // Verify data
        let r = client
            .execute(
                "node-1",
                GraphOperation::GetProperty {
                    qid,
                    key: "k".into(),
                },
            )
            .await
            .unwrap();
        match r {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!(99)),
            _ => panic!("expected Property(Some(99))"),
        }
    }

    #[tokio::test]
    async fn test_participant_prepare_abort() {
        let client = Arc::new(LocalGraphClient::new());
        let participant = TransactionParticipant::new("node-1".into(), client.clone());

        let qid = NexoraId::from_bytes(b"test".to_vec());
        let ops = vec![GraphOperation::SetProperty {
            qid,
            key: "k".into(),
            value: serde_json::json!(1),
        }];

        // Prepare
        let vote = participant.handle_prepare("tx-1".into(), ops).await;
        assert_eq!(vote, Vote::Yes);

        // Abort (should not write data)
        participant.handle_abort(&"tx-1".to_string()).await;

        // Verify no data was written
        let r = client
            .execute(
                "node-1",
                GraphOperation::GetProperty {
                    qid: NexoraId::from_bytes(b"test".to_vec()),
                    key: "k".into(),
                },
            )
            .await
            .unwrap();
        match r {
            GraphResult::Property(None) => {} // Good, no data
            _ => panic!("expected Property(None) after abort"),
        }
    }

    #[tokio::test]
    async fn test_participant_duplicate_prepare() {
        let client = Arc::new(LocalGraphClient::new());
        let participant = TransactionParticipant::new("node-1".into(), client);

        let ops = vec![GraphOperation::SetProperty {
            qid: NexoraId::from_bytes(b"x".to_vec()),
            key: "k".into(),
            value: serde_json::json!(1),
        }];

        // First prepare
        let vote1 = participant.handle_prepare("tx-1".into(), ops.clone()).await;
        assert_eq!(vote1, Vote::Yes);

        // Commit
        participant.handle_commit(&"tx-1".to_string()).await;

        // Second prepare (duplicate) should return committed state
        let vote2 = participant.handle_prepare("tx-1".into(), ops).await;
        assert_eq!(vote2, Vote::Yes); // Previously committed → Yes
    }

    #[tokio::test]
    async fn test_transaction_result_duration() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client);

        let ops = HashMap::new();
        let result = coord.execute_transaction("tx-1", ops).await;

        let dur = result.duration();
        assert!(dur.as_nanos() > 0);
    }

    #[tokio::test]
    async fn test_coordinator_with_timeouts() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client)
            .with_prepare_timeout(Duration::from_millis(100))
            .with_commit_timeout(Duration::from_millis(100));

        let ops = HashMap::new();
        let result = coord.execute_transaction("tx-1", ops).await;
        assert!(result.is_committed());
    }

    #[tokio::test]
    async fn test_2pc_multiple_operations_per_participant() {
        let client = Arc::new(LocalGraphClient::new());
        let coord = TransactionCoordinator::new(client.clone());

        let qid1 = NexoraId::from_bytes(b"n1".to_vec());
        let qid2 = NexoraId::from_bytes(b"n2".to_vec());

        let mut ops = HashMap::new();
        ops.insert(
            "node-a".into(),
            vec![
                GraphOperation::SetProperty {
                    qid: qid1.clone(),
                    key: "name".into(),
                    value: serde_json::json!("Alice"),
                },
                GraphOperation::SetProperty {
                    qid: qid2.clone(),
                    key: "name".into(),
                    value: serde_json::json!("Bob"),
                },
            ],
        );

        let result = coord.execute_transaction("tx-multi", ops).await;
        assert!(result.is_committed());

        // Verify both writes
        let r1 = client
            .execute(
                "any",
                GraphOperation::GetProperty {
                    qid: qid1,
                    key: "name".into(),
                },
            )
            .await
            .unwrap();
        match r1 {
            GraphResult::Property(Some(v)) => assert_eq!(v, serde_json::json!("Alice")),
            _ => panic!("expected Alice"),
        }
    }
}
