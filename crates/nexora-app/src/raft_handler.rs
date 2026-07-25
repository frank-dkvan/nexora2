//! RaftHandler — integrates RaftLogReplicator with the GraphService.
//!
//! Provides a background task that bridges nexora-raft's write-through replication
//! engine with nexora-zenoh's ClusterManager. When cluster mode is active,
//! ownership changes and shard mutations go through Raft consensus before
//! being committed to in-memory state.
//!
//! ## Architecture
//!
//! ```text
//! HTTP mutation → GraphService.set_property()
//!                  → WAL.append(entry)  ← existing path
//!                  → RaftLogReplicator.append_local(seq_no)
//!                    → replicate to followers
//!                    → quorum reached → commit to memory
//! ```
//!
//! **Backward compatibility**: Raft mode is opt-in via `--raft-port`.
//! When `--raft-port` is not set, the existing TCP heartbeat-only
//! cluster mode runs unchanged.

use nexora_core::GraphService;
use nexora_raft::{
    AppendEntriesResponse, CommitGate, LogEntry, RaftConfig, RaftLogReplicator, ReplicationError,
    ReplicationTarget, SnapshotData,
};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the Raft handler, derived from CLI flags.
#[derive(Clone, Debug)]
pub struct RaftHandlerConfig {
    /// Port this Raft node listens on for peer RPC traffic.
    pub raft_port: u16,
    /// Addresses of Raft peer nodes (host:port pairs).
    pub raft_peers: Vec<String>,
    /// Quorum size (majority of cluster).
    pub quorum_size: usize,
    /// Total nodes in the Raft cluster (including self).
    pub total_nodes: usize,
    /// Timeout for RPC calls.
    pub rpc_timeout: Duration,
    /// Maximum batch size for AppendEntries.
    pub max_batch_size: usize,
    /// Current Raft term (always starts at 1).
    pub current_term: u64,
    /// This node's ID.
    pub node_id: String,
    /// Shard ID this Raft node manages.
    pub shard_id: usize,
    /// Directory in which to persist the applied-index watermark so it survives
    /// restart. `None` keeps it in memory only (tests / single-node dev).
    pub state_dir: Option<std::path::PathBuf>,
    /// Whether this node is the leader for this shard. Only the leader should
    /// replicate to followers; non-leaders skip replication to avoid sending
    /// stale/conflicting updates.
    ///
    /// NOTE: there is no leader *election* in this crate — ownership is decided
    /// externally (ShardMap owner / the openraft control plane). This flag is the
    /// gate mechanism; the caller supplies the truth. `main.rs` currently passes a
    /// hardcoded `true` because the `--raft-port` path is an experimental
    /// log-shipping skeleton that carries no real data (see its construction site
    /// for the full rationale). Wire this to a real owner check when the path
    /// graduates to replicating real data.
    pub is_leader: bool,
}

impl Default for RaftHandlerConfig {
    fn default() -> Self {
        Self {
            raft_port: 0,
            raft_peers: Vec::new(),
            quorum_size: 2,
            total_nodes: 3,
            rpc_timeout: Duration::from_secs(5),
            max_batch_size: 100,
            current_term: 1,
            node_id: "local".to_string(),
            shard_id: 0,
            state_dir: None,
            is_leader: false,
        }
    }
}

/// RaftHandler provides the integration layer between Raft consensus
/// and the local GraphService.
///
/// It owns:
/// - A `CommitGate` for quorum-based commit gating
/// - A `RaftLogReplicator` for log replication to followers
/// - A background task that accepts peer RPC connections
pub struct RaftHandler {
    /// The commit gate — intercepts WAL writes and gates on quorum.
    pub commit_gate: Arc<CommitGate>,
    /// Direct reference to the replicator (for debug/metrics).
    pub replicator: Arc<RaftLogReplicator>,
    /// Peer addresses discovered at startup.
    peers: Vec<String>,
    /// Raft port for incoming peer connections.
    raft_port: u16,
    /// Shutdown signal.
    shutdown: Arc<tokio::sync::Notify>,
    /// Graph service reference (for serving peer requests).
    graph: Arc<GraphService>,
    /// Highest seq_no this follower has durably applied. Used for dedup and
    /// fail-closed acknowledgement in the AppendEntries handler.
    applied_index: Arc<std::sync::atomic::AtomicU64>,
    /// File backing `applied_index` so the dedup watermark survives restart.
    /// `None` = in-memory only.
    applied_index_path: Option<Arc<std::path::PathBuf>>,
    /// Whether this node is the leader. Only the leader should replicate to
    /// followers; non-leaders skip the replication loop to avoid stale writes.
    is_leader: bool,
}

/// Load the persisted applied-index watermark from `path`, or 0 if absent /
/// unreadable. A corrupt or missing file is treated as 0 (replay from the
/// start): the apply path is idempotent per entry, so re-reading 0 is safe,
/// just less efficient than resuming from the true watermark.
fn load_applied_index(path: &std::path::Path) -> u64 {
    match std::fs::read_to_string(path) {
        Ok(s) => s.trim().parse().unwrap_or(0),
        Err(_) => 0,
    }
}

/// Persist the applied-index watermark durably: write to a temp file, fsync,
/// then atomically rename over the target so a crash mid-write can never leave
/// a torn value. Errors are logged, not propagated — a failed persist must not
/// abort apply (the in-memory watermark is still correct for this process).
fn persist_applied_index(path: &std::path::Path, value: u64) {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let write = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(value.to_string().as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = write {
        tracing::warn!(error = %e, path = %path.display(), "failed to persist Raft applied_index");
    }
}

impl RaftHandler {
    /// Create a new RaftHandler from configuration.
    pub fn new(config: RaftHandlerConfig, graph: Arc<GraphService>) -> Self {
        let shard_id = config.shard_id;
        let raft_config = RaftConfig {
            quorum_size: config.quorum_size,
            total_nodes: config.total_nodes,
            rpc_timeout: config.rpc_timeout,
            max_batch_size: config.max_batch_size,
            current_term: config.current_term,
            node_id: config.node_id.clone(),
            shard_id,
        };

        // Capture shard_id by move (copy) into the closure so it lives statically
        let qid_to_shard: Arc<dyn Fn(&nexora_id::NexoraId) -> usize + Send + Sync> =
            Arc::new(move |_qid: &nexora_id::NexoraId| shard_id);

        let gate = CommitGate::new(raft_config.clone(), qid_to_shard);
        let replicator = gate.replicator().clone();

        // Restore the applied-index watermark so a restarted follower resumes
        // dedup from where it left off, instead of re-applying every committed
        // entry from seq 1 (or stalling on the gap check if the leader's log was
        // truncated past this node's true position).
        let applied_index_path = config
            .state_dir
            .as_ref()
            .map(|dir| Arc::new(dir.join(format!("raft_applied_index_shard{}", shard_id))));
        let initial_applied = applied_index_path
            .as_ref()
            .map(|p| load_applied_index(p))
            .unwrap_or(0);
        if initial_applied > 0 {
            tracing::info!(
                applied_index = initial_applied,
                shard = shard_id,
                "restored Raft applied_index watermark from disk"
            );
        }

        Self {
            commit_gate: Arc::new(gate),
            replicator,
            peers: config.raft_peers,
            raft_port: config.raft_port,
            shutdown: Arc::new(tokio::sync::Notify::new()),
            graph,
            applied_index: Arc::new(std::sync::atomic::AtomicU64::new(initial_applied)),
            applied_index_path,
            is_leader: config.is_leader,
        }
    }

    /// Start the Raft background task.
    ///
    /// This spawns:
    /// 1. A TCP listener on `raft_port` for incoming peer AppendEntries RPCs
    /// 2. A periodic replication loop that pushes local WAL entries to followers
    ///
    /// Returns immediately after spawning the background tasks.
    pub async fn start(&self) -> Result<(), anyhow::Error> {
        // Register all known peers as followers
        for peer in &self.peers {
            self.replicator.register_follower(peer, 0, 0).await;
        }

        // Start the Raft RPC listener for incoming peer connections
        let replicator = self.replicator.clone();
        let graph = self.graph.clone();
        let shutdown = self.shutdown.clone();
        let raft_port = self.raft_port;
        let peers_for_bg = self.peers.clone();
        let rpc_timeout = self.replicator.config.rpc_timeout;
        let raft_node_id = self.replicator.config.node_id.clone();
        let log_node_id = raft_node_id.clone();
        let is_leader_for_bg = self.is_leader;

        // 1. TCP listener for incoming Raft RPCs (peer → this node)
        let listen_addr = format!("0.0.0.0:{}", raft_port);
        let listener = tokio::net::TcpListener::bind(&listen_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to bind Raft port {}: {}", raft_port, e))?;

        let rpc_graph = graph.clone();
        let rpc_shutdown = shutdown.clone();
        let rpc_applied_index = self.applied_index.clone();
        let rpc_applied_path = self.applied_index_path.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                let g = rpc_graph.clone();
                                let applied = rpc_applied_index.clone();
                                let applied_path = rpc_applied_path.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_raft_rpc(stream, g, applied, applied_path).await {
                                        tracing::trace!(%peer_addr, error=%e, "Raft RPC error");
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::error!("Raft listener error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = rpc_shutdown.notified() => {
                        tracing::debug!("Raft RPC listener shutting down");
                        break;
                    }
                }
            }
        });

        // 2. Periodic replication to followers
        tokio::spawn(async move {
            let interval = Duration::from_millis(500);
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Only the leader should replicate to followers. Non-leaders skip
                        // the replication loop to avoid sending stale/conflicting updates.
                        if !is_leader_for_bg {
                            continue;
                        }

                        // Build a simple TCP-based replication target for each peer
                        for peer in &peers_for_bg {
                            let target = TcpRaftTarget::new(
                                peer.clone(),
                                rpc_timeout,
                            );
                            let result = replicator.replicate_to(&target, peer).await;
                            if let Err(ref e) = result {
                                if !matches!(e, ReplicationError::Connection(_)) {
                                    tracing::warn!(
                                        node = %raft_node_id,
                                        peer = %peer,
                                        error = %e,
                                        "Raft replication failed"
                                    );
                                }
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        tracing::debug!("Raft replication loop shutting down");
                        break;
                    }
                }
            }
        });

        tracing::info!(
            port = raft_port,
            peers = ?self.peers,
            node_id = %log_node_id,
            "Raft consensus started"
        );

        Ok(())
    }

    /// Get the commit gate — used by NodeTask to await quorum before memory commit.
    #[allow(dead_code)]
    pub fn commit_gate(&self) -> &Arc<CommitGate> {
        &self.commit_gate
    }

    /// Get the replicator for metrics/debug access.
    pub fn replicator(&self) -> &Arc<RaftLogReplicator> {
        &self.replicator
    }

    /// Shutdown the Raft handler (background tasks).
    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
        tracing::info!("Raft handler shut down");
    }
}

/// A simple TCP-based ReplicationTarget that sends Raft RPCs over TCP.
///
/// Uses length-prefixed JSON framing matching the nexora-zenoh transport protocol.
#[allow(dead_code)]
struct TcpRaftTarget {
    peer_addr: String,
    timeout: Duration,
}

impl TcpRaftTarget {
    fn new(peer_addr: String, timeout: Duration) -> Self {
        Self { peer_addr, timeout }
    }
}

#[async_trait::async_trait]
impl ReplicationTarget for TcpRaftTarget {
    async fn append_entries(
        &self,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<AppendEntriesResponse, ReplicationError> {
        // Serialize the request as JSON
        let request = serde_json::json!({
            "type": "AppendEntries",
            "entries": entries.iter().map(|e| serde_json::json!({
                "seq_no": e.seq_no,
                "shard_id": e.shard_id,
                "payload": hex::encode(&e.payload),
                "epoch": e.epoch,
                "term": e.term,
            })).collect::<Vec<_>>(),
            "leader_commit": leader_commit,
        });

        let payload = serde_json::to_vec(&request)
            .map_err(|e| ReplicationError::Serialization(e.to_string()))?;

        // Connect to the peer's Raft port using the address directly
        let stream = tokio::net::TcpStream::connect(&self.peer_addr)
            .await
            .map_err(|e| ReplicationError::Connection(e.to_string()))?;

        // Use TCP directly with the same framing as nexora-zenoh
        send_raft_request(stream, &payload)
            .await
            .map_err(ReplicationError::Connection)
    }

    async fn install_snapshot(
        &self,
        shard_id: usize,
        from_seq: u64,
    ) -> Result<SnapshotData, ReplicationError> {
        // Snapshot transfer is not yet implemented over TCP.
        // For now, return an empty snapshot.
        tracing::info!(
            shard_id = shard_id,
            from_seq = from_seq,
            "Snapshot requested but not implemented; returning empty"
        );
        Ok(SnapshotData {
            shard_id,
            last_seq_no: from_seq,
            entries: Vec::new(),
        })
    }
}

/// Send a framed Raft request over a TCP stream and parse the response.
async fn send_raft_request(
    stream: tokio::net::TcpStream,
    payload: &[u8],
) -> Result<AppendEntriesResponse, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = stream;

    // Write: 4B BE length + payload
    stream
        .write_u32(payload.len() as u32)
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(payload).await.map_err(|e| e.to_string())?;
    stream.flush().await.map_err(|e| e.to_string())?;

    // Read response: 4B BE length + JSON
    let resp_len = stream.read_u32().await.map_err(|e| e.to_string())? as usize;
    if resp_len > 65536 {
        return Err("response too large".to_string());
    }
    let mut buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| e.to_string())?;

    let resp: AppendEntriesResponse =
        serde_json::from_slice(&buf).map_err(|e| format!("invalid Raft response: {}", e))?;

    Ok(resp)
}

/// Apply a single Raft log entry to the local graph service.
///
/// Errors are propagated so the caller can refuse to acknowledge entries that
/// did not actually apply — acknowledging a failed apply would let the leader
/// advance its commit index over state the follower never persisted, silently
/// diverging the replicas.
async fn apply_raft_entry(graph: &GraphService, entry: &LogEntry) -> Result<(), String> {
    use nexora_core::flatbuf_codec::{WAL_MAGIC_FB, WAL_MAGIC_JSON};
    use nexora_core::wal::{WalOperation, WalRecord};

    // Detect the payload format from its leading bytes rather than assuming a
    // fixed magic. WalRecords serialized as JSON begin with '{' (0x7B); v2
    // FlatBuffer payloads do not. Passing the wrong magic would route every
    // entry to the wrong decoder and fail (or misparse) all replication.
    let magic = if entry.payload.first() == Some(&0x7B) {
        WAL_MAGIC_JSON
    } else {
        WAL_MAGIC_FB
    };

    // Attempt to decode the payload as a WalRecord (FlatBuffer or JSON format).
    let record: WalRecord =
        nexora_core::flatbuf_codec::decode_wal_record_auto(&magic, &entry.payload, 0)
            .map_err(|e| format!("payload decode: {e}"))?;

    match record.operation {
        WalOperation::NodeEvent { qid, event } => {
            use nexora_core::event::NodeChangeEvent;
            match event.event {
                NodeChangeEvent::PropertySet { key, value } => {
                    graph
                        .set_property(&qid, key.as_str(), value)
                        .await
                        .map_err(|e| format!("set_property: {e}"))?;
                }
                NodeChangeEvent::PropertyRemoved { key, .. } => {
                    graph
                        .remove_property(&qid, key.as_str())
                        .await
                        .map_err(|e| format!("remove_property: {e}"))?;
                }
                NodeChangeEvent::EdgeAdded { edge } => {
                    graph
                        .add_edge(&qid, edge)
                        .await
                        .map_err(|e| format!("add_edge: {e}"))?;
                }
                NodeChangeEvent::EdgeRemoved { edge } => {
                    graph
                        .remove_edge(&qid, edge)
                        .await
                        .map_err(|e| format!("remove_edge: {e}"))?;
                }
                // P0.1: New event types — no-op during replay
                NodeChangeEvent::LabelAdded { .. }
                | NodeChangeEvent::LabelRemoved { .. }
                | NodeChangeEvent::EdgePropertySet { .. }
                | NodeChangeEvent::EdgePropertyRemoved { .. }
                | NodeChangeEvent::NodeDeleted { .. }
                | NodeChangeEvent::NodeRestored => {
                    // Raft replay of label, edge-property, and lifecycle events
                    // is handled at a higher layer after the P0.1 migration
                    // completes. These events are no-ops in the current Raft path.
                }
            }
        }
        WalOperation::SnapshotCheckpoint { .. } => {
            // Checkpoints are informational — no state change needed.
        }
        WalOperation::IngestOffsetCommit { .. } => {
            // Offset commits are informational — no state change needed.
        }
        _ => {
            // NodeEvents, DomainIndexEvent and any future variants are
            // acknowledged but not replayed (requires full graph integration).
            tracing::trace!(op = ?record.operation, "Raft entry acknowledged, not replayed");
        }
    }

    Ok(())
}

/// Handle an incoming Raft RPC from a peer.
///
/// `applied_index` tracks the highest seq_no this follower has durably applied.
/// It is used for two safety properties:
/// 1. **Deduplication / idempotency** — entries at or below `applied_index` were
///    already applied (the leader routinely re-sends on timeout); re-applying a
///    non-idempotent op would corrupt state, so they are skipped.
/// 2. **Fail-closed acknowledgement** — the response's `last_committed` reflects
///    what was *actually* applied contiguously, never a blind echo of
///    `leader_commit`. If an apply fails, we stop and report the last good seq_no
///    so the leader retries from there instead of advancing over missing state.
async fn handle_raft_rpc(
    mut stream: tokio::net::TcpStream,
    graph: Arc<GraphService>,
    applied_index: Arc<std::sync::atomic::AtomicU64>,
    applied_index_path: Option<Arc<std::path::PathBuf>>,
) -> Result<(), String> {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Read: 4B BE length + JSON request
    let len = stream.read_u32().await.map_err(|e| e.to_string())? as usize;
    if len > 65536 {
        return Err("request too large".to_string());
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| e.to_string())?;

    let request: serde_json::Value =
        serde_json::from_slice(&buf).map_err(|e| format!("invalid Raft request: {}", e))?;

    let req_type = request["type"].as_str().unwrap_or("unknown");

    let response = match req_type {
        "AppendEntries" => {
            // Parse entries from leader
            let mut entries: Vec<LogEntry> = match request["entries"].as_array() {
                Some(arr) => arr
                    .iter()
                    .map(|e| LogEntry {
                        seq_no: e["seq_no"].as_u64().unwrap_or(0),
                        shard_id: e["shard_id"].as_u64().unwrap_or(0) as usize,
                        payload: hex::decode(e["payload"].as_str().unwrap_or(""))
                            .unwrap_or_default(),
                        epoch: e["epoch"].as_u64().unwrap_or(0),
                        term: e["term"].as_u64().unwrap_or(1),
                    })
                    .collect(),
                None => Vec::new(),
            };

            let leader_commit = request["leader_commit"].as_u64().unwrap_or(0);
            let last_seq = entries.iter().map(|e| e.seq_no).max().unwrap_or(0);

            // Apply committed entries in strict seq_no order. Entries may arrive
            // out of order across RPCs; sorting lets us maintain a contiguous
            // applied watermark and detect gaps.
            entries.sort_by_key(|e| e.seq_no);

            let mut committed = applied_index.load(Ordering::SeqCst);
            let mut applied = 0u64;
            let mut apply_error: Option<String> = None;

            for entry in &entries {
                if entry.seq_no > leader_commit {
                    break; // don't apply uncommitted entries (rest are higher too)
                }
                if entry.seq_no <= committed {
                    continue; // already applied — dedup, skip idempotently
                }
                // Refuse to skip a gap: applying seq N+2 while N+1 is missing
                // would silently diverge. Stop and let the leader resend.
                if entry.seq_no != committed + 1 {
                    apply_error = Some(format!(
                        "gap before seq {}: expected {}",
                        entry.seq_no,
                        committed + 1
                    ));
                    break;
                }
                match apply_raft_entry(&graph, entry).await {
                    Ok(()) => {
                        committed = entry.seq_no;
                        applied += 1;
                    }
                    Err(e) => {
                        // Fail closed: stop at the first failure and report only
                        // what was applied so the leader retries this entry.
                        apply_error = Some(format!("seq {}: {e}", entry.seq_no));
                        break;
                    }
                }
            }

            let prev = applied_index.swap(committed, Ordering::SeqCst);
            // Persist the watermark durably whenever it advanced, so a restart
            // resumes dedup from here rather than re-applying from seq 1.
            if committed > prev {
                if let Some(ref path) = applied_index_path {
                    persist_applied_index(path, committed);
                }
            }

            if let Some(ref err) = apply_error {
                tracing::warn!(
                    error = %err,
                    committed = committed,
                    leader_commit = leader_commit,
                    "Raft apply halted; acknowledging only contiguously applied entries"
                );
            } else if applied > 0 {
                tracing::debug!(
                    count = applied,
                    committed = committed,
                    "Applied committed Raft entries"
                );
            }

            // success is true only when every committed entry in this batch was
            // applied (no gap, no apply error). last_committed carries the honest
            // watermark either way so the leader can advance or retry correctly.
            AppendEntriesResponse {
                term: 1,
                success: apply_error.is_none(),
                last_committed: committed,
                last_log_seq: last_seq,
            }
        }
        _ => {
            tracing::warn!("Unknown Raft RPC type: {}", req_type);
            AppendEntriesResponse {
                term: 1,
                success: false,
                last_committed: 0,
                last_log_seq: 0,
            }
        }
    };

    let resp_bytes =
        serde_json::to_vec(&response).map_err(|e| format!("serialization error: {}", e))?;
    stream
        .write_u32(resp_bytes.len() as u32)
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(&resp_bytes)
        .await
        .map_err(|e| e.to_string())?;
    stream.flush().await.map_err(|e| e.to_string())?;

    Ok(())
}
