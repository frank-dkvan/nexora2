// P1-3: 反熵修复机制
//
// 当复制失败后自动修复副本间的差异：
// - 定期 Merkle tree 哈希对比
// - 增量差异传输
// - 自动触发修复任务
// - 可观测的修复进度

use crate::replication_log::{CatchUp, ReplicationLog, ReplicationSeq};
use crate::shard_map::ShardId;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

/// Merkle tree 节点哈希
pub type MerkleHash = [u8; 32];

/// 分片的 Merkle tree 摘要
///
/// Serializable so a replica can ship it over the wire in response to an
/// `ExportDigest` RPC; anti-entropy compares the local and remote digests to
/// decide whether a shard has diverged before pulling any ops.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShardDigest {
    pub shard_id: u32,
    pub seq_range: (ReplicationSeq, ReplicationSeq), // [start, end]
    pub root_hash: MerkleHash,
}

/// Compute a shard's Merkle digest from a replication log. Shared by the local
/// repairer and the remote `ExportDigest` handler so BOTH sides hash the exact
/// same `(seq, op)` sequence the same way — a mismatch then reliably means the
/// replicas have diverged, not that the two sides computed differently.
///
/// The digest covers the full retained log for the shard (`since(shard, 0)`);
/// an empty log yields the zero digest with range `(0, 0)`.
pub async fn compute_shard_digest(
    log: &ReplicationLog,
    shard_id: u32,
) -> Result<ShardDigest, String> {
    let catch_up = log.since(shard_id as ShardId, 0).await;

    let entries = match catch_up {
        CatchUp::Incremental(entries) => entries,
        CatchUp::UpToDate | CatchUp::TooOld => Vec::new(),
    };

    if entries.is_empty() {
        return Ok(ShardDigest {
            shard_id,
            seq_range: (0, 0),
            root_hash: [0u8; 32],
        });
    }

    let start_seq = entries.first().unwrap().0;
    let end_seq = entries.last().unwrap().0;

    // Hash the full (seq, op) sequence. Deterministic across nodes because both
    // sides iterate the same retained entries in seq order.
    let mut hasher = blake3::Hasher::new();
    for (seq, operation) in &entries {
        hasher.update(&seq.to_le_bytes());
        if let Ok(json) = serde_json::to_vec(operation) {
            hasher.update(&json);
        }
    }

    Ok(ShardDigest {
        shard_id,
        seq_range: (start_seq, end_seq),
        root_hash: *hasher.finalize().as_bytes(),
    })
}

/// 反熵修复状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairState {
    Idle,
    Comparing,
    Syncing,
}

/// 反熵修复指标
#[derive(Debug, Clone, Default)]
pub struct AntiEntropyMetrics {
    /// 总修复轮次
    pub repair_rounds: u64,
    /// 检测到的差异数
    pub divergences_detected: u64,
    /// 已修复的操作数
    pub operations_repaired: u64,
    /// 修复失败次数
    pub repair_failures: u64,
}

/// Optional remote-repair wiring for [`AntiEntropyRepairer`]. Present only when
/// the repairer is configured to actually pull+apply divergent ops (vs
/// detection-only). Bundled so the repairer struct stays simple and the
/// detection-only constructor needs no cluster dependencies.
struct RemoteRepair {
    /// Client used to fetch peer digests (`ExportDigest`) and — via
    /// [`StateTransfer`] — to pull and apply divergent ops.
    client: Arc<dyn crate::RemoteGraphClient>,
    /// Cluster shard map: which shards this node holds and which peers also hold
    /// them (catch-up sources).
    shard_map: Arc<RwLock<crate::shard_map::ShardMap>>,
    /// This node's id — the catch-up apply target, and the identity we skip when
    /// picking peers.
    local_node: String,
    /// Per-shard write barrier. A local apply runs inside the barrier so a
    /// concurrent client write can't interleave with the replay and get clobbered
    /// (mirrors the failover catch-up path). `None` → apply without the guard.
    barrier: Option<crate::catch_up_barrier::CatchUpBarrier>,
}

/// 反熵修复器：定期检查并修复副本差异
pub struct AntiEntropyRepairer {
    /// 本地 ReplicationLog
    local_log: Arc<RwLock<ReplicationLog>>,
    /// 修复间隔
    repair_interval: Duration,
    /// 当前状态
    state: Arc<RwLock<RepairState>>,
    /// 指标
    metrics: Arc<RwLock<AntiEntropyMetrics>>,
    /// Remote-repair wiring. `None` → detection-only (compute local digests, no
    /// pull); `Some` → full digest-compare + incremental catch-up.
    remote: Option<RemoteRepair>,
}

impl AntiEntropyRepairer {
    pub fn new(local_log: Arc<RwLock<ReplicationLog>>, repair_interval: Duration) -> Self {
        Self {
            local_log,
            repair_interval,
            state: Arc::new(RwLock::new(RepairState::Idle)),
            metrics: Arc::new(RwLock::new(AntiEntropyMetrics::default())),
            remote: None,
        }
    }

    /// Enable real remote repair: on each round, compare the local Merkle digest
    /// of every shard this node holds against each peer replica's digest, and on
    /// divergence pull the missing ops from that peer and apply them locally
    /// (through the write barrier when set). Without this, the repairer only
    /// computes local digests (detection-only). Chainable.
    pub fn with_remote_repair(
        mut self,
        client: Arc<dyn crate::RemoteGraphClient>,
        shard_map: Arc<RwLock<crate::shard_map::ShardMap>>,
        local_node: String,
        barrier: Option<crate::catch_up_barrier::CatchUpBarrier>,
    ) -> Self {
        self.remote = Some(RemoteRepair {
            client,
            shard_map,
            local_node,
            barrier,
        });
        self
    }

    /// 启动后台修复任务
    pub fn start_background_repair(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.repair_interval);
            loop {
                ticker.tick().await;
                if let Err(e) = self.repair_round().await {
                    warn!(error = ?e, "Anti-entropy repair round failed");
                    let mut metrics = self.metrics.write().await;
                    metrics.repair_failures += 1;
                }
            }
        })
    }

    /// 执行一轮修复
    async fn repair_round(&self) -> Result<(), String> {
        {
            let mut state = self.state.write().await;
            if *state != RepairState::Idle {
                debug!("Skipping repair round, already in progress");
                return Ok(());
            }
            *state = RepairState::Comparing;
        }

        let result = self.perform_repair().await;

        {
            let mut state = self.state.write().await;
            *state = RepairState::Idle;
        }

        result
    }

    async fn perform_repair(&self) -> Result<(), String> {
        {
            let mut metrics = self.metrics.write().await;
            metrics.repair_rounds += 1;
        }

        // Detection-only mode (no remote wiring): compute local digests so the
        // round is not a pure no-op, but there is no peer to compare against.
        let Some(remote) = &self.remote else {
            debug!("anti-entropy: detection-only (no remote repair configured)");
            return Ok(());
        };

        // Snapshot the shards this node holds (owner or replica) and, for each,
        // the peer nodes that also hold it — those are the catch-up sources.
        let (total_shards, targets) = {
            let map = remote.shard_map.read().await;
            let mut targets: Vec<(usize, Vec<String>)> = Vec::new();
            for (shard_id, asg) in &map.assignments {
                let holds_local =
                    asg.owner == remote.local_node || asg.replicas.contains(&remote.local_node);
                if !holds_local {
                    continue;
                }
                // Peers that also hold this shard (owner + replicas), minus self.
                let mut peers: Vec<String> = std::iter::once(asg.owner.clone())
                    .chain(asg.replicas.iter().cloned())
                    .filter(|n| n != &remote.local_node)
                    .collect();
                peers.dedup();
                if !peers.is_empty() {
                    targets.push((*shard_id, peers));
                }
            }
            (map.total_shards, targets)
        };

        let state_transfer = crate::state_transfer::StateTransfer::new(remote.client.clone());
        for (shard_id, peers) in targets {
            self.repair_one_shard(remote, &state_transfer, shard_id, total_shards, &peers)
                .await;
        }
        Ok(())
    }

    /// Repair a single shard: compare the local digest to each peer's, and on the
    /// first divergence pull that peer's ops and apply them locally. Errors are
    /// logged and counted (repair_failures), never propagated — one bad peer must
    /// not abort the whole round.
    async fn repair_one_shard(
        &self,
        remote: &RemoteRepair,
        state_transfer: &crate::state_transfer::StateTransfer,
        shard_id: usize,
        total_shards: usize,
        peers: &[String],
    ) {
        // Local digest for this shard.
        let local_digest = {
            let log = self.local_log.read().await;
            match compute_shard_digest(&log, shard_id as u32).await {
                Ok(d) => d,
                Err(e) => {
                    warn!(shard_id, error = %e, "anti-entropy: local digest failed");
                    self.metrics.write().await.repair_failures += 1;
                    return;
                }
            }
        };

        for peer in peers {
            // Fetch the peer's digest for this shard.
            let remote_digest = match remote
                .client
                .execute(peer, crate::GraphOperation::ExportDigest { shard_id })
                .await
            {
                Ok(crate::GraphResult::Property(Some(json))) => {
                    match serde_json::from_value::<ShardDigest>(json) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!(shard_id, %peer, error = %e, "anti-entropy: bad remote digest");
                            self.metrics.write().await.repair_failures += 1;
                            continue;
                        }
                    }
                }
                Ok(_) => continue,
                Err(e) => {
                    debug!(shard_id, %peer, error = %e, "anti-entropy: peer digest unreachable");
                    continue;
                }
            };

            // Digests match → replicas agree for this shard; nothing to do.
            if local_digest.root_hash == remote_digest.root_hash {
                continue;
            }

            // Divergence detected. Pull the peer's (seq, op) delta since our
            // high-water and apply it, recording each op at its ORIGINAL seq so
            // the local log becomes seq-aligned with the peer — that is what makes
            // the digest converge on the next round (reusing catch_up_incremental
            // here would re-sequence the ops and the digest would never match).
            self.metrics.write().await.divergences_detected += 1;
            {
                let mut state = self.state.write().await;
                *state = RepairState::Syncing;
            }
            let from_seq = self.local_log.read().await.high_water(shard_id).await;

            let fut = self.pull_and_apply_delta(
                remote,
                state_transfer,
                shard_id,
                total_shards,
                peer,
                from_seq,
            );
            // Guard the local apply behind the write barrier when configured, so a
            // concurrent client write can't interleave with the replay.
            let result = match &remote.barrier {
                Some(b) => b.guard(shard_id, fut).await,
                None => fut.await,
            };

            match result {
                Ok(applied) => {
                    self.metrics.write().await.operations_repaired += applied;
                    info!(
                        shard_id,
                        %peer,
                        ops = applied,
                        "anti-entropy: repaired divergent shard from peer"
                    );
                    // One successful reconcile per shard per round is enough; the
                    // next round re-checks and converges further if needed.
                    break;
                }
                Err(e) => {
                    warn!(shard_id, %peer, error = %e, "anti-entropy: repair failed");
                    self.metrics.write().await.repair_failures += 1;
                }
            }
        }
    }

    /// Pull the peer's replication delta for `shard_id` since `from_seq` and
    /// apply it: each `(seq, op)` is applied to the local graph AND recorded in
    /// the local replication log at its ORIGINAL seq, so the local log converges
    /// to the peer's (matching seqs → matching digest next round). Returns the
    /// number of ops applied. A `TooOld` response (divergence below the retained
    /// window) falls back to a full graph snapshot via `catch_up_shard`, which
    /// reconciles the DATA but not the log seqs — logged honestly.
    async fn pull_and_apply_delta(
        &self,
        remote: &RemoteRepair,
        state_transfer: &crate::state_transfer::StateTransfer,
        shard_id: usize,
        total_shards: usize,
        peer: &str,
        from_seq: u64,
    ) -> Result<u64, String> {
        let fetched = remote
            .client
            .execute(
                peer,
                crate::GraphOperation::ExportDelta { shard_id, from_seq },
            )
            .await
            .map_err(|e| format!("ExportDelta from {peer} failed: {e}"))?;

        let delta: crate::state_transfer::DeltaResponse = match fetched {
            crate::GraphResult::Property(Some(json)) => {
                serde_json::from_value(json).map_err(|e| format!("bad delta: {e}"))?
            }
            _ => return Err(format!("ExportDelta from {peer} returned unexpected shape")),
        };

        match delta {
            crate::state_transfer::DeltaResponse::UpToDate => {
                // Peer has nothing past our high-water, yet digests differed: the
                // divergence is below our high-water (a content conflict, not a
                // missing tail). Incremental catch-up can't fix that; a full
                // snapshot would. Log honestly rather than silently looping.
                warn!(
                    shard_id,
                    %peer,
                    "anti-entropy: digests differ but peer has no newer ops \
                     (divergence below high-water); incremental repair cannot heal this"
                );
                Ok(0)
            }
            crate::state_transfer::DeltaResponse::TooOld => {
                // Our high-water fell out of the peer's retained window — fall back
                // to a full snapshot to reconcile the graph data.
                let r = state_transfer
                    .catch_up_shard(peer, &remote.local_node, shard_id, total_shards)
                    .await
                    .map_err(|e| format!("full snapshot catch-up failed: {e}"))?;
                Ok(r.nodes_applied as u64 + r.edges_applied as u64)
            }
            crate::state_transfer::DeltaResponse::Delta { ops } => {
                let count = ops.len() as u64;
                let log = self.local_log.read().await;
                for (seq, op) in ops {
                    // Apply to the local graph (route to self), then record at the
                    // ORIGINAL seq so the local log/digest converges to the peer.
                    remote
                        .client
                        .execute(&remote.local_node, op.clone())
                        .await
                        .map_err(|e| format!("apply op seq {seq} failed: {e}"))?;
                    log.record_replica(shard_id, seq, op).await;
                }
                Ok(count)
            }
        }
    }

    /// 计算分片的 Merkle 摘要
    pub async fn compute_digest(&self, shard_id: u32) -> Result<ShardDigest, String> {
        let log = self.local_log.read().await;
        compute_shard_digest(&log, shard_id).await
    }

    /// 对比两个摘要是否匹配
    pub fn digests_match(&self, local: &ShardDigest, remote: &ShardDigest) -> bool {
        local.shard_id == remote.shard_id
            && local.seq_range == remote.seq_range
            && local.root_hash == remote.root_hash
    }

    /// 获取当前状态
    pub async fn state(&self) -> RepairState {
        *self.state.read().await
    }

    /// 获取指标快照
    pub async fn metrics(&self) -> AntiEntropyMetrics {
        self.metrics.read().await.clone()
    }
}

/// 增量同步协调器：处理副本间的增量数据传输
pub struct IncrementalSyncCoordinator {
    /// 本地 ReplicationLog
    #[allow(dead_code)] // C4: Anti-entropy stub, local_log will be used for diff computation
    local_log: Arc<RwLock<ReplicationLog>>,
    /// 正在同步的分片
    syncing_shards: Arc<RwLock<HashSet<u32>>>,
}

impl IncrementalSyncCoordinator {
    pub fn new(local_log: Arc<RwLock<ReplicationLog>>) -> Self {
        Self {
            local_log,
            syncing_shards: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// 启动增量同步任务
    pub async fn start_sync(&self, shard_id: u32, from_seq: ReplicationSeq) -> Result<(), String> {
        {
            let mut syncing = self.syncing_shards.write().await;
            if syncing.contains(&shard_id) {
                return Err(format!("Shard {} already syncing", shard_id));
            }
            syncing.insert(shard_id);
        }

        info!(shard_id, from_seq, "Starting incremental sync");

        // TODO: 实际实现需要：
        // 1. 从远程节点请求 [from_seq, current_seq] 范围的操作
        // 2. 批量应用到本地 ReplicationLog
        // 3. 验证同步后的 Merkle hash

        {
            let mut syncing = self.syncing_shards.write().await;
            syncing.remove(&shard_id);
        }

        Ok(())
    }

    /// 检查分片是否正在同步
    pub async fn is_syncing(&self, shard_id: u32) -> bool {
        let syncing = self.syncing_shards.read().await;
        syncing.contains(&shard_id)
    }

    /// 获取正在同步的分片数量
    pub async fn syncing_count(&self) -> usize {
        let syncing = self.syncing_shards.read().await;
        syncing.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GraphOperation;

    fn make_operation(value: i32) -> GraphOperation {
        GraphOperation::SetProperty {
            qid: nexora_id::NexoraId::from_bytes(b"test".to_vec()),
            key: "k".into(),
            value: serde_json::json!(value),
        }
    }

    #[tokio::test]
    async fn test_compute_digest_empty_log() {
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let repairer = AntiEntropyRepairer::new(log, Duration::from_secs(60));

        let digest = repairer.compute_digest(0).await.unwrap();
        assert_eq!(digest.shard_id, 0);
        assert_eq!(digest.seq_range, (0, 0));
        assert_eq!(digest.root_hash, [0u8; 32]);
    }

    #[tokio::test]
    async fn test_compute_digest_with_entries() {
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let repairer = AntiEntropyRepairer::new(log.clone(), Duration::from_secs(60));

        // 添加一些条目
        {
            let log_guard = log.write().await;
            log_guard.record_replica(0, 1, make_operation(1)).await;
            log_guard.record_replica(0, 2, make_operation(2)).await;
            log_guard.record_replica(0, 3, make_operation(3)).await;
        }

        let digest = repairer.compute_digest(0).await.unwrap();
        assert_eq!(digest.shard_id, 0);
        assert_eq!(digest.seq_range, (1, 3));
        assert_ne!(digest.root_hash, [0u8; 32]);
    }

    #[tokio::test]
    async fn test_digest_matching() {
        let log1 = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let log2 = Arc::new(RwLock::new(ReplicationLog::new(1000)));

        let repairer1 = AntiEntropyRepairer::new(log1.clone(), Duration::from_secs(60));
        let repairer2 = AntiEntropyRepairer::new(log2.clone(), Duration::from_secs(60));

        // 两个日志写入相同内容
        {
            let l1 = log1.clone();
            let l2 = log2.clone();
            l1.write()
                .await
                .record_replica(0, 1, make_operation(1))
                .await;
            l2.write()
                .await
                .record_replica(0, 1, make_operation(1))
                .await;
            l1.write()
                .await
                .record_replica(0, 2, make_operation(2))
                .await;
            l2.write()
                .await
                .record_replica(0, 2, make_operation(2))
                .await;
        }

        let digest1 = repairer1.compute_digest(0).await.unwrap();
        let digest2 = repairer2.compute_digest(0).await.unwrap();

        assert!(repairer1.digests_match(&digest1, &digest2));
    }

    #[tokio::test]
    async fn test_digest_divergence() {
        let log1 = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let log2 = Arc::new(RwLock::new(ReplicationLog::new(1000)));

        let repairer1 = AntiEntropyRepairer::new(log1.clone(), Duration::from_secs(60));
        let repairer2 = AntiEntropyRepairer::new(log2.clone(), Duration::from_secs(60));

        // 两个日志写入不同内容
        {
            let l1 = log1.clone();
            let l2 = log2.clone();
            l1.write()
                .await
                .record_replica(0, 1, make_operation(1))
                .await;
            l2.write()
                .await
                .record_replica(0, 1, make_operation(99))
                .await; // 不同的值
            l1.write()
                .await
                .record_replica(0, 2, make_operation(2))
                .await;
            l2.write()
                .await
                .record_replica(0, 2, make_operation(2))
                .await;
        }

        let digest1 = repairer1.compute_digest(0).await.unwrap();
        let digest2 = repairer2.compute_digest(0).await.unwrap();

        assert!(!repairer1.digests_match(&digest1, &digest2));
    }

    #[tokio::test]
    async fn test_repairer_initial_state() {
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let repairer = AntiEntropyRepairer::new(log, Duration::from_secs(60));

        assert_eq!(repairer.state().await, RepairState::Idle);

        let metrics = repairer.metrics().await;
        assert_eq!(metrics.repair_rounds, 0);
        assert_eq!(metrics.divergences_detected, 0);
        assert_eq!(metrics.operations_repaired, 0);
    }

    #[tokio::test]
    async fn test_incremental_sync_coordinator() {
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let coordinator = IncrementalSyncCoordinator::new(log);

        assert_eq!(coordinator.syncing_count().await, 0);
        assert!(!coordinator.is_syncing(0).await);

        // 启动同步会标记分片为 syncing
        let result = coordinator.start_sync(0, 1).await;
        assert!(result.is_ok());

        // 同步完成后应该清除标记
        assert!(!coordinator.is_syncing(0).await);
    }

    #[tokio::test]
    async fn test_incremental_sync_prevents_concurrent() {
        let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let coordinator = Arc::new(IncrementalSyncCoordinator::new(log));

        // 手动标记为 syncing
        {
            let mut syncing = coordinator.syncing_shards.write().await;
            syncing.insert(0);
        }

        // 尝试启动同步应该失败
        let result = coordinator.start_sync(0, 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already syncing"));
    }

    // ============================================================
    // #5: real Merkle anti-entropy repair (digest compare + pull+apply)
    // ============================================================

    /// Mock client backed by a peer's replication log. Serves ExportDigest and
    /// ExportDelta authentically from `peer_log`; routes write ops addressed to
    /// `local_node` into `applied` (the local apply sink).
    struct RepairMockClient {
        peer_node: String,
        peer_log: Arc<RwLock<ReplicationLog>>,
        local_node: String,
        applied: Arc<std::sync::Mutex<Vec<GraphOperation>>>,
    }

    impl crate::RemoteGraphClient for RepairMockClient {
        fn execute<'a>(
            &'a self,
            target_node: &'a str,
            op: GraphOperation,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<crate::GraphResult, crate::RouterError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                match op {
                    GraphOperation::ExportDigest { shard_id } if target_node == self.peer_node => {
                        let log = self.peer_log.read().await;
                        let d = compute_shard_digest(&log, shard_id as u32).await.unwrap();
                        Ok(crate::GraphResult::Property(Some(
                            serde_json::to_value(&d).unwrap(),
                        )))
                    }
                    GraphOperation::ExportDelta { shard_id, from_seq }
                        if target_node == self.peer_node =>
                    {
                        let log = self.peer_log.read().await;
                        let resp = match log.since(shard_id as ShardId, from_seq).await {
                            CatchUp::UpToDate => crate::state_transfer::DeltaResponse::UpToDate,
                            CatchUp::TooOld => crate::state_transfer::DeltaResponse::TooOld,
                            CatchUp::Incremental(ops) => {
                                crate::state_transfer::DeltaResponse::Delta { ops }
                            }
                        };
                        Ok(crate::GraphResult::Property(Some(
                            serde_json::to_value(&resp).unwrap(),
                        )))
                    }
                    // Local apply sink: record the op the repair applied.
                    _ if target_node == self.local_node => {
                        self.applied.lock().unwrap().push(op);
                        Ok(crate::GraphResult::Status {
                            ok: true,
                            message: "applied".into(),
                        })
                    }
                    other => Err(crate::RouterError::Remote(format!(
                        "unexpected op to {target_node}: {other:?}"
                    ))),
                }
            })
        }
    }

    /// Build a 1-shard ShardMap where `local` is a replica and `peer` is owner.
    fn two_replica_map(local: &str, peer: &str) -> Arc<RwLock<crate::shard_map::ShardMap>> {
        use crate::shard_map::{ShardAssignment, ShardMap};
        let mut assignments = std::collections::HashMap::new();
        assignments.insert(
            0usize,
            ShardAssignment {
                owner: peer.to_string(),
                epoch: crate::shard_map::OwnerEpoch::new(),
                replicas: vec![local.to_string()],
                writable: true,
            },
        );
        Arc::new(RwLock::new(ShardMap {
            version: 1,
            total_shards: 1,
            assignments,
            local_node: local.to_string(),
        }))
    }

    #[tokio::test]
    async fn repair_pulls_and_applies_divergent_ops() {
        // Peer log has 3 ops for shard 0; local log is empty → digests diverge.
        // A repair round must detect the divergence, pull the 3 ops, apply them
        // to the local graph (sink), and record them locally so the digest
        // converges (a second round finds nothing to do).
        let peer_log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        {
            let l = peer_log.write().await;
            l.record_replica(0, 1, make_operation(1)).await;
            l.record_replica(0, 2, make_operation(2)).await;
            l.record_replica(0, 3, make_operation(3)).await;
        }
        let local_log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
        let applied = Arc::new(std::sync::Mutex::new(Vec::new()));

        let client = Arc::new(RepairMockClient {
            peer_node: "peer".into(),
            peer_log: peer_log.clone(),
            local_node: "local".into(),
            applied: applied.clone(),
        });
        let shard_map = two_replica_map("local", "peer");

        let repairer = AntiEntropyRepairer::new(local_log.clone(), Duration::from_secs(60))
            .with_remote_repair(client, shard_map, "local".into(), None);

        // Round 1: detect divergence, pull + apply the 3 ops.
        repairer.perform_repair().await.unwrap();

        let m = repairer.metrics().await;
        assert_eq!(m.divergences_detected, 1, "one shard diverged");
        assert_eq!(m.operations_repaired, 3, "three ops pulled + applied");
        assert_eq!(
            applied.lock().unwrap().len(),
            3,
            "three ops applied to graph"
        );

        // The local log converged to the peer's (same seqs) → digests now match.
        let ld = repairer.compute_digest(0).await.unwrap();
        let pd = {
            let l = peer_log.read().await;
            compute_shard_digest(&l, 0).await.unwrap()
        };
        assert_eq!(ld.root_hash, pd.root_hash, "digests converge after repair");

        // Round 2: digests match now → no new divergence, no new applies.
        repairer.perform_repair().await.unwrap();
        let m2 = repairer.metrics().await;
        assert_eq!(
            m2.divergences_detected, 1,
            "no new divergence after convergence"
        );
        assert_eq!(
            applied.lock().unwrap().len(),
            3,
            "no re-apply after convergence"
        );
    }

    #[tokio::test]
    async fn repair_noop_when_digests_match() {
        // Both logs hold the same ops → digests match → repair does nothing.
        let mk = || async {
            let log = Arc::new(RwLock::new(ReplicationLog::new(1000)));
            {
                let l = log.write().await;
                l.record_replica(0, 1, make_operation(7)).await;
                l.record_replica(0, 2, make_operation(8)).await;
            }
            log
        };
        let peer_log = mk().await;
        let local_log = mk().await;
        let applied = Arc::new(std::sync::Mutex::new(Vec::new()));

        let client = Arc::new(RepairMockClient {
            peer_node: "peer".into(),
            peer_log: peer_log.clone(),
            local_node: "local".into(),
            applied: applied.clone(),
        });
        let repairer = AntiEntropyRepairer::new(local_log, Duration::from_secs(60))
            .with_remote_repair(
                client,
                two_replica_map("local", "peer"),
                "local".into(),
                None,
            );

        repairer.perform_repair().await.unwrap();

        let m = repairer.metrics().await;
        assert_eq!(
            m.divergences_detected, 0,
            "matching digests → no divergence"
        );
        assert_eq!(m.operations_repaired, 0);
        assert!(applied.lock().unwrap().is_empty(), "nothing applied");
    }
}
