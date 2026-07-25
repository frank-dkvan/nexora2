//! Owner故障 failover 基础设施
//!
//! 当 shard owner 不可达时，从 follower 读取数据的机制。

use crate::catch_up_barrier::CatchUpBarrier;
use crate::replication_progress::{ReplicationProgress, SessionReadTracker};
use crate::router::HybridRouter;
use crate::shard_map::ShardMap;
use crate::state_transfer::StateTransfer;
use crate::{GraphOperation, GraphResult, RouterError};
use nexora_id::NexoraId;

/// C2: read consistency level, trading freshness for availability/latency.
///
/// The freshness SLA (P99 read-your-write, seconds-of-staleness tolerated under
/// load) maps to `Majority` as the default: strong enough to see any
/// quorum-committed write, cheap enough to skip the owner round-trip when a
/// caught-up replica answers. `Linearizable` pays the owner cost for the newest
/// value; `Local` accepts any replica (possibly stale) for lowest latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConcern {
    /// Any reachable replica may answer, even if stale. Lowest latency.
    Local,
    /// Serve from a replica that has reached the shard's quorum commit index
    /// (and, if a session seq is supplied, the session's last write). Tolerates
    /// up to N/2 replica failures. The default.
    #[default]
    Majority,
    /// Only the current owner answers — guaranteed newest, highest cost.
    Linearizable,
}

/// 尝试从 follower 读取数据（当 owner 不可达时的 failover 机制）
///
/// 策略：
/// 1. 首先尝试从 owner 读取
/// 2. 如果 owner 失败，按顺序尝试每个 follower
/// 3. 返回第一个成功的结果
///
/// 限制：当前实现是"尽力而为"的读取，不保证 follower 数据是最新的
/// （需要等待复制完成）。完整的解决方案需要：
/// - 读取时的 quorum 确认
/// - 版本向量/逻辑时钟跟踪复制进度
pub async fn read_with_failover(
    router: &HybridRouter,
    qid: &NexoraId,
    op: GraphOperation,
) -> Result<GraphResult, RouterError> {
    let shard_map = router.shard_map_snapshot().await;
    let shard = shard_map.shard_of(qid);

    let assignment = match shard_map.get(shard) {
        Some(a) => a,
        None => {
            return Err(RouterError::NodeNotFound(format!(
                "no assignment for shard {shard}"
            )))
        }
    };

    // 首先尝试从 owner 读取
    let client = match router.remote_client_arc() {
        Some(c) => c,
        None => {
            return Err(RouterError::NodeNotFound(
                "router has no remote client".into(),
            ))
        }
    };

    match client.execute(&assignment.owner, op.clone()).await {
        Ok(result) => {
            tracing::debug!(
                shard = shard,
                owner = %assignment.owner,
                "read from owner succeeded"
            );
            return Ok(result);
        }
        Err(e) => {
            tracing::warn!(
                shard = shard,
                owner = %assignment.owner,
                error = %e,
                "owner read failed, trying followers"
            );
        }
    }

    // Owner 失败，尝试 followers
    for (idx, follower) in assignment.replicas.iter().enumerate() {
        match client.execute(follower, op.clone()).await {
            Ok(result) => {
                tracing::info!(
                    shard = shard,
                    follower = %follower,
                    follower_idx = idx,
                    "failover read from follower succeeded"
                );
                return Ok(result);
            }
            Err(e) => {
                tracing::warn!(
                    shard = shard,
                    follower = %follower,
                    error = %e,
                    "follower read failed, trying next"
                );
            }
        }
    }

    Err(RouterError::NodeNotFound(format!(
        "all replicas (owner + {} followers) unreachable for shard {}",
        assignment.replicas.len(),
        shard
    )))
}

/// C2: read honoring a [`ReadConcern`], with the C1 session read-after-write
/// guard layered on top.
///
/// - `Linearizable`: owner only (newest value, no staleness).
/// - `Local`: owner first, then any reachable replica (may be stale). Same as
///   the legacy `read_with_failover` but session-guarded when `session` is set.
/// - `Majority` (default): prefer the owner; on owner failure, only fail over to
///   a replica that has reached the shard's quorum `commit_index` AND (if a
///   `session` is provided) applied the session's last-written seq for the
///   shard. A replica that is behind either bar is skipped, so a majority read
///   never serves data older than what a quorum already holds, and never
///   violates the caller's read-your-own-writes.
///
/// `progress`/`session` are optional: without them (single-node, or no tracking)
/// this degrades to owner-first failover, preserving current behavior.
pub async fn read_with_concern(
    router: &HybridRouter,
    qid: &NexoraId,
    op: GraphOperation,
    concern: ReadConcern,
    progress: Option<&ReplicationProgress>,
    session: Option<&SessionReadTracker>,
) -> Result<GraphResult, RouterError> {
    let shard_map = router.shard_map_snapshot().await;
    let shard = shard_map.shard_of(qid);
    let assignment = match shard_map.get(shard) {
        Some(a) => a,
        None => {
            return Err(RouterError::NodeNotFound(format!(
                "no assignment for shard {shard}"
            )))
        }
    };
    let client = match router.remote_client_arc() {
        Some(c) => c,
        None => {
            return Err(RouterError::NodeNotFound(
                "router has no remote client".into(),
            ))
        }
    };

    // The session's last-written seq for this shard (0 = no constraint).
    let required_seq = session.map(|s| s.required_seq(shard)).unwrap_or(0);

    // Try the owner first for every concern — it is always the freshest and
    // trivially satisfies the session guard (it took the write).
    match client.execute(&assignment.owner, op.clone()).await {
        Ok(result) => return Ok(result),
        Err(e) => {
            // Linearizable must not fall back to a replica: only the owner is
            // guaranteed newest. Surface the failure.
            if concern == ReadConcern::Linearizable {
                return Err(e);
            }
            tracing::warn!(
                shard = shard,
                owner = %assignment.owner,
                error = %e,
                concern = ?concern,
                "owner read failed; evaluating replica failover under read concern"
            );
        }
    }

    // Owner unreachable and concern allows a replica. Walk followers, applying
    // the concern's freshness gate.
    for follower in &assignment.replicas {
        // Majority: the follower must have reached the quorum commit index and
        // the session's required seq. Local: no freshness gate (any replica).
        if concern == ReadConcern::Majority {
            if let Some(prog) = progress {
                // Session read-after-write guard (C1): skip a follower behind the
                // caller's own last write.
                if required_seq > 0 && !prog.replica_caught_up(shard, follower, required_seq).await
                {
                    tracing::debug!(
                        shard, follower = %follower, required_seq,
                        "majority read skips follower behind session write"
                    );
                    continue;
                }
                // Quorum-committed-data guard: skip a follower that has not
                // applied up to the shard's commit index.
                if let Some(commit_index) = prog.get_commit_index(shard).await {
                    if commit_index > 0
                        && !prog.replica_caught_up(shard, follower, commit_index).await
                    {
                        tracing::debug!(
                            shard, follower = %follower, commit_index,
                            "majority read skips follower behind quorum commit index"
                        );
                        continue;
                    }
                }
            }
        }

        match client.execute(follower, op.clone()).await {
            Ok(result) => {
                tracing::info!(
                    shard, follower = %follower, concern = ?concern,
                    "read failover to replica succeeded under read concern"
                );
                return Ok(result);
            }
            Err(e) => {
                tracing::warn!(
                    shard, follower = %follower, error = %e,
                    "replica read failed, trying next"
                );
            }
        }
    }

    Err(RouterError::NodeNotFound(format!(
        "no replica satisfied {:?} read for shard {} (owner + {} followers unreachable or too stale)",
        concern,
        shard,
        assignment.replicas.len()
    )))
}

/// Owner 提升逻辑（将 follower 提升为新 owner）
///
/// 当检测到 owner 长期不可达时，协调者可以：
/// 1. 选择一个 follower（通常是复制最完整的）
/// 2. 递增 epoch
/// 3. 更新 shard map，将该 follower 提升为新 owner
/// 4. 广播新的 shard map
///
/// 这是一个占位实现，完整的 failover 需要：
/// - 分布式共识（Raft/Paxos）选举新 owner
/// - Lease 机制防止脑裂
/// - 自动检测 owner 健康状态
pub async fn promote_follower_to_owner(
    shard_map: &mut ShardMap,
    shard_id: usize,
    new_owner: String,
) -> Result<(), String> {
    let assignment = shard_map
        .assignments
        .get_mut(&shard_id)
        .ok_or_else(|| format!("shard {} not found in shard map", shard_id))?;

    if !assignment.replicas.contains(&new_owner) {
        return Err(format!(
            "new owner '{}' is not a replica of shard {}",
            new_owner, shard_id
        ));
    }

    let old_owner = assignment.owner.clone();
    assignment.epoch = assignment.epoch.next();
    assignment.owner = new_owner.clone();
    assignment.replicas.retain(|r| r != &new_owner);
    if !assignment.replicas.contains(&old_owner) {
        assignment.replicas.push(old_owner.clone());
    }

    shard_map.version += 1;

    tracing::info!(
        shard = shard_id,
        old_owner = %old_owner,
        new_owner = %new_owner,
        new_epoch = assignment.epoch.value(),
        shard_map_version = shard_map.version,
        "promoted follower to owner"
    );

    Ok(())
}

/// Failover with catch-up: the safe promotion path.
///
/// Before a follower becomes the new owner of `shard_id`, it must catch up to
/// the latest committed state from a surviving replica — otherwise a promotion
/// of a lagging follower silently loses writes the old owner had committed.
///
/// Sequence: **fence → catch-up → reopen → promote**
/// 1. Fence the shard on this node (CatchUpBarrier::begin) — reject writes.
/// 2. Catch up from `source_replica` (a surviving replica with the latest data).
/// 3. Reopen the shard (CatchUpBarrier::end) — allow writes again.
/// 4. Update the shard map to reflect the new owner + bumped epoch.
///
/// If catch-up fails, the shard stays fenced and the promotion is aborted —
/// this node does NOT become owner (fail-safe: no data loss from a partial
/// promotion). The caller should retry or select a different source.
#[allow(clippy::too_many_arguments)] // C1: Failover orchestration needs all context for correctness
pub async fn promote_with_catchup(
    shard_map: &mut ShardMap,
    barrier: &CatchUpBarrier,
    state_transfer: &StateTransfer,
    shard_id: usize,
    total_shards: usize,
    new_owner: String,
    apply_target: &str,   // 本节点的地址（catch-up 应用目标）
    source_replica: &str, // 存活的副本（catch-up 数据源）
    from_seq: u64,        // 本节点已追上的 seq（增量追赶起点）
) -> Result<(), String> {
    // Step 1: fence the shard — block writes during reconciliation
    barrier.begin(shard_id).await;

    // Step 2: catch up from surviving replica
    let catchup_result = state_transfer
        .catch_up_incremental(
            source_replica,
            apply_target,
            shard_id,
            total_shards,
            from_seq,
        )
        .await;

    match catchup_result {
        Ok(result) => {
            tracing::info!(
                shard = shard_id,
                source = %source_replica,
                ops_applied = result.ops_applied,
                incremental = result.incremental,
                "catch-up succeeded before promotion"
            );
        }
        Err(e) => {
            // Fail-safe: reopen (so a retry can re-fence) and abort promotion.
            // This node does NOT become owner — no partial/lossy promotion.
            barrier.end(shard_id).await;
            return Err(format!(
                "catch-up failed for shard {shard_id} from {source_replica}: {e}; \
                 promotion aborted (no data loss)"
            ));
        }
    }

    // Step 3: reopen the shard — reconciliation complete
    barrier.end(shard_id).await;

    // Step 4: update shard map (epoch bump + owner switch)
    promote_follower_to_owner(shard_map, shard_id, new_owner).await?;

    Ok(())
}

/// A1.2 TODO: Read-repair for detected stale replicas.
///
/// When a majority read detects that a replica is stale (behind commit_index or
/// session write seq), we could trigger an async read-repair to bring it up to
/// date. This improves eventual consistency and reduces future read misses.
///
/// Implementation deferred until A1.2 basic functionality is stable and verified.
///
/// # Arguments
/// * `router` - The router for sending repair operations
/// * `shard` - The shard ID with stale data
/// * `stale_replica` - The replica that needs repair
/// * `latest_seq` - The sequence number to repair up to
///
/// # Future implementation notes
/// - Should be async and non-blocking (fire-and-forget)
/// - Rate-limit repairs to avoid overwhelming stale replicas
/// - Consider batching multiple repairs for the same replica
/// - Log repair metrics for monitoring
#[allow(dead_code)]
async fn trigger_read_repair(
    _router: &HybridRouter,
    _shard: usize,
    _stale_replica: &str,
    _latest_seq: u64,
) {
    // Placeholder for future implementation
    tracing::warn!(
        shard = _shard,
        replica = _stale_replica,
        seq = _latest_seq,
        "read-repair not yet implemented (A1.2 TODO)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_map::ShardMap;
    use crate::tcp_transport::TcpRemoteClient;
    use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_read_with_failover_falls_back_to_follower() {
        // 创建一个本地 GraphService 作为 follower
        let graph = Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 100,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ));

        // 写入测试数据到 follower
        let qid = NexoraId::from_bytes(b"test-node".to_vec());
        use nexora_value::PropertyValue;
        graph
            .set_property(&qid, "name", PropertyValue::String("Alice".to_string()))
            .await
            .unwrap();

        // 构建 shard map，owner 指向一个不存在的地址（模拟故障）
        let mut shard_map = ShardMap::new_distributed(
            4,
            &["dead-owner".into(), "follower-1".into()],
            "this-node".into(),
        );

        // 启动一个 TCP server 作为 follower
        use crate::graph_service_adapter::GraphServiceAdapter;
        use crate::tcp_transport::TcpGraphServer;

        let adapter = Arc::new(GraphServiceAdapter::new(graph.clone()));
        let server = TcpGraphServer::new(adapter, "127.0.0.1:0".to_string());
        let follower_addr = server.start().await.unwrap();

        // 注册 follower 地址
        let client = Arc::new(TcpRemoteClient::new());
        client.register_node("follower-1", &follower_addr).await;
        client.register_node("dead-owner", "127.0.0.1:1").await; // 不存在的地址

        // 手动设置 shard assignment 使得 qid 的 shard 由 dead-owner 拥有
        let shard = shard_map.shard_of(&qid);
        if let Some(assignment) = shard_map.assignments.get_mut(&shard) {
            assignment.owner = "dead-owner".to_string();
            assignment.replicas = vec!["follower-1".to_string()];
        }

        let router = HybridRouter::new_clustered_no_local(shard_map, client);

        // 尝试读取 - 应该从 follower failover
        let op = GraphOperation::GetProperty {
            qid: qid.clone(),
            key: "name".to_string(),
        };

        let result = read_with_failover(&router, &qid, op).await;

        match result {
            Ok(GraphResult::Property(Some(value))) => {
                assert_eq!(value, serde_json::json!("Alice"));
            }
            other => panic!("expected Property(Some(Alice)), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_promote_follower_to_owner_updates_shard_map() {
        let mut shard_map = ShardMap::new_distributed_rf(
            4,
            &["node-a".into(), "node-b".into(), "node-c".into()],
            "node-a".into(),
            3,
        );

        let shard_id = 0;
        let original_assignment = shard_map.get(shard_id).unwrap().clone();
        let original_owner = original_assignment.owner.clone();
        let original_epoch = original_assignment.epoch;
        let original_version = shard_map.version;

        let new_owner = original_assignment.replicas[0].clone();

        let result = promote_follower_to_owner(&mut shard_map, shard_id, new_owner.clone()).await;
        assert!(result.is_ok());

        let new_assignment = shard_map.get(shard_id).unwrap();
        assert_eq!(new_assignment.owner, new_owner);
        assert_eq!(new_assignment.epoch, original_epoch.next());
        assert!(new_assignment.replicas.contains(&original_owner));
        assert!(!new_assignment.replicas.contains(&new_owner));
        assert_eq!(shard_map.version, original_version + 1);
    }

    #[tokio::test]
    async fn test_promote_non_replica_fails() {
        let mut shard_map = ShardMap::new_distributed_rf(
            4,
            &["node-a".into(), "node-b".into()],
            "node-a".into(),
            2,
        );

        let result = promote_follower_to_owner(&mut shard_map, 0, "node-z".into()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not a replica"));
    }

    /// Shared harness: a dead owner + one live follower holding `value`, wired so
    /// the qid's shard is owned by the dead node with the follower as replica.
    /// Returns (router, qid, shard).
    async fn dead_owner_live_follower(value: &str) -> (HybridRouter, NexoraId, usize) {
        use crate::graph_service_adapter::GraphServiceAdapter;
        use crate::tcp_transport::TcpGraphServer;
        use nexora_value::PropertyValue;

        let graph = Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 100,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ));
        let qid = NexoraId::from_bytes(b"c2-node".to_vec());
        graph
            .set_property(&qid, "name", PropertyValue::String(value.to_string()))
            .await
            .unwrap();

        let adapter = Arc::new(GraphServiceAdapter::new(graph.clone()));
        let server = TcpGraphServer::new(adapter, "127.0.0.1:0".to_string());
        let follower_addr = server.start().await.unwrap();
        // Leak the server so it keeps serving for the test's lifetime.
        Box::leak(Box::new(server));

        let client = Arc::new(TcpRemoteClient::new());
        client.register_node("follower-1", &follower_addr).await;
        client.register_node("dead-owner", "127.0.0.1:1").await;

        let mut shard_map = ShardMap::new_distributed(
            4,
            &["dead-owner".into(), "follower-1".into()],
            "this-node".into(),
        );
        let shard = shard_map.shard_of(&qid);
        if let Some(a) = shard_map.assignments.get_mut(&shard) {
            a.owner = "dead-owner".to_string();
            a.replicas = vec!["follower-1".to_string()];
        }
        (
            HybridRouter::new_clustered_no_local(shard_map, client),
            qid,
            shard,
        )
    }

    /// C2: Linearizable read does NOT fail over to a follower — a dead owner
    /// surfaces an error rather than a possibly-stale replica value.
    #[tokio::test]
    async fn test_linearizable_read_refuses_follower_failover() {
        let (router, qid, _shard) = dead_owner_live_follower("Alice").await;
        let op = GraphOperation::GetProperty {
            qid: qid.clone(),
            key: "name".into(),
        };
        let result =
            read_with_concern(&router, &qid, op, ReadConcern::Linearizable, None, None).await;
        assert!(
            result.is_err(),
            "linearizable must not serve a follower when the owner is down"
        );
    }

    /// C2: Local read falls over to the follower (no freshness gate).
    #[tokio::test]
    async fn test_local_read_fails_over_to_follower() {
        let (router, qid, _shard) = dead_owner_live_follower("Alice").await;
        let op = GraphOperation::GetProperty {
            qid: qid.clone(),
            key: "name".into(),
        };
        let result = read_with_concern(&router, &qid, op, ReadConcern::Local, None, None).await;
        match result {
            Ok(GraphResult::Property(Some(v))) => assert_eq!(v, serde_json::json!("Alice")),
            other => panic!("expected Property(Some(Alice)), got {other:?}"),
        }
    }

    /// C2: Majority read skips a follower that is behind the session's write, but
    /// admits it once caught up — read-your-own-writes across failover.
    #[tokio::test]
    async fn test_majority_read_respects_session_seq() {
        let (router, qid, shard) = dead_owner_live_follower("Alice").await;
        let progress = ReplicationProgress::new();
        progress
            .init_shard(shard, vec!["follower-1".to_string()])
            .await;
        progress.record_write(shard, 5).await;

        let session = SessionReadTracker::new();
        session.note_write(shard, 5); // session wrote up to seq 5

        let op = GraphOperation::GetProperty {
            qid: qid.clone(),
            key: "name".into(),
        };

        // Follower has only applied seq 3 → behind the session write → skipped →
        // no replica satisfies the majority read → error (not a stale read).
        progress.record_ack(shard, "follower-1", 3).await;
        let stale = read_with_concern(
            &router,
            &qid,
            op.clone(),
            ReadConcern::Majority,
            Some(&progress),
            Some(&session),
        )
        .await;
        assert!(
            stale.is_err(),
            "majority read must not serve a follower behind the session write"
        );

        // Follower catches up to seq 5 → now admitted.
        progress.record_ack(shard, "follower-1", 5).await;
        let fresh = read_with_concern(
            &router,
            &qid,
            op,
            ReadConcern::Majority,
            Some(&progress),
            Some(&session),
        )
        .await;
        match fresh {
            Ok(GraphResult::Property(Some(v))) => assert_eq!(v, serde_json::json!("Alice")),
            other => panic!("expected Property(Some(Alice)) after catch-up, got {other:?}"),
        }
    }

    /// A1.2: Majority read skips a follower behind the shard's quorum commit index.
    #[tokio::test]
    async fn test_majority_read_respects_commit_index() {
        let (router, qid, shard) = dead_owner_live_follower("Bob").await;
        let progress = ReplicationProgress::new();

        // Initialize with 2 replicas for proper quorum calculation
        progress
            .init_shard(
                shard,
                vec!["follower-1".to_string(), "follower-2".to_string()],
            )
            .await;

        // Owner writes seq 10
        progress.record_write(shard, 10).await;

        // follower-2 (not available) acks seq 10, creating quorum commit_index=10
        // follower-1 (available) only acked seq 7, so it's behind
        progress.record_ack(shard, "follower-2", 10).await;
        progress.record_ack(shard, "follower-1", 7).await;

        // With RF=2, quorum=2, so commit_index is min(10, 7) = 7
        // Actually, let's check: sorted [7, 10], quorum_size=2,
        // commit_index = acked_seqs[len - quorum_size] = acked_seqs[2-2] = acked_seqs[0] = 7
        let commit_index = progress.get_commit_index(shard).await.unwrap();
        assert_eq!(commit_index, 7);

        let op = GraphOperation::GetProperty {
            qid: qid.clone(),
            key: "name".into(),
        };

        // follower-1 at seq 7 should be allowed (it's at commit_index)
        let result = read_with_concern(
            &router,
            &qid,
            op.clone(),
            ReadConcern::Majority,
            Some(&progress),
            None,
        )
        .await;

        // Should succeed because follower-1 is at commit_index (7)
        assert!(
            result.is_ok(),
            "majority read should serve follower at commit_index"
        );

        // Now simulate a scenario where commit_index advances but follower-1 doesn't
        // follower-2 acks seq 15, now commit_index should advance
        progress.record_ack(shard, "follower-2", 15).await;
        progress.record_write(shard, 15).await;

        // commit_index should now be min(7, 15) = 7 still with RF=2
        // Wait, with RF=2, quorum=2, we need both to advance commit_index
        // Actually sorted [7, 15], quorum_size=2, commit_index = acked_seqs[0] = 7
        // So commit_index stays at 7 until follower-1 catches up

        // Let me create a scenario with RF=3 where commit_index can advance past follower-1
        progress
            .init_shard(
                shard,
                vec![
                    "follower-1".to_string(),
                    "follower-2".to_string(),
                    "follower-3".to_string(),
                ],
            )
            .await;

        progress.record_write(shard, 20).await;
        progress.record_ack(shard, "follower-1", 5).await; // Stale
        progress.record_ack(shard, "follower-2", 20).await;
        progress.record_ack(shard, "follower-3", 20).await;

        // RF=3, quorum=2, sorted [5, 20, 20], commit_index = acked_seqs[3-2] = acked_seqs[1] = 20
        let commit_index = progress.get_commit_index(shard).await.unwrap();
        assert_eq!(
            commit_index, 20,
            "commit_index should be 20 with 2/3 replicas at seq 20"
        );

        // Now majority read should skip follower-1 (at seq 5, behind commit_index 20)
        let result = read_with_concern(
            &router,
            &qid,
            op.clone(),
            ReadConcern::Majority,
            Some(&progress),
            None,
        )
        .await;

        // Should fail because the only available follower (follower-1) is behind commit_index
        assert!(
            result.is_err(),
            "majority read must not serve a follower behind commit_index (5 < 20)"
        );
    }

    /// A1.3: promote_with_catchup succeeds when catch-up works
    #[tokio::test]
    async fn test_promote_with_catchup_succeeds() {
        use crate::graph_service_adapter::GraphServiceAdapter;
        use crate::tcp_transport::{TcpGraphServer, TcpRemoteClient};
        use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
        use nexora_value::PropertyValue;
        use std::sync::Arc;

        // Create source replica (has the latest data)
        let source_graph = Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 100,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ));

        // Write data to source
        let qid = NexoraId::from_bytes(b"test-node-a13".to_vec());
        source_graph
            .set_property(&qid, "name", PropertyValue::String("Bob".to_string()))
            .await
            .unwrap();

        let source_adapter = Arc::new(GraphServiceAdapter::new(source_graph.clone()));
        let source_server = TcpGraphServer::new(source_adapter, "127.0.0.1:0".to_string());
        let source_addr = source_server.start().await.unwrap();
        Box::leak(Box::new(source_server));

        // Create apply target (the new owner, initially empty)
        let target_graph = Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 100,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ));

        let target_adapter = Arc::new(GraphServiceAdapter::new(target_graph.clone()));
        let target_server = TcpGraphServer::new(target_adapter, "127.0.0.1:0".to_string());
        let target_addr = target_server.start().await.unwrap();
        Box::leak(Box::new(target_server));

        // Set up client and state transfer
        let client = Arc::new(TcpRemoteClient::new());
        client.register_node("source-node", &source_addr).await;
        client.register_node("target-node", &target_addr).await;

        let state_transfer = StateTransfer::new(client.clone());
        let barrier = CatchUpBarrier::new();

        // Create shard map
        let mut shard_map = ShardMap::new_distributed(
            4,
            &[
                "old-owner".into(),
                "target-node".into(),
                "source-node".into(),
            ],
            "this-node".into(),
        );

        let shard_id = shard_map.shard_of(&qid);
        let original_version = shard_map.version;

        // Set initial owner
        if let Some(assignment) = shard_map.assignments.get_mut(&shard_id) {
            assignment.owner = "old-owner".to_string();
            assignment.replicas = vec!["target-node".to_string(), "source-node".to_string()];
        }
        let original_epoch = shard_map.get(shard_id).unwrap().epoch;

        // Verify barrier is not blocked initially
        assert!(!barrier.is_blocked(shard_id).await);

        // Promote with catch-up (from_seq=0 means full catch-up via incremental path)
        let result = promote_with_catchup(
            &mut shard_map,
            &barrier,
            &state_transfer,
            shard_id,
            4,
            "target-node".to_string(),
            "target-node",
            "source-node",
            0,
        )
        .await;

        assert!(
            result.is_ok(),
            "promote_with_catchup should succeed: {:?}",
            result
        );

        // Verify shard map updated
        let new_assignment = shard_map.get(shard_id).unwrap();
        assert_eq!(new_assignment.owner, "target-node");
        assert_eq!(new_assignment.epoch, original_epoch.next());
        assert_eq!(shard_map.version, original_version + 1);

        // Verify barrier is unblocked (reopened)
        assert!(!barrier.is_blocked(shard_id).await);

        // Verify data was transferred to target
        let retrieved = target_graph.get_property(&qid, "name").await.unwrap();
        assert_eq!(
            retrieved,
            Some(PropertyValue::String("Bob".to_string())),
            "target should have caught up with source's data"
        );
    }

    /// A1.3: promote_with_catchup aborts on unreachable source
    #[tokio::test]
    async fn test_promote_with_catchup_aborts_on_source_unreachable() {
        use crate::tcp_transport::TcpRemoteClient;
        use std::sync::Arc;

        let client = Arc::new(TcpRemoteClient::new());
        // Register dead address for source
        client.register_node("dead-source", "127.0.0.1:1").await;
        client.register_node("target-node", "127.0.0.1:2").await;

        let state_transfer = StateTransfer::new(client.clone());
        let barrier = CatchUpBarrier::new();

        let mut shard_map = ShardMap::new_distributed(
            4,
            &["old-owner".into(), "target-node".into()],
            "this-node".into(),
        );

        let shard_id = 0;
        if let Some(assignment) = shard_map.assignments.get_mut(&shard_id) {
            assignment.owner = "old-owner".to_string();
            assignment.replicas = vec!["target-node".to_string()];
        }

        let original_owner = shard_map.get(shard_id).unwrap().owner.clone();
        let original_epoch = shard_map.get(shard_id).unwrap().epoch;
        let original_version = shard_map.version;

        // Attempt promotion with dead source
        let result = promote_with_catchup(
            &mut shard_map,
            &barrier,
            &state_transfer,
            shard_id,
            4,
            "target-node".to_string(),
            "127.0.0.1:2",
            "127.0.0.1:1",
            0,
        )
        .await;

        // Verify it failed
        assert!(result.is_err(), "should fail when source is unreachable");
        let err_msg = result.unwrap_err();
        assert!(
            err_msg.contains("catch-up failed") && err_msg.contains("promotion aborted"),
            "error should indicate promotion was aborted: {err_msg}"
        );

        // Verify shard map unchanged (fail-safe: no partial promotion)
        let assignment = shard_map.get(shard_id).unwrap();
        assert_eq!(
            assignment.owner, original_owner,
            "owner should not change on failure"
        );
        assert_eq!(
            assignment.epoch, original_epoch,
            "epoch should not bump on failure"
        );
        assert_eq!(
            shard_map.version, original_version,
            "version should not bump on failure"
        );

        // Verify barrier was reopened (fail-safe)
        assert!(
            !barrier.is_blocked(shard_id).await,
            "barrier should be unblocked after failure"
        );
    }

    /// A1.3: barrier blocks writes during catch-up
    #[tokio::test]
    async fn test_barrier_blocks_writes_during_catchup() {
        let barrier = CatchUpBarrier::new();
        let shard_id = 5;

        // Initially unblocked
        assert!(!barrier.is_blocked(shard_id).await);

        // Begin blocks
        barrier.begin(shard_id).await;
        assert!(barrier.is_blocked(shard_id).await);

        // End unblocks
        barrier.end(shard_id).await;
        assert!(!barrier.is_blocked(shard_id).await);
    }
}
