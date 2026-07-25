//! Distributed write execution with RF>1 quorum replication support.
//!
//! This module extends the distributed write path to integrate ReplicaWriter,
//! enabling writes to be replicated to followers when RF>1 is configured.

use crate::distributed_query::{WritePlan, WRITE_STAT_COLUMNS};
use crate::replica_writer::ReplicaWriter;
use crate::replication::FencingToken;
use crate::router::HybridRouter;
use crate::{GraphOperation, GraphResult, RouterError};

/// Execute a distributed write with RF>1 quorum replication support.
///
/// When a ReplicaWriter is configured on the router:
/// 1. Each owner executes the write locally against its matched nodes
/// 2. For each affected shard, replicate the write to followers via quorum_write
/// 3. Sum write stats across all owners
///
/// Without a ReplicaWriter (RF=1 or replication not configured), falls back to
/// the original owner-only write path.
pub async fn execute_write_with_replication(
    router: &HybridRouter,
    plan: &WritePlan,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    let owners = distinct_owners(router).await;
    if owners.is_empty() {
        return Err(RouterError::NodeNotFound("no shard owners".into()));
    }

    // Check if replication is configured and snapshot shard map
    let replica_writer = router.replica_writer();
    let shard_map = router.shard_map_snapshot().await;

    let mut handles = Vec::with_capacity(owners.len());
    for owner in owners {
        let op = GraphOperation::ExecuteCypher {
            query: plan.owner_query.clone(),
        };
        let client = router.remote_client_arc();
        let replica_writer_clone = replica_writer.clone();
        let shard_map_clone = shard_map.clone();
        let owner_clone = owner.clone();

        handles.push(tokio::spawn(async move {
            let result = match client {
                Some(c) => c.execute(&owner_clone, op.clone()).await,
                None => Err(RouterError::NodeNotFound(
                    "router has no remote client".into(),
                )),
            };

            // If write succeeded and replication is configured, replicate to followers
            if let (Ok(_), Some(replica_writer)) = (&result, &replica_writer_clone) {
                if let Err(e) =
                    replicate_owner_write_static(&owner_clone, &shard_map_clone, replica_writer, op)
                        .await
                {
                    tracing::warn!(
                        owner = %owner_clone,
                        error = %e,
                        "RF>1 replication failed for distributed write; owner write committed"
                    );
                }
            }

            (owner_clone, result)
        }));
    }

    // Sum the 8 stat columns across owners
    let mut totals = [0i64; WRITE_STAT_COLUMNS.len()];
    for h in handles {
        let (owner, res) = h.await.map_err(|e| RouterError::Remote(e.to_string()))?;
        match res {
            Ok(GraphResult::CypherRows { rows, .. }) => {
                if let Some(row) = rows.first() {
                    for (i, cell) in row.iter().enumerate().take(totals.len()) {
                        totals[i] += cell.as_i64().unwrap_or(0);
                    }
                }
            }
            Ok(other) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} returned non-stat write result: {other:?}"
                )))
            }
            Err(e) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} write failed: {e} (refusing partial write)"
                )))
            }
        }
    }

    let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
    let row: Vec<serde_json::Value> = totals.iter().map(|n| serde_json::json!(n)).collect();
    Ok((columns, vec![row]))
}

/// Replicate an owner's write to its followers via ReplicaWriter (static version).
///
/// This is a best-effort replication after the owner write has committed. In the
/// current implementation, replication failure is logged but does not fail the
/// overall write (the honest gap documented in rf3_pgwire_write test).
///
/// TODO: Two-phase commit for atomic cross-replica writes would move replication
/// before the owner write, rolling back on quorum failure.
async fn replicate_owner_write_static(
    owner: &str,
    shard_map: &crate::shard_map::ShardMap,
    replica_writer: &ReplicaWriter,
    op: GraphOperation,
) -> Result<(), RouterError> {
    // Extract the affected shards from this owner's write. For now, we replicate
    // to all shards owned by this owner (conservative: over-replicates, but safe).
    // A future optimization would extract exact affected shards from the write.
    let owned_shards: Vec<usize> = shard_map
        .assignments
        .iter()
        .filter(|(_, a)| a.owner == owner)
        .map(|(shard_id, _)| *shard_id)
        .collect();

    for shard_id in owned_shards {
        let assignment = match shard_map.get(shard_id) {
            Some(a) => a,
            None => continue,
        };

        // Build a FencingToken for this shard's current epoch
        let token = FencingToken::new(shard_id, assignment.epoch);

        // Replicate the write to followers via quorum_write
        match replica_writer
            .quorum_write(shard_id, &token, op.clone())
            .await
        {
            Ok(status) => {
                tracing::debug!(
                    shard = shard_id,
                    owner = owner,
                    status = ?status,
                    "RF>1 replication succeeded for shard"
                );
            }
            Err(e) => {
                tracing::warn!(
                    shard = shard_id,
                    owner = owner,
                    error = %e,
                    "RF>1 replication failed for shard (owner write already committed)"
                );
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Get the distinct set of shard owners under the router's current map.
async fn distinct_owners(router: &HybridRouter) -> Vec<String> {
    let map = router.shard_map_snapshot().await;
    let owners: std::collections::BTreeSet<String> =
        map.assignments.values().map(|a| a.owner.clone()).collect();
    owners.into_iter().collect()
}
