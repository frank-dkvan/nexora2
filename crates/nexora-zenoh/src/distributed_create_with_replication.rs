//! Distributed CREATE execution with RF>1 quorum replication support.

use crate::distributed_query::{CreateOp, CreatePlan, WRITE_STAT_COLUMNS};
use crate::replica_writer::ReplicaWriter;
use crate::replication::FencingToken;
use crate::router::HybridRouter;
use crate::{GraphOperation, GraphResult, RouterError};
use std::collections::HashMap;

/// Execute a distributed CREATE with RF>1 quorum replication support.
///
/// When a ReplicaWriter is configured on the router:
/// 1. Group nodes by their target owner (based on qid.shard_key())
/// 2. Each owner executes CREATE locally for its assigned nodes
/// 3. For each created node, replicate to followers via SetProperty operations
/// 4. Sum write stats across all owners
pub async fn execute_create_with_replication(
    router: &HybridRouter,
    plan: &CreatePlan,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    if plan.nodes.is_empty() {
        let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
        let row: Vec<serde_json::Value> = vec![serde_json::json!(0); WRITE_STAT_COLUMNS.len()];
        return Ok((columns, vec![row]));
    }

    let shard_map = router.shard_map_snapshot().await;
    let replica_writer = router.replica_writer();

    // Group nodes by their target owner
    let mut by_owner: HashMap<String, Vec<&CreateOp>> = HashMap::new();
    for op in &plan.nodes {
        let shard = shard_map.shard_of(&op.qid);
        let owner = match shard_map.get(shard) {
            Some(a) => &a.owner,
            None => {
                return Err(RouterError::NodeNotFound(format!(
                    "no owner for shard {shard} (qid {})",
                    op.qid.to_hex()
                )))
            }
        };
        by_owner.entry(owner.clone()).or_default().push(op);
    }

    // Build and dispatch a CREATE sub-query for each owner
    let mut handles = Vec::with_capacity(by_owner.len());
    for (owner, ops) in by_owner {
        let query = build_create_subquery(&ops);
        let op = GraphOperation::ExecuteCypher { query };
        let client = router.remote_client_arc();
        let replica_writer_clone = replica_writer.clone();
        let shard_map_clone = shard_map.clone();
        let owner_clone = owner.clone();
        let ops_vec: Vec<CreateOp> = ops.iter().map(|op| (*op).clone()).collect();

        handles.push(tokio::spawn(async move {
            let result = match client {
                Some(c) => c.execute(&owner_clone, op).await,
                None => Err(RouterError::NodeNotFound(
                    "router has no remote client".into(),
                )),
            };

            // If CREATE succeeded and replication is configured, replicate each node
            if let (Ok(_), Some(replica_writer)) = (&result, &replica_writer_clone) {
                if let Err(e) = replicate_created_nodes(
                    &owner_clone,
                    &ops_vec,
                    &shard_map_clone,
                    replica_writer,
                )
                .await
                {
                    tracing::warn!(
                        owner = %owner_clone,
                        error = %e,
                        "RF>1 replication failed for distributed CREATE; owner CREATE committed"
                    );
                }
            }

            (owner_clone, result)
        }));
    }

    // Sum the write stats across owners
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
                    "owner {owner} returned non-stat CREATE result: {other:?}"
                )))
            }
            Err(e) => {
                return Err(RouterError::Remote(format!(
                    "owner {owner} CREATE failed: {e}"
                )))
            }
        }
    }

    let columns: Vec<String> = WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect();
    let row: Vec<serde_json::Value> = totals.iter().map(|n| serde_json::json!(n)).collect();
    Ok((columns, vec![row]))
}

/// Replicate created nodes to their followers via individual SetProperty operations.
async fn replicate_created_nodes(
    owner: &str,
    ops: &[CreateOp],
    shard_map: &crate::shard_map::ShardMap,
    replica_writer: &ReplicaWriter,
) -> Result<(), RouterError> {
    for op in ops {
        let shard = shard_map.shard_of(&op.qid);
        let assignment = match shard_map.get(shard) {
            Some(a) => a,
            None => continue,
        };

        // Only replicate if this owner actually owns this shard
        if assignment.owner != owner {
            continue;
        }

        // Build a FencingToken for this shard's current epoch
        let token = FencingToken::new(shard, assignment.epoch);

        // Replicate each property as a SetProperty operation
        for (key, value) in &op.properties {
            let set_op = GraphOperation::SetProperty {
                qid: op.qid.clone(),
                key: key.clone(),
                value: value.clone(),
            };

            match replica_writer.quorum_write(shard, &token, set_op).await {
                Ok(status) => {
                    tracing::debug!(
                        shard = shard,
                        qid = %op.qid.to_hex(),
                        key = key,
                        status = ?status,
                        "RF>1 property replication succeeded"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        shard = shard,
                        qid = %op.qid.to_hex(),
                        key = key,
                        error = %e,
                        "RF>1 property replication failed (owner CREATE already committed)"
                    );
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}

/// Build a CREATE sub-query for an owner (from distributed_query.rs)
fn build_create_subquery(ops: &[&CreateOp]) -> String {
    let mut parts = Vec::with_capacity(ops.len());
    for op in ops {
        let var = op.variable.as_deref().unwrap_or("_");
        let labels = if op.labels.is_empty() {
            String::new()
        } else {
            format!(":{}", op.labels.join(":"))
        };
        let mut props = op.properties.clone();
        props.insert("__qid".to_string(), serde_json::json!(op.qid.to_hex()));
        let props_str = if props.is_empty() {
            String::new()
        } else {
            let pairs: Vec<String> = props
                .iter()
                .map(|(k, v)| format!("{k}: {}", serde_json::to_string(v).unwrap_or_default()))
                .collect();
            format!(" {{{}}}", pairs.join(", "))
        };
        parts.push(format!("({var}{labels}{props_str})"));
    }
    format!("CREATE {}", parts.join(", "))
}
