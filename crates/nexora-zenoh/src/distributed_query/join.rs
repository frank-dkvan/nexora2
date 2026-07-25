//! Cross-partition single-hop relationship join.
//!
//! For `MATCH (a:La)-[:REL]->(b:Lb) RETURN a.x, b.y`, the source `a` and its
//! outgoing edge live on `a`'s shard owner, but the target `b` may live on a
//! different owner. A per-owner local Cypher run can't answer this: the local
//! executor drops edges whose target isn't in its own snapshot. So the
//! coordinator does the join itself, in three fan-out stages:
//!
//! 1. **Scan sources** — on each owner, `MATCH (a:La) RETURN a` gives the source
//!    ids that owner holds (union across owners = all sources).
//! 2. **Expand edges** — for each source id, fetch its `REL` edges from that
//!    source's owner (`GetEdges`, routed by source id). Each edge yields a
//!    `(source_id, target_id)` pair.
//! 3. **Fetch endpoints** — for the properties the RETURN projects, fetch each
//!    needed node's properties from *its* owner (`GetAllProperties`, routed by
//!    id), then assemble output rows. Target-label filtering (`:Lb`) is applied
//!    by checking the fetched target actually carries the label.
//!
//! Bounded to a single directed typed hop (see the planner's
//! `plan_relationship_join`); anything else stays on the honest 501.

use super::{RelJoinSpec, RelProjection};
use crate::router::HybridRouter;
use crate::{GraphOperation, GraphResult, RouterError};
use nexora_id::NexoraId;
use std::collections::{HashMap, HashSet};

/// Execute a single-hop relationship join across shard owners.
pub async fn execute_rel_join(
    router: &HybridRouter,
    spec: &RelJoinSpec,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>), RouterError> {
    // Stage 1: scan source ids from every owner. A per-owner `MATCH (a:La)
    // RETURN a` returns node objects whose "id" is the hex qid.
    let source_ids = scan_node_ids(router, &spec.source_labels).await?;

    // Stage 2: for each source, fetch its REL edges (routed to the source's
    // owner). Collect (source_id, target_id) pairs for the requested direction.
    let mut pairs: Vec<(NexoraId, NexoraId)> = Vec::new();
    for src in &source_ids {
        let edges = fetch_edges(router, src, Some(&spec.edge_type)).await?;
        for (dir_out, target) in edges {
            // Keep edges matching the pattern's direction. `GetEdges` reports the
            // edge's stored direction relative to the source; an outgoing
            // pattern wants out-edges, incoming wants in-edges.
            if dir_out == spec.outgoing {
                pairs.push((src.clone(), target));
            }
        }
    }

    // Target-label filter: labels aren't exposed via GetAllProperties, so scan
    // the target label set per-owner (union across owners) and keep only pairs
    // whose target is in it. Skipped when the pattern names no target label.
    if !spec.target_labels.is_empty() {
        let valid_targets: HashSet<String> = scan_node_ids(router, &spec.target_labels)
            .await?
            .into_iter()
            .map(|q| q.to_hex())
            .collect();
        pairs.retain(|(_, t)| valid_targets.contains(&t.to_hex()));
    }

    // Stage 3: fetch the properties the projections need. Fetch each distinct
    // endpoint's properties once, from its owner.
    let need_source_props = spec
        .projections
        .iter()
        .any(|p| p.var == spec.source_var && p.property.is_some());
    let need_target_props = spec
        .projections
        .iter()
        .any(|p| p.var == spec.target_var && p.property.is_some());

    let mut props: HashMap<String, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    let mut to_fetch: HashSet<NexoraId> = HashSet::new();
    if need_source_props {
        for (s, _) in &pairs {
            to_fetch.insert(s.clone());
        }
    }
    if need_target_props {
        for (_, t) in &pairs {
            to_fetch.insert(t.clone());
        }
    }
    for id in &to_fetch {
        let p = fetch_props(router, id).await?;
        props.insert(id.to_hex(), p);
    }

    // Assemble output rows.
    let mut rows = Vec::new();
    for (src, tgt) in &pairs {
        let row = spec
            .projections
            .iter()
            .map(|proj| project(proj, spec, src, tgt, &props))
            .collect();
        rows.push(row);
    }

    Ok((spec.columns_vec(), rows))
}

/// Project one output cell for a join row.
fn project(
    proj: &RelProjection,
    spec: &RelJoinSpec,
    src: &NexoraId,
    tgt: &NexoraId,
    props: &HashMap<String, serde_json::Map<String, serde_json::Value>>,
) -> serde_json::Value {
    let id = if proj.var == spec.source_var {
        src
    } else {
        tgt
    };
    match &proj.property {
        // Bare node reference → its id (hex), matching single-node `RETURN a`
        // which exposes {id:...}; here we return the id string for join output.
        None => serde_json::Value::String(id.to_hex()),
        Some(prop) => props
            .get(&id.to_hex())
            .and_then(|m| m.get(prop))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    }
}

impl RelJoinSpec {
    fn columns_vec(&self) -> Vec<String> {
        self.projections.iter().map(|p| p.column.clone()).collect()
    }
}

/// Scan node ids carrying the given labels from every owner, via
/// `MATCH (a:Labels) RETURN a` (used for both source scan and target-label
/// filtering, and by the path join for its source scan / label filters).
pub(super) async fn scan_node_ids(
    router: &HybridRouter,
    labels: &[String],
) -> Result<Vec<NexoraId>, RouterError> {
    let label_str = if labels.is_empty() {
        String::new()
    } else {
        format!(":{}", labels.join(":"))
    };
    let query = format!("MATCH (a{label_str}) RETURN a");

    let owners = super::distinct_owners(router).await;
    if owners.is_empty() {
        return Err(RouterError::NodeNotFound("no shard owners".into()));
    }
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for owner in owners {
        let op = GraphOperation::ExecuteCypher {
            query: query.clone(),
        };
        let Some(client) = router.remote_client_arc() else {
            return Err(RouterError::NodeNotFound(
                "router has no remote client".into(),
            ));
        };
        match client.execute(&owner, op).await {
            Ok(GraphResult::CypherRows { rows, .. }) => {
                for row in rows {
                    if let Some(id) = row.first().and_then(node_cell_id) {
                        if let Ok(qid) = NexoraId::from_hex(&id) {
                            if seen.insert(id) {
                                ids.push(qid);
                            }
                        }
                    }
                }
            }
            Ok(other) => {
                return Err(RouterError::Remote(format!(
                    "source scan on {owner} returned non-row result: {other:?}"
                )))
            }
            Err(e) => {
                return Err(RouterError::Remote(format!(
                    "source scan on {owner} failed: {e} (refusing partial join)"
                )))
            }
        }
    }
    Ok(ids)
}

/// Extract the hex id from a scan result cell. A node cell is
/// `{"id": "<hex>", ...}` (see the cypher executor's node serialization); a
/// bare string cell is taken as-is.
fn node_cell_id(cell: &serde_json::Value) -> Option<String> {
    match cell {
        serde_json::Value::Object(m) => m.get("id").and_then(|v| v.as_str()).map(String::from),
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Fetch a node's edges from its owner. `edge_type` = `Some(t)` filters to type
/// `t`; `None` returns all types (untyped pattern). Returns `(is_outgoing,
/// target_id)` per edge. Shared with the path join.
pub(super) async fn fetch_edges(
    router: &HybridRouter,
    node: &NexoraId,
    edge_type: Option<&str>,
) -> Result<Vec<(bool, NexoraId)>, RouterError> {
    let op = GraphOperation::GetEdges {
        qid: node.clone(),
        edge_type: edge_type.map(|s| s.to_string()),
    };
    // `route` sends to the node's owner (or local). The adapter answers with a
    // JSON array of {edge_type, direction, target}.
    let result = router.route(node, op).await?;
    let arr = match result {
        GraphResult::Property(Some(serde_json::Value::Array(a))) => a,
        GraphResult::Property(_) => Vec::new(),
        other => {
            return Err(RouterError::Remote(format!(
                "GetEdges returned unexpected shape: {other:?}"
            )))
        }
    };
    let mut out = Vec::new();
    for e in arr {
        let dir_out = e.get("direction").and_then(|d| d.as_str()) == Some("out");
        if let Some(t) = e.get("target").and_then(|t| t.as_str()) {
            if let Ok(tid) = NexoraId::from_hex(t) {
                out.push((dir_out, tid));
            }
        }
    }
    Ok(out)
}

/// Fetch a node's properties from its owner. Shared with the path join.
pub(super) async fn fetch_props(
    router: &HybridRouter,
    node: &NexoraId,
) -> Result<serde_json::Map<String, serde_json::Value>, RouterError> {
    let op = GraphOperation::GetAllProperties { qid: node.clone() };
    let result = router.route(node, op).await?;
    let map = match result {
        GraphResult::Property(Some(serde_json::Value::Object(m))) => m,
        _ => serde_json::Map::new(),
    };
    Ok(map)
}
