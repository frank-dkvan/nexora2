//! Batch operations API handlers.

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::handlers::{json_to_pv, AppState};
use nexora_value::Symbol;

/// Monotonic request-id source for batch mutations. Batch ops need a unique
/// request_id per label mutation for dedup; a process-wide counter suffices.
static BATCH_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    BATCH_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

/// Request body for batch node creation.
#[derive(Deserialize)]
pub struct BatchCreateNodesRequest {
    /// Array of node definitions.
    pub nodes: Vec<NodeDefinition>,
}

#[derive(Deserialize)]
pub struct NodeDefinition {
    /// Optional labels to assign.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Properties to set.
    #[serde(default)]
    pub properties: HashMap<String, serde_json::Value>,
}

/// POST /api/v2/batch/nodes — create multiple nodes in a single transaction.
///
/// This is more efficient than individual CREATE calls and ensures atomicity.
pub async fn batch_create_nodes(
    State(state): State<AppState>,
    Json(req): Json<BatchCreateNodesRequest>,
) -> impl IntoResponse {
    const MAX_BATCH_SIZE: usize = 1000;

    if req.nodes.len() > MAX_BATCH_SIZE {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Batch size {} exceeds maximum of {}", req.nodes.len(), MAX_BATCH_SIZE),
                "code": "BATCH_TOO_LARGE"
            })),
        );
    }

    if req.nodes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Batch cannot be empty",
                "code": "EMPTY_BATCH"
            })),
        );
    }

    let mut created_ids = Vec::new();

    for (idx, node_def) in req.nodes.iter().enumerate() {
        // A node is created implicitly by its first property/label write. Use a
        // random id so concurrent batch creates don't collide.
        let qid = nexora_id::NexoraId::new_random();

        // Set properties (this materializes the node).
        for (key, value) in &node_def.properties {
            let pv = json_to_pv(value);
            if let Err(e) = state.graph.set_property(&qid, key, pv).await {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Failed to set property '{}' on node at index {}: {}", key, idx, e),
                        "node_id": qid.to_hex(),
                        "created_so_far": created_ids.len(),
                        "failed_index": idx
                    })),
                );
            }
        }

        // Add labels via the commit path (each needs a unique request_id).
        for label in &node_def.labels {
            let request_id = next_request_id();
            if let Err(e) = state
                .graph
                .add_label(&qid, Symbol::new(label), request_id)
                .await
            {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Failed to add label '{}' to node at index {}: {}", label, idx, e),
                        "node_id": qid.to_hex(),
                        "created_so_far": created_ids.len(),
                        "failed_index": idx
                    })),
                );
            }
        }

        created_ids.push(qid.to_hex());
    }

    (
        StatusCode::CREATED,
        Json(json!({
            "created_count": created_ids.len(),
            "node_ids": created_ids
        })),
    )
}

/// Request body for batch property updates.
#[derive(Deserialize)]
pub struct BatchSetPropertiesRequest {
    /// Array of property update operations.
    pub operations: Vec<PropertyUpdate>,
}

#[derive(Deserialize)]
pub struct PropertyUpdate {
    /// Node ID (hex string).
    pub node_id: String,
    /// Property key.
    pub key: String,
    /// Property value.
    pub value: serde_json::Value,
}

/// POST /api/v2/batch/properties — update properties on multiple nodes.
pub async fn batch_set_properties(
    State(state): State<AppState>,
    Json(req): Json<BatchSetPropertiesRequest>,
) -> impl IntoResponse {
    const MAX_BATCH_SIZE: usize = 1000;

    if req.operations.len() > MAX_BATCH_SIZE {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("Batch size {} exceeds maximum of {}", req.operations.len(), MAX_BATCH_SIZE),
                "code": "BATCH_TOO_LARGE"
            })),
        );
    }

    let mut updated_count = 0;
    let mut errors = Vec::new();

    for (idx, op) in req.operations.iter().enumerate() {
        let qid = match nexora_id::NexoraId::from_hex(&op.node_id) {
            Ok(id) => id,
            Err(e) => {
                errors.push(json!({
                    "index": idx,
                    "node_id": op.node_id,
                    "error": format!("Invalid node ID: {}", e)
                }));
                continue;
            }
        };

        let pv = json_to_pv(&op.value);
        if let Err(e) = state.graph.set_property(&qid, &op.key, pv).await {
            errors.push(json!({
                "index": idx,
                "node_id": op.node_id,
                "key": op.key,
                "error": format!("Failed to set property: {}", e)
            }));
        } else {
            updated_count += 1;
        }
    }

    if errors.is_empty() {
        (
            StatusCode::OK,
            Json(json!({
                "updated_count": updated_count,
                "total": req.operations.len()
            })),
        )
    } else {
        (
            StatusCode::MULTI_STATUS,
            Json(json!({
                "updated_count": updated_count,
                "total": req.operations.len(),
                "errors": errors
            })),
        )
    }
}
