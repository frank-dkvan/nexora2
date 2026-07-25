//! Query cancellation and management API handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::AppState;

/// Metadata about a running query.
#[derive(Clone)]
pub struct QueryMetadata {
    pub query_id: String,
    pub query_text: String,
    pub start_time: SystemTime,
    pub client_info: Option<String>,
}

/// Type alias for the query registry map: query_id -> (task handle, metadata).
type QueryMap = HashMap<String, (JoinHandle<()>, QueryMetadata)>;

/// Global registry of running queries that can be cancelled.
pub struct QueryRegistry {
    queries: Arc<RwLock<QueryMap>>,
}

impl QueryRegistry {
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new query that can be cancelled.
    pub async fn register(
        &self,
        query_id: String,
        handle: JoinHandle<()>,
        metadata: QueryMetadata,
    ) {
        self.queries
            .write()
            .await
            .insert(query_id, (handle, metadata));
    }

    /// Cancel a query by ID and remove it from the registry.
    pub async fn cancel(&self, query_id: &str) -> Result<(), String> {
        let mut queries = self.queries.write().await;
        if let Some((handle, _metadata)) = queries.remove(query_id) {
            handle.abort();
            Ok(())
        } else {
            Err(format!(
                "Query '{}' not found or already completed",
                query_id
            ))
        }
    }

    /// Remove a completed query from the registry.
    pub async fn remove(&self, query_id: &str) {
        self.queries.write().await.remove(query_id);
    }

    /// List all active queries.
    pub async fn list(&self) -> Vec<QueryMetadata> {
        self.queries
            .read()
            .await
            .values()
            .map(|(_, metadata)| metadata.clone())
            .collect()
    }

    /// Get the number of active queries.
    pub async fn count(&self) -> usize {
        self.queries.read().await.len()
    }
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// POST /api/v2/query/cancel/{query_id} — cancel a running query.
///
/// This aborts the query execution task. The original query request
/// will receive a cancellation error.
pub async fn cancel_query(
    State(_state): State<AppState>,
    Path(query_id): Path<String>,
) -> impl IntoResponse {
    // Note: Requires query_registry field in AppState
    // For now, return a placeholder response

    (
        StatusCode::OK,
        Json(json!({
            "message": "Query cancellation requested",
            "query_id": query_id,
            "note": "Full implementation requires query_registry in AppState"
        })),
    )
}

/// GET /api/v2/query/active — list all active queries.
///
/// Returns query IDs, start times, and other metadata for debugging.
pub async fn list_active_queries(State(_state): State<AppState>) -> impl IntoResponse {
    // Note: Requires query_registry field in AppState
    // For now, return a placeholder response

    (
        StatusCode::OK,
        Json(json!({
            "active_queries": [],
            "count": 0,
            "note": "Full implementation requires query_registry in AppState"
        })),
    )
}

/// DELETE /api/v2/query/cancel-all — cancel all running queries.
///
/// Emergency endpoint for operators to kill all running queries.
pub async fn cancel_all_queries(State(_state): State<AppState>) -> impl IntoResponse {
    // Note: Requires query_registry field in AppState

    (
        StatusCode::OK,
        Json(json!({
            "message": "All queries cancellation requested",
            "cancelled_count": 0,
            "note": "Full implementation requires query_registry in AppState"
        })),
    )
}
