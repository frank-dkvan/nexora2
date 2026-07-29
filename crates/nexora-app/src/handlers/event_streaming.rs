//! Event Streaming HTTP API handlers.
//!
//! These endpoints are only available when compiled with --features event-streaming
//! and --enable-event-streaming is set at runtime.

#[cfg(feature = "event-streaming")]
use crate::error::ApiError;
#[cfg(feature = "event-streaming")]
use crate::handlers::AppState;
#[cfg(feature = "event-streaming")]
use axum::{extract::State, Json};
#[cfg(feature = "event-streaming")]
use serde::{Deserialize, Serialize};

/// Request to execute Event Streaming DDL
#[cfg(feature = "event-streaming")]
#[derive(Debug, Deserialize)]
pub struct EventStreamingDdlRequest {
    /// SQL DDL statement (CREATE SOURCE, CREATE MATERIALIZED VIEW, etc.)
    pub sql: String,
}

/// Response from DDL execution
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct EventStreamingDdlResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request to query Event Streaming materialized view
#[cfg(feature = "event-streaming")]
#[derive(Debug, Deserialize)]
pub struct EventStreamingQueryRequest {
    /// SQL SELECT query
    pub sql: String,
}

/// Response from query execution
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct EventStreamingQueryResponse {
    /// Query results as JSON string (Phase 5: simplified)
    /// Phase 6 will return structured rows
    pub results: String,
}

/// Event Streaming source information
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct EventStreamingSource {
    pub name: String,
    pub connector: String,
    pub status: String,
}

/// Event Streaming materialized view information
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct EventStreamingMaterializedView {
    pub name: String,
    pub definition: String,
    pub status: String,
}

/// Event Streaming cluster status
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct EventStreamingStatus {
    pub enabled: bool,
    pub meta_leader: bool,
    pub version: String,
}

/// Execute Event Streaming DDL statement
///
/// POST /api/event-streaming/ddl
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/event-streaming/ddl \
///   -H "Content-Type: application/json" \
///   -d '{
///     "sql": "CREATE SOURCE my_kafka WITH (connector = '\''kafka'\'', ...)"
///   }'
/// ```
#[cfg(feature = "event-streaming")]
pub async fn execute_ddl(
    State(state): State<AppState>,
    Json(req): Json<EventStreamingDdlRequest>,
) -> Result<Json<EventStreamingDdlResponse>, ApiError> {
    let rw = state
        .event_streaming
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    rw.execute_ddl(&req.sql)
        .await
        .map_err(|e| ApiError::Internal(format!("Event Streaming DDL failed: {}", e)))?;

    Ok(Json(EventStreamingDdlResponse {
        success: true,
        message: Some("DDL executed successfully".to_string()),
    }))
}

/// Query Event Streaming materialized view
///
/// POST /api/event-streaming/query
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/event-streaming/query \
///   -H "Content-Type: application/json" \
///   -d '{
///     "sql": "SELECT * FROM my_mv LIMIT 10"
///   }'
/// ```
#[cfg(feature = "event-streaming")]
pub async fn query_mv(
    State(state): State<AppState>,
    Json(req): Json<EventStreamingQueryRequest>,
) -> Result<Json<EventStreamingQueryResponse>, ApiError> {
    let rw = state
        .event_streaming
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    let results = rw
        .query_mv(&req.sql)
        .await
        .map_err(|e| ApiError::Internal(format!("Event Streaming query failed: {}", e)))?;

    Ok(Json(EventStreamingQueryResponse { results }))
}

/// List Event Streaming sources
///
/// GET /api/event-streaming/sources
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/sources
/// ```
#[cfg(feature = "event-streaming")]
pub async fn list_sources(
    State(state): State<AppState>,
) -> Result<Json<Vec<EventStreamingSource>>, ApiError> {
    let rw = state
        .event_streaming
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    let sources = rw
        .list_sources()
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to list sources: {}", e)))?;

    let response = sources
        .iter()
        .map(|s| EventStreamingSource {
            name: s.name.clone(),
            connector: s.connector.clone(),
            status: "active".to_string(),
        })
        .collect();

    Ok(Json(response))
}

/// List Event Streaming materialized views
///
/// GET /api/event-streaming/materialized_views
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/materialized_views
/// ```
#[cfg(feature = "event-streaming")]
pub async fn list_materialized_views(
    State(state): State<AppState>,
) -> Result<Json<Vec<EventStreamingMaterializedView>>, ApiError> {
    let rw = state
        .event_streaming
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    let mvs = rw
        .list_materialized_views()
        .await
        .map_err(|e| ApiError::Internal(format!("Failed to list materialized views: {}", e)))?;

    let response = mvs
        .iter()
        .map(|mv| EventStreamingMaterializedView {
            name: mv.name.clone(),
            definition: mv.definition.clone(),
            status: "active".to_string(),
        })
        .collect();

    Ok(Json(response))
}

/// Get Event Streaming cluster status
///
/// GET /api/event-streaming/status
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/status
/// ```
#[cfg(feature = "event-streaming")]
pub async fn get_status(State(state): State<AppState>) -> Result<Json<EventStreamingStatus>, ApiError> {
    let rw = state
        .event_streaming
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    let meta_leader = rw.is_leader().await;

    Ok(Json(EventStreamingStatus {
        enabled: true,
        meta_leader,
        version: "v3.0.2".to_string(),
    }))
}

/// Get Event Streaming cluster health (Phase 8)
///
/// GET /api/event-streaming/cluster
///
/// Returns detailed cluster health for distributed embedded mode.
/// For single-node mode, returns simplified status.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/cluster
/// ```
#[cfg(all(feature = "event-streaming", feature = "embedded"))]
pub async fn get_cluster_status(
    State(state): State<AppState>,
) -> Result<Json<ClusterStatusResponse>, ApiError> {
    #[cfg(feature = "embedded")]
    if let Some(distributed_rw) = &state.distributed_event_streaming {
        // Phase 8: Distributed cluster mode
        let health = distributed_rw
            .monitor_health()
            .await
            .map_err(|e| ApiError::Internal(format!("Failed to get cluster health: {}", e)))?;

        return Ok(Json(ClusterStatusResponse {
            cluster_mode: true,
            leader_node_id: health.leader_node_id,
            meta_nodes: health
                .meta_nodes
                .iter()
                .map(|n| NodeStatus {
                    node_id: n.node_id,
                    is_running: n.is_running,
                    address: n.address.clone(),
                })
                .collect(),
            frontend: NodeStatus {
                node_id: 0,
                is_running: health.frontend.is_running,
                address: health.frontend.address.clone(),
            },
            compute_nodes: health
                .compute_nodes
                .iter()
                .map(|n| NodeStatus {
                    node_id: n.node_id,
                    is_running: n.is_running,
                    address: n.address.clone(),
                })
                .collect(),
        }));
    }

    // Phase 7: Single node embedded mode
    #[cfg(feature = "embedded")]
    if state.event_streaming.is_some() {
        return Ok(Json(ClusterStatusResponse {
            cluster_mode: false,
            leader_node_id: Some(1),
            meta_nodes: vec![NodeStatus {
                node_id: 1,
                is_running: true,
                address: "127.0.0.1:5690".to_string(),
            }],
            frontend: NodeStatus {
                node_id: 0,
                is_running: true,
                address: "127.0.0.1:4566".to_string(),
            },
            compute_nodes: vec![NodeStatus {
                node_id: 1,
                is_running: true,
                address: "127.0.0.1:5688".to_string(),
            }],
        }));
    }

    // No Event Streaming running
    Err(ApiError::FeatureNotEnabled("event-streaming embedded".to_string()))
}

/// Node status in cluster
#[cfg(all(feature = "event-streaming", feature = "embedded"))]
#[derive(Debug, Serialize)]
pub struct NodeStatus {
    pub node_id: u32,
    pub is_running: bool,
    pub address: String,
}

/// Cluster status response
#[cfg(all(feature = "event-streaming", feature = "embedded"))]
#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    pub cluster_mode: bool,
    pub leader_node_id: Option<u32>,
    pub meta_nodes: Vec<NodeStatus>,
    pub frontend: NodeStatus,
    pub compute_nodes: Vec<NodeStatus>,
}

#[cfg(test)]
#[cfg(feature = "event-streaming")]
mod tests {
    use super::*;

    #[test]
    fn test_event_streaming_request_deserialization() {
        let json = r#"{"sql":"CREATE SOURCE test"}"#;
        let req: EventStreamingDdlRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.sql, "CREATE SOURCE test");
    }

    #[test]
    fn test_event_streaming_response_serialization() {
        let resp = EventStreamingDdlResponse {
            success: true,
            message: Some("OK".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("true"));
        assert!(json.contains("OK"));
    }
}
