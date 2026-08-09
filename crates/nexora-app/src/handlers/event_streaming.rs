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
pub async fn get_status(
    State(state): State<AppState>,
) -> Result<Json<EventStreamingStatus>, ApiError> {
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
    Err(ApiError::FeatureNotEnabled(
        "event-streaming embedded".to_string(),
    ))
}

/// Get distributed library cluster status (Phase 5.3)
///
/// GET /api/event-streaming/cluster/distributed
///
/// Returns detailed status for distributed library mode (in-process multi-node cluster).
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/cluster/distributed
/// ```
#[cfg(all(feature = "event-streaming", feature = "library"))]
pub async fn get_distributed_library_status(
    State(state): State<AppState>,
) -> Result<Json<DistributedLibraryStatusResponse>, ApiError> {
    let cluster = state
        .distributed_library
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("distributed library mode".to_string()))?;

    let (meta_cluster, frontend_pool, compute_cluster) = cluster.as_ref();

    // Meta cluster state (cluster_state() returns the MetaClusterState enum).
    let meta_state = meta_cluster.cluster_state().await;
    let is_leader = meta_cluster.is_leader().await;
    // node_count = configured peers + this node.
    let node_count = meta_cluster.config().meta.raft_peers.len() + 1;

    // Frontend / Compute health (healthy_count() returns the live node count).
    let frontend_healthy = frontend_pool.healthy_count().await;
    let compute_healthy = compute_cluster.healthy_count().await;
    let compute_parallelism = compute_cluster.total_parallelism().await;

    Ok(Json(DistributedLibraryStatusResponse {
        mode: "distributed_library".to_string(),
        meta: MetaClusterStatus {
            is_leader,
            leader_id: None,
            raft_state: format!("{:?}", meta_state),
            node_count,
        },
        frontend: FrontendPoolStatus {
            active_nodes: frontend_healthy,
            total_nodes: node_count,
            healthy: frontend_healthy > 0,
        },
        compute: ComputeClusterStatus {
            active_nodes: compute_healthy,
            total_nodes: node_count,
            healthy: compute_healthy > 0,
            total_parallelism: compute_parallelism,
        },
    }))
}

// ============================================================
// Phase 6.3: EventLogSink Sync Management
// ============================================================

/// Request to start syncing MV to EventLog
#[cfg(feature = "event-streaming")]
#[derive(Debug, Deserialize)]
pub struct SyncStartRequest {
    /// Materialized view name in RisingWave
    pub mv_name: String,
    /// EventLogStore topic to write events to
    pub topic: String,
}

/// Response from sync start
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct SyncStartResponse {
    pub success: bool,
    pub mv_name: String,
    pub topic: String,
    pub message: String,
}

/// Request to stop syncing MV
#[cfg(feature = "event-streaming")]
#[derive(Debug, Deserialize)]
pub struct SyncStopRequest {
    /// Materialized view name to stop syncing
    pub mv_name: String,
}

/// Response from sync stop
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct SyncStopResponse {
    pub success: bool,
    pub mv_name: String,
    pub message: String,
}

/// Sync status information
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct SyncStatus {
    pub mv_name: String,
    pub topic: String,
    pub active: bool,
}

/// Response from sync status query
#[cfg(feature = "event-streaming")]
#[derive(Debug, Serialize)]
pub struct SyncStatusResponse {
    pub syncs: Vec<SyncStatus>,
}

/// Start syncing MV changes to EventLog
///
/// POST /api/event-streaming/sync/start
///
/// Creates an EventLogSink and starts streaming MV changes to the specified EventLogStore topic.
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/event-streaming/sync/start \
///   -H "Content-Type: application/json" \
///   -d '{
///     "mv_name": "enriched_cargo_events",
///     "topic": "nexora.cargo"
///   }'
/// ```
#[cfg(feature = "event-streaming")]
pub async fn start_sync(
    State(state): State<AppState>,
    Json(req): Json<SyncStartRequest>,
) -> Result<Json<SyncStartResponse>, ApiError> {
    // Verify event-first is enabled
    #[cfg(not(feature = "event-first"))]
    {
        return Err(ApiError::FeatureNotEnabled(
            "event-first required for sync".to_string(),
        ));
    }

    #[cfg(feature = "event-first")]
    {
        let event_store = state
            .event_store
            .as_ref()
            .ok_or_else(|| ApiError::FeatureNotEnabled("event-first".to_string()))?;

        let rw_module = state
            .event_streaming
            .as_ref()
            .ok_or_else(|| ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

        // Check if already syncing
        let sinks = state.event_sinks.read().await;
        if sinks.contains_key(&req.mv_name) {
            return Ok(Json(SyncStartResponse {
                success: false,
                mv_name: req.mv_name.clone(),
                topic: req.topic.clone(),
                message: format!("MV {} is already syncing", req.mv_name),
            }));
        }
        drop(sinks);

        // Create EventLogSink
        let sink =
            nexora_risingwave::EventLogSink::new(event_store.clone(), Arc::new(rw_module.clone()));

        let mv_name = req.mv_name.clone();
        let topic = req.topic.clone();

        // Start sync in background task
        let handle = tokio::spawn(async move {
            if let Err(e) = sink.start_sync(&mv_name, &topic).await {
                tracing::error!("EventLogSink failed for MV {}: {}", mv_name, e);
            }
        });

        // Store abort handle
        let mut sinks = state.event_sinks.write().await;
        sinks.insert(req.mv_name.clone(), handle.abort_handle());

        Ok(Json(SyncStartResponse {
            success: true,
            mv_name: req.mv_name,
            topic: req.topic,
            message: "Sync started successfully".to_string(),
        }))
    }
}

/// Stop syncing MV changes
///
/// POST /api/event-streaming/sync/stop
///
/// Stops an active EventLogSink for the specified MV.
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/event-streaming/sync/stop \
///   -H "Content-Type: application/json" \
///   -d '{
///     "mv_name": "enriched_cargo_events"
///   }'
/// ```
#[cfg(feature = "event-streaming")]
pub async fn stop_sync(
    State(state): State<AppState>,
    Json(req): Json<SyncStopRequest>,
) -> Result<Json<SyncStopResponse>, ApiError> {
    let mut sinks = state.event_sinks.write().await;

    if let Some(handle) = sinks.remove(&req.mv_name) {
        handle.abort();
        Ok(Json(SyncStopResponse {
            success: true,
            mv_name: req.mv_name,
            message: "Sync stopped successfully".to_string(),
        }))
    } else {
        Ok(Json(SyncStopResponse {
            success: false,
            mv_name: req.mv_name.clone(),
            message: format!("No active sync found for MV {}", req.mv_name),
        }))
    }
}

/// Get sync status for all active sinks
///
/// GET /api/event-streaming/sync/status
///
/// Returns the list of all active EventLogSink tasks.
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/sync/status
/// ```
#[cfg(feature = "event-streaming")]
pub async fn get_sync_status(
    State(state): State<AppState>,
) -> Result<Json<SyncStatusResponse>, ApiError> {
    let sinks = state.event_sinks.read().await;

    // TODO: Store topic info with abort handle so we can return it here
    // For now, we only return mv_name and active status
    let syncs: Vec<SyncStatus> = sinks
        .keys()
        .map(|mv_name| SyncStatus {
            mv_name: mv_name.clone(),
            topic: "unknown".to_string(), // TODO: Track this
            active: true,
        })
        .collect();

    Ok(Json(SyncStatusResponse { syncs }))
}

/// Distributed library cluster status response
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct DistributedLibraryStatusResponse {
    pub mode: String,
    pub meta: MetaClusterStatus,
    pub frontend: FrontendPoolStatus,
    pub compute: ComputeClusterStatus,
}

/// Meta cluster status
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct MetaClusterStatus {
    pub is_leader: bool,
    pub leader_id: Option<u64>,
    pub raft_state: String,
    pub node_count: usize,
}

/// Frontend pool status
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct FrontendPoolStatus {
    pub active_nodes: usize,
    pub total_nodes: usize,
    pub healthy: bool,
}

/// Compute cluster status
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct ComputeClusterStatus {
    pub active_nodes: usize,
    pub total_nodes: usize,
    pub healthy: bool,
    pub total_parallelism: usize,
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
