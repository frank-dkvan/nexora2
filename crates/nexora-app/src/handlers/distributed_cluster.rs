//! Distributed library mode cluster management HTTP API handlers.
//!
//! These endpoints are only available when compiled with:
//! --features event-streaming,library

#[cfg(all(feature = "event-streaming", feature = "library"))]
use crate::error::ApiError;
#[cfg(all(feature = "event-streaming", feature = "library"))]
use crate::handlers::AppState;
#[cfg(all(feature = "event-streaming", feature = "library"))]
use axum::{extract::State, Json};
#[cfg(all(feature = "event-streaming", feature = "library"))]
use serde::Serialize;

/// Cluster status response
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    /// Node ID of this instance
    pub node_id: String,

    /// Is this node the Meta leader?
    pub is_leader: bool,

    /// Raft term
    pub raft_term: u64,

    /// Number of healthy Frontend nodes
    pub healthy_frontends: usize,

    /// Number of healthy Compute nodes
    pub healthy_computes: usize,

    /// Total worker parallelism across all Compute nodes
    pub total_workers: usize,
}

/// Node info in cluster
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub listen_addr: String,
    pub health: String,
    pub parallelism: Option<usize>,
}

/// Cluster nodes response
#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Serialize)]
pub struct ClusterNodesResponse {
    pub compute_nodes: Vec<NodeInfo>,
}

/// Get distributed cluster status
///
/// GET /api/event-streaming/cluster/status
///
/// Returns cluster health: leadership, healthy nodes, worker count
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/cluster/status
/// ```
#[cfg(all(feature = "event-streaming", feature = "library"))]
pub async fn get_cluster_status(
    State(state): State<AppState>,
) -> Result<Json<ClusterStatusResponse>, ApiError> {
    let distributed = state
        .distributed_library
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("distributed-library".to_string()))?;

    let (meta, frontend_pool, compute_cluster) = distributed.as_ref();

    let is_leader = meta.is_leader().await;
    let raft_term = meta.current_term();
    let healthy_frontends = frontend_pool.healthy_count().await;
    let healthy_computes = compute_cluster.healthy_count().await;
    let total_workers = compute_cluster.total_parallelism().await;

    Ok(Json(ClusterStatusResponse {
        node_id: meta.node_id().to_string(),
        is_leader,
        raft_term,
        healthy_frontends,
        healthy_computes,
        total_workers,
    }))
}

/// List all Compute nodes in the cluster
///
/// GET /api/event-streaming/cluster/nodes
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/event-streaming/cluster/nodes
/// ```
#[cfg(all(feature = "event-streaming", feature = "library"))]
pub async fn list_cluster_nodes(
    State(state): State<AppState>,
) -> Result<Json<ClusterNodesResponse>, ApiError> {
    let distributed = state
        .distributed_library
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("distributed-library".to_string()))?;

    let (_meta, _frontend_pool, compute_cluster) = distributed.as_ref();

    let nodes = compute_cluster.list_nodes().await;

    let compute_nodes = nodes
        .into_iter()
        .map(|n| NodeInfo {
            node_id: n.node_id,
            listen_addr: n.listen_addr.to_string(),
            health: format!("{:?}", n.health),
            parallelism: Some(n.parallelism),
        })
        .collect();

    Ok(Json(ClusterNodesResponse { compute_nodes }))
}
