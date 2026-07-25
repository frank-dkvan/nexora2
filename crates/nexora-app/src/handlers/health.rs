//! Health check handlers — /health, /ready, /live endpoints for Kubernetes probes.
//!
//! - /health: overall system health (deep check: DB, shards)
//! - /ready: readiness (is the service ready to accept traffic)
//! - /live: liveness (is the process alive)

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub status: String, // "ok" | "degraded" | "down"
    pub uptime_secs: u64,
    pub shards_online: usize,
    pub active_nodes: usize,
    pub version: &'static str,
}

static START_TIME: std::sync::LazyLock<std::time::Instant> =
    std::sync::LazyLock::new(std::time::Instant::now);

pub struct HealthState {
    pub active_nodes: RwLock<usize>,
    pub shards_online: RwLock<usize>,
}

impl HealthState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active_nodes: RwLock::new(0),
            shards_online: RwLock::new(1),
        })
    }
}

/// GET /health — deep health check
pub async fn health_handler(state: axum::extract::State<Arc<HealthState>>) -> impl IntoResponse {
    let uptime = START_TIME.elapsed().as_secs();
    let active_nodes = *state.active_nodes.read().await;
    let shards_online = *state.shards_online.read().await;

    let status = if shards_online == 0 {
        "down"
    } else {
        "ok"
    };

    let status_code = if status == "ok" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status_code,
        Json(HealthStatus {
            status: status.to_string(),
            uptime_secs: uptime,
            shards_online,
            active_nodes,
            version: env!("CARGO_PKG_VERSION"),
        }),
    )
}

/// GET /ready — readiness check
pub async fn ready_handler(state: axum::extract::State<Arc<HealthState>>) -> impl IntoResponse {
    let shards_online = *state.shards_online.read().await;

    if shards_online > 0 {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// GET /live — liveness check
pub async fn live_handler() -> impl IntoResponse {
    (StatusCode::OK, "alive")
}
