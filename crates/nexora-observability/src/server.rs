//! HTTP server for observability endpoints
//!
//! Provides:
//! - GET /health - Liveness check (always returns 200 if process alive)
//! - GET /ready - Readiness check (returns 200 only if ready to serve)
//! - GET /metrics - Prometheus metrics export

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::health::{HealthChecker, HealthStatus};
use crate::metrics::MetricsRegistry;

/// Observability server state
#[derive(Clone)]
pub struct ObservabilityState {
    pub health_checker: Arc<HealthChecker>,
    pub metrics: Arc<MetricsRegistry>,
}

/// Observability HTTP server
pub struct ObservabilityServer {
    state: ObservabilityState,
    addr: SocketAddr,
}

impl ObservabilityServer {
    /// Create new observability server
    pub fn new(
        addr: SocketAddr,
        health_checker: Arc<HealthChecker>,
        metrics: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            state: ObservabilityState {
                health_checker,
                metrics,
            },
            addr,
        }
    }

    /// Start the server
    pub async fn serve(self) -> anyhow::Result<()> {
        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/ready", get(ready_handler))
            .route("/metrics", get(metrics_handler))
            .with_state(self.state);

        let listener = TcpListener::bind(self.addr).await?;
        tracing::info!("Observability server listening on {}", self.addr);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

/// GET /health - Liveness probe
async fn health_handler(State(state): State<ObservabilityState>) -> Response {
    let status = state.health_checker.liveness().await;

    match status {
        HealthStatus::Healthy => StatusCode::OK.into_response(),
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// GET /ready - Readiness probe
async fn ready_handler(State(state): State<ObservabilityState>) -> Response {
    let response = state.health_checker.check_all().await;

    match response.status {
        HealthStatus::Healthy => (StatusCode::OK, Json(response)).into_response(),
        HealthStatus::Degraded => (StatusCode::OK, Json(response)).into_response(),
        HealthStatus::Unhealthy => {
            (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response()
        }
    }
}

/// GET /metrics - Prometheus metrics export
async fn metrics_handler(State(state): State<ObservabilityState>) -> Response {
    match state.metrics.export() {
        Ok(metrics) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            metrics,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to export metrics: {}", e),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::HealthChecker;
    use crate::metrics::MetricsRegistry;

    #[tokio::test]
    async fn test_health_endpoint() {
        let health_checker = Arc::new(HealthChecker::new());
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let state = ObservabilityState {
            health_checker,
            metrics,
        };

        let response = health_handler(State(state)).await;
        // Should return OK since no unhealthy components registered
        assert_eq!(response.status(), StatusCode::OK);
    }
}
