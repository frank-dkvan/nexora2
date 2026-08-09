// Example: Integrating observability into nexora-app
//
// This demonstrates how to:
// 1. Initialize health checks for Raft, RocksDB, Iceberg
// 2. Instrument code with metrics
// 3. Start the observability HTTP server

use nexora_observability::health::HealthCheck;
use nexora_observability::{
    ComponentHealth, HealthChecker, HealthStatus, MetricsRegistry, ObservabilityServer,
};
use std::net::SocketAddr;
use std::sync::Arc;

// Example: Raft health check implementation
struct RaftHealthCheck {
    // In real code: reference to Raft node
}

#[async_trait::async_trait]
impl HealthCheck for RaftHealthCheck {
    async fn check(&self) -> ComponentHealth {
        // Check if Raft node is operational
        // In real code: check if node is leader/follower, not isolated
        let is_healthy = true; // Placeholder

        ComponentHealth {
            name: "raft".to_string(),
            status: if is_healthy {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: Some("Raft node operational".to_string()),
            last_check: chrono::Utc::now().timestamp(),
        }
    }
}

// Example: RocksDB health check
struct RocksDBHealthCheck {
    // In real code: reference to RocksDB instance
}

#[async_trait::async_trait]
impl HealthCheck for RocksDBHealthCheck {
    async fn check(&self) -> ComponentHealth {
        // Try a simple read operation
        let is_healthy = true; // Placeholder

        ComponentHealth {
            name: "rocksdb".to_string(),
            status: if is_healthy {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: Some("RocksDB operational".to_string()),
            last_check: chrono::Utc::now().timestamp(),
        }
    }
}

/// Initialize observability infrastructure
pub async fn init_observability(
    bind_addr: SocketAddr,
) -> anyhow::Result<(Arc<HealthChecker>, Arc<MetricsRegistry>)> {
    // Create health checker
    let health_checker = Arc::new(HealthChecker::new());

    // Register component health checks
    health_checker
        .register("raft".to_string(), Arc::new(RaftHealthCheck {}))
        .await;
    health_checker
        .register("rocksdb".to_string(), Arc::new(RocksDBHealthCheck {}))
        .await;

    // Create metrics registry
    let metrics = Arc::new(MetricsRegistry::new()?);

    // Start observability HTTP server
    let server =
        ObservabilityServer::new(bind_addr, Arc::clone(&health_checker), Arc::clone(&metrics));

    tokio::spawn(async move {
        if let Err(e) = server.serve().await {
            tracing::error!("Observability server error: {}", e);
        }
    });

    tracing::info!("Observability endpoints available at http://{}", bind_addr);
    tracing::info!("  GET /health   - Liveness probe");
    tracing::info!("  GET /ready    - Readiness probe");
    tracing::info!("  GET /metrics  - Prometheus metrics");

    Ok((health_checker, metrics))
}

// Example: Instrumenting query execution with metrics
pub async fn execute_query_with_metrics(
    _query: &str,
    metrics: &MetricsRegistry,
) -> anyhow::Result<String> {
    use std::time::Instant;

    // Track active queries
    metrics.active_queries.inc();

    // Time the query
    let start = Instant::now();
    let query_type = "cypher"; // or "sql", "gremlin"

    // Execute query (placeholder)
    let result = "query result".to_string();

    // Record metrics
    let duration = start.elapsed().as_secs_f64();
    metrics
        .query_duration_seconds
        .with_label_values(&[query_type])
        .observe(duration);

    metrics
        .query_total
        .with_label_values(&[query_type, "success"])
        .inc();

    // Done — decrement the active-query gauge.
    metrics.active_queries.dec();

    Ok(result)
}

fn main() {
    // This file is a documentation example of how to wire observability into
    // nexora-app; the functions above are the reference. Nothing to run here.
    println!("See the functions in this file for observability integration examples.");
}

// Example: Instrumenting WAL fsync with metrics
pub async fn wal_fsync_with_metrics(metrics: &MetricsRegistry) -> anyhow::Result<()> {
    use std::time::Instant;

    let start = Instant::now();

    // Perform fsync (placeholder)
    // In real code: file.sync_all()?

    let duration = start.elapsed().as_secs_f64();
    metrics.wal_sync_duration_seconds.observe(duration);
    metrics.wal_fsync_total.inc();

    Ok(())
}
