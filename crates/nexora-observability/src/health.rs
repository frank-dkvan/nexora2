//! Health check implementation for Nexora components
//!
//! Provides /health (liveness) and /ready (readiness) endpoints

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Overall health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// All components healthy
    Healthy,
    /// Some components degraded but service operational
    Degraded,
    /// Critical components down, service unavailable
    Unhealthy,
}

/// Health status of a single component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name (e.g., "raft", "rocksdb", "iceberg")
    pub name: String,
    /// Health status
    pub status: HealthStatus,
    /// Optional human-readable message
    pub message: Option<String>,
    /// Last check timestamp (Unix epoch seconds)
    pub last_check: i64,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    /// Overall status
    pub status: HealthStatus,
    /// Individual component statuses
    pub components: HashMap<String, ComponentHealth>,
    /// Server uptime in seconds
    pub uptime_seconds: u64,
}

/// Health checker trait - implement for each component
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    /// Check component health
    async fn check(&self) -> ComponentHealth;
}

/// Central health checker aggregating all components
pub struct HealthChecker {
    checks: Arc<RwLock<HashMap<String, Arc<dyn HealthCheck>>>>,
    start_time: std::time::Instant,
}

impl HealthChecker {
    /// Create new health checker
    pub fn new() -> Self {
        Self {
            checks: Arc::new(RwLock::new(HashMap::new())),
            start_time: std::time::Instant::now(),
        }
    }

    /// Register a component health check
    pub async fn register(&self, name: String, check: Arc<dyn HealthCheck>) {
        self.checks.write().await.insert(name, check);
    }

    /// Check all components and aggregate status
    pub async fn check_all(&self) -> HealthCheckResponse {
        let checks = self.checks.read().await;
        let mut components = HashMap::new();
        let mut overall_status = HealthStatus::Healthy;

        // Check all components concurrently
        let futures: Vec<_> = checks
            .iter()
            .map(|(name, check)| {
                let name = name.clone();
                let check = Arc::clone(check);
                async move {
                    let health = check.check().await;
                    (name, health)
                }
            })
            .collect();

        let results = futures::future::join_all(futures).await;

        for (name, health) in results {
            // Aggregate status: any unhealthy -> overall unhealthy
            // any degraded -> overall degraded (if not already unhealthy)
            match health.status {
                HealthStatus::Unhealthy => overall_status = HealthStatus::Unhealthy,
                HealthStatus::Degraded if overall_status == HealthStatus::Healthy => {
                    overall_status = HealthStatus::Degraded
                }
                _ => {}
            }
            components.insert(name, health);
        }

        HealthCheckResponse {
            status: overall_status,
            components,
            uptime_seconds: self.start_time.elapsed().as_secs(),
        }
    }

    /// Liveness check - is the process alive?
    /// Returns healthy if we can respond (basic aliveness)
    pub async fn liveness(&self) -> HealthStatus {
        HealthStatus::Healthy
    }

    /// Readiness check - can we serve traffic?
    /// Returns unhealthy if any critical component is down
    pub async fn readiness(&self) -> HealthStatus {
        let response = self.check_all().await;
        response.status
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockHealthCheck {
        status: HealthStatus,
    }

    #[async_trait::async_trait]
    impl HealthCheck for MockHealthCheck {
        async fn check(&self) -> ComponentHealth {
            ComponentHealth {
                name: "mock".to_string(),
                status: self.status,
                message: None,
                last_check: chrono::Utc::now().timestamp(),
            }
        }
    }

    #[tokio::test]
    async fn test_health_checker_aggregation() {
        let checker = HealthChecker::new();

        checker
            .register(
                "healthy".to_string(),
                Arc::new(MockHealthCheck {
                    status: HealthStatus::Healthy,
                }),
            )
            .await;

        checker
            .register(
                "degraded".to_string(),
                Arc::new(MockHealthCheck {
                    status: HealthStatus::Degraded,
                }),
            )
            .await;

        let response = checker.check_all().await;
        assert_eq!(response.status, HealthStatus::Degraded);
        assert_eq!(response.components.len(), 2);
    }

    #[tokio::test]
    async fn test_unhealthy_takes_precedence() {
        let checker = HealthChecker::new();

        checker
            .register(
                "healthy".to_string(),
                Arc::new(MockHealthCheck {
                    status: HealthStatus::Healthy,
                }),
            )
            .await;

        checker
            .register(
                "unhealthy".to_string(),
                Arc::new(MockHealthCheck {
                    status: HealthStatus::Unhealthy,
                }),
            )
            .await;

        let response = checker.check_all().await;
        assert_eq!(response.status, HealthStatus::Unhealthy);
    }
}
