//! Observability infrastructure for Nexora
//!
//! Provides:
//! - Prometheus metrics collection and export
//! - OpenTelemetry distributed tracing
//! - Health check endpoints
//! - Structured logging integration

pub mod health;
pub mod metrics;
pub mod server;
pub mod tracing;

pub use health::{ComponentHealth, HealthChecker, HealthStatus};
pub use metrics::MetricsRegistry;
pub use server::ObservabilityServer;
