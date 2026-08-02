//! Observability infrastructure for Nexora
//!
//! Provides:
//! - Prometheus metrics collection and export
//! - OpenTelemetry distributed tracing
//! - Health check endpoints
//! - Structured logging integration

pub mod health;
pub mod metrics;
pub mod tracing;
pub mod server;

pub use health::{HealthChecker, HealthStatus, ComponentHealth};
pub use metrics::MetricsRegistry;
pub use server::ObservabilityServer;
