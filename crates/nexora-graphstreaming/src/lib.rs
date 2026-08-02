//! GraphStreaming Layer: Automatic event-to-graph projection
//!
//! This crate implements the bridge between `nexora-eventlog` (Iceberg event tables)
//! and `nexora-core` (graph database). It reads events from event streams and applies
//! user-defined projection rules to automatically create/update graph nodes and edges.
//!
//! ## Architecture
//!
//! ```text
//! nexora-eventlog (Iceberg)
//!         │
//!         ▼ stream events
//! EventProjector (applies rules)
//!         │
//!         ▼ generate mutations
//! GraphMutationBuilder
//!         │
//!         ▼ update
//! nexora-core (Graph)
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use nexora_graphstreaming::{EventProjector, ProjectionRule};
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Load projection rules from YAML
//! let rules = ProjectionRule::load_from_file("projections/cargo.yaml").await?;
//!
//! // Create projector
//! let projector = EventProjector::new(
//!     rules,
//!     event_store.clone(),
//!     graph_service.clone(),
//! );
//!
//! // Start streaming projection
//! projector.start().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod event_projector;
pub mod graph_mutation;
pub mod handlers;
pub mod performance;
pub mod projection_rule;
pub mod template_engine;

// Re-exports
pub use config::GraphStreamingConfig;
pub use event_projector::{EventProjector, ProjectionMetrics};
pub use graph_mutation::GraphMutationBuilder;
pub use performance::{MutationBatch, PerformanceConfig, TemplateCache};
pub use projection_rule::{EdgeProjection, NodeProjection, ProjectionRule};
pub use template_engine::TemplateEngine;

// HTTP handlers (re-export for nexora-app)
pub use handlers::{get_projection_metrics, list_projections};

/// Errors that can occur in graph streaming operations
#[derive(Debug, thiserror::Error)]
pub enum GraphStreamingError {
    #[error("Template rendering error: {0}")]
    TemplateError(String),

    #[error("Invalid projection rule: {0}")]
    InvalidRule(String),

    #[error("Graph mutation error: {0}")]
    GraphError(String),

    #[error("Event parsing error: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("YAML parsing error: {0}")]
    YamlError(#[from] serde_yaml::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, GraphStreamingError>;
