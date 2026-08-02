//! Integration with nexora-app: GraphStreaming HTTP endpoints
//!
//! Provides REST API for managing projection rules and monitoring metrics.

use crate::{EventProjector, ProjectionMetrics, ProjectionRule};
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Request to load projection rules
#[derive(Debug, Deserialize)]
pub struct LoadProjectionsRequest {
    /// Directory containing YAML projection files
    pub rules_dir: String,
}

/// Response with loaded projection rules
#[derive(Debug, Serialize)]
pub struct LoadProjectionsResponse {
    pub success: bool,
    pub rules_loaded: usize,
    pub message: String,
}

/// Response with projection metrics
#[derive(Debug, Serialize)]
pub struct ProjectionMetricsResponse {
    pub projections: Vec<ProjectionInfo>,
}

/// Information about a single projection
#[derive(Debug, Serialize)]
pub struct ProjectionInfo {
    pub name: String,
    pub source_topic: String,
    pub status: String,
    pub metrics: ProjectionMetrics,
}

/// List all active projections
///
/// GET /api/graph-streaming/projections
pub async fn list_projections(
    State(projector): State<Arc<EventProjector>>,
) -> Json<Vec<String>> {
    let active = projector.get_active_projections();
    Json(active)
}

/// Get projection metrics
///
/// GET /api/graph-streaming/metrics
pub async fn get_projection_metrics(
    State(projector): State<Arc<EventProjector>>,
) -> Json<ProjectionMetricsResponse> {
    let metrics = projector.get_metrics();

    let projections = metrics
        .into_iter()
        .map(|(name, m)| ProjectionInfo {
            name: name.clone(),
            source_topic: "unknown".to_string(), // TODO: Track topic with metrics
            status: "running".to_string(),
            metrics: m,
        })
        .collect();

    Json(ProjectionMetricsResponse { projections })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_projection_info_serialization() {
        let info = ProjectionInfo {
            name: "test_rule".to_string(),
            source_topic: "test.topic".to_string(),
            status: "running".to_string(),
            metrics: ProjectionMetrics {
                events_processed: 100,
                nodes_created: 80,
                edges_created: 50,
                errors: 2,
            },
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("test_rule"));
        assert!(json.contains("100"));
    }
}
