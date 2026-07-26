//! RisingWave HTTP API handlers.
//!
//! These endpoints are only available when compiled with --features risingwave
//! and --enable-risingwave is set at runtime.

#[cfg(feature = "risingwave")]
use crate::error::ApiError;
#[cfg(feature = "risingwave")]
use crate::handlers::AppState;
#[cfg(feature = "risingwave")]
use axum::{extract::State, Json};
#[cfg(feature = "risingwave")]
use serde::{Deserialize, Serialize};

/// Request to execute RisingWave DDL
#[cfg(feature = "risingwave")]
#[derive(Debug, Deserialize)]
pub struct RisingWaveDdlRequest {
    /// SQL DDL statement (CREATE SOURCE, CREATE MATERIALIZED VIEW, etc.)
    pub sql: String,
}

/// Response from DDL execution
#[cfg(feature = "risingwave")]
#[derive(Debug, Serialize)]
pub struct RisingWaveDdlResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Request to query RisingWave materialized view
#[cfg(feature = "risingwave")]
#[derive(Debug, Deserialize)]
pub struct RisingWaveQueryRequest {
    /// SQL SELECT query
    pub sql: String,
}

/// Response from query execution
#[cfg(feature = "risingwave")]
#[derive(Debug, Serialize)]
pub struct RisingWaveQueryResponse {
    /// Query results as JSON string (Phase 5: simplified)
    /// Phase 6 will return structured rows
    pub results: String,
}

/// RisingWave source information
#[cfg(feature = "risingwave")]
#[derive(Debug, Serialize)]
pub struct RisingWaveSource {
    pub name: String,
    pub connector: String,
    pub status: String,
}

/// RisingWave materialized view information
#[cfg(feature = "risingwave")]
#[derive(Debug, Serialize)]
pub struct RisingWaveMaterializedView {
    pub name: String,
    pub definition: String,
    pub status: String,
}

/// RisingWave cluster status
#[cfg(feature = "risingwave")]
#[derive(Debug, Serialize)]
pub struct RisingWaveStatus {
    pub enabled: bool,
    pub meta_leader: bool,
    pub version: String,
}

/// Execute RisingWave DDL statement
///
/// POST /api/risingwave/ddl
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/risingwave/ddl \
///   -H "Content-Type: application/json" \
///   -d '{
///     "sql": "CREATE SOURCE my_kafka WITH (connector = '\''kafka'\'', ...)"
///   }'
/// ```
#[cfg(feature = "risingwave")]
pub async fn execute_ddl(
    State(state): State<AppState>,
    Json(req): Json<RisingWaveDdlRequest>,
) -> Result<Json<RisingWaveDdlResponse>, ApiError> {
    let rw = state
        .risingwave
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;

    rw.execute_ddl(&req.sql)
        .await
        .map_err(|e| ApiError::Internal(format!("RisingWave DDL failed: {}", e)))?;

    Ok(Json(RisingWaveDdlResponse {
        success: true,
        message: Some("DDL executed successfully".to_string()),
    }))
}

/// Query RisingWave materialized view
///
/// POST /api/risingwave/query
///
/// # Example
///
/// ```bash
/// curl -X POST http://localhost:8080/api/risingwave/query \
///   -H "Content-Type: application/json" \
///   -d '{
///     "sql": "SELECT * FROM my_mv LIMIT 10"
///   }'
/// ```
#[cfg(feature = "risingwave")]
pub async fn query_mv(
    State(state): State<AppState>,
    Json(req): Json<RisingWaveQueryRequest>,
) -> Result<Json<RisingWaveQueryResponse>, ApiError> {
    let rw = state
        .risingwave
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;

    let results = rw
        .query_mv(&req.sql)
        .await
        .map_err(|e| ApiError::Internal(format!("RisingWave query failed: {}", e)))?;

    Ok(Json(RisingWaveQueryResponse { results }))
}

/// List RisingWave sources
///
/// GET /api/risingwave/sources
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/risingwave/sources
/// ```
#[cfg(feature = "risingwave")]
pub async fn list_sources(
    State(state): State<AppState>,
) -> Result<Json<Vec<RisingWaveSource>>, ApiError> {
    let rw = state
        .risingwave
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;

    let sources = rw.list_sources().await
        .map_err(|e| ApiError::Internal(format!("Failed to list sources: {}", e)))?;

    let response = sources
        .iter()
        .map(|s| RisingWaveSource {
            name: s.name.clone(),
            connector: s.connector.clone(),
            status: "active".to_string(),
        })
        .collect();

    Ok(Json(response))
}

/// List RisingWave materialized views
///
/// GET /api/risingwave/materialized_views
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/risingwave/materialized_views
/// ```
#[cfg(feature = "risingwave")]
pub async fn list_materialized_views(
    State(state): State<AppState>,
) -> Result<Json<Vec<RisingWaveMaterializedView>>, ApiError> {
    let rw = state
        .risingwave
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;

    let mvs = rw.list_materialized_views().await
        .map_err(|e| ApiError::Internal(format!("Failed to list materialized views: {}", e)))?;

    let response = mvs
        .iter()
        .map(|mv| RisingWaveMaterializedView {
            name: mv.name.clone(),
            definition: mv.definition.clone(),
            status: "active".to_string(),
        })
        .collect();

    Ok(Json(response))
}

/// Get RisingWave cluster status
///
/// GET /api/risingwave/status
///
/// # Example
///
/// ```bash
/// curl http://localhost:8080/api/risingwave/status
/// ```
#[cfg(feature = "risingwave")]
pub async fn get_status(
    State(state): State<AppState>,
) -> Result<Json<RisingWaveStatus>, ApiError> {
    let rw = state
        .risingwave
        .as_ref()
        .ok_or_else(|| ApiError::FeatureNotEnabled("risingwave".to_string()))?;

    let meta_leader = rw.is_leader().await;

    Ok(Json(RisingWaveStatus {
        enabled: true,
        meta_leader,
        version: "v3.0.2".to_string(),
    }))
}

#[cfg(test)]
#[cfg(feature = "risingwave")]
mod tests {
    use super::*;

    #[test]
    fn test_risingwave_request_serialization() {
        let req = RisingWaveDdlRequest {
            sql: "CREATE SOURCE test".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("CREATE SOURCE test"));
    }

    #[test]
    fn test_risingwave_response_serialization() {
        let resp = RisingWaveDdlResponse {
            success: true,
            message: Some("OK".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("true"));
        assert!(json.contains("OK"));
    }
}
