//! Materialized View API handlers — CREATE/DROP/QUERY/REFRESH materialized views.
#![allow(dead_code)]

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::AppState;
use nexora_core::materialized_view::*;
use nexora_id::PropertyValue;

// ============================================================
// Request/Response Types
// ============================================================

/// Request to create a materialized view
#[derive(Debug, Deserialize)]
pub struct CreateMaterializedViewRequest {
    /// View name
    pub name: String,
    /// Source Cypher query
    pub query: String,
    /// Refresh mode
    #[serde(default)]
    pub refresh_mode: RefreshModeRequest,
    /// Column schema (optional)
    #[serde(default)]
    pub schema: Vec<ColumnDefRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RefreshModeRequest {
    #[default]
    Incremental,
    Manual,
    Scheduled(String),
}

impl From<RefreshModeRequest> for RefreshMode {
    fn from(mode: RefreshModeRequest) -> Self {
        match mode {
            RefreshModeRequest::Incremental => RefreshMode::Incremental,
            RefreshModeRequest::Manual => RefreshMode::Manual,
            RefreshModeRequest::Scheduled(expr) => RefreshMode::Scheduled(expr),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ColumnDefRequest {
    pub name: String,
    pub data_type: String,
}

impl From<ColumnDefRequest> for ColumnDef {
    fn from(req: ColumnDefRequest) -> Self {
        let data_type = match req.data_type.as_str() {
            "string" => DataType::String,
            "integer" => DataType::Integer,
            "float" => DataType::Float,
            "boolean" => DataType::Boolean,
            "list" => DataType::List,
            "map" => DataType::Map,
            _ => DataType::String,
        };

        ColumnDef {
            name: req.name,
            data_type,
        }
    }
}

/// Response for created view
#[derive(Debug, Serialize)]
pub struct CreateMaterializedViewResponse {
    pub view_id: String,
    pub name: String,
    pub status: String,
}

/// Response for view query
#[derive(Debug, Serialize)]
pub struct QueryMaterializedViewResponse {
    pub view_id: String,
    pub rows: Vec<MaterializedRowResponse>,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct MaterializedRowResponse {
    pub key: String,
    pub values: serde_json::Value,
    pub version: u64,
    pub updated_at: String,
}

impl From<MaterializedRow> for MaterializedRowResponse {
    fn from(row: MaterializedRow) -> Self {
        let values: serde_json::Map<String, serde_json::Value> = row
            .values
            .iter()
            .map(|(k, v)| (k.clone(), property_value_to_json(v)))
            .collect();

        MaterializedRowResponse {
            key: row.key,
            values: serde_json::Value::Object(values),
            version: row.version,
            updated_at: row.updated_at.to_rfc3339(),
        }
    }
}

fn property_value_to_json(val: &PropertyValue) -> serde_json::Value {
    match val {
        PropertyValue::String(s) => serde_json::Value::String(s.to_string()),
        PropertyValue::Integer(i) => serde_json::Value::Number((*i).into()),
        PropertyValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::Number(0.into())),
        PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::List(items) => {
            serde_json::Value::Array(items.iter().map(property_value_to_json).collect())
        }
        PropertyValue::Bytes(_) => serde_json::Value::String("<binary>".to_string()),
        PropertyValue::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (k, v) in entries {
                map.insert(k.clone(), property_value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        PropertyValue::Node(_) => serde_json::Value::String("<node>".to_string()),
        PropertyValue::Relationship(_) => serde_json::Value::String("<relationship>".to_string()),
        PropertyValue::Path(_) => serde_json::Value::String("<path>".to_string()),
        PropertyValue::Date(d) => serde_json::Value::String(d.to_string()),
        PropertyValue::LocalDateTime(dt) => serde_json::Value::String(dt.to_string()),
        PropertyValue::ZonedDateTime(dt) => serde_json::Value::String(dt.to_rfc3339()),
        PropertyValue::Duration(_) => serde_json::Value::String("<duration>".to_string()),
        PropertyValue::Point(_) => serde_json::Value::String("<point>".to_string()),
        PropertyValue::BlobRef(_) => serde_json::Value::String("<blobref>".to_string()),
    }
}

/// Query parameters for listing views
#[derive(Debug, Deserialize)]
pub struct ListViewsQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Query parameters for scanning view
#[derive(Debug, Deserialize)]
pub struct ScanViewQuery {
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub column: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
}

// ============================================================
// API Handlers
// ============================================================

/// POST /api/v2/materialized-views — Create a new materialized view
///
/// Example:
/// ```json
/// {
///   "name": "high_speed_forklifts",
///   "query": "MATCH (n:Forklift) WHERE n.speed > 100 RETURN n.id, n.speed",
///   "refresh_mode": "incremental",
///   "schema": [
///     {"name": "id", "data_type": "string"},
///     {"name": "speed", "data_type": "float"}
///   ]
/// }
/// ```
pub async fn create_materialized_view(
    State(state): State<AppState>,
    Json(req): Json<CreateMaterializedViewRequest>,
) -> impl IntoResponse {
    let schema: Vec<ColumnDef> = req.schema.into_iter().map(|c| c.into()).collect();

    let view_id = match state
        .mv_manager
        .create_view(req.name.clone(), req.query, schema, req.refresh_mode.into())
        .await
    {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to create view: {}", e)
                })),
            )
                .into_response()
        }
    };

    (
        StatusCode::CREATED,
        Json(CreateMaterializedViewResponse {
            view_id,
            name: req.name,
            status: "created".to_string(),
        }),
    )
        .into_response()
}

/// GET /api/v2/materialized-views — List all materialized views
pub async fn list_materialized_views(
    State(state): State<AppState>,
    Query(_params): Query<ListViewsQuery>,
) -> impl IntoResponse {
    let views = state.mv_manager.list_views().await;

    let views_json: Vec<_> = views
        .iter()
        .map(|v| {
            serde_json::json!({
                "id": v.id,
                "name": v.name,
                "query": v.source_query,
                "refresh_mode": format!("{:?}", v.refresh_mode),
                "created_at": v.created_at.to_rfc3339(),
                "last_refreshed": v.last_refreshed.map(|t| t.to_rfc3339()),
            })
        })
        .collect();

    (StatusCode::OK, Json(views_json)).into_response()
}

/// GET /api/v2/materialized-views/:view_id — Get view definition
pub async fn get_materialized_view(
    State(state): State<AppState>,
    Path(view_id): Path<String>,
) -> impl IntoResponse {
    match state.mv_manager.get_view(&view_id).await {
        Some(view) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": view.id,
                "name": view.name,
                "query": view.source_query,
                "refresh_mode": format!("{:?}", view.refresh_mode),
                "created_at": view.created_at.to_rfc3339(),
                "last_refreshed": view.last_refreshed.map(|t| t.to_rfc3339()),
                "schema": view.schema.iter().map(|c| serde_json::json!({
                    "name": c.name,
                    "data_type": format!("{:?}", c.data_type)
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "View not found"
            })),
        )
            .into_response(),
    }
}

/// DELETE /api/v2/materialized-views/:view_id — Drop a materialized view
pub async fn drop_materialized_view(
    State(state): State<AppState>,
    Path(view_id): Path<String>,
) -> impl IntoResponse {
    match state.mv_manager.drop_view(&view_id).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "dropped",
                "view_id": view_id
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to drop view: {}", e)
            })),
        )
            .into_response(),
    }
}

/// GET /api/v2/materialized-views/:view_id/data — Query materialized view data
///
/// Query parameters:
/// - limit: Maximum number of rows to return
/// - column: Column name for filtered query
/// - value: Value to match (requires column parameter)
///
/// Examples:
/// - GET /api/v2/materialized-views/abc123/data?limit=100
/// - GET /api/v2/materialized-views/abc123/data?column=speed&value=120
pub async fn query_materialized_view(
    State(state): State<AppState>,
    Path(view_id): Path<String>,
    Query(params): Query<ScanViewQuery>,
) -> impl IntoResponse {
    let rows = if let (Some(column), Some(value_str)) = (params.column, params.value) {
        // Indexed query
        // Try to parse value as different types
        let value = if let Ok(i) = value_str.parse::<i64>() {
            PropertyValue::Integer(i)
        } else if let Ok(f) = value_str.parse::<f64>() {
            PropertyValue::Float(f)
        } else {
            PropertyValue::String(value_str)
        };

        match state
            .mv_manager
            .query_by_column(&view_id, &column, &value)
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Query failed: {}", e)
                    })),
                )
                    .into_response()
            }
        }
    } else {
        // Full scan
        match state.mv_manager.scan_view(&view_id, params.limit).await {
            Ok(rows) => rows,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("Query failed: {}", e)
                    })),
                )
                    .into_response()
            }
        }
    };

    let count = rows.len();
    let rows_response: Vec<_> = rows.into_iter().map(|r| r.into()).collect();

    (
        StatusCode::OK,
        Json(QueryMaterializedViewResponse {
            view_id,
            rows: rows_response,
            count,
        }),
    )
        .into_response()
}

/// POST /api/v2/materialized-views/:view_id/refresh — Manually refresh a materialized view
pub async fn refresh_materialized_view(
    State(state): State<AppState>,
    Path(view_id): Path<String>,
) -> impl IntoResponse {
    // Get view definition
    let view = match state.mv_manager.get_view(&view_id).await {
        Some(v) => v,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "View not found"
                })),
            )
                .into_response()
        }
    };

    // Execute the source query via the Cypher executor, then populate the view
    // with every result row. Each row is identified by its first column value.
    let cypher_result = match nexora_cypher::execute_cypher(&state.graph, &view.source_query).await
    {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(view_id = %view_id, error = %e, "MV refresh failed: source query error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Source query execution failed: {e}"),
                    "view_id": view_id,
                })),
            )
                .into_response();
        }
    };

    // Extract rows from the Cypher result
    let (columns, rows): (Vec<String>, Vec<Vec<serde_json::Value>>) = match cypher_result {
        nexora_cypher::CypherResult::Rows { columns, rows } => (columns, rows),
        _ => (vec![], vec![]),
    };

    tracing::info!(view_id = %view_id, query = %view.source_query, rows = rows.len(), "MV refresh: source query executed");

    // P0.3: Use replace_all for atomic full refresh — avoids stale rows
    // from a previous refresh when the source query produces fewer rows.
    let mv_rows: Vec<nexora_core::materialized_view::MaterializedRow> = rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let mut values = std::collections::HashMap::new();
            for (col_idx, val) in row.iter().enumerate() {
                let col_name = columns
                    .get(col_idx)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{col_idx}"));
                let pv = match val {
                    serde_json::Value::String(s) => PropertyValue::String(s.clone()),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            PropertyValue::Integer(i)
                        } else if let Some(f) = n.as_f64() {
                            PropertyValue::Float(f)
                        } else {
                            PropertyValue::String(n.to_string())
                        }
                    }
                    serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
                    serde_json::Value::Null => PropertyValue::Null,
                    other => PropertyValue::String(other.to_string()),
                };
                values.insert(col_name, pv);
            }
            nexora_core::materialized_view::MaterializedRow {
                key: format!("row_{idx}"),
                values,
                version: 1,
                updated_at: chrono::Utc::now(),
            }
        })
        .collect();

    match state.mv_manager.replace_all(&view_id, mv_rows).await {
        Ok(()) => {
            tracing::info!(view_id = %view_id, rows = rows.len(), "Materialized view refreshed");

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "status": "refreshed",
                    "view_id": view_id,
                    "query": view.source_query,
                    "rows": rows.len(),
                })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!(view_id = %view_id, error = %e, "MV refresh failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Refresh failed: {e}"),
                    "view_id": view_id,
                })),
            )
                .into_response()
        }
    }
}

/// Request to link a Standing Query to a Materialized View
#[derive(Debug, Deserialize)]
pub struct LinkSQRequest {
    pub sq_id: String,
}

/// POST /api/v2/materialized-views/:view_id/link-sq — Link a Standing Query to auto-update MV
pub async fn link_sq_to_mv(
    State(state): State<AppState>,
    Path(view_id): Path<String>,
    Json(req): Json<LinkSQRequest>,
) -> impl IntoResponse {
    // Verify view exists
    let view = match state.mv_manager.get_view(&view_id).await {
        Some(v) => v,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "Materialized view not found"
                })),
            )
                .into_response();
        }
    };

    // Register SQ → MV mapping
    state
        .sq_mv_bridge
        .register_sq_for_mv(req.sq_id.clone(), view_id.clone())
        .await;

    // ⭐ Bootstrap: Fill initial data from the source query
    let initial_count = bootstrap_mv_from_query(
        &state.graph,
        &state.mv_manager,
        &view_id,
        &view.source_query,
    )
    .await
    .unwrap_or(0);

    tracing::info!(
        sq_id = %req.sq_id,
        mv_id = %view_id,
        initial_rows = initial_count,
        "Linked Standing Query to Materialized View with initial data"
    );

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "linked",
            "view_id": view_id,
            "sq_id": req.sq_id,
            "initial_rows": initial_count
        })),
    )
        .into_response()
}

/// Bootstrap a materialized view by executing its source query
async fn bootstrap_mv_from_query(
    graph: &std::sync::Arc<nexora_core::GraphService>,
    mv_manager: &std::sync::Arc<nexora_core::materialized_view::MaterializedViewManager>,
    view_id: &str,
    query: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    tracing::info!(view_id = %view_id, query = %query, "Bootstrapping materialized view");

    // Execute the source query
    let result = nexora_cypher::execute_cypher(graph, query).await?;

    let mut count = 0;

    match result {
        nexora_cypher::CypherResult::Rows { columns, rows } => {
            // Convert each result row to MaterializedRow
            for row_values in rows {
                // Generate a unique key from the row data
                let key = generate_row_key(&columns, &row_values);

                // Convert JSON values to PropertyValue
                let mut properties = std::collections::HashMap::new();
                for (col_idx, col_name) in columns.iter().enumerate() {
                    if let Some(value) = row_values.get(col_idx) {
                        properties.insert(col_name.clone(), json_to_property_value(value));
                    }
                }

                let mv_row = nexora_core::materialized_view::MaterializedRow {
                    key,
                    values: properties,
                    version: 1,
                    updated_at: chrono::Utc::now(),
                };

                mv_manager.upsert_row(view_id, mv_row).await?;
                count += 1;
            }
        }
        _ => {
            tracing::warn!("Bootstrap query returned non-row result");
        }
    }

    tracing::info!(view_id = %view_id, count = count, "Materialized view bootstrapped");
    Ok(count)
}

/// Generate a unique key for a row based on its data
fn generate_row_key(columns: &[String], values: &[serde_json::Value]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        hasher.update(col.as_bytes());
        hasher.update(val.to_string().as_bytes());
    }
    let hash = hasher.finalize();
    format!("{:x}", hash)
}

/// Convert JSON value to PropertyValue
fn json_to_property_value(value: &serde_json::Value) -> nexora_id::PropertyValue {
    match value {
        serde_json::Value::Null => nexora_id::PropertyValue::Null,
        serde_json::Value::Bool(b) => nexora_id::PropertyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                nexora_id::PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                nexora_id::PropertyValue::Float(f)
            } else {
                nexora_id::PropertyValue::Null
            }
        }
        serde_json::Value::String(s) => nexora_id::PropertyValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            let items: Vec<_> = arr.iter().map(json_to_property_value).collect();
            nexora_id::PropertyValue::List(items)
        }
        serde_json::Value::Object(obj) => {
            let map: std::collections::BTreeMap<_, _> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_property_value(v)))
                .collect();
            nexora_id::PropertyValue::Map(map)
        }
    }
}

// ============================================================
// SQL DDL Support
// ============================================================

/// Request to execute SQL DDL
#[derive(Debug, Deserialize)]
pub struct SqlDdlRequest {
    pub sql: String,
}

/// POST /api/v2/sql/ddl — Execute SQL DDL (CREATE/DROP MATERIALIZED VIEW)
pub async fn execute_sql_ddl(
    state: State<AppState>,
    Json(req): Json<SqlDdlRequest>,
) -> axum::response::Response {
    let sql = req.sql.trim();

    // Check if it's CREATE MATERIALIZED VIEW
    if sql.to_uppercase().starts_with("CREATE MATERIALIZED VIEW") {
        match crate::sql_ddl_parser::parse_create_mv(sql) {
            Ok(stmt) => {
                // Convert to REST API request
                let create_req = CreateMaterializedViewRequest {
                    name: stmt.view_name,
                    query: stmt.query,
                    refresh_mode: match stmt.refresh_mode {
                        RefreshMode::Incremental => RefreshModeRequest::Incremental,
                        RefreshMode::Manual => RefreshModeRequest::Manual,
                        RefreshMode::Scheduled(_) => RefreshModeRequest::Incremental, // Default to incremental
                    },
                    schema: stmt
                        .schema
                        .into_iter()
                        .map(|col| ColumnDefRequest {
                            name: col.name,
                            data_type: match col.data_type {
                                DataType::String => "string".to_string(),
                                DataType::Integer => "integer".to_string(),
                                DataType::Float => "float".to_string(),
                                DataType::Boolean => "boolean".to_string(),
                                DataType::List => "string".to_string(), // Default to string
                                DataType::Map => "string".to_string(),  // Default to string
                            },
                        })
                        .collect(),
                };

                // Call existing handler directly
                create_materialized_view(state, Json(create_req))
                    .await
                    .into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("SQL parse error: {}", e)
                })),
            )
                .into_response(),
        }
    }
    // Check if it's DROP MATERIALIZED VIEW
    else if sql.to_uppercase().starts_with("DROP MATERIALIZED VIEW") {
        match crate::sql_ddl_parser::parse_drop_mv(sql) {
            Ok(view_name) => {
                // Find view by name
                let views = state.mv_manager.list_views().await;
                if let Some(view) = views.iter().find(|v| v.name == view_name) {
                    // Delete the view
                    match state.mv_manager.drop_view(&view.id).await {
                        Ok(_) => (
                            StatusCode::OK,
                            Json(json!({
                                "status": "dropped",
                                "view_id": &view.id,
                                "view_name": view_name
                            })),
                        )
                            .into_response(),
                        Err(e) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({
                                "error": format!("Failed to drop view: {}", e)
                            })),
                        )
                            .into_response(),
                    }
                } else {
                    (
                        StatusCode::NOT_FOUND,
                        Json(json!({
                            "error": format!("Materialized view '{}' not found", view_name)
                        })),
                    )
                        .into_response()
                }
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("SQL parse error: {}", e)
                })),
            )
                .into_response(),
        }
    } else {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Unsupported SQL DDL. Supported: CREATE MATERIALIZED VIEW, DROP MATERIALIZED VIEW"
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_refresh_mode_serialization() {
        let json = r#"{"name":"test","query":"MATCH (n) RETURN n","refresh_mode":"incremental"}"#;
        let req: CreateMaterializedViewRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req.refresh_mode, RefreshModeRequest::Incremental));
    }

    #[test]
    fn test_column_def_conversion() {
        let req = ColumnDefRequest {
            name: "age".to_string(),
            data_type: "integer".to_string(),
        };
        let col: ColumnDef = req.into();
        assert_eq!(col.name, "age");
        assert!(matches!(col.data_type, DataType::Integer));
    }
}
