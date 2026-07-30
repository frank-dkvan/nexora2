//! Iceberg REST Catalog HTTP endpoints
//!
//! This module implements the Iceberg REST Catalog v1 specification,
//! exposing RisingWave's internal Iceberg metadata as standard HTTP endpoints.
//!
//! Reference: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::ApiError;
use crate::AppState;

/// Iceberg REST Catalog configuration response
#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogConfig {
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub overrides: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub defaults: HashMap<String, String>,
}

/// List namespaces response
#[derive(Debug, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
}

/// Namespace properties response
#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceResponse {
    pub namespace: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

/// Create namespace request
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

/// List tables response
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
}

/// Table identifier
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

/// Load table response
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LoadTableResponse {
    pub metadata_location: Option<String>,
    pub metadata: TableMetadata,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub config: HashMap<String, String>,
}

/// Simplified table metadata (subset of full Iceberg spec)
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    pub current_schema_id: i32,
    pub schemas: Vec<Schema>,
    pub partition_spec: Vec<PartitionField>,
    pub default_spec_id: i32,
    pub last_partition_id: Option<i32>,
    pub properties: HashMap<String, String>,
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<Snapshot>,
    pub snapshot_log: Vec<SnapshotLog>,
    pub metadata_log: Vec<MetadataLog>,
    pub sort_orders: Vec<SortOrder>,
    pub default_sort_order_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionField {
    #[serde(rename = "field-id")]
    pub field_id: i32,
    pub name: String,
    pub transform: String,
    #[serde(rename = "source-id")]
    pub source_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotLog {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetadataLog {
    pub metadata_file: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortOrder {
    pub order_id: i32,
    pub fields: Vec<SortField>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SortField {
    pub transform: String,
    pub source_id: i32,
    pub direction: String,
    pub null_order: String,
}

/// Create Iceberg REST catalog routes
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/config", get(get_config))
        .route(
            "/v1/namespaces",
            get(list_namespaces).post(create_namespace),
        )
        .route("/v1/namespaces/{namespace}", get(get_namespace))
        .route(
            "/v1/namespaces/{namespace}/tables",
            get(list_tables).post(create_table),
        )
        .route("/v1/namespaces/{namespace}/tables/{table}", get(load_table))
}

/// GET /v1/config
///
/// Returns catalog configuration
async fn get_config() -> Result<Json<CatalogConfig>, ApiError> {
    Ok(Json(CatalogConfig {
        overrides: HashMap::new(),
        defaults: HashMap::new(),
    }))
}

/// GET /v1/namespaces
///
/// List all namespaces in the catalog
async fn list_namespaces(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListNamespacesResponse>, ApiError> {
    // Get RisingWave module
    let risingwave = state
        .event_streaming
        .as_ref()
        .ok_or(ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    // Query RisingWave Meta's iceberg_tables
    let tables = risingwave.list_hosted_iceberg_tables().await.map_err(|e| {
        ApiError::InternalServerError(format!("Failed to list Iceberg tables: {}", e))
    })?;

    // Extract unique namespaces
    let mut namespaces = std::collections::HashSet::new();
    for table in tables {
        // table_namespace is a dot-separated string like "nexora_db"
        // Convert to Vec<String> for Iceberg REST spec
        let ns_parts: Vec<String> = table
            .table_namespace
            .split('.')
            .map(|s| s.to_string())
            .collect();
        namespaces.insert(ns_parts);
    }

    let namespaces: Vec<Vec<String>> = namespaces.into_iter().collect();

    Ok(Json(ListNamespacesResponse { namespaces }))
}

/// POST /v1/namespaces
///
/// Create a new namespace
async fn create_namespace(
    State(_state): State<Arc<AppState>>,
    Json(request): Json<CreateNamespaceRequest>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    // RisingWave creates namespaces automatically when first table is created
    // For now, just return success
    Ok(Json(NamespaceResponse {
        namespace: request.namespace,
        properties: request.properties,
    }))
}

/// GET /v1/namespaces/{namespace}
///
/// Get namespace properties
async fn get_namespace(
    State(_state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<NamespaceResponse>, ApiError> {
    let ns_parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();

    Ok(Json(NamespaceResponse {
        namespace: ns_parts,
        properties: HashMap::new(),
    }))
}

/// GET /v1/namespaces/{namespace}/tables
///
/// List all tables in a namespace
async fn list_tables(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<ListTablesResponse>, ApiError> {
    let risingwave = state
        .event_streaming
        .as_ref()
        .ok_or(ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    let tables = risingwave.list_hosted_iceberg_tables().await.map_err(|e| {
        ApiError::InternalServerError(format!("Failed to list Iceberg tables: {}", e))
    })?;

    // Filter tables by namespace
    let identifiers: Vec<TableIdentifier> = tables
        .into_iter()
        .filter(|t| t.table_namespace == namespace)
        .map(|t| TableIdentifier {
            namespace: t
                .table_namespace
                .split('.')
                .map(|s| s.to_string())
                .collect(),
            name: t.table_name,
        })
        .collect();

    Ok(Json(ListTablesResponse { identifiers }))
}

/// POST /v1/namespaces/{namespace}/tables
///
/// Create a new table (stub implementation)
async fn create_table(
    State(_state): State<Arc<AppState>>,
    Path(_namespace): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Tables are created by RisingWave sinks automatically
    // Return 501 Not Implemented for now
    Err(ApiError::NotImplemented(
        "Table creation is handled by RisingWave CREATE SINK",
    ))
}

/// GET /v1/namespaces/{namespace}/tables/{table}
///
/// Load table metadata
async fn load_table(
    State(state): State<Arc<AppState>>,
    Path((namespace, table_name)): Path<(String, String)>,
) -> Result<Json<LoadTableResponse>, ApiError> {
    let risingwave = state
        .event_streaming
        .as_ref()
        .ok_or(ApiError::FeatureNotEnabled("event-streaming".to_string()))?;

    let tables = risingwave.list_hosted_iceberg_tables().await.map_err(|e| {
        ApiError::InternalServerError(format!("Failed to list Iceberg tables: {}", e))
    })?;

    // Find matching table
    let table = tables
        .into_iter()
        .find(|t| t.table_namespace == namespace && t.table_name == table_name)
        .ok_or(ApiError::NotFound(format!(
            "Table {}.{} not found",
            namespace, table_name
        )))?;

    // TODO: Parse actual metadata from metadata_location S3 file
    // For now, return minimal metadata
    let metadata = TableMetadata {
        format_version: 2,
        table_uuid: uuid::Uuid::new_v4().to_string(),
        location: format!("s3://nexora-events/{}/{}", namespace, table_name),
        current_schema_id: 0,
        schemas: vec![],
        partition_spec: vec![],
        default_spec_id: 0,
        last_partition_id: None,
        properties: HashMap::new(),
        current_snapshot_id: None,
        snapshots: vec![],
        snapshot_log: vec![],
        metadata_log: vec![],
        sort_orders: vec![],
        default_sort_order_id: 0,
    };

    Ok(Json(LoadTableResponse {
        metadata_location: table.metadata_location,
        metadata,
        config: HashMap::new(),
    }))
}
