//! Request and response types for the nexora API.
//!
//! These types mirror the request/response shapes defined in
//! `nexora-app::handlers` and the OpenAPI specification.

use serde::{Deserialize, Serialize};

// ============================================================
// Health
// ============================================================

/// Response from `GET /api/v2/health`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub mode: String,
    pub profile: Option<String>,
    pub active_nodes: u64,
    pub shards: u64,
    pub standing_queries: u64,
    pub readiness: String,
    pub liveness: String,
    pub durability: String,
    pub version: String,
    pub uptime_seconds: u64,
}

/// Response from `GET /api/v2/health/ready`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub shards: u64,
}

/// Response from `GET /api/v2/health/live`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessResponse {
    pub alive: bool,
}

// ============================================================
// Cypher Query
// ============================================================

/// Request body for `POST /api/v2/query/cypher`.
#[derive(Debug, Clone, Serialize)]
pub struct CypherRequest {
    pub query: String,
}

impl CypherRequest {
    /// Create a new Cypher query request.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }
}

/// Write operation statistics (Neo4j-compatible).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub properties_set: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

/// Response from `POST /api/v2/query/cypher`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CypherResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub error: Option<String>,
    pub as_of: Option<u64>,
    pub write_stats: Option<WriteStats>,
}

// ============================================================
// SQL Query
// ============================================================

/// Request body for `POST /api/v2/query/sql`.
#[derive(Debug, Clone, Serialize)]
pub struct SqlRequest {
    pub query: String,
}

impl SqlRequest {
    /// Create a new SQL query request.
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
        }
    }
}

/// Response from `POST /api/v2/query/sql`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlResponse {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
    pub row_count: usize,
    pub query_time_ms: u64,
    pub translated_cypher: Option<String>,
    pub error: Option<String>,
}

// ============================================================
// EXPLAIN
// ============================================================

/// Request body for `POST /api/v2/query/explain`.
#[derive(Debug, Clone, Serialize)]
pub struct ExplainRequest {
    pub query: String,
    #[serde(default)]
    pub analyze: bool,
}

/// Actual execution statistics returned when `analyze = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActualStats {
    pub actual_rows: usize,
    pub execution_time_ms: f64,
    pub nodes_examined: usize,
}

/// Execution plan returned by EXPLAIN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlanResponse {
    pub start_with: String,
    pub filters: Vec<String>,
    pub cost: f64,
}

/// Response from `POST /api/v2/query/explain`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResponse {
    pub query: String,
    pub plan: ExecutionPlanResponse,
    pub estimated_cost: f64,
    pub estimated_rows: usize,
    pub explanation: String,
    pub actual_stats: Option<ActualStats>,
}

// ============================================================
// Graph — Property CRUD
// ============================================================

/// Request body for `PUT /api/v2/graph/node/{qid}/property/{key}`.
#[derive(Debug, Clone, Serialize)]
pub struct SetPropertyRequest {
    pub value: serde_json::Value,
}

/// Response from `GET /api/v2/graph/node/{qid}/property/{key}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPropertyResponse {
    pub node_id: String,
    pub key: String,
    pub value: serde_json::Value,
    pub not_found: Option<bool>,
}

// ============================================================
// Graph — Edges
// ============================================================

/// Request body for `POST /api/v2/graph/node/{qid}/edges`.
#[derive(Debug, Clone, Serialize)]
pub struct AddEdgeRequest {
    pub edge_type: String,
    pub target: String,
    pub direction: String,
}

impl AddEdgeRequest {
    /// Create a new edge request. `direction` must be `"out"` or `"in"`.
    pub fn new(
        edge_type: impl Into<String>,
        target: impl Into<String>,
        direction: impl Into<String>,
    ) -> Self {
        Self {
            edge_type: edge_type.into(),
            target: target.into(),
            direction: direction.into(),
        }
    }
}

/// A single edge returned by `GET /api/v2/graph/node/{qid}/edges`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeInfo {
    pub edge_type: String,
    pub direction: String,
    pub other: String,
}

/// Response from `GET /api/v2/graph/node/{qid}/edges`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetEdgesResponse {
    pub edges: Vec<EdgeInfo>,
}

// ============================================================
// Standing Queries
// ============================================================

/// Pattern definition for a standing query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqPatternRequest {
    #[serde(rename = "type")]
    pub pattern_type: String,
    pub key: Option<String>,
    pub condition: Option<serde_json::Value>,
    pub labels: Option<Vec<String>>,
}

/// Request body for `POST /api/v2/standing-query`.
#[derive(Debug, Clone, Serialize)]
pub struct CreateSqRequest {
    pub name: String,
    pub pattern: SqPatternRequest,
}

/// A standing query in list output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandingQueryInfo {
    pub id: String,
    pub name: String,
    pub created_at: String,
    pub match_count: u64,
}

/// Response from `GET /api/v2/standing-query`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListStandingQueriesResponse {
    pub standing_queries: Vec<StandingQueryInfo>,
}

/// Response from `POST /api/v2/standing-query`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSqResponse {
    pub id: String,
    pub name: String,
}

// ============================================================
// Vector Search
// ============================================================

/// Request body for `POST /api/v2/vector/index`.
#[derive(Debug, Clone, Serialize)]
pub struct VectorInsertRequest {
    pub qid: String,
    pub vector: Vec<f32>,
}

/// Request body for `POST /api/v2/vector/search`.
#[derive(Debug, Clone, Serialize)]
pub struct VectorSearchRequest {
    pub vector: Vec<f32>,
    pub k: usize,
}

/// A single nearest-neighbor result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorNeighbor {
    pub qid: String,
    pub distance: f64,
}

/// Response from `POST /api/v2/vector/search`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResponse {
    pub query: Vec<f32>,
    pub k: usize,
    pub neighbors: Vec<VectorNeighbor>,
}

/// Response from `POST /api/v2/vector/index`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexResponse {
    pub status: String,
    pub qid: String,
    pub index_size: usize,
}

/// Response from `GET /api/v2/vector/node/{qid}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorGetResponse {
    pub qid: String,
    pub vector: Vec<f32>,
}

/// Response from `DELETE /api/v2/vector/node/{qid}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorDeleteResponse {
    pub status: String,
    pub qid: String,
    pub index_size: usize,
}

// ============================================================
// Ingest
// ============================================================

/// Request body for `POST /api/v2/ingest/file`.
#[derive(Debug, Clone, Serialize)]
pub struct FileIngestRequest {
    pub path: String,
    #[serde(default = "default_id_field")]
    pub id_field: String,
}

#[allow(dead_code)]
fn default_id_field() -> String {
    "id".to_string()
}

/// Response from `POST /api/v2/ingest/file`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileIngestResponse {
    pub status: String,
    pub path: String,
    pub name: String,
}

/// Response from `GET /api/v2/ingest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListIngestsResponse {
    pub ingests: Vec<String>,
    pub count: usize,
}

/// Response from `DELETE /api/v2/ingest/{name}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteIngestResponse {
    pub status: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Request for `POST /api/v2/ingest/bulk` — a batch of JSON record objects.
#[derive(Debug, Clone, Serialize)]
pub struct BulkIngestRequest {
    pub records: Vec<serde_json::Value>,
    #[serde(default = "default_id_field")]
    pub id_field: String,
}

/// Response from `POST /api/v2/ingest/bulk`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkIngestResponse {
    pub status: String,
    #[serde(default)]
    pub ingested: usize,
    #[serde(default)]
    pub nodes: usize,
    #[serde(default)]
    pub skipped: usize,
}

// ============================================================
// Streams
// ============================================================

/// A stream source in list output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSourceInfo {
    pub name: String,
    pub source_type: String,
    pub topic: String,
    pub brokers: String,
    pub started_at: String,
}

/// Response from `GET /api/v2/streams`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListStreamsResponse {
    pub streams: Vec<StreamSourceInfo>,
    pub count: usize,
}

/// Request body for `POST /api/v2/streams/kafka`.
#[derive(Debug, Clone, Serialize)]
pub struct KafkaStreamRequest {
    pub brokers: String,
    pub topic: String,
    #[serde(default = "default_group_id")]
    pub group_id: String,
}

#[allow(dead_code)]
fn default_group_id() -> String {
    "nexora-app-consumer".to_string()
}

/// Response from `DELETE /api/v2/streams/{name}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStreamResponse {
    pub status: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

// ============================================================
// Recipes
// ============================================================

/// A single step in a recipe definition.
#[derive(Debug, Clone, Serialize)]
pub struct RecipeStepDef {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A trigger definition for a recipe.
#[derive(Debug, Clone, Serialize)]
pub struct RecipeTriggerDef {
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
}

/// Request body for `POST /api/v2/recipes`.
#[derive(Debug, Clone, Serialize)]
pub struct CreateRecipeRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub steps: Vec<RecipeStepDef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<RecipeTriggerDef>,
}

/// A recipe summary in list output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeSummary {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub num_standing_queries: usize,
    pub num_ingest_sources: usize,
    pub num_outputs: usize,
    pub has_trigger: bool,
}

/// Response from `GET /api/v2/recipes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRecipesResponse {
    pub recipes: Vec<RecipeSummary>,
    pub count: usize,
}

/// Response from `POST /api/v2/recipes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRecipeResponse {
    pub status: String,
    pub name: String,
    pub recipe_count: usize,
}

/// Response from `DELETE /api/v2/recipes/{name}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecipeResponse {
    pub status: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

/// Response from `POST /api/v2/recipes/{name}/execute`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRecipeResponse {
    pub run_id: String,
    pub recipe: String,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// A recipe run record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeRunRecord {
    pub run_id: String,
    pub recipe_name: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// Response from `GET /api/v2/recipes/{name}/runs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRecipeRunsResponse {
    pub recipe: String,
    pub runs: Vec<RecipeRunRecord>,
    pub total_runs: usize,
}

// ============================================================
// UDF (User-Defined Functions)
// ============================================================

/// Request body for `POST /api/v2/udf/register`.
#[derive(Debug, Clone, Serialize)]
pub struct UdfRegisterRequest {
    pub name: String,
    pub code: String,
    #[serde(default = "default_language")]
    pub language: String,
}

#[allow(dead_code)]
fn default_language() -> String {
    "native".to_string()
}

/// Request body for `POST /api/v2/udf/execute`.
#[derive(Debug, Clone, Serialize)]
pub struct UdfExecuteRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,
}

/// UDF info in list output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdfInfo {
    pub name: String,
    pub language: String,
}

// ============================================================
// Materialized Views
// ============================================================

/// Column definition for a materialized view.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnDefRequest {
    pub name: String,
    pub data_type: String,
}

/// Request body for `POST /api/v2/materialized-views`.
#[derive(Debug, Clone, Serialize)]
pub struct CreateMaterializedViewRequest {
    pub name: String,
    pub query: String,
    #[serde(default)]
    pub refresh_mode: String,
    #[serde(default)]
    pub schema: Vec<ColumnDefRequest>,
}

/// Response from `POST /api/v2/materialized-views`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateMaterializedViewResponse {
    pub view_id: String,
    pub name: String,
    pub status: String,
}

/// A materialized view summary in list output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewSummary {
    pub id: String,
    pub name: String,
    pub query: String,
    pub refresh_mode: String,
    pub created_at: String,
    pub last_refreshed: Option<String>,
}

/// A row in a materialized view query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedRowResponse {
    pub key: String,
    pub values: serde_json::Value,
    pub version: u64,
    pub updated_at: String,
}

/// Response from `GET /api/v2/materialized-views/{view_id}/data`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMaterializedViewResponse {
    pub view_id: String,
    pub rows: Vec<MaterializedRowResponse>,
    pub count: usize,
}

/// Request body for linking a standing query to a materialized view.
#[derive(Debug, Clone, Serialize)]
pub struct LinkSqRequest {
    pub sq_id: String,
}

// ============================================================
// SQL DDL
// ============================================================

/// Request body for `POST /api/v2/sql/ddl`.
#[derive(Debug, Clone, Serialize)]
pub struct SqlDdlRequest {
    pub sql: String,
}

// ============================================================
// Storage
// ============================================================

/// Response from `GET /api/v2/storage/status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatusResponse {
    pub backend: String,
    pub total_objects: u64,
    pub total_size_bytes: u64,
    pub hot_objects: u64,
    pub warm_objects: u64,
    pub cold_objects: u64,
    pub note: Option<String>,
}

/// Response from `POST /api/v2/storage/migrate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMigrateResponse {
    pub status: Option<String>,
    pub migrated_objects: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
}

// ============================================================
// System
// ============================================================

/// Response from `GET /api/v2/system/info`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfoResponse {
    pub version: String,
    pub rust_version: Option<String>,
    pub mode: String,
    pub num_shards: u64,
    pub max_nodes_per_shard: u64,
    pub rocksdb_path: Option<String>,
    pub wal_dir: Option<String>,
}

/// Response from `GET /api/v2/system/config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfigResponse {
    pub version: String,
    pub host: String,
    pub port: u16,
}

// ============================================================
// Auth
// ============================================================

/// Request body for `POST /api/v2/auth/token`.
#[derive(Debug, Clone, Serialize)]
pub struct TokenRequest {
    pub user_id: String,
    #[serde(default = "default_role")]
    pub role: String,
}

#[allow(dead_code)]
fn default_role() -> String {
    "operator".to_string()
}

/// Response from `POST /api/v2/auth/token`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenResponse {
    pub token: String,
    pub user_id: String,
    pub role: String,
}

// ============================================================
// Cluster
// ============================================================

/// Response from `GET /api/v2/cluster/stats` (dynamic shape).
pub type ClusterStatsResponse = serde_json::Value;

/// Response from `GET /api/v2/cluster/raft` (dynamic shape).
pub type RaftStatusResponse = serde_json::Value;

// ============================================================
// Metrics
// ============================================================

/// Response from `GET /api/v2/metrics` (dynamic shape).
pub type MetricsJsonResponse = serde_json::Value;

// ============================================================
// Time Travel
// ============================================================

/// Response from `GET /api/v2/graph/history`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeTravelResponse {
    pub active_nodes: u64,
    pub time_travel: String,
}
