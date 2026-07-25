/**
 * Type definitions for the Nexora TypeScript SDK.
 *
 * These interfaces describe the request/response shapes for the Nexora
 * HTTP API. Field names match the server's JSON keys exactly.
 */

// ---------------------------------------------------------------------------
// Client configuration
// ---------------------------------------------------------------------------

export interface NexoraClientConfig {
  /** Base URL of the Nexora server (default `http://localhost:8080`). */
  baseUrl?: string;
  /** Bearer token for authentication. */
  apiKey?: string;
  /** Default request timeout in milliseconds (default 30000). */
  timeout?: number;
  /** Maximum retry attempts on 5xx / connection errors (default 3). */
  maxRetries?: number;
  /** Base delay for exponential backoff in milliseconds (default 500). */
  backoffFactor?: number;
  /** Extra default headers to send with every request. */
  headers?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Graph — Node & Edge
// ---------------------------------------------------------------------------

export interface Node {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
}

export interface Edge {
  from_id: string;
  to_id: string;
  edge_type: string;
  properties: Record<string, unknown>;
}

export type EdgeDirection = 'out' | 'in' | 'both';

/** Request body for POST /api/v2/graph/node/{qid}/edges */
export interface AddEdgeRequest {
  label: string;
  target: string;
  properties?: Record<string, unknown>;
}

/** Edge as returned by GET /api/v2/graph/node/{qid}/edges */
export interface EdgeInfo {
  edge_type: string;
  direction: string;
  other: string;
  properties?: Record<string, unknown>;
}

/** PUT /api/v2/graph/node/{qid}/property/{key} */
export interface SetPropertyRequest {
  value: unknown;
}

/** Response from GET /api/v2/graph/node/{qid}/property/{key} */
export interface GetPropertyResponse {
  value: unknown;
  not_found?: boolean;
}

/** GET /api/v2/graph/history */
export interface HistoryRequest {
  qid: string;
  as_of: number;
}

export interface HistoryResponse {
  qid: string;
  as_of: number;
  properties?: Record<string, unknown>;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Query — Cypher, SQL, Explain
// ---------------------------------------------------------------------------

/** POST /api/v2/query/cypher */
export interface CypherQueryRequest {
  query: string;
  params?: Record<string, unknown>;
}

/** Raw server response from the cypher endpoint (positional rows). */
export interface CypherRawResponse {
  columns: string[];
  rows: unknown[][];
  error?: string;
  duration_ms?: number;
}

/** Normalised query result — rows are keyed by column name. */
export interface CypherResult {
  columns: string[];
  rows: Record<string, unknown>[];
  durationMs?: number;
}

/** POST /api/v2/query/sql */
export interface SqlQueryRequest {
  query: string;
}

export interface SqlResult {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  query_time_ms?: number;
  translated_cypher?: string;
}

/** POST /api/v2/query/explain */
export interface ExplainRequest {
  query: string;
  analyze?: boolean;
}

export interface ExplainResult {
  query: string;
  plan: string;
  estimated_cost?: number;
  estimated_rows?: number;
  explanation?: string;
  actual_stats?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Standing Queries
// ---------------------------------------------------------------------------

export type FilterConditionType =
  | 'GreaterThan'
  | 'LessThan'
  | 'Equals'
  | 'Contains'
  | 'Exists'
  | 'IsNull'
  | 'IsNotNull';

export interface FilterCondition {
  type: FilterConditionType;
  value?: unknown;
}

export type StandingQueryType = 'PropertyFilter' | 'LabelFilter';

export interface StandingQueryPattern {
  type: StandingQueryType;
  key?: string;
  condition?: FilterCondition;
  labels?: string[];
}

/** POST /api/v2/standing-query */
export interface CreateStandingQueryRequest {
  pattern: StandingQueryPattern;
  name?: string;
}

export interface CreateStandingQueryResponse {
  id: string;
  name?: string;
}

export interface StandingQueryInfo {
  id: string;
  name: string;
  match_count: number;
}

export interface ListStandingQueriesResponse {
  standing_queries: StandingQueryInfo[];
}

// ---------------------------------------------------------------------------
// Vector
// ---------------------------------------------------------------------------

/** POST /api/v2/vector/index */
export interface VectorIndexRequest {
  qid: string;
  vector: number[];
}

export interface VectorIndexResponse {
  status: string;
  qid: string;
  index_size: number;
}

/** POST /api/v2/vector/search */
export interface VectorSearchRequest {
  vector: number[];
  k?: number;
}

export interface VectorNeighbor {
  qid: string;
  distance: number;
}

export interface VectorSearchResponse {
  query: unknown;
  k: number;
  neighbors: VectorNeighbor[];
}

export interface VectorGetResponse {
  qid: string;
  vector: number[];
}

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

/** POST /api/v2/ingest/file */
export interface IngestFileRequest {
  name: string;
  path: string;
  format: string;
}

export interface IngestFileResponse {
  status: string;
  name: string;
  path: string;
}

export interface IngestInfo {
  name: string;
  status: string;
  path: string;
  format: string;
}

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

/** POST /api/v2/recipes */
export interface CreateRecipeRequest {
  name: string;
  config: Record<string, unknown>;
}

export interface CreateRecipeResponse {
  status: string;
  name: string;
  recipe_count?: number;
}

export interface Recipe {
  name: string;
  config: Record<string, unknown>;
  status?: string;
  description?: string;
  steps?: RecipeStep[];
}

export interface RecipeStep {
  query: string;
  description?: string;
}

export interface RecipeExecuteResponse {
  run_id: string;
  recipe: string;
  status: string;
  result?: unknown;
}

export interface RecipeRun {
  run_id: string;
  status: string;
  started_at?: string;
  finished_at?: string;
  result?: unknown;
}

// ---------------------------------------------------------------------------
// UDF
// ---------------------------------------------------------------------------

/** POST /api/v2/udf/register */
export interface RegisterUdfRequest {
  name: string;
  code: string;
  language: string;
}

export interface RegisterUdfResponse {
  status: string;
  name: string;
  language: string;
}

/** POST /api/v2/udf/execute */
export interface ExecuteUdfRequest {
  name: string;
  args?: unknown[];
}

export interface ExecuteUdfResponse {
  result: unknown;
  name: string;
}

export interface UdfInfo {
  name: string;
  language: string;
}

export interface ListUdfsResponse {
  udfs: UdfInfo[];
}

// ---------------------------------------------------------------------------
// Materialized Views
// ---------------------------------------------------------------------------

export interface MaterializedViewColumn {
  name: string;
  data_type: string;
}

/** POST /api/v2/materialized-views */
export interface CreateMaterializedViewRequest {
  name: string;
  query: string;
  refresh_mode?: string;
  schema?: MaterializedViewColumn[];
}

export interface CreateMaterializedViewResponse {
  view_id: string;
  name: string;
  status: string;
}

export interface MaterializedView {
  id: string;
  name: string;
  query: string;
  refresh_mode: string;
  created_at?: string;
  last_refreshed?: string;
  schema?: MaterializedViewColumn[];
}

export interface MaterializedViewData {
  view_id: string;
  rows: Record<string, unknown>[];
  count: number;
}

export interface MaterializedViewActionResponse {
  status: string;
  view_id: string;
  query?: string;
}

/** POST /api/v2/materialized-views/{view_id}/link-sq */
export interface LinkSqRequest {
  standing_query_id: string;
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

export interface StorageStatus {
  backend: string;
  total_objects: number;
  total_size_bytes?: number;
  hot_objects?: number;
  warm_objects?: number;
  cold_objects?: number;
}

export interface StorageMigrateResponse {
  status: string;
  migrated_objects: number;
}

// ---------------------------------------------------------------------------
// System & Health
// ---------------------------------------------------------------------------

export interface SystemInfo {
  version: string;
  rust_version?: string;
  mode?: string;
  num_shards?: number;
  max_nodes_per_shard?: number;
  [key: string]: unknown;
}

export interface SystemConfig {
  [key: string]: unknown;
}

export interface HealthStatus {
  status: string;
  active_nodes?: number;
  shards?: number;
  standing_queries?: number;
  mode?: string;
  [key: string]: unknown;
}

export interface ReadinessStatus {
  status: string;
  ready: boolean;
}

export interface LivenessStatus {
  status: string;
  alive: boolean;
}

export interface ClusterStats {
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/** POST /api/v2/auth/token */
export interface GenerateTokenRequest {
  role?: string;
  expires_in?: number;
}

export interface TokenResponse {
  token: string;
  role?: string;
  expires_in?: number;
}

// ---------------------------------------------------------------------------
// WebSocket messages
// ---------------------------------------------------------------------------

/** Generic message envelope for /api/v2/ws/sq/{id} */
export interface StandingQueryMessage {
  type?: string;
  match_count?: number;
  qid?: string;
  data?: unknown;
  [key: string]: unknown;
}

/** Generic message envelope for /api/v2/ws/query */
export interface CypherStreamMessage {
  type: string;
  columns?: string[];
  row?: unknown[];
  rows?: unknown[][];
  error?: string;
  [key: string]: unknown;
}
