// --- API response types ---

export interface HealthResponse {
  status: string;
  mode: string;
  active_nodes: number;
  shards: number;
  standing_queries: number;
  readiness: string;
  liveness: string;
  durability: string;
  version: string;
  uptime_seconds: number;
}

export interface CypherResponse {
  columns: string[];
  rows: Array<Array<unknown>>;
  error?: string;
  as_of?: number;
  message?: string;
}

export interface NodePropertyResponse {
  value?: unknown;
  error?: string;
}

export interface EdgeResponse {
  edges: EdgeItem[];
  error?: string;
}

export interface EdgeItem {
  edge_type: string;
  direction: 'out' | 'in' | 'undirected';
  other: string;
}

export interface StandingQueryItem {
  id: string;
  name: string;
  match_count: number;
  pattern?: unknown;
}

export interface StandingQueryList {
  standing_queries: StandingQueryItem[];
}

export interface SqPatternCondition {
  type: 'GreaterThan' | 'LessThan' | 'Equals' | 'Contains' | 'Exists' | 'StartsWith';
  value?: unknown;
}

export interface SqPattern {
  type: 'PropertyFilter' | 'LabelFilter' | 'EdgePattern' | 'And' | 'Or' | 'Not';
  key?: string;
  condition?: SqPatternCondition;
  labels?: string[];
  patterns?: SqPattern[];
}

export interface SqCreateRequest {
  name: string;
  pattern: SqPattern;
}

export interface FileIngestRequest {
  path: string;
  id_field: string;
}

export interface FileIngestResponse {
  path: string;
  id_field: string;
  events_processed: number;
  status: string;
}

export interface SystemInfoResponse {
  version: string;
  rust_version: string;
  mode: string;
  num_shards: number;
  max_nodes_per_shard: number;
  rocksdb_path?: string;
  wal_dir?: string;
}

export interface MetricsResponse {
  active_nodes: number;
  standing_queries: number;
  events_total: number;
  sq_matches_total: number;
  errors_total: number;
  fragment_count: number;
  wal_append_total: number;
  wal_append_avg_us: number;
}

export interface GraphEvent {
  qid: string;
  key: string;
  value: unknown;
  event_time: string;
}

export interface GraphHistoryResponse {
  active_node_count: number;
  time_travel: string;
}

// --- Vector search types ---

export interface VectorIndexRequest {
  qid: string;
  vector: number[];
}

export interface VectorSearchRequest {
  vector: number[];
  k?: number;
}

export interface VectorSearchResult {
  qid: string;
  distance: number;
}

export interface VectorSearchResponse {
  results: VectorSearchResult[];
}

export interface VectorGetResponse {
  qid: string;
  vector: number[];
}

// --- Materialized view types ---

export interface MvColumnDef {
  name: string;
  data_type: string;
}

export interface MvDefinition {
  view_id: string;
  name: string;
  columns: MvColumnDef[];
  refresh_mode: string;
  linked_sq_id?: string;
  row_count: number;
  last_refreshed_at?: string;
}

export interface MvListResponse {
  views: MvDefinition[];
}

export interface MvCreateRequest {
  name: string;
  columns: MvColumnDef[];
  refresh_mode: string;
}

export interface MvDataResponse {
  rows: Record<string, unknown>[];
  columns: string[];
}

// --- Cluster types ---

export interface ClusterNodeInfo {
  node_id: string;
  address: string;
  status: 'alive' | 'dead' | 'suspected';
  is_local: boolean;
}

export interface ShardInfo {
  shard_id: number;
  owner_node: string;
  follower_nodes: string[];
  epoch: number;
}

export interface ClusterStatsResponse {
  node_id: string;
  nodes: ClusterNodeInfo[];
  shards: ShardInfo[];
  local_shard_count: number;
  total_nodes: number;
  alive_nodes: number;
}

// --- EXPLAIN types ---

export interface ExplainPlanNode {
  operator: string;
  estimated_rows: number;
  children?: ExplainPlanNode[];
  details?: Record<string, unknown>;
}

export interface ExplainResponse {
  plan: ExplainPlanNode;
  query: string;
  is_read_only: boolean;
}

// --- UDF types ---

export interface UdfInfo {
  name: string;
  language: string;
}

export interface UdfListResponse {
  udfs: UdfInfo[];
}

// --- Recipe types ---

export interface RecipeInfo {
  name: string;
  description: string;
  version: string;
  num_standing_queries: number;
  num_ingest_sources: number;
}

export interface RecipeListResponse {
  recipes: RecipeInfo[];
}

// --- Pagination wrapper ---

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
  has_more: boolean;
}

// --- API error ---

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public details?: unknown,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

// --- Message types for Cypher WebSocket ---

export interface WsQueryRequest {
  type: 'RunQuery';
  queryId: string;
  query: string;
  language: string;
}

export interface WsQueryCancel {
  type: 'CancelQuery';
  queryId: string;
}

export type WsClientMessage = WsQueryRequest | WsQueryCancel;

export interface WsTabularResults {
  type: 'TabularResults';
  queryId: string;
  columns: string[];
  results: Array<Array<unknown>>;
}

export interface WsNodeResults {
  type: 'NodeResults';
  queryId: string;
  results: Array<{
    id: string;
    hostIndex: number;
    label: string;
    properties: Record<string, unknown>;
  }>;
}

export interface WsQueryStarted {
  type: 'QueryStarted';
  queryId: string;
  isReadOnly: boolean;
  columns: string[];
}

export interface WsQueryFinished {
  type: 'QueryFinished';
  queryId: string;
}

export interface WsQueryFailed {
  type: 'QueryFailed';
  queryId: string;
  message: string;
}

export type WsServerMessage =
  | WsTabularResults
  | WsNodeResults
  | WsQueryStarted
  | WsQueryFinished
  | WsQueryFailed
  | { type: 'MessageOk' }
  | { type: 'MessageError'; error: string };

export interface SlowQueryStats {
  total_queries: number;
  slow_queries: number;
  slow_query_ratio: number;
  threshold_ms: number;
  note?: string;
}
