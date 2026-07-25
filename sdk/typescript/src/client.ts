/**
 * Main client for the Nexora streaming graph database.
 *
 * Uses the native `fetch` API — no external dependencies. Works in Node 18+
 * and modern browsers.
 *
 * @example
 * ```ts
 * import { NexoraClient } from 'nexora';
 *
 * const client = new NexoraClient('http://localhost:8080');
 * await client.setProperty('alice', 'name', 'Alice');
 * const name = await client.getProperty('alice', 'name');
 * const rows = await client.query('MATCH (n) RETURN n LIMIT 10');
 * ```
 */

import {
  AddEdgeRequest,
  ClusterStats,
  CreateMaterializedViewRequest,
  CreateMaterializedViewResponse,
  CreateRecipeRequest,
  CreateRecipeResponse,
  CreateStandingQueryRequest,
  CreateStandingQueryResponse,
  CypherResult,
  EdgeInfo,
  ExecuteUdfRequest,
  ExecuteUdfResponse,
  ExplainRequest,
  ExplainResult,
  GenerateTokenRequest,
  GetPropertyResponse,
  HealthStatus,
  HistoryResponse,
  IngestFileRequest,
  IngestFileResponse,
  IngestInfo,
  LivenessStatus,
  LinkSqRequest,
  ListStandingQueriesResponse,
  MaterializedView,
  MaterializedViewActionResponse,
  MaterializedViewData,
  NexoraClientConfig,
  ReadinessStatus,
  Recipe,
  RecipeExecuteResponse,
  RecipeRun,
  RegisterUdfRequest,
  RegisterUdfResponse,
  SqlResult,
  StorageMigrateResponse,
  StorageStatus,
  StandingQueryInfo,
  SystemConfig,
  SystemInfo,
  TokenResponse,
  UdfInfo,
  VectorGetResponse,
  VectorIndexRequest,
  VectorIndexResponse,
  VectorSearchRequest,
  VectorSearchResponse,
} from './types';
import {
  AuthenticationError,
  ConnectionError,
  NodeNotFoundError,
  NexoraError,
  QueryError,
  TimeoutError,
} from './errors';

/** Default configuration values. */
const DEFAULTS = {
  baseUrl: 'http://localhost:8080',
  timeout: 30_000,
  maxRetries: 3,
  backoffFactor: 500,
} as const;

export class NexoraClient {
  readonly baseUrl: string;
  readonly apiKey?: string;
  readonly timeout: number;
  readonly maxRetries: number;
  readonly backoffFactor: number;
  private readonly defaultHeaders: Record<string, string>;

  constructor(config?: NexoraClientConfig);
  constructor(baseUrl?: string, apiKey?: string);
  constructor(configOrUrl?: NexoraClientConfig | string, apiKey?: string) {
    if (typeof configOrUrl === 'string') {
      this.baseUrl = (configOrUrl || DEFAULTS.baseUrl).replace(/\/$/, '');
      this.apiKey = apiKey;
      this.timeout = DEFAULTS.timeout;
      this.maxRetries = DEFAULTS.maxRetries;
      this.backoffFactor = DEFAULTS.backoffFactor;
      this.defaultHeaders = {};
    } else {
      const c = configOrUrl ?? {};
      this.baseUrl = (c.baseUrl ?? DEFAULTS.baseUrl).replace(/\/$/, '');
      this.apiKey = c.apiKey;
      this.timeout = c.timeout ?? DEFAULTS.timeout;
      this.maxRetries = c.maxRetries ?? DEFAULTS.maxRetries;
      this.backoffFactor = c.backoffFactor ?? DEFAULTS.backoffFactor;
      this.defaultHeaders = c.headers ?? {};
    }
  }

  // ------------------------------------------------------------------
  // Low-level request helper
  // ------------------------------------------------------------------

  /**
   * Execute an HTTP request with retry and error mapping.
   *
   * Retries on connection errors and HTTP 5xx responses up to
   * `maxRetries` times, with exponential backoff.
   */
  async request<T = unknown>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    let lastError: NexoraError | undefined;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        return await this.doRequest<T>(method, path, body);
      } catch (err) {
        const qe = err as NexoraError;

        // Non-retryable errors — re-raise immediately
        if (
          qe instanceof AuthenticationError ||
          qe instanceof NodeNotFoundError ||
          qe instanceof QueryError
        ) {
          throw err;
        }

        lastError = qe;

        if (attempt < this.maxRetries) {
          const delay = this.backoffFactor * Math.pow(2, attempt);
          await this.sleep(delay);
          continue;
        }
        throw err;
      }
    }

    throw lastError ?? new NexoraError('Unexpected state in retry loop');
  }

  /** Execute a single HTTP request (no retry). */
  private async doRequest<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const url = this.baseUrl + path;
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...this.defaultHeaders,
    };
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    let resp: Response;
    try {
      resp = await fetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        throw new TimeoutError(`Request to ${path} timed out`);
      }
      throw new ConnectionError(`Cannot connect to ${this.baseUrl}: ${(err as Error).message}`);
    } finally {
      clearTimeout(timer);
    }

    const text = await resp.text();
    let parsed: unknown;
    try {
      parsed = text ? JSON.parse(text) : {};
    } catch {
      parsed = { raw: text };
    }

    if (resp.status >= 400) {
      const bodyObj = parsed as Record<string, unknown>;
      const errorMsg =
        (typeof bodyObj?.error === 'string' && bodyObj.error) ||
        (typeof bodyObj?.raw === 'string' && bodyObj.raw) ||
        text;

      if (resp.status === 401 || resp.status === 403) {
        throw new AuthenticationError(errorMsg, {
          statusCode: resp.status,
          responseBody: parsed,
        });
      }
      if (resp.status === 404) {
        throw new NodeNotFoundError(errorMsg, {
          statusCode: resp.status,
          responseBody: parsed,
        });
      }
      if (resp.status === 400) {
        throw new QueryError(errorMsg, {
          statusCode: resp.status,
          responseBody: parsed,
        });
      }
      throw new NexoraError(errorMsg, {
        statusCode: resp.status,
        responseBody: parsed,
      });
    }

    return parsed as T;
  }

  // ------------------------------------------------------------------
  // ID encoding
  // ------------------------------------------------------------------

  /**
   * Encode a string node ID to hexadecimal.
   * Nexora uses hex-encoded node IDs in URL paths.
   */
  static hexId(nodeId: string): string {
    let hex = '';
    for (let i = 0; i < nodeId.length; i++) {
      hex += nodeId.charCodeAt(i).toString(16).padStart(2, '0');
    }
    return hex;
  }

  private hexId(nodeId: string): string {
    return NexoraClient.hexId(nodeId);
  }

  // ------------------------------------------------------------------
  // Utility
  // ------------------------------------------------------------------

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private static substituteParams(
    cypher: string,
    params?: Record<string, unknown>,
  ): string {
    if (!params) return cypher;
    return cypher.replace(/\$(\w+)/g, (match, key: string) => {
      if (key in params) {
        const val = params[key];
        if (typeof val === 'string') return JSON.stringify(val);
        return String(val);
      }
      return match;
    });
  }

  // ------------------------------------------------------------------
  // Query APIs
  // ------------------------------------------------------------------

  /**
   * Execute a Cypher query and return rows as dictionaries.
   *
   * @param cypher  Cypher query string.
   * @param params  Optional parameters for client-side `$param` substitution.
   * @returns List of row objects keyed by column name.
   */
  async query(
    cypher: string,
    params?: Record<string, unknown>,
  ): Promise<CypherResult> {
    const queryStr = NexoraClient.substituteParams(cypher, params);
    const resp = await this.request<{
      columns?: string[];
      rows?: unknown[][];
      error?: string;
      duration_ms?: number;
    }>('POST', '/api/v2/query/cypher', { query: queryStr });

    if (resp.error) {
      throw new QueryError(resp.error);
    }

    const columns = resp.columns ?? [];
    const rawRows = resp.rows ?? [];

    const rows: Record<string, unknown>[] = rawRows.map((row) => {
      if (Array.isArray(row)) {
        const obj: Record<string, unknown> = {};
        for (let i = 0; i < columns.length; i++) {
          obj[columns[i]] = row[i];
        }
        return obj;
      }
      if (row !== null && typeof row === 'object') {
        return row as Record<string, unknown>;
      }
      return { value: row };
    });

    return { columns, rows, durationMs: resp.duration_ms };
  }

  /**
   * Execute a Cypher query and return the first row, or `undefined`.
   */
  async queryOne(
    cypher: string,
    params?: Record<string, unknown>,
  ): Promise<Record<string, unknown> | undefined> {
    const result = await this.query(cypher, params);
    return result.rows[0];
  }

  /** Execute a Cypher query (alias for {@link query}). */
  async cypher(
    cypher: string,
    params?: Record<string, unknown>,
  ): Promise<CypherResult> {
    return this.query(cypher, params);
  }

  /**
   * Execute a SQL query.
   * @param query SQL query string.
   */
  async sql(query: string): Promise<SqlResult> {
    return this.request<SqlResult>('POST', '/api/v2/query/sql', { query });
  }

  /**
   * Generate an execution plan for a Cypher query.
   * @param query    Cypher query string to explain.
   * @param analyze If `true`, also execute and include actual stats.
   */
  async explain(query: string, analyze?: boolean): Promise<ExplainResult> {
    const body: ExplainRequest = { query, analyze };
    return this.request<ExplainResult>('POST', '/api/v2/query/explain', body);
  }

  // ------------------------------------------------------------------
  // Graph — Node properties
  // ------------------------------------------------------------------

  /**
   * Set a property on a node.
   * @param nodeId Node identifier string.
   * @param key    Property key.
   * @param value  Property value (any JSON-serializable value).
   */
  async setProperty(nodeId: string, key: string, value: unknown): Promise<void> {
    const qid = this.hexId(nodeId);
    await this.request('PUT', `/api/v2/graph/node/${qid}/property/${key}`, {
      value,
    });
  }

  /**
   * Get a property value from a node.
   * @returns The property value, or `undefined` if not found.
   */
  async getProperty(nodeId: string, key: string): Promise<unknown> {
    const qid = this.hexId(nodeId);
    const resp = await this.request<GetPropertyResponse>(
      'GET',
      `/api/v2/graph/node/${qid}/property/${key}`,
    );
    if (resp.not_found) return undefined;
    return resp.value;
  }

  // ------------------------------------------------------------------
  // Graph — Edges
  // ------------------------------------------------------------------

  /**
   * Get edges connected to a node.
   * @param nodeId    Node identifier string.
   * @param direction `"out"`, `"in"`, or `"both"` (default `"both"`).
   */
  async getEdges(
    nodeId: string,
    direction?: 'out' | 'in' | 'both',
  ): Promise<EdgeInfo[]> {
    const qid = this.hexId(nodeId);
    const resp = await this.request<{ edges?: EdgeInfo[] }>(
      'GET',
      `/api/v2/graph/node/${qid}/edges`,
    );
    const edges = resp.edges ?? [];
    if (direction && direction !== 'both') {
      return edges.filter((e) => e.direction === direction);
    }
    return edges;
  }

  /**
   * Add a directed edge from a node to a target node.
   * @param nodeId   Source node identifier string.
   * @param label    Edge label / type (e.g. `"KNOWS"`).
   * @param target   Target node identifier string (will be hex-encoded).
   * @param properties Optional edge properties.
   */
  async addEdge(
    nodeId: string,
    label: string,
    target: string,
    properties?: Record<string, unknown>,
  ): Promise<void> {
    const qid = this.hexId(nodeId);
    const body: AddEdgeRequest = {
      label,
      target: this.hexId(target),
    };
    if (properties) body.properties = properties;
    await this.request('POST', `/api/v2/graph/node/${qid}/edges`, body);
  }

  // ------------------------------------------------------------------
  // Graph — History (time travel)
  // ------------------------------------------------------------------

  /**
   * Retrieve the state of a node at a specific point in time.
   * @param qid   Node identifier string (will be hex-encoded).
   * @param asOf  Unix timestamp (milliseconds) to travel back to.
   */
  async history(qid: string, asOf: number): Promise<HistoryResponse> {
    const hexQid = this.hexId(qid);
    const path = `/api/v2/graph/history?qid=${encodeURIComponent(hexQid)}&as_of=${asOf}`;
    return this.request<HistoryResponse>('GET', path);
  }

  // ------------------------------------------------------------------
  // Standing Queries
  // ------------------------------------------------------------------

  /** List all registered Standing Queries. */
  async listStandingQueries(): Promise<StandingQueryInfo[]> {
    const resp = await this.request<ListStandingQueriesResponse>(
      'GET',
      '/api/v2/standing-query',
    );
    return resp.standing_queries ?? [];
  }

  /**
   * Register a new Standing Query.
   * @param pattern Pattern specification.
   * @param name    Optional name for the query.
   * @returns The Standing Query ID assigned by the server.
   */
  async createStandingQuery(
    pattern: CreateStandingQueryRequest['pattern'],
    name?: string,
  ): Promise<string> {
    const body: CreateStandingQueryRequest = { pattern, name };
    const resp = await this.request<CreateStandingQueryResponse>(
      'POST',
      '/api/v2/standing-query',
      body,
    );
    return resp.id;
  }

  /** Get a Standing Query by ID. */
  async getStandingQuery(id: string): Promise<StandingQueryInfo> {
    return this.request<StandingQueryInfo>('GET', `/api/v2/standing-query/${id}`);
  }

  /** Delete a Standing Query by ID. */
  async deleteStandingQuery(id: string): Promise<void> {
    await this.request('DELETE', `/api/v2/standing-query/${id}`);
  }

  // ------------------------------------------------------------------
  // Vector operations
  // ------------------------------------------------------------------

  /**
   * Index a vector for a node.
   * @param qid    Node identifier string (will be hex-encoded).
   * @param vector Embedding vector.
   */
  async vectorIndex(qid: string, vector: number[]): Promise<VectorIndexResponse> {
    const body: VectorIndexRequest = { qid: this.hexId(qid), vector };
    return this.request<VectorIndexResponse>('POST', '/api/v2/vector/index', body);
  }

  /**
   * Search for k-nearest neighbors of a vector.
   * @param vector Query embedding vector.
   * @param k      Number of nearest neighbors (default 10).
   */
  async vectorSearch(vector: number[], k?: number): Promise<VectorSearchResponse> {
    const body: VectorSearchRequest = { vector, k };
    return this.request<VectorSearchResponse>('POST', '/api/v2/vector/search', body);
  }

  /**
   * Get the vector associated with a node.
   * @param qid Node identifier string (will be hex-encoded).
   * @returns Vector data, or `undefined` if not found.
   */
  async vectorGet(qid: string): Promise<VectorGetResponse | undefined> {
    const hexQid = this.hexId(qid);
    try {
      return await this.request<VectorGetResponse>(
        'GET',
        `/api/v2/vector/node/${hexQid}`,
      );
    } catch (err) {
      if (err instanceof NodeNotFoundError) return undefined;
      throw err;
    }
  }

  /**
   * Delete the vector associated with a node.
   * @param qid Node identifier string (will be hex-encoded).
   */
  async vectorDelete(qid: string): Promise<VectorIndexResponse> {
    const hexQid = this.hexId(qid);
    return this.request<VectorIndexResponse>(
      'DELETE',
      `/api/v2/vector/node/${hexQid}`,
    );
  }

  // ------------------------------------------------------------------
  // Ingest
  // ------------------------------------------------------------------

  /**
   * Start a file ingest pipeline.
   * @param name   Ingest pipeline name.
   * @param path   Path to the file (relative to server's ingest directory).
   * @param format File format (e.g. `"jsonl"`).
   */
  async ingestFile(
    name: string,
    path: string,
    format: string,
  ): Promise<IngestFileResponse> {
    const body: IngestFileRequest = { name, path, format };
    return this.request<IngestFileResponse>('POST', '/api/v2/ingest/file', body);
  }

  /** List all ingest pipelines. */
  async listIngests(): Promise<IngestInfo[]> {
    const resp = await this.request<{ ingests?: IngestInfo[] } | IngestInfo[]>(
      'GET',
      '/api/v2/ingest',
    );
    if (Array.isArray(resp)) return resp;
    return resp.ingests ?? [];
  }

  /** Cancel an ingest pipeline by name. */
  async cancelIngest(name: string): Promise<void> {
    await this.request('DELETE', `/api/v2/ingest/${encodeURIComponent(name)}`);
  }

  // ------------------------------------------------------------------
  // Recipes
  // ------------------------------------------------------------------

  /** List all recipes. */
  async listRecipes(): Promise<Recipe[]> {
    const resp = await this.request<{ recipes?: Recipe[] } | Recipe[]>(
      'GET',
      '/api/v2/recipes',
    );
    if (Array.isArray(resp)) return resp;
    return resp.recipes ?? [];
  }

  /**
   * Create a new recipe.
   * @param name   Recipe name.
   * @param config Recipe configuration.
   */
  async createRecipe(
    name: string,
    config: Record<string, unknown>,
  ): Promise<CreateRecipeResponse> {
    const body: CreateRecipeRequest = { name, config };
    return this.request<CreateRecipeResponse>('POST', '/api/v2/recipes', body);
  }

  /** Get a recipe by name. */
  async getRecipe(name: string): Promise<Recipe> {
    return this.request<Recipe>(
      'GET',
      `/api/v2/recipes/${encodeURIComponent(name)}`,
    );
  }

  /** Delete a recipe by name. */
  async deleteRecipe(name: string): Promise<{ status: string; name: string }> {
    return this.request('DELETE', `/api/v2/recipes/${encodeURIComponent(name)}`);
  }

  /** Execute a recipe by name. */
  async executeRecipe(name: string): Promise<RecipeExecuteResponse> {
    return this.request<RecipeExecuteResponse>(
      'POST',
      `/api/v2/recipes/${encodeURIComponent(name)}/execute`,
    );
  }

  /** Get execution runs for a recipe. */
  async getRecipeRuns(name: string): Promise<RecipeRun[]> {
    const resp = await this.request<{ runs?: RecipeRun[] } | RecipeRun[]>(
      'GET',
      `/api/v2/recipes/${encodeURIComponent(name)}/runs`,
    );
    if (Array.isArray(resp)) return resp;
    return resp.runs ?? [];
  }

  // ------------------------------------------------------------------
  // UDF
  // ------------------------------------------------------------------

  /**
   * Register a new user-defined function.
   * @param name     UDF name.
   * @param code     Function code.
   * @param language `"native"`, `"wasm"`, or `"python"`.
   */
  async registerUdf(
    name: string,
    code: string,
    language: string,
  ): Promise<RegisterUdfResponse> {
    const body: RegisterUdfRequest = { name, code, language };
    return this.request<RegisterUdfResponse>('POST', '/api/v2/udf/register', body);
  }

  /** List all registered UDFs. */
  async listUdfs(): Promise<UdfInfo[]> {
    const resp = await this.request<{ udfs?: UdfInfo[] } | UdfInfo[]>(
      'GET',
      '/api/v2/udf',
    );
    if (Array.isArray(resp)) return resp;
    return resp.udfs ?? [];
  }

  /**
   * Execute a UDF by name.
   * @param name UDF name.
   * @param args Optional array of arguments.
   */
  async executeUdf(name: string, args?: unknown[]): Promise<ExecuteUdfResponse> {
    const body: ExecuteUdfRequest = { name, args };
    return this.request<ExecuteUdfResponse>('POST', '/api/v2/udf/execute', body);
  }

  /** Delete a UDF by name. */
  async deleteUdf(name: string): Promise<{ status: string; name: string }> {
    return this.request('DELETE', `/api/v2/udf/${encodeURIComponent(name)}`);
  }

  // ------------------------------------------------------------------
  // Materialized Views
  // ------------------------------------------------------------------

  /** List all materialized views. */
  async listMaterializedViews(): Promise<MaterializedView[]> {
    const resp = await this.request<
      { views?: MaterializedView[] } | MaterializedView[]
    >('GET', '/api/v2/materialized-views');
    if (Array.isArray(resp)) return resp;
    return resp.views ?? [];
  }

  /**
   * Create a new materialized view.
   * @param definition View definition.
   */
  async createMaterializedView(
    definition: CreateMaterializedViewRequest,
  ): Promise<CreateMaterializedViewResponse> {
    return this.request<CreateMaterializedViewResponse>(
      'POST',
      '/api/v2/materialized-views',
      definition,
    );
  }

  /** Get a materialized view by ID. */
  async getMaterializedView(viewId: string): Promise<MaterializedView> {
    return this.request<MaterializedView>(
      'GET',
      `/api/v2/materialized-views/${viewId}`,
    );
  }

  /** Drop a materialized view by ID. */
  async dropMaterializedView(
    viewId: string,
  ): Promise<MaterializedViewActionResponse> {
    return this.request<MaterializedViewActionResponse>(
      'DELETE',
      `/api/v2/materialized-views/${viewId}`,
    );
  }

  /**
   * Query data from a materialized view.
   * @param viewId The materialized view identifier.
   * @param limit  Optional maximum number of rows.
   */
  async queryMaterializedView(
    viewId: string,
    limit?: number,
  ): Promise<MaterializedViewData> {
    const params = new URLSearchParams();
    if (limit !== undefined) params.set('limit', String(limit));
    const qs = params.toString();
    const path = `/api/v2/materialized-views/${viewId}/data${qs ? `?${qs}` : ''}`;
    return this.request<MaterializedViewData>('GET', path);
  }

  /** Manually refresh a materialized view. */
  async refreshMaterializedView(
    viewId: string,
  ): Promise<MaterializedViewActionResponse> {
    return this.request<MaterializedViewActionResponse>(
      'POST',
      `/api/v2/materialized-views/${viewId}/refresh`,
    );
  }

  /**
   * Link a Standing Query to a materialized view.
   * @param viewId          The materialized view identifier.
   * @param standingQueryId The Standing Query ID to link.
   */
  async linkStandingQuery(
    viewId: string,
    standingQueryId: string,
  ): Promise<MaterializedViewActionResponse> {
    const body: LinkSqRequest = { standing_query_id: standingQueryId };
    return this.request<MaterializedViewActionResponse>(
      'POST',
      `/api/v2/materialized-views/${viewId}/link-sq`,
      body,
    );
  }

  // ------------------------------------------------------------------
  // Storage
  // ------------------------------------------------------------------

  /** Get storage tier statistics. */
  async storageStatus(): Promise<StorageStatus> {
    return this.request<StorageStatus>('GET', '/api/v2/storage/status');
  }

  /** Manually trigger cold storage migration. */
  async storageMigrate(): Promise<StorageMigrateResponse> {
    return this.request<StorageMigrateResponse>('POST', '/api/v2/storage/migrate');
  }

  // ------------------------------------------------------------------
  // System & Health
  // ------------------------------------------------------------------

  /** Get system information. */
  async systemInfo(): Promise<SystemInfo> {
    return this.request<SystemInfo>('GET', '/api/v2/system/info');
  }

  /** Get system configuration. */
  async systemConfig(): Promise<SystemConfig> {
    return this.request<SystemConfig>('GET', '/api/v2/system/config');
  }

  /** Check server health. */
  async health(): Promise<HealthStatus> {
    return this.request<HealthStatus>('GET', '/api/v2/health');
  }

  /** Readiness check. */
  async ready(): Promise<ReadinessStatus> {
    return this.request<ReadinessStatus>('GET', '/api/v2/health/ready');
  }

  /** Liveness check. */
  async live(): Promise<LivenessStatus> {
    return this.request<LivenessStatus>('GET', '/api/v2/health/live');
  }

  /** Get cluster statistics (cluster mode only). */
  async clusterStats(): Promise<ClusterStats> {
    return this.request<ClusterStats>('GET', '/api/v2/cluster/stats');
  }

  // ------------------------------------------------------------------
  // Auth
  // ------------------------------------------------------------------

  /**
   * Generate an authentication token.
   * @param role       Optional role for the token.
   * @param expiresIn  Optional expiration in seconds.
   */
  async generateToken(
    role?: string,
    expiresIn?: number,
  ): Promise<TokenResponse> {
    const body: GenerateTokenRequest = { role, expires_in: expiresIn };
    return this.request<TokenResponse>('POST', '/api/v2/auth/token', body);
  }
}
