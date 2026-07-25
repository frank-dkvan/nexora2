//! The main `NexoraClient` struct — a typed async HTTP client for the nexora API.

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;

use crate::error::NexoraClientError;
use crate::types::*;

/// Default base URL for a local nexora instance.
const DEFAULT_BASE_URL: &str = "http://localhost:8080";

/// Default request timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default number of retries for transient failures.
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Builder for constructing a [`NexoraClient`] with custom configuration.
#[derive(Debug, Clone)]
pub struct NexoraClientBuilder {
    base_url: String,
    bearer_token: Option<String>,
    timeout: Duration,
    max_retries: u32,
}

impl Default for NexoraClientBuilder {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            bearer_token: None,
            timeout: DEFAULT_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

impl NexoraClientBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the base URL of the nexora server.
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Set the Bearer token used for authentication.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Set the per-request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of retries for transient failures.
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Build the [`NexoraClient`].
    ///
    /// Returns an error if the base URL is invalid.
    pub fn build(self) -> Result<NexoraClient, NexoraClientError> {
        let client = Client::builder()
            .timeout(self.timeout)
            .build()
            .map_err(NexoraClientError::Http)?;

        // Validate the base URL by parsing it.
        reqwest::Url::parse(&self.base_url).map_err(|e| NexoraClientError::Url(e.to_string()))?;

        Ok(NexoraClient {
            inner: Arc::new(NexoraClientInner {
                base_url: self.base_url,
                bearer_token: self.bearer_token,
                max_retries: self.max_retries,
                client,
            }),
        })
    }
}

/// Internal shared state.
struct NexoraClientInner {
    base_url: String,
    bearer_token: Option<String>,
    max_retries: u32,
    client: Client,
}

impl std::fmt::Debug for NexoraClientInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexoraClientInner")
            .field("base_url", &self.base_url)
            .field("has_token", &self.bearer_token.is_some())
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

/// A typed async HTTP client for the nexora streaming graph database API.
///
/// Clone the client to share it across tasks — internally it uses an
/// `Arc` so cloning is cheap.
///
/// # Example
///
/// ```no_run
/// # use nexora_client::NexoraClient;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = NexoraClient::builder()
///     .base_url("http://localhost:8080")
///     .bearer_token("my-secret-token")
///     .build()?;
///
/// let health = client.health().await?;
/// println!("Status: {}", health.status);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct NexoraClient {
    inner: Arc<NexoraClientInner>,
}

impl NexoraClient {
    /// Create a new [`NexoraClientBuilder`] for custom configuration.
    pub fn builder() -> NexoraClientBuilder {
        NexoraClientBuilder::new()
    }

    /// Create a client pointing at the default `http://localhost:8080`.
    pub fn new() -> Result<Self, NexoraClientError> {
        NexoraClientBuilder::new().build()
    }

    /// Create a client with the given base URL and no authentication.
    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, NexoraClientError> {
        NexoraClientBuilder::new().base_url(base_url).build()
    }

    /// Returns the base URL this client is configured with.
    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    // ---------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------

    fn url(&self, path: &str) -> String {
        // base_url may or may not have a trailing slash; normalize.
        let base = self.inner.base_url.trim_end_matches('/');
        format!("{base}{path}")
    }

    fn add_auth(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.inner.bearer_token {
            req = req.bearer_auth(token);
        }
        req
    }

    /// Execute a request with retry logic for transient failures.
    async fn execute<T>(&self, req: reqwest::RequestBuilder) -> Result<T, NexoraClientError>
    where
        T: serde::de::DeserializeOwned,
    {
        let max_retries = self.inner.max_retries;
        let mut last_err: Option<NexoraClientError> = None;

        for attempt in 0..=max_retries {
            // Clone the request builder for each attempt.
            let req = req.try_clone().ok_or_else(|| {
                NexoraClientError::Other("request is not cloneable for retry".into())
            })?;

            let result = self.add_auth(req).send().await;

            match result {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        let body = resp.text().await.map_err(NexoraClientError::Http)?;
                        return serde_json::from_str(&body).map_err(NexoraClientError::Deserialize);
                    }

                    let body = resp.text().await.unwrap_or_default();
                    let err = NexoraClientError::Status {
                        status: status.as_u16(),
                        body,
                    };

                    // Retry on 429 / 5xx.
                    if err.is_retryable() && attempt < max_retries {
                        last_err = Some(err);
                        // Exponential backoff: 100ms, 200ms, 400ms, ...
                        tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempt))).await;
                        continue;
                    }
                    return Err(err);
                }
                Err(e) => {
                    let err = NexoraClientError::Http(e);
                    if err.is_retryable() && attempt < max_retries {
                        last_err = Some(err);
                        tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempt))).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| NexoraClientError::MaxRetriesExceeded(max_retries)))
    }

    /// Execute a GET request and deserialize the JSON response.
    async fn get<T>(&self, path: &str) -> Result<T, NexoraClientError>
    where
        T: serde::de::DeserializeOwned,
    {
        let req = self.inner.client.get(self.url(path));
        self.execute(req).await
    }

    /// Execute a POST request with a JSON body and deserialize the response.
    async fn post<T, B>(&self, path: &str, body: &B) -> Result<T, NexoraClientError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let req = self.inner.client.post(self.url(path)).json(body);
        self.execute(req).await
    }

    /// Execute a PUT request with a JSON body and deserialize the response.
    async fn put<T, B>(&self, path: &str, body: &B) -> Result<T, NexoraClientError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let req = self.inner.client.put(self.url(path)).json(body);
        self.execute(req).await
    }

    /// Execute a DELETE request and deserialize the response.
    async fn delete<T>(&self, path: &str) -> Result<T, NexoraClientError>
    where
        T: serde::de::DeserializeOwned,
    {
        let req = self.inner.client.delete(self.url(path));
        self.execute(req).await
    }

    // =========================================================
    // Health
    // =========================================================

    /// `GET /api/v2/health` — overall system health.
    pub async fn health(&self) -> Result<HealthResponse, NexoraClientError> {
        self.get("/api/v2/health").await
    }

    /// `GET /api/v2/health/ready` — Kubernetes readiness probe.
    pub async fn readiness(&self) -> Result<ReadinessResponse, NexoraClientError> {
        self.get("/api/v2/health/ready").await
    }

    /// `GET /api/v2/health/live` — Kubernetes liveness probe.
    pub async fn liveness(&self) -> Result<LivenessResponse, NexoraClientError> {
        self.get("/api/v2/health/live").await
    }

    // =========================================================
    // Cypher / SQL Query
    // =========================================================

    /// `POST /api/v2/query/cypher` — execute a Cypher query.
    pub async fn execute_cypher(
        &self,
        query: impl Into<String>,
    ) -> Result<CypherResponse, NexoraClientError> {
        self.post("/api/v2/query/cypher", &CypherRequest::new(query))
            .await
    }

    /// `POST /api/v2/query/cypher` — execute a Cypher query from a `CypherRequest`.
    pub async fn execute_cypher_request(
        &self,
        req: &CypherRequest,
    ) -> Result<CypherResponse, NexoraClientError> {
        self.post("/api/v2/query/cypher", req).await
    }

    /// `POST /api/v2/query/sql` — execute a SQL query.
    pub async fn execute_sql(
        &self,
        query: impl Into<String>,
    ) -> Result<SqlResponse, NexoraClientError> {
        self.post("/api/v2/query/sql", &SqlRequest::new(query))
            .await
    }

    /// `POST /api/v2/query/explain` — generate a query execution plan.
    pub async fn explain_query(
        &self,
        query: impl Into<String>,
        analyze: bool,
    ) -> Result<ExplainResponse, NexoraClientError> {
        self.post(
            "/api/v2/query/explain",
            &ExplainRequest {
                query: query.into(),
                analyze,
            },
        )
        .await
    }

    // =========================================================
    // Graph — Properties
    // =========================================================

    /// `GET /api/v2/graph/node/{qid}/property/{key}` — get a node property.
    pub async fn get_property(
        &self,
        qid: &str,
        key: &str,
    ) -> Result<GetPropertyResponse, NexoraClientError> {
        self.get(&format!("/api/v2/graph/node/{qid}/property/{key}"))
            .await
    }

    /// `PUT /api/v2/graph/node/{qid}/property/{key}` — set a node property.
    pub async fn set_property(
        &self,
        qid: &str,
        key: &str,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.put(
            &format!("/api/v2/graph/node/{qid}/property/{key}"),
            &SetPropertyRequest { value },
        )
        .await
    }

    // =========================================================
    // Graph — Edges
    // =========================================================

    /// `GET /api/v2/graph/node/{qid}/edges` — get all edges of a node.
    pub async fn get_edges(&self, qid: &str) -> Result<GetEdgesResponse, NexoraClientError> {
        self.get(&format!("/api/v2/graph/node/{qid}/edges")).await
    }

    /// `POST /api/v2/graph/node/{qid}/edges` — add an edge.
    pub async fn add_edge(
        &self,
        qid: &str,
        edge_type: impl Into<String>,
        target: impl Into<String>,
        direction: impl Into<String>,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post(
            &format!("/api/v2/graph/node/{qid}/edges"),
            &AddEdgeRequest::new(edge_type, target, direction),
        )
        .await
    }

    // =========================================================
    // Graph — Time Travel
    // =========================================================

    /// `GET /api/v2/graph/history` — time-travel status.
    pub async fn time_travel(&self) -> Result<TimeTravelResponse, NexoraClientError> {
        self.get("/api/v2/graph/history").await
    }

    // =========================================================
    // Standing Queries
    // =========================================================

    /// `GET /api/v2/standing-query` — list all standing queries.
    pub async fn list_standing_queries(
        &self,
    ) -> Result<ListStandingQueriesResponse, NexoraClientError> {
        self.get("/api/v2/standing-query").await
    }

    /// `POST /api/v2/standing-query` — create a standing query.
    pub async fn create_standing_query(
        &self,
        req: &CreateSqRequest,
    ) -> Result<CreateSqResponse, NexoraClientError> {
        self.post("/api/v2/standing-query", req).await
    }

    /// `GET /api/v2/standing-query/{id}` — get a standing query by ID.
    pub async fn get_standing_query(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.get(&format!("/api/v2/standing-query/{id}")).await
    }

    /// `DELETE /api/v2/standing-query/{id}` — delete a standing query.
    pub async fn delete_standing_query(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.delete(&format!("/api/v2/standing-query/{id}")).await
    }

    // =========================================================
    // Vector Search
    // =========================================================

    /// `POST /api/v2/vector/index` — insert a vector embedding for a node.
    pub async fn vector_index(
        &self,
        qid: impl Into<String>,
        vector: Vec<f32>,
    ) -> Result<VectorIndexResponse, NexoraClientError> {
        self.post(
            "/api/v2/vector/index",
            &VectorInsertRequest {
                qid: qid.into(),
                vector,
            },
        )
        .await
    }

    /// `POST /api/v2/vector/search` — k-NN vector similarity search.
    pub async fn vector_search(
        &self,
        vector: Vec<f32>,
        k: usize,
    ) -> Result<VectorSearchResponse, NexoraClientError> {
        self.post("/api/v2/vector/search", &VectorSearchRequest { vector, k })
            .await
    }

    /// `GET /api/v2/vector/node/{qid}` — get the vector for a node.
    pub async fn vector_get(&self, qid: &str) -> Result<VectorGetResponse, NexoraClientError> {
        self.get(&format!("/api/v2/vector/node/{qid}")).await
    }

    /// `DELETE /api/v2/vector/node/{qid}` — remove a vector from the index.
    pub async fn vector_delete(
        &self,
        qid: &str,
    ) -> Result<VectorDeleteResponse, NexoraClientError> {
        self.delete(&format!("/api/v2/vector/node/{qid}")).await
    }

    // =========================================================
    // Ingest
    // =========================================================

    /// `POST /api/v2/ingest/file` — start file-based data ingestion.
    pub async fn start_file_ingest(
        &self,
        path: impl Into<String>,
    ) -> Result<FileIngestResponse, NexoraClientError> {
        self.post(
            "/api/v2/ingest/file",
            &FileIngestRequest {
                path: path.into(),
                id_field: "id".to_string(),
            },
        )
        .await
    }

    /// `POST /api/v2/ingest/file` — start file-based ingestion with a custom id field.
    pub async fn start_file_ingest_with_id_field(
        &self,
        path: impl Into<String>,
        id_field: impl Into<String>,
    ) -> Result<FileIngestResponse, NexoraClientError> {
        self.post(
            "/api/v2/ingest/file",
            &FileIngestRequest {
                path: path.into(),
                id_field: id_field.into(),
            },
        )
        .await
    }

    /// `POST /api/v2/ingest/bulk` — synchronous batch ingest of JSON records.
    ///
    /// Each record is a JSON object; its non-`id_field` fields become node
    /// properties. Returns per-batch stats. This is the entry point the `nex`
    /// CLI uses for one-shot ingestion.
    pub async fn bulk_ingest(
        &self,
        records: Vec<serde_json::Value>,
        id_field: impl Into<String>,
    ) -> Result<BulkIngestResponse, NexoraClientError> {
        self.post(
            "/api/v2/ingest/bulk",
            &BulkIngestRequest {
                records,
                id_field: id_field.into(),
            },
        )
        .await
    }

    /// `GET /api/v2/ingest` — list active ingest tasks.
    pub async fn list_ingests(&self) -> Result<ListIngestsResponse, NexoraClientError> {
        self.get("/api/v2/ingest").await
    }

    /// `DELETE /api/v2/ingest/{name}` — cancel an ingest task.
    pub async fn delete_ingest(
        &self,
        name: &str,
    ) -> Result<DeleteIngestResponse, NexoraClientError> {
        self.delete(&format!("/api/v2/ingest/{name}")).await
    }

    // =========================================================
    // Streams
    // =========================================================

    /// `GET /api/v2/streams` — list active stream sources.
    pub async fn list_streams(&self) -> Result<ListStreamsResponse, NexoraClientError> {
        self.get("/api/v2/streams").await
    }

    /// `POST /api/v2/streams/kafka` — start a Kafka stream source.
    pub async fn start_kafka_stream(
        &self,
        brokers: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post(
            "/api/v2/streams/kafka",
            &KafkaStreamRequest {
                brokers: brokers.into(),
                topic: topic.into(),
                group_id: "nexora-app-consumer".to_string(),
            },
        )
        .await
    }

    /// `POST /api/v2/streams/kafka` — start a Kafka stream with a custom group ID.
    pub async fn start_kafka_stream_with_group(
        &self,
        brokers: impl Into<String>,
        topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post(
            "/api/v2/streams/kafka",
            &KafkaStreamRequest {
                brokers: brokers.into(),
                topic: topic.into(),
                group_id: group_id.into(),
            },
        )
        .await
    }

    /// `DELETE /api/v2/streams/{name}` — stop a stream source.
    pub async fn delete_stream(
        &self,
        name: &str,
    ) -> Result<DeleteStreamResponse, NexoraClientError> {
        self.delete(&format!("/api/v2/streams/{name}")).await
    }

    // =========================================================
    // Recipes
    // =========================================================

    /// `GET /api/v2/recipes` — list all recipes.
    pub async fn list_recipes(&self) -> Result<ListRecipesResponse, NexoraClientError> {
        self.get("/api/v2/recipes").await
    }

    /// `POST /api/v2/recipes` — create a new recipe.
    pub async fn create_recipe(
        &self,
        req: &CreateRecipeRequest,
    ) -> Result<CreateRecipeResponse, NexoraClientError> {
        self.post("/api/v2/recipes", req).await
    }

    /// `GET /api/v2/recipes/{name}` — get recipe details.
    pub async fn get_recipe(&self, name: &str) -> Result<serde_json::Value, NexoraClientError> {
        self.get(&format!("/api/v2/recipes/{name}")).await
    }

    /// `DELETE /api/v2/recipes/{name}` — delete a recipe.
    pub async fn delete_recipe(
        &self,
        name: &str,
    ) -> Result<DeleteRecipeResponse, NexoraClientError> {
        self.delete(&format!("/api/v2/recipes/{name}")).await
    }

    /// `POST /api/v2/recipes/{name}/execute` — execute a recipe.
    pub async fn execute_recipe(
        &self,
        name: &str,
    ) -> Result<ExecuteRecipeResponse, NexoraClientError> {
        let req = self
            .inner
            .client
            .post(self.url(&format!("/api/v2/recipes/{name}/execute")));
        self.execute(req).await
    }

    /// `GET /api/v2/recipes/{name}/runs` — get recipe execution history.
    pub async fn get_recipe_runs(
        &self,
        name: &str,
    ) -> Result<GetRecipeRunsResponse, NexoraClientError> {
        self.get(&format!("/api/v2/recipes/{name}/runs")).await
    }

    // =========================================================
    // UDF (User-Defined Functions)
    // =========================================================

    /// `POST /api/v2/udf/register` — register a new UDF.
    pub async fn udf_register(
        &self,
        req: &UdfRegisterRequest,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post("/api/v2/udf/register", req).await
    }

    /// `GET /api/v2/udf` — list all registered UDFs.
    pub async fn udf_list(&self) -> Result<Vec<UdfInfo>, NexoraClientError> {
        self.get("/api/v2/udf").await
    }

    /// `POST /api/v2/udf/execute` — execute a UDF by name.
    pub async fn udf_execute(
        &self,
        req: &UdfExecuteRequest,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post("/api/v2/udf/execute", req).await
    }

    /// `DELETE /api/v2/udf/{name}` — delete a UDF.
    pub async fn udf_delete(&self, name: &str) -> Result<serde_json::Value, NexoraClientError> {
        self.delete(&format!("/api/v2/udf/{name}")).await
    }

    /// `POST /api/v2/udf/{name}/execute` — execute a UDF by name (path-based).
    pub async fn udf_execute_by_name(
        &self,
        name: &str,
        args: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post(
            &format!("/api/v2/udf/{name}/execute"),
            &UdfExecuteRequest {
                name: name.to_string(),
                args,
            },
        )
        .await
    }

    // =========================================================
    // Materialized Views
    // =========================================================

    /// `POST /api/v2/materialized-views` — create a materialized view.
    pub async fn create_materialized_view(
        &self,
        req: &CreateMaterializedViewRequest,
    ) -> Result<CreateMaterializedViewResponse, NexoraClientError> {
        self.post("/api/v2/materialized-views", req).await
    }

    /// `GET /api/v2/materialized-views` — list all materialized views.
    pub async fn list_materialized_views(
        &self,
    ) -> Result<Vec<MaterializedViewSummary>, NexoraClientError> {
        self.get("/api/v2/materialized-views").await
    }

    /// `GET /api/v2/materialized-views/{view_id}` — get a view definition.
    pub async fn get_materialized_view(
        &self,
        view_id: &str,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.get(&format!("/api/v2/materialized-views/{view_id}"))
            .await
    }

    /// `DELETE /api/v2/materialized-views/{view_id}` — drop a materialized view.
    pub async fn drop_materialized_view(
        &self,
        view_id: &str,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.delete(&format!("/api/v2/materialized-views/{view_id}"))
            .await
    }

    /// `GET /api/v2/materialized-views/{view_id}/data` — query view data.
    pub async fn query_materialized_view(
        &self,
        view_id: &str,
        limit: Option<usize>,
    ) -> Result<QueryMaterializedViewResponse, NexoraClientError> {
        let mut path = format!("/api/v2/materialized-views/{view_id}/data");
        if let Some(l) = limit {
            path.push_str(&format!("?limit={l}"));
        }
        self.get(&path).await
    }

    /// `POST /api/v2/materialized-views/{view_id}/refresh` — refresh a view.
    pub async fn refresh_materialized_view(
        &self,
        view_id: &str,
    ) -> Result<serde_json::Value, NexoraClientError> {
        let req = self
            .inner
            .client
            .post(self.url(&format!("/api/v2/materialized-views/{view_id}/refresh")));
        self.execute(req).await
    }

    /// `POST /api/v2/materialized-views/{view_id}/link-sq` — link a standing query to a view.
    pub async fn link_sq_to_materialized_view(
        &self,
        view_id: &str,
        sq_id: impl Into<String>,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post(
            &format!("/api/v2/materialized-views/{view_id}/link-sq"),
            &LinkSqRequest {
                sq_id: sq_id.into(),
            },
        )
        .await
    }

    // =========================================================
    // SQL DDL
    // =========================================================

    /// `POST /api/v2/sql/ddl` — execute SQL DDL (CREATE/DROP MATERIALIZED VIEW).
    pub async fn execute_sql_ddl(
        &self,
        sql: impl Into<String>,
    ) -> Result<serde_json::Value, NexoraClientError> {
        self.post("/api/v2/sql/ddl", &SqlDdlRequest { sql: sql.into() })
            .await
    }

    // =========================================================
    // Storage
    // =========================================================

    /// `GET /api/v2/storage/status` — get tiered storage status.
    pub async fn storage_status(&self) -> Result<StorageStatusResponse, NexoraClientError> {
        self.get("/api/v2/storage/status").await
    }

    /// `POST /api/v2/storage/migrate` — trigger storage tier migration.
    pub async fn storage_migrate(&self) -> Result<StorageMigrateResponse, NexoraClientError> {
        let req = self.inner.client.post(self.url("/api/v2/storage/migrate"));
        self.execute(req).await
    }

    // =========================================================
    // System
    // =========================================================

    /// `GET /api/v2/system/info` — get system information.
    pub async fn system_info(&self) -> Result<SystemInfoResponse, NexoraClientError> {
        self.get("/api/v2/system/info").await
    }

    /// `GET /api/v2/system/config` — get system configuration.
    pub async fn system_config(&self) -> Result<SystemConfigResponse, NexoraClientError> {
        self.get("/api/v2/system/config").await
    }

    // =========================================================
    // Auth
    // =========================================================

    /// `POST /api/v2/auth/token` — generate an authentication token.
    pub async fn generate_token(
        &self,
        user_id: impl Into<String>,
        role: impl Into<String>,
    ) -> Result<TokenResponse, NexoraClientError> {
        self.post(
            "/api/v2/auth/token",
            &TokenRequest {
                user_id: user_id.into(),
                role: role.into(),
            },
        )
        .await
    }

    // =========================================================
    // Cluster
    // =========================================================

    /// `GET /api/v2/cluster/stats` — get cluster-wide statistics.
    pub async fn cluster_stats(&self) -> Result<ClusterStatsResponse, NexoraClientError> {
        self.get("/api/v2/cluster/stats").await
    }

    /// `GET /api/v2/cluster/raft` — get Raft consensus status.
    pub async fn raft_status(&self) -> Result<RaftStatusResponse, NexoraClientError> {
        self.get("/api/v2/cluster/raft").await
    }

    // =========================================================
    // Metrics
    // =========================================================

    /// `GET /api/v2/metrics` — get metrics in JSON format.
    pub async fn metrics_json(&self) -> Result<MetricsJsonResponse, NexoraClientError> {
        self.get("/api/v2/metrics").await
    }

    /// `GET /metrics` — get Prometheus-format metrics as raw text.
    pub async fn metrics_prometheus(&self) -> Result<String, NexoraClientError> {
        let req = self.inner.client.get(self.url("/metrics"));
        let resp = self
            .add_auth(req)
            .send()
            .await
            .map_err(NexoraClientError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(NexoraClientError::Status {
                status: status.as_u16(),
                body,
            });
        }

        resp.text().await.map_err(NexoraClientError::Http)
    }
}

impl Default for NexoraClient {
    fn default() -> Self {
        Self::new().expect("default NexoraClient should construct successfully")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let b = NexoraClientBuilder::new();
        assert_eq!(b.base_url, DEFAULT_BASE_URL);
        assert_eq!(b.timeout, DEFAULT_TIMEOUT);
        assert_eq!(b.max_retries, DEFAULT_MAX_RETRIES);
        assert!(b.bearer_token.is_none());
    }

    #[test]
    fn client_constructs_with_defaults() {
        let client = NexoraClient::new().unwrap();
        assert_eq!(client.base_url(), DEFAULT_BASE_URL);
    }

    #[test]
    fn client_constructs_with_custom_url() {
        let client = NexoraClient::with_base_url("http://example.com:9090").unwrap();
        assert_eq!(client.base_url(), "http://example.com:9090");
    }

    #[test]
    fn client_constructs_with_bearer_token() {
        let client = NexoraClient::builder()
            .bearer_token("secret")
            .build()
            .unwrap();
        assert_eq!(client.base_url(), DEFAULT_BASE_URL);
    }

    #[test]
    fn client_clone_is_cheap() {
        let client = NexoraClient::new().unwrap();
        let cloned = client.clone();
        assert_eq!(client.base_url(), cloned.base_url());
    }

    #[test]
    fn url_normalizes_trailing_slash() {
        let client = NexoraClient::builder()
            .base_url("http://localhost:8080/")
            .build()
            .unwrap();
        // Internal URL construction strips trailing slash from base.
        let url = client.url("/api/v2/health");
        assert_eq!(url, "http://localhost:8080/api/v2/health");
    }

    #[test]
    fn url_no_trailing_slash() {
        let client = NexoraClient::with_base_url("http://localhost:8080").unwrap();
        let url = client.url("/api/v2/health");
        assert_eq!(url, "http://localhost:8080/api/v2/health");
    }

    #[test]
    fn url_multiple_trailing_slashes() {
        let client = NexoraClient::with_base_url("http://localhost:8080///").unwrap();
        let url = client.url("/api/v2/health");
        assert_eq!(url, "http://localhost:8080/api/v2/health");
    }

    #[test]
    fn url_with_port() {
        let client = NexoraClient::with_base_url("http://graph.io:9090").unwrap();
        let url = client.url("/api/v1/query");
        assert_eq!(url, "http://graph.io:9090/api/v1/query");
    }

    #[test]
    fn url_https() {
        let client = NexoraClient::with_base_url("https://secure.graph.com").unwrap();
        let url = client.url("/api/v2/health");
        assert_eq!(url, "https://secure.graph.com/api/v2/health");
    }

    #[test]
    fn url_with_path_segment() {
        let client = NexoraClient::with_base_url("http://localhost:8080/prefix").unwrap();
        let url = client.url("/api/v2/health");
        assert_eq!(url, "http://localhost:8080/prefix/api/v2/health");
    }

    #[test]
    fn url_root_path() {
        let client = NexoraClient::with_base_url("http://localhost:8080").unwrap();
        let url = client.url("/");
        assert_eq!(url, "http://localhost:8080/");
    }

    #[test]
    fn error_is_retryable_for_5xx() {
        let err = NexoraClientError::Status {
            status: 500,
            body: "Internal Server Error".to_string(),
        };
        assert!(err.is_retryable());
        assert!(err.is_server_error());
    }

    #[test]
    fn error_is_retryable_for_429() {
        let err = NexoraClientError::Status {
            status: 429,
            body: "Too Many Requests".to_string(),
        };
        assert!(err.is_retryable());
        assert!(!err.is_server_error());
    }

    #[test]
    fn error_is_not_retryable_for_4xx() {
        let err = NexoraClientError::Status {
            status: 404,
            body: "Not Found".to_string(),
        };
        assert!(!err.is_retryable());
    }
}
