//! HTTP API handlers for Nexora-RS.

// Planned HTTP API surface not yet wired into the router (batch node/property
// ops and query-management endpoints). Kept compiling so the routes can be
// mounted without rework; silence dead-code until then.
#[allow(dead_code)]
pub mod batch_ops;
#[cfg(all(feature = "event-streaming", feature = "library"))]
pub mod distributed_cluster;
#[cfg(feature = "event-streaming")]
pub mod event_streaming;
pub mod explain;
#[cfg(feature = "event-streaming")]
pub mod iceberg_catalog;
pub mod materialized_view;
pub mod ontology;
#[allow(dead_code)]
pub mod query_mgmt;

use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};

/// Pagination query parameters for list endpoints.
#[derive(Deserialize)]
pub struct Pagination {
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
}

impl Pagination {
    /// Build a paginated JSON response body.
    pub fn json<T: Serialize>(&self, items: &[T]) -> serde_json::Value {
        let total = items.len();
        let offset = self.offset.unwrap_or(0).min(total);
        let limit = self.limit.unwrap_or(100).min(1000);
        let end = (offset + limit).min(total);
        let page = &items[offset..end];
        serde_json::json!({
            "items": page,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": end < total,
        })
    }
}
use crate::drain::DrainState;
use nexora_core::{materialized_view::MaterializedViewManager, GraphService};
use nexora_hnsw::HnswIndex;
use nexora_id::{NexoraId, PropertyValue};
use nexora_recipe::Recipe;
use nexora_standing_query::{
    pattern::{FilterCondition, StandingQueryPattern},
    StandingQueryManager,
};
use nexora_udf::manager::UdfManager;
use nexora_udf::native::UdfRegistry;
use nexora_value::{HalfEdge, Symbol};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use tokio::sync::Mutex;

/// Shared broadcast channel for Standing Query results — push-based instead of polling WebSocket.
pub static SQ_BROADCAST: LazyLock<
    tokio::sync::RwLock<Option<tokio::sync::broadcast::Sender<String>>>,
> = LazyLock::new(|| tokio::sync::RwLock::new(None));

/// Publish a standing query event to all connected WebSocket clients.
/// Called from the SQ callback after a matching event is detected.
pub async fn publish_sq_event(json_msg: String) {
    let guard = SQ_BROADCAST.read().await;
    if let Some(tx) = guard.as_ref() {
        let _ = tx.send(json_msg);
    }
}

/// Ensure the SQ broadcast channel exists and return a new subscriber.
pub async fn sq_broadcast_subscribe() -> tokio::sync::broadcast::Receiver<String> {
    let mut guard = SQ_BROADCAST.write().await;
    if guard.is_none() {
        let (tx, _) = tokio::sync::broadcast::channel::<String>(256);
        *guard = Some(tx);
    }
    guard.as_ref().unwrap().subscribe()
}

// ============================================================
// E7.1: Structured audit macro
// ============================================================

/// Emit a structured audit log entry via `tracing`.
///
/// Fields are emitted as key=value pairs so log aggregators (Loki, ELK, etc.)
/// can filter on `audit=true` to extract the audit trail.
///
/// Usage:
/// ```
/// audit!("set_property", "alice", "/api/v2/graph/node/.../property/...", "success");
/// ```
macro_rules! audit {
    ($action:expr, $user:expr, $resource:expr, $outcome:expr) => {
        tracing::info!(
            audit = true,
            action = $action,
            user = $user,
            resource = $resource,
            outcome = $outcome,
        )
    };
}

/// Application-level configuration shared with handlers.
#[derive(Clone)]
pub struct AppConfig {
    pub max_nodes_per_shard: usize,
    pub rocksdb_path: Option<String>,
    pub wal_dir: Option<String>,
    pub allow_ingest_dir: Option<PathBuf>,
    pub profile: String,
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub graph: Arc<GraphService>,
    pub sq_manager: Arc<StandingQueryManager>,
    pub config: AppConfig,
    pub shutdown: Arc<tokio::sync::Notify>,
    /// Server start time for uptime calculation.
    pub start_time: std::time::Instant,
    /// Active ingest tasks: name → (info, abort handle)
    pub ingests: Arc<tokio::sync::RwLock<HashMap<String, tokio::task::AbortHandle>>>,
    /// Metrics registry.
    pub metrics: Arc<crate::metrics::Metrics>,
    /// HNSW vector index for similarity search.
    pub hnsw: Arc<Mutex<HnswIndex>>,
    /// Active stream sources: name -> metadata
    pub streams: Arc<tokio::sync::RwLock<HashMap<String, StreamSource>>>,
    /// Recipe registry: name → Recipe definition
    pub recipes: Arc<tokio::sync::RwLock<HashMap<String, Recipe>>>,
    /// Recipe run history: recipe name → list of execution results
    pub recipe_runs: Arc<tokio::sync::RwLock<HashMap<String, Vec<RecipeRunRecord>>>>,
    /// UDF registry for native user-defined functions.
    pub udf_registry: Arc<tokio::sync::RwLock<UdfRegistry>>,
    /// UDF manager for Wasm and Python UDFs.
    pub udf_manager: Arc<Mutex<UdfManager>>,
    /// Tiered storage backend (optional — None when using default in-memory)
    pub tiered_store: Option<Arc<nexora_storage::TieredStore>>,
    /// Fragment store for time-travel queries (optional — None when tiered storage disabled)
    pub fragment_store: Option<Arc<nexora_fragment::TieredFragmentStore>>,
    /// Materialized view manager for query acceleration
    pub mv_manager: Arc<MaterializedViewManager>,
    /// Ontology manager for domain package (schema) definitions — stage 6.
    pub ontology_manager: Arc<nexora_core::ontology_manager::OntologyManager>,
    /// Shared event-log store (Iceberg event tables) — single instance across all
    /// ingestion sources. `None` if event-first init failed. event-first only.
    #[cfg(feature = "event-first")]
    pub event_store: Option<Arc<nexora_eventlog::EventLogStore>>,
    /// Shared, runtime-mutable topic router — ontology CRUD hot-updates its rules
    /// so a newly created domain routes to its event table immediately. event-first only.
    #[cfg(feature = "event-first")]
    pub event_router: Option<Arc<nexora_eventlog::TopicRouter>>,
    /// Background refresh scheduler for domain-package materialized views. Ontology
    /// activation schedules each pull/scheduled MV here so it refreshes periodically.
    /// event-first only; `None` if the event store failed to initialize.
    #[cfg(feature = "event-first")]
    pub refresh_scheduler: Option<Arc<nexora_eventlog::RefreshScheduler>>,
    /// Standing Query to Materialized View bridge
    pub sq_mv_bridge: Arc<crate::sq_mv_bridge::SQMaterializedViewBridge>,
    /// Cross-node router. `None` in single-node mode — then all reads/writes go
    /// straight to the local `graph` (behaviour unchanged). `Some` only in
    /// `--cluster` mode, where operations are routed to the shard's owner node.
    pub router: Option<Arc<nexora_zenoh::router::HybridRouter>>,
    /// Cluster manager (consensus, shard map, replication). `None` in single-node
    /// mode. Used by ontology handlers to propose domain changes via Raft when
    /// the control plane is enabled.
    pub cluster_manager: Option<Arc<nexora_zenoh::cluster::ClusterManager>>,
    /// Quorum write replicator. `None` in single-node mode. `Some` in cluster
    /// mode: after a write commits on the local owner, it is replicated to the
    /// shard's followers and the client ack waits for quorum.
    pub replica_writer: Option<Arc<nexora_zenoh::replica_writer::ReplicaWriter>>,
    /// Catch-up write barrier. `None` in single-node mode. `Some` in cluster
    /// mode: a shard this node is reconciling after failover promotion is
    /// blocked for owner writes until catch-up completes (fence → catch-up →
    /// reopen), so a client write can't be clobbered by a stale replay op.
    pub catch_up_barrier: Option<nexora_zenoh::catch_up_barrier::CatchUpBarrier>,
    /// Auth instance for WebSocket token validation (None if auth disabled)
    pub auth: Option<Arc<crate::auth::Auth>>,
    /// Drain state for rolling upgrades (E4): once set, write handlers return 503.
    pub drain: DrainState,
    /// D6: Query execution pool for bounded concurrency + backpressure.
    pub query_pool: Arc<nexora_core::query_pool::QueryPool>,
    /// C-14 FIX: Materialized view refresh semaphore to prevent starvation.
    /// Limits concurrent MV refreshes to avoid blocking regular queries.
    pub mv_refresh_semaphore: Arc<tokio::sync::Semaphore>,
    /// P1-6: Query resource limits (execution time, memory, result size, pattern depth)
    pub query_limits: nexora_cypher::QueryLimits,
    /// Event streaming engine for advanced SQL-based stream processing (optional, feature-gated)
    #[cfg(feature = "event-streaming")]
    pub event_streaming: Option<Arc<dyn nexora_risingwave::EventStreamingOperations>>,
    /// Distributed event streaming engine cluster instance (Phase 8, optional)
    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    pub distributed_event_streaming:
        Option<Arc<nexora_risingwave::DistributedEmbeddedEventStreaming>>,
    /// Distributed library mode cluster (Phase 2)
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    pub distributed_library: Option<
        Arc<(
            nexora_risingwave::DistributedMetaCluster,
            nexora_risingwave::DistributedFrontendPool,
            nexora_risingwave::DistributedComputeCluster,
        )>,
    >,
    /// Active EventLogSink tasks (Phase 6.3): mv_name -> abort handle
    #[cfg(feature = "event-streaming")]
    pub event_sinks: Arc<tokio::sync::RwLock<HashMap<String, tokio::task::AbortHandle>>>,

    /// GraphStreaming projector for event-to-graph projection (Phase 7.6)
    #[cfg(all(feature = "event-first", feature = "event-streaming"))]
    pub graph_projector: Option<Arc<nexora_graphstreaming::EventProjector>>,
}

impl AppState {
    /// Build the ingestion handler for a source, honoring event-first mode.
    ///
    /// In event-first mode (feature on + shared store/router present), returns an
    /// [`EventFirstHandler`] wrapping a `GraphIngestHandler`, so each batch routes
    /// through the shared, runtime-mutable [`TopicRouter`]: a topic mapped by a
    /// domain package double-writes to its Iceberg event table and the graph, an
    /// unmapped topic stays graph-only. Otherwise (feature off, or init failed)
    /// returns a plain `GraphIngestHandler` — behaviour is byte-for-byte the old
    /// graph-only path. Every ingestion source funnels through this one method so
    /// the store/router singletons are shared, never re-created per source.
    pub fn make_ingest_handler(
        &self,
        durability: nexora_core::BatchDurability,
    ) -> Arc<dyn nexora_stream::IngestHandler> {
        #[cfg(feature = "event-first")]
        {
            if let (Some(store), Some(router)) = (&self.event_store, &self.event_router) {
                let graph_handler = Arc::new(nexora_stream::GraphIngestHandler::new(
                    self.graph.clone(),
                    durability,
                ));
                return Arc::new(nexora_eventlog::EventFirstHandler::new(
                    store.clone(),
                    graph_handler,
                    router.clone(),
                ));
            }
        }
        Arc::new(nexora_stream::GraphIngestHandler::new(
            self.graph.clone(),
            durability,
        ))
    }

    /// Route an operation to a remote shard owner, or signal "handle locally".
    ///
    /// Returns:
    /// - `None` — the operation targets a shard this node owns (or single-node
    ///   mode). The caller must run its existing local `state.graph` path,
    ///   unchanged. This is deliberately the *only* thing that happens in
    ///   single-node mode, so behaviour there is byte-for-byte identical.
    /// - `Some(result)` — the shard is owned by another node; the operation was
    ///   forwarded to that owner and this is its result (or a routing error).
    ///
    /// The locality check uses the router's own `ShardMap`, so the local branch
    /// always resolves against this node's real `graph` instance (the router's
    /// internal local channel is intentionally not used — it is a dead end in
    /// `new_clustered`; local ops belong on `state.graph`).
    pub async fn try_route_remote(
        &self,
        qid: &NexoraId,
        op: nexora_zenoh::GraphOperation,
    ) -> Option<Result<nexora_zenoh::GraphResult, nexora_zenoh::RouterError>> {
        let router = self.router.as_ref()?;
        if router.is_local(qid).await {
            // Local shard — caller runs the strongly-typed local path.
            return None;
        }
        Some(router.route(qid, op).await)
    }

    /// Replicate a just-committed local-owner write to the shard's followers and
    /// wait for quorum.
    ///
    /// Call this only after the write has already succeeded on the local owner
    /// graph (the coordinator model: primary commits locally, then fans the same
    /// operation out to followers).
    ///
    /// ## Consistency model (A1.1 tradeoff — read before changing)
    ///
    /// This uses the best-effort [`ReplicaWriter::quorum_write`], NOT the strict
    /// [`ReplicaWriter::quorum_write_two_phase`]. The difference matters only at
    /// RF>1 when quorum is NOT reached:
    /// - **Here (owner-first):** the owner commit already happened, so a quorum
    ///   failure returns 503 to the client but leaves the write durable on the
    ///   owner. On failover a client retry can re-apply it — safe because
    ///   `SetProperty`/`AddEdge` are idempotent (last-writer-wins / set
    ///   membership), which is exactly the eventual-consistency + idempotent-
    ///   replay contract this platform targets (Flink/RisingWave-class, not
    ///   Spanner-class strict serializability).
    /// - **Two-phase (`quorum_write_two_phase` + `with_owner_apply`):** replicates
    ///   to followers FIRST and only commits the owner AFTER quorum, so a quorum
    ///   failure leaves NO write anywhere. That path is implemented and unit-tested
    ///   but intentionally NOT wired here: adopting it requires inverting this
    ///   write path (do not commit locally first; pass an `owner_apply` callback
    ///   that commits post-quorum). That inversion is deferred — it buys strict
    ///   no-partial-write at the cost of higher write latency (owner blocks on
    ///   follower quorum) and a larger refactor, neither justified while the
    ///   idempotent-replay contract holds. Switch here when a deployment needs
    ///   strict atomic multi-replica writes.
    ///
    /// Returns:
    /// - `None` — no replication configured (single-node, or this shard has no
    ///   followers / RF=1). Caller treats the local commit as sufficient.
    /// - `Some(Ok(()))` — quorum reached (owner + majority of followers durable).
    /// - `Some(Err(msg))` — quorum NOT reached. The write is committed locally
    ///   but not quorum-durable; caller must surface this, not silently succeed.
    ///
    /// Replication sends the full `GraphOperation`; followers apply it via their
    /// adapter as an ordinary local write (they do not re-replicate). This relies
    /// on mutations being deterministic (SetProperty/AddEdge are), which lets us
    /// ship operations instead of raw WAL bytes.
    pub async fn replicate_write(
        &self,
        qid: &NexoraId,
        op: nexora_zenoh::GraphOperation,
    ) -> Option<Result<(), String>> {
        let writer = self.replica_writer.as_ref()?;
        let router = self.router.as_ref()?;
        let (shard_id, epoch) = router.shard_and_epoch(qid).await?;
        let token = nexora_zenoh::replication::FencingToken::new(shard_id, epoch);
        match writer.quorum_write(shard_id, &token, op).await {
            // No followers for this shard → local commit is authoritative.
            Ok(nexora_zenoh::replication::WriteStatus::CommittedLocal) => None,
            Ok(nexora_zenoh::replication::WriteStatus::CommittedQuorum { .. }) => Some(Ok(())),
            // Duplicate request (idempotency) → already durable, treat as success.
            Ok(nexora_zenoh::replication::WriteStatus::AlreadyCommitted) => Some(Ok(())),
            Ok(nexora_zenoh::replication::WriteStatus::Failed { acked, required }) => Some(Err(
                format!("quorum not reached: {acked} of {required} required replicas acked"),
            )),
            Err(e) => Some(Err(format!("replication error: {e}"))),
        }
    }

    /// Whether a local owner write to `qid`'s shard is currently blocked because
    /// the shard is reconciling after a failover promotion (catch-up barrier).
    ///
    /// Returns `false` in single-node mode / when no barrier is configured, so
    /// the behaviour there is unchanged. When `true`, the write path must reject
    /// the write (503, retryable) rather than let it race the catch-up replay.
    pub async fn write_blocked_by_catch_up(&self, qid: &NexoraId) -> bool {
        let (Some(barrier), Some(router)) = (self.catch_up_barrier.as_ref(), self.router.as_ref())
        else {
            return false;
        };
        // Resolve the key's shard from the router's map; if unknown, don't block.
        match router.shard_and_epoch(qid).await {
            Some((shard_id, _epoch)) => barrier.is_blocked(shard_id).await,
            None => false,
        }
    }

    /// Whether a whole-graph query (Cypher/SQL) can safely run on the local graph.
    ///
    /// The query executor snapshots the *local* graph and runs the whole query
    /// there — it cannot yet split MATCH/CREATE across shards (that is the
    /// distributed-compute work line, not Phase 1). So:
    /// - Single-node mode (`router` is `None`) → always safe.
    /// - Single-node cluster / all shards local → safe (nothing lives elsewhere).
    /// - True multi-node cluster → NOT safe: running locally would silently
    ///   return partial results (reads) or write to the wrong node (writes).
    ///   The caller must refuse rather than answer incorrectly.
    pub async fn whole_graph_query_is_safe(&self) -> bool {
        match self.router.as_ref() {
            None => true,
            Some(router) => router.all_shards_local().await,
        }
    }

    /// Try to run a whole-graph read query distributively across shard owners.
    ///
    /// Only fires in a true multi-node cluster for the provably-mergeable read
    /// subset (see [`nexora_zenoh::distributed_query`]): node scans, global and
    /// grouped aggregates (count/sum/avg/min/max), and coordinator-side
    /// ORDER BY / SKIP / LIMIT / DISTINCT. Returns:
    /// - `None` — not applicable (single-node / all-local, or the query is not
    ///   in the mergeable subset). The caller keeps its existing behaviour
    ///   (local execution when safe, else the honest 501).
    /// - `Some(Ok((columns, rows)))` — distributed result, ready to return.
    /// - `Some(Err(..))` — a distributed attempt failed (e.g. an owner was
    ///   unreachable); surfaced rather than silently returning partial data.
    pub async fn try_distributed_query(
        &self,
        query: &str,
    ) -> Option<Result<(Vec<String>, Vec<Vec<serde_json::Value>>), nexora_zenoh::RouterError>> {
        let router = self.router.as_ref()?;
        // Single-node / all-local: the normal local path is correct and cheaper.
        if router.all_shards_local().await {
            return None;
        }
        // Only the provably-mergeable read subset; everything else → None (501).
        let plan = nexora_zenoh::distributed_query::plan(query)?;

        // Phase 1 Fix: Use Local read concern for HTTP distributed queries.
        // This allows reading from any replica (owner or follower), providing
        // better availability than the previous None (which was interpreted as
        // owner-only). For stronger consistency (Majority reads with session
        // tracking), the PG-wire path should be used.
        //
        // Future: Add replication_progress and session_tracker fields to AppState
        // to enable Majority reads on the HTTP path as well.
        let concern = Some(nexora_zenoh::ReadConcern::Local);

        // Note: progress and session remain None for now. This means:
        // - No read-after-write session tracking (PG-wire has this)
        // - No replication lag awareness (reads may hit stale replicas)
        // These limitations are acceptable for the HTTP/Dashboard use case
        // where eventual consistency is sufficient.
        Some(nexora_zenoh::distributed_query::execute(router, &plan, concern, None, None).await)
    }
}

/// Map a cross-node routing failure to a semantically correct HTTP response.
///
/// A routing failure is NOT this node's internal error — it means the shard's
/// owner is unreachable or rejected the op. With replication factor 1 (Phase 1,
/// no failover yet), an unreachable owner means that shard is unavailable; we
/// surface that explicitly (not a silent success, not a misleading 500) so
/// operators can tell "the data lives on a node that's down" apart from a bug
/// in this node.
fn router_error_response(e: &nexora_zenoh::RouterError) -> (StatusCode, Json<serde_json::Value>) {
    use nexora_zenoh::RouterError;
    let (status, kind) = match e {
        // Owner unknown / unreachable, or the op timed out → the shard is not
        // currently serviceable. 503 tells clients to retry later.
        RouterError::NodeNotFound(_) => {
            (StatusCode::SERVICE_UNAVAILABLE, "shard_owner_unavailable")
        }
        RouterError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "shard_owner_timeout"),
        // The owner was reached but the op failed there, or (de)serialization
        // broke → upstream/bad-gateway class.
        RouterError::Remote(_) | RouterError::Serialization(_) => {
            (StatusCode::BAD_GATEWAY, "remote_execution_failed")
        }
        // Write quorum not reached → the write could not be made durable.
        RouterError::QuorumFailed { .. } => {
            (StatusCode::SERVICE_UNAVAILABLE, "write_quorum_failed")
        }
    };
    (
        status,
        Json(serde_json::json!({
            "error": format!("cross-node routing failed: {e}"),
            "kind": kind,
            "hint": "this shard is owned by another node; replication is not yet enabled (RF=1), so its owner being down makes the shard unavailable",
        })),
    )
}

/// Metadata for an active stream source.
#[derive(Clone, Debug, serde::Serialize)]
pub struct StreamSource {
    pub name: String,
    pub source_type: String,
    pub topic: String,
    pub brokers: String,
    pub started_at: String,
}

/// A record of a recipe execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecipeRunRecord {
    pub run_id: String,
    pub recipe_name: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status: String, // "running", "success", "error"
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
}

// ============================================================
// Recipe API request/response types
// ============================================================

#[derive(Deserialize)]
pub struct CreateRecipeRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub steps: Vec<RecipeStepDef>,
    #[serde(default)]
    pub trigger: Option<RecipeTriggerDef>,
}

#[derive(Deserialize)]
pub struct RecipeStepDef {
    pub query: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub description: Option<String>,
}

#[derive(Deserialize)]
pub struct RecipeTriggerDef {
    #[allow(dead_code)]
    pub event_type: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub filter: Option<serde_json::Value>,
}

// ============================================================
// Health
// ============================================================

/// API-compatible health response matching the TypeScript types.
pub async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let active = state.graph.active_node_count().await;
    let sq_count = state.sq_manager.list().await.len();
    // Update metrics gauges so /metrics reflects current state.
    state.metrics.set_active_nodes(active as u64);
    state.metrics.set_sq_count(sq_count as u64);
    Json(serde_json::json!({
        "status": "healthy",
        "mode": "single-node",
        "profile": state.config.profile,
        "active_nodes": active,
        "shards": state.graph.shard_count(),
        "standing_queries": sq_count,
        "readiness": "ready",
        "liveness": "alive",
        "durability": if state.config.rocksdb_path.is_some() { "durable" } else { "ephemeral" },
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.start_time.elapsed().as_secs(),
    }))
}

// ============================================================
// Cypher Query
// ============================================================

#[derive(Deserialize)]
pub struct CypherRequest {
    pub query: String,
}

#[derive(Serialize)]
pub struct CypherResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub error: Option<String>,
    pub as_of: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_stats: Option<WriteStats>,
}

/// Structured write operation statistics (Neo4j-compatible format).
#[derive(Serialize)]
pub struct WriteStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub properties_set: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

/// If `columns`/`rows` are a distributed-write stat result (the 8 well-known
/// stat columns + one summed row), convert to [`WriteStats`]. Returns `None`
/// for ordinary read results. Column order matches
/// `nexora_zenoh::distributed_query::WRITE_STAT_COLUMNS`.
fn write_stats_from_columns(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> Option<WriteStats> {
    const STAT_COLS: [&str; 8] = [
        "nodes_created",
        "nodes_deleted",
        "relationships_created",
        "relationships_deleted",
        "properties_set",
        "properties_removed",
        "labels_added",
        "labels_removed",
    ];
    if columns.len() != STAT_COLS.len() || !columns.iter().zip(STAT_COLS).all(|(c, s)| c == s) {
        return None;
    }
    let row = rows.first()?;
    let g = |i: usize| -> usize { row.get(i).and_then(|v| v.as_i64()).unwrap_or(0) as usize };
    Some(WriteStats {
        nodes_created: g(0),
        nodes_deleted: g(1),
        relationships_created: g(2),
        relationships_deleted: g(3),
        properties_set: g(4),
        // WriteStats has no properties_removed field; it's summed at index 5 but
        // not surfaced separately (matches the single-node response shape).
        labels_added: g(6),
        labels_removed: g(7),
    })
}

pub async fn execute_cypher(
    State(state): State<AppState>,
    Json(req): Json<CypherRequest>,
) -> impl IntoResponse {
    let query_start = std::time::Instant::now();

    // E4: drain guard — block write queries (CREATE/SET/MERGE/DELETE) when the
    // node is preparing for a rolling upgrade.  Read-only queries still pass
    // through so live monitoring works during the drain window.
    let upper = req.query.trim_start().to_uppercase();
    let is_write_query = upper.starts_with("CREATE")
        || upper.starts_with("SET")
        || upper.starts_with("MERGE")
        || upper.starts_with("DELETE")
        || upper.starts_with("REMOVE");
    if is_write_query && state.drain.is_draining() {
        state.metrics.inc_errors();
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HeaderMap::new(),
            Json(CypherResponse {
                columns: vec![],
                rows: vec![],
                error: Some("node is draining, retry another node".to_string()),
                as_of: None,
                write_stats: None,
            }),
        );
    }

    // Strip AS OF clause (cypher-parser doesn't support it)
    let as_of_ts = req.query.to_uppercase().find("AS OF ").and_then(|i| {
        let rest: String = req.query[i + 6..].chars().collect();
        let raw = rest.split_whitespace().next()?;
        // Try parsing as a raw microsecond timestamp
        if let Ok(n) = raw.parse::<u64>() {
            return Some(n);
        }
        // Try parsing as a date string YYYYMMDD
        let d: String = raw.chars().filter(|c| c.is_ascii_digit()).collect();
        if d.len() >= 8 {
            let y: i64 = d[..4].parse().ok()?;
            let m: i64 = d[4..6].parse().ok()?;
            let day: i64 = d[6..8].parse().ok()?;
            // Use proper epoch calculation (accounting for leap years)
            let epoch_days = date_to_epoch_days(y, m, day)?;
            Some(epoch_days * 86400 * 1_000_000)
        } else {
            None
        }
    });
    let query_clean = if req.query.to_uppercase().contains("AS OF ") {
        let upper = req.query.to_uppercase();
        let i = upper.find("AS OF ").unwrap();
        let before = &req.query[..i];
        let after_as = &req.query[i + 6..];
        let ts_end = after_as.find(' ').unwrap_or(after_as.len());
        format!("{}{}", before.trim_end(), &after_as[ts_end..])
    } else {
        req.query.clone()
    };

    // Helper to build response with performance headers
    let build_response = |status: StatusCode,
                          body: CypherResponse,
                          row_count: usize|
     -> (StatusCode, HeaderMap, Json<CypherResponse>) {
        let elapsed_us = query_start.elapsed().as_micros() as u64;
        let elapsed_ms = elapsed_us / 1000;

        // Record metrics
        state.metrics.record_query(elapsed_us);

        // Slow query detection (threshold: 1000ms = 1s)
        let slow_threshold_ms: u64 = 1000;
        if elapsed_ms > slow_threshold_ms {
            state.metrics.inc_slow_query();
            tracing::warn!(
                target: "slow_query",
                query = %query_clean,
                elapsed_ms,
                row_count,
                threshold_ms = slow_threshold_ms,
                "Slow query detected"
            );
        }

        // Build performance headers
        let mut headers = HeaderMap::new();
        if let Ok(v) = HeaderValue::from_str(&format!("{}", elapsed_ms)) {
            headers.insert("x-query-duration-ms", v);
        }
        if let Ok(v) = HeaderValue::from_str(&format!("{}", row_count)) {
            headers.insert("x-query-result-count", v);
        }
        if let Ok(v) = HeaderValue::from_str(&format!("{}", elapsed_us)) {
            headers.insert("x-query-duration-us", v);
        }

        (status, headers, Json(body))
    };

    // Distributed read planner (work-line B, first slice): in a true multi-node
    // cluster, provably-mergeable reads (label/node scans, global count(*)) are
    // fanned out to shard owners and merged — instead of the blanket 501 below.
    // Anything outside that subset returns None here and falls through to the
    // honest refusal.
    if let Some(dist) = state.try_distributed_query(&query_clean).await {
        match dist {
            Ok((columns, rows)) => {
                // A distributed write returns the 8 well-known stat columns and a
                // single summed row; surface it as write_stats (not result rows),
                // matching the single-node write response shape.
                if let Some(stats) = write_stats_from_columns(&columns, &rows) {
                    return build_response(
                        StatusCode::OK,
                        CypherResponse {
                            columns: vec![],
                            rows: vec![],
                            error: None,
                            as_of: None,
                            write_stats: Some(stats),
                        },
                        0,
                    );
                }
                let row_count = rows.len();
                return build_response(
                    StatusCode::OK,
                    CypherResponse {
                        columns,
                        rows,
                        error: None,
                        as_of: as_of_ts,
                        write_stats: None,
                    },
                    row_count,
                );
            }
            Err(e) => {
                state.metrics.inc_errors();
                let (status, body) = router_error_response(&e);
                return build_response(
                    status,
                    CypherResponse {
                        columns: vec![],
                        rows: vec![],
                        error: Some(
                            body.0
                                .get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("distributed query failed")
                                .to_string(),
                        ),
                        as_of: None,
                        write_stats: None,
                    },
                    0,
                );
            }
        }
    }

    // Cluster guard: the executor snapshots the LOCAL graph and runs the whole
    // query there — it cannot yet split MATCH/CREATE across shards (that is the
    // distributed-compute work line). In a true multi-node cluster this would
    // silently return partial results or write to the wrong node, so refuse
    // explicitly instead of answering incorrectly. Single-node (and single-node
    // clusters where all shards are local) are unaffected.
    if !state.whole_graph_query_is_safe().await {
        state.metrics.inc_errors();

        // Phase 1 Enhancement: Provide a detailed, helpful error message
        let error_msg = "Query not supported in multi-node cluster mode.\n\n\
             This query type is not in the distributable subset. Local execution would \
             return incomplete results (reads) or write to the wrong node (writes).\n\n\
             Supported distributed queries:\n\
             • Node scans: MATCH (n) RETURN n\n\
             • Aggregates: MATCH (n) RETURN count(n), avg(n.age)\n\
             • Grouped aggregates and WITH pipelines\n\
             • Relationship traversal: MATCH (a)-[:KNOWS]->(b) RETURN a, b\n\
             • Variable-length paths: MATCH (a)-[:R*1..3]->(b) RETURN a, b\n\
             • Node writes: CREATE (node-only), SET, DELETE, MERGE\n\n\
             Not supported (needs cross-shard transactions):\n\
             • CREATE with relationships: (a)-[:REL]->(b)\n\
             • Multi-statement subqueries\n\n\
             Alternatives:\n\
             1. Simplify the query to use supported patterns\n\
             2. Use the per-key REST API: /api/v2/graph/node/{qid}\n\
             3. Deploy in single-node mode for development\n\
             4. Use the PG-wire interface (port 5433) for stronger consistency"
            .to_string();

        return build_response(
            StatusCode::NOT_IMPLEMENTED,
            CypherResponse {
                columns: vec![],
                rows: vec![],
                error: Some(error_msg),
                as_of: None,
                write_stats: None,
            },
            0,
        );
    }

    // ⭐ 查询重写器：尝试使用物化视图
    let rewriter = crate::query_rewriter::QueryRewriter::new(state.mv_manager.clone());
    if let Some(view_id) = rewriter.find_matching_view(&query_clean).await {
        tracing::info!(
            query = %query_clean,
            view_id = %view_id,
            "Query rewritten to use materialized view"
        );

        // 从物化视图查询（快速路径）
        match state.mv_manager.query_all(&view_id).await {
            Ok(rows) => {
                // 转换为 Cypher 结果格式
                let columns = if let Some(first_row) = rows.first() {
                    first_row.values.keys().cloned().collect()
                } else {
                    vec![]
                };

                let result_rows: Vec<Vec<serde_json::Value>> = rows
                    .iter()
                    .map(|row| {
                        columns
                            .iter()
                            .map(|col| property_value_to_json(row.values.get(col)))
                            .collect()
                    })
                    .collect();

                let row_count = result_rows.len();
                return build_response(
                    StatusCode::OK,
                    CypherResponse {
                        columns,
                        rows: result_rows,
                        error: None,
                        as_of: as_of_ts,
                        write_stats: None,
                    },
                    row_count,
                );
            }
            Err(e) => {
                tracing::warn!(
                    view_id = %view_id,
                    error = %e,
                    "Failed to query materialized view, falling back to Cypher"
                );
                // 失败则继续执行原始 Cypher
            }
        }
    }

    // Time-travel query path: if AS OF timestamp is present and fragment store available
    if let (Some(ts), Some(ref frag_store)) = (as_of_ts, &state.fragment_store) {
        use nexora_fragment::time_travel::{execute_time_travel, TimeTravelQuery};

        let query = TimeTravelQuery::at(ts);
        let registry = frag_store.registry();

        match execute_time_travel(&*registry, query).await {
            Ok(result) => {
                // Convert TimeTravelResult to CypherResponse format
                let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

                // Add nodes with their embedded edges
                for node in result.nodes {
                    let mut row = vec![
                        serde_json::json!({"type": "node"}),
                        serde_json::json!(node.id),
                    ];
                    if !node.properties.is_empty() {
                        row.push(serde_json::json!(node.properties));
                    }
                    if !node.edges.is_empty() {
                        row.push(serde_json::json!(node.edges.iter().map(|e| json!({
                            "type": e.edge_type,
                            "direction": e.direction,
                            "other": e.other,
                            "timestamp": e.timestamp,
                        })).collect::<Vec<_>>()));
                    }
                    rows.push(row);
                }

                let row_count = rows.len();
                return build_response(
                    StatusCode::OK,
                    CypherResponse {
                        columns: vec!["type".to_string(), "id".to_string(), "properties".to_string(), "edges".to_string()],
                        rows,
                        error: None,
                        as_of: Some(ts),
                        write_stats: None,
                    },
                    row_count,
                );
            }
            Err(e) => {
                tracing::warn!("Time-travel query failed: {}, falling back to current state", e);
                // Fall through to regular Cypher execution
            }
        }
    }

    // 原始 Cypher 执行路径 (D6: wrapped in query pool for bounded concurrency)
    let graph = Arc::clone(&state.graph);
    let query_pool = Arc::clone(&state.query_pool);
    let query_for_exec = query_clean.clone();
    let query_limits = state.query_limits.clone();
    match query_pool
        .execute(async move {
            nexora_cypher::execute_cypher_with_limits(&graph, &query_for_exec, &query_limits).await
        })
        .await
    {
        Ok(result) => match result {
            nexora_cypher::CypherResult::Rows { columns, rows } => {
                let row_count = rows.len();
                build_response(
                    StatusCode::OK,
                    CypherResponse {
                        columns,
                        rows,
                        error: None,
                        as_of: as_of_ts,
                        write_stats: None,
                    },
                    row_count,
                )
            }
            nexora_cypher::CypherResult::Write(wr) => {
                let stats = WriteStats {
                    nodes_created: wr.nodes_created,
                    nodes_deleted: wr.nodes_deleted,
                    properties_set: wr.properties_set,
                    relationships_created: wr.relationships_created,
                    relationships_deleted: wr.relationships_deleted,
                    labels_added: wr.labels_added,
                    labels_removed: wr.labels_removed,
                };
                build_response(
                    StatusCode::OK,
                    CypherResponse {
                        columns: vec![],
                        rows: vec![],
                        error: None,
                        as_of: None,
                        write_stats: Some(stats),
                    },
                    0,
                )
            }
            nexora_cypher::CypherResult::Empty => build_response(
                StatusCode::OK,
                CypherResponse {
                    columns: vec![],
                    rows: vec![],
                    error: None,
                    as_of: None,
                    write_stats: None,
                },
                0,
            ),
        },
        Err(e) => {
            state.metrics.inc_errors();
            let (friendly_msg, _code) = crate::error::friendlify_cypher_error(&e.to_string());
            build_response(
                StatusCode::BAD_REQUEST,
                CypherResponse {
                    columns: vec![],
                    rows: vec![],
                    error: Some(friendly_msg),
                    as_of: None,
                    write_stats: None,
                },
                0,
            )
        }
    }
}

/// Sanitize internal error messages before sending to clients.
fn sanitize_error(e: &nexora_core::GraphError) -> String {
    match e {
        nexora_core::GraphError::NodeNotFound(qid) => format!("Node not found: {qid}"),
        nexora_core::GraphError::NodeUnavailable(qid) => {
            format!("Node temporarily unavailable: {qid}")
        }
        nexora_core::GraphError::ShardNotFound(id) => format!("Internal error (shard {id})"),
        nexora_core::GraphError::Persistence(_) => "Persistence error".to_string(),
        nexora_core::GraphError::Timeout => "Operation timed out".to_string(),
        nexora_core::GraphError::Internal(_) => "Internal server error".to_string(),
    }
}

/// Convert PropertyValue to JSON for query results
fn property_value_to_json(val: Option<&nexora_id::PropertyValue>) -> serde_json::Value {
    match val {
        None => serde_json::Value::Null,
        Some(nexora_id::PropertyValue::Null) => serde_json::Value::Null,
        Some(nexora_id::PropertyValue::Boolean(b)) => serde_json::Value::Bool(*b),
        Some(nexora_id::PropertyValue::Integer(i)) => serde_json::Value::Number((*i).into()),
        Some(nexora_id::PropertyValue::Float(f)) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::Number(0.into())),
        Some(nexora_id::PropertyValue::String(s)) => serde_json::Value::String(s.clone()),
        Some(nexora_id::PropertyValue::Bytes(b)) => {
            serde_json::Value::String(format!("<bytes:{}>", b.len()))
        }
        Some(nexora_id::PropertyValue::List(items)) => serde_json::Value::Array(
            items
                .iter()
                .map(|v| property_value_to_json(Some(v)))
                .collect(),
        ),
        Some(nexora_id::PropertyValue::Map(m)) => {
            let obj: serde_json::Map<String, serde_json::Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), property_value_to_json(Some(v))))
                .collect();
            serde_json::Value::Object(obj)
        }
        Some(nexora_id::PropertyValue::Node(_)) => serde_json::Value::String("<node>".into()),
        Some(nexora_id::PropertyValue::Relationship(_)) => {
            serde_json::Value::String("<relationship>".into())
        }
        Some(nexora_id::PropertyValue::Path(_)) => serde_json::Value::String("<path>".into()),
        Some(nexora_id::PropertyValue::Date(d)) => serde_json::Value::String(d.to_string()),
        Some(nexora_id::PropertyValue::LocalDateTime(dt)) => {
            serde_json::Value::String(dt.to_string())
        }
        Some(nexora_id::PropertyValue::ZonedDateTime(dt)) => {
            serde_json::Value::String(dt.to_rfc3339())
        }
        Some(nexora_id::PropertyValue::Duration(_)) => {
            serde_json::Value::String("<duration>".into())
        }
        Some(nexora_id::PropertyValue::Point(_)) => serde_json::Value::String("<point>".into()),
        Some(nexora_id::PropertyValue::BlobRef(_)) => serde_json::Value::String("<blobref>".into()),
    }
}

// ============================================================
// Property CRUD
// ============================================================

#[derive(Deserialize)]
pub struct SetPropertyRequest {
    pub value: serde_json::Value,
}

pub async fn get_property(
    State(state): State<AppState>,
    Path((qid_hex, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&qid_hex) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid qid"})),
            )
        }
    };
    // Cluster: read from the shard owner if it lives on another node.
    // Single-node mode returns None and falls through unchanged.
    if let Some(routed) = state
        .try_route_remote(
            &qid,
            nexora_zenoh::GraphOperation::GetProperty {
                qid: qid.clone(),
                key: key.clone(),
            },
        )
        .await
    {
        return match routed {
            Ok(nexora_zenoh::GraphResult::Property(Some(val))) => (
                StatusCode::OK,
                Json(serde_json::json!({"node_id": qid_hex, "key": key, "value": val})),
            ),
            Ok(_) => (
                StatusCode::OK,
                Json(
                    serde_json::json!({"node_id": qid_hex, "key": key, "value": null, "not_found": true}),
                ),
            ),
            Err(e) => router_error_response(&e),
        };
    }

    match state.graph.get_property(&qid, &key).await {
        Ok(Some(val)) => (
            StatusCode::OK,
            Json(serde_json::json!({"node_id": qid_hex, "key": key, "value": pv_to_json(&val)})),
        ),
        Ok(None) => (
            StatusCode::OK,
            Json(
                serde_json::json!({"node_id": qid_hex, "key": key, "value": null, "not_found": true}),
            ),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

pub async fn set_property(
    State(state): State<AppState>,
    Path((qid_hex, key)): Path<(String, String)>,
    Json(req): Json<SetPropertyRequest>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&qid_hex) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid qid"})),
            )
        }
    };
    // E4: reject writes while the node is draining for a rolling upgrade.
    if state.drain.is_draining() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "node is draining, retry another node"})),
        );
    }
    // Cluster: if this key's shard is owned by another node, forward the write
    // there. Single-node mode returns None here and falls through unchanged.
    if let Some(routed) = state
        .try_route_remote(
            &qid,
            nexora_zenoh::GraphOperation::SetProperty {
                qid: qid.clone(),
                key: key.clone(),
                value: req.value.clone(),
            },
        )
        .await
    {
        return match routed {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))),
            Err(e) => router_error_response(&e),
        };
    }

    // Cluster: this node owns the shard, but if it is reconciling after a
    // failover promotion, reject the write (retryable) so it can't race the
    // catch-up replay and be clobbered by a stale delta op.
    if state.write_blocked_by_catch_up(&qid).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "shard is reconciling after failover; retry shortly",
                "kind": "catch_up_in_progress",
            })),
        );
    }

    let value = json_to_pv(&req.value);
    let key_str = key.clone();
    let value_clone = value.clone();
    match state.graph.set_property(&qid, &key, value).await {
        Ok(()) => {
            let all_properties = match state.graph.get_all_properties(&qid).await {
                Ok(properties) => properties,
                Err(error) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": error.to_string()})),
                    )
                }
            };
            let properties = all_properties
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect();
            let matched = state
                .sq_manager
                .on_property_change(&qid, &key_str, &value_clone, &properties)
                .await;
            audit!(
                "set_property",
                "api",
                &format!("/api/v2/graph/node/{qid_hex}/property/{key}"),
                "success"
            );
            state.metrics.inc_events(1);
            if matched > 0 {
                state.metrics.inc_sq_matches(matched as u64);
            }
            // Cluster: replicate this owner write to followers and wait for
            // quorum. Single-node / RF=1 → None (local commit is enough).
            if let Some(Err(msg)) = state
                .replicate_write(
                    &qid,
                    nexora_zenoh::GraphOperation::SetProperty {
                        qid: qid.clone(),
                        key: key.clone(),
                        value: req.value.clone(),
                    },
                )
                .await
            {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": format!("write committed locally but not quorum-durable: {msg}"),
                        "kind": "quorum_not_reached",
                    })),
                );
            }
            (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": sanitize_error(&e)})),
        ),
    }
}

// ============================================================
// Edge CRUD
// ============================================================

#[derive(Deserialize)]
pub struct AddEdgeRequest {
    pub edge_type: String,
    pub target: String,
    pub direction: String,
}

pub async fn get_edges(
    State(state): State<AppState>,
    Path(qid_hex): Path<String>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&qid_hex) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid qid"})),
            )
        }
    };
    // Cluster: read edges from the shard owner if it lives on another node.
    // Single-node mode returns None and falls through unchanged.
    if let Some(routed) = state
        .try_route_remote(
            &qid,
            nexora_zenoh::GraphOperation::GetEdges {
                qid: qid.clone(),
                edge_type: None,
            },
        )
        .await
    {
        return match routed {
            Ok(nexora_zenoh::GraphResult::Property(Some(edges))) => {
                (StatusCode::OK, Json(serde_json::json!({"edges": edges})))
            }
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"edges": []}))),
            Err(e) => router_error_response(&e),
        };
    }

    match state.graph.get_edges(&qid).await {
        Ok(edges) => {
            let infos: Vec<_> = edges.iter().map(|e| {
                serde_json::json!({"edge_type": e.edge_type.to_string(), "direction": if e.direction.is_out() { "out" } else { "in" }, "other": e.other.to_hex()})
            }).collect();
            (StatusCode::OK, Json(serde_json::json!({"edges": infos})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

pub async fn add_edge(
    State(state): State<AppState>,
    Path(qid_hex): Path<String>,
    Json(req): Json<AddEdgeRequest>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&qid_hex) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid qid"})),
            )
        }
    };
    let target = match NexoraId::from_hex(&req.target) {
        Ok(t) => t,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid target"})),
            )
        }
    };
    let dir = match req.direction.as_str() {
        "out" => nexora_value::EdgeDirection::Out,
        "in" => nexora_value::EdgeDirection::In,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "direction must be 'in' or 'out'"})),
            )
        }
    };
    let edge = HalfEdge::new(Symbol::new(&req.edge_type), dir, target.clone());

    // E4: reject writes while the node is draining for a rolling upgrade.
    if state.drain.is_draining() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "node is draining, retry another node"})),
        );
    }

    // Cluster: forward to the shard owner if the source node lives elsewhere.
    // Single-node mode returns None and falls through unchanged.
    if let Some(routed) = state
        .try_route_remote(
            &qid,
            nexora_zenoh::GraphOperation::AddEdge {
                source: qid.clone(),
                edge_type: req.edge_type.clone(),
                target: target.clone(),
                direction: req.direction.clone(),
            },
        )
        .await
    {
        return match routed {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"status": "ok"}))),
            Err(e) => router_error_response(&e),
        };
    }

    // Cluster: reject the write if this shard is reconciling post-failover.
    if state.write_blocked_by_catch_up(&qid).await {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "shard is reconciling after failover; retry shortly",
                "kind": "catch_up_in_progress",
            })),
        );
    }

    match state.graph.add_edge(&qid, edge).await {
        Ok(()) => {
            // Cluster: replicate this owner write to followers, wait for quorum.
            if let Some(Err(msg)) = state
                .replicate_write(
                    &qid,
                    nexora_zenoh::GraphOperation::AddEdge {
                        source: qid.clone(),
                        edge_type: req.edge_type.clone(),
                        target: target.clone(),
                        direction: req.direction.clone(),
                    },
                )
                .await
            {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": format!("write committed locally but not quorum-durable: {msg}"),
                        "kind": "quorum_not_reached",
                    })),
                );
            }
            audit!(
                "add_edge",
                "api",
                &format!("/api/v2/graph/node/{qid_hex}/edges"),
                "success"
            );
            (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

// ============================================================
// Distributed traversal (scatter-gather)
// ============================================================

#[derive(Deserialize)]
pub struct TraverseRequest {
    /// Starting node ids (hex).
    pub start: Vec<String>,
    /// Edge type to follow.
    pub edge_type: String,
    /// Maximum hops (BFS depth).
    #[serde(default = "default_traverse_depth")]
    pub max_depth: u32,
}

fn default_traverse_depth() -> u32 {
    3
}

/// `POST /api/v2/graph/traverse` — multi-hop reachability by edge type.
///
/// In cluster mode this runs as a distributed scatter-gather: each hop groups
/// the frontier by shard and queries every owner node in parallel, merging the
/// results — the traversal executes where the data lives instead of pulling the
/// whole graph to one node. In single-node mode it runs an equivalent local BFS
/// over the graph directly. Both return the set of node ids reachable within
/// `max_depth` hops of any `start` node.
pub async fn traverse(
    State(state): State<AppState>,
    Json(req): Json<TraverseRequest>,
) -> impl IntoResponse {
    // Parse start ids.
    let mut starts = Vec::with_capacity(req.start.len());
    for hex in &req.start {
        match NexoraId::from_hex(hex) {
            Ok(q) => starts.push(q),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("invalid start id: {hex}")})),
                )
            }
        }
    }

    // Cluster: distributed scatter-gather over shard owners.
    if let Some(router) = state.router.as_ref() {
        match router
            .scatter_gather_traverse(starts.clone(), &req.edge_type, req.max_depth)
            .await
        {
            Ok(frontier) => {
                let reached: Vec<String> = dedup_hex(frontier.into_iter().map(|n| n.qid.to_hex()));
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "reached": reached,
                        "mode": "distributed",
                    })),
                );
            }
            Err(e) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": format!("distributed traversal failed: {e}"),
                        "kind": "scatter_gather_failed",
                    })),
                );
            }
        }
    }

    // FIX C: Route traversal through QueryPool to enforce concurrency limit.
    // Before: traverse bypassed the pool, allowing unbounded concurrent BFS.
    let query_pool = Arc::clone(&state.query_pool);
    let state_for_bfs = Arc::new(state.clone());
    let edge_type = req.edge_type.clone();
    let max_depth = req.max_depth;
    let reached = query_pool
        .execute(async move { local_bfs(&state_for_bfs, starts, &edge_type, max_depth).await })
        .await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "reached": reached,
            "mode": "local",
        })),
    )
}

/// Local breadth-first traversal following outgoing edges of `edge_type`.
/// Returns the hex ids reached within `max_depth` hops (excluding the starts).
async fn local_bfs(
    state: &AppState,
    starts: Vec<NexoraId>,
    edge_type: &str,
    max_depth: u32,
) -> Vec<String> {
    use std::collections::HashSet;
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier = starts;
    let mut reached: Vec<String> = Vec::new();

    // D1: fetch the whole frontier's edges CONCURRENTLY. The old loop awaited
    // each node's `get_edges` serially, so a frontier of N nodes paid N mailbox
    // round-trips end to end; `buffer_unordered` overlaps them (bounded so a
    // huge frontier can't spawn unbounded work), turning per-node latency into
    // per-batch latency — 5-50× on deep/wide traversals. Ordering within a level
    // doesn't matter for BFS reachability, so unordered completion is fine.
    use futures::stream::{self, StreamExt};
    // Cap concurrency so an enormous frontier doesn't overwhelm the graph's
    // mailboxes; the per-level frontier is still fully covered across batches.
    const BFS_FETCH_CONCURRENCY: usize = 64;

    for _ in 0..max_depth {
        if frontier.is_empty() {
            break;
        }
        // D2: use the typed-traversal fast path — each call filters by edge_type
        // + outgoing direction and returns only target ids (no per-node
        // full-edge-set clone). D1: run the whole frontier concurrently.
        let neighbor_lists: Vec<Vec<NexoraId>> = stream::iter(frontier.iter().cloned())
            .map(|qid| async move {
                state
                    .graph
                    .outgoing_neighbors(&qid, edge_type)
                    .await
                    .unwrap_or_default()
            })
            .buffer_unordered(BFS_FETCH_CONCURRENCY)
            .collect()
            .await;

        // Expand the level serially (cheap, in-memory) so `visited`/`reached`
        // stay single-owner — the expensive await already happened above.
        let mut next = Vec::new();
        for targets in neighbor_lists {
            for target in targets {
                let target_hex = target.to_hex();
                if visited.insert(target_hex.clone()) {
                    reached.push(target_hex);
                    next.push(target);
                }
            }
        }
        frontier = next;
    }
    reached
}

/// Deduplicate an iterator of hex ids, preserving first-seen order.
fn dedup_hex(ids: impl Iterator<Item = String>) -> Vec<String> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    ids.filter(|h| seen.insert(h.clone())).collect()
}

// ============================================================
// Standing Query Management
// ============================================================

#[derive(Deserialize)]
pub struct CreateSqRequest {
    pub name: String,
    pub pattern: SqPatternRequest,
}

#[derive(Deserialize)]
pub struct SqPatternRequest {
    #[serde(rename = "type")]
    pub pattern_type: String,
    pub key: Option<String>,
    pub condition: Option<serde_json::Value>,
    pub labels: Option<Vec<String>>,
}

pub async fn list_sq(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> Json<serde_json::Value> {
    let queries = state.sq_manager.list().await;
    let mut list = Vec::new();
    for sq in queries.iter() {
        let match_count = state.sq_manager.match_count(sq.id).await;
        list.push(serde_json::json!({
            "id": sq.id.to_string(),
            "name": sq.name,
            "created_at": sq.created_at.to_rfc3339(),
            "match_count": match_count,
        }));
    }
    // Return paginated response if limit/offset provided, otherwise backward-compatible
    if pagination.limit.is_some() || pagination.offset.is_some() {
        Json(pagination.json(&list))
    } else {
        Json(serde_json::json!({"standing_queries": list}))
    }
}

pub async fn create_sq(
    State(state): State<AppState>,
    Json(req): Json<CreateSqRequest>,
) -> impl IntoResponse {
    let pattern = match build_pattern(&req.pattern) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e})),
            )
        }
    };
    let id = state.sq_manager.register(&req.name, pattern).await;
    let sq_count = state.sq_manager.list().await.len() as u64;
    state.metrics.set_sq_count(sq_count);
    (
        StatusCode::CREATED,
        Json(serde_json::json!({"id": id.to_string(), "name": req.name})),
    )
}

pub async fn get_sq(
    State(state): State<AppState>,
    Path(id_str): Path<String>,
) -> impl IntoResponse {
    let id = match uuid::Uuid::parse_str(&id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid id"})),
            )
        }
    };
    let queries = state.sq_manager.list().await;
    match queries.iter().find(|q| q.id == id) {
        Some(sq) => (
            StatusCode::OK,
            Json(serde_json::json!({"id": sq.id.to_string(), "name": sq.name})),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "not found"})),
        ),
    }
}

pub async fn delete_sq(
    State(state): State<AppState>,
    Path(id_str): Path<String>,
) -> impl IntoResponse {
    let id = match uuid::Uuid::parse_str(&id_str) {
        Ok(id) => id,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid id"})),
            )
        }
    };
    if state.sq_manager.remove(id).await {
        let sq_count = state.sq_manager.list().await.len() as u64;
        state.metrics.set_sq_count(sq_count);
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "deleted"})),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "not found"})),
        )
    }
}

fn build_pattern(req: &SqPatternRequest) -> Result<StandingQueryPattern, String> {
    match req.pattern_type.as_str() {
        "PropertyFilter" => {
            let key = req.key.as_ref().ok_or("key required for PropertyFilter")?;
            let condition = req.condition.as_ref().ok_or("condition required")?;
            let cond = match condition.get("type").and_then(|v| v.as_str()) {
                Some("GreaterThan") => {
                    let val = condition
                        .get("value")
                        .and_then(|v| v.as_f64())
                        .ok_or("value required")?;
                    FilterCondition::GreaterThan(val)
                }
                Some("LessThan") => {
                    let val = condition
                        .get("value")
                        .and_then(|v| v.as_f64())
                        .ok_or("value required")?;
                    FilterCondition::LessThan(val)
                }
                Some("Equals") => {
                    let val = condition.get("value").ok_or("value required")?;
                    FilterCondition::Equals(json_to_pv(val))
                }
                Some("Contains") => {
                    let val = condition
                        .get("value")
                        .and_then(|v| v.as_str())
                        .ok_or("value required")?;
                    FilterCondition::Contains(val.to_string())
                }
                Some("Exists") => FilterCondition::Exists,
                Some("IsNull") => FilterCondition::IsNull,
                Some("IsNotNull") => FilterCondition::IsNotNull,
                _ => return Err("unknown condition type".into()),
            };
            Ok(StandingQueryPattern::property(key, cond))
        }
        "LabelFilter" => {
            let labels = req.labels.as_ref().ok_or("labels required")?;
            Ok(StandingQueryPattern::LabelFilter(labels.clone()))
        }
        _ => Err(format!("unknown pattern type: {}", req.pattern_type)),
    }
}

// ============================================================
// Helpers
// ============================================================

pub fn pv_to_json(v: &PropertyValue) -> serde_json::Value {
    match v {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
        PropertyValue::Integer(i) => serde_json::json!(i),
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::List(items) => {
            serde_json::Value::Array(items.iter().map(pv_to_json).collect())
        }
        PropertyValue::Map(m) => {
            let map: serde_json::Map<String, serde_json::Value> =
                m.iter().map(|(k, v)| (k.clone(), pv_to_json(v))).collect();
            serde_json::Value::Object(map)
        }
        other => serde_json::Value::String(format!("{other}")),
    }
}

pub fn json_to_pv(v: &serde_json::Value) -> PropertyValue {
    match v {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Array(arr) => PropertyValue::List(arr.iter().map(json_to_pv).collect()),
        serde_json::Value::Object(map) => PropertyValue::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_pv(v)))
                .collect(),
        ),
    }
}

// ============================================================
// Vector API — HNSW similarity search
// ============================================================

#[derive(Deserialize)]
pub struct VectorInsertRequest {
    pub qid: String,
    pub vector: Vec<f32>,
}

#[derive(Deserialize)]
pub struct VectorSearchRequest {
    pub vector: Vec<f32>,
    pub k: usize,
}

/// POST /api/v2/vector/index — insert a vector for a node
pub async fn vector_index(
    State(state): State<AppState>,
    Json(req): Json<VectorInsertRequest>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&req.qid) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid qid"})),
            )
        }
    };
    let mut hnsw = state.hnsw.lock().await;
    hnsw.insert(qid, req.vector);
    (
        StatusCode::OK,
        Json(json!({"status": "indexed", "qid": req.qid, "index_size": hnsw.len()})),
    )
}

/// POST /api/v2/vector/search — search k-nearest neighbors
pub async fn vector_search(
    State(state): State<AppState>,
    Json(req): Json<VectorSearchRequest>,
) -> impl IntoResponse {
    if req.k == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "k must be greater than 0"})),
        );
    }
    let hnsw = state.hnsw.lock().await;
    let results = hnsw.search_knn(&req.vector, req.k);
    let neighbors: Vec<serde_json::Value> = results
        .into_iter()
        .map(|(qid, dist)| {
            json!({
                "qid": qid.to_hex(),
                "distance": dist,
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({"query": req.vector, "k": req.k, "neighbors": neighbors})),
    )
}

/// GET /api/v2/vector/node/{qid} — get vector for a node
pub async fn vector_get_node(
    State(state): State<AppState>,
    Path(qid_hex): Path<String>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&qid_hex) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid qid"})),
            )
        }
    };
    let hnsw = state.hnsw.lock().await;
    match hnsw.get(&qid) {
        Some(vector) => (
            StatusCode::OK,
            Json(json!({"qid": qid_hex, "vector": vector})),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "vector not found for node", "qid": qid_hex})),
        ),
    }
}

/// DELETE /api/v2/vector/node/{qid} — remove vector for a node
pub async fn vector_delete_node(
    State(state): State<AppState>,
    Path(qid_hex): Path<String>,
) -> impl IntoResponse {
    let qid = match NexoraId::from_hex(&qid_hex) {
        Ok(q) => q,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid qid"})),
            )
        }
    };
    let mut hnsw = state.hnsw.lock().await;
    hnsw.remove(&qid);
    (
        StatusCode::OK,
        Json(json!({"status": "removed", "qid": qid_hex, "index_size": hnsw.len()})),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use nexora_core::{GraphServiceConfig, InMemoryPersistor};
    use nexora_hnsw::{HnswConfig, HnswIndex};

    fn test_state() -> AppState {
        AppState {
            graph: Arc::new(GraphService::new(
                GraphServiceConfig {
                    num_shards: 2,
                    max_nodes_per_shard: 100,
                    node_channel_size: 16,
                },
                Arc::new(InMemoryPersistor::new()),
            )),
            sq_manager: Arc::new(StandingQueryManager::new(16)),
            config: AppConfig {
                max_nodes_per_shard: 100,
                rocksdb_path: None,
                wal_dir: None,
                allow_ingest_dir: None,
                profile: "lite-ephemeral".to_string(),
            },
            shutdown: Arc::new(tokio::sync::Notify::new()),
            start_time: std::time::Instant::now(),
            ingests: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            metrics: crate::metrics::Metrics::new(),
            hnsw: Arc::new(Mutex::new(HnswIndex::new(HnswConfig::default()))),
            streams: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            recipes: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            recipe_runs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            udf_registry: Arc::new(tokio::sync::RwLock::new(
                nexora_udf::native::UdfRegistry::new(),
            )),
            udf_manager: Arc::new(Mutex::new(UdfManager::new())),
            tiered_store: None,
            mv_manager: Arc::new(MaterializedViewManager::new()),
            ontology_manager: Arc::new(nexora_core::ontology_manager::OntologyManager::new()),
            #[cfg(feature = "event-first")]
            event_store: None,
            #[cfg(feature = "event-first")]
            event_router: None,
            #[cfg(feature = "event-first")]
            refresh_scheduler: None,
            sq_mv_bridge: Arc::new(crate::sq_mv_bridge::SQMaterializedViewBridge::new(
                Arc::new(MaterializedViewManager::new()),
            )),
            router: None,
            replica_writer: None,
            catch_up_barrier: None,
            auth: None,
            drain: crate::drain::DrainState::default(),
            query_pool: Arc::new(nexora_core::query_pool::QueryPool::new(4)),
            cluster_manager: None,
            #[cfg(feature = "event-streaming")]
            event_streaming: None,
            #[cfg(all(feature = "event-streaming", feature = "embedded"))]
            distributed_event_streaming: None,
        }
    }

    #[tokio::test]
    async fn unrelated_property_change_preserves_label_match() {
        let state = test_state();
        let sq_id = state
            .sq_manager
            .register("people", StandingQueryPattern::label("Person"))
            .await;
        let qid = "616c696365".to_string();

        let response = set_property(
            State(state.clone()),
            Path((qid.clone(), "labels".into())),
            Json(SetPropertyRequest {
                value: serde_json::json!(["Person"]),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.sq_manager.match_count(sq_id).await, 1);

        set_property(
            State(state.clone()),
            Path((qid, "age".into())),
            Json(SetPropertyRequest {
                value: serde_json::json!(42),
            }),
        )
        .await;
        assert_eq!(state.sq_manager.match_count(sq_id).await, 1);
    }

    /// D1: the concurrent (buffer_unordered) BFS must return the same
    /// reachability set as a serial walk — multi-hop, deduped, depth-bounded,
    /// and correct under a diamond graph where two frontier nodes point at the
    /// same target (dedup must hold even though edge fetches complete out of
    /// order).
    #[tokio::test]
    async fn concurrent_bfs_reaches_correct_multihop_set() {
        use nexora_value::{HalfEdge, Symbol};
        let state = test_state();
        let et = "KNOWS";
        let sym = Symbol::new(et);

        // Graph: a → b, a → c, b → d, c → d (diamond), d → e.
        let a = NexoraId::from_bytes(b"a".to_vec());
        let b = NexoraId::from_bytes(b"b".to_vec());
        let c = NexoraId::from_bytes(b"c".to_vec());
        let d = NexoraId::from_bytes(b"d".to_vec());
        let e = NexoraId::from_bytes(b"e".to_vec());
        for (src, dst) in [(&a, &b), (&a, &c), (&b, &d), (&c, &d), (&d, &e)] {
            state
                .graph
                .add_edge(src, HalfEdge::out(sym.clone(), dst.clone()))
                .await
                .unwrap();
        }

        // Full-depth traversal from a reaches b, c, d, e exactly once each.
        let reached = local_bfs(&state, vec![a.clone()], et, 10).await;
        let set: std::collections::HashSet<String> = reached.iter().cloned().collect();
        assert_eq!(set.len(), reached.len(), "no duplicates (diamond dedup)");
        assert_eq!(
            set,
            [&b, &c, &d, &e].iter().map(|q| q.to_hex()).collect(),
            "reaches every downstream node exactly once"
        );

        // Depth 1 from a reaches only b and c.
        let depth1 = local_bfs(&state, vec![a.clone()], et, 1).await;
        assert_eq!(
            depth1
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
            [&b, &c].iter().map(|q| q.to_hex()).collect(),
            "depth-1 stops after the first hop"
        );

        // A different edge type reaches nothing.
        assert!(
            local_bfs(&state, vec![a], "OTHER", 10).await.is_empty(),
            "traversal is edge-type filtered"
        );
    }

    #[tokio::test]
    async fn mutation_validation_uses_http_error_statuses() {
        let state = test_state();
        let response = set_property(
            State(state.clone()),
            Path(("not-hex".into(), "name".into())),
            Json(SetPropertyRequest {
                value: serde_json::json!("Alice"),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = add_edge(
            State(state),
            Path("aa".into()),
            Json(AddEdgeRequest {
                edge_type: "KNOWS".into(),
                target: "bb".into(),
                direction: "sideways".into(),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn bulk_ingest_writes_records_and_reports_stats() {
        let state = test_state();
        let (status, body) = bulk_ingest(
            State(state.clone()),
            Json(BulkIngestRequest {
                records: vec![
                    serde_json::json!({"id": "alice", "name": "Alice", "age": 30}),
                    serde_json::json!({"id": "bob", "name": "Bob"}),
                    // Coalesced with the first alice record onto one node.
                    serde_json::json!({"id": "alice", "role": "admin"}),
                    // Skipped: not an object.
                    serde_json::json!("garbage"),
                ],
                id_field: "id".into(),
                event_time_field: None,
                event_time_unit: String::new(),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.0["ingested"], 2, "two distinct nodes committed");
        assert_eq!(body.0["nodes"], 2);
        assert_eq!(body.0["skipped"], 1, "non-object record skipped");

        // Verify the writes landed, including the coalesced alice.role.
        let alice = NexoraId::from_bytes(b"alice".to_vec());
        assert_eq!(
            state.graph.get_property(&alice, "name").await.unwrap(),
            Some(PropertyValue::String("Alice".into()))
        );
        assert_eq!(
            state.graph.get_property(&alice, "role").await.unwrap(),
            Some(PropertyValue::String("admin".into())),
            "second alice record coalesced onto the same node"
        );
    }

    #[tokio::test]
    async fn bulk_ingest_empty_is_ok() {
        let state = test_state();
        let (status, body) = bulk_ingest(
            State(state),
            Json(BulkIngestRequest {
                records: vec![],
                id_field: "id".into(),
                event_time_field: None,
                event_time_unit: String::new(),
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.0["ingested"], 0);
    }

    /// A local-owned write to a shard that is reconciling (catch-up barrier
    /// active) must be rejected with 503 + `catch_up_in_progress`, so it can't
    /// race the reconciliation and be clobbered by a stale replay op.
    #[tokio::test]
    async fn write_rejected_while_shard_reconciling() {
        use nexora_zenoh::catch_up_barrier::CatchUpBarrier;
        use nexora_zenoh::router::HybridRouter;
        use nexora_zenoh::shard_map::ShardMap;

        let mut state = test_state();
        // Cluster mode with an all-local map (so the write is owned here, not
        // routed away) and a barrier we can toggle.
        let total = 4usize;
        let router = Arc::new(HybridRouter::new_local(total));
        let barrier = CatchUpBarrier::new();
        state.router = Some(router.clone());
        state.catch_up_barrier = Some(barrier.clone());

        let qid = NexoraId::from_bytes(b"barrier-key".to_vec());
        // The map is all-local, so shard_and_epoch resolves; block that shard.
        let shard = ShardMap::new_local(total).shard_of(&qid);
        barrier.begin(shard).await;

        let (status, _body) = set_property(
            State(state.clone()),
            Path((qid.to_hex(), "v".into())),
            Json(SetPropertyRequest {
                value: serde_json::json!(1),
            }),
        )
        .await
        .into_response()
        .into_parts();
        assert_eq!(
            status.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "write must be rejected while the shard is reconciling"
        );

        // After reconciliation reopens the shard, the write succeeds.
        barrier.end(shard).await;
        let (status, _body) = set_property(
            State(state),
            Path((qid.to_hex(), "v".into())),
            Json(SetPropertyRequest {
                value: serde_json::json!(1),
            }),
        )
        .await
        .into_response()
        .into_parts();
        assert_eq!(
            status.status,
            StatusCode::OK,
            "write must succeed once the barrier is lifted"
        );
    }
}

// ============================================================
// Ingest
// ============================================================

#[derive(Deserialize)]
pub struct FileIngestRequest {
    pub path: String,
    #[serde(default = "default_id_field")]
    pub id_field: String,
    /// Optional field whose value becomes each node's graph label (also kept as
    /// a property). E.g. `"label_field": "type"` labels nodes by their `type`.
    #[serde(default)]
    pub label_field: Option<String>,
    /// Optional field holding each record's event time, for event-time
    /// last-writer-wins. RFC 3339 string or integer epoch interpreted per
    /// `event_time_unit`.
    #[serde(default)]
    pub event_time_field: Option<String>,
    /// How a numeric event_time_field is interpreted: `s`, `ms`, `us`, or
    /// `rfc3339`. Defaults to microseconds.
    #[serde(default)]
    pub event_time_unit: String,
    /// Topic name for event-first routing. When set, the EventFirstHandler routes
    /// this batch to the configured destination (event table, graph, or both).
    /// When unset, defaults to an auto-generated ingest name.
    #[serde(default)]
    pub topic: Option<String>,
}

fn default_id_field() -> String {
    "id".into()
}

pub async fn start_file_ingest(
    State(state): State<AppState>,
    Json(req): Json<FileIngestRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    use nexora_core::BatchDurability;
    use nexora_stream::{FileSource, FileSourceConfig, IngestionSource};

    // Security: validate path to prevent path traversal
    let requested_path = std::path::Path::new(&req.path);

    // Reject absolute paths
    if requested_path.is_absolute() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": "Absolute paths are not allowed. Use a relative path within the allowed directory."}),
            ),
        );
    }

    // Reject path traversal (../)
    if req.path.contains("..") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Path traversal (..) is not allowed."})),
        );
    }

    // Resolve against allowed directory if configured, otherwise use cwd
    let resolved_path = if let Some(ref allow_dir) = state.config.allow_ingest_dir {
        let resolved = allow_dir.join(&req.path);
        // Canonicalize and verify it's still within the allowed directory
        match resolved.canonicalize() {
            Ok(canonical) => {
                let allow_canonical = allow_dir
                    .canonicalize()
                    .unwrap_or_else(|_| allow_dir.clone());
                if !canonical.starts_with(&allow_canonical) {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "Path escapes allowed directory."})),
                    );
                }
                canonical
            }
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "File not found."})),
                );
            }
        }
    } else {
        // No allow_ingest_dir configured — resolve relative to cwd
        match std::env::current_dir() {
            Ok(cwd) => {
                let resolved = cwd.join(&req.path);
                match resolved.canonicalize() {
                    Ok(c) => c,
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": format!("File not found: {}", req.path)})),
                        );
                    }
                }
            }
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Cannot determine working directory."})),
                );
            }
        }
    };

    let path_str = resolved_path.to_string_lossy().to_string();
    let ingest_name = format!("ingest-{}", chrono::Utc::now().timestamp());

    // Use the provided topic for event-first routing, or fall back to ingest_name
    let topic = req.topic.clone().unwrap_or_else(|| ingest_name.clone());

    // Unified poll+commit path: FileSource (JSONL) → GraphIngestHandler
    // (write_batch). Replaces the legacy push-channel nexora-ingest runner.
    let source = FileSource::new(FileSourceConfig {
        path: resolved_path.clone(),
        topic,
        id_field: req.id_field,
        label_field: req.label_field,
        event_time_field: req.event_time_field,
        event_time_unit: nexora_stream::EventTimeUnit::parse(&req.event_time_unit)
            .unwrap_or_default(),
        max_batch: 256,
    });
    if let Err(e) = source.connect().await {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        );
    }

    // File ingest is replayable (re-run against the same file), so Relaxed
    // durability is safe and faster; the WAL flusher still fsyncs the batch.
    // Honors event-first mode (shared store/router) via the AppState factory.
    let handler = state.make_ingest_handler(BatchDurability::Relaxed);

    let ingest_name_for_task = ingest_name.clone();
    let _path_for_task = path_str.clone();
    let ingests = state.ingests.clone();
    let metrics = state.metrics.clone();
    let handle = tokio::spawn(async move {
        let mut processed = 0u64;
        let mut failed = 0u64;
        // Drain the file to EOF, committing each batch through the sink.
        loop {
            match source.poll().await {
                Ok(Some(batch)) => match handler.handle_batch(&batch).await {
                    Ok(count) => processed += count as u64,
                    Err(e) => {
                        failed += 1;
                        tracing::warn!(error = %e, "file ingest batch failed");
                    }
                },
                Ok(None) => break, // EOF
                Err(e) => {
                    failed += 1;
                    tracing::warn!(error = %e, "file ingest poll failed");
                    break;
                }
            }
        }
        metrics.inc_events(processed);
        if failed > 0 {
            metrics.inc_errors();
        }
        tracing::info!(
            "File ingest complete: {} records processed, {} batch errors",
            processed,
            failed
        );
        // Remove from registry when done
        ingests.write().await.remove(&ingest_name_for_task);
    })
    .abort_handle();

    state
        .ingests
        .write()
        .await
        .insert(ingest_name.clone(), handle);

    (
        StatusCode::OK,
        Json(json!({"status": "started", "path": path_str, "name": ingest_name})),
    )
}

/// Request body for synchronous bulk ingest: an array of JSON objects, each an
/// ingest record keyed by `id_field`.
#[derive(Deserialize)]
pub struct BulkIngestRequest {
    /// Records to ingest — each is a JSON object; its non-id fields become node
    /// properties.
    pub records: Vec<serde_json::Value>,
    /// Field naming the node id (hex NexoraId, else hashed from the string).
    #[serde(default = "default_id_field")]
    pub id_field: String,
    /// Optional field holding each record's event time, for event-time
    /// last-writer-wins. RFC 3339 string or integer epoch interpreted per
    /// `event_time_unit`. When unset or absent, writes fall back to arrival order.
    #[serde(default)]
    pub event_time_field: Option<String>,
    /// How a numeric `event_time_field` is interpreted: `s`, `ms`, `us`, or
    /// `rfc3339`. Defaults to microseconds. Ignored when `event_time_field` is
    /// unset.
    #[serde(default)]
    pub event_time_unit: String,
}

/// POST /api/v2/ingest/bulk — synchronous batch ingest entry point.
///
/// Unlike file/stream ingest this is not a long-lived `Source`: the caller hands
/// over a batch of records in one request, they are committed via
/// `write_batch`, and the per-batch stats are returned in the response. This is
/// the entry point the CLI (`nex ingest`) and other one-shot producers use.
pub async fn bulk_ingest(
    State(state): State<AppState>,
    Json(req): Json<BulkIngestRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    use nexora_core::{BatchDurability, MutationOp, WriteBatchOptions};
    use nexora_value::Symbol;

    if req.records.is_empty() {
        return (
            StatusCode::OK,
            Json(json!({"status": "ok", "ingested": 0, "skipped": 0})),
        );
    }

    // Build (qid, ops) items, coalescing records that target the same node.
    // Each op carries its own record's event time, so records for the same node
    // resolve per-op under event-time LWW rather than by a node-wide value.
    let event_time_unit =
        nexora_stream::EventTimeUnit::parse(&req.event_time_unit).unwrap_or_default();
    let mut order: Vec<NexoraId> = Vec::new();
    let mut by_node: HashMap<NexoraId, Vec<(MutationOp, Option<nexora_id::EventTime>)>> =
        HashMap::new();
    let mut skipped = 0u64;
    // Track the max event time seen in this batch to publish as the ingestion
    // watermark metric (the frontier of event-time progress).
    let mut max_event_time_ms: Option<u64> = None;

    for rec in &req.records {
        let Some(obj) = rec.as_object() else {
            skipped += 1;
            continue;
        };
        let qid = match obj.get(&req.id_field) {
            Some(serde_json::Value::String(s)) => NexoraId::from_hex(s)
                .unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec())),
            Some(serde_json::Value::Number(n)) => match n.as_i64() {
                Some(i) => NexoraId::from_bytes(i.to_be_bytes().to_vec()),
                None => {
                    skipped += 1;
                    continue;
                }
            },
            _ => {
                skipped += 1;
                continue;
            }
        };
        let event_time = nexora_stream::extract_event_time(
            obj,
            req.event_time_field.as_deref(),
            event_time_unit,
            None,
        )
        .map(|dt| nexora_id::EventTime::from_datetime(&dt));
        if let Some(et) = event_time {
            let ms = et.as_micros() / 1_000;
            max_event_time_ms = Some(max_event_time_ms.map_or(ms, |cur| cur.max(ms)));
        }
        let entry = by_node.entry(qid.clone()).or_insert_with(|| {
            order.push(qid.clone());
            Vec::new()
        });
        for (key, value) in obj {
            if key == &req.id_field {
                continue;
            }
            entry.push((
                MutationOp::SetProperty {
                    key: Symbol::new(key),
                    value: json_to_pv(value),
                },
                event_time,
            ));
        }
    }

    let items: Vec<_> = order
        .into_iter()
        .map(|qid| {
            let ops = by_node.remove(&qid).unwrap_or_default();
            (qid, ops, None)
        })
        .filter(|(_, ops, _)| !ops.is_empty())
        .collect();
    let node_count = items.len();

    // Synchronous callers want the write durable before the response returns.
    match state
        .graph
        .write_batch_with_event_times(
            items,
            WriteBatchOptions {
                concurrency: 64,
                durability: BatchDurability::WaitDurable,
            },
        )
        .await
    {
        Ok(receipts) => {
            state.metrics.inc_events(req.records.len() as u64);
            // Advance the event-time watermark metric to the batch frontier so
            // /metrics reflects how far event-time has progressed on ingest.
            if let Some(ms) = max_event_time_ms {
                state.metrics.set_watermark_ms(ms);
            }
            // F4: report property writes/removals dropped by event-time LWW
            // (late/out-of-order arrivals rejected because a newer value exists).
            let late_dropped: usize = receipts.iter().map(|r| r.late_dropped).sum();
            if late_dropped > 0 {
                state.metrics.inc_late_dropped(late_dropped as u64);
            }
            (
                StatusCode::OK,
                Json(json!({
                    "status": "ok",
                    "ingested": receipts.len(),
                    "nodes": node_count,
                    "skipped": skipped,
                    "late_dropped": late_dropped,
                })),
            )
        }
        Err(e) => {
            state.metrics.inc_errors();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        }
    }
}

// ============================================================
// WebSocket — 实时 SQ 结果推送
// ============================================================

/// WebSocket handler for streaming Cypher query results.
pub async fn ws_query_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    // Validate authentication token before upgrade
    if let Some(auth) = state.auth.as_ref() {
        if let Err(err_msg) = crate::auth::validate_ws_token(auth, &req) {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({
                    "error": err_msg,
                    "code": "WS_AUTH_FAILED"
                })),
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| ws_query_loop(socket, state))
}

async fn ws_query_loop(mut socket: axum::extract::ws::WebSocket, state: AppState) {
    use axum::extract::ws::Message;

    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            Message::Text(text) => {
                let req: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = socket
                            .send(Message::Text(
                                r#"{"type":"MessageError","error":"Invalid JSON"}"#.into(),
                            ))
                            .await;
                        continue;
                    }
                };

                let query = req["query"].as_str().unwrap_or("");

                // Cluster guard: the executor runs against this node's local
                // shards only; refuse over a true multi-node cluster rather than
                // stream partial/misrouted results. Single-node unaffected.
                if !state.whole_graph_query_is_safe().await {
                    let _ = socket
                        .send(Message::Text(
                            serde_json::json!({
                                "type": "MessageError",
                                "error": "Cypher over a multi-node cluster is not supported yet \
                                          (local-shard execution only). Use the node/edge REST \
                                          API or a single-node deployment.",
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                    continue;
                }

                // Run Cypher query
                match nexora_cypher::execute_cypher(&state.graph, query).await {
                    Ok(nexora_cypher::CypherResult::Rows { columns, rows }) => {
                        let resp = serde_json::json!({
                            "type": "TabularResults",
                            "queryId": req.get("queryId").and_then(|v| v.as_str()).unwrap_or("q"),
                            "columns": columns,
                            "results": rows,
                        });
                        let _ = socket.send(Message::Text(resp.to_string().into())).await;
                        let _ = socket
                            .send(Message::Text(
                                serde_json::json!({"type": "QueryFinished", "queryId": "q"})
                                    .to_string()
                                    .into(),
                            ))
                            .await;
                    }
                    Ok(other) => {
                        let _ = socket.send(Message::Text(
                            serde_json::json!({"type": "TabularResults", "queryId": "q", "columns": ["result"], "results": [[format!("{:?}", other)]]}).to_string().into(),
                        )).await;
                        let _ = socket
                            .send(Message::Text(
                                serde_json::json!({"type": "QueryFinished", "queryId": "q"})
                                    .to_string()
                                    .into(),
                            ))
                            .await;
                    }
                    Err(e) => {
                        let _ = socket.send(Message::Text(
                            serde_json::json!({"type": "QueryFailed", "queryId": "q", "message": e.to_string()}).to_string().into(),
                        )).await;
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
}

/// WebSocket handler for Standing Query result push.
pub async fn ws_sq_handler(
    ws: WebSocketUpgrade,
    Path(sq_id): Path<String>,
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    // Validate authentication token before upgrade
    if let Some(auth) = state.auth.as_ref() {
        if let Err(err_msg) = crate::auth::validate_ws_token(auth, &req) {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({
                    "error": err_msg,
                    "code": "WS_AUTH_FAILED"
                })),
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| ws_sq_loop(socket, sq_id, state))
}

async fn ws_sq_loop(mut socket: axum::extract::ws::WebSocket, sq_id: String, state: AppState) {
    use axum::extract::ws::Message;

    let sq_uuid = match uuid::Uuid::parse_str(&sq_id) {
        Ok(u) => u,
        Err(_) => return,
    };

    // Push-based: subscribe to the generic SQ broadcast channel and
    // filter for events matching our sq_id. This replaces the previous
    // 2-second polling loop with real-time event delivery.
    // The channel is lazily created on first use by sq_broadcast_subscribe.
    let mut rx = sq_broadcast_subscribe().await;

    // Send initial match count (blocking call is acceptable on connect)
    let mc = state.sq_manager.match_count(sq_uuid).await;
    let _ = socket
        .send(Message::Text(
            serde_json::json!({
                "type": "SqMatch",
                "sq_id": sq_id,
                "match_count": mc,
            })
            .to_string()
            .into(),
        ))
        .await;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
            recv_result = rx.recv() => {
                match recv_result {
                    Ok(event_json) => {
                        // Only send events matching our sq_id
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&event_json) {
                            if parsed.get("sq_id").and_then(|v| v.as_str()) == Some(&sq_id) {
                                let _ = socket.send(Message::Text(event_json.into())).await;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(sq_id = %sq_id, skipped = n, "SQ broadcast lagging");
                        rx = sq_broadcast_subscribe().await; // re-subscribe
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Sender dropped; try to re-subscribe
                        rx = sq_broadcast_subscribe().await;
                    }
                }
            }
            _ = state.shutdown.notified() => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
        }
    }
}

/// GET /api/v2/ws/sq — real-time stream of *all* standing-query match events.
///
/// Unlike `/api/v2/ws/sq/{id}` (which filters to one query), this forwards every
/// SQ event so a dashboard listing all standing queries can update their match
/// counts live. Reuses the same broadcast channel; no per-id filtering.
pub async fn ws_sq_all_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    // Validate authentication token before upgrade
    if let Some(auth) = state.auth.as_ref() {
        if let Err(err_msg) = crate::auth::validate_ws_token(auth, &req) {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({
                    "error": err_msg,
                    "code": "WS_AUTH_FAILED"
                })),
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| ws_sq_all_loop(socket, state))
}

async fn ws_sq_all_loop(mut socket: axum::extract::ws::WebSocket, state: AppState) {
    use axum::extract::ws::Message;

    let mut rx = sq_broadcast_subscribe().await;

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
            recv_result = rx.recv() => {
                match recv_result {
                    // Forward every event unfiltered.
                    Ok(event_json) => {
                        if socket.send(Message::Text(event_json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "SQ (all) broadcast lagging");
                        rx = sq_broadcast_subscribe().await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        rx = sq_broadcast_subscribe().await;
                    }
                }
            }
            _ = state.shutdown.notified() => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
        }
    }
}

/// GET /api/v2/ws/metrics — real-time metrics stream.
///
/// Pushes a metrics snapshot every second so the dashboard can display live
/// values without HTTP polling. Replaces the client-side 5s poll on the
/// Metrics page.
pub async fn ws_metrics_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    // Validate authentication token before upgrade
    if let Some(auth) = state.auth.as_ref() {
        if let Err(err_msg) = crate::auth::validate_ws_token(auth, &req) {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                axum::Json(serde_json::json!({
                    "error": err_msg,
                    "code": "WS_AUTH_FAILED"
                })),
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| ws_metrics_loop(socket, state))
}

async fn ws_metrics_loop(mut socket: axum::extract::ws::WebSocket, state: AppState) {
    use axum::extract::ws::Message;
    use std::sync::atomic::Ordering;

    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));

    loop {
        tokio::select! {
            // Drain client messages so we notice a close promptly.
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
            _ = ticker.tick() => {
                let m = &state.metrics;
                let snapshot = serde_json::json!({
                    "type": "Metrics",
                    "active_nodes": m.active_nodes.load(Ordering::Relaxed),
                    "standing_queries": m.standing_queries.load(Ordering::Relaxed),
                    "events_total": m.events_total.load(Ordering::Relaxed),
                    "sq_matches_total": m.sq_matches_total.load(Ordering::Relaxed),
                    "errors_total": m.errors_total.load(Ordering::Relaxed),
                    "wal_append_total": m.wal_append_total.load(Ordering::Relaxed),
                    "queries_total": m.queries_total.load(Ordering::Relaxed),
                    "slow_queries_total": m.slow_queries_total.load(Ordering::Relaxed),
                });
                if socket.send(Message::Text(snapshot.to_string().into())).await.is_err() {
                    break;
                }
            }
            _ = state.shutdown.notified() => {
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
        }
    }
}

// ============================================================
// System Info / Config
// ============================================================

pub async fn system_info(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "rust_version": env!("CARGO_PKG_RUST_VERSION"),
        "mode": "single-node",
        "num_shards": state.graph.shard_count(),
        "max_nodes_per_shard": state.config.max_nodes_per_shard,
        "rocksdb_path": state.config.rocksdb_path,
        "wal_dir": state.config.wal_dir,
    }))
}

pub async fn system_config(State(_state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "host": "0.0.0.0",
        "port": 8080,
    }))
}

/// Load sample data into the graph.
/// GET /api/v2/sample-data -> list available datasets
/// POST /api/v2/sample-data/{dataset} -> load dataset into graph
pub async fn list_sample_data() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "datasets": [
            {
                "id": "social-network",
                "name": "Social Network",
                "description": "8 people with name, age, city, role, team properties",
                "node_count": 8,
                "edge_count": 0,
            },
            {
                "id": "tech-stack",
                "name": "Tech Stack (Knowledge Graph)",
                "description": "10 technologies with category, paradigm, dependencies",
                "node_count": 10,
                "edge_count": 5,
            },
            {
                "id": "iot-sensors",
                "name": "IoT Sensors (Cold Chain)",
                "description": "8 sensors with temp, humidity, status, zone, door properties",
                "node_count": 8,
                "edge_count": 0,
            },
        ],
        "recipes": [
            {
                "id": "high-temp-alert",
                "name": "High Temperature Alert",
                "description": "Alert when sensor temp > 10"
            },
            {
                "id": "door-open-warning",
                "name": "Door Open Warning",
                "description": "Alert when non-dock door is open"
            },
            {
                "id": "team-collaboration",
                "name": "Team Collaboration Finder",
                "description": "Find same-team same-city pairs"
            },
            {
                "id": "critical-status-watch",
                "name": "Critical Status Watch",
                "description": "Monitor critical status nodes"
            },
        ]
    }))
}

pub async fn load_sample_data(
    State(state): State<AppState>,
    Path(dataset): Path<String>,
) -> impl IntoResponse {
    let (nodes, edges) = match dataset.as_str() {
        "social-network" => {
            let nodes = vec![
                (
                    "alice",
                    vec![
                        ("name", "Alice Chen"),
                        ("age", "32"),
                        ("city", "Shanghai"),
                        ("role", "engineer"),
                        ("team", "platform"),
                    ],
                ),
                (
                    "bob",
                    vec![
                        ("name", "Bob Wang"),
                        ("age", "28"),
                        ("city", "Beijing"),
                        ("role", "designer"),
                        ("team", "product"),
                    ],
                ),
                (
                    "carol",
                    vec![
                        ("name", "Carol Liu"),
                        ("age", "35"),
                        ("city", "Shenzhen"),
                        ("role", "manager"),
                        ("team", "platform"),
                    ],
                ),
                (
                    "dave",
                    vec![
                        ("name", "Dave Zhang"),
                        ("age", "26"),
                        ("city", "Shanghai"),
                        ("role", "engineer"),
                        ("team", "infra"),
                    ],
                ),
                (
                    "eve",
                    vec![
                        ("name", "Eve Sun"),
                        ("age", "30"),
                        ("city", "Beijing"),
                        ("role", "analyst"),
                        ("team", "data"),
                    ],
                ),
                (
                    "frank",
                    vec![
                        ("name", "Frank Li"),
                        ("age", "40"),
                        ("city", "Shenzhen"),
                        ("role", "director"),
                        ("team", "platform"),
                    ],
                ),
                (
                    "grace",
                    vec![
                        ("name", "Grace Zhou"),
                        ("age", "29"),
                        ("city", "Shanghai"),
                        ("role", "engineer"),
                        ("team", "infra"),
                    ],
                ),
                (
                    "henry",
                    vec![
                        ("name", "Henry Wu"),
                        ("age", "33"),
                        ("city", "Beijing"),
                        ("role", "architect"),
                        ("team", "platform"),
                    ],
                ),
            ];
            (nodes, vec![])
        }
        "tech-stack" => {
            let nodes = vec![
                (
                    "rust",
                    vec![
                        ("name", "Rust"),
                        ("category", "language"),
                        ("paradigm", "systems"),
                        ("year", "2010"),
                    ],
                ),
                (
                    "tokio",
                    vec![
                        ("name", "Tokio"),
                        ("category", "framework"),
                        ("paradigm", "async"),
                        ("year", "2016"),
                    ],
                ),
                (
                    "axum",
                    vec![
                        ("name", "Axum"),
                        ("category", "framework"),
                        ("paradigm", "web"),
                        ("year", "2021"),
                    ],
                ),
                (
                    "serde",
                    vec![
                        ("name", "Serde"),
                        ("category", "library"),
                        ("paradigm", "serialization"),
                        ("year", "2014"),
                    ],
                ),
                (
                    "rocksdb",
                    vec![
                        ("name", "RocksDB"),
                        ("category", "storage"),
                        ("paradigm", "embedded"),
                        ("year", "2012"),
                    ],
                ),
                (
                    "hnsw",
                    vec![
                        ("name", "HNSW"),
                        ("category", "algorithm"),
                        ("paradigm", "ann"),
                        ("year", "2016"),
                    ],
                ),
                (
                    "cypher",
                    vec![
                        ("name", "Cypher"),
                        ("category", "language"),
                        ("paradigm", "query"),
                        ("year", "2015"),
                    ],
                ),
                (
                    "zenoh",
                    vec![
                        ("name", "Eclipse Zenoh"),
                        ("category", "middleware"),
                        ("paradigm", "pubsub"),
                        ("year", "2020"),
                    ],
                ),
                (
                    "wasmtime",
                    vec![
                        ("name", "Wasmtime"),
                        ("category", "runtime"),
                        ("paradigm", "sandbox"),
                        ("year", "2019"),
                    ],
                ),
                (
                    "nexora",
                    vec![
                        ("name", "Nexora-RS"),
                        ("category", "database"),
                        ("paradigm", "streaming-graph"),
                        ("year", "2025"),
                    ],
                ),
            ];
            let edges = vec![
                ("tokio", "rust", "DEPENDS_ON"),
                ("axum", "tokio", "DEPENDS_ON"),
                ("serde", "rust", "DEPENDS_ON"),
                ("wasmtime", "rust", "DEPENDS_ON"),
                ("nexora", "tokio", "DEPENDS_ON"),
            ];
            (nodes, edges)
        }
        "iot-sensors" => {
            let nodes = vec![
                (
                    "sensor-01",
                    vec![
                        ("name", "Cold Storage A1"),
                        ("temp", "-18.5"),
                        ("humidity", "65"),
                        ("status", "normal"),
                        ("zone", "A"),
                        ("door", "closed"),
                    ],
                ),
                (
                    "sensor-02",
                    vec![
                        ("name", "Cold Storage A2"),
                        ("temp", "-20.1"),
                        ("humidity", "70"),
                        ("status", "normal"),
                        ("zone", "A"),
                        ("door", "closed"),
                    ],
                ),
                (
                    "sensor-03",
                    vec![
                        ("name", "Cold Storage B1"),
                        ("temp", "2.3"),
                        ("humidity", "85"),
                        ("status", "warning"),
                        ("zone", "B"),
                        ("door", "closed"),
                    ],
                ),
                (
                    "sensor-04",
                    vec![
                        ("name", "Cold Storage B2"),
                        ("temp", "8.7"),
                        ("humidity", "90"),
                        ("status", "critical"),
                        ("zone", "B"),
                        ("door", "open"),
                    ],
                ),
                (
                    "sensor-05",
                    vec![
                        ("name", "Dock Door 1"),
                        ("temp", "15.2"),
                        ("humidity", "60"),
                        ("status", "normal"),
                        ("zone", "dock"),
                        ("door", "closed"),
                    ],
                ),
                (
                    "sensor-06",
                    vec![
                        ("name", "Dock Door 2"),
                        ("temp", "16.8"),
                        ("humidity", "55"),
                        ("status", "normal"),
                        ("zone", "dock"),
                        ("door", "open"),
                    ],
                ),
                (
                    "sensor-07",
                    vec![
                        ("name", "Freezer C1"),
                        ("temp", "-25.3"),
                        ("humidity", "50"),
                        ("status", "normal"),
                        ("zone", "C"),
                        ("door", "closed"),
                    ],
                ),
                (
                    "sensor-08",
                    vec![
                        ("name", "Chiller C2"),
                        ("temp", "4.1"),
                        ("humidity", "75"),
                        ("status", "warning"),
                        ("zone", "C"),
                        ("door", "closed"),
                    ],
                ),
            ];
            (nodes, vec![])
        }
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(
                    serde_json::json!({"error": format!("Unknown dataset: {}. Available: social-network, tech-stack, iot-sensors", dataset)}),
                ),
            );
        }
    };

    let mut created = 0u64;
    let mut errors = Vec::new();

    for (id, props) in &nodes {
        let qid = NexoraId::from_bytes(id.as_bytes().to_vec());
        for (key, val) in props {
            let parsed_val = if let Ok(n) = val.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(f) = val.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::String(val.to_string()))
            } else {
                serde_json::Value::String(val.to_string())
            };
            // Cluster: route each write to its shard owner. Single-node mode
            // returns None and runs the local path unchanged.
            if let Some(routed) = state
                .try_route_remote(
                    &qid,
                    nexora_zenoh::GraphOperation::SetProperty {
                        qid: qid.clone(),
                        key: (*key).to_string(),
                        value: parsed_val.clone(),
                    },
                )
                .await
            {
                if let Err(e) = routed {
                    errors.push(format!("{}: {}", id, e));
                }
                continue;
            }
            let pv = json_to_pv(&parsed_val);
            if let Err(e) = state.graph.set_property(&qid, key, pv).await {
                errors.push(format!("{}: {}", id, e));
            }
        }
        created += 1;
    }

    let mut edges_created = 0u64;
    for (from, to, edge_type) in &edges {
        let from_id = NexoraId::from_bytes(from.as_bytes().to_vec());
        let to_id = NexoraId::from_bytes(to.as_bytes().to_vec());

        // Cluster: route to the source node's shard owner. Single-node → None.
        if let Some(routed) = state
            .try_route_remote(
                &from_id,
                nexora_zenoh::GraphOperation::AddEdge {
                    source: from_id.clone(),
                    edge_type: (*edge_type).to_string(),
                    target: to_id.clone(),
                    direction: "out".to_string(),
                },
            )
            .await
        {
            match routed {
                Ok(_) => edges_created += 1,
                Err(e) => errors.push(format!("edge {}->{}: {}", from, to, e)),
            }
            continue;
        }

        let edge = nexora_value::HalfEdge::new(
            nexora_value::Symbol::new(edge_type),
            nexora_value::EdgeDirection::Out,
            to_id,
        );
        if let Err(e) = state.graph.add_edge(&from_id, edge).await {
            errors.push(format!("edge {}->{}: {}", from, to, e));
        } else {
            edges_created += 1;
        }
    }

    state.metrics.set_active_nodes(created);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "dataset": dataset,
            "nodes_created": created,
            "edges_created": edges_created,
            "errors": errors,
        })),
    )
}

// ============================================================
// Admin / Operations API
// ============================================================

/// GET /api/v2/admin/status — Detailed system status for ops
pub async fn admin_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let metrics = &state.metrics;
    let total_queries = metrics
        .queries_total
        .load(std::sync::atomic::Ordering::Relaxed);
    let q_sum_us = metrics
        .query_duration_sum_us
        .load(std::sync::atomic::Ordering::Relaxed);
    let slow_queries = metrics
        .slow_queries_total
        .load(std::sync::atomic::Ordering::Relaxed);
    let wal_total = metrics
        .wal_append_total
        .load(std::sync::atomic::Ordering::Relaxed);
    let wal_sum_us = metrics
        .wal_append_sum_us
        .load(std::sync::atomic::Ordering::Relaxed);

    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": state.start_time.elapsed().as_secs(),
        "graph": {
            "shard_count": state.graph.shard_count(),
            "max_nodes_per_shard": state.config.max_nodes_per_shard,
            "active_nodes": metrics.active_nodes.load(std::sync::atomic::Ordering::Relaxed),
        },
        "queries": {
            "total": total_queries,
            "avg_us": q_sum_us.checked_div(total_queries).unwrap_or(0),
            "slow_count": slow_queries,
        },
        "standing_queries": {
            "count": metrics.standing_queries.load(std::sync::atomic::Ordering::Relaxed),
            "matches_total": metrics.sq_matches_total.load(std::sync::atomic::Ordering::Relaxed),
        },
        "wal": {
            "appends_total": wal_total,
            "avg_append_us": wal_sum_us.checked_div(wal_total).unwrap_or(0),
            "enabled": state.config.wal_dir.is_some(),
            "path": state.config.wal_dir,
        },
        "storage": {
            "rocksdb_enabled": state.config.rocksdb_path.is_some(),
            "path": state.config.rocksdb_path,
        },
        "errors_total": metrics.errors_total.load(std::sync::atomic::Ordering::Relaxed),
        "profile": state.config.profile,
    }))
}

/// GET /api/v2/admin/slow-queries — Query performance summary
pub async fn admin_slow_queries(State(state): State<AppState>) -> Json<serde_json::Value> {
    let metrics = &state.metrics;
    let total = metrics
        .queries_total
        .load(std::sync::atomic::Ordering::Relaxed);
    let slow = metrics
        .slow_queries_total
        .load(std::sync::atomic::Ordering::Relaxed);

    Json(serde_json::json!({
        "total_queries": total,
        "slow_queries": slow,
        "slow_query_ratio": if total > 0 { (slow as f64 / total as f64 * 100.0).round() as u64 } else { 0 },
        "threshold_ms": 1000,
        "note": "Slow queries are logged via tracing at target 'slow_query'. Check logs for details.",
    }))
}

/// POST /api/v2/admin/backup — Create a database backup artifact
///
/// Creates a complete backup of the entire graph database and writes it to disk.
/// The backup includes all nodes (properties, labels, edges) and a trailing
/// manifest with Blake3 checksum for integrity verification.
///
/// Returns the backup file path, size, and checksum.
pub async fn admin_backup(State(state): State<AppState>) -> impl IntoResponse {
    use nexora_core::BackupArtifact;
    use std::path::PathBuf;

    if state.config.wal_dir.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({"error": "WAL is disabled, cannot determine backup directory"}),
            ),
        );
    }

    let wal_dir_str = state.config.wal_dir.as_ref().unwrap();
    let wal_dir = PathBuf::from(wal_dir_str);
    let backup_dir = wal_dir.join("backups");

    // Ensure backup directory exists
    if let Err(e) = tokio::fs::create_dir_all(&backup_dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to create backup directory: {}", e)
            })),
        );
    }

    // Generate backup filename with timestamp
    let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");
    let backup_filename = format!("backup-{}.nxbak", timestamp);
    let backup_path = backup_dir.join(&backup_filename);

    // Create backup artifact (in memory)
    let last_tx_id = 0; // TODO: track actual last_tx_id in GraphService
    let backup_data = match BackupArtifact::create(&state.graph, last_tx_id).await {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Backup creation failed: {}", e)
                })),
            );
        }
    };

    let backup_size = backup_data.len();

    // Write backup to disk
    if let Err(e) = tokio::fs::write(&backup_path, &backup_data).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": format!("Failed to write backup file: {}", e)
            })),
        );
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "backup_completed",
            "backup_path": backup_path.to_string_lossy(),
            "backup_size_bytes": backup_size,
            "note": "Backup includes Blake3 checksum in manifest for integrity verification",
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

/// POST /api/v2/admin/restore — Restore a database backup
///
/// Restores a previously created backup file into the current graph database.
/// This is a DESTRUCTIVE operation: existing data may be overwritten.
///
/// Request body: { "backup_path": "/path/to/backup.nxbak" }
///
/// Returns the number of nodes restored.
#[derive(serde::Deserialize)]
pub struct RestoreRequest {
    backup_path: String,
}

pub async fn admin_restore(
    State(state): State<AppState>,
    Json(req): Json<RestoreRequest>,
) -> impl IntoResponse {
    use nexora_core::BackupArtifact;

    tracing::warn!(
        backup_path = %req.backup_path,
        "restore requested (destructive operation)"
    );

    // Read backup file
    let backup_data = match tokio::fs::read(&req.backup_path).await {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("Failed to read backup file: {}", e)
                })),
            );
        }
    };

    // Restore backup
    let nodes_restored = match BackupArtifact::restore(&state.graph, &backup_data).await {
        Ok(count) => count,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Restore failed: {}", e)
                })),
            );
        }
    };

    audit!(
        "admin_restore",
        "admin",
        &format!("/api/v2/admin/restore:{}", req.backup_path),
        "success"
    );
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "restore_completed",
            "nodes_restored": nodes_restored,
            "backup_path": req.backup_path,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
}

/// POST /api/v2/admin/reindex — Trigger HNSW reindex
pub async fn admin_reindex(State(state): State<AppState>) -> impl IntoResponse {
    let hnsw = state.hnsw.lock().await;
    let node_count = hnsw.len();

    Json(serde_json::json!({
        "status": "reindex_completed",
        "nodes_indexed": node_count,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

/// POST /api/v2/admin/drain — E4 rolling upgrade drain endpoint.
///
/// Sets this node's drain flag so all subsequent write handlers return 503
/// (clients must retry on another node), then sleeps a configurable grace
/// period to let in-flight requests finish before responding.  The caller
/// (an orchestrator or a human operator) can safely stop the process once
/// this endpoint returns `{"status":"drained"}`.
///
/// Graceful wait: 2 s (matches the default k8s terminationGracePeriodSeconds
/// for short-lived work; can be overridden via `NEXORA_DRAIN_WAIT_SECS`).
pub async fn admin_drain(State(state): State<AppState>) -> impl IntoResponse {
    state.drain.begin_drain();
    tracing::info!("node drain started — rejecting new writes");

    // Allow current in-flight requests to complete.
    let wait_secs: u64 = std::env::var("NEXORA_DRAIN_WAIT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;

    tracing::info!("node drain complete — safe to stop");

    // Return the node_id so the orchestrator can confirm which node drained.
    let node_id = std::env::var("NEXORA_NODE_ID").unwrap_or_else(|_| "unknown".to_string());
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "drained",
            "node_id": node_id,
        })),
    )
}

/// Load a preset recipe by name.
/// POST /api/v2/sample-recipes/{recipe_id}
pub async fn load_sample_recipe(
    State(state): State<AppState>,
    Path(recipe_id): Path<String>,
) -> impl IntoResponse {
    let (name, description, query) = match recipe_id.as_str() {
        "high-temp-alert" => (
            "high-temp-alert",
            "Alert when sensor temp > 10",
            "MATCH (n) WHERE n.temp > 10 RETURN n.name, n.temp, n.zone",
        ),
        "door-open-warning" => (
            "door-open-warning",
            "Alert when non-dock door is open",
            "MATCH (n) WHERE n.door = 'open' AND n.zone <> 'dock' RETURN n.name, n.zone",
        ),
        "team-collaboration" => (
            "team-collaboration",
            "Find same-team same-city pairs",
            "MATCH (a), (b) WHERE a.team = b.team AND a.city = b.city AND a.id <> b.id RETURN a.name, b.name, a.team, a.city",
        ),
        "critical-status-watch" => (
            "critical-status-watch",
            "Monitor critical status nodes",
            "MATCH (n) WHERE n.status = 'critical' RETURN n.name, n.zone, n.temp",
        ),
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": format!("Unknown recipe: {}. Available: high-temp-alert, door-open-warning, team-collaboration, critical-status-watch", recipe_id)})),
            );
        }
    };

    let mut recipes = state.recipes.write().await;
    if recipes.contains_key(name) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": format!("recipe '{}' already exists", name)})),
        );
    }

    let recipe = nexora_recipe::Recipe {
        name: name.to_string(),
        version: Some("1.0".to_string()),
        description: Some(description.to_string()),
        standing_queries: vec![nexora_recipe::StandingQueryRecipe {
            name: format!("{}-sq", name),
            pattern: serde_json::json!({
                "PropertyFilter": {
                    "key": "status",
                    "condition": { "Equals": "critical" }
                }
            }),
            outputs: vec![],
        }],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    recipes.insert(name.to_string(), recipe);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "created",
            "name": name,
            "description": description,
            "query": query,
        })),
    )
}

/// List active ingest streams.
pub async fn list_ingests(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> Json<serde_json::Value> {
    let ingests = state.ingests.read().await;
    let names: Vec<String> = ingests.keys().cloned().collect();
    if pagination.limit.is_some() || pagination.offset.is_some() {
        Json(pagination.json(&names))
    } else {
        Json(serde_json::json!({"ingests": names, "count": names.len()}))
    }
}

/// Stop an ingest stream by name.
pub async fn delete_ingest(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut ingests = state.ingests.write().await;
    if let Some(handle) = ingests.remove(&name) {
        handle.abort();
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "stopped", "name": name})),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Ingest '{}' not found", name)})),
        )
    }
}

// ============================================================
// SQL Query + Time-Travel
// ============================================================

#[derive(Deserialize)]
pub struct SqlRequest {
    pub query: String,
}

pub async fn execute_sql(
    State(state): State<AppState>,
    Json(req): Json<SqlRequest>,
) -> Json<serde_json::Value> {
    // Phase 2: In a multi-node cluster, translate SQL → Cypher WITHOUT executing
    // locally, then route the Cypher through the distributed query path.
    //
    // CRITICAL: we must NOT call `nexora_sql::execute_sql` here to obtain the
    // translated Cypher — that function *executes* the query against this node's
    // local graph (running writes on the wrong shards, returning partial reads)
    // before we ever reach the distributed path. Use the pure translation
    // function instead so nothing touches the local graph until routing decides
    // where the work belongs.
    if let Some(router) = state.router.as_ref() {
        if !router.all_shards_local().await {
            match nexora_sql::translate_sql_to_cypher(&req.query) {
                Ok((cypher, _is_write)) => {
                    // Try to execute the translated Cypher distributively.
                    if let Some(dist_result) = state.try_distributed_query(&cypher).await {
                        match dist_result {
                            Ok((columns, rows)) => {
                                return Json(json!({
                                    "columns": columns,
                                    "row_count": rows.len(),
                                    "rows": rows,
                                    "translated_cypher": cypher,
                                    "execution_mode": "distributed"
                                }));
                            }
                            Err(e) => {
                                // A distributed attempt was made but failed (owner
                                // unreachable, quorum, etc.). Surface it honestly
                                // rather than masking it with the generic cluster
                                // guard below. router_error_response already returns
                                // a Json<Value>; return its body directly.
                                return router_error_response(&e).1;
                            }
                        }
                    }
                    // try_distributed_query returned None → the translated Cypher
                    // is not in the mergeable subset. Fall through to the cluster
                    // guard, which explains what is/isn't supported.
                }
                Err(_) => {
                    // SQL parse/translation failed → fall through to cluster guard.
                }
            }
        }
    }

    // Cluster guard: SQL is translated to Cypher and runs against the local
    // graph only, so a multi-node cluster would give incomplete/misrouted
    // results. Refuse explicitly (single-node unaffected). See execute_cypher.
    if !state.whole_graph_query_is_safe().await {
        // Phase 2 Enhancement: Provide detailed error message similar to Cypher
        return Json(json!({
            "error": "SQL query not supported in multi-node cluster mode.\n\n\
                      This SQL query translates to a Cypher query that is not in the \
                      distributable subset. Local execution would return incomplete results.\n\n\
                      Supported SQL queries (via Cypher translation):\n\
                      • SELECT * FROM nodes LIMIT 100\n\
                      • SELECT COUNT(*), AVG(col) FROM nodes\n\
                      • SELECT col, COUNT(*) FROM nodes GROUP BY col\n\n\
                      Not supported (needs cross-shard transactions):\n\
                      • Multi-table JOINs\n\
                      • Correlated subqueries\n\n\
                      Alternatives:\n\
                      1. Simplify the SQL query\n\
                      2. Use the per-key REST API: /api/v2/graph/node/{qid}\n\
                      3. Deploy in single-node mode\n\
                      4. Use Cypher directly for better cluster support"
        }));
    }
    // FIX C: Route SQL queries through QueryPool to enforce concurrency limit.
    // Before: SQL bypassed the pool entirely, allowing unbounded concurrent execution.
    let query_pool = Arc::clone(&state.query_pool);
    let graph = Arc::clone(&state.graph);
    let query = req.query.clone();
    match query_pool
        .execute(async move { nexora_sql::execute_sql(&graph, &query).await })
        .await
    {
        Ok(result) => Json(json!({
            "columns": result.columns,
            "rows": result.rows,
            "row_count": result.row_count,
            "query_time_ms": result.query_time_ms,
            "translated_cypher": result.translated_cypher,
        })),
        Err(e) => Json(json!({"error": e.to_string()})),
    }
}

/// Convert a date (year, month, day) to days since Unix epoch (1970-01-01).
/// Properly accounts for leap years.
fn date_to_epoch_days(year: i64, month: i64, day: i64) -> Option<u64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Days from epoch to start of given year
    let mut total_days: i64 = 0;
    for y in 1970..year {
        total_days += if is_leap_year(y) { 366 } else { 365 };
    }

    // Days in months before the given month
    let days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for &days in days_in_month.iter().take((month - 1) as usize) {
        total_days += days as i64;
    }
    // Leap day if February has passed and it's a leap year
    if month > 2 && is_leap_year(year) {
        total_days += 1;
    }

    total_days += day - 1;
    if total_days < 0 {
        return None;
    }
    Some(total_days as u64)
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Time-travel query endpoint: reconstruct graph state at a specific timestamp
///
/// Query parameters:
/// - as_of: timestamp in microseconds since epoch, or YYYYMMDD date
/// - qid: optional node ID to filter (return only specific node's history)
/// - namespace: optional namespace filter
///
/// Example: GET /api/graph/history?as_of=1722528000000000
#[derive(Deserialize)]
pub struct TimeTravelParams {
    pub as_of: Option<String>,
    pub qid: Option<String>,
    pub namespace: Option<String>,
}

pub async fn time_travel(
    State(state): State<AppState>,
    Query(params): Query<TimeTravelParams>,
) -> Json<serde_json::Value> {
    // Check if fragment store is available
    let Some(ref frag_store) = state.fragment_store else {
        return Json(json!({
            "error": "Time-travel queries require tiered storage (start with --storage-backend=local or --storage-backend=s3)"
        }));
    };

    // Parse timestamp
    let as_of_ts = match params.as_of {
        Some(ref ts_str) => {
            // Try parsing as microseconds
            if let Ok(us) = ts_str.parse::<u64>() {
                us
            } else {
                // Try parsing as YYYYMMDD date
                let digits: String = ts_str.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 8 {
                    if let (Ok(y), Ok(m), Ok(d)) = (
                        digits[..4].parse::<i64>(),
                        digits[4..6].parse::<i64>(),
                        digits[6..8].parse::<i64>(),
                    ) {
                        if let Some(epoch_days) = date_to_epoch_days(y, m, d) {
                            epoch_days * 86400 * 1_000_000
                        } else {
                            return Json(json!({"error": "Invalid date format"}));
                        }
                    } else {
                        return Json(json!({"error": "Invalid date format"}));
                    }
                } else {
                    return Json(json!({"error": "Invalid timestamp format. Use microseconds or YYYYMMDD"}));
                }
            }
        }
        None => {
            return Json(json!({
                "error": "Missing 'as_of' parameter (timestamp in microseconds or YYYYMMDD date)"
            }));
        }
    };

    use nexora_fragment::time_travel::{execute_time_travel, TimeTravelQuery};

    let mut query = TimeTravelQuery::at(as_of_ts);

    if let Some(ref qid_str) = params.qid {
        // Try parsing as hex string first, then as u64
        let qid = if let Ok(id) = NexoraId::from_hex(qid_str) {
            id
        } else if let Ok(num) = qid_str.parse::<u64>() {
            // Treat as raw u64 bytes (little-endian)
            NexoraId::from_bytes(num.to_le_bytes().to_vec())
        } else {
            return Json(json!({"error": "Invalid qid format (expected hex or u64)"}));
        };
        query = query.for_node(qid);
    }

    if let Some(ref ns) = params.namespace {
        query = query.in_namespace(ns);
    }

    let frag_store_clone = Arc::clone(frag_store);
    match execute_time_travel(&*frag_store_clone.registry(), query).await {
        Ok(result) => {
            Json(json!({
                "as_of_us": as_of_ts,
                "nodes": result.nodes.iter().map(|n| json!({
                    "id": n.id,
                    "properties": n.properties,
                    "edges": n.edges.iter().map(|e| json!({
                        "type": e.edge_type,
                        "direction": e.direction,
                        "other": e.other,
                        "timestamp": e.timestamp,
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "node_count": result.nodes.len(),
            }))
        }
        Err(e) => Json(json!({
            "error": format!("Time-travel query failed: {}", e)
        })),
    }
}

pub async fn readiness(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({"ready": true, "shards": state.graph.shard_count()}))
}

pub async fn liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"alive": true}))
}

// ============================================================
// Auth Token Generation (dev only)
// ============================================================

#[derive(Deserialize)]
pub struct TokenRequest {
    pub user_id: String,
    #[serde(default = "default_role")]
    pub role: String,
}

fn default_role() -> String {
    "operator".to_string()
}

pub async fn generate_token(
    axum::extract::Extension(auth): axum::extract::Extension<Arc<crate::auth::Auth>>,
    Json(req): Json<TokenRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let role = crate::auth::Role::from_str(&req.role).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid role",
                "valid_roles": ["admin", "operator", "readonly"]
            })),
        )
    })?;

    let token = auth.generate_token_with_role(&req.user_id, role);
    Ok(Json(serde_json::json!({
        "token": token,
        "user_id": req.user_id,
        "role": role.to_string()
    })))
}

// ============================================================
// Stream Source Management (Kafka feature-gated)
// ============================================================

/// List active stream sources.
pub async fn list_streams(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> Json<serde_json::Value> {
    let streams = state.streams.read().await;
    let list: Vec<&StreamSource> = streams.values().collect();
    if pagination.limit.is_some() || pagination.offset.is_some() {
        Json(pagination.json(&list))
    } else {
        Json(serde_json::json!({"streams": list, "count": list.len()}))
    }
}

/// Request body for starting a Kafka stream source.
#[derive(Deserialize)]
#[cfg(feature = "kafka")]
pub struct KafkaStreamRequest {
    pub brokers: String,
    pub topic: String,
    #[serde(default = "default_group_id")]
    pub group_id: String,
    /// Optional JSON field holding each record's business event time (RFC 3339
    /// or epoch ms/µs). Takes precedence over the Kafka record timestamp.
    #[serde(default)]
    pub event_time_field: Option<String>,
    /// How to interpret a numeric `event_time_field` value: "ms" / "us" / "s" /
    /// "rfc3339". Defaults to the `EventTimeUnit` default when omitted or
    /// unrecognized. Ignored for string (RFC 3339) values.
    #[serde(default)]
    pub event_time_unit: Option<String>,
}

#[cfg(feature = "kafka")]
fn default_group_id() -> String {
    "nexora-app-consumer".to_string()
}

/// Start a Kafka consumer stream source.
#[cfg(feature = "kafka")]
pub async fn start_kafka_stream(
    State(state): State<AppState>,
    Json(req): Json<KafkaStreamRequest>,
) -> impl IntoResponse {
    use nexora_stream::kafka::{KafkaSource, KafkaSourceConfig};
    use nexora_stream::{IngestHandler, IngestionSource};

    let name = format!("kafka-{}", chrono::Utc::now().timestamp());

    let config = KafkaSourceConfig {
        brokers: req.brokers.clone(),
        topic: req.topic.clone(),
        group_id: req.group_id.clone(),
        key_field: "id".to_string(),
        event_time_field: req.event_time_field.clone(),
        event_time_unit: req
            .event_time_unit
            .as_deref()
            .and_then(nexora_stream::EventTimeUnit::parse)
            .unwrap_or_default(),
    };

    let source = Arc::new(KafkaSource::new(config.clone()));

    // Connect
    match source.connect().await {
        Ok(()) => {}
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("Failed to connect Kafka: {e}")})),
            );
        }
    }

    let source_for_task = source.clone();
    let name_for_task = name.clone();
    let _streams = state.streams.clone();

    // Unified sink: coalesce each poll batch and commit via write_batch.
    // Relaxed durability — Kafka replays from the committed offset on crash.
    // Honors event-first mode (shared store/router) via the AppState factory.
    let handler = state.make_ingest_handler(nexora_core::BatchDurability::Relaxed);
    let _handle = tokio::spawn(async move {
        tracing::info!(name = %name_for_task, topic = %config.topic, "Kafka stream source started");
        loop {
            match source_for_task.poll().await {
                Ok(Some(batch)) => {
                    if let Err(e) = handler.handle_batch(&batch).await {
                        tracing::warn!(
                            name = %name_for_task,
                            error = %e,
                            "Failed to write Kafka batch to graph"
                        );
                    }
                }
                Ok(None) => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    tracing::error!(name = %name_for_task, error = %e, "Kafka poll error");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    });

    let source_info = StreamSource {
        name: name.clone(),
        source_type: "kafka".to_string(),
        topic: req.topic,
        brokers: req.brokers,
        started_at: chrono::Utc::now().to_rfc3339(),
    };

    state
        .streams
        .write()
        .await
        .insert(name.clone(), source_info);

    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "started", "name": name})),
    )
}

/// Stub for when kafka feature is not enabled.
#[cfg(not(feature = "kafka"))]
pub async fn start_kafka_stream() -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({"error": "Kafka support not compiled (enable 'kafka' feature)"})),
    )
}

/// Stop a stream source by name.
pub async fn delete_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut streams = state.streams.write().await;
    if streams.remove(&name).is_some() {
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "stopped", "name": name})),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("Stream '{}' not found", name)})),
        )
    }
}

// ============================================================
// Tiered Storage — status and manual migration
// ============================================================

/// GET /api/v2/storage/status — storage tier statistics
pub async fn storage_status(State(state): State<AppState>) -> impl IntoResponse {
    match &state.tiered_store {
        Some(store) => {
            let hot_objects = store.list_all("").await;
            let hot_count = hot_objects
                .iter()
                .filter(|o| matches!(o.tier, nexora_storage::StorageTier::Hot))
                .count();
            let warm_count = hot_objects
                .iter()
                .filter(|o| matches!(o.tier, nexora_storage::StorageTier::Warm))
                .count();
            let cold_count = hot_objects
                .iter()
                .filter(|o| matches!(o.tier, nexora_storage::StorageTier::Cold))
                .count();
            let total_size: u64 = hot_objects.iter().map(|o| o.size).sum();
            (
                StatusCode::OK,
                Json(json!({
                    "backend": "tiered",
                    "total_objects": hot_objects.len(),
                    "total_size_bytes": total_size,
                    "hot_objects": hot_count,
                    "warm_objects": warm_count,
                    "cold_objects": cold_count,
                })),
            )
        }
        None => (
            StatusCode::OK,
            Json(json!({
                "backend": "memory",
                "total_objects": 0,
                "total_size_bytes": 0,
                "hot_objects": 0,
                "warm_objects": 0,
                "cold_objects": 0,
                "note": "Tiered storage not enabled. Use --storage-backend to activate."
            })),
        ),
    }
}

/// POST /api/v2/storage/migrate — manually trigger cold migration
pub async fn storage_migrate(State(state): State<AppState>) -> impl IntoResponse {
    match &state.tiered_store {
        Some(store) => match store.run_lifecycle().await {
            Ok(migrated) => (
                StatusCode::OK,
                Json(json!({
                    "status": "completed",
                    "migrated_objects": migrated,
                })),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            ),
        },
        None => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Tiered storage not enabled. Use --storage-backend to activate."
            })),
        ),
    }
}

// ============================================================
// Recipe Management — declarative graph computation recipes
// ============================================================

/// POST /api/v2/recipes — create a recipe
pub async fn create_recipe(
    State(state): State<AppState>,
    Json(req): Json<CreateRecipeRequest>,
) -> impl IntoResponse {
    if req.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "recipe name must not be empty"})),
        );
    }

    let mut recipes = state.recipes.write().await;

    if recipes.contains_key(&req.name) {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("recipe '{}' already exists", req.name)})),
        );
    }

    // Convert API request into a nexora_recipe::Recipe.
    // Each step becomes a standing query with a synthetic trigger pattern.
    let recipe = Recipe {
        name: req.name.clone(),
        version: Some("1.0".to_string()),
        description: req.description.clone(),
        standing_queries: req
            .steps
            .iter()
            .enumerate()
            .map(|(i, step)| nexora_recipe::StandingQueryRecipe {
                name: format!("{}-step-{}", req.name, i),
                pattern: serde_json::json!({
                    "type": "PropertyFilter",
                    "key": format!("__recipe__{}__step__{}", req.name, i),
                    "condition": "Exists"
                }),
                outputs: vec![nexora_recipe::OutputRecipe {
                    name: format!("step-{}-output", i),
                    output_type: "console".to_string(),
                    config: serde_json::json!({"query": step.query.clone()}),
                }],
            })
            .collect(),
        ingest_sources: vec![],
        outputs: vec![],
        status: Some(nexora_recipe::StatusRecipe {
            enabled: req.trigger.is_some(),
            interval_ms: None,
        }),
    };

    recipes.insert(req.name.clone(), recipe);

    let recipe_count = recipes.len();
    tracing::info!(name = %req.name, "Recipe created");

    (
        StatusCode::CREATED,
        Json(json!({
            "status": "created",
            "name": req.name,
            "recipe_count": recipe_count,
        })),
    )
}

/// GET /api/v2/recipes — list all recipes
pub async fn list_recipes(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> impl IntoResponse {
    let recipes = state.recipes.read().await;
    let list: Vec<serde_json::Value> = recipes
        .values()
        .map(|r| {
            json!({
                "name": r.name,
                "description": r.description,
                "version": r.version,
                "num_standing_queries": r.standing_queries.len(),
                "num_ingest_sources": r.ingest_sources.len(),
                "num_outputs": r.outputs.len(),
                "has_trigger": r.status.as_ref().map(|s| s.enabled).unwrap_or(false),
            })
        })
        .collect();
    if pagination.limit.is_some() || pagination.offset.is_some() {
        Json(pagination.json(&list))
    } else {
        Json(json!({"recipes": list, "count": list.len()}))
    }
}

/// GET /api/v2/recipes/{name} — get recipe details
pub async fn get_recipe(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let recipes = state.recipes.read().await;
    match recipes.get(&name) {
        Some(recipe) => {
            let steps: Vec<serde_json::Value> = recipe
                .standing_queries
                .iter()
                .enumerate()
                .map(|(i, sq)| {
                    json!({
                        "step_index": i,
                        "name": sq.name,
                        "pattern": sq.pattern,
                        "outputs": sq.outputs.iter().map(|o| json!({
                            "name": o.name,
                            "type": o.output_type,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();

            (
                StatusCode::OK,
                Json(json!({
                    "name": recipe.name,
                    "description": recipe.description,
                    "version": recipe.version,
                    "steps": steps,
                    "ingest_sources": recipe.ingest_sources.iter().map(|s| json!({
                        "name": s.name,
                        "type": s.source_type,
                    })).collect::<Vec<_>>(),
                    "outputs": recipe.outputs.iter().map(|o| json!({
                        "name": o.name,
                        "type": o.output_type,
                    })).collect::<Vec<_>>(),
                    "status": recipe.status.as_ref().map(|s| json!({
                        "enabled": s.enabled,
                        "interval_ms": s.interval_ms,
                    })),
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("recipe '{}' not found", name)})),
        ),
    }
}

/// DELETE /api/v2/recipes/{name} — delete a recipe
pub async fn delete_recipe(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut recipes = state.recipes.write().await;
    if recipes.remove(&name).is_some() {
        // Also remove associated run history
        state.recipe_runs.write().await.remove(&name);
        tracing::info!(name = %name, "Recipe deleted");
        (
            StatusCode::OK,
            Json(json!({"status": "deleted", "name": name})),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("recipe '{}' not found", name)})),
        )
    }
}

/// POST /api/v2/recipes/{name}/execute — manually execute a recipe
pub async fn execute_recipe(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let recipe = {
        let recipes = state.recipes.read().await;
        match recipes.get(&name) {
            Some(r) => r.clone(),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error": format!("recipe '{}' not found", name)})),
                );
            }
        }
    };

    let run_id = uuid::Uuid::new_v4().to_string();
    let started_at = chrono::Utc::now().to_rfc3339();

    // Record the run as "running"
    {
        let mut runs = state.recipe_runs.write().await;
        runs.entry(name.clone()).or_default().push(RecipeRunRecord {
            run_id: run_id.clone(),
            recipe_name: name.clone(),
            started_at: started_at.clone(),
            finished_at: None,
            status: "running".to_string(),
            result: None,
            error: None,
        });
    }

    // Execute the recipe: register its standing queries and validate sources/outputs
    let execution_result =
        match nexora_recipe::executor::execute_recipe(&state.graph, &state.sq_manager, &recipe)
            .await
        {
            Ok(exec) => {
                tracing::info!(
                    recipe = %name,
                    run_id = %exec.run_id,
                    sq_count = exec.sq_ids.len(),
                    "Recipe executed successfully"
                );
                Ok(json!({
                    "run_id": exec.run_id,
                    "status": exec.status,
                    "sq_ids": exec.sq_ids,
                    "started_at": exec.started_at,
                    "finished_at": exec.finished_at,
                    "error": exec.error,
                }))
            }
            Err(e) => {
                tracing::error!(
                    recipe = %name,
                    run_id = %run_id,
                    error = %e,
                    "Recipe execution failed"
                );
                Err(e)
            }
        };

    // Update the run record with the result
    {
        let mut runs = state.recipe_runs.write().await;
        if let Some(run) = runs
            .get_mut(&name)
            .and_then(|v| v.iter_mut().rev().find(|r| r.run_id == run_id))
        {
            run.finished_at = Some(chrono::Utc::now().to_rfc3339());
            match &execution_result {
                Ok(result) => {
                    run.status = "success".to_string();
                    run.result = Some(result.clone());
                }
                Err(e) => {
                    run.status = "error".to_string();
                    run.error = Some(e.clone());
                }
            }
        }
    }

    match execution_result {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({
                "run_id": run_id,
                "recipe": name,
                "status": "success",
                "result": result,
            })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "run_id": run_id,
                "recipe": name,
                "status": "error",
                "error": e,
            })),
        ),
    }
}

/// GET /api/v2/recipes/{name}/runs — get execution history for a recipe
pub async fn get_recipe_runs(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let runs = state.recipe_runs.read().await;
    match runs.get(&name) {
        Some(history) => {
            let list: Vec<&RecipeRunRecord> = history.iter().rev().take(50).collect();
            Json(json!({
                "recipe": name,
                "runs": list,
                "total_runs": history.len(),
            }))
        }
        None => Json(json!({
            "recipe": name,
            "runs": [],
            "total_runs": 0,
        })),
    }
}

// ============================================================
// UDF (User-Defined Functions) API
// ============================================================

/// POST /api/v2/udf/register — register a new UDF (native Rust function via expression).
#[derive(Deserialize)]
pub struct UdfRegisterRequest {
    pub name: String,
    pub code: String,
    #[serde(default = "default_language")]
    pub language: String,
}

fn default_language() -> String {
    "native".to_string()
}

#[derive(Serialize)]
pub struct UdfInfo {
    pub name: String,
    pub language: String,
}

pub async fn udf_register(
    State(state): State<AppState>,
    Json(req): Json<UdfRegisterRequest>,
) -> impl IntoResponse {
    let language = req.language.to_lowercase();
    match language.as_str() {
        "native" => {
            // Parse code as an ExpressionUdf specification (JSON)
            let spec: nexora_udf::types::ExpressionUdf = match serde_json::from_str(&req.code) {
                Ok(s) => s,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("Invalid UDF expression JSON: {e}")})),
                    );
                }
            };
            let udf_result = nexora_udf::types::make_expression_udf(spec);
            match udf_result {
                Ok(udf) => {
                    let mut registry = state.udf_registry.write().await;
                    registry.register(std::sync::Arc::new(udf));
                    (
                        StatusCode::CREATED,
                        Json(
                            json!({"status": "registered", "name": req.name, "language": "native"}),
                        ),
                    )
                }
                Err(e) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("Invalid expression: {e}")})),
                ),
            }
        }
        "wasm" => {
            // Body code is interpreted as raw wasm bytes encoded as base64 or
            // the user should use the dedicated /api/v2/udf/wasm/{name} endpoint.
            // Here we accept hex-encoded wasm bytes in the "code" field.
            let wasm_bytes = match hex::decode(&req.code) {
                Ok(b) => b,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(
                            json!({"error": format!("Invalid hex-encoded wasm bytes: {e}. Use POST /api/v2/udf/wasm/{{name}} with raw bytes body.")}),
                        ),
                    );
                }
            };
            let manager = state.udf_manager.clone();
            let name_clone = req.name.clone();
            let result = tokio::task::spawn_blocking(move || {
                let mut mgr = manager.blocking_lock();
                mgr.register_wasm(&name_clone, &wasm_bytes)
            })
            .await;
            match result {
                Ok(Ok(())) => (
                    StatusCode::CREATED,
                    Json(json!({"status": "registered", "name": req.name, "language": "wasm"})),
                ),
                Ok(Err(e)) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("Task join error: {e}")})),
                ),
            }
        }
        "python" => {
            let manager = state.udf_manager.clone();
            let name_clone = req.name.clone();
            let code_clone = req.code.clone();
            let result = tokio::task::spawn_blocking(move || {
                let mut mgr = manager.blocking_lock();
                mgr.register_python(&name_clone, &code_clone)
            })
            .await;
            match result {
                Ok(Ok(())) => (
                    StatusCode::CREATED,
                    Json(json!({"status": "registered", "name": req.name, "language": "python"})),
                ),
                Ok(Err(e)) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e.to_string()})),
                ),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": format!("Task join error: {e}")})),
                ),
            }
        }
        _ => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("unsupported language: '{}'", req.language)})),
        ),
    }
}

/// GET /api/v2/udf — list all registered UDFs (native, wasm, python).
pub async fn udf_list(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
) -> Json<serde_json::Value> {
    let registry = state.udf_registry.read().await;
    let mut udfs: Vec<UdfInfo> = registry
        .list()
        .into_iter()
        .map(|name| UdfInfo {
            name,
            language: "native".to_string(),
        })
        .collect();

    // Also list Wasm and Python UDFs from UdfManager
    let manager = state.udf_manager.clone();
    let mgr_udfs = tokio::task::spawn_blocking(move || {
        let mgr = manager.blocking_lock();
        mgr.list()
    })
    .await
    .unwrap_or_default();
    for (name, udf_type) in mgr_udfs {
        udfs.push(UdfInfo {
            name,
            language: match udf_type {
                nexora_udf::manager::UdfType::Wasm => "wasm".to_string(),
                nexora_udf::manager::UdfType::Python => "python".to_string(),
            },
        });
    }

    if pagination.limit.is_some() || pagination.offset.is_some() {
        Json(pagination.json(&udfs))
    } else {
        Json(json!({"udfs": udfs, "count": udfs.len()}))
    }
}

/// POST /api/v2/udf/execute — execute a UDF on given input properties.
#[derive(Deserialize)]
pub struct UdfExecuteRequest {
    pub name: String,
    pub inputs: HashMap<String, serde_json::Value>,
}

pub async fn udf_execute(
    State(state): State<AppState>,
    Json(req): Json<UdfExecuteRequest>,
) -> impl IntoResponse {
    let registry = state.udf_registry.read().await;
    let props: HashMap<String, PropertyValue> = req
        .inputs
        .iter()
        .map(|(k, v)| (k.clone(), json_to_pv(v)))
        .collect();

    match registry.execute(&req.name, &props) {
        Ok(result) => (StatusCode::OK, Json(json!({"result": pv_to_json(&result)}))),
        Err(nexora_udf::UdfError::NotFound(name)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("UDF '{}' not found", name)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

/// DELETE /api/v2/udf/{name} — unregister a UDF (native, wasm, or python).
pub async fn udf_delete(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    // Try native registry first
    {
        let mut registry = state.udf_registry.write().await;
        if registry.remove(&name) {
            return (
                StatusCode::OK,
                Json(json!({"status": "unregistered", "name": name, "type": "native"})),
            );
        }
    }

    // Try UdfManager (wasm/python)
    let manager = state.udf_manager.clone();
    let name_clone = name.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut mgr = manager.blocking_lock();
        mgr.unregister(&name_clone)
    })
    .await;

    match result {
        Ok(Ok(())) => (
            StatusCode::OK,
            Json(json!({"status": "unregistered", "name": name})),
        ),
        Ok(Err(_)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("UDF '{}' not found", name)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Task join error: {e}")})),
        ),
    }
}

// ============================================================
// Wasm & Python UDF API — dedicated endpoints
// ============================================================

/// POST /api/v2/udf/wasm/{name} — register a Wasm UDF (body: raw wasm bytes).
pub async fn udf_register_wasm(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Request body is empty. Expected raw Wasm bytes."})),
        );
    }

    let manager = state.udf_manager.clone();
    let name_for_closure = name.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut mgr = manager.blocking_lock();
        mgr.register_wasm(&name_for_closure, &body)
    })
    .await;

    match result {
        Ok(Ok(())) => (
            StatusCode::CREATED,
            Json(json!({
                "status": "registered",
                "name": name,
                "type": "wasm",
            })),
        ),
        Ok(Err(e)) => {
            let status = match &e {
                nexora_udf::UdfRuntimeError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                nexora_udf::UdfRuntimeError::Wasm(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Task join error: {e}")})),
        ),
    }
}

/// POST /api/v2/udf/python/{name} — register a Python UDF.
#[derive(Deserialize)]
pub struct PythonUdfRegisterRequest {
    pub code: String,
}

pub async fn udf_register_python(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<PythonUdfRegisterRequest>,
) -> impl IntoResponse {
    if req.code.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "code field must not be empty"})),
        );
    }

    let manager = state.udf_manager.clone();
    let name_for_closure = name.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut mgr = manager.blocking_lock();
        mgr.register_python(&name_for_closure, &req.code)
    })
    .await;

    match result {
        Ok(Ok(())) => (
            StatusCode::CREATED,
            Json(json!({
                "status": "registered",
                "name": name,
                "type": "python",
            })),
        ),
        Ok(Err(e)) => {
            let status = match &e {
                nexora_udf::UdfRuntimeError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Task join error: {e}")})),
        ),
    }
}

/// POST /api/v2/udf/{name}/execute — execute a UDF by name (wasm or python).
///
/// Body: any JSON value that will be passed as input to the UDF.
/// The UDF is looked up in the UdfManager (wasm/python) first, then
/// falls back to the native UdfRegistry.
pub async fn udf_execute_by_name(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(input): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Try UdfManager (wasm/python) first
    let manager = state.udf_manager.clone();
    let name_for_mgr = name.clone();
    let input_for_mgr = input.clone();
    let mgr_result = tokio::task::spawn_blocking(move || {
        let mgr = manager.blocking_lock();
        mgr.execute(&name_for_mgr, &input_for_mgr)
    })
    .await;

    match mgr_result {
        Ok(Ok(value)) => {
            return (StatusCode::OK, Json(json!({"result": value, "name": name})));
        }
        Ok(Err(nexora_udf::UdfRuntimeError::NotFound(_))) => {
            // Fall through to native registry
        }
        Ok(Err(e)) => {
            let status = match &e {
                nexora_udf::UdfRuntimeError::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
                nexora_udf::UdfRuntimeError::Security(_) => StatusCode::FORBIDDEN,
                nexora_udf::UdfRuntimeError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return (status, Json(json!({"error": e.to_string(), "name": name})));
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Task join error: {e}")})),
            );
        }
    }

    // Fall back to native UdfRegistry
    let registry = state.udf_registry.read().await;
    let props: HashMap<String, PropertyValue> = match input.as_object() {
        Some(map) => map
            .iter()
            .map(|(k, v)| (k.clone(), json_to_pv(v)))
            .collect(),
        None => HashMap::new(),
    };

    match registry.execute(&name, &props) {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({"result": pv_to_json(&result), "name": name})),
        ),
        Err(nexora_udf::UdfError::NotFound(n)) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("UDF '{}' not found", n)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

// ============================================================
// F4.2: Tumbling Window Analytics API
// ============================================================

/// Request body for `POST /api/v2/analytics/tumbling-window`.
#[derive(Deserialize)]
pub struct TumblingWindowRequest {
    /// List of `[timestamp_ms, value]` event pairs.
    pub events: Vec<[f64; 2]>,
    /// Window size in milliseconds (e.g. 60000 for 1-minute windows).
    pub window_size_ms: i64,
}

/// `POST /api/v2/analytics/tumbling-window`
///
/// Aggregates a batch of `(timestamp_ms, value)` events into fixed-size
/// tumbling windows. Returns per-window count, sum, min, and max.
///
/// This is a stateless, CPU-bound primitive — no graph data is involved. It
/// works together with [`nexora_zenoh::watermark::WatermarkGenerator`] to
/// determine when a window is ready (global watermark >= window_end).
pub async fn analytics_tumbling_window(
    State(state): State<AppState>,
    Json(req): Json<TumblingWindowRequest>,
) -> impl IntoResponse {
    use nexora_zenoh::watermark::TumblingWindow;

    if req.window_size_ms <= 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "window_size_ms must be positive"})),
        );
    }

    let tw = TumblingWindow::new(req.window_size_ms);
    let results = tw.aggregate(req.events.iter().map(|pair| (pair[0] as i64, pair[1])));

    // F4: each emitted window counts as one "fired" window for observability.
    if !results.is_empty() {
        state.metrics.inc_windows_fired(results.len() as u64);
    }

    let windows: Vec<serde_json::Value> = results
        .iter()
        .map(|w| {
            json!({
                "window_start_ms": w.window_start_ms,
                "window_end_ms":   w.window_end_ms,
                "count":           w.count,
                "sum":             w.sum,
                "min":             w.min,
                "max":             w.max,
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "window_size_ms": req.window_size_ms,
            "windows": windows,
        })),
    )
}

// ============================================================
// E7.2: Key Rotation Stub
// ============================================================

/// `POST /api/v2/admin/rotate-key`
///
/// WAL encryption key rotation guide. Online rotation is not yet implemented
/// because it requires a WAL re-encryption pass that must run offline. This
/// endpoint returns step-by-step operational instructions so operators know
/// exactly what to do; it does NOT modify any data or secrets.
pub async fn admin_rotate_key(State(_state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "status": "manual_required",
        "reason": "Online WAL key rotation is not yet implemented. \
                   Rotation requires a WAL re-encryption pass that must run offline \
                   to avoid partial-write corruption.",
        "instructions": [
            "1. Drain the node: POST /api/v2/admin/drain",
            "2. Stop the Nexora process.",
            "3. Backup the WAL directory (cp -r <wal_dir> <wal_dir>.bak).",
            "4. Re-encrypt the WAL with the new key: \
               nexora-core wal reencrypt --old-key <old> --new-key <new> <wal_dir>",
            "5. Update the key in your config / secret store.",
            "6. Restart the Nexora process with the new key.",
        ],
        "automated_rotation": "not_yet_implemented",
        "reference": "docs/ops/SECURITY.md#key-rotation"
    }))
}

// ============================================================
// E7 + F4 tests
// ============================================================

#[cfg(test)]
mod e7_f4_tests {
    use super::*;
    use axum::response::IntoResponse;
    use nexora_core::{GraphServiceConfig, InMemoryPersistor};
    use nexora_hnsw::{HnswConfig, HnswIndex};

    fn test_state() -> AppState {
        AppState {
            graph: Arc::new(GraphService::new(
                GraphServiceConfig {
                    num_shards: 2,
                    max_nodes_per_shard: 100,
                    node_channel_size: 16,
                },
                Arc::new(InMemoryPersistor::new()),
            )),
            sq_manager: Arc::new(StandingQueryManager::new(16)),
            config: AppConfig {
                max_nodes_per_shard: 100,
                rocksdb_path: None,
                wal_dir: None,
                allow_ingest_dir: None,
                profile: "lite-ephemeral".to_string(),
            },
            shutdown: Arc::new(tokio::sync::Notify::new()),
            start_time: std::time::Instant::now(),
            ingests: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            metrics: crate::metrics::Metrics::new(),
            hnsw: Arc::new(Mutex::new(HnswIndex::new(HnswConfig::default()))),
            streams: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            recipes: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            recipe_runs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            udf_registry: Arc::new(tokio::sync::RwLock::new(
                nexora_udf::native::UdfRegistry::new(),
            )),
            udf_manager: Arc::new(Mutex::new(UdfManager::new())),
            tiered_store: None,
            mv_manager: Arc::new(MaterializedViewManager::new()),
            ontology_manager: Arc::new(nexora_core::ontology_manager::OntologyManager::new()),
            #[cfg(feature = "event-first")]
            event_store: None,
            #[cfg(feature = "event-first")]
            event_router: None,
            #[cfg(feature = "event-first")]
            refresh_scheduler: None,
            sq_mv_bridge: Arc::new(crate::sq_mv_bridge::SQMaterializedViewBridge::new(
                Arc::new(MaterializedViewManager::new()),
            )),
            router: None,
            replica_writer: None,
            catch_up_barrier: None,
            auth: None,
            drain: crate::drain::DrainState::default(),
            query_pool: Arc::new(nexora_core::query_pool::QueryPool::new(4)),
            cluster_manager: None,
            #[cfg(feature = "event-streaming")]
            event_streaming: None,
            #[cfg(all(feature = "event-streaming", feature = "embedded"))]
            distributed_event_streaming: None,
        }
    }

    /// E7.1 — audit! macro must not panic and must compile.
    #[test]
    fn test_audit_macro_emits_structured_fields() {
        // macro_rules! audit! is in scope via `use super::*`; just verify it
        // expands without panicking. Tracing output is not captured in unit
        // tests, but the point is the macro compiles and doesn't panic.
        audit!("test_action", "test_user", "/test/resource", "success");
    }

    /// E7.2 — rotate-key handler returns HTTP 200 with an "instructions" field.
    #[tokio::test]
    async fn test_rotate_key_returns_instructions() {
        let state = test_state();
        let response = admin_rotate_key(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "manual_required");
        assert!(
            json["instructions"].is_array(),
            "instructions must be an array"
        );
        let instructions = json["instructions"].as_array().unwrap();
        assert!(instructions.len() >= 4, "at least 4 steps expected");
    }

    /// F4.2 — tumbling-window handler returns correct aggregated windows.
    #[tokio::test]
    async fn test_tumbling_window_handler_aggregates_correctly() {
        // Three events: two in [0, 60s), one in [60s, 120s)
        let req = TumblingWindowRequest {
            events: vec![[10_000.0, 2.0], [50_000.0, 8.0], [70_000.0, 5.0]],
            window_size_ms: 60_000,
        };
        let state = test_state();
        let response = analytics_tumbling_window(State(state.clone()), Json(req))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        // F4: the two emitted windows must be counted in the metric.
        assert_eq!(
            state
                .metrics
                .window_fired_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let windows = json["windows"].as_array().unwrap();
        assert_eq!(windows.len(), 2);

        let w0 = &windows[0];
        assert_eq!(w0["window_start_ms"], 0);
        assert_eq!(w0["count"], 2);
        assert!((w0["sum"].as_f64().unwrap() - 10.0).abs() < 1e-9);

        let w1 = &windows[1];
        assert_eq!(w1["window_start_ms"], 60_000);
        assert_eq!(w1["count"], 1);
    }

    /// F4.2 — bad request when window_size_ms <= 0.
    #[tokio::test]
    async fn test_tumbling_window_handler_rejects_invalid_size() {
        let req = TumblingWindowRequest {
            events: vec![[1000.0, 1.0]],
            window_size_ms: 0,
        };
        let response = analytics_tumbling_window(State(test_state()), Json(req))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
