//! Nexora-RS Application — HTTP API server with graceful shutdown.
//!
//! Lite mode: runs with zero external dependencies (InMemory, no WAL).
//! Durable mode: RocksDB persistence + WAL for crash recovery.

#![recursion_limit = "1024"]

mod auth;
mod compat;
mod config;
mod config_loader;
mod drain;
mod error;
mod handlers;
mod metrics;
mod openapi;
mod query_rewriter;
mod raft_handler;
mod request_id;
#[cfg(feature = "event-streaming")]
mod risingwave_init;
mod security;
mod sq_mv_bridge;
mod sql_ddl_parser;
mod telemetry;

use axum::{
    http::Method,
    response::Html,
    routing::{delete, get, post},
    Router,
};

use anyhow::Context;
use clap::Parser;
use handlers::{publish_sq_event, AppConfig, AppState};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_hnsw::{HnswConfig, HnswIndex};
use nexora_standing_query::StandingQueryManager;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

#[cfg(all(feature = "event-streaming", feature = "library"))]
use clap::ValueEnum;

#[cfg(all(feature = "event-streaming", feature = "library"))]
#[derive(Debug, Clone, Copy, ValueEnum)]
enum MetaBackendType {
    Memory,
    Sqlite,
    Etcd,
}

// Ingestion handlers are now built via `AppState::make_ingest_handler` (see
// handlers.rs), which honors event-first mode using the shared EventLogStore +
// TopicRouter singletons. The old per-source `create_ingest_handler` factory
// (which re-created a store per source at a hard-coded path) has been removed.
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

/// Adapts the shared [`EventLogStore`] to the `nexora-zenoh`
/// [`EventTableScanner`] trait, so a peer's graph handler can serve this node's
/// local event tables (as Arrow IPC bytes) for cross-node event queries.
/// nexora-zenoh sits below nexora-eventlog in the dependency graph, so this
/// bridge lives here in the app layer where both are in scope.
#[cfg(feature = "event-first")]
struct EventStoreScanner {
    store: Arc<nexora_eventlog::EventLogStore>,
}

#[cfg(feature = "event-first")]
#[async_trait::async_trait]
impl nexora_zenoh::EventTableScanner for EventStoreScanner {
    async fn scan_table_ipc(&self, table: &str) -> Result<Vec<u8>, String> {
        self.store
            .scan_table_ipc(table)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Applies an ontology broadcast by a peer node: registers it in the local
/// ontology manager and activates its event-first plane (create event tables +
/// update the topic router + schedule its materialized views). Does NOT
/// re-broadcast — the originating node fans the ontology out to every peer, and
/// each peer applies it exactly once here.
///
/// Bridges the `nexora-zenoh` [`OntologyApplier`] trait; lives in the app layer
/// because it needs the ontology manager, event store, router, and scheduler
/// (all above nexora-zenoh in the dependency graph).
#[cfg(feature = "event-first")]
struct AppOntologyApplier {
    ontology_manager: Arc<nexora_core::ontology_manager::OntologyManager>,
    event_store: Arc<nexora_eventlog::EventLogStore>,
    event_router: Arc<nexora_eventlog::TopicRouter>,
    refresh_scheduler: Arc<nexora_eventlog::RefreshScheduler>,
}

#[cfg(feature = "event-first")]
#[async_trait::async_trait]
impl nexora_zenoh::OntologyApplier for AppOntologyApplier {
    async fn apply_ontology(&self, pkg_json: &str) -> Result<(), String> {
        let pkg: nexora_core::domain_package::DomainPackage =
            serde_json::from_str(pkg_json).map_err(|e| format!("parse ontology: {e}"))?;
        // Register (idempotent create/update) so it persists and is queryable.
        let domain = self
            .ontology_manager
            .create(pkg.clone())
            .await
            .map_err(|e| format!("register ontology: {e}"))?;
        // Activate event-first plane: create each mapped topic's event table and
        // point its router rule at Both, then schedule its views.
        for m in &pkg.mappings {
            if let Err(e) = self
                .event_store
                .ensure_table_from_domain(&m.source, &pkg)
                .await
            {
                tracing::warn!(
                    "apply_ontology: ensure table '{}' for '{}' failed: {}",
                    m.source,
                    domain,
                    e
                );
            }
        }
        self.event_router.apply_domain_package(&pkg);
        handlers::ontology::schedule_domain_views(&self.refresh_scheduler, &pkg).await;
        tracing::info!("Applied broadcast ontology '{}' locally", domain);
        Ok(())
    }

    async fn remove_ontology(&self, domain: &str) -> Result<(), String> {
        // Capture the package before removal so its router rules can be unmapped.
        // The event *tables* are intentionally kept (append-only source of truth);
        // removal only stops new double-writes and drops routing.
        let pkg_before = self.ontology_manager.get(domain).await;
        self.ontology_manager
            .remove(domain)
            .await
            .map_err(|e| format!("remove ontology: {e}"))?;
        if let Some(pkg) = pkg_before {
            self.event_router.remove_domain_package(&pkg);
        }
        tracing::info!("Removed broadcast ontology '{}' locally", domain);
        Ok(())
    }
}

/// DeepStreaming: streaming graph engine
#[derive(Parser, Debug)]
#[command(name = "nexora-app", version, about)]
struct Cli {
    /// Path to configuration file (default: ./nexora.toml)
    #[arg(long)]
    config: Option<PathBuf>,

    /// HTTP listen address. Defaults to loopback so a bare run is not exposed
    /// to the network; pass --host 0.0.0.0 to expose it (containers already do).
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// HTTP listen port
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Number of graph shards
    #[arg(long, default_value_t = 256)]
    num_shards: usize,

    /// Max nodes per shard before LRU eviction
    #[arg(long, default_value_t = 10_000)]
    max_nodes_per_shard: usize,

    /// RocksDB data directory (falls back to config file if not provided)
    #[arg(long)]
    rocksdb_path: Option<PathBuf>,

    /// Use in-memory storage instead of RocksDB
    #[arg(long)]
    no_rocksdb: bool,

    /// WAL directory for crash recovery
    #[arg(long, default_value = "./nexora-data/wal")]
    wal_dir: PathBuf,

    /// Disable WAL (not recommended for production)
    #[arg(long)]
    no_wal: bool,

    /// WAL fsync policy: group (default, best throughput, ack=durable),
    /// always (fsync every write), every_n (fsync every --wal-sync-interval
    /// records; may lose recent writes on crash), or never (rely on OS).
    /// Overrides nexora.toml [storage.wal] sync_policy.
    #[arg(long)]
    wal_sync_policy: Option<String>,

    /// Records between fsyncs when --wal-sync-policy=every_n.
    #[arg(long)]
    wal_sync_interval: Option<u64>,

    /// Application run profile: lite-ephemeral, single-durable, or clustered
    #[arg(long)]
    profile: Option<String>,

    /// Require authentication for API endpoints (default: true for security)
    #[arg(long, default_value_t = true)]
    require_auth: bool,

    /// Allow unauthenticated access (disables --require-auth). Use only for development!
    #[arg(long, conflicts_with = "require_auth")]
    allow_unauthenticated: bool,

    /// Secret key for HMAC-SHA256 token signing (or set NEXORA_AUTH_SECRET env var)
    #[arg(long)]
    auth_secret: Option<String>,

    /// Explicitly permit running with the built-in default auth secret. Without
    /// this flag the server refuses to start on the default secret whenever it
    /// is network-exposed (non-loopback --host), because that secret is public
    /// in the source and lets anyone forge an admin token. Local/dev only.
    #[arg(long)]
    insecure_dev: bool,

    /// Directory allowed for file ingest (security: prevents path traversal)
    #[arg(long)]
    allow_ingest_dir: Option<PathBuf>,

    /// Enable rate limiting (enabled by default)
    #[arg(long, default_value_t = true)]
    rate_limit: bool,

    /// Rate limit: requests per second per client IP
    #[arg(long, default_value_t = 100.0)]
    rate_limit_rate: f64,

    /// Rate limit: maximum burst capacity per client IP
    #[arg(long, default_value_t = 200)]
    rate_limit_burst: usize,

    /// TLS certificate file path (enables HTTPS)
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// TLS private key file path (requires --tls-cert)
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// Generate a self-signed TLS certificate and key (development only)
    #[arg(long)]
    gen_tls_cert: Option<PathBuf>,

    /// CORS allowed origin (default: "none" for security, use "*" for development or specific domain for production)
    #[arg(long, default_value = "none")]
    cors_origin: String,

    /// Refuse to start if CRITICAL security issues are detected (also: NEXORA_STRICT_SECURITY=true)
    #[arg(long)]
    strict_security: bool,

    /// Persist the audit trail to a file (newline-delimited JSON, append mode).
    /// When unset, audit entries are only emitted via the `tracing` logger.
    #[arg(long)]
    audit_log_file: Option<PathBuf>,

    /// OpenTelemetry OTLP/gRPC collector endpoint (e.g. "http://localhost:4317").
    /// Requires building with `--features otel`; exports distributed traces.
    #[arg(long)]
    otlp_endpoint: Option<String>,

    /// Service name reported to the OTLP collector (`service.name` resource attribute).
    #[arg(long, default_value = "nexora")]
    otel_service_name: String,

    // ============================================================
    // Cluster mode (Phase C — Distributed)
    // ============================================================
    /// Enable cluster mode (distributed multi-node operation)
    #[arg(long)]
    cluster: bool,

    /// Path to cluster configuration YAML file.
    /// When set, cluster configuration is loaded from this file instead of CLI args.
    /// See config/examples/cluster-3node.yaml for format.
    #[arg(long, conflicts_with_all = &["node_id", "cluster_listen_addr", "cluster_heartbeat_addr", "peers", "replication_factor"])]
    cluster_config: Option<PathBuf>,

    /// This node's unique ID in the cluster (e.g., "node-1")
    #[arg(long)]
    node_id: Option<String>,

    /// Address for inter-node graph operations (e.g., "127.0.0.1:7000")
    #[arg(long)]
    cluster_listen_addr: Option<String>,

    /// Address for heartbeat protocol (e.g., "127.0.0.1:7001")
    #[arg(long)]
    cluster_heartbeat_addr: Option<String>,

    /// Peer node to bootstrap with, as "node_id@graph_addr@heartbeat_addr"
    /// where each addr is host:port (e.g. "node-2@127.0.0.1:7110@127.0.0.1:7111").
    /// The legacy colon form "node_id:graph_addr:heartbeat_addr" is still accepted
    /// for backward compatibility. Can be specified multiple times for multiple peers.
    #[arg(long = "peer")]
    peers: Vec<String>,

    /// Replication factor for cluster mode: 1 owner + (rf-1) followers per shard.
    /// 1 (default) = no replication (a node failure loses its shards' data).
    /// 3 = tolerate one node failure. Clamped to the number of nodes.
    #[arg(long, default_value_t = 1)]
    replication_factor: usize,

    /// Opt-in background Merkle anti-entropy interval, in seconds. When set,
    /// each node periodically compares its per-shard replication-log digest with
    /// its peer replicas' and pulls+applies any divergent ops (defense-in-depth;
    /// divergence is otherwise healed on failover catch-up + read-repair).
    /// Unset (default) disables it. Requires a replication log to be useful.
    #[arg(long)]
    anti_entropy_secs: Option<u64>,

    /// Opt-in event-table retention sweep interval, in seconds (event-first only).
    /// When set, a background task periodically identifies snapshots older than
    /// `--retention-days` and (on iceberg 0.9.1) logs them as expirable; actual
    /// deletion lands when the iceberg-rust expire-snapshots API is available.
    /// Unset (default) disables the sweep.
    #[arg(long)]
    retention_secs: Option<u64>,

    /// Retention window in days for the event-table retention sweep (default 90).
    /// Snapshots older than this are candidates for expiry (see `--retention-secs`).
    #[arg(long, default_value_t = 90)]
    retention_days: u64,

    // ============================================================
    // Raft consensus mode (opt-in, requires --cluster)
    // ============================================================
    /// Enable Raft consensus by specifying the TCP port for Raft RPC traffic.
    /// When set, quorum-based log replication replaces simple heartbeats.
    #[arg(long)]
    raft_port: Option<u16>,

    /// Raft peer addresses in "host:port" format.
    /// Can be specified multiple times for multiple peers.
    #[arg(long = "raft-peer")]
    raft_peers: Vec<String>,

    // ============================================================
    // Stream ingestion (Kafka)
    // ============================================================
    /// Kafka bootstrap servers (e.g., "localhost:9092").
    /// Enables real-time streaming data ingestion via Kafka.
    #[arg(long)]
    kafka_brokers: Option<String>,

    /// Kafka topic to consume from.
    #[arg(long, requires = "kafka_brokers")]
    kafka_topic: Option<String>,

    /// Kafka consumer group ID.
    #[arg(long, default_value = "nexora-app-consumer")]
    kafka_group_id: String,

    /// JSON field holding each record's business event time, applied to every
    /// stream source (Kafka/MQTT/WebSocket/Kinesis/Zenoh). Enables event-time
    /// last-writer-wins + windowing. RFC 3339 string or integer epoch (ms/µs).
    /// Unset → sources fall back to transport metadata or arrival order.
    #[arg(long)]
    event_time_field: Option<String>,

    /// Offset-aligned checkpoint interval (seconds) for stream ingestion. A
    /// positive value, with persistence (RocksDB) enabled, makes the ingestion
    /// pipeline periodically flush graph state and bind it to the current source
    /// offsets; on restart it resumes from that checkpoint (seeking the source
    /// back to the flushed cut) — giving exactly-once ingestion even under the
    /// relaxed per-batch durability used for throughput. A value of 0 disables
    /// checkpointing (a crash may then lose the last un-fsynced batch and,
    /// because the broker offset is already committed, NOT replay it).
    #[arg(long, default_value_t = 30)]
    checkpoint_secs: u64,

    /// MQTT broker host (e.g., "localhost"). Enables MQTT ingestion.
    #[arg(long)]
    mqtt_host: Option<String>,

    /// MQTT broker port.
    #[arg(long, default_value_t = 1883, requires = "mqtt_host")]
    mqtt_port: u16,

    /// MQTT topic filter(s) to subscribe to (repeatable; supports +/# wildcards).
    #[arg(long, requires = "mqtt_host")]
    mqtt_topic: Vec<String>,

    /// MQTT QoS level (0, 1, or 2).
    #[arg(long, default_value_t = 1, requires = "mqtt_host")]
    mqtt_qos: u8,

    /// WebSocket URL to ingest from (e.g. "ws://host:9001/stream").
    #[arg(long)]
    ws_url: Option<String>,

    // ============================================================
    // Event Streams integration (opt-in, SQL-based stream processing)
    // ============================================================
    /// Enable SQL-based event stream processing (requires --features event-streaming)
    #[cfg(feature = "event-streaming")]
    #[arg(long)]
    enable_event_streaming: bool,

    /// Event Streaming mode: single or distributed
    #[cfg(feature = "event-streaming")]
    #[arg(
        long,
        default_value = "single",
        value_parser = ["single", "distributed"]
    )]
    event_streaming_mode: String,

    /// Run event stream engine as embedded subprocess
    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    #[arg(long, requires = "enable_event_streaming")]
    embedded_event_streaming: bool,

    /// Run event stream engine as in-process library (requires --features library)
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long)]
    library_event_streaming: bool,

    /// Run event stream engine as distributed in-process library (multi-node cluster)
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, conflicts_with = "library_event_streaming")]
    distributed_library_event_streaming: bool,

    /// Node ID for distributed library mode (e.g., "meta-1")
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, requires = "distributed_library_event_streaming")]
    library_node_id: Option<String>,

    /// Meta listen address for distributed library mode (e.g., "0.0.0.0:5690")
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, requires = "distributed_library_event_streaming")]
    library_meta_addr: Option<String>,

    /// Meta advertise address for distributed library mode (e.g., "node1:5690")
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, requires = "distributed_library_event_streaming")]
    library_meta_advertise: Option<String>,

    /// Peer Meta nodes for Raft cluster (format: "node_id@addr", repeatable)
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(
        long = "library-meta-peer",
        requires = "distributed_library_event_streaming"
    )]
    library_meta_peers: Vec<String>,

    /// Event streams Meta node address (e.g., "127.0.0.1:5690")
    #[cfg(feature = "event-streaming")]
    #[arg(long, requires = "enable_event_streaming")]
    event_streaming_meta_addr: Option<String>,

    /// Event streams Frontend node address (e.g., "127.0.0.1:4566")
    #[cfg(feature = "event-streaming")]
    #[arg(long, requires = "enable_event_streaming")]
    event_streaming_frontend_addr: Option<String>,

    /// Enable event streams Meta HA with Raft
    #[cfg(feature = "event-streaming")]
    #[arg(long, requires = "enable_event_streaming")]
    event_streaming_ha: bool,

    /// Enable event streams cluster mode (3-node HA)
    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    #[arg(long, requires = "embedded_event_streaming")]
    event_streaming_cluster: bool,

    /// Event streams Raft node ID (for HA mode)
    #[cfg(feature = "event-streaming")]
    #[arg(long, requires = "event_streaming_ha")]
    event_streaming_raft_node_id: Option<u64>,

    /// Event streams Raft peer node IDs (e.g., "2,3" for a 3-node cluster)
    #[cfg(feature = "event-streaming")]
    #[arg(long, requires = "event_streaming_ha", value_delimiter = ',')]
    event_streaming_raft_peers: Vec<u64>,

    /// Meta backend storage type (memory, sqlite, etcd)
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, value_enum, default_value = "memory")]
    meta_backend: MetaBackendType,

    // ============================================================
    // GraphStreaming (Phase 7.6 - Event-to-Graph Projection)
    // ============================================================
    /// Directory containing YAML projection rules for GraphStreaming
    #[cfg(all(feature = "event-first", feature = "event-streaming"))]
    #[arg(long)]
    graph_streaming_rules: Option<PathBuf>,

    /// SQLite database path for Meta backend (required when --meta-backend=sqlite)
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, requires_if("sqlite", "meta_backend"))]
    meta_backend_sqlite_path: Option<PathBuf>,

    /// Etcd endpoints for Meta backend (comma-separated, e.g. "http://etcd1:2379,http://etcd2:2379")
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    #[arg(long, requires_if("etcd", "meta_backend"), value_delimiter = ',')]
    meta_backend_etcd_endpoints: Vec<String>,

    // ============================================================
    // Kinesis
    // ============================================================
    /// Kinesis stream name to ingest from. Enables Kinesis ingestion.
    #[arg(long)]
    kinesis_stream: Option<String>,

    /// AWS region for Kinesis (e.g. "us-east-1").
    #[arg(long, requires = "kinesis_stream")]
    kinesis_region: Option<String>,

    /// Kinesis endpoint override (e.g. "http://localhost:4566" for LocalStack).
    #[arg(long, requires = "kinesis_stream")]
    kinesis_endpoint: Option<String>,

    /// zenoh key expression to subscribe to (e.g. "nexora/ingest/**").
    /// Enables zenoh ingestion.
    #[arg(long)]
    zenoh_key: Option<String>,

    // ============================================================
    // Data-at-rest encryption
    // ============================================================
    /// Enable WAL encryption (AES-256-GCM).
    /// Requires either --encryption-key, --encryption-key-file, or NEXORA_ENCRYPTION_KEY env var.
    #[arg(long)]
    encrypt_wal: bool,

    /// Hex-encoded AES-256 key for WAL encryption (64 hex characters).
    /// Overrides NEXORA_ENCRYPTION_KEY env var.
    #[arg(long)]
    encryption_key: Option<String>,

    /// Path to a file containing a hex-encoded AES-256 key.
    #[arg(long)]
    encryption_key_file: Option<PathBuf>,

    // ============================================================
    // PostgreSQL Wire Protocol (nexora-pgwire)
    // ============================================================
    /// Port for PostgreSQL wire protocol server.
    /// When set, Nexora accepts PostgreSQL simple-query clients on this port.
    #[arg(long)]
    pg_port: Option<u16>,

    /// Address for the PostgreSQL protocol listener.
    #[arg(long, default_value = "127.0.0.1")]
    pg_bind: String,

    /// Use trust authentication for PG connections (no password).
    /// WARNING: Only for development / localhost environments.
    #[arg(long)]
    pg_trust: bool,

    /// JSON PG users: {"name":{"password":"secret","role":"operator|readonly"}}
    #[arg(long)]
    pg_users: Option<PathBuf>,

    /// Maximum concurrent PostgreSQL connections.
    #[arg(long, default_value_t = 100)]
    pg_max_connections: usize,

    /// Close an idle PostgreSQL connection after this many seconds.
    #[arg(long, default_value_t = 600)]
    pg_idle_timeout: u64,

    /// TLS certificate for PostgreSQL connections.
    #[arg(long, requires = "pg_tls_key")]
    pg_tls_cert: Option<PathBuf>,

    /// TLS private key for PostgreSQL connections.
    #[arg(long, requires = "pg_tls_cert")]
    pg_tls_key: Option<PathBuf>,

    // ============================================================
    // Tiered storage (nexora-storage)
    // ============================================================
    /// Storage backend: memory (default), local, or s3
    #[arg(long, default_value = "memory")]
    storage_backend: String,

    /// S3 bucket name (required when --storage-backend=s3)
    #[arg(long)]
    s3_bucket: Option<String>,

    /// AWS region (default: us-east-1)
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    /// S3 endpoint URL (e.g. "https://s3.us-east-1.amazonaws.com" or
    /// "http://localhost:9000" for MinIO). Overrides AWS_ENDPOINT_URL env var.
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 access key ID. Falls back to AWS_ACCESS_KEY_ID env var.
    #[arg(long)]
    s3_access_key: Option<String>,

    /// S3 secret access key. Falls back to AWS_SECRET_ACCESS_KEY env var.
    #[arg(long)]
    s3_secret_key: Option<String>,

    /// Use path-style S3 addressing (required for MinIO; AWS uses virtual-host).
    #[arg(long)]
    s3_path_style: bool,

    /// Archive nodes to S3 after N days of inactivity
    #[arg(long, default_value_t = 30)]
    s3_cold_after_days: u64,

    /// Fragment sealing interval (seconds): how often the graph→fragment
    /// pipeline closes a time window into an immutable fragment. Only active
    /// when --storage-backend is not "memory".
    #[arg(long, default_value_t = 300)]
    seal_interval_secs: u64,

    /// Idle node eviction TTL (seconds): nodes not accessed for this long are
    /// evicted (state persisted, memory released). Only active when tiered
    /// storage is enabled. 0 = disabled.
    #[arg(long, default_value_t = 3600)]
    idle_evict_secs: u64,

    // ============================================================
    // UDF — User Defined Functions
    // ============================================================
    /// Directory for UDF scripts and .wasm files (default: ./udf)
    #[arg(long, default_value = "./udf")]
    udf_dir: PathBuf,

    // ============================================================
    // Webhook output sink (Standing Query results → HTTP endpoint)
    // ============================================================
    /// POST every Standing Query result (match/unmatch) to this HTTP URL.
    /// Enables a webhook output sink wired to the SQ result stream.
    #[arg(long)]
    webhook_url: Option<String>,

    /// Extra HTTP header for webhook POSTs, formatted "Name: Value".
    /// Repeatable. Requires --webhook-url.
    #[arg(long = "webhook-header", requires = "webhook_url")]
    webhook_headers: Vec<String>,

    /// Bearer token sent as `Authorization: Bearer <token>` on webhook POSTs.
    /// Requires --webhook-url.
    #[arg(long, requires = "webhook_url")]
    webhook_bearer_token: Option<String>,

    /// Request timeout (seconds) for webhook POSTs.
    #[arg(long, default_value_t = 10, requires = "webhook_url")]
    webhook_timeout_secs: u64,

    /// Maximum retries for a failing webhook POST (exponential backoff).
    #[arg(long, default_value_t = 5, requires = "webhook_url")]
    webhook_max_retries: u32,

    // ============================================================
    // Event Store Configuration (event-first mode)
    // ============================================================
    /// Event store backend: local (default), s3, or rest.
    /// - local: local filesystem + SQLite catalog (single-node dev)
    /// - s3: S3 data files + local SQLite catalog (catalog per-node, needs shared
    ///   catalog file for multi-node)
    /// - rest: REST catalog (Lakekeeper etc.) + S3 data files (multi-node shared,
    ///   recommended for production)
    #[arg(long, default_value = "local")]
    event_store_backend: String,

    /// REST catalog endpoint (e.g. "http://localhost:8181/catalog").
    /// Required when --event-store-backend=rest.
    #[arg(long)]
    event_store_rest_uri: Option<String>,

    /// REST catalog warehouse name/identifier (e.g. "nexora").
    /// Required when --event-store-backend=rest.
    #[arg(long)]
    event_store_rest_warehouse: Option<String>,

    /// Event store data directory for local backend.
    #[arg(long, default_value = "./nexora-data/events")]
    event_store_dir: PathBuf,

    /// S3 endpoint for event store (e.g. "http://localhost:9000" for MinIO).
    /// Fallback chain: this flag → --s3-endpoint → AWS_ENDPOINT_URL env var.
    #[arg(long)]
    event_store_s3_endpoint: Option<String>,

    /// S3 bucket for event store.
    /// Fallback chain: this flag → --s3-bucket.
    #[arg(long)]
    event_store_s3_bucket: Option<String>,

    /// S3 region for event store.
    #[arg(long, default_value = "us-east-1")]
    event_store_s3_region: String,

    /// S3 access key for event store.
    /// Fallback chain: this flag → --s3-access-key → AWS_ACCESS_KEY_ID env var.
    #[arg(long)]
    event_store_s3_access_key: Option<String>,

    /// S3 secret key for event store.
    /// Fallback chain: this flag → --s3-secret-key → AWS_SECRET_ACCESS_KEY env var.
    #[arg(long)]
    event_store_s3_secret_key: Option<String>,

    /// S3 path prefix for event store (e.g. "prod" or "staging").
    #[arg(long)]
    event_store_s3_prefix: Option<String>,

    /// Use path-style S3 addressing for event store (required for MinIO).
    #[arg(long)]
    event_store_s3_path_style: bool,

    /// Local path for event store catalog database.
    /// Used even in S3 mode (catalog is local, data is in S3).
    #[arg(long, default_value = "./nexora-data/event_catalog.db")]
    event_store_catalog_path: PathBuf,
}

/// Parse one `--peer` value into a [`PeerConfig`].
///
/// Accepts two forms:
/// - Preferred: `node_id@graph_addr@heartbeat_addr` (each addr is `host:port`).
///   `@` never appears inside a socket address, so this split is unambiguous.
/// - Legacy: `node_id:graph_addr:heartbeat_addr`. Because `host:port` addresses
///   contain a colon, the two addresses are the trailing four colon-separated
///   tokens (`host:port:host:port`); we reject anything that doesn't have exactly
///   that shape rather than silently dropping a port.
///
/// Returns `None` (with a warning) on malformed input.
fn parse_peer(p: &str) -> Option<nexora_zenoh::PeerConfig> {
    use nexora_zenoh::PeerConfig;

    // Preferred '@'-delimited form.
    if p.contains('@') {
        let parts: Vec<&str> = p.split('@').collect();
        if parts.len() == 3 && !parts.iter().any(|s| s.is_empty()) {
            return Some(PeerConfig {
                node_id: parts[0].to_string(),
                graph_addr: parts[1].to_string(),
                heartbeat_addr: parts[2].to_string(),
            });
        }
        tracing::warn!("Invalid peer format (expected node_id@graph_addr@heartbeat_addr): {p}");
        return None;
    }

    // Legacy colon form: node_id:host:port:host:port (5 tokens).
    let tokens: Vec<&str> = p.split(':').collect();
    if tokens.len() == 5 {
        return Some(PeerConfig {
            node_id: tokens[0].to_string(),
            graph_addr: format!("{}:{}", tokens[1], tokens[2]),
            heartbeat_addr: format!("{}:{}", tokens[3], tokens[4]),
        });
    }

    tracing::warn!(
        "Invalid peer format: {p}. Use node_id@graph_addr@heartbeat_addr \
         (each addr host:port), e.g. node-2@127.0.0.1:7110@127.0.0.1:7111"
    );
    None
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.num_shards == 0 {
        anyhow::bail!("--num-shards must be greater than zero");
    }
    if cli.max_nodes_per_shard == 0 {
        anyhow::bail!("--max-nodes-per-shard must be greater than zero");
    }
    if !cli.no_wal && cli.no_rocksdb {
        anyhow::bail!("WAL requires RocksDB (use --no-wal if you want in-memory mode)");
    }

    // Initialize logging (and OTLP span export when built with --features otel).
    // The guard flushes buffered spans on drop, so it must live until shutdown.
    let _tracing_guard = telemetry::init(telemetry::TracingOptions {
        otlp_endpoint: cli.otlp_endpoint.as_deref(),
        service_name: &cli.otel_service_name,
    });

    // Persist the audit trail to a file, if configured. Errors here are fatal:
    // if the operator asked for a durable audit log, we should not run without it.
    if let Some(ref audit_path) = cli.audit_log_file {
        security::init_audit_sink(audit_path.clone())
            .map_err(|e| anyhow::anyhow!("failed to open audit log file {audit_path:?}: {e}"))?;
        tracing::info!("   Audit:  persisting to {}", audit_path.display());
    }

    // Resolve profile from --profile flag or derive from flags
    let resolved_profile = if let Some(ref p) = cli.profile {
        let profile: config::RunProfile = p.parse().map_err(|e: String| anyhow::anyhow!("{e}"))?;
        config::validate_profile(cli.no_rocksdb, cli.cluster, profile)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        profile
    } else if cli.cluster {
        config::RunProfile::Clustered
    } else if cli.no_rocksdb {
        config::RunProfile::LiteEphemeral
    } else {
        config::RunProfile::SingleDurable
    };

    let mode = match resolved_profile {
        config::RunProfile::LiteEphemeral => "lite (in-memory, ephemeral)",
        config::RunProfile::SingleDurable => "single-durable (RocksDB + WAL)",
        config::RunProfile::Clustered => "clustered (distributed)",
    };

    tracing::info!("🚀 Starting DeepStreaming...");
    tracing::info!("   Mode:   {mode}");
    tracing::info!("   Build:  {}", env!("CARGO_PKG_VERSION"));

    // Load configuration file (optional)
    let config_file = config_loader::load_config(cli.config.as_deref())
        .context("Failed to load configuration file")?;

    // Resolve RocksDB path: CLI arg > config file > default
    let rocksdb_path = if cli.rocksdb_path.is_some() {
        cli.rocksdb_path.clone().unwrap()
    } else {
        config_file.storage.as_ref()
            .and_then(|s| s.rocksdb.as_ref())
            .map(|r| PathBuf::from(&r.path))
            .unwrap_or_else(|| PathBuf::from("./nexora-data"))
    };

    tracing::info!("RocksDB path resolved to: {}", rocksdb_path.display());

    // Graph configuration
    let graph_config = GraphServiceConfig {
        num_shards: cli.num_shards,
        max_nodes_per_shard: cli.max_nodes_per_shard,
        node_channel_size: 64,
    };

    // Create metrics registry (shared between SQ callback and HTTP handlers)
    let metrics_state = metrics::Metrics::new();

    // A1: build ONE control-plane store shared by all metadata domains (shard
    // map, MV defs, SQ defs). One durable backend, one fsync policy, one restore
    // path — and the single apply target A2's consensus will write through. With
    // --no-rocksdb it's in-memory (ephemeral, matching the mode's semantics).
    use nexora_core::control_plane_store::{
        ControlPlaneStore, InMemoryControlPlaneStore, RocksDbControlPlaneStore,
    };
    let control_plane_store: Arc<dyn ControlPlaneStore> = if cli.no_rocksdb {
        Arc::new(InMemoryControlPlaneStore::new())
    } else {
        let cp_path = rocksdb_path.join("control_plane");
        match RocksDbControlPlaneStore::open(&cp_path) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                tracing::warn!(
                    "Failed to open control-plane store at {}: {e}; falling back to in-memory \
                     (metadata will NOT survive restart)",
                    cp_path.display()
                );
                Arc::new(InMemoryControlPlaneStore::new())
            }
        }
    };

    // Create MaterializedViewManager over the shared store (BEFORE Standing Query
    // manager). Loads any persisted view definitions + rows on open.
    use nexora_core::materialized_view::MaterializedViewManager;
    let mv_manager = Arc::new(
        MaterializedViewManager::with_store(control_plane_store.clone()).unwrap_or_else(|e| {
            tracing::warn!(
                "Failed to create MV manager over control-plane store: {}, falling back to in-memory",
                e
            );
            MaterializedViewManager::new()
        }),
    );

    // Create OntologyManager over the shared store (stage 6). Restores any
    // persisted domain-package definitions on open.
    use nexora_core::ontology_manager::OntologyManager;
    let ontology_manager = Arc::new(
        match OntologyManager::with_store(control_plane_store.clone()).await {
            Ok(mgr) => mgr,
            Err(e) => {
                tracing::warn!(
                    "Failed to create ontology manager over control-plane store: {}, falling back to in-memory",
                    e
                );
                OntologyManager::new()
            }
        },
    );

    // Event-first: build the shared EventLogStore + TopicRouter singletons (one
    // per process, shared across every ingestion source — not re-created per
    // source as before). The store uses the real `--rocksdb-path` (was hard-coded
    // to ./nexora-data). Persisted ontologies restored above are replayed here so
    // their event tables exist and their topics route to Both on restart.
    #[cfg(feature = "event-first")]
    let (event_store, event_router, refresh_scheduler) = {
        use nexora_eventlog::{EventLogStore, RefreshScheduler, StorageConfig, TopicRouter};

        // Unified S3 connection resolution (step 1: config unification).
        //
        // Each S3 connection field resolves through a fallback chain so operators
        // configure credentials ONCE:
        //   1. event-store-specific flag  (--event-store-s3-*)   — highest priority
        //   2. shared tiered-storage flag (--s3-*)               — reused if present
        //   3. AWS_* environment variable                        — standard fallback
        // This keeps every existing invocation working (explicit event-store flags
        // still win) while letting a single --s3-* / AWS_* set feed both the tiered
        // store and the event store.
        let es_endpoint = cli
            .event_store_s3_endpoint
            .clone()
            .or_else(|| cli.s3_endpoint.clone())
            .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok());
        let es_bucket = cli
            .event_store_s3_bucket
            .clone()
            .or_else(|| cli.s3_bucket.clone());
        // region always has a default on both flags; prefer the event-store one only
        // if the user overrode it, else fall back to the shared --s3-region.
        let es_region = if cli.event_store_s3_region != "us-east-1" {
            cli.event_store_s3_region.clone()
        } else if cli.s3_region != "us-east-1" {
            cli.s3_region.clone()
        } else {
            "us-east-1".to_string()
        };
        let es_access_key = cli
            .event_store_s3_access_key
            .clone()
            .or_else(|| cli.s3_access_key.clone())
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok());
        let es_secret_key = cli
            .event_store_s3_secret_key
            .clone()
            .or_else(|| cli.s3_secret_key.clone())
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok());
        // path-style: true if EITHER flag set it (both default false).
        let es_path_style = cli.event_store_s3_path_style || cli.s3_path_style;

        // Build StorageConfig based on --event-store-backend
        let storage_config = if cli.event_store_backend == "rest" {
            // REST catalog mode (Lakekeeper etc.): shared metadata across nodes.
            // Still needs S3 connection info for the client's local FileIO.
            let uri = cli.event_store_rest_uri.clone().ok_or_else(|| {
                anyhow::anyhow!("--event-store-rest-uri required when --event-store-backend=rest")
            })?;
            let warehouse = cli.event_store_rest_warehouse.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "--event-store-rest-warehouse required when --event-store-backend=rest"
                )
            })?;
            let endpoint = es_endpoint.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 endpoint required for rest backend (set --event-store-s3-endpoint, --s3-endpoint, or AWS_ENDPOINT_URL)")
            })?;
            let access_key = es_access_key.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 access key required (set --event-store-s3-access-key, --s3-access-key, or AWS_ACCESS_KEY_ID)")
            })?;
            let secret_key = es_secret_key.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 secret key required (set --event-store-s3-secret-key, --s3-secret-key, or AWS_SECRET_ACCESS_KEY)")
            })?;

            tracing::info!(
                "Event store: REST catalog mode (uri={}, warehouse={}, s3_endpoint={})",
                uri,
                warehouse,
                endpoint
            );

            StorageConfig::rest(
                uri,
                warehouse,
                endpoint,
                es_region.clone(),
                access_key,
                secret_key,
                es_path_style,
            )
        } else if cli.event_store_backend == "s3" {
            // S3 mode: validate required parameters (via unified resolution)
            let endpoint = es_endpoint.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 endpoint required for s3 backend (set --event-store-s3-endpoint, --s3-endpoint, or AWS_ENDPOINT_URL)")
            })?;
            let bucket = es_bucket.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 bucket required for s3 backend (set --event-store-s3-bucket or --s3-bucket)")
            })?;
            let access_key = es_access_key.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 access key required (set --event-store-s3-access-key, --s3-access-key, or AWS_ACCESS_KEY_ID)")
            })?;
            let secret_key = es_secret_key.clone().ok_or_else(|| {
                anyhow::anyhow!("S3 secret key required (set --event-store-s3-secret-key, --s3-secret-key, or AWS_SECRET_ACCESS_KEY)")
            })?;

            tracing::info!(
                "Event store: S3 mode (endpoint={}, bucket={}, path_style={})",
                endpoint,
                bucket,
                es_path_style
            );

            StorageConfig::s3(
                endpoint,
                bucket,
                es_region.clone(),
                access_key,
                secret_key,
                cli.event_store_s3_prefix.clone(),
                es_path_style,
                cli.event_store_catalog_path.to_str().unwrap(),
            )
        } else {
            // Local FS mode (default)
            tracing::info!(
                "Event store: Local FS mode (dir={})",
                cli.event_store_dir.display()
            );
            StorageConfig::local_fs(cli.event_store_dir.to_str().unwrap())
        };

        match EventLogStore::new_with_config(storage_config).await {
            Ok(store) => {
                let store = Arc::new(store);
                let router = Arc::new(TopicRouter::all_graph());
                let scheduler = Arc::new(RefreshScheduler::new(store.clone()));
                // Replay restored ontologies: ensure each mapped topic's table
                // exists, route it to Both, and schedule its materialized views.
                for domain in ontology_manager.list().await {
                    if let Some(pkg) = ontology_manager.get(&domain).await {
                        for m in &pkg.mappings {
                            if let Err(e) = store.ensure_table_from_domain(&m.source, &pkg).await {
                                tracing::warn!(
                                    "Failed to ensure event table '{}' for domain '{}' on restore: {}",
                                    m.source, domain, e
                                );
                            }
                        }
                        router.apply_domain_package(&pkg);
                        handlers::ontology::schedule_domain_views(&scheduler, &pkg).await;
                    }
                }
                tracing::info!("EventLogStore initialized (event-first mode enabled)");
                (Some(store), Some(router), Some(scheduler))
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create EventLogStore: {}, event-first disabled (graph-only)",
                    e
                );
                (None, None, None)
            }
        }
    };

    // Event-first: opt-in background retention sweep over all event tables. Runs
    // only when `--retention-secs` is set and the shared store initialized. Note
    // (iceberg 0.9.1): identify_expirable_snapshots reports candidates and warns;
    // actual snapshot deletion awaits the iceberg-rust expire API (0.10+).
    #[cfg(feature = "event-first")]
    if let (Some(secs), Some(store)) = (cli.retention_secs, event_store.clone()) {
        use nexora_eventlog::{RetentionManager, RetentionPolicy};
        let policy = RetentionPolicy::new(cli.retention_days, 1);
        let mgr = RetentionManager::new(store);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(secs));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                match mgr.expire_all(&policy).await {
                    Ok(n) => {
                        tracing::info!("Retention sweep: {} expirable snapshot(s) identified", n)
                    }
                    Err(e) => tracing::warn!("Retention sweep failed: {}", e),
                }
            }
        });
        tracing::info!(
            "Event-table retention sweep enabled (every {}s, retain {} days)",
            secs,
            cli.retention_days
        );
    }

    // Create Standing Query to Materialized View bridge
    let sq_mv_bridge = Arc::new(sq_mv_bridge::SQMaterializedViewBridge::new(
        mv_manager.clone(),
    ));

    // Create Standing Query manager (BEFORE graph, so callback can reference it)
    let sq_manager = Arc::new(StandingQueryManager::new(1024));
    let sq_cb = make_sq_callback(&sq_manager, &metrics_state);
    // Sealing pipeline channel (P2-B): the mutation callback forwards SealEvents
    // here; a background task (spawned after the tiered store is built) drains
    // and seals them into fragments. Only wired when tiered storage is enabled.
    let seal_tx = if cli.storage_backend != "memory" {
        Some(())
    } else {
        None
    };
    let (seal_sender, seal_receiver) =
        tokio::sync::mpsc::unbounded_channel::<nexora_fragment::SealEvent>();
    let sq_mutation_cb = make_sq_mutation_callback(
        &sq_manager,
        &metrics_state,
        seal_tx.map(|_| seal_sender.clone()),
    );

    // GAP-3: Event-driven incremental MV refresh. The SQ manager broadcasts every
    // StandingQueryResult on its result channel; subscribe here and forward each
    // result to the bridge so matched/unmatched nodes update their materialized
    // views immediately (no polling, no scheduler needed for SQ-backed views).
    {
        let mut rx = sq_manager.subscribe();
        let bridge_for_mv = sq_mv_bridge.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(result) => bridge_for_mv.on_sq_result(&result).await,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            skipped = n,
                            "MV bridge lagged behind SQ result stream; some incremental updates were dropped"
                        );
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::debug!("SQ result stream closed; MV bridge subscriber exiting");
                        break;
                    }
                }
            }
        });
    }

    // Create persistence backend
    // Resolve WAL encryption key if --encrypt-wal is set
    let wal_encryption_key: Option<[u8; 32]> = if cli.encrypt_wal {
        // Priority: CLI arg --encryption-key > CLI arg --encryption-key-file > env var
        let hex_key = if let Some(ref hex) = cli.encryption_key {
            Some(hex.clone())
        } else if let Some(ref key_file) = cli.encryption_key_file {
            match std::fs::read_to_string(key_file) {
                Ok(content) => Some(content.trim().to_string()),
                Err(e) => anyhow::bail!(
                    "Failed to read encryption key file {}: {e}",
                    key_file.display()
                ),
            }
        } else {
            std::env::var("NEXORA_ENCRYPTION_KEY").ok()
        };
        match hex_key {
            Some(hex) => {
                let bytes = hex::decode(hex.trim())
                    .map_err(|e| anyhow::anyhow!("Invalid encryption key (not valid hex): {e}"))?;
                if bytes.len() != 32 {
                    anyhow::bail!(
                        "Encryption key must be 32 bytes (64 hex chars), got {} bytes",
                        bytes.len()
                    );
                }
                let mut key = [0u8; 32];
                key.copy_from_slice(&bytes);
                tracing::info!("   WAL encryption: enabled (AES-256-GCM)");
                Some(key)
            }
            None => {
                anyhow::bail!(
                    "--encrypt-wal requires an encryption key. Provide it via --encryption-key, --encryption-key-file, or the NEXORA_ENCRYPTION_KEY environment variable."
                );
            }
        }
    } else {
        None
    };

    // Resolve the WAL sync policy: CLI flag > nexora.toml [storage.wal] > default.
    // Previously the TOML fields were dead (never parsed) and the engine always
    // used its hardcoded group-commit default; this makes both sources effective.
    let wal_sync_policy = {
        let toml_cfg = config::load_toml_config(None);
        let toml_wal = toml_cfg
            .as_ref()
            .and_then(|c| c.storage.as_ref())
            .and_then(|s| s.wal.as_ref());
        let policy_str = cli
            .wal_sync_policy
            .clone()
            .or_else(|| toml_wal.map(|w| w.sync_policy.clone()))
            .unwrap_or_else(|| "group".to_string());
        let interval = cli
            .wal_sync_interval
            .or_else(|| toml_wal.map(|w| w.sync_interval))
            .unwrap_or(1000);
        config::parse_wal_sync_policy(&policy_str, interval)
            .map_err(|e| anyhow::anyhow!("invalid WAL sync policy: {e}"))?
    };

    let graph: Arc<GraphService> = if !cli.no_rocksdb {
        let path = &rocksdb_path;
        tracing::info!("   RocksDB: {}", path.display());
        let persistor = Arc::new(nexora_persistor_rocksdb::RocksDbPersistor::open(path)?);

        if !cli.no_wal {
            let wal_path = &cli.wal_dir;
            tracing::info!(
                "   WAL:     {} (sync: {:?})",
                wal_path.display(),
                wal_sync_policy
            );
            let graph = GraphService::new_with_wal_policy(
                graph_config.clone(),
                persistor,
                wal_path.clone(),
                wal_encryption_key,
                wal_sync_policy,
            )?;
            // Replay WAL for crash recovery
            let replayed = graph.replay_all_wals().await?;
            if replayed > 0 {
                tracing::info!("   WAL replay: {} records recovered", replayed);
            }
            let graph = graph
                .with_sq_callback(sq_cb.clone())
                .with_mutation_callback(sq_mutation_cb.clone());
            Arc::new(graph)
        } else {
            tracing::info!("   WAL:     disabled");
            Arc::new(
                GraphService::new(graph_config, persistor)
                    .with_sq_callback(sq_cb.clone())
                    .with_mutation_callback(sq_mutation_cb.clone()),
            )
        }
    } else {
        tracing::info!("   Storage: in-memory (data lost on restart)");
        let persistor = Arc::new(InMemoryPersistor::new());
        let graph = GraphService::new(graph_config, persistor);
        let graph = graph
            .with_sq_callback(sq_cb.clone())
            .with_mutation_callback(sq_mutation_cb.clone());
        Arc::new(graph)
    };

    // Set graph reference for SQ edge traversal
    sq_manager.set_graph(graph.clone()).await;

    // A1: wire SQ definition persistence through the SAME control-plane store as
    // the shard map and MV definitions, then restore any SQ definitions + match
    // states persisted by a prior run. This makes SQ metadata survive a restart
    // (previously it was in-memory only and lost). With --no-rocksdb the store is
    // in-memory, so restore is a no-op across process restarts — durability
    // requires a durable backend.
    sq_manager.set_store(control_plane_store.clone()).await;
    match sq_manager.restore().await {
        Ok(0) => {}
        Ok(n) => tracing::info!("   Standing Queries: restored {n} definition(s) from persistence"),
        Err(e) => tracing::warn!("   Standing Queries: restore failed: {e}"),
    }

    // ============================================================
    // SinkRegistry: centralized management of all output sinks
    // ============================================================
    let sink_registry = Arc::new(nexora_output::SinkRegistry::new());

    // Register console output sink (always active)
    {
        let console = nexora_output::ConsoleOutput::new("console");
        sink_registry.register(Arc::new(console)).await;
        tracing::info!("   Output: console (SQ results → stdout)");
    }

    // Register webhook output sink when --webhook-url is set. It joins the same
    // registry as the console sink, so every SQ result is fanned out to it via
    // the single subscriber below (no separate broadcast subscription — that
    // would double-deliver).
    if let Some(ref webhook_url) = cli.webhook_url {
        let headers = cli
            .webhook_headers
            .iter()
            .filter_map(|h| {
                h.split_once(':')
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            })
            .collect::<Vec<_>>();
        let webhook_config = nexora_output::WebhookConfig {
            url: webhook_url.clone(),
            bearer_token: cli.webhook_bearer_token.clone(),
            timeout_secs: cli.webhook_timeout_secs,
            max_retries: cli.webhook_max_retries,
            headers,
            ..Default::default()
        };
        let webhook = nexora_output::WebhookOutput::with_config("webhook", webhook_config);
        sink_registry.register(Arc::new(webhook)).await;
        tracing::info!("   Output: webhook (SQ results → {webhook_url})");
    }

    // Wire SQ results → SinkRegistry (unified routing to all sinks)
    {
        let mut sq_rx = sq_manager.subscribe();
        let registry = sink_registry.clone();
        tokio::spawn(async move {
            while let Ok(result) = sq_rx.recv().await {
                registry.route(&result).await;
            }
        });
    }

    // ============================================================
    // Tiered storage backend (nexora-storage)
    // ============================================================
    let tiered_store: Option<Arc<nexora_storage::TieredStore>> = match cli.storage_backend.as_str()
    {
        "s3" => {
            let bucket = cli.s3_bucket.clone().ok_or_else(|| {
                anyhow::anyhow!("--s3-bucket is required when --storage-backend=s3")
            })?;
            // Resolve endpoint and credentials from flags, falling back to the
            // standard AWS environment variables.
            let endpoint = cli
                .s3_endpoint
                .clone()
                .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
                .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", cli.s3_region));
            let access_key = cli
                .s3_access_key
                .clone()
                .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
                .unwrap_or_default();
            let secret_key = cli
                .s3_secret_key
                .clone()
                .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
                .unwrap_or_default();
            if access_key.is_empty() || secret_key.is_empty() {
                tracing::warn!(
                    "   Storage: S3 credentials are empty (no --s3-access-key/--s3-secret-key \
                     and no AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars). \
                     Requests will be unsigned and will likely fail against a real bucket."
                );
            }
            let s3_config = nexora_storage::S3Config {
                endpoint,
                bucket: bucket.clone(),
                region: cli.s3_region.clone(),
                access_key,
                secret_key,
                prefix: None,
                path_style: cli.s3_path_style,
            };
            // Warm + cold both live in S3, under distinct key prefixes so the
            // cold tier is a separate namespace within the same bucket. Hot is
            // DURABLE local SSD (not MemoryStorage) so hot-tier data survives a
            // restart — losing it on every restart would defeat the point of a
            // persistence backend.
            let warm_cfg = nexora_storage::S3Config {
                prefix: Some("warm".to_string()),
                ..s3_config.clone()
            };
            let cold_cfg = nexora_storage::S3Config {
                prefix: Some("cold".to_string()),
                ..s3_config
            };
            let hot = Arc::new(nexora_storage::LocalStorage::new(
                rocksdb_path.join("hot"),
                nexora_storage::StorageTier::Hot,
            ));
            let warm: Arc<dyn nexora_storage::StorageBackend> =
                Arc::new(nexora_storage::S3Storage::new(warm_cfg));
            let cold: Arc<dyn nexora_storage::StorageBackend> =
                Arc::new(nexora_storage::S3Storage::new(cold_cfg));

            let cold_after_ms = cli.s3_cold_after_days * 86_400_000; // days -> ms
            let rules = vec![nexora_storage::LifecycleRule {
                from: nexora_storage::StorageTier::Hot,
                to: nexora_storage::StorageTier::Warm,
                age_ms: cold_after_ms,
            }];
            let store = nexora_storage::TieredStore::with_rules(hot, warm, cold, rules);
            tracing::info!(
                "   Storage: s3 (bucket={}, region={}, path_style={}, cold_after={}d)",
                bucket,
                cli.s3_region,
                cli.s3_path_style,
                cli.s3_cold_after_days
            );
            Some(Arc::new(store))
        }
        "local" => {
            let hot = Arc::new(nexora_storage::LocalStorage::new(
                rocksdb_path.join("hot"),
                nexora_storage::StorageTier::Hot,
            ));
            let warm = Arc::new(nexora_storage::LocalStorage::new(
                rocksdb_path.join("warm"),
                nexora_storage::StorageTier::Warm,
            ));
            let cold = Arc::new(nexora_storage::LocalStorage::new(
                rocksdb_path.join("cold"),
                nexora_storage::StorageTier::Cold,
            ));
            let store = nexora_storage::TieredStore::new(hot, warm, cold);
            tracing::info!(
                "   Storage: local (tiered under {})",
                rocksdb_path.display()
            );
            Some(Arc::new(store))
        }
        _ => {
            // "memory" — default, no tiered store (current behavior)
            None
        }
    };

    // Fragment store for time-travel queries (created when tiered storage is enabled)
    let mut fragment_store: Option<Arc<nexora_fragment::TieredFragmentStore>> = None;

    // Sealing pipeline (P2-B): if tiered storage is enabled, drain SealEvents
    // from the mutation callback into a FragmentSealer, sealing a time-windowed
    // fragment every `seal_interval` (and running the tiered lifecycle so aged
    // fragments sink to warm/cold). Decoupled from the hot write path — the
    // callback only does a non-blocking channel send.
    if let Some(ts) = tiered_store.clone() {
        use nexora_fragment::{FragmentSealer, FragmentStore, TieredFragmentStore};
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let registry = Arc::new(FragmentStore::new(
            rocksdb_path.join("fragments"),
            "graph",
        ));
        let frag_store = Arc::new(TieredFragmentStore::new(registry, ts, "graph"));
        fragment_store = Some(frag_store.clone());
        let sealer = Arc::new(FragmentSealer::new(frag_store.clone(), "graph", now_us));
        let seal_interval = std::time::Duration::from_secs(cli.seal_interval_secs);
        let idle_ttl = if cli.idle_evict_secs > 0 {
            Some(std::time::Duration::from_secs(cli.idle_evict_secs))
        } else {
            None
        };
        let graph_for_evict = graph.clone();

        let mut rx = seal_receiver;
        let sealer_drain = sealer.clone();
        // Drain task: fold every SealEvent into the current window.
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                sealer_drain.record(ev).await;
            }
        });
        // Seal task: on each tick, close the window into a fragment and run the
        // tiered lifecycle (hot→warm→cold by age), then evict idle nodes (if TTL
        // is configured) so sealed-and-aged data is released from hot memory.
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(seal_interval);
            ticker.tick().await; // skip immediate first tick
            loop {
                ticker.tick().await;
                let end_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0);
                match sealer.seal(end_us).await {
                    Ok(Some(id)) => tracing::debug!("sealed fragment {id}"),
                    Ok(None) => {}
                    Err(e) => tracing::warn!("fragment seal failed: {e}"),
                }
                if let Err(e) = frag_store.run_lifecycle().await {
                    tracing::warn!("fragment lifecycle failed: {e}");
                }
                // Evict idle nodes: sealed data is durably in fragments, so nodes
                // no one touches can be slept (state persisted, memory released).
                if let Some(ttl) = idle_ttl {
                    let (evicted, retained) = graph_for_evict.evict_idle_nodes(ttl).await;
                    if evicted > 0 {
                        tracing::info!("idle sweep: {evicted} nodes evicted, {retained} retained");
                    }
                }
            }
        });
        tracing::info!(
            "   Fragments: sealing pipeline active (interval={}s)",
            cli.seal_interval_secs
        );
    } else {
        // Tiered storage disabled → drop the receiver so the sender is a no-op.
        drop(seal_receiver);
    }

    tracing::info!("   Graph:  {} shards", graph.shard_count());
    let graph_for_shutdown = graph.clone();

    // Shutdown signal for WebSocket connections
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let shutdown_ws = shutdown_notify.clone();

    // Resolve auth secret
    let auth_secret = cli.auth_secret.clone()
        .or_else(|| std::env::var("NEXORA_AUTH_SECRET").ok())
        .unwrap_or_else(|| {
            tracing::warn!("No auth secret provided, using default dev secret. Set --auth-secret or NEXORA_AUTH_SECRET for production.");
            "nexora-dev-secret-change-me".to_string()
        });

    // Effective auth gate: --allow-unauthenticated overrides the (default-true)
    // --require-auth. Route construction below must key off THIS, not the raw
    // cli.require_auth — otherwise --allow-unauthenticated is silently ignored
    // and the auth middleware stays attached (every admin call 401s).
    let effective_require_auth = cli.require_auth && !cli.allow_unauthenticated;

    // Print prominent security warnings for insecure configurations
    print_security_warnings(&cli, &auth_secret);

    // ============================================================
    // Raft consensus integration (opt-in via --raft-port)
    // ============================================================
    let raft_handler: Option<Arc<raft_handler::RaftHandler>> =
        if let Some(raft_port) = cli.raft_port {
            if !cli.cluster {
                anyhow::bail!("--raft-port requires --cluster to be enabled");
            }
            let node_id = cli
                .node_id
                .clone()
                .unwrap_or_else(|| format!("node-{}", cli.port));

            let raft_peers = cli.raft_peers.clone();
            if raft_peers.is_empty() {
                tracing::warn!(
                "--raft-port set but no --raft-peer specified; running as single-node Raft cluster"
            );
            }

            let total_nodes = raft_peers.len() + 1; // self + peers
            let raft_config = raft_handler::RaftHandlerConfig {
                raft_port,
                raft_peers: raft_peers.clone(),
                quorum_size: (total_nodes / 2) + 1, // majority
                total_nodes,
                rpc_timeout: std::time::Duration::from_secs(5),
                max_batch_size: 100,
                current_term: 1,
                node_id: node_id.clone(),
                shard_id: 0,
                state_dir: Some(rocksdb_path.join("raft_state")),
                // is_leader gates the replication loop (a non-leader skips it, so
                // followers never push stale AppendEntries). It is hardcoded true
                // here as a deliberate placeholder for this EXPERIMENTAL path, not
                // an unfinished gap:
                //   1. This --raft-port path is a log-shipping skeleton, NOT real
                //      consensus — it carries empty payloads and replicates no real
                //      data (see the warning logged just below, and
                //      crates/nexora-raft/src/lib.rs:21). Precise leader gating has
                //      no data-correctness payoff until it does.
                //   2. The real ownership signal (ShardMap::is_owner / the openraft
                //      control plane) does not exist at this point — ShardMap is
                //      built ~40 lines below (ClusterManager construction). Wiring a
                //      truthful value needs either an init reorder or a dynamic
                //      Arc<dyn Fn()->bool> owner-check threaded from ClusterManager.
                // When this path graduates to replicating real data, replace this
                // with that dynamic owner check. See docs/production-planning/HA_ROADMAP.md.
                is_leader: true,
            };

            // Ensure the Raft state dir exists so the applied-index watermark
            // can be persisted (see RaftHandler::new / persist_applied_index).
            let raft_state_dir = rocksdb_path.join("raft_state");
            if let Err(e) = std::fs::create_dir_all(&raft_state_dir) {
                tracing::warn!(error = %e, path = %raft_state_dir.display(),
                    "failed to create Raft state dir; applied_index will not persist");
            }

            let handler = raft_handler::RaftHandler::new(raft_config, graph.clone());
            handler.start().await?;

            let handler = Arc::new(handler);
            tracing::info!(
                "   Raft:   enabled (port={}, peers={:?}, quorum={})",
                raft_port,
                raft_peers,
                (total_nodes / 2) + 1
            );

            // The "Raft" path is NOT full Raft consensus: there is no leader
            // election, log entries carry empty payloads, and the term never
            // advances (see crates/nexora-raft/src/lib.rs and write_through.rs).
            // It does not currently replicate real data or provide consensus.
            tracing::warn!(
                "⚠️  --raft-port ENABLES AN EXPERIMENTAL LOG-SHIPPING SKELETON, NOT \
                 RAFT CONSENSUS — no leader election, no real log replication. It \
                 does NOT make writes durable across nodes. See \
                 docs/production-planning/HA_ROADMAP.md."
            );
            Some(handler)
        } else {
            None
        };

    // Start cluster mode if enabled (must be before AppState since it uses graph.clone())
    let cluster_manager = if cli.cluster {
        use nexora_zenoh::{ClusterConfig, ClusterManager, GraphServiceAdapter, PeerConfig};
        use std::time::Duration;

        let cluster_config = if let Some(ref config_path) = cli.cluster_config {
            // Load from YAML file
            match ClusterConfig::from_file(config_path) {
                Ok(config) => {
                    tracing::info!(
                        "   Cluster: loaded config from {} (node={}, rf={}, shards={})",
                        config_path.display(),
                        config.node_id,
                        config.replication_factor,
                        config.total_shards
                    );
                    config
                }
                Err(e) => {
                    anyhow::bail!(
                        "Failed to load cluster config from {}: {}",
                        config_path.display(),
                        e
                    );
                }
            }
        } else {
            // Build from CLI arguments (legacy path)
            let node_id = cli
                .node_id
                .clone()
                .unwrap_or_else(|| format!("node-{}", cli.port));
            let listen_addr = cli
                .cluster_listen_addr
                .clone()
                .unwrap_or_else(|| format!("0.0.0.0:{}", cli.port + 1000));
            let heartbeat_addr = cli
                .cluster_heartbeat_addr
                .clone()
                .unwrap_or_else(|| format!("0.0.0.0:{}", cli.port + 1001));

            // Parse peers. Preferred form uses '@' between the three fields:
            //   "node_id@graph_addr@heartbeat_addr"  (each addr is host:port)
            // The legacy colon form is still accepted:
            //   "node_id:graph_addr:heartbeat_addr"
            // The legacy split MUST NOT use splitn(3, ':'), because a host:port
            // addr contains a colon — that put the port of graph_addr into
            // heartbeat_addr and left graph_addr without a port ("127.0.0.1"),
            // producing "invalid socket address" at connect time. Instead we take
            // the first segment as node_id and re-join the middle back into two
            // host:port pairs from the remaining colon-separated tokens.
            let peers: Vec<PeerConfig> = cli.peers.iter().filter_map(|p| parse_peer(p)).collect();

            // Durable replication log under the data dir (unless in-memory mode), so
            // incremental catch-up survives a node restart instead of falling back
            // to a full snapshot.
            let replication_log_dir = if cli.no_rocksdb {
                None
            } else {
                Some(rocksdb_path.join("replog"))
            };

            // A0: durable shard-map snapshot under the data dir (unless in-memory
            // mode), so runtime failover/rebalance ownership + epochs survive a
            // restart instead of reverting to the cold membership-derived map.
            let shard_map_dir = if cli.no_rocksdb {
                None
            } else {
                Some(rocksdb_path.join("shardmap"))
            };

            ClusterConfig {
                node_id: node_id.clone(),
                listen_addr,
                heartbeat_addr,
                total_shards: cli.num_shards,
                peers,
                heartbeat_interval: Duration::from_secs(2),
                failure_timeout: Duration::from_secs(10),
                replication_factor: cli.replication_factor,
                replication_log_dir,
                shard_map_dir,
                anti_entropy_interval: cli.anti_entropy_secs.map(Duration::from_secs),
            }
        };

        let node_id = cluster_config.node_id.clone();

        let mut cm = ClusterManager::new(cluster_config);
        // Share the cluster's epoch fence, replication log, AND catch-up barrier
        // with the adapter: the fence rejects a deposed owner's stale writes; the
        // log records admitted writes by seq so a lagging replica can catch up
        // incrementally; the barrier makes an owner refuse whole-graph reads
        // while it is still reconciling a promoted shard (avoids silent partial
        // results at the coordinator).
        let base_adapter = GraphServiceAdapter::with_fence_log_and_barrier(
            graph.clone(),
            cm.fence(),
            cm.replication_log(),
            cm.catch_up_barrier(),
        );
        // Event-first: give the graph handler a scanner (so peers can serve this
        // node's local event tables for cross-node event queries) and an ontology
        // applier (so a peer's ontology broadcast registers the schema here too).
        #[cfg(feature = "event-first")]
        let base_adapter = if let (Some(store), Some(router_es), Some(scheduler)) = (
            event_store.clone(),
            event_router.clone(),
            refresh_scheduler.clone(),
        ) {
            let applier = AppOntologyApplier {
                ontology_manager: ontology_manager.clone(),
                event_store: store.clone(),
                event_router: router_es,
                refresh_scheduler: scheduler,
            };
            base_adapter
                .with_event_scanner(Arc::new(EventStoreScanner { store }))
                .with_ontology_applier(Arc::new(applier))
        } else {
            base_adapter
        };
        let adapter = Arc::new(base_adapter);
        cm.start(adapter).await?;

        // Raft ontology consensus: spawn the activation drain task and reconcile
        // on startup (a follower caught up via snapshot has committed DomainDef
        // entries in the store but never saw per-entry activation events).
        #[cfg(feature = "event-first")]
        if let Some(rx) = cm.take_ontology_activation_rx() {
            // Startup reconcile: activate every committed ontology already in the
            // Raft state machine's store (covers snapshot-install catch-up).
            let reconcile_ontologies = cm.committed_ontologies();
            if !reconcile_ontologies.is_empty() {
                tracing::info!(
                    "Reconciling {} committed ontologies from Raft state machine",
                    reconcile_ontologies.len()
                );
                for (domain, pkg_json) in reconcile_ontologies {
                    if let Err(e) = reconcile_ontology(
                        &ontology_manager,
                        event_store.as_ref(),
                        event_router.as_ref(),
                        refresh_scheduler.as_ref(),
                        &domain,
                        &pkg_json,
                    )
                    .await
                    {
                        tracing::warn!(
                            "Startup reconcile: ontology '{}' activation failed: {}",
                            domain,
                            e
                        );
                    }
                }
            }

            // Spawn the activation drain task: consume committed DomainDef Put/Delete
            // events from the Raft state machine's apply callback and activate them
            // locally (create event tables, update router, schedule views / or remove).
            let ontology_mgr = ontology_manager.clone();
            let store_opt = event_store.clone();
            let router_opt = event_router.clone();
            let scheduler_opt = refresh_scheduler.clone();
            tokio::spawn(async move {
                drain_ontology_activations(rx, ontology_mgr, store_opt, router_opt, scheduler_opt)
                    .await;
            });
            tracing::info!("Ontology activation drain task spawned (Raft consensus mode)");
        }

        tracing::info!("   Cluster: node={node_id}, shards={}", cli.num_shards);

        // E6: Cluster mode now has the HA mechanisms wired into the serving path
        // — quorum replication (two-phase A1.1), W+R>N reads (A1.2), failover
        // catch-up (A1.3), WAL torn-write repair (A4), and state transfer (C1),
        // all covered by chaos tests. What remains before dropping the caveat is
        // a long-running multi-node soak (E2): the mechanisms are verified in
        // tests but not yet battle-tested under sustained real-world load. So the
        // banner is downgraded from "NO FAULT TOLERANCE" to a soak-pending caveat,
        // keeping the self-description honest in both directions.
        tracing::warn!(
            "⚠️  CLUSTER MODE: HA mechanisms are implemented and chaos-tested \
             (quorum replication RF>1, W+R>N reads, failover with catch-up, WAL \
             recovery, state transfer) but NOT YET validated by a long-running \
             multi-node soak. Treat as pre-production: run in \"tolerable downtime, \
             supervised, data backed up elsewhere\" scenarios until soak (E2) \
             completes. See docs/production-planning/ROADMAP_TO_PRODUCTION_LEADING_2026-07-18.md."
        );

        Some(Arc::new(cm))
    } else {
        None
    };

    // Create UDF manager
    let udf_manager = Arc::new(tokio::sync::Mutex::new(
        nexora_udf::manager::UdfManager::new(),
    ));

    // Cross-node router for AppState: only present in cluster mode. In single-node
    // mode this is None and every read/write goes straight to the local graph
    // (behaviour unchanged). Handlers route remote-owned shards through this.
    let app_router = cluster_manager.as_ref().map(|cm| cm.router_arc());
    // Quorum write replicator: only in cluster mode. After a local owner write,
    // the write path uses this to replicate to followers and wait for quorum.
    let app_replica_writer = cluster_manager.as_ref().map(|cm| cm.replica_writer_arc());
    // Catch-up write barrier: only in cluster mode. The write path consults it
    // to reject writes to a shard reconciling after a failover promotion.
    let app_catch_up_barrier = cluster_manager.as_ref().map(|cm| cm.catch_up_barrier());
    // C2: replication progress: only in cluster mode. The PG-wire read path uses
    // it to gate ReadConcern::Majority on replicas caught up to quorum, so a
    // failover read never serves data older than a quorum holds.
    let app_replication_progress = cluster_manager.as_ref().map(|cm| cm.replication_progress());

    // Prepare auth instance for AppState (needed for WebSocket token validation)
    let auth_for_state = if effective_require_auth {
        Some(Arc::new(crate::auth::Auth::new(&auth_secret)))
    } else {
        None
    };

    // ============================================================
    // Event Streams integration (opt-in via --enable-event-streaming)
    // ============================================================
    // The embedded/distributed process wrappers only exist under the `embedded`
    // feature. In `library` mode (`event-streaming` without `embedded`) those tuple
    // slots are unused, so alias them to `()` placeholders to keep the shared
    // type annotation valid in both configs.
    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    type RwEmbeddedInstance = nexora_risingwave::EmbeddedEventStreaming;
    #[cfg(all(feature = "event-streaming", not(feature = "embedded")))]
    type RwEmbeddedInstance = ();
    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    type RwDistributedInstance = nexora_risingwave::DistributedEmbeddedEventStreaming;
    #[cfg(all(feature = "event-streaming", not(feature = "embedded")))]
    type RwDistributedInstance = ();

    #[cfg(feature = "event-streaming")]
    let (event_streaming_module, embedded_event_streaming, distributed_event_streaming): (
        Option<Arc<nexora_risingwave::EventStreamingModule>>,
        Option<RwEmbeddedInstance>,
        Option<RwDistributedInstance>,
    ) = {
        // Merge config file and CLI args (CLI takes precedence)
        let rw_config = config_file.event_streaming.as_ref();
        let enabled = cli.enable_event_streaming || rw_config.map_or(false, |c| c.enabled);

        if enabled {
            // Phase 8: Check for cluster mode first
            #[cfg(feature = "embedded")]
            let cluster_mode =
                cli.event_streaming_cluster || rw_config.map_or(false, |c| c.cluster_mode);

            #[cfg(not(feature = "embedded"))]
            let cluster_mode = false;

            if cluster_mode {
                // Phase 8: Distributed embedded mode (3-node HA)
                #[cfg(feature = "embedded")]
                {
                    tracing::info!("   Event Streaming: starting distributed cluster (3 nodes)...");

                    let data_dir = rw_config
                        .and_then(|c| Some(std::path::PathBuf::from(&c.data_dir)))
                        .unwrap_or_else(|| rocksdb_path.join("event-streaming-cluster"));

                    let binary_path = rw_config
                        .and_then(|c| c.binary_path.as_ref().map(std::path::PathBuf::from));

                    let startup_timeout = rw_config.map_or(60, |c| c.startup_timeout_secs);
                    let shutdown_timeout = rw_config.map_or(30, |c| c.shutdown_timeout_secs);

                    // Build distributed config from TOML or defaults
                    let dist_config = if let Some(rw) = rw_config {
                        if let Some(meta_nodes_cfg) = &rw.meta_nodes {
                            // Use custom config from TOML
                            let meta_nodes = meta_nodes_cfg
                                .iter()
                                .map(|m| nexora_risingwave::MetaNodeConfig {
                                    node_id: m.node_id,
                                    listen_addr: m.listen_addr.clone(),
                                    advertise_addr: m.advertise_addr.clone(),
                                    dashboard_addr: m.dashboard_addr.clone(),
                                })
                                .collect();

                            let frontend = nexora_risingwave::FrontendNodeConfig {
                                listen_addr: rw.frontend_addr.clone(),
                            };

                            let compute_nodes = if let Some(compute_cfg) = &rw.compute_nodes {
                                compute_cfg
                                    .iter()
                                    .map(|c| nexora_risingwave::ComputeNodeConfig {
                                        listen_addr: c.listen_addr.clone(),
                                        parallelism: c.parallelism,
                                    })
                                    .collect()
                            } else {
                                vec![nexora_risingwave::ComputeNodeConfig {
                                    listen_addr: "127.0.0.1:5688".to_string(),
                                    parallelism: num_cpus::get(),
                                }]
                            };

                            nexora_risingwave::DistributedConfig {
                                binary_path: binary_path.clone(),
                                data_dir: data_dir.clone(),
                                meta_nodes,
                                frontend,
                                compute_nodes,
                                startup_timeout_secs: startup_timeout,
                                shutdown_timeout_secs: shutdown_timeout,
                            }
                        } else {
                            // Use defaults
                            let mut cfg = nexora_risingwave::DistributedConfig::default();
                            cfg.binary_path = binary_path.clone();
                            cfg.data_dir = data_dir.clone();
                            cfg.startup_timeout_secs = startup_timeout;
                            cfg.shutdown_timeout_secs = shutdown_timeout;
                            cfg
                        }
                    } else {
                        // No config file, use defaults
                        let mut cfg = nexora_risingwave::DistributedConfig::default();
                        cfg.binary_path = binary_path;
                        cfg.data_dir = data_dir;
                        cfg
                    };

                    match nexora_risingwave::DistributedEmbeddedEventStreaming::start(dist_config)
                        .await
                    {
                        Ok(instance) => {
                            tracing::info!("   Event Streaming: distributed cluster started");
                            (None, None, Some(instance))
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to start distributed event streams cluster: {}",
                                e
                            );
                            anyhow::bail!("Distributed event streams initialization failed: {}", e);
                        }
                    }
                }
                #[cfg(not(feature = "embedded"))]
                {
                    tracing::error!("Cluster mode requires 'embedded' feature");
                    anyhow::bail!("Cluster mode not available without embedded feature");
                }
            } else {
                // Phase 7.5: Embedded RisingWave support (single node)
                #[cfg(feature = "embedded")]
                let embedded_instance = {
                    let use_embedded =
                        cli.embedded_event_streaming || rw_config.map_or(false, |c| c.embedded);

                    if use_embedded {
                        tracing::info!("   Event Streaming: starting embedded process...");

                        // CLI args override config file
                        let meta_addr = cli
                            .event_streaming_meta_addr
                            .clone()
                            .or_else(|| rw_config.and_then(|c| Some(c.meta_addr.clone())))
                            .unwrap_or_else(|| "127.0.0.1:5690".to_string());
                        let frontend_addr = cli
                            .event_streaming_frontend_addr
                            .clone()
                            .or_else(|| rw_config.and_then(|c| Some(c.frontend_addr.clone())))
                            .unwrap_or_else(|| "127.0.0.1:4566".to_string());

                        let data_dir = rw_config
                            .and_then(|c| Some(std::path::PathBuf::from(&c.data_dir)))
                            .unwrap_or_else(|| rocksdb_path.join("event-streaming"));

                        let binary_path = rw_config
                            .and_then(|c| c.binary_path.as_ref().map(std::path::PathBuf::from));

                        let startup_timeout = rw_config.map_or(60, |c| c.startup_timeout_secs);
                        let shutdown_timeout = rw_config.map_or(30, |c| c.shutdown_timeout_secs);

                        let parallelism = rw_config
                            .and_then(|c| c.parallelism)
                            .unwrap_or_else(num_cpus::get);

                        let embedded_config = nexora_risingwave::EmbeddedConfig {
                            binary_path,
                            data_dir,
                            meta: nexora_risingwave::MetaConfig {
                                listen_addr: meta_addr.clone(),
                                backend: nexora_risingwave::MetaBackend::Memory,
                            },
                            frontend: nexora_risingwave::FrontendConfig {
                                listen_addr: frontend_addr.clone(),
                            },
                            compute: nexora_risingwave::ComputeConfig { parallelism },
                            startup_timeout_secs: startup_timeout,
                            shutdown_timeout_secs: shutdown_timeout,
                        };

                        match nexora_risingwave::EmbeddedEventStreaming::start(embedded_config)
                            .await
                        {
                            Ok(instance) => {
                                tracing::info!(
                                    "   Event Streaming: embedded process started (PID: {})",
                                    instance.pid()
                                );
                                Some(instance)
                            }
                            Err(e) => {
                                tracing::error!("Failed to start embedded event streams: {}", e);
                                anyhow::bail!(
                                    "Embedded event streams initialization failed: {}",
                                    e
                                );
                            }
                        }
                    } else {
                        None
                    }
                };

                #[cfg(not(feature = "embedded"))]
                let embedded_instance: Option<RwEmbeddedInstance> = None;

                let meta_addr = cli
                    .event_streaming_meta_addr
                    .clone()
                    .or_else(|| rw_config.map(|c| c.meta_addr.clone()))
                    .unwrap_or_else(|| {
                        tracing::warn!(
                            "No --event-streaming-meta-addr provided, using default 127.0.0.1:5690"
                        );
                        "127.0.0.1:5690".to_string()
                    });
                let frontend_addr = cli.event_streaming_frontend_addr.clone()
                    .or_else(|| rw_config.map(|c| c.frontend_addr.clone()))
                    .unwrap_or_else(|| {
                        tracing::warn!("No --event-streaming-frontend-addr provided, using default 127.0.0.1:4566");
                        "127.0.0.1:4566".to_string()
                    });

                let meta_socket: std::net::SocketAddr = meta_addr.parse().map_err(|e| {
                    anyhow::anyhow!("Invalid --event-streaming-meta-addr '{}': {}", meta_addr, e)
                })?;
                let frontend_socket: std::net::SocketAddr = frontend_addr.parse().map_err(|e| {
                    anyhow::anyhow!(
                        "Invalid --event-streaming-frontend-addr '{}': {}",
                        frontend_addr,
                        e
                    )
                })?;

                let mut config = nexora_risingwave::EventStreamingConfig::new()
                    .with_meta_addr(meta_socket)
                    .with_frontend_addr(frontend_socket);

                // Enable HA mode with Raft if requested
                if cli.event_streaming_ha {
                    let node_id = cli.event_streaming_raft_node_id.unwrap_or_else(|| {
                        tracing::warn!(
                            "No --event-streaming-raft-node-id provided, using default 1"
                        );
                        1
                    });
                    let peers: Vec<(u64, String)> = cli
                        .event_streaming_raft_peers
                        .iter()
                        .map(|&peer_id| (peer_id, format!("node-{}:5690", peer_id)))
                        .collect();
                    config = config.with_ha(true).with_raft_peers(peers);
                    tracing::info!(
                        "   Event Streaming: HA enabled (Raft node_id={}, peers={:?})",
                        node_id,
                        cli.event_streaming_raft_peers
                    );
                }

                match nexora_risingwave::EventStreamingModule::start(config).await {
                    Ok(module) => {
                        tracing::info!(
                            "   Event Streaming: started (meta={}, frontend={})",
                            meta_addr,
                            frontend_addr
                        );
                        (Some(Arc::new(module)), embedded_instance, None)
                    }
                    Err(e) => {
                        tracing::error!("Failed to start event streaming engine: {}", e);
                        anyhow::bail!("Event streaming initialization failed: {}", e);
                    }
                }
            }
        } else {
            (None, None, None)
        }
    };

    // Without the `event-streaming` feature the crate is not linked, so the bindings
    // cannot name its types. All downstream consumers are `event-streaming`-gated, so
    // these placeholders are never read; underscore names silence unused warnings.
    #[cfg(not(feature = "event-streaming"))]
    let (_event_streaming_module, _embedded_event_streaming, _distributed_event_streaming): (
        Option<()>,
        Option<()>,
        Option<()>,
    ) = (None, None, None);

    // ============================================================
    // Library mode: in-process RisingWave (--features library)
    // ============================================================
    // Starts a full single-node RisingWave instance inside the nexora process
    // using vendored crates — no external `event-streaming` binary needed.
    // Enabled by --library-event-streaming; mutually exclusive with --embedded-event-streaming.
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    let mut library_event_streaming_client: Option<
        Arc<dyn nexora_risingwave::EventStreamingOperations>,
    > = None;

    #[cfg(all(feature = "event-streaming", feature = "library"))]
    let library_event_streaming: Option<nexora_risingwave::EmbeddedLibrary> = if cli
        .library_event_streaming
    {
        let frontend_addr = cli
            .event_streaming_frontend_addr
            .clone()
            .unwrap_or_else(|| "127.0.0.1:4566".to_string());

        // Use a persistent store when RocksDB is enabled, in-memory otherwise.
        let lib_config = if cli.no_rocksdb {
            nexora_risingwave::EmbeddedLibraryConfig::new()
                .with_frontend_listen_addr(&frontend_addr)
                .in_memory()
        } else {
            let store_dir = rocksdb_path.join("event-streaming-library");
            nexora_risingwave::EmbeddedLibraryConfig::new()
                .with_frontend_listen_addr(&frontend_addr)
                .with_store_directory(store_dir)
        };

        match nexora_risingwave::EmbeddedLibrary::start(lib_config) {
            Ok(instance) => {
                tracing::info!(
                    "   Event Streaming: in-process library started \
                         (RisingWave engine embedded in nexora binary, frontend={})",
                    frontend_addr,
                );

                // Wait a moment for RisingWave to be ready
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                // Create client module for HTTP API
                match nexora_risingwave::LibraryEventStreamingModule::connect(frontend_addr.clone())
                    .await
                {
                    Ok(client_module) => {
                        tracing::info!("   Event Streaming: client connected to library instance");
                        // Store both: library instance for lifecycle, client for API
                        library_event_streaming_client = Some(Arc::new(client_module)
                            as Arc<dyn nexora_risingwave::EventStreamingOperations>);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to connect Event Streaming client: {}", e);
                    }
                }

                Some(instance)
            }
            Err(e) => {
                tracing::error!("Failed to start in-process event streaming engine: {}", e);
                anyhow::bail!("Library event streams initialization failed: {}", e);
            }
        }
    } else {
        None
    };
    #[cfg(not(all(feature = "event-streaming", feature = "library")))]
    let library_event_streaming: Option<()> = None;

    // ============================================================
    // Distributed library mode: multi-node in-process cluster
    // ============================================================
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    let distributed_library_cluster: Option<
        Arc<(
            nexora_risingwave::DistributedMetaCluster,
            nexora_risingwave::DistributedFrontendPool,
            nexora_risingwave::DistributedComputeCluster,
        )>,
    > = {
        if let Some(ref rw_config) = config_file.event_streaming {
            if let Some(ref dist_config) = rw_config.distributed {
                if dist_config.enabled {
                    tracing::info!(
                        "   Event Streaming: starting distributed library mode (node_id={})",
                        dist_config.node_id
                    );

                    use nexora_risingwave::{
                        ComputeNodeConfig, DistributedLibraryConfig, FrontendNodeConfig,
                        MetaBackend, MetaNodeConfig,
                    };
                    use std::path::PathBuf;

                    // Parse backend configuration
                    let backend = match dist_config.meta.backend.as_str() {
                        "etcd" => {
                            let endpoints =
                                dist_config.meta.etcd_endpoints.clone().ok_or_else(|| {
                                    anyhow::anyhow!("etcd backend requires etcd_endpoints")
                                })?;
                            MetaBackend::Etcd { endpoints }
                        }
                        "sqlite" => {
                            let path = dist_config
                                .meta
                                .sqlite_path
                                .as_ref()
                                .map(PathBuf::from)
                                .unwrap_or_else(|| {
                                    PathBuf::from(format!(
                                        "./nexora-data/meta-{}.db",
                                        dist_config.node_id
                                    ))
                                });
                            MetaBackend::Sqlite { path }
                        }
                        "memory" => MetaBackend::Memory,
                        other => anyhow::bail!("Unknown meta backend: {}", other),
                    };

                    let meta_listen: std::net::SocketAddr = dist_config.meta.listen_addr.parse()?;

                    let frontend_config = dist_config.frontend.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("distributed mode requires frontend configuration")
                    })?;
                    let frontend_listen: std::net::SocketAddr =
                        frontend_config.listen_addr.parse()?;

                    let compute_config = dist_config.compute.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("distributed mode requires compute configuration")
                    })?;
                    let compute_listen: std::net::SocketAddr =
                        compute_config.listen_addr.parse()?;
                    let compute_internal_rpc: Option<std::net::SocketAddr> = compute_config
                        .internal_rpc_addr
                        .as_ref()
                        .map(|s| s.parse())
                        .transpose()?;

                    let lib_dist_config = DistributedLibraryConfig {
                        node_id: dist_config.node_id.clone(),
                        meta: MetaNodeConfig {
                            listen_addr: meta_listen,
                            advertise_addr: dist_config.meta.advertise_addr.clone(),
                            raft_peers: dist_config.meta.raft_peers.clone(),
                            backend,
                            election_timeout_ms: dist_config.meta.election_timeout_ms,
                            heartbeat_interval_ms: dist_config.meta.heartbeat_interval_ms,
                        },
                        frontend: FrontendNodeConfig {
                            listen_addr: frontend_listen,
                        },
                        compute: ComputeNodeConfig {
                            listen_addr: compute_listen,
                            parallelism: compute_config.parallelism,
                            internal_rpc_addr: compute_internal_rpc,
                        },
                        data_dir: PathBuf::from(&dist_config.data_dir),
                    };

                    // Validate configuration
                    lib_dist_config.validate().map_err(|e| {
                        anyhow::anyhow!("Invalid distributed library configuration: {}", e)
                    })?;

                    // Start cluster components
                    match nexora_risingwave::start_distributed_library_cluster(lib_dist_config)
                        .await
                    {
                        Ok((meta, frontend, compute)) => {
                            tracing::info!(
                                "   Event Streaming: distributed library cluster started"
                            );
                            Some(Arc::new((meta, frontend, compute)))
                        }
                        Err(e) => {
                            tracing::error!("Failed to start distributed library cluster: {}", e);
                            anyhow::bail!(
                                "Distributed library cluster initialization failed: {}",
                                e
                            );
                        }
                    }
                } else {
                    None
                }
            } else if cli.distributed_library_event_streaming {
                // CLI-driven distributed library mode
                tracing::info!(
                    "   Event Streaming: starting distributed library mode (CLI-driven)"
                );

                use nexora_risingwave::{
                    ComputeNodeConfig, DistributedLibraryConfig, FrontendNodeConfig,
                    MetaBackend, MetaNodeConfig,
                };

                let node_id = cli.library_node_id.ok_or_else(|| {
                    anyhow::anyhow!("--library-node-id is required for distributed library mode")
                })?;

                let meta_addr_str = cli.library_meta_addr.ok_or_else(|| {
                    anyhow::anyhow!("--library-meta-addr is required for distributed library mode")
                })?;
                let meta_listen: std::net::SocketAddr = meta_addr_str.parse()?;

                let meta_advertise = cli.library_meta_advertise.ok_or_else(|| {
                    anyhow::anyhow!("--library-meta-advertise is required for distributed library mode")
                })?;

                let frontend_addr_str = cli
                    .event_streaming_frontend_addr
                    .clone()
                    .unwrap_or_else(|| "127.0.0.1:4566".to_string());
                let frontend_listen: std::net::SocketAddr = frontend_addr_str.parse()?;

                // Determine Meta backend from CLI args
                let backend = match cli.meta_backend {
                    MetaBackendType::Memory => {
                        tracing::warn!("Using in-memory Meta backend - data will not persist across restarts!");
                        MetaBackend::Memory
                    }
                    MetaBackendType::Sqlite => {
                        let path = cli.meta_backend_sqlite_path.clone().unwrap_or_else(|| {
                            let default_path = rocksdb_path.join(format!("meta-{}.db", node_id));
                            tracing::info!("Using default SQLite path: {:?}", default_path);
                            default_path
                        });
                        tracing::info!("Using SQLite Meta backend: {:?}", path);
                        MetaBackend::Sqlite { path }
                    }
                    MetaBackendType::Etcd => {
                        if cli.meta_backend_etcd_endpoints.is_empty() {
                            anyhow::bail!("--meta-backend-etcd-endpoints is required when using etcd backend");
                        }
                        tracing::info!("Using Etcd Meta backend: {:?}", cli.meta_backend_etcd_endpoints);
                        MetaBackend::Etcd {
                            endpoints: cli.meta_backend_etcd_endpoints.clone(),
                        }
                    }
                };

                let lib_dist_config = DistributedLibraryConfig {
                    node_id: node_id.clone(),
                    meta: MetaNodeConfig {
                        listen_addr: meta_listen,
                        advertise_addr: meta_advertise,
                        raft_peers: cli.library_meta_peers.clone(),
                        backend,
                        election_timeout_ms: 3000,
                        heartbeat_interval_ms: 1000,
                    },
                    frontend: FrontendNodeConfig {
                        listen_addr: frontend_listen,
                    },
                    compute: ComputeNodeConfig {
                        listen_addr: frontend_listen, // Reuse frontend addr for compute
                        parallelism: Some(num_cpus::get()),
                        internal_rpc_addr: None,
                    },
                    data_dir: rocksdb_path.join("event-streaming-distributed"),
                };

                lib_dist_config.validate().map_err(|e| {
                    anyhow::anyhow!("Invalid distributed library configuration: {}", e)
                })?;

                // Start cluster components
                match nexora_risingwave::start_distributed_library_cluster(lib_dist_config)
                    .await
                {
                    Ok((meta, frontend, compute)) => {
                        tracing::info!(
                            "   Event Streaming: distributed library cluster started (node_id={}, backend={:?})",
                            node_id,
                            cli.meta_backend
                        );
                        Some(Arc::new((meta, frontend, compute)))
                    }
                    Err(e) => {
                        tracing::error!("Failed to start distributed library cluster: {}", e);
                        anyhow::bail!(
                            "Distributed library cluster initialization failed: {}",
                            e
                        );
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    #[cfg(not(all(feature = "event-streaming", feature = "library")))]
    let _distributed_library_cluster: Option<()> = None;

    let state = AppState {
        graph: graph.clone(),
        sq_manager: sq_manager.clone(),
        config: AppConfig {
            max_nodes_per_shard: cli.max_nodes_per_shard,
            rocksdb_path: if cli.no_rocksdb {
                None
            } else {
                Some(rocksdb_path.to_string_lossy().to_string())
            },
            wal_dir: if cli.no_wal {
                None
            } else {
                Some(cli.wal_dir.to_string_lossy().to_string())
            },
            allow_ingest_dir: cli.allow_ingest_dir.clone(),
            profile: resolved_profile.as_str().to_string(),
        },
        shutdown: shutdown_notify,
        start_time: std::time::Instant::now(),
        ingests: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        metrics: metrics_state.clone(),
        hnsw: Arc::new(tokio::sync::Mutex::new(HnswIndex::new(
            HnswConfig::default(),
        ))),
        streams: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        recipes: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        recipe_runs: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        udf_registry: Arc::new(tokio::sync::RwLock::new(
            nexora_udf::native::UdfRegistry::new(),
        )),
        udf_manager,
        tiered_store,
        fragment_store,
        mv_manager: mv_manager.clone(),
        ontology_manager: ontology_manager.clone(),
        #[cfg(feature = "event-first")]
        event_store: event_store.clone(),
        #[cfg(feature = "event-first")]
        event_router: event_router.clone(),
        #[cfg(feature = "event-first")]
        refresh_scheduler: refresh_scheduler.clone(),
        sq_mv_bridge,
        router: app_router.clone(),
        cluster_manager: cluster_manager.clone(),
        replica_writer: app_replica_writer,
        catch_up_barrier: app_catch_up_barrier,
        auth: auth_for_state,
        drain: crate::drain::DrainState::default(),
        query_pool: Arc::new(nexora_core::query_pool::QueryPool::new(
            std::thread::available_parallelism()
                .map(|n| n.get() * 4)
                .unwrap_or(64),
        )),
        #[cfg(feature = "event-streaming")]
        event_streaming: {
            #[cfg(feature = "library")]
            {
                library_event_streaming_client.or_else(|| {
                    event_streaming_module
                        .as_ref()
                        .map(|m| m.clone() as Arc<dyn nexora_risingwave::EventStreamingOperations>)
                })
            }
            #[cfg(not(feature = "library"))]
            {
                event_streaming_module
                    .as_ref()
                    .map(|m| m.clone() as Arc<dyn nexora_risingwave::EventStreamingOperations>)
            }
        },
        #[cfg(all(feature = "event-streaming", feature = "embedded"))]
        distributed_event_streaming: distributed_event_streaming.map(Arc::new),
        #[cfg(all(feature = "event-streaming", feature = "library"))]
        distributed_library: distributed_library_cluster,
        #[cfg(feature = "event-streaming")]
        event_sinks: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        #[cfg(all(feature = "event-first", feature = "event-streaming"))]
        graph_projector: None, // Will be initialized later
    };

    // ============================================================
    // Kafka stream source (CLI-driven)
    // ============================================================
    if let (Some(ref _brokers), Some(ref _topic)) = (&cli.kafka_brokers, &cli.kafka_topic) {
        #[cfg(feature = "kafka")]
        {
            let brokers = _brokers;
            let topic = _topic;
            use nexora_stream::kafka::{KafkaSource, KafkaSourceConfig};
            use nexora_stream::IngestionSource;
            use std::sync::Arc;

            let config = KafkaSourceConfig {
                brokers: brokers.clone(),
                topic: topic.clone(),
                group_id: cli.kafka_group_id.clone(),
                key_field: "id".to_string(),
                event_time_field: cli.event_time_field.clone(),
                event_time_unit: nexora_stream::EventTimeUnit::default(),
            };

            let source = Arc::new(KafkaSource::new(config.clone()));

            match source.connect().await {
                Ok(()) => {
                    let graph_for_kafka = state.graph.clone();
                    let streams_for_kafka = state.streams.clone();
                    let topic_name = topic.clone();
                    let brokers_name = brokers.clone();

                    let name = format!("kafka-{}", chrono::Utc::now().timestamp());
                    let name_for_task = name.clone();

                    let stream_info = handlers::StreamSource {
                        name: name.clone(),
                        source_type: "kafka".to_string(),
                        topic: topic.clone(),
                        brokers: brokers.clone(),
                        started_at: chrono::Utc::now().to_rfc3339(),
                    };
                    streams_for_kafka
                        .write()
                        .await
                        .insert(name.clone(), stream_info);

                    tracing::info!(
                        name = %name_for_task,
                        topic = %topic_name,
                        brokers = %brokers_name,
                        "Kafka stream source started (CLI, via IngestionPipeline)"
                    );

                    // Drive the source through the unified IngestionPipeline
                    // instead of a bespoke poll loop. The pipeline owns polling,
                    // offset commit (at-least-once: commit AFTER the batch is
                    // applied, so a crash replays uncommitted records — graph
                    // writes are idempotent by qid, so replay converges),
                    // event-time watermark advance, and — when configured — an
                    // offset-aligned checkpoint. GraphIngestHandler coalesces each
                    // batch by node id and commits via write_batch (one WAL
                    // group-commit fsync per batch), which now also fires the SQ /
                    // property-index side effects for batched property writes.
                    let handler = state.make_ingest_handler(nexora_core::BatchDurability::Relaxed);
                    let metrics_for_wm = state.metrics.clone();
                    let mut pipeline = nexora_stream::IngestionPipeline::new(
                        nexora_stream::IngestionConfig::default(),
                        Arc::new(nexora_stream::InMemoryOffsetStore::new()),
                    )
                    .with_source(source)
                    // 500ms bounded out-of-orderness (µs) — the ingest
                    // watermark trails the max observed event time by this much.
                    .with_watermarks(500_000)
                    .with_watermark_observer(Arc::new(move |wm| {
                        // Report event-time progress to Prometheus without a
                        // reverse crate dependency (stream must not depend on app).
                        metrics_for_wm.set_watermark_ms(wm.as_micros() / 1000);
                    }));

                    // Exactly-once via offset-aligned checkpoints. Only when
                    // persistence is enabled (in-memory mode can't checkpoint
                    // durably) and --checkpoint-secs > 0. The coordinator flushes
                    // graph state and binds it to source offsets; on restart we
                    // recover that cut and seed it so the pipeline seeks the Kafka
                    // consumer back to the flushed offset (see IngestionPipeline
                    // run/seek) — replaying anything past it (idempotent by qid).
                    let checkpoint_recovery = if !cli.no_rocksdb && cli.checkpoint_secs > 0 {
                        let ckpt_dir = rocksdb_path.join("checkpoints");
                        match nexora_stream::FileCheckpointStore::new(&ckpt_dir) {
                            Ok(store) => {
                                let coordinator =
                                    Arc::new(nexora_stream::CheckpointCoordinator::new(
                                        state.graph.clone(),
                                        Arc::new(store),
                                        state.graph.shard_count(),
                                    ));
                                // Recover the last checkpoint's offset cut BEFORE run().
                                let recovery = coordinator.recover_and_resume().await;
                                pipeline = pipeline.with_checkpointing(
                                    coordinator,
                                    std::time::Duration::from_secs(cli.checkpoint_secs),
                                );
                                recovery.ok().flatten()
                            }
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    dir = %ckpt_dir.display(),
                                    "checkpoint store open failed; running without checkpointing"
                                );
                                None
                            }
                        }
                    } else {
                        None
                    };

                    let pipeline = Arc::new(pipeline);
                    // Seed recovery offsets (if any) so run() seeks the source to
                    // the checkpointed cut instead of the broker-committed offset.
                    if let Some(plan) = checkpoint_recovery {
                        let seeded = pipeline.seed_recovery(&plan).await;
                        tracing::info!(
                            offsets = seeded,
                            "Kafka ingestion resuming from checkpoint recovery plan"
                        );
                    }
                    pipeline.run(handler);

                    tracing::info!(
                        "   Kafka:  brokers={}, topic={}, group={}",
                        brokers,
                        topic,
                        cli.kafka_group_id
                    );
                }
                Err(e) => {
                    tracing::error!("   Kafka:  failed to connect: {e}");
                }
            }
        }
        #[cfg(not(feature = "kafka"))]
        {
            tracing::error!(
                "   Kafka:  --kafka-brokers provided but 'kafka' feature is not enabled. \
                 Rebuild with: cargo build -p nexora-app --features kafka"
            );
            anyhow::bail!(
                "Kafka support not compiled. Enable the 'kafka' feature: \
                 cargo build -p nexora-app --features kafka"
            );
        }
    }

    // ============================================================
    // MQTT ingestion (feature-gated behind "mqtt")
    // ============================================================
    if let Some(ref _mqtt_host) = cli.mqtt_host {
        #[cfg(feature = "mqtt")]
        {
            use nexora_stream::{GraphIngestHandler, IngestHandler, MqttSource, MqttSourceConfig};

            if cli.mqtt_topic.is_empty() {
                anyhow::bail!("--mqtt-host requires at least one --mqtt-topic");
            }

            let config = MqttSourceConfig {
                host: _mqtt_host.clone(),
                port: cli.mqtt_port,
                client_id: format!("nexora-app-{}", chrono::Utc::now().timestamp()),
                topics: cli.mqtt_topic.clone(),
                qos: cli.mqtt_qos,
                id_field: "id".to_string(),
                event_time_field: cli.event_time_field.clone(),
                event_time_unit: nexora_stream::EventTimeUnit::default(),
                max_batch: 256,
                buffer_capacity: 10_000,
            };
            let source = Arc::new(MqttSource::new(config.clone()));
            let topics_disp = cli.mqtt_topic.join(", ");

            // Drive MQTT through the unified IngestionPipeline (like Kafka),
            // instead of a bespoke poll loop. The pipeline owns connect + poll +
            // watermark advance. NO checkpointing: MQTT is push/QoS-driven with no
            // replayable server-side offset (see mqtt_source docs), so an
            // offset-aligned checkpoint is meaningless — `seek` is the trait's
            // no-op default. Durability is WaitDurable: an acked write must be on
            // disk because the broker will not redeliver an already-acked message.
            //
            // The pipeline's run() connects the source itself (MQTT connect spawns
            // its event-loop drainer), so we do NOT pre-connect — that would spawn
            // a second, leaked event loop.
            let handler = state.make_ingest_handler(nexora_core::BatchDurability::WaitDurable);
            let metrics_for_wm = state.metrics.clone();
            let pipeline = Arc::new(
                nexora_stream::IngestionPipeline::new(
                    nexora_stream::IngestionConfig::default(),
                    Arc::new(nexora_stream::InMemoryOffsetStore::new()),
                )
                .with_source(source)
                .with_watermarks(500_000)
                .with_watermark_observer(Arc::new(move |wm| {
                    metrics_for_wm.set_watermark_ms(wm.as_micros() / 1000);
                })),
            );
            pipeline.run(handler);
            tracing::info!(
                "   MQTT:   host={}:{}, topics=[{}], qos={} (via IngestionPipeline)",
                _mqtt_host,
                cli.mqtt_port,
                topics_disp,
                cli.mqtt_qos
            );
        }
        #[cfg(not(feature = "mqtt"))]
        {
            tracing::error!(
                "   MQTT:   --mqtt-host provided but 'mqtt' feature is not enabled. \
                 Rebuild with: cargo build -p nexora-app --features mqtt"
            );
            anyhow::bail!(
                "MQTT support not compiled. Enable the 'mqtt' feature: \
                 cargo build -p nexora-app --features mqtt"
            );
        }
    }

    // ============================================================
    // WebSocket ingestion (feature-gated behind "websocket")
    // ============================================================
    if let Some(ref _ws_url) = cli.ws_url {
        #[cfg(feature = "websocket")]
        {
            use nexora_stream::{
                GraphIngestHandler, IngestHandler, WebSocketSource, WebSocketSourceConfig,
            };

            let config = WebSocketSourceConfig {
                url: _ws_url.clone(),
                topic: format!("ws-{}", chrono::Utc::now().timestamp()),
                id_field: "id".to_string(),
                event_time_field: cli.event_time_field.clone(),
                event_time_unit: nexora_stream::EventTimeUnit::default(),
                max_batch: 256,
                buffer_capacity: 10_000,
            };
            let source = Arc::new(WebSocketSource::new(config));
            let url_disp = _ws_url.clone();

            // Drive WebSocket through the unified IngestionPipeline (like
            // Kafka/MQTT/Kinesis). Same shape as MQTT: push-model with no
            // replayable offset, so NO checkpointing (seek is the trait no-op)
            // and WaitDurable — a dropped connection loses in-flight frames with
            // no redelivery, so an acked write must already be on disk. The
            // pipeline owns connect (which spawns the frame-reader drainer), so
            // we do NOT pre-connect — that would leak a second drainer.
            let handler = state.make_ingest_handler(nexora_core::BatchDurability::WaitDurable);
            let metrics_for_wm = state.metrics.clone();
            let pipeline = Arc::new(
                nexora_stream::IngestionPipeline::new(
                    nexora_stream::IngestionConfig::default(),
                    Arc::new(nexora_stream::InMemoryOffsetStore::new()),
                )
                .with_source(source)
                .with_watermarks(500_000)
                .with_watermark_observer(Arc::new(move |wm| {
                    metrics_for_wm.set_watermark_ms(wm.as_micros() / 1000);
                })),
            );
            pipeline.run(handler);
            tracing::info!("   WS:     url={url_disp} (via IngestionPipeline)");
        }
        #[cfg(not(feature = "websocket"))]
        {
            tracing::error!(
                "   WS:     --ws-url provided but 'websocket' feature is not enabled. \
                 Rebuild with: cargo build -p nexora-app --features websocket"
            );
            anyhow::bail!(
                "WebSocket support not compiled. Enable the 'websocket' feature: \
                 cargo build -p nexora-app --features websocket"
            );
        }
    }

    // ============================================================
    // Kinesis ingestion (feature-gated behind "kinesis")
    // ============================================================
    if let Some(ref _kinesis_stream) = cli.kinesis_stream {
        #[cfg(feature = "kinesis")]
        {
            use nexora_stream::{
                GraphIngestHandler, IngestHandler, KinesisSource, KinesisSourceConfig,
            };

            let config = KinesisSourceConfig {
                stream_name: _kinesis_stream.clone(),
                region: cli.kinesis_region.clone(),
                endpoint_url: cli.kinesis_endpoint.clone(),
                id_field: "id".to_string(),
                event_time_field: cli.event_time_field.clone(),
                event_time_unit: nexora_stream::EventTimeUnit::default(),
                max_batch: 256,
            };
            // Kinesis resumes from its OWN committed sequence numbers (string
            // AFTER_SEQUENCE_NUMBER checkpoints in an OffsetStore), loaded inside
            // connect() — distinct from the pipeline's u64 offset-cut checkpoint,
            // which does not fit Kinesis's string sequences. Attach a PERSISTENT
            // offset store (rocksdb-offsets) so a restart resumes after the last
            // applied sequence instead of re-reading the whole retention window
            // from TRIM_HORIZON (the prior handrolled loop attached no store and
            // never committed, so it re-read everything on every restart).
            #[cfg(feature = "rocksdb-offsets")]
            let offset_store: Arc<dyn nexora_stream::OffsetStore> = {
                let dir = rocksdb_path.join("kinesis_offsets");
                match nexora_stream::RocksDbOffsetStore::open(&dir) {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        tracing::warn!(
                            error = %e, dir = %dir.display(),
                            "Kinesis offset store open failed; resume disabled (TRIM_HORIZON on restart)"
                        );
                        Arc::new(nexora_stream::InMemoryOffsetStore::new())
                    }
                }
            };
            #[cfg(not(feature = "rocksdb-offsets"))]
            let offset_store: Arc<dyn nexora_stream::OffsetStore> =
                Arc::new(nexora_stream::InMemoryOffsetStore::new());

            let source = Arc::new(KinesisSource::new(config).with_offset_store(offset_store));
            let stream_disp = _kinesis_stream.clone();

            // Drive Kinesis through the unified IngestionPipeline (like Kafka/MQTT).
            // The pipeline connects (Kinesis self-resumes from its offset store),
            // polls, commits (Kinesis persists the sequence per shard), and advances
            // the watermark. Relaxed durability: records are replayable within the
            // retention window, and commit-after-apply + idempotent-by-qid writes
            // converge on replay. The pipeline's u64 seek() is a no-op for Kinesis
            // (its resume is string-sequence-based, handled in connect()).
            let handler: Arc<dyn IngestHandler> =
                state.make_ingest_handler(nexora_core::BatchDurability::Relaxed);
            let metrics_for_wm = state.metrics.clone();
            let pipeline = Arc::new(
                nexora_stream::IngestionPipeline::new(
                    nexora_stream::IngestionConfig::default(),
                    Arc::new(nexora_stream::InMemoryOffsetStore::new()),
                )
                .with_source(source)
                .with_watermarks(500_000)
                .with_watermark_observer(Arc::new(move |wm| {
                    metrics_for_wm.set_watermark_ms(wm.as_micros() / 1000);
                })),
            );
            pipeline.run(handler);
            tracing::info!("   Kinesis: stream={stream_disp} (via IngestionPipeline)");
        }
        #[cfg(not(feature = "kinesis"))]
        {
            tracing::error!(
                "   Kinesis: --kinesis-stream provided but 'kinesis' feature is not enabled. \
                 Rebuild with: cargo build -p nexora-app --features kinesis"
            );
            anyhow::bail!(
                "Kinesis support not compiled. Enable the 'kinesis' feature: \
                 cargo build -p nexora-app --features kinesis"
            );
        }
    }

    // ============================================================
    // zenoh ingestion (feature-gated behind "zenoh")
    // ============================================================
    if let Some(ref _zenoh_key) = cli.zenoh_key {
        #[cfg(feature = "zenoh")]
        {
            use nexora_stream::{
                GraphIngestHandler, IngestHandler, ZenohSource, ZenohSourceConfig,
            };

            let config = ZenohSourceConfig {
                key_expr: _zenoh_key.clone(),
                topic: format!("zenoh-{}", chrono::Utc::now().timestamp()),
                id_field: "id".to_string(),
                event_time_field: cli.event_time_field.clone(),
                event_time_unit: nexora_stream::EventTimeUnit::default(),
                max_batch: 256,
                buffer_capacity: 10_000,
            };
            let source = Arc::new(ZenohSource::new(config));
            let key_disp = _zenoh_key.clone();

            // Drive zenoh through the unified IngestionPipeline (like the other
            // sources). Same shape as MQTT/WebSocket: pub/sub push-model with no
            // replayable offset, so NO checkpointing (seek is the trait no-op) and
            // WaitDurable — a dropped sample is lost with no redelivery, so an
            // acked write must already be on disk. The pipeline owns connect
            // (which spawns the subscriber drainer), so we do NOT pre-connect —
            // that would leak a second drainer.
            let handler = state.make_ingest_handler(nexora_core::BatchDurability::WaitDurable);
            let metrics_for_wm = state.metrics.clone();
            let pipeline = Arc::new(
                nexora_stream::IngestionPipeline::new(
                    nexora_stream::IngestionConfig::default(),
                    Arc::new(nexora_stream::InMemoryOffsetStore::new()),
                )
                .with_source(source)
                .with_watermarks(500_000)
                .with_watermark_observer(Arc::new(move |wm| {
                    metrics_for_wm.set_watermark_ms(wm.as_micros() / 1000);
                })),
            );
            pipeline.run(handler);
            tracing::info!("   zenoh:  key={key_disp} (via IngestionPipeline)");
        }
        #[cfg(not(feature = "zenoh"))]
        {
            tracing::error!(
                "   zenoh:  --zenoh-key provided but 'zenoh' feature is not enabled. \
                 Rebuild with: cargo build -p nexora-app --features zenoh"
            );
            anyhow::bail!(
                "zenoh support not compiled. Enable the 'zenoh' feature: \
                 cargo build -p nexora-app --features zenoh"
            );
        }
    }

    // ============================================================
    // Phase 7.6: Initialize GraphStreaming (Event-to-Graph Projection)
    // ============================================================
    #[cfg(all(feature = "event-first", feature = "event-streaming"))]
    if let Some(ref rules_dir) = cli.graph_streaming_rules {
        if let Some(ref event_store) = state.event_store {
            tracing::info!("Initializing GraphStreaming from rules directory: {:?}", rules_dir);

            match nexora_graphstreaming::ProjectionRule::load_from_directory(rules_dir) {
                Ok(rules) => {
                    if rules.is_empty() {
                        tracing::warn!("No projection rules found in {:?}", rules_dir);
                    } else {
                        tracing::info!("Loaded {} projection rule(s)", rules.len());

                        // Create EventProjector
                        let projector = match nexora_graphstreaming::EventProjector::new(
                            rules,
                            event_store.clone(),
                            state.graph_service.clone(),
                        ) {
                            Ok(p) => Arc::new(p),
                            Err(e) => {
                                tracing::error!("Failed to create EventProjector: {}", e);
                                anyhow::bail!("GraphStreaming initialization failed: {}", e);
                            }
                        };

                        // Start projection loops
                        if let Err(e) = projector.start().await {
                            tracing::error!("Failed to start EventProjector: {}", e);
                            anyhow::bail!("GraphStreaming startup failed: {}", e);
                        }

                        // Store in AppState for HTTP API
                        let projector_clone = projector.clone();
                        let mut state_mut = state.clone();
                        #[cfg(all(feature = "event-first", feature = "event-streaming"))]
                        {
                            state_mut.graph_projector = Some(projector_clone);
                        }

                        tracing::info!("✅ GraphStreaming started successfully");
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to load projection rules from {:?}: {}", rules_dir, e);
                    anyhow::bail!("GraphStreaming rule loading failed: {}", e);
                }
            }
        } else {
            tracing::warn!("GraphStreaming rules provided but event-first is not enabled");
        }
    }

    let cors = if cli.cors_origin == "none" {
        // CORS disabled - very restrictive, only same-origin requests allowed
        CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ])
    } else if cli.cors_origin == "*" {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers(Any)
    } else {
        let origin: axum::http::HeaderValue = cli
            .cors_origin
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid --cors-origin value: {}", cli.cors_origin))?;
        CorsLayer::new()
            .allow_origin([origin])
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::DELETE,
                Method::OPTIONS,
            ])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ])
    };

    // Build router
    let public_routes = Router::new()
        .route("/", get(dashboard))
        .route("/test", get(test_page))
        .route("/sdk-docs", get(sdk_docs_page))
        .route("/dashboard", get(dashboard))
        .route("/api/health", get(handlers::health))
        .route("/api/health/ready", get(handlers::readiness))
        .route("/api/health/live", get(handlers::liveness))
        // OpenAPI/Swagger documentation
        .route(
            "/api/openapi.json",
            get(|| async { axum::Json(openapi::openapi_spec()) }),
        )
        .route(
            "/api/docs",
            get(|| async { Html(openapi::swagger_ui_html()) }),
        )
        // Prometheus metrics
        .route(
            "/metrics",
            get({
                let m = metrics_state.clone();
                move || async move { metrics::render_metrics(&m) }
            }),
        )
        // Metrics JSON endpoint (for frontend)
        .route(
            "/api/metrics",
            get({
                let m = metrics_state.clone();
                move || async move { metrics::render_json(&m) }
            }),
        );

    // Phase 7.5: Event Streaming health check endpoint
    #[cfg(feature = "event-streaming")]
    let public_routes = {
        #[cfg(feature = "embedded")]
        let embedded_state = embedded_event_streaming.as_ref().map(|e| {
            serde_json::json!({
                "embedded": true,
                "pid": e.pid(),
                "state": format!("{:?}", e.state()),
            })
        });

        #[cfg(not(feature = "embedded"))]
        let embedded_state: Option<serde_json::Value> = None;

        let rw_module = event_streaming_module.as_ref().map(|m| m.clone());
        public_routes.route(
            "/api/health/event-streaming",
            get(move || {
                let module = rw_module.clone();
                let embedded = embedded_state.clone();
                async move {
                    let status = if module.is_some() {
                        serde_json::json!({
                            "enabled": true,
                            "connected": true,
                            "embedded_info": embedded,
                        })
                    } else {
                        serde_json::json!({
                            "enabled": false,
                            "connected": false,
                        })
                    };
                    axum::Json(status)
                }
            }),
        )
    };

    let admin_routes = Router::new()
        // Auth token generation (admin only - prevents arbitrary token generation)
        .route("/api/auth/token", post(handlers::generate_token))
        // Standing Query management (admin only)
        .route(
            "/api/standing-query",
            get(handlers::list_sq).post(handlers::create_sq),
        )
        .route(
            "/api/standing-query/{id}",
            get(handlers::get_sq).delete(handlers::delete_sq),
        );

    let operator_routes = Router::new()
        .route("/api/query/cypher", post(handlers::execute_cypher))
        .route("/api/query/sql", post(handlers::execute_sql))
        .route("/api/graph/history", get(handlers::time_travel))
        .route(
            "/api/graph/node/{qid}/property/{key}",
            get(handlers::get_property).put(handlers::set_property),
        )
        .route("/api/graph/node/{qid}/edges", get(handlers::get_edges))
        .route("/api/graph/node/{qid}/edges", post(handlers::add_edge))
        // Distributed multi-hop traversal (scatter-gather in cluster mode)
        .route("/api/graph/traverse", post(handlers::traverse))
        // Vector similarity search (HNSW)
        .route("/api/vector/index", post(handlers::vector_index))
        .route("/api/vector/search", post(handlers::vector_search))
        .route(
            "/api/vector/node/{qid}",
            get(handlers::vector_get_node).delete(handlers::vector_delete_node),
        )
        // Ingest
        .route("/api/ingest/file", post(handlers::start_file_ingest))
        .route("/api/ingest/bulk", post(handlers::bulk_ingest))
        .route("/api/ingest", get(handlers::list_ingests))
        .route("/api/ingest/{name}", delete(handlers::delete_ingest))
        // F4.2: Tumbling window analytics
        .route(
            "/api/analytics/tumbling-window",
            post(handlers::analytics_tumbling_window),
        )
        // Stream sources
        .route("/api/streams", get(handlers::list_streams))
        .route("/api/streams/kafka", post(handlers::start_kafka_stream))
        .route("/api/streams/{name}", delete(handlers::delete_stream))
        // System
        .route("/api/system/info", get(handlers::system_info))
        .route("/api/system/config", get(handlers::system_config))
        // Sample data & preset recipes
        .route("/api/sample-data", get(handlers::list_sample_data))
        .route(
            "/api/sample-data/{dataset}",
            post(handlers::load_sample_data),
        )
        .route(
            "/api/sample-recipes/{recipe_id}",
            post(handlers::load_sample_recipe),
        )
        // Admin / Operations
        .route("/api/admin/status", get(handlers::admin_status))
        .route("/api/admin/slow-queries", get(handlers::admin_slow_queries))
        .route("/api/admin/backup", post(handlers::admin_backup))
        .route("/api/admin/restore", post(handlers::admin_restore))
        .route("/api/admin/reindex", post(handlers::admin_reindex))
        // E7.2: WAL encryption key rotation guide
        .route("/api/admin/rotate-key", post(handlers::admin_rotate_key))
        // E4: Rolling upgrade drain — mark this node as draining, wait grace
        // period, then signal it is safe to stop.
        .route("/api/admin/drain", post(handlers::admin_drain))
        // Recipe management
        .route(
            "/api/recipes",
            get(handlers::list_recipes).post(handlers::create_recipe),
        )
        .route(
            "/api/recipes/{name}",
            get(handlers::get_recipe).delete(handlers::delete_recipe),
        )
        .route(
            "/api/recipes/{name}/execute",
            post(handlers::execute_recipe),
        )
        .route("/api/recipes/{name}/runs", get(handlers::get_recipe_runs))
        // Storage
        .route("/api/storage/status", get(handlers::storage_status))
        .route("/api/storage/migrate", post(handlers::storage_migrate))
        // UDF management
        .route("/api/udf/register", post(handlers::udf_register))
        .route("/api/udf", get(handlers::udf_list))
        .route("/api/udf/execute", post(handlers::udf_execute))
        .route("/api/udf/wasm/{name}", post(handlers::udf_register_wasm))
        .route(
            "/api/udf/python/{name}",
            post(handlers::udf_register_python),
        )
        .route(
            "/api/udf/{name}/execute",
            post(handlers::udf_execute_by_name),
        )
        .route("/api/udf/{name}", delete(handlers::udf_delete))
        // Materialized Views
        .route(
            "/api/materialized-views",
            get(handlers::materialized_view::list_materialized_views)
                .post(handlers::materialized_view::create_materialized_view),
        )
        .route(
            "/api/materialized-views/{view_id}",
            get(handlers::materialized_view::get_materialized_view)
                .delete(handlers::materialized_view::drop_materialized_view),
        )
        .route(
            "/api/materialized-views/{view_id}/data",
            get(handlers::materialized_view::query_materialized_view),
        )
        .route(
            "/api/materialized-views/{view_id}/refresh",
            post(handlers::materialized_view::refresh_materialized_view),
        )
        .route(
            "/api/materialized-views/{view_id}/link-sq",
            post(handlers::materialized_view::link_sq_to_mv),
        )
        // SQL DDL for Materialized Views
        .route(
            "/api/sql/ddl",
            post(handlers::materialized_view::execute_sql_ddl),
        )
        // Ontology management (stage 6) — domain-package (schema) definitions
        .route(
            "/api/ontologies",
            get(handlers::ontology::list_ontologies).post(handlers::ontology::create_ontology),
        )
        .route(
            "/api/ontologies/yaml",
            post(handlers::ontology::create_ontology_yaml),
        )
        .route(
            "/api/ontologies/validate",
            post(handlers::ontology::validate_ontology),
        )
        .route(
            "/api/ontologies/{domain}",
            get(handlers::ontology::get_ontology).delete(handlers::ontology::delete_ontology),
        )
        // Query Optimizer
        .route("/api/query/explain", post(handlers::explain::explain_query))
        // RESTful alias routes (plural + kebab-case, for SDK/CLI friendliness)
        .route(
            "/api/nodes/{qid}/properties/{key}",
            get(handlers::get_property).put(handlers::set_property),
        )
        .route(
            "/api/nodes/{qid}/edges",
            get(handlers::get_edges).post(handlers::add_edge),
        )
        .route(
            "/api/standing-queries",
            get(handlers::list_sq).post(handlers::create_sq),
        )
        .route(
            "/api/standing-queries/{id}",
            get(handlers::get_sq).delete(handlers::delete_sq),
        )
        .route(
            "/api/vectors/{qid}",
            get(handlers::vector_get_node).delete(handlers::vector_delete_node),
        )
        // WebSocket
        .route("/api/ws/query", get(handlers::ws_query_handler))
        .route("/api/ws/sq", get(handlers::ws_sq_all_handler))
        .route("/api/ws/sq/{id}", get(handlers::ws_sq_handler))
        .route("/api/ws/metrics", get(handlers::ws_metrics_handler));

    // Event Streaming HTTP endpoints (feature-gated)
    #[cfg(feature = "event-streaming")]
    let operator_routes = operator_routes
        .route(
            "/api/event-streaming/ddl",
            post(handlers::event_streaming::execute_ddl),
        )
        .route(
            "/api/event-streaming/query",
            post(handlers::event_streaming::query_mv),
        )
        .route(
            "/api/event-streaming/sources",
            get(handlers::event_streaming::list_sources),
        )
        .route(
            "/api/event-streaming/materialized_views",
            get(handlers::event_streaming::list_materialized_views),
        )
        .route(
            "/api/event-streaming/status",
            get(handlers::event_streaming::get_status),
        )
        // Phase 6.3: EventLogSink sync management
        .route(
            "/api/event-streaming/sync/start",
            post(handlers::event_streaming::start_sync),
        )
        .route(
            "/api/event-streaming/sync/stop",
            post(handlers::event_streaming::stop_sync),
        )
        .route(
            "/api/event-streaming/sync/status",
            get(handlers::event_streaming::get_sync_status),
        );

    // Phase 7.6: GraphStreaming HTTP endpoints
    #[cfg(all(feature = "event-first", feature = "event-streaming"))]
    let operator_routes = operator_routes
        .route(
            "/api/graph-streaming/projections",
            get(nexora_graphstreaming::list_projections),
        )
        .route(
            "/api/graph-streaming/metrics",
            get(nexora_graphstreaming::get_projection_metrics),
        );

    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    let operator_routes = operator_routes.route(
        "/api/event-streaming/cluster",
        get(handlers::event_streaming::get_cluster_status),
    );

    #[cfg(all(feature = "event-streaming", feature = "library"))]
    let operator_routes = operator_routes
        .route(
            "/api/event-streaming/cluster/status",
            get(handlers::distributed_cluster::get_cluster_status),
        )
        .route(
            "/api/event-streaming/cluster/nodes",
            get(handlers::distributed_cluster::list_cluster_nodes),
        )
        .route(
            "/api/event-streaming/cluster/distributed",
            get(handlers::event_streaming::get_distributed_library_status),
        );

    #[cfg(feature = "event-streaming")]
    let iceberg_routes = handlers::iceberg_catalog::routes();

    let mut app = Router::new().merge(public_routes);

    // Add cluster stats endpoint if in cluster mode

    // Add cluster stats endpoint if in cluster mode
    if let Some(ref cm) = cluster_manager {
        let cm_stats = cm.clone();
        app = app.route(
            "/api/cluster/stats",
            get(move || {
                let cm = cm_stats.clone();
                async move {
                    let stats = cm.stats().await;
                    axum::Json(serde_json::to_value(&stats).unwrap_or_default())
                }
            }),
        );

        // Dynamic membership: add a node to the cluster and rebalance.
        // Body: {"node_id": "node-d", "graph_addr": "127.0.0.1:7030"}.
        // Returns the shard reassignments the rebalance produced.
        let cm_add = cm.clone();
        app = app.route(
            "/api/cluster/add-node",
            post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let cm = cm_add.clone();
                async move {
                    let node_id = body.get("node_id").and_then(|v| v.as_str());
                    let graph_addr = body.get("graph_addr").and_then(|v| v.as_str());
                    let (node_id, graph_addr) = match (node_id, graph_addr) {
                        (Some(n), Some(a)) => (n.to_string(), a.to_string()),
                        _ => {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(serde_json::json!({
                                    "error": "body must contain string fields node_id and graph_addr"
                                })),
                            );
                        }
                    };
                    match cm.add_node(node_id.clone(), graph_addr).await {
                        Ok(moves) => (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::json!({
                                "node_id": node_id,
                                "reassignments": moves.len(),
                                "moved": moves.iter().map(|(s, old, new)| serde_json::json!({
                                    "shard": s, "old_owner": old, "new_owner": new
                                })).collect::<Vec<_>>(),
                            })),
                        ),
                        Err(e) => (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            axum::Json(serde_json::json!({ "error": e.to_string() })),
                        ),
                    }
                }
            }),
        );

        // Dynamic membership: remove a node from the cluster and rebalance.
        // Body: {"node_id": "node-d"}.
        let cm_rm = cm.clone();
        app = app.route(
            "/api/cluster/remove-node",
            post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let cm = cm_rm.clone();
                async move {
                    let node_id = match body.get("node_id").and_then(|v| v.as_str()) {
                        Some(n) => n.to_string(),
                        None => {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(serde_json::json!({
                                    "error": "body must contain string field node_id"
                                })),
                            );
                        }
                    };
                    match cm.remove_node(&node_id).await {
                        Ok(moves) => (
                            axum::http::StatusCode::OK,
                            axum::Json(serde_json::json!({
                                "node_id": node_id,
                                "reassignments": moves.len(),
                            })),
                        ),
                        Err(e) => (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            axum::Json(serde_json::json!({ "error": e.to_string() })),
                        ),
                    }
                }
            }),
        );

        // Add Raft stats endpoint if Raft is enabled
        if let Some(ref rh) = raft_handler {
            let rh = rh.clone();
            app = app.route(
                "/api/cluster/raft",
                get(move || {
                    let rh = rh.clone();
                    async move {
                        let commit_idx = rh.replicator().commit_index().await;
                        let last_applied = rh.replicator().last_applied().await;
                        let followers = rh.replicator().follower_count().await;
                        axum::Json(serde_json::json!({
                            "commit_index": commit_idx,
                            "last_applied": last_applied,
                            "follower_count": followers,
                        }))
                    }
                }),
            );
        }
    }

    // Add Iceberg REST catalog routes if event-streaming is enabled
    #[cfg(feature = "event-streaming")]
    {
        let state_arc = Arc::new(state.clone());
        app = app.nest("/api/iceberg/catalog", iceberg_routes.with_state(state_arc));
    }

    // Apply RBAC to admin routes if auth is enabled
    let admin_routes = if effective_require_auth {
        // require_role middleware needs Extension<Arc<Auth>>
        // Since auth Extension is already layered on the full app, the admin routes
        // will inherit it when merged. We add the role check middleware here.
        admin_routes.route_layer(axum::middleware::from_fn(auth::require_role(
            auth::Role::Admin,
        )))
    } else {
        admin_routes
    };

    // Apply RBAC to operator routes if auth is enabled
    let operator_routes = if effective_require_auth {
        // Operator routes require at least Operator role (blocks readonly users from writes)
        operator_routes.route_layer(axum::middleware::from_fn(auth::require_role(
            auth::Role::Operator,
        )))
    } else {
        operator_routes
    };

    app = app.merge(operator_routes).merge(admin_routes);

    // Security layers (applied innermost to outermost)
    app = app
        .layer(axum::middleware::from_fn(security::security_headers))
        .layer(axum::middleware::from_fn(security::audit_log));

    // Rate limiter — configurable via CLI
    let app = if cli.rate_limit {
        let rate_limiter = security::RateLimiter::new(security::RateLimitConfig {
            rate: cli.rate_limit_rate,
            burst: cli.rate_limit_burst,
            ..Default::default()
        });
        tracing::info!(
            "   Rate limit: {:.0} req/s, burst {}",
            cli.rate_limit_rate,
            cli.rate_limit_burst
        );
        app.layer(axum::middleware::from_fn(security::rate_limit))
            .layer(axum::extract::Extension(rate_limiter))
    } else {
        tracing::warn!("   Rate limit: DISABLED");
        app
    };

    let app = app
        .layer(axum::middleware::from_fn(request_id::request_id_middleware))
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(16 * 1024 * 1024)) // 16MB max body
        .layer(cors);

    // Conditionally apply auth middleware
    let app = if effective_require_auth {
        tracing::info!("   Auth:   enabled (HMAC-SHA256)");
        app.layer(axum::middleware::from_fn(auth::require_auth))
            .layer(axum::extract::Extension(Arc::new(auth::Auth::new(
                &auth_secret,
            ))))
    } else {
        tracing::warn!("   Auth:   DISABLED (set --require-auth to enable)");
        app
    };

    // Clone query_pool before moving state into with_state
    let query_pool_for_pg = state.query_pool.clone();

    let app = app.with_state(state);

    // Handle --gen-tls-cert: generate self-signed cert and exit
    if let Some(ref dir) = cli.gen_tls_cert {
        generate_tls_cert(dir)?;
        return Ok(());
    }

    // ============================================================
    // Start PostgreSQL wire protocol server (optional)
    // ============================================================
    let pg_server = if let Some(pg_port) = cli.pg_port {
        let pg_config = nexora_pgwire::PgConfig {
            port: pg_port,
            bind_addr: cli.pg_bind.clone(),
            trust: cli.pg_trust,
            users_file: cli.pg_users.as_ref().map(|p| p.display().to_string()),
            max_connections: cli.pg_max_connections,
            idle_timeout_secs: cli.pg_idle_timeout,
            server_version: "14.0".to_string(),
            tls_cert: cli
                .pg_tls_cert
                .as_ref()
                .map(|path| path.display().to_string()),
            tls_key: cli
                .pg_tls_key
                .as_ref()
                .map(|path| path.display().to_string()),
            shutdown_grace_secs: 10,
        };
        // Pass the cluster router so PG-wire routes whole-graph queries to shard
        // owners in cluster mode. In single-node mode app_router is None and the
        // PG path executes against the local graph exactly as before.
        let server = nexora_pgwire::spawn_pg_server_with_router(
            graph.clone(),
            mv_manager.clone(),
            Some(sq_manager.clone()),
            app_router.clone(),
            app_replication_progress.clone(),
            query_pool_for_pg,
            #[cfg(feature = "event-first")]
            event_store.clone(),
            pg_config,
        )
        .await?;
        tracing::info!("   PG:     listening on {}", server.local_addr());
        if cli.pg_trust {
            tracing::warn!("   PG:     WARNING: trust authentication (no password)");
        }
        if app_router.is_some() {
            tracing::info!("   PG:     cluster routing enabled (distributed query planner)");
        }
        Some(server)
    } else {
        None
    };

    let protocol = if let (Some(ref cert), Some(ref key)) = (&cli.tls_cert, &cli.tls_key) {
        tracing::info!(
            "   TLS:    enabled (cert={}, key={})",
            cert.display(),
            key.display()
        );
        start_tls_server(app, &cli.host, cli.port, cert, key, shutdown_ws).await
    } else {
        let addr = format!("{}:{}", cli.host, cli.port);
        tracing::info!("   HTTP:   listening on {addr}");
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        let graceful =
            axum::serve(listener, app).with_graceful_shutdown(shutdown_signal(shutdown_ws));
        graceful.await.map_err(Into::into)
    };
    // Stop and drain PG connections before the final persistence pass. Do this
    // even when the HTTP listener exits with an error.
    if let Some(server) = pg_server {
        server.shutdown().await?;
        tracing::info!("PG server shut down");
    }
    protocol?;
    // The server has stopped accepting requests and drained in-flight handlers,
    // so no mutation can race with the final persistence pass.
    tracing::info!("Flushing active nodes...");
    graph_for_shutdown.flush_all_nodes().await?;

    // Shutdown cluster manager if active
    if let Some(ref cm) = cluster_manager {
        cm.shutdown();
        tracing::info!("Cluster manager shut down");
    }

    // Shutdown Raft handler if active
    if let Some(ref rh) = raft_handler {
        rh.shutdown();
        tracing::info!("Raft handler shut down");
    }

    // Shutdown embedded RisingWave if active
    #[cfg(all(feature = "event-streaming", feature = "embedded"))]
    if let Some(embedded) = embedded_event_streaming {
        tracing::info!("Shutting down embedded event streaming engine...");
        if let Err(e) = embedded.shutdown().await {
            tracing::error!("Failed to shutdown embedded event streaming engine: {}", e);
        } else {
            tracing::info!("Embedded event streaming engine shut down");
        }
    }

    // Shutdown in-process library RisingWave if active
    #[cfg(all(feature = "event-streaming", feature = "library"))]
    if let Some(rw) = library_event_streaming {
        tracing::info!("Shutting down in-process event streaming engine...");
        if let Err(e) = rw.shutdown().await {
            tracing::error!("Failed to shutdown library event streaming engine: {}", e);
        } else {
            tracing::info!("In-process event streaming engine shut down");
        }
    }

    // Phase 6.3: Shutdown all EventLogSink tasks
    #[cfg(feature = "event-streaming")]
    {
        let sinks = state.event_sinks.read().await;
        let count = sinks.len();
        if count > 0 {
            tracing::info!("Stopping {} EventLogSink task(s)...", count);
            for (mv_name, handle) in sinks.iter() {
                tracing::debug!("Aborting EventLogSink for MV: {}", mv_name);
                handle.abort();
            }
            drop(sinks);
            tracing::info!("All EventLogSink tasks stopped");
        }
    }

    tracing::info!("🛑 DeepStreaming shutdown complete");
    Ok(())
}

/// Create a property change callback that triggers SQ evaluation.
///
/// GAP-3: the SQ→MV bridge is now driven by the SQ result broadcast channel
/// (subscribed at startup), so this callback no longer needs a bridge handle.
fn make_sq_callback(
    sq_manager: &Arc<StandingQueryManager>,
    metrics: &Arc<metrics::Metrics>,
) -> nexora_core::graph::PropertyChangeCallback {
    let metrics = metrics.clone();
    let sqm2 = sq_manager.clone();
    Arc::new(
        move |qid: nexora_id::NexoraId,
              key: String,
              value: nexora_id::PropertyValue,
              props: std::collections::HashMap<String, nexora_id::PropertyValue>| {
            let sqm = sqm2.clone();
            let metrics = metrics.clone();
            Box::pin(async move {
                // Process Standing Query matches
                let matched = sqm.on_property_change(&qid, &key, &value, &props).await;
                if matched > 0 {
                    metrics.inc_sq_matches(matched as u64);
                    // Notify connected SQ WebSocket clients of new matches.
                    let event = serde_json::json!({
                        "type": "SqMatch",
                        "matches": matched,
                    });
                    if let Ok(json) = serde_json::to_string(&event) {
                        publish_sq_event(json).await;
                    }
                }
                metrics.inc_events(1);
            })
        },
    )
}

/// P0.5: Mutation callback that dispatches NodeChangeEvent to the
/// appropriate SQ manager method (label added / edge added / etc.).
fn make_sq_mutation_callback(
    sq_manager: &Arc<StandingQueryManager>,
    metrics: &Arc<metrics::Metrics>,
    seal_tx: Option<tokio::sync::mpsc::UnboundedSender<nexora_fragment::SealEvent>>,
) -> nexora_core::graph::GraphMutationCallback {
    let sqm = sq_manager.clone();
    let metrics = metrics.clone();
    Arc::new(
        move |qid: nexora_id::NexoraId, event: nexora_core::event::NodeChangeEvent| {
            let sqm = sqm.clone();
            let metrics = metrics.clone();
            let seal_tx = seal_tx.clone();
            Box::pin(async move {
                // Feed the sealing pipeline (P2-B): forward property/edge sets as
                // SealEvents so historical state accumulates into fragments. Only
                // when tiered storage is enabled (seal_tx is Some). Non-blocking
                // send on an unbounded channel — never stalls the write path.
                if let Some(tx) = &seal_tx {
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0);
                    match &event {
                        nexora_core::event::NodeChangeEvent::PropertySet { key, value } => {
                            let _ = tx.send(nexora_fragment::SealEvent::PropertySet {
                                qid: qid.clone(),
                                key: key.as_str().to_string(),
                                // Plain JSON (not serde's tagged enum form) so the
                                // sealed record round-trips through time-travel,
                                // which expects bare values.
                                value: handlers::pv_to_json(value),
                                ts_us: now_us,
                            });
                        }
                        nexora_core::event::NodeChangeEvent::EdgeAdded { edge } => {
                            let _ = tx.send(nexora_fragment::SealEvent::EdgeAdded {
                                source: qid.clone(),
                                edge_type: edge.edge_type.as_str().to_string(),
                                direction: match edge.direction {
                                    nexora_value::EdgeDirection::Out => "out".to_string(),
                                    nexora_value::EdgeDirection::In => "in".to_string(),
                                },
                                target: edge.other.clone(),
                                ts_us: now_us,
                            });
                        }
                        _ => {}
                    }
                }
                let matched = match &event {
                    nexora_core::event::NodeChangeEvent::LabelAdded { label } => {
                        sqm.on_label_added(&qid, label.as_str()).await
                    }
                    nexora_core::event::NodeChangeEvent::LabelRemoved { label } => {
                        sqm.on_label_removed(&qid, label.as_str()).await
                    }
                    nexora_core::event::NodeChangeEvent::EdgeAdded { edge } => {
                        sqm.on_edge_added(&qid, edge.edge_type.as_str(), &edge.other)
                            .await
                    }
                    nexora_core::event::NodeChangeEvent::EdgeRemoved { edge } => {
                        sqm.on_edge_removed(&qid, edge.edge_type.as_str(), &edge.other)
                            .await
                    }
                    nexora_core::event::NodeChangeEvent::NodeDeleted { .. } => {
                        sqm.cleanup_node(&qid).await;
                        0
                    }
                    _ => 0,
                };
                if matched > 0 {
                    metrics.inc_sq_matches(matched as u64);
                    let event = serde_json::json!({
                        "type": "SqMatch",
                        "matches": matched,
                    });
                    if let Ok(json) = serde_json::to_string(&event) {
                        publish_sq_event(json).await;
                    }
                }
                metrics.inc_events(1);
            })
        },
    )
}

/// Serve the embedded dashboard (static/dashboard.html).
async fn dashboard() -> Html<&'static str> {
    Html(include_str!("static/dashboard.html"))
}

/// Serve the legacy test page.
async fn test_page() -> Html<&'static str> {
    Html(include_str!("static/test.html"))
}

/// Serve the SDK documentation page.
async fn sdk_docs_page() -> Html<&'static str> {
    Html(include_str!("static/sdk-docs.html"))
}
/// Print prominent ASCII-boxed security warnings for insecure configurations.
/// This ensures operators cannot miss critical security issues at startup.
fn print_security_warnings(cli: &Cli, auth_secret: &str) {
    let mut dangers: Vec<(&str, &str, &str)> = Vec::new(); // (level, message, fix)

    // Check: auth disabled
    let actual_require_auth = cli.require_auth && !cli.allow_unauthenticated;
    if !actual_require_auth {
        dangers.push((
            "CRITICAL",
            "Authentication is DISABLED — anyone can access the API",
            "Remove --allow-unauthenticated flag (or use --require-auth)",
        ));
    }

    // Check: default auth secret (hard block in production)
    if actual_require_auth && auth_secret == "nexora-dev-secret-change-me" {
        // The default secret is published in the source, so anyone can forge an
        // admin token signed with it. Refuse to start on it whenever the server
        // is network-exposed (non-loopback bind) or in an explicit production /
        // strict-security context — unless the operator opts out with
        // --insecure-dev. Only a loopback-bound dev run gets the soft warning.
        let is_loopback = cli
            .host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
        let is_production = std::env::var("NEXORA_ENV")
            .map(|v| v == "production" || v == "prod")
            .unwrap_or(false)
            || cli.strict_security;
        let must_block = !cli.insecure_dev && (is_production || !is_loopback);

        if must_block {
            eprintln!();
            eprintln!("  {}", "=".repeat(62));
            eprintln!("  FATAL: Default authentication secret not allowed");
            eprintln!("  {}", "=".repeat(62));
            eprintln!("  The default secret 'nexora-dev-secret-change-me' is public");
            eprintln!("  in the source — anyone can forge an admin token with it. It");
            eprintln!("  is forbidden when the server is network-exposed (non-loopback");
            eprintln!("  --host) or in production / strict-security mode.");
            eprintln!();
            eprintln!("  Fix: Set a strong random secret via:");
            eprintln!("    --auth-secret <your-secret>");
            eprintln!("    or NEXORA_AUTH_SECRET environment variable");
            eprintln!();
            eprintln!("  Generate a secure secret:");
            eprintln!("    openssl rand -hex 32");
            eprintln!();
            eprintln!("  (Local dev only: --insecure-dev bypasses this check.)");
            eprintln!("  {}", "=".repeat(62));
            eprintln!();
            std::process::exit(1);
        }

        dangers.push((
            "CRITICAL",
            "Using default auth secret — CHANGE IT before production!",
            "Set --auth-secret <your-secret> or NEXORA_AUTH_SECRET env var",
        ));
    }

    // Check: WAL encryption disabled (only relevant if WAL is enabled)
    if !cli.no_wal && !cli.encrypt_wal {
        dangers.push((
            "WARNING",
            "WAL encryption is disabled — data at rest is not encrypted",
            "Add --encrypt-wal to enable WAL encryption",
        ));
    }

    // Check: CORS wildcard (only warn if explicitly set to *)
    if cli.cors_origin == "*" {
        dangers.push((
            "WARNING",
            "CORS origin is '*' — allows any website to make requests",
            "Set --cors-origin https://your-domain.com or 'none' to disable",
        ));
    }

    // Check: no TLS (CRITICAL in strict mode)
    let has_tls = cli.tls_cert.is_some() || cli.gen_tls_cert.is_some();
    if !has_tls {
        let is_production = std::env::var("NEXORA_ENV")
            .map(|v| v == "production" || v == "prod")
            .unwrap_or(false);

        let level = if is_production || cli.strict_security {
            "CRITICAL"
        } else {
            "WARNING"
        };

        dangers.push((
            level,
            "TLS is not enabled — API traffic is unencrypted (HTTP)",
            "Use --tls-cert/--tls-key or --gen-tls-cert for HTTPS",
        ));
    }

    // Check: rate limiting disabled
    if !cli.rate_limit {
        dangers.push((
            "WARNING",
            "Rate limiting is disabled — API is vulnerable to abuse",
            "Add --rate-limit to enable rate limiting",
        ));
    }

    if dangers.is_empty() {
        return;
    }

    // Check if strict security mode is requested (CLI flag or env var)
    let strict = cli.strict_security
        || std::env::var("NEXORA_STRICT_SECURITY")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

    let has_critical = dangers.iter().any(|(level, _, _)| *level == "CRITICAL");

    eprintln!();
    eprintln!("  {}", "=".repeat(62));
    eprintln!(
        "  {}  SECURITY WARNINGS  {}",
        "=".repeat(20),
        "=".repeat(20)
    );
    eprintln!("  {}", "=".repeat(62));

    for (level, msg, fix) in &dangers {
        let icon = if *level == "CRITICAL" { "[!]" } else { "[*]" };
        eprintln!("  {} {}: {}", icon, level, msg);
        eprintln!("       Fix: {}", fix);
    }

    eprintln!("  {}", "=".repeat(62));

    if strict && has_critical {
        eprintln!("  STRICT SECURITY MODE — refusing to start with CRITICAL issues.");
        eprintln!("  Fix the issues above or remove --strict-security / unset");
        eprintln!("  NEXORA_STRICT_SECURITY to override (NOT recommended in production).");
        eprintln!();
        std::process::exit(1);
    }

    if !strict {
        eprintln!("  Set --strict-security or NEXORA_STRICT_SECURITY=true to refuse");
        eprintln!("  starting with CRITICAL issues.");
    }
    eprintln!();
}

async fn shutdown_signal(shutdown_ws: Arc<tokio::sync::Notify>) {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => { tracing::info!("Received Ctrl+C"); }
            _ = sigterm.recv() => { tracing::info!("Received SIGTERM"); }
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.ok();
        tracing::info!("Received Ctrl+C");
    }
    // Notify WebSocket connections to close
    shutdown_ws.notify_waiters();
}

/// Generate a self-signed TLS certificate and private key for development use.
fn generate_tls_cert(dir: &Path) -> anyhow::Result<()> {
    use rcgen::{CertificateParams, KeyPair};
    use std::fs;

    fs::create_dir_all(dir)?;

    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");

    // Generate a self-signed certificate valid for localhost
    let mut params = CertificateParams::new(vec!["localhost".to_string()])?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "nexora development certificate");

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    // Write certificate
    fs::write(&cert_path, cert.pem())?;
    // Write private key
    fs::write(&key_path, key_pair.serialize_pem())?;

    // Set restrictive permissions on private key (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&key_path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(&key_path, perms)?;
    }

    tracing::info!(
        "✅ Generated self-signed TLS certificate:\n   cert: {}\n   key:  {}",
        cert_path.display(),
        key_path.display()
    );
    tracing::warn!(
        "⚠️  Self-signed certificates are for DEVELOPMENT only. Use a real CA in production."
    );

    Ok(())
}

/// Start the Axum server with TLS (HTTPS + WSS).
async fn start_tls_server(
    app: Router,
    host: &str,
    port: u16,
    cert_path: &Path,
    key_path: &Path,
    shutdown_ws: Arc<tokio::sync::Notify>,
) -> anyhow::Result<()> {
    use axum_server::tls_rustls::RustlsConfig;

    let addr = format!("{host}:{port}");
    tracing::info!("   HTTPS:  listening on {addr}");

    // Load TLS configuration from PEM files
    let tls_config = RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to load TLS certificate from {} / {}: {e}",
                cert_path.display(),
                key_path.display()
            )
        })?;

    // Create axum_server handle for graceful shutdown
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();

    // Spawn shutdown signal handler
    let shutdown_signal_handle = shutdown_signal(shutdown_ws);
    tokio::spawn(async move {
        shutdown_signal_handle.await;
        tracing::info!("Shutting down TLS server...");
        shutdown_handle.shutdown();
    });

    // Bind with TLS
    axum_server::bind_rustls(addr.parse()?, tls_config)
        .handle(handle)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

/// Reconcile a single committed ontology from the Raft state machine's store on
/// startup (used after a snapshot-install catch-up, where the domain landed in
/// the store but no per-entry activation event fired). Deserializes the package
/// and activates it: register in OntologyManager, ensure event tables, update
/// router, schedule views. Idempotent.
#[cfg(feature = "event-first")]
async fn reconcile_ontology(
    ontology_manager: &Arc<nexora_core::ontology_manager::OntologyManager>,
    event_store: Option<&Arc<nexora_eventlog::EventLogStore>>,
    event_router: Option<&Arc<nexora_eventlog::TopicRouter>>,
    refresh_scheduler: Option<&Arc<nexora_eventlog::RefreshScheduler>>,
    domain: &str,
    pkg_json: &[u8],
) -> Result<(), String> {
    let pkg: nexora_core::domain_package::DomainPackage =
        serde_json::from_slice(pkg_json).map_err(|e| format!("parse: {e}"))?;
    ontology_manager
        .create(pkg.clone())
        .await
        .map_err(|e| format!("register: {e}"))?;
    if let (Some(store), Some(router)) = (event_store, event_router) {
        for m in &pkg.mappings {
            if let Err(e) = store.ensure_table_from_domain(&m.source, &pkg).await {
                tracing::warn!(
                    "reconcile '{}': ensure table '{}' failed: {}",
                    domain,
                    m.source,
                    e
                );
            }
        }
        router.apply_domain_package(&pkg);
        if let Some(scheduler) = refresh_scheduler {
            handlers::ontology::schedule_domain_views(scheduler, &pkg).await;
        }
    }
    tracing::info!("Reconciled committed ontology '{}'", domain);
    Ok(())
}

/// Drain the ontology-activation channel from the Raft state machine's apply
/// callback. For each committed Put: deserialize + register in OntologyManager +
/// ensure event tables + update router + schedule views. For Delete: remove from
/// OntologyManager + drop router rules. Runs until the channel closes (cluster
/// shutdown). Activation is idempotent so replays are safe.
#[cfg(feature = "event-first")]
async fn drain_ontology_activations(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<nexora_zenoh::control_raft_sm::OntologyActivation>,
    ontology_manager: Arc<nexora_core::ontology_manager::OntologyManager>,
    event_store: Option<Arc<nexora_eventlog::EventLogStore>>,
    event_router: Option<Arc<nexora_eventlog::TopicRouter>>,
    refresh_scheduler: Option<Arc<nexora_eventlog::RefreshScheduler>>,
) {
    while let Some(event) = rx.recv().await {
        match event {
            nexora_zenoh::control_raft_sm::OntologyActivation::Put { domain, pkg_json } => {
                if let Err(e) = reconcile_ontology(
                    &ontology_manager,
                    event_store.as_ref(),
                    event_router.as_ref(),
                    refresh_scheduler.as_ref(),
                    &domain,
                    &pkg_json,
                )
                .await
                {
                    tracing::error!("Activation failed for ontology '{}': {}", domain, e);
                }
            }
            nexora_zenoh::control_raft_sm::OntologyActivation::Delete { domain } => {
                let pkg_before = ontology_manager.get(&domain).await;
                if let Err(e) = ontology_manager.remove(&domain).await {
                    tracing::error!("Deactivation: remove '{}' failed: {}", domain, e);
                } else {
                    if let (Some(router), Some(pkg)) = (event_router.as_ref(), pkg_before) {
                        router.remove_domain_package(&pkg);
                    }
                    tracing::info!("Deactivated ontology '{}'", domain);
                }
            }
        }
    }
}
