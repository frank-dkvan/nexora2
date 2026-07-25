//! PostgreSQL Wire Protocol server for Nexora.
//!
//! Implements PG Wire Protocol v3.0 on top of the `pgwire` crate,
//! enabling psql and other clients configured for PostgreSQL's simple-query
//! protocol to execute SQL against the graph engine.
//!
//! # Architecture
//!
//! ```text
//! psql / simple-query client → :5432 → nexora-pgwire → nexora-sql → GraphService
//! ```
//!
//! This crate is a thin protocol adapter: it handles PG message
//! encoding/decoding and delegates all SQL execution to the
//! existing `nexora-sql` translator and `nexora-cypher` executor.
//!
//! # Phase 1 Features
//!
//! - Simple Query protocol (text-based SQL → immediate response)
//! - Trust authentication (no password — development mode)
//! - SCRAM-SHA-256 password authentication
//! - TLS, connection limits, and idle timeouts
//! - Graceful shutdown
//!
//! # Quick Start
//!
//! ```bash
//! # Start Nexora with PG protocol on port 5432 (trust auth)
//! cargo run --bin nexora -- --profile lite-ephemeral --no-rocksdb --no-wal --pg-port 5432 --pg-trust
//!
//! # Connect with psql
//! psql -h localhost -p 5432 -U admin -d nexora
//! ```

pub mod auth;
pub mod catalog;
pub mod config;
pub mod copy_handler;
pub mod error_mapping;
pub mod event_table_handler;
pub mod extended_query;
pub mod mv_handler;
pub mod pg_catalog;
pub mod server;
pub mod session;
pub mod simple_query;
pub mod type_mapping;

use std::sync::Arc;

/// Shared application state available to all PG connections.
///
/// This is a subset of nexora-app's AppState — only the pieces
/// needed by the PG protocol layer.
#[derive(Clone)]
pub struct PgAppState {
    /// The core graph engine (shared with HTTP API).
    pub graph: Arc<nexora_core::GraphService>,
    /// Materialized View manager for SQL-based MV queries (P0.2)
    pub mv_manager: Arc<nexora_core::materialized_view::MaterializedViewManager>,
    /// Standing Query manager for pattern matching and alerts (P0 fix)
    pub sq_manager: Option<Arc<nexora_standing_query::StandingQueryManager>>,
    /// Cross-node router. `None` in single-node mode — then every read/write
    /// goes straight to the local `graph` (behaviour unchanged). In `--cluster`
    /// mode this routes whole-graph queries to shard owners and merges results,
    /// matching the HTTP API's distributed path. See [`try_distributed_sql`].
    pub router: Option<Arc<nexora_zenoh::router::HybridRouter>>,
    /// Per-session read-after-write tracker (C1). Each PG connection notes its
    /// writes here; distributed reads consult it to ensure a replica has caught
    /// up to the session's last-written seq before serving (no stale reads).
    pub session_tracker: Arc<nexora_zenoh::SessionReadTracker>,
    /// Cluster-wide replication progress (C2). Tracks each replica's applied seq
    /// and quorum commit_index per shard. Distributed reads use this to enforce
    /// ReadConcern::Majority (only fail over to replicas caught up to quorum).
    /// `None` in single-node mode or when replication is not configured.
    pub replication_progress: Option<Arc<nexora_zenoh::ReplicationProgress>>,
    /// FIX C: Query concurrency limiter (shared with HTTP API).
    pub query_pool: Arc<nexora_core::query_pool::QueryPool>,
    /// Whether to use trust authentication (no password required).
    /// WARNING: Only for development/localhost use.
    pub trust_auth: bool,
    /// Registered users for SCRAM-SHA-256 authentication.
    /// Key is username, value is (password, role_name).
    pub users: Arc<std::collections::HashMap<String, (String, String)>>,
    /// Version reported to PostgreSQL clients.
    pub server_version: String,
    /// Event log store for querying Iceberg event tables (event-first mode).
    #[cfg(feature = "event-first")]
    pub event_store: Option<Arc<nexora_eventlog::EventLogStore>>,
}

/// Re-exports for convenience.
pub use config::{PgConfig, PgConfigError};
pub use server::{
    active_connection_count, run_pg_server, spawn_pg_server, spawn_pg_server_with_router,
    PgServerError, PgServerHandle,
};

/// Get the number of currently active PG connections.
pub fn active_connections() -> usize {
    active_connection_count()
}
