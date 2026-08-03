//! Event-first ingestion: topic-partitioned, append-only Iceberg event tables.
//!
//! Nexora captures each upstream source's original payload verbatim as a
//! [`RawEvent`] and appends it to a business-topic event table **before**
//! projecting it into the graph. The event table is the source of truth; the
//! graph snapshot is a derived projection that can be rebuilt by replaying the
//! log.
//!
//! Storage stack (feature `olap`): the event tables are **Apache Iceberg**
//! tables (hidden `days(event_time)` partitioning, ACID snapshots, schema
//! evolution), written as Parquet via `object_store` and queried through
//! **DataFusion** (`IcebergTableProvider`). See the design doc for why this
//! replaces the earlier hand-rolled `ColumnarFragment` approach.
//!
//! Without `olap` the crate exposes only the format-agnostic [`RawEvent`] +
//! WAL record type, so downstream crates that don't need the event-table
//! machinery don't pay the arrow/datafusion/iceberg compile cost.

/// Re-export: the source-of-truth event record (defined in `nexora-core`).
pub use nexora_core::raw_event::RawEvent;

/// OLAP spike: minimal Iceberg + DataFusion round-trip to validate the stack.
#[cfg(feature = "olap")]
pub mod spike;

/// Storage config: local FS / S3 backend selection (阶段 5)
#[cfg(feature = "olap")]
pub mod storage_config;

/// Event log store: Iceberg table management and append (阶段 1)
#[cfg(feature = "olap")]
pub mod event_log_store;

/// RawEvent → Arrow RecordBatch conversion (阶段 1)
#[cfg(feature = "olap")]
pub mod record_batch_writer;

/// Topic router: topic → destination routing (阶段 1)
#[cfg(feature = "olap")]
pub mod router;

/// Domain loader: load DomainPackage definitions (阶段 1)
#[cfg(feature = "olap")]
pub mod domain_loader;

/// Schema mapper: DomainPackage → Iceberg Schema (阶段 2)
#[cfg(feature = "olap")]
pub mod schema_mapper;

/// DataFusion store: SQL query layer over Iceberg tables (阶段 3)
#[cfg(feature = "olap")]
pub mod datafusion_store;

/// Materialized views: pre-computed aggregations (阶段 4)
#[cfg(feature = "olap")]
pub mod materialized_view;

/// View manager: create and manage materialized views (阶段 4)
#[cfg(feature = "olap")]
pub mod view_manager;

/// View refresher: execute view refresh logic (阶段 4)
#[cfg(feature = "olap")]
pub mod view_refresher;

/// Refresh scheduler: background periodic view refresh (阶段 4)
#[cfg(feature = "olap")]
pub mod refresh_scheduler;

/// Retention: snapshot expiration and retention policy (阶段 5 Phase 2)
#[cfg(feature = "olap")]
pub mod retention;

/// Event-first handler: coordinates event table + graph writes (阶段 1)
#[cfg(feature = "olap")]
pub mod event_first_handler;

/// Microbatch writer: batches events before Iceberg commit (P1-3 优化)
#[cfg(feature = "olap")]
pub mod microbatch_writer;

/// Circuit breaker: protects against cascading failures (P1-2)
#[cfg(feature = "olap")]
pub mod circuit_breaker;

// Re-exports for convenience
#[cfg(feature = "olap")]
pub use datafusion_store::DataFusionEventStore;
#[cfg(feature = "olap")]
pub use domain_loader::DomainLoader;
#[cfg(feature = "olap")]
pub use event_first_handler::EventFirstHandler;
#[cfg(feature = "olap")]
pub use event_log_store::EventLogStore;
#[cfg(feature = "olap")]
pub use materialized_view::{Aggregation, MaterializedView, RefreshMode, ViewTransform};
#[cfg(feature = "olap")]
pub use microbatch_writer::{MicrobatchConfig, MicrobatchWriter};
#[cfg(feature = "olap")]
pub use refresh_scheduler::RefreshScheduler;
#[cfg(feature = "olap")]
pub use retention::{RetentionManager, RetentionPolicy};
#[cfg(feature = "olap")]
pub use router::{Destination, TopicRouter};
#[cfg(feature = "olap")]
pub use schema_mapper::SchemaMapper;
#[cfg(feature = "olap")]
pub use storage_config::StorageConfig;
#[cfg(feature = "olap")]
pub use view_manager::ViewManager;
#[cfg(feature = "olap")]
pub use view_refresher::ViewRefresher;

// Re-export DomainPackage from nexora_core for convenience
#[cfg(feature = "olap")]
pub use nexora_core::DomainPackage;
