//! Common trait for Event Streaming modules.
//!
//! Provides a unified interface for both standard and library-mode modules.

use crate::error::Result;
use crate::catalog::{MaterializedViewInfo, SourceInfo};
use async_trait::async_trait;

/// Common interface for Event Streaming operations.
///
/// Implemented by both `EventStreamingModule` (client-server mode) and
/// `LibraryEventStreamingModule` (embedded library mode).
#[async_trait]
pub trait EventStreamingOperations: Send + Sync {
    /// Execute a DDL statement (CREATE SOURCE, CREATE MATERIALIZED VIEW, etc.)
    async fn execute_ddl(&self, sql: &str) -> Result<()>;

    /// Query a materialized view or table
    async fn query_mv(&self, sql: &str) -> Result<String>;

    /// List all sources
    async fn list_sources(&self) -> Result<Vec<SourceInfo>>;

    /// List all materialized views
    async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewInfo>>;

    /// Check if meta node is leader
    async fn is_leader(&self) -> bool;
}
