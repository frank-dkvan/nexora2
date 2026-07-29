//! Wrapper module for library-mode RisingWave.
//!
//! Provides an `EventStreamingModule`-compatible interface for library-mode
//! RisingWave. Uses `LibraryClient` to connect to the embedded instance.

use crate::error::Result;
use crate::catalog::{MaterializedViewInfo, SourceInfo};
use crate::library_client::LibraryClient;
use crate::event_streaming_trait::EventStreamingOperations;
use async_trait::async_trait;
use std::sync::Arc;

/// EventStreamingModule implementation for library mode.
///
/// This wraps a `LibraryClient` to provide the same API as the standard
/// `EventStreamingModule`, but connects to an embedded RisingWave instance
/// via PostgreSQL protocol.
#[derive(Clone)]
pub struct LibraryEventStreamingModule {
    client: Arc<LibraryClient>,
}

impl LibraryEventStreamingModule {
    /// Create a new library-mode module by connecting to the embedded instance.
    ///
    /// # Arguments
    ///
    /// - `frontend_addr`: The pgwire address (e.g., "127.0.0.1:4566")
    pub async fn connect(frontend_addr: String) -> Result<Self> {
        let client = LibraryClient::connect(frontend_addr).await?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Get frontend address
    pub fn frontend_addr(&self) -> &str {
        self.client.frontend_addr()
    }
}

#[async_trait]
impl EventStreamingOperations for LibraryEventStreamingModule {
    async fn execute_ddl(&self, sql: &str) -> Result<()> {
        self.client.execute_ddl(sql).await
    }

    async fn query_mv(&self, sql: &str) -> Result<String> {
        self.client.query_mv(sql).await
    }

    async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        self.client.list_sources().await
    }

    async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewInfo>> {
        self.client.list_materialized_views().await
    }

    async fn is_leader(&self) -> bool {
        true // Always true in single-node library mode
    }
}
