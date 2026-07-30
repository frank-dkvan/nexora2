//! RisingWave Frontend node wrapper.
//!
//! Phase 3: Simplified implementation with placeholder logic.
//! Phase 4: Full integration with vendor/risingwave Frontend node.

use crate::error::{EventStreamingError, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Wrapper around RisingWave Frontend node.
///
/// The Frontend node is responsible for:
/// - SQL parsing and planning
/// - Query execution coordination
/// - Client connection handling
/// - Materialized view queries
#[derive(Debug)]
pub struct FrontendWrapper {
    addr: SocketAddr,
    meta_addr: SocketAddr,
    state: Arc<RwLock<FrontendState>>,
}

#[derive(Debug)]
struct FrontendState {
    running: bool,
}

impl FrontendWrapper {
    /// Create a new Frontend node wrapper.
    ///
    /// # Arguments
    ///
    /// - `addr`: The address to bind the Frontend node to
    /// - `meta_addr`: The Meta node address to connect to
    ///
    /// # Example
    ///
    /// ```rust
    /// use nexora_risingwave::frontend_wrapper::FrontendWrapper;
    ///
    /// let frontend = FrontendWrapper::new(
    ///     "127.0.0.1:4566".parse().unwrap(),
    ///     "127.0.0.1:5690".parse().unwrap(),
    /// );
    /// ```
    pub fn new(addr: SocketAddr, meta_addr: SocketAddr) -> Self {
        Self {
            addr,
            meta_addr,
            state: Arc::new(RwLock::new(FrontendState { running: false })),
        }
    }

    /// Create a placeholder Frontend wrapper for testing.
    ///
    /// Used by distributed library mode when actual Frontend is not yet initialized.
    pub fn new_placeholder() -> Result<Self> {
        Ok(Self {
            addr: "127.0.0.1:4566".parse().unwrap(),
            meta_addr: "127.0.0.1:5690".parse().unwrap(),
            state: Arc::new(RwLock::new(FrontendState { running: true })),
        })
    }

    /// Start the Frontend node.
    ///
    /// In Phase 3, this is a simplified implementation that just marks
    /// the node as running. Phase 4 will start the actual RisingWave
    /// Frontend node process.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the Frontend node started successfully
    /// - `Err(_)` if startup failed
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if state.running {
            return Err(EventStreamingError::FrontendStartFailed(
                "frontend node already running".to_string(),
            ));
        }

        info!(
            "Starting RisingWave Frontend node on {} (meta: {})",
            self.addr, self.meta_addr
        );

        // Phase 3: Placeholder
        // Phase 4: Will start actual Frontend node:
        //   - Connect to Meta node
        //   - Initialize FrontendService
        //   - Start PostgreSQL wire protocol server
        //   - Start gRPC server for internal communication

        state.running = true;
        Ok(())
    }

    /// Stop the Frontend node gracefully.
    pub async fn stop(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if !state.running {
            return Ok(());
        }

        info!("Stopping RisingWave Frontend node");

        // Phase 3: Placeholder
        // Phase 4: Will stop actual Frontend node

        state.running = false;
        Ok(())
    }

    /// Execute a SQL DDL statement.
    ///
    /// DDL statements include:
    /// - CREATE SOURCE
    /// - CREATE MATERIALIZED VIEW
    /// - CREATE SINK
    /// - DROP statements
    ///
    /// # Arguments
    ///
    /// - `sql`: The SQL DDL statement to execute
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::frontend_wrapper::FrontendWrapper;
    /// # async fn example(frontend: FrontendWrapper) -> Result<(), Box<dyn std::error::Error>> {
    /// frontend.execute_ddl("CREATE SOURCE my_source WITH (...)").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let state = self.state.read().await;
        if !state.running {
            return Err(EventStreamingError::DdlFailed(
                "frontend node not running".to_string(),
            ));
        }

        info!("Executing DDL: {}", sql);

        // Phase 3: Placeholder - just validate SQL is not empty
        if sql.trim().is_empty() {
            return Err(EventStreamingError::DdlFailed(
                "empty SQL statement".to_string(),
            ));
        }

        // Phase 4: Will execute actual DDL:
        //   - Parse SQL using RisingWave parser
        //   - Create execution plan
        //   - Send DDL request to Meta node
        //   - Wait for acknowledgment

        Ok(())
    }

    /// Execute a SQL query on materialized views.
    ///
    /// # Arguments
    ///
    /// - `sql`: The SQL query to execute
    ///
    /// # Returns
    ///
    /// Rows as Vec<Vec<String>> representing the query results.
    /// Phase 4 will return proper typed row structures.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::frontend_wrapper::FrontendWrapper;
    /// # async fn example(frontend: FrontendWrapper) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = frontend.query("SELECT * FROM my_mv LIMIT 10").await?;
    /// println!("Results: {:?}", results);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let state = self.state.read().await;
        if !state.running {
            return Err(EventStreamingError::QueryFailed(
                "frontend node not running".to_string(),
            ));
        }

        info!("Executing query: {}", sql);

        // Phase 3: Placeholder - return empty result set
        if sql.trim().is_empty() {
            return Err(EventStreamingError::QueryFailed(
                "empty SQL statement".to_string(),
            ));
        }

        // Phase 4: Will execute actual query:
        //   - Parse SQL using RisingWave parser
        //   - Create query plan
        //   - Execute query against materialized views
        //   - Return result rows

        Ok(Vec::new())
    }

    /// Execute a SQL query on materialized views (legacy string-based API).
    ///
    /// # Deprecated
    ///
    /// Use `query()` instead, which returns structured rows.
    pub async fn query_mv(&self, sql: &str) -> Result<String> {
        let _rows = self.query(sql).await?;
        Ok("[]".to_string())
    }

    /// Get the Frontend node address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Check if the Frontend node is running.
    pub async fn is_running(&self) -> bool {
        self.state.read().await.running
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_frontend_lifecycle() {
        let frontend = FrontendWrapper::new(
            "127.0.0.1:14566".parse().unwrap(),
            "127.0.0.1:15690".parse().unwrap(),
        );

        assert!(!frontend.is_running().await);
        assert_eq!(frontend.addr().port(), 14566);

        frontend.start().await.unwrap();
        assert!(frontend.is_running().await);

        frontend.stop().await.unwrap();
        assert!(!frontend.is_running().await);
    }

    #[tokio::test]
    async fn test_frontend_ddl() {
        let frontend = FrontendWrapper::new(
            "127.0.0.1:14567".parse().unwrap(),
            "127.0.0.1:15690".parse().unwrap(),
        );

        frontend.start().await.unwrap();

        // Valid DDL should succeed
        let result = frontend
            .execute_ddl("CREATE SOURCE my_source WITH (...)")
            .await;
        assert!(result.is_ok());

        // Empty DDL should fail
        let result = frontend.execute_ddl("").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_frontend_query() {
        let frontend = FrontendWrapper::new(
            "127.0.0.1:14568".parse().unwrap(),
            "127.0.0.1:15690".parse().unwrap(),
        );

        frontend.start().await.unwrap();

        // Valid query should succeed
        let result = frontend.query_mv("SELECT * FROM my_mv").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "[]");

        // Empty query should fail
        let result = frontend.query_mv("").await;
        assert!(result.is_err());
    }
}
