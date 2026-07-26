//! Main RisingWave module that coordinates all components.

use crate::config::RisingWaveConfig;
use crate::error::Result;
use crate::frontend_wrapper::FrontendNode;
use crate::meta_wrapper::MetaNode;
use std::sync::Arc;
use tracing::info;

/// Main RisingWave module that coordinates Meta, Frontend, and Compute nodes.
///
/// This is the primary API for integrating RisingWave into Nexora.
///
/// # Phase 3 Implementation
///
/// Phase 3 provides a simplified implementation with placeholder logic.
/// Phase 4 will integrate with actual RisingWave components from vendor/.
///
/// # Example
///
/// ```rust,no_run
/// use nexora_risingwave::{RisingWaveModule, RisingWaveConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RisingWaveConfig::new();
/// let rw = RisingWaveModule::start(config).await?;
///
/// rw.execute_ddl("CREATE SOURCE ...").await?;
/// let results = rw.query_mv("SELECT * FROM ...").await?;
///
/// rw.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct RisingWaveModule {
    meta: Arc<MetaNode>,
    frontend: Arc<FrontendNode>,
    config: RisingWaveConfig,
}

impl RisingWaveModule {
    /// Start the RisingWave module with the given configuration.
    ///
    /// This will start:
    /// 1. Meta node (for cluster coordination)
    /// 2. Frontend node (for SQL query processing)
    /// 3. Optionally, Compute node (if configured)
    ///
    /// # Arguments
    ///
    /// - `config`: RisingWave configuration
    ///
    /// # Returns
    ///
    /// A running RisingWave module instance.
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to start.
    pub async fn start(config: RisingWaveConfig) -> Result<Self> {
        info!("Starting RisingWave module");

        // Phase 3: Start Meta and Frontend with simplified wrappers
        let meta = Arc::new(MetaNode::new(config.meta_addr));
        meta.start().await?;

        let frontend = Arc::new(FrontendNode::new(config.frontend_addr, config.meta_addr));
        frontend.start().await?;

        // Phase 4: Will also start Compute node if configured

        info!("RisingWave module started successfully");

        Ok(Self {
            meta,
            frontend,
            config,
        })
    }

    /// Execute a SQL DDL statement.
    ///
    /// DDL statements are sent to the Frontend node, which coordinates
    /// with the Meta node to update the catalog.
    ///
    /// # Arguments
    ///
    /// - `sql`: The SQL DDL statement
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::RisingWaveModule;
    /// # async fn example(rw: &RisingWaveModule) -> Result<(), Box<dyn std::error::Error>> {
    /// rw.execute_ddl("
    ///     CREATE SOURCE my_kafka_source WITH (
    ///         connector = 'kafka',
    ///         topic = 'events',
    ///         properties.bootstrap.server = 'localhost:9092'
    ///     ) FORMAT PLAIN ENCODE JSON
    /// ").await?;
    ///
    /// rw.execute_ddl("
    ///     CREATE MATERIALIZED VIEW enriched_events AS
    ///     SELECT id, data, processing_time
    ///     FROM my_kafka_source
    /// ").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        self.frontend.execute_ddl(sql).await
    }

    /// Query a materialized view.
    ///
    /// Queries are executed by the Frontend node and may involve
    /// compute nodes for data retrieval.
    ///
    /// # Arguments
    ///
    /// - `sql`: The SQL query
    ///
    /// # Returns
    ///
    /// Query results as a JSON string (Phase 3).
    /// Phase 4 will return structured row data.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::RisingWaveModule;
    /// # async fn example(rw: &RisingWaveModule) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = rw.query_mv("
    ///     SELECT id, COUNT(*) as count
    ///     FROM enriched_events
    ///     GROUP BY id
    ///     LIMIT 10
    /// ").await?;
    /// println!("Results: {}", results);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_mv(&self, sql: &str) -> Result<String> {
        self.frontend.query_mv(sql).await
    }

    /// Check if the Meta node is the leader.
    ///
    /// In single-node mode, always returns true.
    /// In HA mode, returns true only if this Meta node is the leader.
    pub async fn is_leader(&self) -> bool {
        self.meta.is_leader().await
    }

    /// Get the RisingWave configuration.
    pub fn config(&self) -> &RisingWaveConfig {
        &self.config
    }

    /// Shutdown the RisingWave module gracefully.
    ///
    /// This will stop all components in reverse order:
    /// 1. Compute node (if running)
    /// 2. Frontend node
    /// 3. Meta node
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down RisingWave module");

        // Phase 4: Will also stop Compute node if running

        self.frontend.stop().await?;
        self.meta.stop().await?;

        info!("RisingWave module shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_module_lifecycle() {
        let config = RisingWaveConfig::new()
            .with_meta_addr("127.0.0.1:15690".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14566".parse().unwrap());

        let rw = RisingWaveModule::start(config).await.unwrap();

        assert!(rw.is_leader().await);
        assert!(rw.meta.is_running().await);
        assert!(rw.frontend.is_running().await);

        rw.shutdown().await.unwrap();

        assert!(!rw.meta.is_running().await);
        assert!(!rw.frontend.is_running().await);
    }

    #[tokio::test]
    async fn test_module_ddl() {
        let config = RisingWaveConfig::new()
            .with_meta_addr("127.0.0.1:15691".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14567".parse().unwrap());

        let rw = RisingWaveModule::start(config).await.unwrap();

        // Execute DDL
        let result = rw
            .execute_ddl("CREATE SOURCE my_source WITH (...)")
            .await;
        assert!(result.is_ok());

        rw.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_module_query() {
        let config = RisingWaveConfig::new()
            .with_meta_addr("127.0.0.1:15692".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14568".parse().unwrap());

        let rw = RisingWaveModule::start(config).await.unwrap();

        // Execute query
        let result = rw.query_mv("SELECT * FROM my_mv").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "[]");

        rw.shutdown().await.unwrap();
    }
}
