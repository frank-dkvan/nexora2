//! Main RisingWave module that coordinates all components.

use crate::config::EventStreamingConfig;
use crate::error::Result;
use crate::event_sink::ColumnValue;
use crate::event_streaming_trait::EventStreamingOperations;
use crate::frontend_wrapper::FrontendWrapper;
use crate::meta_wrapper::MetaNode;
use async_trait::async_trait;
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
/// use nexora_risingwave::{EventStreamingModule, EventStreamingConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = EventStreamingConfig::new();
/// let rw = EventStreamingModule::start(config).await?;
///
/// rw.execute_ddl("CREATE SOURCE ...").await?;
/// let results = rw.query_mv("SELECT * FROM ...").await?;
///
/// rw.shutdown().await?;
/// # Ok(())
/// # }
/// ```
pub struct EventStreamingModule {
    meta: Arc<MetaNode>,
    frontend: Arc<FrontendWrapper>,
    config: EventStreamingConfig,
}

impl EventStreamingModule {
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
    pub async fn start(config: EventStreamingConfig) -> Result<Self> {
        info!("Starting RisingWave module");

        // Phase 3: Start Meta and Frontend with simplified wrappers
        let meta = Arc::new(MetaNode::new(config.meta_addr));
        meta.start().await?;

        let frontend = Arc::new(FrontendWrapper::new(config.frontend_addr, config.meta_addr));
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
    /// # use nexora_risingwave::EventStreamingModule;
    /// # async fn example(rw: &EventStreamingModule) -> Result<(), Box<dyn std::error::Error>> {
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
    /// # use nexora_risingwave::EventStreamingModule;
    /// # async fn example(rw: &EventStreamingModule) -> Result<(), Box<dyn std::error::Error>> {
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

    /// List all sources from RisingWave catalog
    ///
    /// # Returns
    ///
    /// A list of all sources (Kafka, Kinesis, etc.) registered in RisingWave.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::EventStreamingModule;
    /// # async fn example(rw: &EventStreamingModule) -> Result<(), Box<dyn std::error::Error>> {
    /// let sources = rw.list_sources().await?;
    /// for source in sources {
    ///     println!("Source: {} ({})", source.name, source.connector);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_sources(&self) -> Result<Vec<crate::catalog::SourceInfo>> {
        let catalog = crate::catalog::CatalogClient::new(self.config.frontend_addr);
        catalog.list_sources().await
    }

    /// List all materialized views from RisingWave catalog
    ///
    /// # Returns
    ///
    /// A list of all materialized views registered in RisingWave.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::EventStreamingModule;
    /// # async fn example(rw: &EventStreamingModule) -> Result<(), Box<dyn std::error::Error>> {
    /// let mvs = rw.list_materialized_views().await?;
    /// for mv in mvs {
    ///     println!("MV: {}", mv.name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_materialized_views(
        &self,
    ) -> Result<Vec<crate::catalog::MaterializedViewInfo>> {
        let catalog = crate::catalog::CatalogClient::new(self.config.frontend_addr);
        catalog.list_materialized_views().await
    }

    /// Subscribe to materialized view changes (CDC-like streaming)
    ///
    /// Returns a channel receiver that streams change events (Insert, Update, Delete)
    /// whenever the materialized view state changes.
    ///
    /// # Phase 6 Implementation
    ///
    /// Phase 6 provides a polling-based implementation. Future versions will use
    /// RisingWave's native CDC connector for more efficient streaming.
    ///
    /// # Arguments
    ///
    /// - `mv_name`: Name of the materialized view to subscribe to
    ///
    /// # Returns
    ///
    /// A receiver that yields Change events as they occur.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::{EventStreamingModule, Change};
    /// # async fn example(rw: &EventStreamingModule) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut rx = rw.subscribe_mv("enriched_events").await?;
    ///
    /// while let Some(change) = rx.recv().await {
    ///     match change {
    ///         Change::Insert(row) => println!("New row: {:?}", row),
    ///         Change::Update { old, new } => println!("Updated: {:?} -> {:?}", old, new),
    ///         Change::Delete(row) => println!("Deleted: {:?}", row),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn subscribe_mv(
        &self,
        mv_name: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<crate::event_sink::Change>> {
        use crate::event_sink::{Change, Row};
        use std::time::Duration;

        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        // Clone what we need for the spawned task
        let frontend = self.frontend.clone();
        let mv_name = mv_name.to_string();

        tokio::spawn(async move {
            let mut last_row_count = 0;

            loop {
                // Phase 6: Simple polling approach
                // Query the MV for new rows using a watermark-based approach
                let query = format!(
                    "SELECT * FROM {} ORDER BY processing_time DESC LIMIT 100",
                    mv_name
                );

                match frontend.query_mv(&query).await {
                    Ok(result_json) => {
                        // Phase 6: Parse JSON results and convert to Change events
                        // For now, we treat all results as Inserts (simplified)
                        if let Ok(rows) =
                            serde_json::from_str::<Vec<serde_json::Value>>(&result_json)
                        {
                            let current_count = rows.len();

                            if current_count > last_row_count {
                                // New rows detected - send as Insert events
                                for row_json in rows.iter().skip(last_row_count) {
                                    if let Some(obj) = row_json.as_object() {
                                        let mut columns = Vec::new();

                                        for (key, value) in obj {
                                            let col_value = json_to_column_value(value);
                                            columns.push((key.clone(), col_value));
                                        }

                                        let row = Row::new(columns);
                                        if tx.send(Change::Insert(row)).await.is_err() {
                                            // Receiver dropped, stop polling
                                            return;
                                        }
                                    }
                                }

                                last_row_count = current_count;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to poll MV {}: {}", mv_name, e);
                    }
                }

                // Poll every 1 second (configurable in future versions)
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        Ok(rx)
    }

    /// Get the RisingWave configuration.
    pub fn config(&self) -> &EventStreamingConfig {
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

#[async_trait]
impl EventStreamingOperations for EventStreamingModule {
    async fn execute_ddl(&self, sql: &str) -> Result<()> {
        self.execute_ddl(sql).await
    }

    async fn query_mv(&self, sql: &str) -> Result<String> {
        self.query_mv(sql).await
    }

    async fn list_sources(&self) -> Result<Vec<crate::catalog::SourceInfo>> {
        self.list_sources().await
    }

    async fn list_materialized_views(&self) -> Result<Vec<crate::catalog::MaterializedViewInfo>> {
        self.list_materialized_views().await
    }

    async fn is_leader(&self) -> bool {
        self.is_leader().await
    }

    async fn list_hosted_iceberg_tables(
        &self,
    ) -> Result<Vec<crate::event_streaming_trait::IcebergTable>> {
        // Client-server mode uses the placeholder FrontendWrapper / CatalogClient,
        // which do not hold a live pgwire connection to a RisingWave frontend, so
        // the hosted Iceberg catalog cannot be read here. The functional path is
        // library mode (LibraryEventStreamingModule), which queries
        // rw_catalog.iceberg_tables directly over pgwire.
        Ok(vec![])
    }
}

/// Helper function to convert JSON value to ColumnValue
fn json_to_column_value(value: &serde_json::Value) -> ColumnValue {
    match value {
        serde_json::Value::Null => ColumnValue::Null,
        serde_json::Value::Bool(b) => ColumnValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    ColumnValue::Int32(i as i32)
                } else {
                    ColumnValue::Int64(i)
                }
            } else if let Some(f) = n.as_f64() {
                ColumnValue::Float64(f)
            } else {
                ColumnValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => {
            // Try to parse as timestamp (RFC3339 format)
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                ColumnValue::Timestamp(dt.with_timezone(&chrono::Utc))
            } else {
                ColumnValue::String(s.clone())
            }
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // Complex types stored as JSON
            ColumnValue::Json(value.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_module_lifecycle() {
        let config = EventStreamingConfig::new()
            .with_meta_addr("127.0.0.1:15690".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14566".parse().unwrap());

        let rw = EventStreamingModule::start(config).await.unwrap();

        assert!(rw.is_leader().await);
        assert!(rw.meta.is_running().await);
        assert!(rw.frontend.is_running().await);

        rw.shutdown().await.unwrap();

        assert!(!rw.meta.is_running().await);
        assert!(!rw.frontend.is_running().await);
    }

    #[tokio::test]
    async fn test_module_ddl() {
        let config = EventStreamingConfig::new()
            .with_meta_addr("127.0.0.1:15691".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14567".parse().unwrap());

        let rw = EventStreamingModule::start(config).await.unwrap();

        // Execute DDL
        let result = rw.execute_ddl("CREATE SOURCE my_source WITH (...)").await;
        assert!(result.is_ok());

        rw.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_module_query() {
        let config = EventStreamingConfig::new()
            .with_meta_addr("127.0.0.1:15692".parse().unwrap())
            .with_frontend_addr("127.0.0.1:14568".parse().unwrap());

        let rw = EventStreamingModule::start(config).await.unwrap();

        // Execute query
        let result = rw.query_mv("SELECT * FROM my_mv").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "[]");

        rw.shutdown().await.unwrap();
    }
}
