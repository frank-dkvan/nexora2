//! Client wrapper for library-mode RisingWave.
//!
//! When RisingWave is started via `EmbeddedLibrary`, it runs as a full
//! standalone instance inside the process. This module provides a PostgreSQL
//! client wrapper that connects to the embedded instance via its pgwire port.

use crate::error::{Result, EventStreamingError};
use crate::catalog::{MaterializedViewInfo, SourceInfo};
use tokio_postgres::{Client, NoTls};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Client for library-mode RisingWave instance.
///
/// Connects to the embedded RisingWave via PostgreSQL wire protocol.
pub struct LibraryClient {
    client: Arc<Mutex<Client>>,
    frontend_addr: String,
}

impl LibraryClient {
    /// Create a new client and connect to the library-mode RisingWave instance.
    ///
    /// # Arguments
    ///
    /// - `frontend_addr`: The pgwire address (e.g., "127.0.0.1:4566")
    pub async fn connect(frontend_addr: String) -> Result<Self> {
        let conn_str = format!("host={} port={} user=root dbname=dev",
            frontend_addr.split(':').next().unwrap_or("127.0.0.1"),
            frontend_addr.split(':').nth(1).unwrap_or("4566")
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| EventStreamingError::QueryFailed(
                format!("Failed to connect to RisingWave: {}", e)
            ))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("RisingWave connection error: {}", e);
            }
        });

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            frontend_addr,
        })
    }

    /// Execute a DDL statement (CREATE SOURCE, CREATE MATERIALIZED VIEW, etc.)
    pub async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let client = self.client.lock().await;
        client.execute(sql, &[])
            .await
            .map_err(|e| EventStreamingError::DdlFailed(format!("{}", e)))?;
        Ok(())
    }

    /// Query a materialized view or table.
    ///
    /// Returns results as JSON string (array of objects).
    pub async fn query_mv(&self, sql: &str) -> Result<String> {
        let client = self.client.lock().await;
        let rows = client.query(sql, &[])
            .await
            .map_err(|e| EventStreamingError::QueryFailed(format!("{}", e)))?;

        // Convert rows to JSON
        let mut results = Vec::new();
        for row in rows {
            let mut obj = serde_json::Map::new();
            for (idx, column) in row.columns().iter().enumerate() {
                let value = row_value_to_json(&row, idx);
                obj.insert(column.name().to_string(), value);
            }
            results.push(serde_json::Value::Object(obj));
        }

        Ok(serde_json::to_string(&results).unwrap())
    }

    /// List all sources
    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        let client = self.client.lock().await;
        let rows = client.query(
            "SELECT name FROM rw_sources WHERE name NOT LIKE 'rw_%'",
            &[]
        ).await.map_err(|e| EventStreamingError::QueryFailed(format!("{}", e)))?;

        let mut sources = Vec::new();
        for row in rows {
            let name: String = row.get(0);
            sources.push(SourceInfo {
                name,
                connector: "unknown".to_string(), // Would need to query rw_sources table for details
                schema: "public".to_string(), // Default schema
                properties: std::collections::HashMap::new(), // Would need additional query
            });
        }
        Ok(sources)
    }

    /// List all materialized views
    pub async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewInfo>> {
        let client = self.client.lock().await;
        let rows = client.query(
            "SELECT name, definition FROM rw_materialized_views WHERE name NOT LIKE 'rw_%'",
            &[]
        ).await.map_err(|e| EventStreamingError::QueryFailed(format!("{}", e)))?;

        let mut mvs = Vec::new();
        for row in rows {
            let name: String = row.get(0);
            let definition: Option<String> = row.get(1);
            mvs.push(MaterializedViewInfo {
                name,
                definition: definition.unwrap_or_default(),
                schema: "public".to_string(), // Default schema
                columns: Vec::new(), // Would need additional query to get column info
            });
        }
        Ok(mvs)
    }

    /// Get the frontend address this client is connected to
    pub fn frontend_addr(&self) -> &str {
        &self.frontend_addr
    }
}

/// Helper to convert PostgreSQL row value to JSON
fn row_value_to_json(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
    use tokio_postgres::types::Type;

    let column = &row.columns()[idx];
    match column.type_() {
        &Type::INT2 => {
            if let Ok(v) = row.try_get::<_, i16>(idx) {
                serde_json::Value::Number(v.into())
            } else {
                serde_json::Value::Null
            }
        }
        &Type::INT4 => {
            if let Ok(v) = row.try_get::<_, i32>(idx) {
                serde_json::Value::Number(v.into())
            } else {
                serde_json::Value::Null
            }
        }
        &Type::INT8 => {
            if let Ok(v) = row.try_get::<_, i64>(idx) {
                serde_json::Value::Number(v.into())
            } else {
                serde_json::Value::Null
            }
        }
        &Type::FLOAT4 => {
            if let Ok(v) = row.try_get::<_, f32>(idx) {
                serde_json::json!(v)
            } else {
                serde_json::Value::Null
            }
        }
        &Type::FLOAT8 => {
            if let Ok(v) = row.try_get::<_, f64>(idx) {
                serde_json::json!(v)
            } else {
                serde_json::Value::Null
            }
        }
        &Type::TEXT | &Type::VARCHAR => {
            if let Ok(v) = row.try_get::<_, String>(idx) {
                serde_json::Value::String(v)
            } else {
                serde_json::Value::Null
            }
        }
        &Type::BOOL => {
            if let Ok(v) = row.try_get::<_, bool>(idx) {
                serde_json::Value::Bool(v)
            } else {
                serde_json::Value::Null
            }
        }
        &Type::TIMESTAMP => {
            if let Ok(v) = row.try_get::<_, chrono::NaiveDateTime>(idx) {
                serde_json::Value::String(v.to_string())
            } else {
                serde_json::Value::Null
            }
        }
        _ => {
            // Fallback: try to get as string
            if let Ok(v) = row.try_get::<_, String>(idx) {
                serde_json::Value::String(v)
            } else {
                serde_json::Value::Null
            }
        }
    }
}
