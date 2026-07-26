//! EventLogSink: Bridge between RisingWave materialized views and Nexora EventLogStore.
//!
//! This module implements the event pipeline connection that allows RisingWave MV changes
//! to flow into Nexora's EventLogStore, enabling the advanced path:
//! Kafka → RisingWave (SQL MV) → EventLogStore → Graph

use crate::error::{Result, RisingWaveError};
use crate::module::RisingWaveModule;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error, info, warn};

/// Change event from RisingWave materialized view (CDC-like)
#[derive(Debug, Clone)]
pub enum Change {
    /// New row inserted into MV
    Insert(Row),
    /// Row updated in MV (old state → new state)
    Update { old: Row, new: Row },
    /// Row deleted from MV
    Delete(Row),
}

/// A row from a RisingWave materialized view
#[derive(Debug, Clone)]
pub struct Row {
    /// Column name → value mappings
    pub columns: Vec<(String, ColumnValue)>,
}

impl Row {
    /// Create a new row from column mappings
    pub fn new(columns: Vec<(String, ColumnValue)>) -> Self {
        Self { columns }
    }

    /// Get a column value by name
    pub fn get(&self, name: &str) -> Option<&ColumnValue> {
        self.columns
            .iter()
            .find(|(col_name, _)| col_name == name)
            .map(|(_, value)| value)
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|(name, _)| name.as_str()).collect()
    }

    /// Iterate over columns
    pub fn iter(&self) -> impl Iterator<Item = &(String, ColumnValue)> {
        self.columns.iter()
    }
}

/// Column value types supported by RisingWave
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Boolean(bool),
    Timestamp(chrono::DateTime<chrono::Utc>),
    /// JSON value (for complex types)
    Json(serde_json::Value),
    Null,
}

/// Bridge between RisingWave materialized views and Nexora EventLogStore
pub struct EventLogSink {
    #[cfg(feature = "event-first")]
    event_store: Arc<nexora_eventlog::EventLogStore>,
    risingwave: Arc<RisingWaveModule>,
}

impl EventLogSink {
    /// Create a new EventLogSink
    ///
    /// # Arguments
    ///
    /// - `event_store`: Nexora EventLogStore to write events to
    /// - `risingwave`: RisingWave module for subscribing to MV changes
    #[cfg(feature = "event-first")]
    pub fn new(
        event_store: Arc<nexora_eventlog::EventLogStore>,
        risingwave: Arc<RisingWaveModule>,
    ) -> Self {
        Self {
            event_store,
            risingwave,
        }
    }

    /// Start syncing MV changes to EventLogStore
    ///
    /// Subscribes to the given materialized view and streams all changes
    /// (inserts, updates, deletes) to the EventLogStore under the specified topic.
    ///
    /// # Arguments
    ///
    /// - `mv_name`: Name of the materialized view to subscribe to
    /// - `topic`: EventLogStore topic to write events to
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use nexora_risingwave::EventLogSink;
    /// # async fn example(sink: EventLogSink) -> Result<(), Box<dyn std::error::Error>> {
    /// // Start syncing enriched_events MV to nexora.enriched topic
    /// sink.start_sync("enriched_events", "nexora.enriched").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "event-first")]
    pub async fn start_sync(&self, mv_name: &str, topic: &str) -> Result<()> {
        info!("Starting EventLogSink: {} → {}", mv_name, topic);

        let mut rx = self.risingwave.subscribe_mv(mv_name).await?;
        let mut processed = 0u64;
        let mut errors = 0u64;

        while let Some(change) = rx.recv().await {
            match self.process_change(&change, topic).await {
                Ok(_) => {
                    processed += 1;
                    if processed % 1000 == 0 {
                        info!(
                            "EventLogSink progress: {} events processed, {} errors",
                            processed, errors
                        );
                    }
                }
                Err(e) => {
                    errors += 1;
                    error!("Failed to process change: {}", e);
                    if errors % 100 == 0 {
                        warn!(
                            "EventLogSink error rate high: {} errors out of {} total",
                            errors,
                            processed + errors
                        );
                    }
                }
            }
        }

        info!(
            "EventLogSink stopped: {} events processed, {} errors",
            processed, errors
        );
        Ok(())
    }

    /// Process a single change event and write to EventLogStore
    #[cfg(feature = "event-first")]
    async fn process_change(&self, change: &Change, topic: &str) -> Result<()> {
        match change {
            Change::Insert(row) => {
                let event = self.row_to_event(row)?;
                self.event_store.append(topic, event).await.map_err(|e| {
                    RisingWaveError::Internal(format!("EventLogStore append failed: {}", e))
                })?;
                debug!("Inserted event into topic: {}", topic);
            }
            Change::Update { old: _, new } => {
                // For updates, we append the new state
                // (EventLogStore is append-only, so we don't delete the old state)
                let event = self.row_to_event(new)?;
                self.event_store.append(topic, event).await.map_err(|e| {
                    RisingWaveError::Internal(format!("EventLogStore append failed: {}", e))
                })?;
                debug!("Updated event in topic: {}", topic);
            }
            Change::Delete(row) => {
                // For deletes, we can either:
                // 1. Skip (don't write to EventLogStore)
                // 2. Write a tombstone event with a _deleted flag
                //
                // For now, we write a tombstone for auditability
                let mut event = self.row_to_event(row)?;
                if let serde_json::Value::Object(ref mut map) = event {
                    map.insert("_deleted".to_string(), json!(true));
                    map.insert(
                        "_deleted_at".to_string(),
                        json!(chrono::Utc::now().to_rfc3339()),
                    );
                }
                self.event_store.append(topic, event).await.map_err(|e| {
                    RisingWaveError::Internal(format!("EventLogStore append failed: {}", e))
                })?;
                debug!("Deleted event from topic: {}", topic);
            }
        }
        Ok(())
    }

    /// Convert RisingWave Row to Nexora Event (JSON)
    fn row_to_event(&self, row: &Row) -> Result<serde_json::Value> {
        let mut obj = serde_json::Map::new();

        for (col_name, col_value) in row.iter() {
            let json_value = self.value_to_json(col_value)?;
            obj.insert(col_name.clone(), json_value);
        }

        Ok(serde_json::Value::Object(obj))
    }

    /// Convert RisingWave ColumnValue to JSON
    fn value_to_json(&self, value: &ColumnValue) -> Result<serde_json::Value> {
        Ok(match value {
            ColumnValue::Int32(v) => json!(v),
            ColumnValue::Int64(v) => json!(v),
            ColumnValue::Float32(v) => json!(v),
            ColumnValue::Float64(v) => json!(v),
            ColumnValue::String(v) => json!(v),
            ColumnValue::Boolean(v) => json!(v),
            ColumnValue::Timestamp(v) => json!(v.to_rfc3339()),
            ColumnValue::Json(v) => v.clone(),
            ColumnValue::Null => serde_json::Value::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_creation() {
        let row = Row::new(vec![
            ("id".to_string(), ColumnValue::Int64(42)),
            ("name".to_string(), ColumnValue::String("test".to_string())),
            ("active".to_string(), ColumnValue::Boolean(true)),
        ]);

        assert_eq!(row.column_names(), vec!["id", "name", "active"]);
        assert!(matches!(row.get("id"), Some(ColumnValue::Int64(42))));
        assert!(matches!(
            row.get("name"),
            Some(ColumnValue::String(s)) if s == "test"
        ));
        assert!(matches!(
            row.get("active"),
            Some(ColumnValue::Boolean(true))
        ));
        assert!(row.get("nonexistent").is_none());
    }

    #[test]
    fn test_value_to_json() {
        #[cfg(feature = "event-first")]
        {
            use nexora_eventlog::{EventLogStore, StorageConfig};
            use std::sync::Arc;

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let event_store = Arc::new(
                    EventLogStore::new_with_config(StorageConfig::local_fs("./test_events"))
                        .await
                        .unwrap(),
                );
                let config = crate::config::RisingWaveConfig::new()
                    .with_meta_addr("127.0.0.1:15690".parse().unwrap())
                    .with_frontend_addr("127.0.0.1:14566".parse().unwrap());
                let rw = Arc::new(
                    crate::module::RisingWaveModule::start(config)
                        .await
                        .unwrap(),
                );
                let sink = EventLogSink::new(event_store, rw);

                let row = Row::new(vec![
                    ("int32".to_string(), ColumnValue::Int32(42)),
                    ("int64".to_string(), ColumnValue::Int64(1234567890)),
                    ("float32".to_string(), ColumnValue::Float32(3.14)),
                    (
                        "string".to_string(),
                        ColumnValue::String("hello".to_string()),
                    ),
                    ("bool".to_string(), ColumnValue::Boolean(true)),
                    ("null".to_string(), ColumnValue::Null),
                ]);

                let event = sink.row_to_event(&row).unwrap();
                let obj = event.as_object().unwrap();

                assert_eq!(obj.get("int32").unwrap().as_i64().unwrap(), 42);
                assert_eq!(obj.get("int64").unwrap().as_i64().unwrap(), 1234567890);
                assert!(obj.get("float32").unwrap().is_number());
                assert_eq!(obj.get("string").unwrap().as_str().unwrap(), "hello");
                assert_eq!(obj.get("bool").unwrap().as_bool().unwrap(), true);
                assert!(obj.get("null").unwrap().is_null());
            });
        }
    }

    #[test]
    fn test_change_variants() {
        let row1 = Row::new(vec![("id".to_string(), ColumnValue::Int64(1))]);
        let row2 = Row::new(vec![("id".to_string(), ColumnValue::Int64(2))]);

        let insert = Change::Insert(row1.clone());
        let update = Change::Update {
            old: row1.clone(),
            new: row2.clone(),
        };
        let delete = Change::Delete(row1.clone());

        assert!(matches!(insert, Change::Insert(_)));
        assert!(matches!(update, Change::Update { .. }));
        assert!(matches!(delete, Change::Delete(_)));
    }
}
