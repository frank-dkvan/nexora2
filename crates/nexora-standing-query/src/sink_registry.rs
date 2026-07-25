//! Sink implementations for Standing Query result propagation
//!
//! Sinks receive Standing Query match/unmatch events and forward them to
//! external systems (webhooks, Kafka, files, etc.)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Configuration for a Webhook sink
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookSinkConfig {
    /// Target webhook URL
    pub url: String,
    /// Optional HTTP headers (e.g., Authorization)
    pub headers: HashMap<String, String>,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Max retries on failure
    pub max_retries: u32,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
}

/// Configuration for a Kafka sink
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    /// Kafka broker addresses
    pub brokers: Vec<String>,
    /// Target topic name
    pub topic: String,
    /// Optional partition key field
    pub partition_key: Option<String>,
}

/// Configuration for a File sink
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileSinkConfig {
    /// Output file path
    pub path: String,
    /// File format: json, jsonl, csv
    pub format: String,
    /// Whether to append or overwrite
    pub append: bool,
}

/// Unified sink configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SinkConfig {
    Webhook(WebhookSinkConfig),
    Kafka(KafkaSinkConfig),
    File(FileSinkConfig),
}

/// A registered sink
#[derive(Clone, Debug)]
pub struct RegisteredSink {
    pub id: Uuid,
    pub name: String,
    pub config: SinkConfig,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Sink registry - manages all registered sinks
pub struct SinkRegistry {
    sinks: Arc<RwLock<HashMap<Uuid, RegisteredSink>>>,
    /// Map from SQ ID to sink IDs
    sq_to_sinks: Arc<RwLock<HashMap<Uuid, Vec<Uuid>>>>,
}

impl SinkRegistry {
    pub fn new() -> Self {
        Self {
            sinks: Arc::new(RwLock::new(HashMap::new())),
            sq_to_sinks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new sink
    pub async fn register(&self, name: String, config: SinkConfig) -> Uuid {
        let id = Uuid::new_v4();
        let sink = RegisteredSink {
            id,
            name,
            config,
            created_at: chrono::Utc::now(),
        };

        self.sinks.write().await.insert(id, sink);
        tracing::info!(sink_id = %id, "Sink registered");
        id
    }

    /// Unregister a sink
    pub async fn unregister(&self, id: Uuid) -> bool {
        let removed = self.sinks.write().await.remove(&id).is_some();
        if removed {
            // Remove from all SQ subscriptions
            let mut sq_to_sinks = self.sq_to_sinks.write().await;
            for sinks in sq_to_sinks.values_mut() {
                sinks.retain(|sink_id| *sink_id != id);
            }
            tracing::info!(sink_id = %id, "Sink unregistered");
        }
        removed
    }

    /// Subscribe a sink to a Standing Query
    pub async fn subscribe(&self, sq_id: Uuid, sink_id: Uuid) -> Result<(), String> {
        // Verify sink exists
        if !self.sinks.read().await.contains_key(&sink_id) {
            return Err(format!("Sink {} not found", sink_id));
        }

        let mut sq_to_sinks = self.sq_to_sinks.write().await;
        let sinks = sq_to_sinks.entry(sq_id).or_insert_with(Vec::new);
        if !sinks.contains(&sink_id) {
            sinks.push(sink_id);
            tracing::info!(sq_id = %sq_id, sink_id = %sink_id, "Sink subscribed to SQ");
        }
        Ok(())
    }

    /// Unsubscribe a sink from a Standing Query
    pub async fn unsubscribe(&self, sq_id: Uuid, sink_id: Uuid) -> bool {
        let mut sq_to_sinks = self.sq_to_sinks.write().await;
        if let Some(sinks) = sq_to_sinks.get_mut(&sq_id) {
            let before = sinks.len();
            sinks.retain(|id| *id != sink_id);
            let removed = sinks.len() < before;
            if removed {
                tracing::info!(sq_id = %sq_id, sink_id = %sink_id, "Sink unsubscribed from SQ");
            }
            removed
        } else {
            false
        }
    }

    /// Get all sinks subscribed to a Standing Query
    pub async fn get_sinks_for_sq(&self, sq_id: Uuid) -> Vec<RegisteredSink> {
        let sq_to_sinks = self.sq_to_sinks.read().await;
        let sink_ids = sq_to_sinks.get(&sq_id).cloned().unwrap_or_default();

        let sinks = self.sinks.read().await;
        sink_ids
            .iter()
            .filter_map(|id| sinks.get(id).cloned())
            .collect()
    }

    /// List all registered sinks
    pub async fn list_all(&self) -> Vec<RegisteredSink> {
        self.sinks.read().await.values().cloned().collect()
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}
