//! Stream Ingestion Framework — pluggable connectors for stream sources.
//!
//! Supports Kafka, Kinesis, and Pulsar with offset management backed by
//! RocksDB for exactly-once semantics. All sources implement the
//! `IngestionSource` trait, producing batches of graph mutations.
//!
//! ## Architecture
//!
//! ```text
//! Kafka Topic ─→ IngestionSource.poll() ─→ batch of (qid, op)
//!                                                │
//!                                                ▼
//!                              GraphService.ingest_batch(batch)
//!                                                │
//!                                                ▼
//!                              CommitGate.after_wal_write() [quorum]
//!                                                │
//!                                                ▼
//!                              OffsetManager.commit(offset)
//! ```

use nexora_id::NexoraId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub mod checkpoint;
pub mod file_source;
pub mod graph_sink;
pub mod reduct_writer;
pub mod wal_reduct_replicator;
pub mod watermark;
pub use checkpoint::{
    CheckpointCoordinator, CheckpointManifest, CheckpointStore, FileCheckpointStore,
    InMemoryCheckpointStore, RecoveryPlan,
};
pub use watermark::{EventOutcome, TumblingWindow, WatermarkGenerator, WindowAggregate};

pub use reduct_writer::ReductBlobWriter;
pub use wal_reduct_replicator::WalToReductReplicator;

#[cfg(feature = "rocksdb-offsets")]
pub use checkpoint::RocksDbCheckpointStore;
pub use file_source::{FileSource, FileSourceConfig};
pub use graph_sink::{json_to_property_value, GraphIngestHandler};

#[cfg(feature = "mqtt")]
pub mod mqtt_source;
#[cfg(feature = "mqtt")]
pub use mqtt_source::{MqttSource, MqttSourceConfig};

#[cfg(feature = "websocket")]
pub mod websocket_source;
#[cfg(feature = "websocket")]
pub use websocket_source::{WebSocketSource, WebSocketSourceConfig};

#[cfg(feature = "kinesis")]
pub mod kinesis_source;
#[cfg(feature = "kinesis")]
pub use kinesis_source::{KinesisSource, KinesisSourceConfig};

#[cfg(feature = "zenoh")]
pub mod zenoh_source;
#[cfg(feature = "zenoh")]
pub use zenoh_source::{ZenohSource, ZenohSourceConfig};

#[cfg(feature = "rocksdb-offsets")]
pub mod rocksdb_offset_store;
#[cfg(feature = "rocksdb-offsets")]
pub use rocksdb_offset_store::RocksDbOffsetStore;

// ============================================================
// Core Types
// ============================================================

/// A single ingest record from a stream source.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct IngestRecord {
    /// The graph node ID this record belongs to.
    pub qid: NexoraId,
    /// Property key to set.
    pub key: String,
    /// Property value to set.
    pub value: serde_json::Value,
    /// Edge type (if this is an edge operation).
    pub edge_type: Option<String>,
    /// Target node (if this is an edge operation).
    pub edge_target: Option<NexoraId>,
    /// Timestamp from the source message.
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Node label to add (if this record carries a label rather than a
    /// property/edge). Populated when a source is configured with a
    /// `label_field`: that field's value becomes the node's graph label, so
    /// `type: "Forklift"` in source data becomes the `:Forklift` label that
    /// label scans, Standing Query `LabelFilter`s, and the pg-wire table catalog
    /// key off. When set, this record is applied as a label, not a property.
    #[serde(default)]
    pub label: Option<String>,
}

/// How a numeric event-time field is interpreted. Chosen explicitly per source
/// rather than guessed from magnitude — a magnitude heuristic silently
/// misreads, e.g., epoch seconds as milliseconds (1970) or seconds as µs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EventTimeUnit {
    /// Numeric field is epoch seconds.
    Seconds,
    /// Numeric field is epoch milliseconds.
    Millis,
    /// Numeric field is epoch microseconds. The engine's native unit; the
    /// default when unspecified.
    #[default]
    Micros,
    /// Field is an RFC 3339 / ISO 8601 string. Numeric values are rejected.
    Rfc3339,
}

impl EventTimeUnit {
    /// Parse from a config string (case-insensitive): `s`/`sec`/`seconds`,
    /// `ms`/`millis`, `us`/`µs`/`micros`, `rfc3339`/`iso8601`. Unknown → `None`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => Some(Self::Seconds),
            "ms" | "milli" | "millis" | "millisecond" | "milliseconds" => Some(Self::Millis),
            "us" | "µs" | "micro" | "micros" | "microsecond" | "microseconds" => {
                Some(Self::Micros)
            }
            "rfc3339" | "iso8601" | "iso" | "string" => Some(Self::Rfc3339),
            _ => None,
        }
    }
}

/// Extract an event time from a JSON payload for event-time processing.
///
/// Resolution order (the "config-first" policy): if `event_time_field` names a
/// field present in `obj`, parse it as the event time using `unit`; otherwise
/// fall back to `source_meta` (a transport-level timestamp such as a Kafka
/// record time); otherwise `None` (the graph then uses its internal clock, i.e.
/// arrival order).
///
/// A string field is always parsed as RFC 3339 regardless of `unit` (the unit
/// only governs how a *numeric* field is interpreted); `unit == Rfc3339`
/// additionally means a numeric field is rejected. A field present but
/// unparseable falls through to `source_meta`.
pub fn extract_event_time(
    obj: &serde_json::Map<String, serde_json::Value>,
    event_time_field: Option<&str>,
    unit: EventTimeUnit,
    source_meta: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(field) = event_time_field {
        if let Some(v) = obj.get(field) {
            if let Some(dt) = json_value_to_datetime(v, unit) {
                return Some(dt);
            }
        }
    }
    source_meta
}

/// Parse a JSON value as an event-time `DateTime<Utc>`. A string is always tried
/// as RFC 3339; a number is interpreted per `unit` (rejected when `unit` is
/// `Rfc3339`). Returns `None` if it can't parse.
fn json_value_to_datetime(
    v: &serde_json::Value,
    unit: EventTimeUnit,
) -> Option<chrono::DateTime<chrono::Utc>> {
    match v {
        serde_json::Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc)),
        serde_json::Value::Number(n) => {
            let raw = n.as_i64()?;
            match unit {
                EventTimeUnit::Seconds => chrono::DateTime::from_timestamp(raw, 0),
                EventTimeUnit::Millis => chrono::DateTime::from_timestamp_millis(raw),
                EventTimeUnit::Micros => chrono::DateTime::from_timestamp_micros(raw),
                // A numeric value under an RFC 3339 field is a config/data
                // mismatch; reject rather than silently misinterpret.
                EventTimeUnit::Rfc3339 => None,
            }
        }
        _ => None,
    }
}

/// A batch of ingest records from a stream source.
#[derive(Clone, Debug)]
pub struct IngestBatch {
    /// Records in this batch.
    pub records: Vec<IngestRecord>,
    /// The source partition/stream identifier.
    pub partition: String,
    /// The offset at which this batch starts.
    pub offset_start: u64,
    /// The offset at which this batch ends.
    pub offset_end: u64,
    /// Topic or stream name.
    pub topic: String,
    /// Raw events (阶段 1 新增:事件优先摄入)
    ///
    /// 当 source 支持"事件优先"模式时,这里携带原始 RawEvent。
    /// `None` 表示兼容模式(只有 IngestRecord,用于纯图投影)。
    pub raw_events: Option<Vec<nexora_core::RawEvent>>,
}

/// Source offset tracking.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourceOffset {
    pub topic: String,
    pub partition: String,
    pub offset: u64,
    pub committed_at: chrono::DateTime<chrono::Utc>,
}

/// Stats for an ingestion source.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct IngestionStats {
    pub records_ingested: u64,
    pub batches_processed: u64,
    pub errors: u64,
    pub last_offset: Option<SourceOffset>,
    pub lag_records: u64,
    pub rate_per_sec: f64,
}

// ============================================================
// IngestionSource Trait
// ============================================================

/// Pluggable ingestion source for stream connectors.
///
/// Implementations include KafkaSource, KinesisSource, PulsarSource,
/// and a MockSource for testing.
#[async_trait::async_trait]
pub trait IngestionSource: Send + Sync {
    /// Connect to the source and initialize subscriptions.
    async fn connect(&self) -> Result<(), IngestionError>;

    /// Poll for the next batch of records.
    async fn poll(&self) -> Result<Option<IngestBatch>, IngestionError>;

    /// Commit an offset after successful processing.
    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError>;

    /// Reposition the source to resume reading from the given offsets — the
    /// LAST-PROCESSED offset per (topic, partition), so the source resumes at the
    /// NEXT record after it (matching `commit`'s semantics). Called once after
    /// `connect`, during checkpoint recovery, so replay starts exactly at the
    /// checkpointed cut rather than the source's own committed position.
    ///
    /// Default: no-op. Sources whose resume position is externally managed and
    /// already correct (e.g. a file source that tracks its own byte offset, or a
    /// test mock) need not override this. Sources whose broker tracks a separate
    /// committed offset (Kafka) MUST override it, or checkpoint recovery is
    /// silently ignored and the broker offset wins.
    async fn seek(&self, _offsets: &[SourceOffset]) -> Result<(), IngestionError> {
        Ok(())
    }

    /// Get current stats.
    fn stats(&self) -> IngestionStats;

    /// Get the source's configured topics.
    fn topics(&self) -> Vec<String>;

    /// Close the source connection gracefully.
    async fn close(&self) -> Result<(), IngestionError>;
}

#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("poll error: {0}")]
    Poll(String),
    #[error("commit error: {0}")]
    Commit(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("timeout")]
    Timeout,
    #[error("source not connected")]
    NotConnected,
}

// ============================================================
// Offset Manager
// ============================================================

/// Persisted offset manager for exactly-once processing.
///
/// Stores committed offsets so that on restart, ingestion resumes
/// from the last committed position.
pub struct OffsetManager {
    /// In-memory offset cache: (topic, partition) → offset.
    offsets: tokio::sync::RwLock<HashMap<(String, String), SourceOffset>>,
    /// Persisted offsets for crash recovery.
    persisted: Arc<dyn OffsetStore>,
}

impl OffsetManager {
    pub fn new(store: Arc<dyn OffsetStore>) -> Self {
        Self {
            offsets: tokio::sync::RwLock::new(HashMap::new()),
            persisted: store,
        }
    }

    /// Get the last committed offset for a topic/partition.
    pub async fn get(&self, topic: &str, partition: &str) -> Option<SourceOffset> {
        let offsets = self.offsets.read().await;
        offsets
            .get(&(topic.to_string(), partition.to_string()))
            .cloned()
    }

    /// Commit an offset (both in-memory and persisted).
    pub async fn commit(&self, offset: SourceOffset) -> Result<(), String> {
        let key = (offset.topic.clone(), offset.partition.clone());
        {
            let mut offsets = self.offsets.write().await;
            offsets.insert(key.clone(), offset.clone());
        }
        self.persisted.save(&key.0, &key.1, offset.offset).await
    }

    /// Load persisted offsets on startup.
    pub async fn load(&self) -> Result<usize, String> {
        let stored = self.persisted.load_all().await?;
        let count = stored.len();
        let mut offsets = self.offsets.write().await;
        for (topic, partition, offset, committed_at) in stored {
            let key = (topic.clone(), partition.clone());
            offsets.insert(
                key,
                SourceOffset {
                    topic,
                    partition,
                    offset,
                    committed_at,
                },
            );
        }
        Ok(count)
    }
}

/// Persisted offset storage interface.
///
/// The `u64` methods (`save`/`load`) suit numeric positions (Kafka offset, file
/// line index). Sources with opaque **string** positions — notably Kinesis
/// sequence numbers, which are large decimal strings that don't fit `u64` — use
/// [`save_str`](OffsetStore::save_str) / [`load_str`](OffsetStore::load_str).
/// Both are keyed by `(topic, partition)`; a store may keep them in separate
/// namespaces. Default implementations return "unsupported" so existing stores
/// keep compiling; [`InMemoryOffsetStore`] implements both.
#[async_trait::async_trait]
pub trait OffsetStore: Send + Sync {
    async fn save(&self, topic: &str, partition: &str, offset: u64) -> Result<(), String>;
    async fn load(&self, topic: &str, partition: &str) -> Result<Option<u64>, String>;
    async fn load_all(
        &self,
    ) -> Result<Vec<(String, String, u64, chrono::DateTime<chrono::Utc>)>, String>;

    /// Persist a string checkpoint (e.g. a Kinesis sequence number) for a
    /// `(topic, partition)`. Default: unsupported.
    async fn save_str(&self, _topic: &str, _partition: &str, _value: &str) -> Result<(), String> {
        Err("string offsets not supported by this OffsetStore".into())
    }

    /// Load a previously saved string checkpoint. Default: `None`.
    async fn load_str(&self, _topic: &str, _partition: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// In-memory offset store for testing. Holds both numeric and string offsets.
pub struct InMemoryOffsetStore {
    offsets: tokio::sync::RwLock<HashMap<(String, String), u64>>,
    str_offsets: tokio::sync::RwLock<HashMap<(String, String), String>>,
}

impl InMemoryOffsetStore {
    pub fn new() -> Self {
        Self {
            offsets: tokio::sync::RwLock::new(HashMap::new()),
            str_offsets: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryOffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl OffsetStore for InMemoryOffsetStore {
    async fn save(&self, topic: &str, partition: &str, offset: u64) -> Result<(), String> {
        let mut offsets = self.offsets.write().await;
        offsets.insert((topic.to_string(), partition.to_string()), offset);
        Ok(())
    }

    async fn load(&self, topic: &str, partition: &str) -> Result<Option<u64>, String> {
        let offsets = self.offsets.read().await;
        Ok(offsets
            .get(&(topic.to_string(), partition.to_string()))
            .copied())
    }

    async fn load_all(
        &self,
    ) -> Result<Vec<(String, String, u64, chrono::DateTime<chrono::Utc>)>, String> {
        let offsets = self.offsets.read().await;
        let now = chrono::Utc::now();
        Ok(offsets
            .iter()
            .map(|((t, p), o)| (t.clone(), p.clone(), *o, now))
            .collect())
    }

    async fn save_str(&self, topic: &str, partition: &str, value: &str) -> Result<(), String> {
        let mut offsets = self.str_offsets.write().await;
        offsets.insert(
            (topic.to_string(), partition.to_string()),
            value.to_string(),
        );
        Ok(())
    }

    async fn load_str(&self, topic: &str, partition: &str) -> Result<Option<String>, String> {
        let offsets = self.str_offsets.read().await;
        Ok(offsets
            .get(&(topic.to_string(), partition.to_string()))
            .cloned())
    }
}

// ============================================================
// Graph Ingestion Pipeline
// ============================================================

/// Configuration for the ingestion pipeline.
#[derive(Clone, Debug)]
pub struct IngestionConfig {
    /// Maximum records per batch.
    pub max_batch_size: usize,
    /// Polling interval.
    pub poll_interval: Duration,
    /// Commit interval (how often to persist offsets).
    pub commit_interval: Duration,
    /// Number of concurrent ingest workers.
    pub concurrency: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            poll_interval: Duration::from_millis(100),
            commit_interval: Duration::from_secs(5),
            concurrency: 4,
        }
    }
}

/// The ingestion pipeline — connects sources to the graph.
pub struct IngestionPipeline {
    config: IngestionConfig,
    sources: Vec<Arc<dyn IngestionSource>>,
    offset_manager: OffsetManager,
    stats: tokio::sync::RwLock<HashMap<String, IngestionStats>>,
    /// B2: optional offset-aligned checkpoint coordinator. When set, the run
    /// loop periodically (every `checkpoint_interval`) captures the current
    /// source offsets and binds them atomically to a flushed graph state — the
    /// exactly-once recovery point. `None` → no checkpointing (offsets are still
    /// committed to the source, but there's no (offset, state) atomic pair).
    checkpoint: Option<Arc<checkpoint::CheckpointCoordinator>>,
    /// B2: how often to trigger an offset-aligned checkpoint. Ignored when
    /// `checkpoint` is `None`.
    checkpoint_interval: Duration,
    /// B2: latest offset per "{topic}:{partition}", tracked so a checkpoint can
    /// capture the exact offsets applied so far.
    latest_offsets: tokio::sync::RwLock<HashMap<String, u64>>,
    /// F4: optional event-time watermark generator. When set, the run loop
    /// advances it with the max event time in each ingested batch and exposes
    /// the current watermark via [`IngestionPipeline::current_watermark`]. `None`
    /// → no event-time tracking (watermark reads as `EventTime::MIN`).
    watermark: Option<tokio::sync::RwLock<watermark::WatermarkGenerator>>,
    /// F4: optional observer invoked with the current watermark after each batch
    /// advances it. Lets a consumer (e.g. the app's Prometheus metrics) report
    /// watermark progress WITHOUT `nexora-stream` depending on the app crate —
    /// the app injects a closure that calls `set_watermark_ms`. `None` → no
    /// reporting. Only invoked when `watermark` is also set.
    #[allow(clippy::type_complexity)]
    watermark_observer: Option<Arc<dyn Fn(nexora_id::EventTime) + Send + Sync>>,
}

impl IngestionPipeline {
    pub fn new(config: IngestionConfig, store: Arc<dyn OffsetStore>) -> Self {
        Self {
            config,
            sources: Vec::new(),
            offset_manager: OffsetManager::new(store),
            stats: tokio::sync::RwLock::new(HashMap::new()),
            checkpoint: None,
            checkpoint_interval: Duration::from_secs(30),
            latest_offsets: tokio::sync::RwLock::new(HashMap::new()),
            watermark: None,
            watermark_observer: None,
        }
    }

    /// F4: register an observer called with the current watermark after each
    /// batch advances it. Enables watermark reporting (e.g. Prometheus
    /// `set_watermark_ms`) from the app layer without a reverse crate dependency.
    /// No effect unless watermarking is also enabled via [`Self::with_watermarks`].
    /// Chainable.
    pub fn with_watermark_observer(
        mut self,
        observer: Arc<dyn Fn(nexora_id::EventTime) + Send + Sync>,
    ) -> Self {
        self.watermark_observer = Some(observer);
        self
    }

    /// F4: enable event-time watermark tracking on the ingestion stream.
    ///
    /// `max_out_of_orderness_us` bounds how far the watermark trails the maximum
    /// observed event time (microseconds) — the tolerance for out-of-order
    /// arrival. The run loop advances the watermark with each batch's max event
    /// time; read it back with [`IngestionPipeline::current_watermark`].
    /// Chainable.
    pub fn with_watermarks(mut self, max_out_of_orderness_us: u64) -> Self {
        self.watermark = Some(tokio::sync::RwLock::new(
            watermark::WatermarkGenerator::new(max_out_of_orderness_us),
        ));
        self
    }

    /// F4: the current event-time watermark, or [`nexora_id::EventTime::MIN`]
    /// when watermarking is disabled or no event has been observed yet.
    pub async fn current_watermark(&self) -> nexora_id::EventTime {
        match &self.watermark {
            Some(gen) => gen.read().await.current(),
            None => nexora_id::EventTime::MIN,
        }
    }

    /// Register an ingestion source.
    pub fn with_source(mut self, source: Arc<dyn IngestionSource>) -> Self {
        self.sources.push(source);
        self
    }

    /// B2: attach an offset-aligned checkpoint coordinator. The run loop will
    /// trigger a checkpoint every `interval`, binding the current source offsets
    /// to a flushed graph state for exactly-once recovery. Chainable.
    pub fn with_checkpointing(
        mut self,
        coordinator: Arc<checkpoint::CheckpointCoordinator>,
        interval: Duration,
    ) -> Self {
        self.checkpoint = Some(coordinator);
        self.checkpoint_interval = interval;
        self
    }

    /// Load persisted offsets for all registered sources.
    pub async fn load_offsets(&self) -> Result<usize, String> {
        self.offset_manager.load().await
    }

    /// F1.4: seed the pipeline from a crash-recovery plan.
    ///
    /// Applies `plan.resume_offsets` into both the `latest_offsets` tracker
    /// (so the next checkpoint captures a correct baseline) and the
    /// `OffsetManager` (so `get()` reports the resume position). Ingestion then
    /// continues from these offsets rather than from the source's default start
    /// position — closing the offset-alignment half of exactly-once recovery.
    /// Graph state is restored independently by the persistence layer.
    ///
    /// Returns the number of (topic, partition) offsets seeded.
    pub async fn seed_recovery(&self, plan: &checkpoint::RecoveryPlan) -> usize {
        let mut latest = self.latest_offsets.write().await;
        for (key, offset) in &plan.resume_offsets {
            latest.insert(key.clone(), *offset);
            // Mirror into the offset manager, splitting "{topic}:{partition}".
            if let Some((topic, partition)) = key.split_once(':') {
                let _ = self
                    .offset_manager
                    .commit(SourceOffset {
                        topic: topic.to_string(),
                        partition: partition.to_string(),
                        offset: *offset,
                        committed_at: chrono::Utc::now(),
                    })
                    .await;
            }
        }
        plan.resume_offsets.len()
    }

    /// Run the ingestion pipeline in a background task.
    /// Returns a join handle that resolves when all sources are closed.
    ///
    /// Uses a bounded channel (capacity=100) for backpressure: if the graph
    /// handler processes batches slower than sources produce them, the poll
    /// loop will block when the channel is full, preventing unbounded memory
    /// growth.
    pub fn run(
        self: Arc<Self>,
        graph_handler: Arc<dyn IngestHandler>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Create bounded channel for backpressure (capacity=100 batches)
            let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<(IngestBatch, SourceOffset)>(100);

            // Clone Arc for the processor task
            let pipeline_clone = self.clone();

            // Spawn batch processing task
            let handler = graph_handler.clone();
            let pipeline = pipeline_clone;

            let processor_handle = tokio::spawn(async move {
                while let Some((batch, offset)) = batch_rx.recv().await {
                    let topic = batch.topic.clone();

                    match handler.handle_batch(&batch).await {
                        Ok(count) => {
                            // F4: advance the event-time watermark
                            if let Some(gen) = &pipeline.watermark {
                                let max_et = batch
                                    .records
                                    .iter()
                                    .filter_map(|r| r.timestamp.as_ref())
                                    .map(nexora_id::EventTime::from_datetime)
                                    .max();
                                if let Some(et) = max_et {
                                    let current = gen.write().await.observe(et);
                                    if let Some(obs) = &pipeline.watermark_observer {
                                        obs(current);
                                    }
                                }
                            }

                            // Track applied offset for checkpoint
                            {
                                let key = format!("{}:{}", offset.topic, offset.partition);
                                pipeline.latest_offsets.write().await.insert(key, offset.offset);
                            }

                            // Update stats
                            let mut stats = pipeline.stats.write().await;
                            let s = stats.entry(topic).or_default();
                            s.records_ingested += count as u64;
                            s.batches_processed += 1;
                            s.last_offset = Some(offset);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Batch processing failed");
                            let mut stats = pipeline.stats.write().await;
                            let s = stats.entry(topic).or_default();
                            s.errors += 1;
                        }
                    }
                }
            });

            // C-4 FIX: Connect all sources with proper cleanup on error
            let mut connected_sources = Vec::new();
            for (idx, source) in self.sources.iter().enumerate() {
                match source.connect().await {
                    Ok(()) => {
                        tracing::info!(source_index = idx, "Source connected");
                        connected_sources.push(idx);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, source_index = idx, "Failed to connect source");

                        // Clean up: close all previously connected sources
                        for &prev_idx in &connected_sources {
                            if let Err(close_err) = self.sources[prev_idx].close().await {
                                tracing::warn!(
                                    error = %close_err,
                                    source_index = prev_idx,
                                    "Failed to close source during error cleanup"
                                );
                            }
                        }

                        // Abort the processor task to prevent it from waiting indefinitely
                        processor_handle.abort();

                        return;
                    }
                }
            }

            // Checkpoint recovery: if `seed_recovery` seeded resume offsets, seek
            // each source to that cut so replay starts exactly where the last
            // checkpoint left off — NOT the source's own committed position. This
            // is what makes checkpoint recovery authoritative (see Source::seek).
            // A fresh start (no seeded offsets) makes this an empty no-op seek.
            {
                let recovery_offsets: Vec<SourceOffset> = self
                    .latest_offsets
                    .read()
                    .await
                    .iter()
                    .filter_map(|(key, off)| {
                        key.split_once(':').map(|(topic, partition)| SourceOffset {
                            topic: topic.to_string(),
                            partition: partition.to_string(),
                            offset: *off,
                            committed_at: chrono::Utc::now(),
                        })
                    })
                    .collect();
                if !recovery_offsets.is_empty() {
                    for source in &self.sources {
                        if let Err(e) = source.seek(&recovery_offsets).await {
                            tracing::error!(
                                error = %e,
                                "Failed to seek source to checkpoint offsets; \
                                 aborting to avoid resuming from the wrong position"
                            );
                            return;
                        }
                    }
                    tracing::info!(
                        offsets = recovery_offsets.len(),
                        "Sources sought to checkpoint recovery offsets"
                    );
                }
            }

            let mut commit_timer = tokio::time::interval(self.config.commit_interval);
            // B2: checkpoint timer only fires meaningfully when a coordinator is
            // attached; when absent we still tick (cheap) but skip the body.
            let mut checkpoint_timer = tokio::time::interval(self.checkpoint_interval);

            loop {
                tokio::select! {
                    _ = commit_timer.tick() => {
                        // Periodic commit
                        for source in &self.sources {
                            if let Some(last) = &source.stats().last_offset {
                                let _ = source.commit(last).await;
                            }
                        }
                    }
                    _ = checkpoint_timer.tick() => {
                        // B2: offset-aligned checkpoint. Capture the offsets
                        // applied so far and bind them atomically to a flushed
                        // graph state. Recovery resumes from these offsets, so a
                        // post-checkpoint crash replays only what came after.
                        if let Some(coordinator) = &self.checkpoint {
                            let offsets = self.latest_offsets.read().await.clone();
                            if !offsets.is_empty() {
                                match coordinator.checkpoint(offsets).await {
                                    Ok(m) => tracing::info!(
                                        epoch = m.epoch,
                                        nodes_flushed = m.nodes_flushed,
                                        "offset-aligned checkpoint committed"
                                    ),
                                    Err(e) => tracing::error!(
                                        error = %e,
                                        "offset-aligned checkpoint failed"
                                    ),
                                }
                            }
                        }
                    }
                    _ = tokio::time::sleep(self.config.poll_interval) => {
                        for source in &self.sources {
                            match source.poll().await {
                                Ok(Some(batch)) => {
                                    let topic = batch.topic.clone();
                                    let offset = SourceOffset {
                                        topic: topic.clone(),
                                        partition: batch.partition.clone(),
                                        // `offset_end` is last-processed + 1; a source's
                                        // `commit` takes the LAST-PROCESSED offset and adds
                                        // 1 itself for the broker's "next to read" (see
                                        // KafkaSource::commit). Committing `offset_end`
                                        // directly would double-count the +1 and skip one
                                        // message on restart — commit `offset_end - 1`.
                                        offset: batch.offset_end.saturating_sub(1),
                                        committed_at: chrono::Utc::now(),
                                    };

                                    // Send to bounded channel - blocks if channel is full (backpressure)
                                    if batch_tx.send((batch, offset.clone())).await.is_err() {
                                        tracing::error!("Batch processor channel closed, stopping ingestion");
                                        break;
                                    }

                                    // Commit offset after successful send
                                    let _ = source.commit(&offset).await;
                                }
                                Ok(None) => {} // No data available
                                Err(e) => {
                                    tracing::error!(error = %e, "Poll failed");
                                }
                            }
                        }
                    }
                }
            }

            // Drop the sender to signal processor to stop
            drop(batch_tx);

            // Wait for processor to finish
            let _ = processor_handle.await;
        })
    }

    /// Get stats for a specific source.
    pub async fn source_stats(&self, topic: &str) -> Option<IngestionStats> {
        let stats = self.stats.read().await;
        stats.get(topic).cloned()
    }
}

/// Handler that processes ingested batches into the graph.
#[async_trait::async_trait]
pub trait IngestHandler: Send + Sync {
    async fn handle_batch(&self, batch: &IngestBatch) -> Result<usize, String>;
}

// ============================================================
// Mock Source (for testing)
// ============================================================

/// Mock ingestion source for testing.
pub struct MockSource {
    records: Arc<tokio::sync::Mutex<Vec<IngestRecord>>>,
    topic: String,
    partition: String,
    stats: Arc<tokio::sync::Mutex<IngestionStats>>,
    consumed: Arc<std::sync::atomic::AtomicBool>,
    /// Records the offsets this source was `seek`ed to, so tests can assert
    /// checkpoint recovery repositioned the source (rather than resuming from
    /// its default start). Empty until `seek` is called.
    sought: Arc<tokio::sync::Mutex<Vec<SourceOffset>>>,
}

impl MockSource {
    pub fn new(topic: &str, records: Vec<IngestRecord>) -> Self {
        Self {
            records: Arc::new(tokio::sync::Mutex::new(records)),
            topic: topic.to_string(),
            partition: "0".to_string(),
            stats: Arc::new(tokio::sync::Mutex::new(IngestionStats::default())),
            consumed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            sought: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Test helper: a shared handle to the offsets this source was seeked to.
    pub fn sought_handle(&self) -> Arc<tokio::sync::Mutex<Vec<SourceOffset>>> {
        self.sought.clone()
    }
}

#[async_trait::async_trait]
impl IngestionSource for MockSource {
    async fn connect(&self) -> Result<(), IngestionError> {
        Ok(())
    }

    async fn poll(&self) -> Result<Option<IngestBatch>, IngestionError> {
        if self.consumed.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(None);
        }
        self.consumed
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let records = self.records.lock().await;
        let len = records.len() as u64;
        Ok(Some(IngestBatch {
            records: records.clone(),
            partition: self.partition.clone(),
            offset_start: 0,
            offset_end: len,
            topic: self.topic.clone(),
            raw_events: None, // MockSource 不产生 RawEvent
        }))
    }

    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
        let mut stats = self.stats.lock().await;
        stats.last_offset = Some(offset.clone());
        Ok(())
    }

    async fn seek(&self, offsets: &[SourceOffset]) -> Result<(), IngestionError> {
        let mut sought = self.sought.lock().await;
        sought.extend(offsets.iter().cloned());
        Ok(())
    }

    fn stats(&self) -> IngestionStats {
        self.stats.try_lock().unwrap().clone()
    }

    fn topics(&self) -> Vec<String> {
        vec![self.topic.clone()]
    }

    async fn close(&self) -> Result<(), IngestionError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_event_time_prefers_payload_field() {
        // A string field parses as RFC 3339 regardless of the configured unit.
        let obj = serde_json::json!({"ts": "2026-01-02T03:04:05Z", "v": 1})
            .as_object()
            .unwrap()
            .clone();
        let meta = Some(chrono::Utc::now());
        let got = extract_event_time(&obj, Some("ts"), EventTimeUnit::Micros, meta).unwrap();
        assert_eq!(got.to_rfc3339(), "2026-01-02T03:04:05+00:00");
    }

    #[test]
    fn extract_event_time_interprets_numeric_by_unit() {
        let obj = |v: i64| serde_json::json!({ "ts": v }).as_object().unwrap().clone();
        // Same instant expressed in each unit → same result under the matching unit.
        let s = extract_event_time(
            &obj(1_700_000_000),
            Some("ts"),
            EventTimeUnit::Seconds,
            None,
        )
        .unwrap();
        assert_eq!(s.timestamp(), 1_700_000_000);
        let ms = extract_event_time(
            &obj(1_700_000_000_000),
            Some("ts"),
            EventTimeUnit::Millis,
            None,
        )
        .unwrap();
        assert_eq!(ms.timestamp(), 1_700_000_000);
        let us = extract_event_time(
            &obj(1_700_000_000_000_000),
            Some("ts"),
            EventTimeUnit::Micros,
            None,
        )
        .unwrap();
        assert_eq!(us.timestamp(), 1_700_000_000);
    }

    #[test]
    fn extract_event_time_rejects_numeric_under_rfc3339_unit() {
        // A numeric value under an RFC 3339 field is a config/data mismatch:
        // reject (fall through to meta) rather than silently misinterpret.
        let obj = serde_json::json!({"ts": 1_700_000_000})
            .as_object()
            .unwrap()
            .clone();
        assert!(extract_event_time(&obj, Some("ts"), EventTimeUnit::Rfc3339, None).is_none());
    }

    #[test]
    fn extract_event_time_falls_back_to_source_meta() {
        // Field absent → use transport metadata.
        let obj = serde_json::json!({"v": 1}).as_object().unwrap().clone();
        let meta = chrono::DateTime::from_timestamp(1_700_000_000, 0);
        let got = extract_event_time(&obj, Some("ts"), EventTimeUnit::Micros, meta).unwrap();
        assert_eq!(got.timestamp(), 1_700_000_000);

        // No field configured, no meta → None (arrival order downstream).
        assert!(extract_event_time(&obj, None, EventTimeUnit::Micros, None).is_none());
    }

    #[test]
    fn event_time_unit_parses_aliases() {
        assert_eq!(EventTimeUnit::parse("s"), Some(EventTimeUnit::Seconds));
        assert_eq!(EventTimeUnit::parse("ms"), Some(EventTimeUnit::Millis));
        assert_eq!(EventTimeUnit::parse("US"), Some(EventTimeUnit::Micros));
        assert_eq!(
            EventTimeUnit::parse("rfc3339"),
            Some(EventTimeUnit::Rfc3339)
        );
        assert_eq!(EventTimeUnit::parse("nonsense"), None);
        // The default, used when config is unset/blank, is microseconds.
        assert_eq!(EventTimeUnit::default(), EventTimeUnit::Micros);
    }

    #[tokio::test]
    async fn test_offset_manager_commit_and_load() {
        let store = Arc::new(InMemoryOffsetStore::new());
        let manager = OffsetManager::new(store);

        let offset = SourceOffset {
            topic: "test".into(),
            partition: "0".into(),
            offset: 42,
            committed_at: chrono::Utc::now(),
        };
        manager.commit(offset).await.unwrap();

        let loaded = manager.get("test", "0").await.unwrap();
        assert_eq!(loaded.offset, 42);
    }

    /// String checkpoints (e.g. Kinesis sequence numbers) round-trip through the
    /// store and are namespaced separately from numeric offsets.
    #[tokio::test]
    async fn test_offset_store_string_checkpoint() {
        let store = InMemoryOffsetStore::new();

        // Absent → None.
        assert_eq!(store.load_str("stream", "shard-0").await.unwrap(), None);

        // Save a Kinesis-style large decimal sequence number.
        let seq = "49590338271490256608559692538361571095921575989136588898";
        store.save_str("stream", "shard-0", seq).await.unwrap();
        assert_eq!(
            store
                .load_str("stream", "shard-0")
                .await
                .unwrap()
                .as_deref(),
            Some(seq)
        );

        // Numeric and string namespaces are independent for the same key.
        store.save("stream", "shard-0", 7).await.unwrap();
        assert_eq!(store.load("stream", "shard-0").await.unwrap(), Some(7));
        assert_eq!(
            store
                .load_str("stream", "shard-0")
                .await
                .unwrap()
                .as_deref(),
            Some(seq),
            "numeric save must not clobber the string checkpoint"
        );

        // Overwrite advances the checkpoint.
        store.save_str("stream", "shard-0", "999").await.unwrap();
        assert_eq!(
            store
                .load_str("stream", "shard-0")
                .await
                .unwrap()
                .as_deref(),
            Some("999")
        );
    }

    /// The default trait impl reports string offsets unsupported (guards stores
    /// that only implement the numeric methods).
    #[tokio::test]
    async fn test_offset_store_default_str_unsupported() {
        struct NumericOnly;
        #[async_trait::async_trait]
        impl OffsetStore for NumericOnly {
            async fn save(&self, _: &str, _: &str, _: u64) -> Result<(), String> {
                Ok(())
            }
            async fn load(&self, _: &str, _: &str) -> Result<Option<u64>, String> {
                Ok(None)
            }
            async fn load_all(
                &self,
            ) -> Result<Vec<(String, String, u64, chrono::DateTime<chrono::Utc>)>, String>
            {
                Ok(vec![])
            }
        }
        let store = NumericOnly;
        assert!(store.save_str("t", "p", "x").await.is_err());
        assert_eq!(store.load_str("t", "p").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mock_source_poll_once() {
        let records = vec![IngestRecord {
            qid: nexora_id::NexoraId::from_bytes(b"n1".to_vec()),
            key: "name".into(),
            value: serde_json::json!("test"),
            edge_type: None,
            edge_target: None,
            timestamp: None,
            label: None,
        }];
        let source = MockSource::new("test", records);

        let batch = source.poll().await.unwrap().unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].key, "name");

        // Second poll returns None
        assert!(source.poll().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_ingestion_pipeline_config() {
        let config = IngestionConfig::default();
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.concurrency, 4);
    }

    #[tokio::test]
    async fn watermark_disabled_reads_min() {
        let store = Arc::new(InMemoryOffsetStore::new());
        let pipeline = IngestionPipeline::new(IngestionConfig::default(), store);
        assert_eq!(
            pipeline.current_watermark().await,
            nexora_id::EventTime::MIN,
            "watermark reads MIN when disabled"
        );
    }

    /// Handler that just counts records, so the pipeline run loop can advance the
    /// watermark without a real graph.
    struct CountingHandler;
    #[async_trait::async_trait]
    impl IngestHandler for CountingHandler {
        async fn handle_batch(&self, batch: &IngestBatch) -> Result<usize, String> {
            Ok(batch.records.len())
        }
    }

    #[tokio::test]
    async fn pipeline_advances_watermark_from_batch_event_times() {
        // A batch whose max event time is 2_000_000µs; with 500_000µs OOO the
        // watermark should reach 1_500_000µs after the batch is processed.
        let ts = |us: i64| chrono::DateTime::from_timestamp_micros(us);
        let records = vec![
            IngestRecord {
                qid: nexora_id::NexoraId::from_bytes(b"n1".to_vec()),
                key: "v".into(),
                value: serde_json::json!(1),
                edge_type: None,
                edge_target: None,
                timestamp: ts(1_000_000),
                label: None,
            },
            IngestRecord {
                qid: nexora_id::NexoraId::from_bytes(b"n2".to_vec()),
                key: "v".into(),
                value: serde_json::json!(2),
                edge_type: None,
                edge_target: None,
                timestamp: ts(2_000_000),
                label: None,
            },
        ];
        let store = Arc::new(InMemoryOffsetStore::new());
        let pipeline = Arc::new(
            IngestionPipeline::new(IngestionConfig::default(), store)
                .with_source(Arc::new(MockSource::new("t", records)))
                .with_watermarks(500_000),
        );
        let handle = pipeline.clone().run(Arc::new(CountingHandler));

        // Poll the watermark until it advances (the run loop processes the batch
        // on its poll tick), with a bounded wait so the test can't hang.
        let mut advanced = nexora_id::EventTime::MIN;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            advanced = pipeline.current_watermark().await;
            if advanced != nexora_id::EventTime::MIN {
                break;
            }
        }
        handle.abort();
        assert_eq!(
            advanced.as_micros(),
            1_500_000,
            "watermark = max_event_time(2_000_000) - ooo(500_000)"
        );
    }

    #[tokio::test]
    async fn pipeline_seeks_source_to_recovery_offsets() {
        // Checkpoint recovery: after seed_recovery seeds an offset cut, run() must
        // seek the source to it (so a real Kafka consumer resumes from the flushed
        // checkpoint, not its broker-committed offset). We assert the source's
        // recorded seek matches the seeded offset.
        let source = Arc::new(MockSource::new("orders", Vec::new()));
        let sought = source.sought_handle();

        let store = Arc::new(InMemoryOffsetStore::new());
        let pipeline =
            Arc::new(IngestionPipeline::new(IngestionConfig::default(), store).with_source(source));

        // Seed a recovery plan: resume "orders:0" from last-processed offset 41.
        let plan = RecoveryPlan {
            epoch: 1,
            resume_offsets: HashMap::from([("orders:0".to_string(), 41u64)]),
        };
        let seeded = pipeline.seed_recovery(&plan).await;
        assert_eq!(seeded, 1);

        let handle = pipeline.run(Arc::new(CountingHandler));

        // The seek happens right after connect, before the poll loop. Wait for it.
        let mut got: Vec<SourceOffset> = Vec::new();
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            got = sought.lock().await.clone();
            if !got.is_empty() {
                break;
            }
        }
        handle.abort();

        assert_eq!(
            got.len(),
            1,
            "source must be sought exactly once on recovery"
        );
        assert_eq!(got[0].topic, "orders");
        assert_eq!(got[0].partition, "0");
        assert_eq!(
            got[0].offset, 41,
            "source must be sought to the checkpointed last-processed offset"
        );
    }
}

// ============================================================
// Kafka Source (feature-gated behind "kafka")
// ============================================================

#[cfg(feature = "kafka")]
pub mod kafka {
    //! Kafka ingestion source using `rdkafka`.

    use crate::{
        EventTimeUnit, IngestBatch, IngestRecord, IngestionError, IngestionSource, IngestionStats,
        SourceOffset,
    };
    use nexora_id::NexoraId;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::ClientConfig;
    use rdkafka::Message;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Mutex;

    /// Configuration for a Kafka ingestion source.
    #[derive(Clone, Debug)]
    pub struct KafkaSourceConfig {
        /// Kafka bootstrap servers (e.g., "localhost:9092").
        pub brokers: String,
        /// Topic to consume.
        pub topic: String,
        /// Consumer group ID.
        pub group_id: String,
        /// Key field to use as the NexoraId source (from message key or JSON field).
        pub key_field: String,
        /// Optional JSON field holding each record's business event time. When
        /// set and present, it takes precedence over the Kafka record timestamp
        /// (which is transport/broker time). RFC 3339 string or integer epoch
        /// interpreted per `event_time_unit`. When unset/absent, the record
        /// timestamp is used.
        pub event_time_field: Option<String>,
        /// How a numeric `event_time_field` is interpreted: seconds, milliseconds,
        /// microseconds, or RFC 3339 string. Defaults to microseconds (the
        /// engine's native unit). A string field is always parsed as RFC 3339
        /// regardless of this setting.
        pub event_time_unit: EventTimeUnit,
    }

    /// A Kafka ingestion source.
    pub struct KafkaSource {
        config: KafkaSourceConfig,
        consumer: Mutex<Option<BaseConsumer>>,
        stats: Mutex<IngestionStats>,
        connected: AtomicBool,
        stopped: AtomicBool,
    }

    impl KafkaSource {
        pub fn new(config: KafkaSourceConfig) -> Self {
            Self {
                config,
                consumer: Mutex::new(None),
                stats: Mutex::new(IngestionStats::default()),
                connected: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
            }
        }

        /// Signal the source to stop polling.
        pub fn stop(&self) {
            self.stopped.store(true, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl IngestionSource for KafkaSource {
        async fn connect(&self) -> Result<(), IngestionError> {
            let consumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.config.brokers)
                .set("group.id", &self.config.group_id)
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest")
                .create()
                .map_err(|e| IngestionError::Connection(format!("Kafka consumer create: {e}")))?;

            consumer
                .subscribe(&[&self.config.topic])
                .map_err(|e| IngestionError::Connection(format!("Kafka subscribe: {e}")))?;

            let mut guard = self.consumer.lock().await;
            *guard = Some(consumer);
            self.connected.store(true, Ordering::Relaxed);
            Ok(())
        }

        async fn poll(&self) -> Result<Option<IngestBatch>, IngestionError> {
            if self.stopped.load(Ordering::Relaxed) {
                return Ok(None);
            }

            let guard = self.consumer.lock().await;
            let consumer = guard.as_ref().ok_or(IngestionError::NotConnected)?;

            // Poll for messages with a non-zero timeout so the thread yields
            // when there's no data, rather than burning CPU in a hot loop.
            let poll_result: Option<
                rdkafka::error::KafkaResult<rdkafka::message::BorrowedMessage<'_>>,
            > = consumer.poll(std::time::Duration::from_millis(100));
            match poll_result {
                Some(Ok(msg)) => {
                    let payload: &[u8] = msg
                        .payload()
                        .ok_or(IngestionError::Poll("empty payload".into()))?;

                    let json: serde_json::Value = serde_json::from_slice(payload)
                        .map_err(|e| IngestionError::Serialization(format!("JSON parse: {e}")))?;

                    // Determine QID: prefer message key, fall back to key_field in JSON
                    let id_bytes: Vec<u8> = {
                        let key_opt: Option<&[u8]> = msg.key();
                        match key_opt {
                            Some(key_bytes) => key_bytes.to_vec(),
                            None => match json.get(&self.config.key_field) {
                                Some(val) => val.as_str().unwrap_or("").as_bytes().to_vec(),
                                None => hash_bytes(payload).to_vec(),
                            },
                        }
                    };
                    let qid = NexoraId::from_bytes(id_bytes);

                    let partition = msg.partition() as u64;
                    let offset = msg.offset();

                    // Kafka record timestamp is transport/broker time — the
                    // event-time fallback when no payload field is configured.
                    let record_ts = msg
                        .timestamp()
                        .to_millis()
                        .and_then(chrono::DateTime::from_timestamp_millis);
                    // Config-first: prefer the configured payload field, else the
                    // record timestamp. Object payloads can read the field; scalar
                    // payloads only have the record timestamp.
                    let event_time = match &json {
                        serde_json::Value::Object(map) => crate::extract_event_time(
                            map,
                            self.config.event_time_field.as_deref(),
                            self.config.event_time_unit,
                            record_ts,
                        ),
                        _ => record_ts,
                    };

                    let mut records = Vec::new();

                    // If the message is a JSON object, create a record per top-level field
                    if let serde_json::Value::Object(map) = &json {
                        for (key, value) in map {
                            if *key == self.config.key_field {
                                continue;
                            }
                            records.push(IngestRecord {
                                qid: qid.clone(),
                                key: key.clone(),
                                value: value.clone(),
                                edge_type: None,
                                edge_target: None,
                                timestamp: event_time,
                                label: None,
                            });
                        }
                    } else {
                        // Scalar or array value: set as a single "value" property
                        records.push(IngestRecord {
                            qid: qid.clone(),
                            key: "value".to_string(),
                            value: json.clone(),
                            edge_type: None,
                            edge_target: None,
                            timestamp: event_time,
                            label: None,
                        });
                    }

                    if records.is_empty() {
                        return Ok(None);
                    }

                    let len = records.len() as u64;

                    let mut stats = self.stats.lock().await;
                    stats.records_ingested += len;
                    stats.batches_processed += 1;

                    Ok(Some(IngestBatch {
                        records,
                        partition: format!("{partition}"),
                        offset_start: offset as u64,
                        offset_end: (offset + 1) as u64,
                        topic: self.config.topic.clone(),
                        raw_events: None, // Kafka uses record-based ingestion, not event-first mode
                    }))
                }
                Some(Err(e)) => Err(IngestionError::Poll(format!("Kafka poll error: {e}"))),
                None => Ok(None),
            }
        }

        async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
            let mut stats = self.stats.lock().await;
            stats.last_offset = Some(offset.clone());

            // Actually commit the offset to the Kafka broker so that on restart
            // the consumer resumes from the last acknowledged position.
            let guard = self.consumer.lock().await;
            if let Some(consumer) = guard.as_ref() {
                let partition: i32 = offset.partition.parse().unwrap_or(0);
                // Build an explicit topic-partition list for the offset we want
                // to commit. The consumer is configured with
                // `enable.auto.commit=false`, so `store_offsets` alone would
                // never reach the broker (it only stages offsets for the
                // auto-commit background thread, which is disabled). We must call
                // `commit` explicitly, or on restart the group resumes from
                // `auto.offset.reset=earliest` and replays the entire topic.
                //
                // Kafka commits the *next* offset to read, i.e. last-processed+1.
                let mut commit_tpl = rdkafka::TopicPartitionList::new();
                commit_tpl
                    .add_partition_offset(
                        &offset.topic,
                        partition,
                        rdkafka::Offset::Offset(offset.offset as i64 + 1),
                    )
                    .map_err(|e| IngestionError::Commit(format!("Kafka commit tpl: {e}")))?;

                if let Err(e) = consumer.commit(&commit_tpl, rdkafka::consumer::CommitMode::Sync) {
                    tracing::warn!(
                        topic = %offset.topic,
                        partition = %offset.partition,
                        offset = offset.offset,
                        error = %e,
                        "Kafka offset commit failed"
                    );
                    return Err(IngestionError::Commit(format!("Kafka commit: {e}")));
                }
            }
            Ok(())
        }

        async fn seek(&self, offsets: &[SourceOffset]) -> Result<(), IngestionError> {
            // Reposition the consumer to resume from the checkpointed cut instead
            // of its broker-committed offset. Each `offset.offset` is the
            // LAST-PROCESSED position, so we seek to +1 (the next record to read)
            // — the same convention `commit` uses. This is what makes checkpoint
            // recovery authoritative: with Relaxed durability the broker offset
            // may be AHEAD of the last durably-flushed checkpoint, so resuming
            // from the broker offset would skip un-flushed records (data loss).
            // Seeking back to the checkpoint offset replays them (idempotent by
            // qid), closing the loss window.
            let guard = self.consumer.lock().await;
            let consumer = guard.as_ref().ok_or(IngestionError::NotConnected)?;
            for offset in offsets {
                if offset.topic != self.config.topic {
                    continue;
                }
                let partition: i32 = offset.partition.parse().unwrap_or(0);
                consumer
                    .seek(
                        &offset.topic,
                        partition,
                        rdkafka::Offset::Offset(offset.offset as i64 + 1),
                        std::time::Duration::from_secs(5),
                    )
                    .map_err(|e| {
                        IngestionError::Commit(format!(
                            "Kafka seek {}:{} to {}: {e}",
                            offset.topic, partition, offset.offset
                        ))
                    })?;
                tracing::info!(
                    topic = %offset.topic,
                    partition = %offset.partition,
                    resume_from = offset.offset + 1,
                    "Kafka consumer sought to checkpoint offset"
                );
            }
            Ok(())
        }

        fn stats(&self) -> IngestionStats {
            self.stats.try_lock().map(|s| s.clone()).unwrap_or_default()
        }

        fn topics(&self) -> Vec<String> {
            vec![self.config.topic.clone()]
        }

        async fn close(&self) -> Result<(), IngestionError> {
            self.stopped.store(true, Ordering::Relaxed);
            let mut guard = self.consumer.lock().await;
            if let Some(_consumer) = guard.take() {
                // Consumer is dropped here, which closes it
            }
            Ok(())
        }
    }

    /// Ensure the Kafka consumer is properly unsubscribed even if the source is
    /// dropped without explicitly calling `close()` (e.g., on panic or early return).
    /// This triggers an immediate consumer group rebalance instead of waiting for
    /// the session timeout.
    impl Drop for KafkaSource {
        fn drop(&mut self) {
            // We can't run async code in Drop, but we can take the consumer
            // synchronously and drop it, which will call rdkafka's Drop implementation.
            // For a graceful unsubscribe, call close() explicitly before drop.
            if let Ok(mut guard) = self.consumer.try_lock() {
                if let Some(consumer) = guard.take() {
                    // Attempt to unsubscribe to trigger immediate rebalance.
                    // If this fails (e.g., connection already lost), the drop
                    // of the consumer will still free resources.
                    let _ = consumer.unsubscribe();
                    // consumer is dropped here, which closes the connection
                }
            }
        }
    }

    /// Simple deterministic hash for QID generation using SHA-256.
    /// Avoids DefaultHasher which is not stable across Rust versions/architectures.
    fn hash_bytes(data: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&result);
        out
    }
}
