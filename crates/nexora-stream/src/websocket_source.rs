//! WebSocket ingestion source (feature `websocket`) — a **push** source adapted
//! to the poll+commit [`IngestionSource`] contract via an internal buffer.
//!
//! Same shape as [`crate::mqtt_source`]: WebSocket is push-model (the server
//! streams frames; there is no offset to seek), so `connect` spawns a background
//! task that reads the socket, parses each text/binary frame's JSON into
//! records, and pushes them into a **bounded** channel; `poll` drains that
//! channel into an [`IngestBatch`]. The bound applies backpressure.
//!
//! Delivery is at-most-once at this layer: WebSocket has no per-message ack or
//! replay, so a dropped connection loses in-flight frames. `commit` only
//! advances stats. Application-level reliability (sequence numbers, replay)
//! would have to live in the payload protocol, which this generic source does
//! not assume.

use crate::{
    IngestBatch, IngestRecord, IngestionError, IngestionSource, IngestionStats, SourceOffset,
};
use futures_util::StreamExt;
use nexora_id::NexoraId;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

use crate::circuit_breaker::StreamCircuitBreaker;
use std::sync::Arc;

/// Configuration for a [`WebSocketSource`].
#[derive(Clone, Debug)]
pub struct WebSocketSourceConfig {
    /// WebSocket URL to connect to (e.g. "ws://localhost:9001/stream").
    pub url: String,
    /// Logical topic/stream name (used for stats and the batch topic field).
    pub topic: String,
    /// JSON field holding the node id (hex NexoraId, else hashed from string).
    pub id_field: String,
    /// Optional JSON field holding each record's event time, for event-time
    /// processing. RFC 3339 string or integer epoch interpreted per
    /// `event_time_unit`. Unset/absent → the record carries no event time.
    pub event_time_field: Option<String>,
    /// How a numeric `event_time_field` is interpreted: seconds, milliseconds,
    /// microseconds, or RFC 3339 string. Defaults to microseconds.
    pub event_time_unit: crate::EventTimeUnit,
    /// Max records returned per `poll`.
    pub max_batch: usize,
    /// Bounded internal buffer capacity (records). Backpressure kicks in here.
    pub buffer_capacity: usize,
}

impl Default for WebSocketSourceConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            topic: "websocket".into(),
            id_field: "id".into(),
            event_time_field: None,
            event_time_unit: crate::EventTimeUnit::default(),
            max_batch: 256,
            buffer_capacity: 10_000,
        }
    }
}

/// A WebSocket ingestion source implementing [`IngestionSource`].
pub struct WebSocketSource {
    config: WebSocketSourceConfig,
    rx: Mutex<Option<mpsc::Receiver<IngestRecord>>>,
    drainer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stats: Mutex<IngestionStats>,
    /// Monotonic message sequence, used as a synthetic offset (WS has none).
    seq: AtomicU64,
    stopped: AtomicBool,
    /// P1-2: Circuit breaker for WebSocket connection operations
    breaker: Arc<StreamCircuitBreaker>,
}

impl WebSocketSource {
    pub fn new(config: WebSocketSourceConfig) -> Self {
        Self {
            config,
            rx: Mutex::new(None),
            drainer: Mutex::new(None),
            stats: Mutex::new(IngestionStats::default()),
            seq: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            breaker: Arc::new(StreamCircuitBreaker::new("websocket")),
        }
    }

    /// The single logical partition of a WebSocket source.
    const PARTITION: &'static str = "0";
}

#[async_trait::async_trait]
impl IngestionSource for WebSocketSource {
    async fn connect(&self) -> Result<(), IngestionError> {
        // P1-3: Wrap with retry logic, then P1-2 circuit breaker
        use nexora_common::retry::{retry_with_backoff_config, RetryConfig};

        let retry_config = RetryConfig::conservative(); // 5 attempts for WebSocket connection

        retry_with_backoff_config(retry_config, || async {
            // P1-2: Circuit breaker wrapper
            let connect_op = async {
            let (ws_stream, _resp) = tokio_tungstenite::connect_async(&self.config.url)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("WS connect {}: {}", self.config.url, e)
                })?;

            let (tx, rx) = mpsc::channel::<IngestRecord>(self.config.buffer_capacity.max(1));
            let id_field = self.config.id_field.clone();
            let event_time_field = self.config.event_time_field.clone();
            let event_time_unit = self.config.event_time_unit;

            // Background drainer: read frames, parse JSON → records, push into the
            // bounded buffer (send awaits when full → backpressure). Exits when the
            // receiver drops (closed) or the socket ends.
            let drainer = tokio::spawn(async move {
                let (_write, mut read) = ws_stream.split();
                while let Some(msg) = read.next().await {
                    let payload: Vec<u8> = match msg {
                        Ok(Message::Text(t)) => t.into_bytes(),
                        Ok(Message::Binary(b)) => b,
                        Ok(Message::Close(_)) => break,
                        Ok(_) => continue, // ping/pong/frame — ignore
                        Err(e) => {
                            tracing::warn!(error = %e, "WS read error");
                            break;
                        }
                    };
                    let records = match parse_frame(
                        &payload,
                        &id_field,
                        event_time_field.as_deref(),
                        event_time_unit,
                    ) {
                        Ok(recs) => recs,
                        Err(e) => {
                            tracing::warn!(error = %e, "WS: skip bad frame");
                            continue;
                        }
                    };
                    for rec in records {
                        if tx.send(rec).await.is_err() {
                            return; // receiver dropped — source closed
                        }
                    }
                }
            });

            *self.rx.lock().await = Some(rx);
            *self.drainer.lock().await = Some(drainer);
            Ok(())
        };

        self.breaker.call::<_, (), anyhow::Error>(connect_op).await.map_err(|e| match e {
            crate::circuit_breaker::CircuitBreakerError::CircuitOpen(s) => {
                IngestionError::Connection(format!("Circuit breaker open: {}", s))
            }
            crate::circuit_breaker::CircuitBreakerError::OperationFailed(e) => {
                IngestionError::Connection(format!("{}", e))
            }
        })
        })
        .await
        .map_err(|e| IngestionError::Connection(format!("Failed to connect to WebSocket after retries: {}", e)))
    }

    async fn poll(&self) -> Result<Option<IngestBatch>, IngestionError> {
        if self.stopped.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let mut guard = self.rx.lock().await;
        let rx = guard.as_mut().ok_or(IngestionError::NotConnected)?;

        let mut records = Vec::new();
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(rec)) => records.push(rec),
            Ok(None) => return Ok(None), // channel closed
            Err(_) => return Ok(None),   // idle this poll
        }
        while records.len() < self.config.max_batch {
            match rx.try_recv() {
                Ok(rec) => records.push(rec),
                Err(_) => break,
            }
        }

        let len = records.len() as u64;
        let offset_end = self.seq.fetch_add(len, Ordering::Relaxed) + len;
        let offset_start = offset_end - len;
        {
            let mut stats = self.stats.lock().await;
            stats.records_ingested += len;
            stats.batches_processed += 1;
        }

        Ok(Some(IngestBatch {
            records,
            partition: Self::PARTITION.to_string(),
            offset_start,
            offset_end,
            topic: self.config.topic.clone(),
            raw_events: None,
        }))
    }

    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
        // No replayable offset for a raw WebSocket; just record progress.
        let mut stats = self.stats.lock().await;
        stats.last_offset = Some(offset.clone());
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
        // Dropping the receiver signals the drainer to exit; abort for good
        // measure (also closes the socket, owned by the task).
        *self.rx.lock().await = None;
        if let Some(handle) = self.drainer.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

/// Parse a WebSocket frame payload into records — one per top-level field of a
/// JSON object (skipping the id field), node id from `id_field`; a non-object
/// value becomes a single `value` property keyed by the id field's absence
/// (topic-less, so hashed from the raw payload). Mirrors the MQTT source.
fn parse_frame(
    payload: &[u8],
    id_field: &str,
    event_time_field: Option<&str>,
    event_time_unit: crate::EventTimeUnit,
) -> Result<Vec<IngestRecord>, String> {
    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("JSON parse: {e}"))?;

    match &json {
        serde_json::Value::Object(map) => {
            let qid = match map.get(id_field) {
                Some(serde_json::Value::String(s)) => NexoraId::from_hex(s)
                    .unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec())),
                Some(serde_json::Value::Number(n)) => n
                    .as_i64()
                    .map(|i| NexoraId::from_bytes(i.to_be_bytes().to_vec()))
                    .ok_or_else(|| "id must be string or integer".to_string())?,
                _ => return Err(format!("missing/invalid id field '{id_field}'")),
            };
            let timestamp = crate::extract_event_time(map, event_time_field, event_time_unit, None);
            let mut records = Vec::new();
            for (key, value) in map {
                if key == id_field {
                    continue;
                }
                records.push(IngestRecord {
                    qid: qid.clone(),
                    key: key.clone(),
                    value: value.clone(),
                    edge_type: None,
                    edge_target: None,
                    timestamp,
                    label: None,
                });
            }
            Ok(records)
        }
        _ => Err("frame is not a JSON object".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_object_frame() {
        let recs = parse_frame(br#"{"id":"n1","a":1,"b":2}"#, "id", None, crate::EventTimeUnit::default()).unwrap();
        assert_eq!(recs.len(), 2);
        let qid = NexoraId::from_bytes(b"n1".to_vec());
        assert!(recs.iter().all(|r| r.qid == qid));
        let keys: Vec<_> = recs.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"a") && keys.contains(&"b") && !keys.contains(&"id"));
    }

    #[test]
    fn parse_hex_id() {
        let recs = parse_frame(br#"{"id":"666f6f","v":1}"#, "id", None, crate::EventTimeUnit::default()).unwrap();
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"foo".to_vec()));
    }

    #[test]
    fn parse_missing_id_errors() {
        assert!(parse_frame(br#"{"v":1}"#, "id", None, crate::EventTimeUnit::default()).is_err());
    }

    #[test]
    fn parse_non_object_errors() {
        assert!(parse_frame(b"42", "id", None, crate::EventTimeUnit::default()).is_err());
        assert!(parse_frame(b"not json", "id", None, crate::EventTimeUnit::default()).is_err());
    }
}
