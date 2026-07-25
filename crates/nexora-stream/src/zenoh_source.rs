//! Zenoh ingestion source (feature `zenoh`) — a **push** source adapted to the
//! poll+commit [`IngestionSource`] contract via an internal buffer.
//!
//! Same shape as [`crate::mqtt_source`] / [`crate::websocket_source`]: zenoh is
//! push-model (a subscriber receives samples the publisher pushes; there is no
//! offset to seek), so `connect` opens a session, declares a subscriber on the
//! configured key expression, and spawns a background task that parses each
//! sample's payload into records and pushes them into a **bounded** channel;
//! `poll` drains that channel into an [`IngestBatch`]. The bound applies
//! backpressure.
//!
//! Delivery is best-effort at this layer (zenoh's default): no per-sample ack or
//! replay, so `commit` only advances stats. Only `Put` samples are ingested;
//! `Delete` samples are ignored (a graph delete semantics mapping is a
//! follow-up if a use case needs it).

use crate::{
    IngestBatch, IngestRecord, IngestionError, IngestionSource, IngestionStats, SourceOffset,
};
use nexora_id::NexoraId;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use zenoh::sample::SampleKind;

/// Configuration for a [`ZenohSource`].
#[derive(Clone, Debug)]
pub struct ZenohSourceConfig {
    /// Key expression to subscribe to (e.g. "nexora/ingest/**").
    pub key_expr: String,
    /// Logical topic/stream name (used for stats and the batch topic field).
    pub topic: String,
    /// JSON field holding the node id (hex NexoraId, else hashed from string).
    pub id_field: String,
    /// Optional JSON field holding each record's event time, for event-time
    /// processing. RFC 3339 string or integer epoch interpreted per
    /// `event_time_unit`. When absent, the Zenoh sample's own timestamp (if any)
    /// is used as a fallback.
    pub event_time_field: Option<String>,
    /// How a numeric `event_time_field` is interpreted: seconds, milliseconds,
    /// microseconds, or RFC 3339 string. Defaults to microseconds.
    pub event_time_unit: crate::EventTimeUnit,
    /// Max records returned per `poll`.
    pub max_batch: usize,
    /// Bounded internal buffer capacity (records). Backpressure kicks in here.
    pub buffer_capacity: usize,
}

impl Default for ZenohSourceConfig {
    fn default() -> Self {
        Self {
            key_expr: String::new(),
            topic: "zenoh".into(),
            id_field: "id".into(),
            event_time_field: None,
            event_time_unit: crate::EventTimeUnit::default(),
            max_batch: 256,
            buffer_capacity: 10_000,
        }
    }
}

/// A zenoh ingestion source implementing [`IngestionSource`].
pub struct ZenohSource {
    config: ZenohSourceConfig,
    rx: Mutex<Option<mpsc::Receiver<IngestRecord>>>,
    /// Kept alive so the session + subscriber stay up; dropping disconnects.
    session: Mutex<Option<zenoh::Session>>,
    drainer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stats: Mutex<IngestionStats>,
    /// Monotonic message sequence, used as a synthetic offset (zenoh has none).
    seq: AtomicU64,
    stopped: AtomicBool,
}

impl ZenohSource {
    pub fn new(config: ZenohSourceConfig) -> Self {
        Self {
            config,
            rx: Mutex::new(None),
            session: Mutex::new(None),
            drainer: Mutex::new(None),
            stats: Mutex::new(IngestionStats::default()),
            seq: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    /// The single logical partition of a zenoh source.
    const PARTITION: &'static str = "0";
}

#[async_trait::async_trait]
impl IngestionSource for ZenohSource {
    async fn connect(&self) -> Result<(), IngestionError> {
        let session = zenoh::open(zenoh::Config::default())
            .await
            .map_err(|e| IngestionError::Connection(format!("zenoh open: {e}")))?;

        let subscriber = session
            .declare_subscriber(self.config.key_expr.clone())
            .await
            .map_err(|e| {
                IngestionError::Connection(format!("zenoh subscribe {}: {e}", self.config.key_expr))
            })?;

        let (tx, rx) = mpsc::channel::<IngestRecord>(self.config.buffer_capacity.max(1));
        let id_field = self.config.id_field.clone();
        let event_time_field = self.config.event_time_field.clone();
        let event_time_unit = self.config.event_time_unit;

        // Background drainer: receive samples, parse Put payloads into records,
        // push into the bounded buffer (send awaits when full → backpressure).
        // Exits when the receiver drops (closed) or the subscriber ends.
        let drainer = tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                if sample.kind() != SampleKind::Put {
                    continue; // ignore Delete samples
                }
                let key = sample.key_expr().as_str().to_string();
                let payload = sample.payload().to_bytes();
                // Zenoh samples may carry a source timestamp (NTP64). Use it as
                // the event-time fallback when the payload has no configured
                // event-time field.
                let sample_ts = sample.timestamp().and_then(|ts| {
                    let d = ts.get_time().to_duration();
                    chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                });
                let records = match parse_sample(
                    &key,
                    &payload,
                    &id_field,
                    event_time_field.as_deref(),
                    event_time_unit,
                    sample_ts,
                ) {
                    Ok(recs) => recs,
                    Err(e) => {
                        tracing::warn!(key = %key, error = %e, "zenoh: skip bad sample");
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
        *self.session.lock().await = Some(session);
        *self.drainer.lock().await = Some(drainer);
        Ok(())
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
        }))
    }

    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
        // No replayable offset for zenoh pub/sub; just record progress.
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
        // measure; drop the session to disconnect.
        *self.rx.lock().await = None;
        if let Some(handle) = self.drainer.lock().await.take() {
            handle.abort();
        }
        *self.session.lock().await = None;
        Ok(())
    }
}

/// Parse a zenoh sample payload into records — one per top-level field of a
/// JSON object (skipping the id field), node id from `id_field` (fallback to the
/// sample key when absent). Mirrors the MQTT source.
fn parse_sample(
    key: &str,
    payload: &[u8],
    id_field: &str,
    event_time_field: Option<&str>,
    event_time_unit: crate::EventTimeUnit,
    sample_ts: Option<chrono::DateTime<chrono::Utc>>,
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
                _ => NexoraId::from_bytes(key.as_bytes().to_vec()),
            };
            let timestamp =
                crate::extract_event_time(map, event_time_field, event_time_unit, sample_ts);
            let mut records = Vec::new();
            for (k, value) in map {
                if k == id_field {
                    continue;
                }
                records.push(IngestRecord {
                    qid: qid.clone(),
                    key: k.clone(),
                    value: value.clone(),
                    edge_type: None,
                    edge_target: None,
                    timestamp,
                    label: None,
                });
            }
            Ok(records)
        }
        other => {
            // Scalar/array: key the node by the sample key, set a `value` prop.
            // No JSON object to read a field from, so the event time is the
            // sample's own timestamp (if any).
            Ok(vec![IngestRecord {
                qid: NexoraId::from_bytes(key.as_bytes().to_vec()),
                key: "value".to_string(),
                value: other.clone(),
                edge_type: None,
                edge_target: None,
                timestamp: sample_ts,
                label: None,
            }])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_object_sample() {
        let recs = parse_sample(
            "nexora/ingest/a",
            br#"{"id":"z1","x":1,"y":2}"#,
            "id",
            None,
            None,
        )
        .unwrap();
        assert_eq!(recs.len(), 2);
        let qid = NexoraId::from_bytes(b"z1".to_vec());
        assert!(recs.iter().all(|r| r.qid == qid));
    }

    #[test]
    fn parse_missing_id_falls_back_to_key() {
        let recs = parse_sample("sensors/5", br#"{"temp":20}"#, "id", None, None).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"sensors/5".to_vec()));
        assert_eq!(recs[0].key, "temp");
    }

    #[test]
    fn parse_scalar_keyed_by_sample_key() {
        let recs = parse_sample("counter", b"42", "id", None, None).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"counter".to_vec()));
        assert_eq!(recs[0].key, "value");
        assert_eq!(recs[0].value, serde_json::json!(42));
    }

    #[test]
    fn parse_hex_id() {
        let recs = parse_sample("k", br#"{"id":"666f6f","v":1}"#, "id", None, None).unwrap();
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"foo".to_vec()));
    }

    #[test]
    fn parse_bad_json_errors() {
        assert!(parse_sample("k", b"not json", "id", None, None).is_err());
    }
}
