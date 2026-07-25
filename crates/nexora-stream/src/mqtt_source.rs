//! MQTT ingestion source (feature `mqtt`) — a **push** source adapted to the
//! poll+commit [`IngestionSource`] contract via an internal buffer.
//!
//! # Push → poll adaptation
//!
//! MQTT is push-model: the broker delivers messages through an event loop the
//! consumer does not drive, and there is no server-side offset to seek. To fit
//! the same [`IngestionSource`] trait as Kafka/file, `connect` spawns a
//! background task that drives the `rumqttc` event loop and pushes parsed
//! records into a **bounded** channel; `poll` drains that channel into an
//! [`IngestBatch`]. The bound applies backpressure: if the graph can't keep up,
//! the buffer fills and the drainer awaits, rather than growing without limit.
//!
//! # Delivery semantics
//!
//! Durability is the MQTT QoS, handled by the transport: QoS 1 (default here)
//! gives at-least-once — `rumqttc` auto-acks `PUBLISH` after the event loop
//! yields it, so a record is acked to the broker once buffered. There is no
//! replayable offset, so `commit` only advances stats; use `BatchDurability`
//! `WaitDurable` on the graph sink if the ingest must not lose un-fsynced writes
//! across a crash (MQTT will not redeliver an already-acked message).

use crate::{
    IngestBatch, IngestRecord, IngestionError, IngestionSource, IngestionStats, SourceOffset,
};
use nexora_id::NexoraId;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Configuration for an [`MqttSource`].
#[derive(Clone, Debug)]
pub struct MqttSourceConfig {
    /// Broker host (e.g. "localhost").
    pub host: String,
    /// Broker port (typically 1883, or 8883 for TLS — TLS not configured here).
    pub port: u16,
    /// MQTT client id (must be unique per broker connection).
    pub client_id: String,
    /// Topic filters to subscribe to (supports MQTT wildcards `+` / `#`).
    pub topics: Vec<String>,
    /// QoS level for subscriptions (0, 1, or 2; clamped to valid range).
    pub qos: u8,
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

impl Default for MqttSourceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 1883,
            client_id: "nexora-mqtt".into(),
            topics: Vec::new(),
            qos: 1,
            id_field: "id".into(),
            event_time_field: None,
            event_time_unit: crate::EventTimeUnit::default(),
            max_batch: 256,
            buffer_capacity: 10_000,
        }
    }
}

/// An MQTT ingestion source implementing [`IngestionSource`].
pub struct MqttSource {
    config: MqttSourceConfig,
    /// Receiver end of the buffer the background drainer feeds. `None` until
    /// `connect`.
    rx: Mutex<Option<mpsc::Receiver<IngestRecord>>>,
    /// Kept alive so the connection (and its event loop task) stays up; dropping
    /// it disconnects.
    client: Mutex<Option<AsyncClient>>,
    /// Background event-loop task handle, aborted on `close`.
    drainer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stats: Mutex<IngestionStats>,
    /// Monotonic message sequence, used as a synthetic offset (MQTT has none).
    seq: AtomicU64,
    stopped: AtomicBool,
}

impl MqttSource {
    pub fn new(config: MqttSourceConfig) -> Self {
        Self {
            config,
            rx: Mutex::new(None),
            client: Mutex::new(None),
            drainer: Mutex::new(None),
            stats: Mutex::new(IngestionStats::default()),
            seq: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    fn qos(&self) -> QoS {
        match self.config.qos {
            0 => QoS::AtMostOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtLeastOnce,
        }
    }

    /// The single logical partition of an MQTT source.
    const PARTITION: &'static str = "0";
}

#[async_trait::async_trait]
impl IngestionSource for MqttSource {
    async fn connect(&self) -> Result<(), IngestionError> {
        let mut opts = MqttOptions::new(
            self.config.client_id.clone(),
            self.config.host.clone(),
            self.config.port,
        );
        opts.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(opts, self.config.buffer_capacity.max(16));

        // Subscribe to all configured topics.
        for topic in &self.config.topics {
            client
                .subscribe(topic, self.qos())
                .await
                .map_err(|e| IngestionError::Connection(format!("MQTT subscribe {topic}: {e}")))?;
        }

        let (tx, rx) = mpsc::channel::<IngestRecord>(self.config.buffer_capacity.max(1));
        let id_field = self.config.id_field.clone();
        let event_time_field = self.config.event_time_field.clone();
        let event_time_unit = self.config.event_time_unit;

        // Background drainer: drive the event loop, parse PUBLISH packets into
        // records, and push them into the bounded buffer. `tx.send` awaits when
        // the buffer is full → backpressure. Exits when the receiver drops
        // (source closed) or the connection errors terminally.
        let drainer = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        let records = match parse_publish(
                            &publish.topic,
                            &publish.payload,
                            &id_field,
                            event_time_field.as_deref(),
                            event_time_unit,
                        ) {
                            Ok(recs) => recs,
                            Err(e) => {
                                tracing::warn!(topic = %publish.topic, error = %e, "MQTT: skip bad payload");
                                continue;
                            }
                        };
                        for rec in records {
                            if tx.send(rec).await.is_err() {
                                return; // receiver dropped — source closed
                            }
                        }
                    }
                    Ok(_) => {} // other events (acks, pings, connack) — ignore
                    Err(e) => {
                        // Transport error: log and keep looping; rumqttc retries
                        // the connection internally. Back off briefly to avoid a
                        // hot error loop.
                        tracing::warn!(error = %e, "MQTT event loop error");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });

        *self.rx.lock().await = Some(rx);
        *self.client.lock().await = Some(client);
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
        // Block for the first record (with a timeout so the caller isn't parked
        // forever on an idle topic), then drain whatever else is buffered up to
        // max_batch without blocking.
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(rec)) => records.push(rec),
            Ok(None) => return Ok(None), // channel closed
            Err(_) => return Ok(None),   // idle — no data this poll
        }
        while records.len() < self.config.max_batch {
            match rx.try_recv() {
                Ok(rec) => records.push(rec),
                Err(_) => break, // buffer drained
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

        // MQTT has no per-topic offset; report the synthetic sequence and the
        // first subscribed topic (records carry their own real topic upstream).
        let topic = self.config.topics.first().cloned().unwrap_or_default();
        Ok(Some(IngestBatch {
            records,
            partition: Self::PARTITION.to_string(),
            offset_start,
            offset_end,
            topic,
        }))
    }

    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
        // No replayable broker offset for MQTT (QoS handles delivery); just
        // record progress in stats.
        let mut stats = self.stats.lock().await;
        stats.last_offset = Some(offset.clone());
        Ok(())
    }

    fn stats(&self) -> IngestionStats {
        self.stats.try_lock().map(|s| s.clone()).unwrap_or_default()
    }

    fn topics(&self) -> Vec<String> {
        self.config.topics.clone()
    }

    async fn close(&self) -> Result<(), IngestionError> {
        self.stopped.store(true, Ordering::Relaxed);
        // Drop the client (disconnects) and the receiver (signals the drainer to
        // exit), then abort the task for good measure.
        if let Some(client) = self.client.lock().await.take() {
            let _ = client.disconnect().await;
        }
        *self.rx.lock().await = None;
        if let Some(handle) = self.drainer.lock().await.take() {
            handle.abort();
        }
        Ok(())
    }
}

/// Parse an MQTT PUBLISH payload into records — one per top-level field of a
/// JSON object (skipping the id field), node id from `id_field`. A non-object
/// JSON value becomes a single `value` property. Mirrors the Kafka source.
fn parse_publish(
    topic: &str,
    payload: &[u8],
    id_field: &str,
    event_time_field: Option<&str>,
    event_time_unit: crate::EventTimeUnit,
) -> Result<Vec<IngestRecord>, String> {
    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(|e| format!("JSON parse: {e}"))?;

    match &json {
        serde_json::Value::Object(map) => {
            let qid = extract_id(map, id_field, topic);
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
        other => {
            // Scalar/array: key the node by topic, set a single "value" property.
            let qid = NexoraId::from_bytes(topic.as_bytes().to_vec());
            Ok(vec![IngestRecord {
                qid,
                key: "value".to_string(),
                value: other.clone(),
                edge_type: None,
                edge_target: None,
                timestamp: None,
                label: None,
            }])
        }
    }
}

/// Resolve the node id from the JSON object's `id_field`, falling back to the
/// topic name when the field is absent.
fn extract_id(
    map: &serde_json::Map<String, serde_json::Value>,
    id_field: &str,
    topic: &str,
) -> NexoraId {
    match map.get(id_field) {
        Some(serde_json::Value::String(s)) => {
            NexoraId::from_hex(s).unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec()))
        }
        Some(serde_json::Value::Number(n)) => n
            .as_i64()
            .map(|i| NexoraId::from_bytes(i.to_be_bytes().to_vec()))
            .unwrap_or_else(|| NexoraId::from_bytes(topic.as_bytes().to_vec())),
        _ => NexoraId::from_bytes(topic.as_bytes().to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A JSON object yields one record per non-id field, keyed by id_field.
    #[test]
    fn parse_object_payload() {
        let payload = br#"{"id":"node-1","x":1,"y":"two"}"#;
        let recs = parse_publish(
            "sensors/temp",
            payload,
            "id",
            None,
            crate::EventTimeUnit::default(),
        )
        .unwrap();
        assert_eq!(recs.len(), 2, "two non-id fields");
        let expected_qid = NexoraId::from_bytes(b"node-1".to_vec());
        assert!(recs.iter().all(|r| r.qid == expected_qid));
        let keys: Vec<_> = recs.iter().map(|r| r.key.as_str()).collect();
        assert!(keys.contains(&"x") && keys.contains(&"y"));
        assert!(!keys.contains(&"id"), "id field is not a property");
    }

    /// Missing id field falls back to topic-derived node id.
    #[test]
    fn parse_object_missing_id_falls_back_to_topic() {
        let payload = br#"{"temp":21.5}"#;
        let recs = parse_publish(
            "room/1",
            payload,
            "id",
            None,
            crate::EventTimeUnit::default(),
        )
        .unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"room/1".to_vec()));
        assert_eq!(recs[0].key, "temp");
    }

    /// A scalar payload becomes a single `value` property keyed by topic.
    #[test]
    fn parse_scalar_payload() {
        let recs = parse_publish(
            "counter",
            b"42",
            "id",
            None,
            crate::EventTimeUnit::default(),
        )
        .unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"counter".to_vec()));
        assert_eq!(recs[0].key, "value");
        assert_eq!(recs[0].value, serde_json::json!(42));
    }

    /// Malformed JSON is a parse error (the drainer skips it, non-fatal).
    #[test]
    fn parse_bad_json_errors() {
        assert!(parse_publish(
            "t",
            b"not json",
            "id",
            None,
            crate::EventTimeUnit::default()
        )
        .is_err());
    }

    /// Hex id decodes to the raw bytes (matches Kafka/file id handling).
    #[test]
    fn extract_id_hex() {
        let payload = br#"{"id":"666f6f","v":1}"#; // "foo" in hex
        let recs =
            parse_publish("t", payload, "id", None, crate::EventTimeUnit::default()).unwrap();
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"foo".to_vec()));
    }

    /// qos() clamps out-of-range values to AtLeastOnce.
    #[test]
    fn qos_mapping() {
        let mk = |q: u8| {
            MqttSource::new(MqttSourceConfig {
                qos: q,
                ..Default::default()
            })
            .qos()
        };
        assert_eq!(mk(0), QoS::AtMostOnce);
        assert_eq!(mk(1), QoS::AtLeastOnce);
        assert_eq!(mk(2), QoS::ExactlyOnce);
        assert_eq!(mk(9), QoS::AtLeastOnce, "invalid qos → AtLeastOnce");
    }
}
