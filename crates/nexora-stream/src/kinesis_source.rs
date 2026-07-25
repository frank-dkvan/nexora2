//! Kinesis ingestion source (feature `kinesis`) — poll+commit, same shape as
//! the Kafka source.
//!
//! Kinesis is poll-model with a resumable position, so it maps cleanly onto the
//! [`IngestionSource`] contract:
//!   - `connect` resolves the stream's shards and opens a shard iterator per
//!     shard (resuming `AFTER_SEQUENCE_NUMBER` from a committed checkpoint if an
//!     [`OffsetStore`] has one, else `TRIM_HORIZON`).
//!   - `poll` calls `GetRecords` on each shard's iterator, parses record data
//!     into [`IngestRecord`]s, and advances the in-memory iterator.
//!   - `commit` persists the last-processed sequence number per shard via
//!     [`OffsetStore::save_str`], so a restart resumes `AFTER_SEQUENCE_NUMBER`
//!     rather than replaying from the horizon (exactly-once within retention).
//!
//! Kinesis sequence numbers are large decimal strings, not the `u64` the shared
//! [`SourceOffset`] carries, so the checkpoint uses the string offset methods
//! (`save_str`/`load_str`) keyed by `(stream, shard_id)`. (`SourceOffset.offset`
//! is set to the record count for stats/monotonicity only.)

use crate::{
    IngestBatch, IngestRecord, IngestionError, IngestionSource, IngestionStats, OffsetStore,
    SourceOffset,
};
use aws_sdk_kinesis::Client;
use nexora_id::NexoraId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Configuration for a [`KinesisSource`].
#[derive(Clone, Debug)]
pub struct KinesisSourceConfig {
    /// Kinesis stream name.
    pub stream_name: String,
    /// AWS region (e.g. "us-east-1"). `None` uses the SDK's default resolution.
    pub region: Option<String>,
    /// Optional endpoint override (e.g. "http://localhost:4566" for LocalStack).
    pub endpoint_url: Option<String>,
    /// JSON field holding the node id (hex NexoraId, else hashed from string).
    pub id_field: String,
    /// Optional JSON field holding each record's event time, for event-time
    /// processing (out-of-order-safe LWW + windowing). RFC 3339 string or
    /// integer epoch interpreted per `event_time_unit`. Unset/absent → the
    /// record carries no event time.
    pub event_time_field: Option<String>,
    /// How a numeric `event_time_field` is interpreted: seconds, milliseconds,
    /// microseconds, or RFC 3339 string. Defaults to microseconds.
    pub event_time_unit: crate::EventTimeUnit,
    /// Max records requested per shard per `poll` (Kinesis GetRecords limit).
    pub max_batch: i32,
}

impl Default for KinesisSourceConfig {
    fn default() -> Self {
        Self {
            stream_name: String::new(),
            region: None,
            endpoint_url: None,
            id_field: "id".into(),
            event_time_field: None,
            event_time_unit: crate::EventTimeUnit::default(),
            max_batch: 256,
        }
    }
}

/// Per-shard cursor: the next shard iterator to read from, and the last
/// sequence number we processed (the checkpoint).
struct ShardCursor {
    iterator: Option<String>,
    last_sequence: Option<String>,
}

/// A Kinesis ingestion source implementing [`IngestionSource`].
pub struct KinesisSource {
    config: KinesisSourceConfig,
    offset_store: Option<Arc<dyn OffsetStore>>,
    client: Mutex<Option<Client>>,
    /// shard_id → cursor.
    shards: Mutex<HashMap<String, ShardCursor>>,
    stats: Mutex<IngestionStats>,
    seq: AtomicU64,
    stopped: AtomicBool,
}

impl KinesisSource {
    pub fn new(config: KinesisSourceConfig) -> Self {
        Self {
            config,
            offset_store: None,
            client: Mutex::new(None),
            shards: Mutex::new(HashMap::new()),
            stats: Mutex::new(IngestionStats::default()),
            seq: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    /// Attach an offset store so the source resumes from committed sequence
    /// numbers on `connect` and persists them on `commit`.
    pub fn with_offset_store(mut self, store: Arc<dyn OffsetStore>) -> Self {
        self.offset_store = Some(store);
        self
    }

    /// Signal the source to stop polling.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// Offset-store key for a shard's checkpoint (topic = stream, partition =
    /// shard id). The stored value is the sequence-number string.
    fn checkpoint_key(&self, shard_id: &str) -> (String, String) {
        (self.config.stream_name.clone(), shard_id.to_string())
    }
}

#[async_trait::async_trait]
impl IngestionSource for KinesisSource {
    async fn connect(&self) -> Result<(), IngestionError> {
        // Build the SDK config, honoring optional region + endpoint override.
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &self.config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &self.config.endpoint_url {
            loader = loader.endpoint_url(endpoint.clone());
        }
        let aws_cfg = loader.load().await;
        let client = Client::new(&aws_cfg);

        // List shards for the stream.
        let shards_resp = client
            .list_shards()
            .stream_name(&self.config.stream_name)
            .send()
            .await
            .map_err(|e| IngestionError::Connection(format!("Kinesis list_shards: {e}")))?;

        let mut shards = self.shards.lock().await;
        for shard in shards_resp.shards() {
            let shard_id = shard.shard_id().to_string();

            // Resume from a committed string checkpoint (Kinesis sequence
            // number) if present; else start at TRIM_HORIZON (oldest retained).
            let after_sequence: Option<String> = if let Some(store) = &self.offset_store {
                let (t, p) = self.checkpoint_key(&shard_id);
                store.load_str(&t, &p).await.map_err(|e| {
                    IngestionError::Connection(format!("Kinesis checkpoint load: {e}"))
                })?
            } else {
                None
            };

            let iterator = build_iterator(
                &client,
                &self.config.stream_name,
                &shard_id,
                after_sequence.as_deref(),
            )
            .await?;
            shards.insert(
                shard_id,
                ShardCursor {
                    iterator: Some(iterator),
                    last_sequence: None,
                },
            );
        }
        drop(shards);

        *self.client.lock().await = Some(client);
        Ok(())
    }

    async fn poll(&self) -> Result<Option<IngestBatch>, IngestionError> {
        if self.stopped.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let client_guard = self.client.lock().await;
        let client = client_guard.as_ref().ok_or(IngestionError::NotConnected)?;

        let mut shards = self.shards.lock().await;
        let mut records = Vec::new();
        let mut last_shard = String::new();

        // Round-robin one GetRecords per shard that still has an iterator.
        for (shard_id, cursor) in shards.iter_mut() {
            let Some(iterator) = cursor.iterator.clone() else {
                continue;
            };
            let resp = client
                .get_records()
                .shard_iterator(iterator)
                .limit(self.config.max_batch)
                .send()
                .await
                .map_err(|e| IngestionError::Poll(format!("Kinesis get_records: {e}")))?;

            // Advance the iterator for next poll (None = shard closed).
            cursor.iterator = resp.next_shard_iterator().map(|s| s.to_string());

            for rec in resp.records() {
                let seq = rec.sequence_number().to_string();
                cursor.last_sequence = Some(seq);
                last_shard = shard_id.clone();
                match parse_data(
                    rec.data().as_ref(),
                    &self.config.id_field,
                    self.config.event_time_field.as_deref(),
                    self.config.event_time_unit,
                ) {
                    Ok(mut recs) => records.append(&mut recs),
                    Err(e) => {
                        let mut stats = self.stats.lock().await;
                        stats.errors += 1;
                        tracing::warn!(error = %e, "Kinesis: skip bad record");
                    }
                }
            }
        }

        if records.is_empty() {
            return Ok(None);
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
            partition: last_shard,
            offset_start,
            offset_end,
            topic: self.config.stream_name.clone(),
        }))
    }

    async fn commit(&self, offset: &SourceOffset) -> Result<(), IngestionError> {
        {
            let mut stats = self.stats.lock().await;
            stats.last_offset = Some(offset.clone());
        }
        // Persist each shard's last-processed sequence number as its string
        // checkpoint, so a restart resumes AFTER it (exactly-once within the
        // stream's retention window). No-op for shards that produced nothing.
        if let Some(store) = &self.offset_store {
            let shards = self.shards.lock().await;
            for (shard_id, cursor) in shards.iter() {
                if let Some(seq) = &cursor.last_sequence {
                    let (t, p) = self.checkpoint_key(shard_id);
                    store
                        .save_str(&t, &p, seq)
                        .await
                        .map_err(IngestionError::Commit)?;
                }
            }
        }
        Ok(())
    }

    fn stats(&self) -> IngestionStats {
        self.stats.try_lock().map(|s| s.clone()).unwrap_or_default()
    }

    fn topics(&self) -> Vec<String> {
        vec![self.config.stream_name.clone()]
    }

    async fn close(&self) -> Result<(), IngestionError> {
        self.stopped.store(true, Ordering::Relaxed);
        *self.client.lock().await = None;
        self.shards.lock().await.clear();
        Ok(())
    }
}

/// Open a shard iterator, resuming after a committed sequence number when one is
/// provided, else starting at TRIM_HORIZON (oldest available).
///
/// B1.4 full-snapshot fallback: a committed checkpoint can age past the stream's
/// retention window (24h–365d). When that happens, `AFTER_SEQUENCE_NUMBER`
/// resolution fails, and there is no point retrying the same dead sequence — the
/// data behind it is gone from Kinesis. Rather than wedge the source on a fatal
/// error, we fall back to `TRIM_HORIZON` (replay from the oldest still-retained
/// record), which is the closest available approximation of a full re-read. This
/// is safe precisely because graph writes are idempotent by `__qid` (roadmap
/// B1.3): replaying already-applied records converges rather than duplicates. The
/// gap between the expired checkpoint and the retention horizon is unrecoverable
/// from Kinesis alone and is logged loudly rather than hidden.
async fn build_iterator(
    client: &Client,
    stream: &str,
    shard_id: &str,
    after_sequence: Option<&str>,
) -> Result<String, IngestionError> {
    match request_iterator(client, stream, shard_id, after_sequence).await {
        Ok(it) => Ok(it),
        // Only a resume attempt can trip the expired-checkpoint case; a
        // TRIM_HORIZON open failing is a genuine error, so don't mask it.
        Err(e) if after_sequence.is_some() => {
            tracing::warn!(
                stream = %stream,
                shard_id = %shard_id,
                error = %e,
                "Kinesis: committed checkpoint no longer resumable (likely aged past \
                 retention); falling back to TRIM_HORIZON full replay. Records between the \
                 expired checkpoint and the retention horizon are unrecoverable from Kinesis; \
                 idempotent-by-qid writes make the replay safe (roadmap B1.4)."
            );
            request_iterator(client, stream, shard_id, None).await
        }
        Err(e) => Err(e),
    }
}

/// Single `get_shard_iterator` call — `AFTER_SEQUENCE_NUMBER` when a checkpoint is
/// given, else `TRIM_HORIZON`. Fallback logic lives in [`build_iterator`].
async fn request_iterator(
    client: &Client,
    stream: &str,
    shard_id: &str,
    after_sequence: Option<&str>,
) -> Result<String, IngestionError> {
    use aws_sdk_kinesis::types::ShardIteratorType;
    let mut req = client
        .get_shard_iterator()
        .stream_name(stream)
        .shard_id(shard_id);
    req = match after_sequence {
        Some(seq) => req
            .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
            .starting_sequence_number(seq),
        None => req.shard_iterator_type(ShardIteratorType::TrimHorizon),
    };
    let resp = req
        .send()
        .await
        .map_err(|e| IngestionError::Connection(format!("Kinesis get_shard_iterator: {e}")))?;
    resp.shard_iterator()
        .map(|s| s.to_string())
        .ok_or_else(|| IngestionError::Connection("Kinesis returned no shard iterator".into()))
}

/// Parse a Kinesis record's data blob into records — one per top-level field of
/// a JSON object (skipping the id field), node id from `id_field`. Mirrors the
/// Kafka source's object handling; a non-object value becomes a `value` prop.
fn parse_data(
    data: &[u8],
    id_field: &str,
    event_time_field: Option<&str>,
    event_time_unit: crate::EventTimeUnit,
) -> Result<Vec<IngestRecord>, String> {
    let json: serde_json::Value =
        serde_json::from_slice(data).map_err(|e| format!("JSON parse: {e}"))?;

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
            // Kinesis records also carry an ApproximateArrivalTimestamp, but that
            // is processing time; prefer the configured business event-time field.
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
        _ => Err("record is not a JSON object".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_object_data() {
        let recs = parse_data(
            br#"{"id":"k1","a":1,"b":"two"}"#,
            "id",
            None,
            crate::EventTimeUnit::default(),
        )
        .unwrap();
        assert_eq!(recs.len(), 2);
        let qid = NexoraId::from_bytes(b"k1".to_vec());
        assert!(recs.iter().all(|r| r.qid == qid));
    }

    #[test]
    fn parse_hex_id() {
        let recs = parse_data(
            br#"{"id":"666f6f","v":1}"#,
            "id",
            None,
            crate::EventTimeUnit::default(),
        )
        .unwrap();
        assert_eq!(recs[0].qid, NexoraId::from_bytes(b"foo".to_vec()));
    }

    #[test]
    fn parse_missing_id_errors() {
        assert!(parse_data(br#"{"v":1}"#, "id", None, crate::EventTimeUnit::default()).is_err());
    }

    #[test]
    fn parse_bad_json_errors() {
        assert!(parse_data(b"not json", "id", None, crate::EventTimeUnit::default()).is_err());
    }
}
