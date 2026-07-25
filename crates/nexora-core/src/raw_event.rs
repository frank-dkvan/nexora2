//! Raw event: the source-of-truth record for the event-first ingestion model.
//!
//! In the event-first architecture an upstream source's original payload is
//! captured *verbatim* as a [`RawEvent`] and appended to a topic-partitioned,
//! append-only event log **before** it is projected into the graph. The graph
//! snapshot becomes a derived projection that can be rebuilt by replaying the
//! event log through a projector — so a change to the projection rules can be
//! re-applied to history instead of being lost.
//!
//! This type lives in `nexora-core` (not `nexora-eventlog`) so the WAL — the
//! durability layer that must persist a raw event before it is sealed into a
//! columnar fragment — can reference it without a dependency cycle
//! (`nexora-eventlog` depends on `nexora-core`, not the reverse). `nexora-eventlog`
//! re-exports it as its public surface.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A raw event captured from an upstream source, retained verbatim as the
/// source of truth. One `RawEvent` becomes one row in the topic's event table.
///
/// The `payload` is the original decoded message (JSON), never decomposed into
/// per-field graph mutations at capture time — that decomposition is deferred
/// to the projector, so it can be re-run under new rules during a replay.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawEvent {
    /// Stable unique id for this event (dedup + row identity in the event table).
    pub event_id: Uuid,
    /// Event time in microseconds since the Unix epoch (the ingestion pipeline's
    /// `extract_event_time`: payload field → transport timestamp → arrival).
    pub event_time_us: u64,
    /// Wall-clock time (micros since epoch) at which this event was ingested.
    pub ingest_time_us: u64,
    /// Logical source name that produced the event (e.g. `"kafka"`, `"zenoh"`).
    pub source: String,
    /// Business topic this event was routed to (the event-table name).
    pub topic: String,
    /// Transport partition, when the source has one (Kafka partition, Kinesis
    /// shard). `None` for push sources without a partition concept.
    pub partition: Option<i32>,
    /// Transport offset, when the source is replayable (Kafka/Kinesis). `None`
    /// for push sources (Zenoh/MQTT/WebSocket) that carry only a synthetic seq.
    pub offset: Option<i64>,
    /// Optional transport subject/key (e.g. Zenoh key expression, MQTT topic).
    /// Retained for provenance and as a dynamic-routing input.
    pub subject: Option<String>,
    /// The original decoded payload, retained verbatim.
    pub payload: Value,
}

impl RawEvent {
    /// Build a raw event with a freshly generated `event_id`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_time_us: u64,
        ingest_time_us: u64,
        source: impl Into<String>,
        topic: impl Into<String>,
        partition: Option<i32>,
        offset: Option<i64>,
        subject: Option<String>,
        payload: Value,
    ) -> Self {
        Self {
            event_id: Uuid::new_v4(),
            event_time_us,
            ingest_time_us,
            source: source.into(),
            topic: topic.into(),
            partition,
            offset,
            subject,
            payload,
        }
    }

    /// Render this event as a columnar-fragment row: the `{id, timestamp,
    /// properties}` shape [`ColumnarFragment::from_rows`](../../nexora_fragment/columnar/struct.ColumnarFragment.html#method.from_rows)
    /// expects. The event's provenance (event_id, source, topic, offset,
    /// subject, ingest_time) and every top-level payload field are flattened
    /// into `properties`, so each becomes a queryable column of the event table.
    ///
    /// `id` = the event id, `timestamp` = the event time (micros), so time-range
    /// selection and the fragment's per-column stats pushdown both work directly.
    pub fn to_fragment_row(&self) -> Value {
        let mut props = serde_json::Map::new();
        // Provenance columns (prefixed to avoid colliding with payload fields).
        props.insert("_event_id".into(), Value::String(self.event_id.to_string()));
        props.insert("_event_time_us".into(), Value::from(self.event_time_us));
        props.insert("_ingest_time_us".into(), Value::from(self.ingest_time_us));
        props.insert("_source".into(), Value::String(self.source.clone()));
        props.insert("_topic".into(), Value::String(self.topic.clone()));
        if let Some(p) = self.partition {
            props.insert("_partition".into(), Value::from(p));
        }
        if let Some(o) = self.offset {
            props.insert("_offset".into(), Value::from(o));
        }
        if let Some(s) = &self.subject {
            props.insert("_subject".into(), Value::String(s.clone()));
        }
        // Flatten top-level payload fields into columns. A non-object payload is
        // retained under a single `_payload` column so nothing is lost.
        match &self.payload {
            Value::Object(map) => {
                for (k, v) in map {
                    // Payload keys never shadow provenance columns (which are
                    // underscore-prefixed); a payload key starting with `_` is
                    // kept as-is under the assumption sources don't use that.
                    props.insert(k.clone(), v.clone());
                }
            }
            other => {
                props.insert("_payload".into(), other.clone());
            }
        }

        let mut row = serde_json::Map::new();
        row.insert("id".into(), Value::String(self.event_id.to_string()));
        row.insert("timestamp".into(), Value::from(self.event_time_us));
        row.insert("properties".into(), Value::Object(props));
        Value::Object(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> RawEvent {
        RawEvent::new(
            1_500,
            2_000,
            "kafka",
            "orders",
            Some(3),
            Some(42),
            Some("orders/eu".into()),
            json!({"order_id": "A1", "amount": 99}),
        )
    }

    #[test]
    fn serde_round_trips() {
        let ev = sample();
        let bytes = serde_json::to_vec(&ev).unwrap();
        let back: RawEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn fragment_row_flattens_payload_and_provenance() {
        let ev = sample();
        let row = ev.to_fragment_row();
        assert_eq!(row["id"], json!(ev.event_id.to_string()));
        assert_eq!(row["timestamp"], json!(1_500));
        let props = &row["properties"];
        // Provenance columns.
        assert_eq!(props["_source"], json!("kafka"));
        assert_eq!(props["_topic"], json!("orders"));
        assert_eq!(props["_partition"], json!(3));
        assert_eq!(props["_offset"], json!(42));
        assert_eq!(props["_subject"], json!("orders/eu"));
        // Payload fields become top-level columns.
        assert_eq!(props["order_id"], json!("A1"));
        assert_eq!(props["amount"], json!(99));
    }

    #[test]
    fn non_object_payload_kept_under_payload_column() {
        let ev = RawEvent::new(1, 1, "file", "raw", None, None, None, json!("hello"));
        let row = ev.to_fragment_row();
        assert_eq!(row["properties"]["_payload"], json!("hello"));
        // Optional columns absent when their source field is None.
        assert_eq!(row["properties"].get("_partition"), None);
        assert_eq!(row["properties"].get("_offset"), None);
        assert_eq!(row["properties"].get("_subject"), None);
    }
}
