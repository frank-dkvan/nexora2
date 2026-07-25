//! Shared graph-write sink for all ingestion sources.
//!
//! [`GraphIngestHandler`] implements [`crate::IngestHandler`], turning an
//! [`crate::IngestBatch`] into a single concurrent [`GraphService::write_batch`]
//! commit. Every ingestion source (Kafka, file, and future Kinesis/Pulsar/MQTT/
//! NATS/…) funnels through this one path, so:
//!   - one WAL group-commit fsync amortizes each poll batch (vs one fsync per
//!     record on the old serial `for record { set_property().await }` loops), and
//!   - per-source durability is a single knob ([`BatchDurability`]).
//!
//! Records are coalesced by node id: multiple records for the same node in one
//! batch become one atomic per-node commit.

use crate::{IngestBatch, IngestHandler};
use nexora_core::{BatchDurability, GraphService, MutationOp, WriteBatchOptions};
use nexora_id::{EventTime, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::collections::HashMap;
use std::sync::Arc;

/// An [`IngestHandler`] that commits batches into a [`GraphService`] via
/// `write_batch`.
pub struct GraphIngestHandler {
    graph: Arc<GraphService>,
    durability: BatchDurability,
    concurrency: usize,
}

impl GraphIngestHandler {
    /// Create a handler writing into `graph`.
    ///
    /// `durability` picks the ack contract for every batch this handler commits
    /// (see [`BatchDurability`]): `WaitDurable` for sources that cannot replay,
    /// `Relaxed` for replayable stream/file sources that tolerate bounded loss.
    pub fn new(graph: Arc<GraphService>, durability: BatchDurability) -> Self {
        Self {
            graph,
            durability,
            concurrency: 64,
        }
    }

    /// Override the per-batch commit concurrency (default 64).
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Convert a batch into per-node items, coalescing records that target the
    /// same node. A record with `edge_type` + `edge_target` becomes an
    /// `AddEdge`; otherwise it sets `key`/`value` as a property.
    ///
    /// Each op carries its own record's event time (converted to [`EventTime`]),
    /// so records for the same node in one batch resolve per-op under event-time
    /// last-writer-wins — a late record cannot overwrite a newer one even within
    /// the same batch. An op from a record with no timestamp gets `None`,
    /// falling back to the node's internal clock (arrival order). The per-node
    /// slot (third tuple element) is always `None`; the per-op times carry the
    /// event time.
    #[allow(clippy::type_complexity)]
    fn batch_to_items(
        batch: &IngestBatch,
    ) -> Vec<(
        nexora_id::NexoraId,
        Vec<(MutationOp, Option<EventTime>)>,
        Option<EventTime>,
    )> {
        // Preserve first-seen node order for deterministic, testable output.
        let mut order: Vec<nexora_id::NexoraId> = Vec::new();
        let mut by_node: HashMap<nexora_id::NexoraId, Vec<(MutationOp, Option<EventTime>)>> =
            HashMap::new();

        for record in &batch.records {
            let event_time = record.timestamp.as_ref().map(EventTime::from_datetime);
            let entry = by_node.entry(record.qid.clone()).or_insert_with(|| {
                order.push(record.qid.clone());
                Vec::new()
            });
            let op = match (&record.label, &record.edge_type, &record.edge_target) {
                // A label record (from a source's `label_field`) becomes an
                // AddLabel op, so the value registers in the graph's label index
                // rather than as an ordinary property.
                (Some(label), _, _) if !label.is_empty() => MutationOp::AddLabel {
                    label: Symbol::new(label),
                },
                (_, Some(edge_type), Some(target)) => MutationOp::AddEdge {
                    edge: HalfEdge::out(Symbol::new(edge_type), target.clone()),
                },
                _ => MutationOp::SetProperty {
                    key: Symbol::new(&record.key),
                    value: json_to_property_value(&record.value),
                },
            };
            entry.push((op, event_time));
        }

        order
            .into_iter()
            .map(|qid| {
                let ops = by_node.remove(&qid).unwrap_or_default();
                (qid, ops, None)
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl IngestHandler for GraphIngestHandler {
    async fn handle_batch(&self, batch: &IngestBatch) -> Result<usize, String> {
        let items = Self::batch_to_items(batch);
        if items.is_empty() {
            return Ok(0);
        }
        let record_count = batch.records.len();
        self.graph
            .write_batch_with_event_times(
                items,
                WriteBatchOptions {
                    concurrency: self.concurrency,
                    durability: self.durability,
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(record_count)
    }
}

/// Convert a JSON value from an ingest record into a graph [`PropertyValue`].
pub fn json_to_property_value(v: &serde_json::Value) -> PropertyValue {
    match v {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            PropertyValue::List(arr.iter().map(json_to_property_value).collect())
        }
        serde_json::Value::Object(map) => PropertyValue::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_property_value(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IngestBatch, IngestRecord};
    use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
    use nexora_id::NexoraId;

    fn prop_record(qid: &NexoraId, key: &str, value: serde_json::Value) -> IngestRecord {
        IngestRecord {
            qid: qid.clone(),
            key: key.into(),
            value,
            edge_type: None,
            edge_target: None,
            timestamp: None,
            label: None,
        }
    }

    fn batch(records: Vec<IngestRecord>) -> IngestBatch {
        let n = records.len() as u64;
        IngestBatch {
            records,
            partition: "0".into(),
            offset_start: 0,
            offset_end: n,
            topic: "t".into(),
            raw_events: None,
        }
    }

    fn svc() -> Arc<GraphService> {
        Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 10_000,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ))
    }

    /// Records for the same node coalesce into one item with all ops.
    #[test]
    fn batch_to_items_coalesces_by_node() {
        let a = NexoraId::from_bytes(b"a".to_vec());
        let b = NexoraId::from_bytes(b"b".to_vec());
        let batch = batch(vec![
            prop_record(&a, "x", serde_json::json!(1)),
            prop_record(&b, "y", serde_json::json!(2)),
            prop_record(&a, "z", serde_json::json!(3)),
        ]);

        let items = GraphIngestHandler::batch_to_items(&batch);
        assert_eq!(items.len(), 2, "two distinct nodes → two items");
        // First-seen order preserved: a before b.
        assert_eq!(items[0].0, a);
        assert_eq!(items[0].1.len(), 2, "node a has two ops coalesced");
        assert_eq!(items[1].0, b);
        assert_eq!(items[1].1.len(), 1);
    }

    /// Each op carries its own record's event time, so records for one node in a
    /// single batch keep distinct per-op times (the #2 fix: no node-wide max).
    #[test]
    fn batch_to_items_carries_per_op_event_time() {
        use nexora_id::EventTime;
        let a = NexoraId::from_bytes(b"a".to_vec());
        let t_new = chrono::DateTime::from_timestamp(1_700_000_010, 0).unwrap();
        let t_old = chrono::DateTime::from_timestamp(1_700_000_005, 0).unwrap();
        let mut r_new = prop_record(&a, "x", serde_json::json!(1));
        r_new.timestamp = Some(t_new);
        let mut r_old = prop_record(&a, "x", serde_json::json!(2));
        r_old.timestamp = Some(t_old);

        let items = GraphIngestHandler::batch_to_items(&batch(vec![r_new, r_old]));
        assert_eq!(items.len(), 1);
        let ops = &items[0].1;
        assert_eq!(
            ops.len(),
            2,
            "two writes to the same key are not merged here"
        );
        // Each op keeps its own record's event time, not a node-wide max.
        assert_eq!(ops[0].1, Some(EventTime::from_datetime(&t_new)));
        assert_eq!(ops[1].1, Some(EventTime::from_datetime(&t_old)));
    }

    /// An edge record becomes an AddEdge op.
    #[test]
    fn batch_to_items_edge_record() {
        let a = NexoraId::from_bytes(b"src".to_vec());
        let b = NexoraId::from_bytes(b"dst".to_vec());
        let rec = IngestRecord {
            qid: a.clone(),
            key: String::new(),
            value: serde_json::Value::Null,
            edge_type: Some("KNOWS".into()),
            edge_target: Some(b.clone()),
            timestamp: None,
            label: None,
        };
        let items = GraphIngestHandler::batch_to_items(&batch(vec![rec]));
        assert_eq!(items.len(), 1);
        assert!(matches!(items[0].1[0].0, MutationOp::AddEdge { .. }));
    }

    #[test]
    fn batch_to_items_label_record() {
        let a = NexoraId::from_bytes(b"n1".to_vec());
        let rec = IngestRecord {
            qid: a.clone(),
            key: String::new(),
            value: serde_json::Value::Null,
            edge_type: None,
            edge_target: None,
            timestamp: None,
            label: Some("Forklift".into()),
        };
        let items = GraphIngestHandler::batch_to_items(&batch(vec![rec]));
        assert_eq!(items.len(), 1);
        assert!(
            matches!(&items[0].1[0].0, MutationOp::AddLabel { label } if label.as_str() == "Forklift"),
            "a label record must become an AddLabel op"
        );
    }

    /// End-to-end: handle_batch writes properties reachable via the graph.
    #[tokio::test]
    async fn handle_batch_writes_to_graph() {
        let graph = svc();
        let handler = GraphIngestHandler::new(graph.clone(), BatchDurability::WaitDurable);
        let a = NexoraId::from_bytes(b"node-a".to_vec());

        let n = handler
            .handle_batch(&batch(vec![
                prop_record(&a, "name", serde_json::json!("alice")),
                prop_record(&a, "age", serde_json::json!(30)),
            ]))
            .await
            .unwrap();
        assert_eq!(n, 2, "returns record count");

        assert_eq!(
            graph.get_property(&a, "name").await.unwrap(),
            Some(PropertyValue::String("alice".into()))
        );
        assert_eq!(
            graph.get_property(&a, "age").await.unwrap(),
            Some(PropertyValue::Integer(30))
        );
    }

    /// Empty batch is a no-op returning 0.
    #[tokio::test]
    async fn handle_batch_empty() {
        let handler = GraphIngestHandler::new(svc(), BatchDurability::Relaxed);
        assert_eq!(handler.handle_batch(&batch(vec![])).await.unwrap(), 0);
    }

    fn edge_record(src: &NexoraId, edge_type: &str, dst: &NexoraId) -> IngestRecord {
        IngestRecord {
            qid: src.clone(),
            key: String::new(),
            value: serde_json::Value::Null,
            edge_type: Some(edge_type.into()),
            edge_target: Some(dst.clone()),
            timestamp: None,
            label: None,
        }
    }

    fn label_record(qid: &NexoraId, label: &str) -> IngestRecord {
        IngestRecord {
            qid: qid.clone(),
            key: String::new(),
            value: serde_json::Value::Null,
            edge_type: None,
            edge_target: None,
            timestamp: None,
            label: Some(label.into()),
        }
    }

    /// B1 replay idempotency: applying the SAME batch twice — as happens on
    /// at-least-once replay after a crash before the source offset is committed —
    /// must converge to identical graph state, not duplicate or diverge.
    ///
    /// This is the load-bearing property of the upstream-replay recovery path:
    /// the ingestion loop commits the offset only AFTER a successful write, so a
    /// crash in that window re-delivers the batch on restart. Convergence holds
    /// because every op type is idempotent by node id:
    /// - SetProperty overwrites (last-write-wins to the same value),
    /// - AddLabel is set-membership,
    /// - AddEdge dedups in the node's `HashSet<HalfEdge>`.
    #[tokio::test]
    async fn replay_same_batch_is_idempotent() {
        let graph = svc();
        let handler = GraphIngestHandler::new(graph.clone(), BatchDurability::WaitDurable);

        let a = NexoraId::from_bytes(b"device-a".to_vec());
        let b = NexoraId::from_bytes(b"device-b".to_vec());

        let make_batch = || {
            batch(vec![
                prop_record(&a, "name", serde_json::json!("alice")),
                prop_record(&a, "speed", serde_json::json!(42)),
                label_record(&a, "Forklift"),
                edge_record(&a, "NEAR", &b),
            ])
        };

        // First delivery.
        handler.handle_batch(&make_batch()).await.unwrap();
        // Replay (crash before offset commit → same batch re-delivered).
        handler.handle_batch(&make_batch()).await.unwrap();
        // A third time for good measure — still must not diverge.
        handler.handle_batch(&make_batch()).await.unwrap();

        // Properties: single value each, not appended/duplicated.
        assert_eq!(
            graph.get_property(&a, "name").await.unwrap(),
            Some(PropertyValue::String("alice".into()))
        );
        assert_eq!(
            graph.get_property(&a, "speed").await.unwrap(),
            Some(PropertyValue::Integer(42))
        );

        // Edge: exactly one NEAR edge a→b despite three deliveries (HashSet dedup).
        let edges = graph.get_edges(&a).await.unwrap();
        let near_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.edge_type.as_str() == "NEAR")
            .collect();
        assert_eq!(
            near_edges.len(),
            1,
            "replay must not duplicate the a→b NEAR edge; got {near_edges:?}"
        );
    }

    /// B1: a partial replay (offset never advanced past a batch) that overlaps a
    /// newer write must not resurrect stale values or lose the newer write.
    /// Models: batch-1 applied + committed, batch-2 applied but crash before
    /// commit, restart replays batch-2 — the final state must reflect batch-2.
    #[tokio::test]
    async fn replay_after_newer_write_keeps_latest() {
        let graph = svc();
        let handler = GraphIngestHandler::new(graph.clone(), BatchDurability::WaitDurable);
        let a = NexoraId::from_bytes(b"sensor-1".to_vec());

        // Batch 1: initial reading (committed).
        handler
            .handle_batch(&batch(vec![prop_record(&a, "temp", serde_json::json!(20))]))
            .await
            .unwrap();
        // Batch 2: newer reading (applied, crash before commit).
        handler
            .handle_batch(&batch(vec![prop_record(&a, "temp", serde_json::json!(25))]))
            .await
            .unwrap();
        // Restart replays batch 2 (the uncommitted one) — same value, converges.
        handler
            .handle_batch(&batch(vec![prop_record(&a, "temp", serde_json::json!(25))]))
            .await
            .unwrap();

        assert_eq!(
            graph.get_property(&a, "temp").await.unwrap(),
            Some(PropertyValue::Integer(25)),
            "replay of the newer batch must keep the latest value"
        );
    }

    #[tokio::test]
    async fn handle_batch_fires_sq_callback_end_to_end() {
        // End-to-end: the ingest path a stream source drives — GraphIngestHandler
        // → write_batch → apply_batch_side_effects — must fire the graph's
        // sq_callback for batched property writes, so Standing Queries match on
        // stream ingest exactly as on single writes. This is the pipeline→SQ link
        // (the pipeline calls handle_batch); before the fix it was silently broken.
        use std::sync::{Arc as StdArc, Mutex};

        let seen: StdArc<Mutex<Vec<(String, PropertyValue)>>> = StdArc::new(Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let cb: nexora_core::graph::PropertyChangeCallback =
            Arc::new(move |_qid, key, value, _all| {
                let seen = seen_cb.clone();
                Box::pin(async move {
                    seen.lock().unwrap().push((key, value));
                })
            });

        let graph = Arc::new(
            GraphService::new(
                GraphServiceConfig {
                    num_shards: 4,
                    max_nodes_per_shard: 10_000,
                    node_channel_size: 64,
                },
                Arc::new(InMemoryPersistor::new()),
            )
            .with_sq_callback(cb),
        );
        let handler = GraphIngestHandler::new(graph, BatchDurability::WaitDurable);

        let a = NexoraId::from_bytes(b"sensor-1".to_vec());
        handler
            .handle_batch(&batch(vec![
                prop_record(&a, "speed", serde_json::json!(120)),
                prop_record(&a, "status", serde_json::json!("active")),
            ]))
            .await
            .unwrap();

        let calls = seen.lock().unwrap().clone();
        assert_eq!(
            calls.len(),
            2,
            "stream ingest must fire sq_callback once per property write, got {}",
            calls.len()
        );
        let keys: Vec<&str> = calls.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"speed") && keys.contains(&"status"));
    }
}
