//! graph→fragment sealing pipeline.
//!
//! P2-B (DISTRIBUTED_EVOLUTION §5): turns the live graph's mutation stream into
//! immutable, time-windowed fragments in the [`TieredFragmentStore`], so
//! historical data ages into shared (warm/cold) storage and stays queryable via
//! time-travel — without this, the tiered-storage machinery has nothing to hold.
//!
//! Flow:
//! 1. The graph fires a mutation callback on every write (property/edge set).
//!    [`FragmentSealer::record`] accumulates it into the current time window,
//!    grouped by node id: within a window we fold repeated writes to the same
//!    node into one record (last-writer-wins per property; edges accumulate).
//! 2. On window close ([`FragmentSealer::seal`], driven by a timer or a size
//!    threshold), the accumulated nodes are serialized as `nodes.jsonl` (one
//!    JSON record per node, the shape [`crate::time_travel`] replays) and sealed
//!    into the tiered store as a fragment covering `[window_start, now)`.
//! 3. The tiered store's lifecycle then ages the sealed fragment hot→warm→cold.
//!
//! The sealer is deliberately decoupled from the hot read/write path: it only
//! observes mutations, so live latency is unaffected (the callback does a cheap
//! in-memory append; the expensive seal happens off the write path).

use crate::metadata::FragmentMetadata;
use crate::tiered_store::TieredFragmentStore;
use crate::FragmentId;
use bytes::Bytes;
use nexora_id::NexoraId;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// One node's accumulated state within the current window.
#[derive(Default, Clone)]
struct NodeAccum {
    /// Property key → latest value set in this window (last-writer-wins).
    properties: BTreeMap<String, Value>,
    /// Edges added in this window: (edge_type, direction, target_hex).
    edges: Vec<(String, String, String)>,
    /// Latest event timestamp (micros) seen for this node in the window.
    last_ts_us: u64,
}

/// Accumulates graph mutations into time-windowed fragments and seals them into
/// a [`TieredFragmentStore`].
pub struct FragmentSealer {
    store: Arc<TieredFragmentStore>,
    namespace: String,
    inner: Mutex<Window>,
}

struct Window {
    /// Window start (micros since epoch).
    start_us: u64,
    /// Per-node accumulated state, keyed by node id hex.
    nodes: BTreeMap<String, NodeAccum>,
    /// Total events folded into this window (for the size-based seal trigger).
    event_count: usize,
}

impl Window {
    fn new(start_us: u64) -> Self {
        Self {
            start_us,
            nodes: BTreeMap::new(),
            event_count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// A graph mutation observed by the sealer — the minimal projection of a
/// `NodeChangeEvent` the sealer needs. The caller (app wiring) translates the
/// core event into this so `nexora-fragment` needn't depend on the event enum.
#[derive(Clone, Debug)]
pub enum SealEvent {
    /// A property was set on a node.
    PropertySet {
        qid: NexoraId,
        key: String,
        value: Value,
        ts_us: u64,
    },
    /// An edge was added from a node.
    EdgeAdded {
        source: NexoraId,
        edge_type: String,
        direction: String,
        target: NexoraId,
        ts_us: u64,
    },
}

impl FragmentSealer {
    /// Create a sealer writing into `store` under `namespace`, opening its first
    /// window at `start_us`.
    pub fn new(store: Arc<TieredFragmentStore>, namespace: &str, start_us: u64) -> Self {
        Self {
            store,
            namespace: namespace.to_string(),
            inner: Mutex::new(Window::new(start_us)),
        }
    }

    /// Record a mutation into the current window. Cheap in-memory fold — safe to
    /// call from the graph's mutation callback on the hot path.
    pub async fn record(&self, event: SealEvent) {
        let mut w = self.inner.lock().await;
        w.event_count += 1;
        match event {
            SealEvent::PropertySet {
                qid,
                key,
                value,
                ts_us,
            } => {
                let acc = w.nodes.entry(qid.to_hex()).or_default();
                acc.properties.insert(key, value);
                acc.last_ts_us = acc.last_ts_us.max(ts_us);
            }
            SealEvent::EdgeAdded {
                source,
                edge_type,
                direction,
                target,
                ts_us,
            } => {
                let acc = w.nodes.entry(source.to_hex()).or_default();
                acc.edges.push((edge_type, direction, target.to_hex()));
                acc.last_ts_us = acc.last_ts_us.max(ts_us);
            }
        }
    }

    /// Number of events folded into the current (unsealed) window.
    pub async fn pending_events(&self) -> usize {
        self.inner.lock().await.event_count
    }

    /// Seal the current window into a fragment covering `[start_us, end_us)` and
    /// open a fresh window at `end_us`. No-op (returns `None`) if the window is
    /// empty. Returns the sealed fragment's id on success.
    pub async fn seal(
        &self,
        end_us: u64,
    ) -> Result<Option<FragmentId>, crate::store::FragmentError> {
        // Swap out the current window under the lock, then do IO unlocked.
        let (start_us, nodes) = {
            let mut w = self.inner.lock().await;
            if w.is_empty() {
                // Still advance the window start so the next fragment's range is
                // contiguous, but seal nothing.
                w.start_us = end_us;
                return Ok(None);
            }
            let start = w.start_us;
            let taken = std::mem::replace(&mut *w, Window::new(end_us));
            (start, taken.nodes)
        };

        // Serialize accumulated nodes as newline-delimited JSON records — the
        // exact shape `time_travel::execute_time_travel` replays.
        let mut body = String::new();
        let mut node_count = 0u64;
        let mut edge_count = 0u64;
        for (id_hex, acc) in &nodes {
            let mut rec = Map::new();
            rec.insert("id".into(), Value::String(id_hex.clone()));
            // Use the node's latest event ts, clamped into the window.
            let ts = if acc.last_ts_us == 0 {
                end_us
            } else {
                acc.last_ts_us
            };
            rec.insert("timestamp".into(), Value::from(ts));
            if !acc.properties.is_empty() {
                rec.insert(
                    "properties".into(),
                    Value::Object(acc.properties.clone().into_iter().collect()),
                );
            }
            if !acc.edges.is_empty() {
                let edges: Vec<Value> = acc
                    .edges
                    .iter()
                    .map(|(et, dir, tgt)| {
                        let mut e = Map::new();
                        e.insert("edge_type".into(), Value::String(et.clone()));
                        e.insert("direction".into(), Value::String(dir.clone()));
                        e.insert("other".into(), Value::String(tgt.clone()));
                        Value::Object(e)
                    })
                    .collect();
                edge_count += edges.len() as u64;
                rec.insert("edges".into(), Value::Array(edges));
            }
            body.push_str(&Value::Object(rec).to_string());
            body.push('\n');
            node_count += 1;
        }

        let id = FragmentId {
            start_us,
            end_us,
            uuid: uuid::Uuid::new_v4(),
        };
        let mut meta = FragmentMetadata::new(id.clone(), self.namespace.clone());
        meta.node_count = node_count;
        meta.edge_count = edge_count;
        meta.time_range = (start_us, end_us);

        self.store.put_fragment(meta, Bytes::from(body)).await?;
        tracing::info!(
            fragment = %id,
            nodes = node_count,
            edges = edge_count,
            "sealed fragment into tiered store"
        );
        Ok(Some(id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FragmentStore;
    use crate::tiered_store::TieredFragmentStore;
    use nexora_storage::{MemoryStorage, StorageTier, TieredStore};

    fn sealer() -> (Arc<TieredFragmentStore>, FragmentSealer) {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(FragmentStore::new(dir.path(), "hist"));
        let tiered = Arc::new(TieredStore::new(
            Arc::new(MemoryStorage::new(StorageTier::Hot)),
            Arc::new(MemoryStorage::new(StorageTier::Warm)),
            Arc::new(MemoryStorage::new(StorageTier::Cold)),
        ));
        let store = Arc::new(TieredFragmentStore::new(registry, tiered, "hist"));
        let s = FragmentSealer::new(store.clone(), "hist", 1000);
        (store, s)
    }

    #[tokio::test]
    async fn seal_empty_window_is_noop() {
        let (_store, s) = sealer();
        assert!(s.seal(2000).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn seal_produces_queryable_fragment() {
        use crate::time_travel::{execute_time_travel, TimeTravelQuery};

        let (store, s) = sealer();
        let n1 = NexoraId::from_bytes(b"seal-n1".to_vec());
        s.record(SealEvent::PropertySet {
            qid: n1.clone(),
            key: "name".into(),
            value: serde_json::json!("Alice"),
            ts_us: 1500,
        })
        .await;
        s.record(SealEvent::PropertySet {
            qid: n1.clone(),
            key: "speed".into(),
            value: serde_json::json!(50),
            ts_us: 1600,
        })
        .await;
        assert_eq!(s.pending_events().await, 2);

        let fid = s.seal(2000).await.unwrap().expect("must seal a fragment");
        assert_eq!(store.tier_of(&fid).await, Some(StorageTier::Hot));

        // Pull the sealed body back and replay via time-travel.
        let body = store.get_fragment(&fid).await.unwrap();
        let dir = store.registry().create_fragment_dir(&fid).unwrap();
        std::fs::write(dir.join("nodes.jsonl"), &body).unwrap();

        let result = execute_time_travel(store.registry(), TimeTravelQuery::at(2000))
            .await
            .unwrap();
        let node = result
            .find_node(&n1)
            .expect("sealed node must be queryable");
        assert_eq!(
            node.properties.get("name"),
            Some(&nexora_id::PropertyValue::String("Alice".into()))
        );
        assert_eq!(
            node.properties.get("speed"),
            Some(&nexora_id::PropertyValue::Integer(50))
        );
    }

    #[tokio::test]
    async fn window_advances_after_seal() {
        let (_store, s) = sealer();
        let n = NexoraId::from_bytes(b"w".to_vec());
        s.record(SealEvent::PropertySet {
            qid: n.clone(),
            key: "a".into(),
            value: serde_json::json!(1),
            ts_us: 1100,
        })
        .await;
        let f1 = s.seal(2000).await.unwrap().unwrap();
        // New window starts at 2000: next fragment's range begins there.
        s.record(SealEvent::PropertySet {
            qid: n,
            key: "b".into(),
            value: serde_json::json!(2),
            ts_us: 2500,
        })
        .await;
        let f2 = s.seal(3000).await.unwrap().unwrap();
        assert_eq!(f1.start_us, 1000);
        assert_eq!(f1.end_us, 2000);
        assert_eq!(f2.start_us, 2000);
        assert_eq!(f2.end_us, 3000);
    }

    #[tokio::test]
    async fn edges_are_sealed_and_replayed() {
        use crate::time_travel::{execute_time_travel, TimeTravelQuery};

        let (store, s) = sealer();
        let a = NexoraId::from_bytes(b"edge-a".to_vec());
        let b = NexoraId::from_bytes(b"edge-b".to_vec());
        s.record(SealEvent::EdgeAdded {
            source: a.clone(),
            edge_type: "LINK".into(),
            direction: "out".into(),
            target: b.clone(),
            ts_us: 1500,
        })
        .await;
        let fid = s.seal(2000).await.unwrap().unwrap();
        let body = store.get_fragment(&fid).await.unwrap();
        let dir = store.registry().create_fragment_dir(&fid).unwrap();
        std::fs::write(dir.join("nodes.jsonl"), &body).unwrap();

        let result = execute_time_travel(store.registry(), TimeTravelQuery::at(2000))
            .await
            .unwrap();
        let node = result.find_node(&a).unwrap();
        assert_eq!(node.edges.len(), 1);
        assert_eq!(node.edges[0].edge_type, "LINK");
    }
}
