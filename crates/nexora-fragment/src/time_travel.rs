//! Time-travel query support — query the graph state "as of" a specific timestamp.
//!
//! This module provides the `TimeTravelQuery` interface that lets users
//! reconstruct the graph state at any historical point by replaying
//! fragments in time order.

use crate::fragment_id::FragmentId;
use crate::metadata::{FragmentMetadata, PropertyCondition};
use crate::store::FragmentStore;
use nexora_id::{NexoraId, PropertyValue};
use std::collections::HashMap;

/// Convert a serde_json::Value to a PropertyValue.
fn json_to_property_value(v: &serde_json::Value) -> Option<PropertyValue> {
    match v {
        serde_json::Value::Null => Some(PropertyValue::Null),
        serde_json::Value::Bool(b) => Some(PropertyValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(PropertyValue::Integer(i))
            } else {
                Some(PropertyValue::Float(n.as_f64()?))
            }
        }
        serde_json::Value::String(s) => Some(PropertyValue::String(s.clone())),
        serde_json::Value::Array(arr) => {
            let items: Vec<PropertyValue> = arr.iter().filter_map(json_to_property_value).collect();
            Some(PropertyValue::List(items))
        }
        serde_json::Value::Object(obj) => {
            let mut map = std::collections::BTreeMap::new();
            for (k, v) in obj {
                if let Some(pv) = json_to_property_value(v) {
                    map.insert(k.clone(), pv);
                }
            }
            Some(PropertyValue::Map(map))
        }
    }
}

/// A point-in-time query for the graph state.
#[derive(Clone, Debug)]
pub struct TimeTravelQuery {
    /// The "as of" timestamp (microseconds since epoch).
    pub as_of_us: u64,
    /// Optional namespace filter.
    pub namespace: Option<String>,
    /// Optional node ID filter (return only this node's state).
    pub node_id: Option<NexoraId>,
    /// Optional property filter.
    pub property_filter: Option<(String, PropertyCondition)>,
}

impl TimeTravelQuery {
    /// Create a new time-travel query for a specific timestamp.
    pub fn at(as_of_us: u64) -> Self {
        Self {
            as_of_us,
            namespace: None,
            node_id: None,
            property_filter: None,
        }
    }

    /// Filter to a specific node.
    pub fn for_node(mut self, qid: NexoraId) -> Self {
        self.node_id = Some(qid);
        self
    }

    /// Filter to a namespace.
    pub fn in_namespace(mut self, ns: &str) -> Self {
        self.namespace = Some(ns.to_string());
        self
    }

    /// Filter by a property condition.
    pub fn with_property(mut self, key: &str, cond: PropertyCondition) -> Self {
        self.property_filter = Some((key.to_string(), cond));
        self
    }
}

/// A reconstructed node state at a point in time.
#[derive(Clone, Debug)]
pub struct NodeSnapshot {
    pub id: NexoraId,
    pub timestamp: u64,
    pub properties: HashMap<String, PropertyValue>,
    pub edges: Vec<EdgeSnapshot>,
}

/// A reconstructed edge state at a point in time.
#[derive(Clone, Debug)]
pub struct EdgeSnapshot {
    pub edge_type: String,
    pub direction: String,
    pub other: NexoraId,
    pub timestamp: u64,
}

/// Result of a time-travel query.
#[derive(Clone, Debug)]
pub struct TimeTravelResult {
    pub as_of: u64,
    pub fragments_scanned: usize,
    pub nodes: Vec<NodeSnapshot>,
}

impl TimeTravelResult {
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn find_node(&self, id: &NexoraId) -> Option<&NodeSnapshot> {
        self.nodes.iter().find(|n| &n.id == id)
    }
}

/// Execute a time-travel query against the fragment store.
pub async fn execute_time_travel(
    store: &FragmentStore,
    query: TimeTravelQuery,
) -> Result<TimeTravelResult, crate::store::FragmentError> {
    // Find all fragments that are wholly before or overlapping the as_of timestamp
    let fragments = store.query_range(0, query.as_of_us).await;

    // Filter by namespace if specified
    let fragments: Vec<FragmentMetadata> = fragments
        .into_iter()
        .filter(|f| {
            if let Some(ref ns) = query.namespace {
                &f.namespace == ns
            } else {
                true
            }
        })
        .filter(|f| {
            // Apply property filter on fragment metadata (min/max pruning)
            if let Some(ref prop_filter) = query.property_filter {
                let (ref key, ref cond) = *prop_filter;
                f.might_match_property(key, cond)
            } else {
                true
            }
        })
        .collect();

    let scanned = fragments.len();

    // Reconstruct node states by replaying fragments in chronological order
    let mut node_states: HashMap<Vec<u8>, NodeSnapshot> = HashMap::new();

    for frag in &fragments {
        let frag_dir = store.fragment_dir(&frag.id);

        // Try to load node data from the fragment directory
        let nodes_file = frag_dir.join("nodes.jsonl");
        if nodes_file.exists() {
            if let Ok(content) = std::fs::read_to_string(&nodes_file) {
                for line in content.lines() {
                    if let Ok(entry) = serde_json::from_str::<serde_json::Value>(line) {
                        let id_str = entry.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        // Fragments sealed from the live graph write the node id
                        // as hex (`qid.to_hex()`); older hand-written test
                        // fragments use an opaque string. Try hex first, fall
                        // back to raw bytes so both round-trip to the same id the
                        // caller would build from the same string.
                        let qid = NexoraId::from_hex(id_str)
                            .unwrap_or_else(|_| NexoraId::from_bytes(id_str.as_bytes().to_vec()));

                        // Filter by node ID if specified
                        if let Some(ref filter_id) = query.node_id {
                            if &qid != filter_id {
                                continue;
                            }
                        }

                        let timestamp = entry
                            .get("timestamp")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(frag.id.end_us);

                        if timestamp > query.as_of_us {
                            continue; // Skip events after the as_of timestamp
                        }

                        let id_key = qid.as_bytes().to_vec();
                        let snapshot = node_states.entry(id_key).or_insert_with(|| NodeSnapshot {
                            id: qid.clone(),
                            timestamp: 0,
                            properties: HashMap::new(),
                            edges: Vec::new(),
                        });

                        // Apply property updates
                        if let Some(props) = entry.get("properties").and_then(|v| v.as_object()) {
                            for (key, value) in props {
                                if let Some(pv) = json_to_property_value(value) {
                                    snapshot.properties.insert(key.clone(), pv);
                                }
                            }
                        }

                        // Apply edge updates
                        if let Some(edges) = entry.get("edges").and_then(|v| v.as_array()) {
                            for edge in edges {
                                let edge_type = edge
                                    .get("edge_type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let direction = edge
                                    .get("direction")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("out")
                                    .to_string();
                                let other_str =
                                    edge.get("other").and_then(|v| v.as_str()).unwrap_or("");
                                let other = NexoraId::from_hex(other_str).unwrap_or_else(|_| {
                                    NexoraId::from_bytes(other_str.as_bytes().to_vec())
                                });

                                snapshot.edges.push(EdgeSnapshot {
                                    edge_type,
                                    direction,
                                    other,
                                    timestamp,
                                });
                            }
                        }

                        snapshot.timestamp = snapshot.timestamp.max(timestamp);
                    }
                }
            }
        }
    }

    // Collect results, applying property filter
    let nodes: Vec<NodeSnapshot> = node_states
        .into_values()
        .filter(|snapshot| {
            if let Some(ref prop_filter) = query.property_filter {
                let (ref key, ref cond) = *prop_filter;
                match cond {
                    PropertyCondition::Exists => snapshot.properties.contains_key(key),
                    PropertyCondition::IsNull => !snapshot.properties.contains_key(key),
                    PropertyCondition::GreaterThan(t) => snapshot
                        .properties
                        .get(key)
                        .and_then(|v| v.as_f64())
                        .is_some_and(|v| v > *t),
                    PropertyCondition::LessThan(t) => snapshot
                        .properties
                        .get(key)
                        .and_then(|v| v.as_f64())
                        .is_some_and(|v| v < *t),
                    PropertyCondition::Equals(val) => {
                        snapshot.properties.get(key).is_some_and(|v| v == val)
                    }
                }
            } else {
                true
            }
        })
        .collect();

    Ok(TimeTravelResult {
        as_of: query.as_of_us,
        fragments_scanned: scanned,
        nodes,
    })
}

/// Consolidate multiple small fragments into a single larger fragment.
pub async fn consolidate_fragments(
    store: &FragmentStore,
    fragment_ids: &[FragmentId],
    target_start: u64,
    target_end: u64,
) -> Result<FragmentId, crate::store::FragmentError> {
    let new_id = FragmentId {
        start_us: target_start,
        end_us: target_end,
        uuid: uuid::Uuid::new_v4(),
    };

    // Create the new fragment directory
    let new_dir = store.create_fragment_dir(&new_id)?;
    let merged_file = new_dir.join("nodes.jsonl");

    // Merge all source fragments into the new one
    let mut merged_content = String::new();
    for fid in fragment_ids {
        let frag_dir = store.fragment_dir(fid);
        let nodes_file = frag_dir.join("nodes.jsonl");
        if nodes_file.exists() {
            if let Ok(content) = std::fs::read_to_string(&nodes_file) {
                merged_content.push_str(&content);
                if !content.ends_with('\n') {
                    merged_content.push('\n');
                }
            }
        }
    }

    // Write merged content
    std::fs::write(&merged_file, &merged_content)?;

    // Register the new consolidated fragment
    let mut meta = FragmentMetadata::new(new_id.clone(), "consolidated".into());
    meta.is_consolidated = true;
    meta.time_range = (target_start, target_end);
    store.register(meta).await?;

    // Remove old fragments
    for fid in fragment_ids {
        store.remove(fid).await?;
        let old_dir = store.fragment_dir(fid);
        let _ = std::fs::remove_dir_all(&old_dir);
    }

    Ok(new_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::FragmentStore;
    use tempfile::tempdir;

    fn make_store() -> FragmentStore {
        let dir = tempdir().unwrap();
        FragmentStore::new(dir.path(), "test")
    }

    fn write_fragment_nodes(store: &FragmentStore, fid: &FragmentId, lines: &[&str]) {
        let dir = store.create_fragment_dir(fid).unwrap();
        let content = lines.join("\n");
        std::fs::write(dir.join("nodes.jsonl"), content).unwrap();
    }

    #[tokio::test]
    async fn test_time_travel_basic() {
        let store = make_store();

        let fid1 = FragmentId {
            start_us: 1000,
            end_us: 2000,
            uuid: uuid::Uuid::new_v4(),
        };
        let fid2 = FragmentId {
            start_us: 2000,
            end_us: 3000,
            uuid: uuid::Uuid::new_v4(),
        };

        // Register fragments
        store
            .register(FragmentMetadata::new(fid1.clone(), "test".into()))
            .await
            .unwrap();
        store
            .register(FragmentMetadata::new(fid2.clone(), "test".into()))
            .await
            .unwrap();

        // Write node data
        write_fragment_nodes(
            &store,
            &fid1,
            &[r#"{"id":"node1","timestamp":1500,"properties":{"name":"Alice","speed":50}}"#],
        );
        write_fragment_nodes(
            &store,
            &fid2,
            &[
                r#"{"id":"node1","timestamp":2500,"properties":{"speed":80},"edges":[{"edge_type":"KNOWS","direction":"out","other":"node2"}]}"#,
            ],
        );

        // Query as of 1600 — should see Alice with speed=50
        let result = execute_time_travel(&store, TimeTravelQuery::at(1600))
            .await
            .unwrap();
        assert_eq!(result.node_count(), 1);
        let node = &result.nodes[0];
        assert_eq!(
            node.properties.get("name"),
            Some(&PropertyValue::String("Alice".into()))
        );
        assert_eq!(
            node.properties.get("speed"),
            Some(&PropertyValue::Integer(50))
        );

        // Query as of 2600 — should see speed updated to 80 and an edge
        let result = execute_time_travel(&store, TimeTravelQuery::at(2600))
            .await
            .unwrap();
        assert_eq!(result.node_count(), 1);
        let node = &result.nodes[0];
        assert_eq!(
            node.properties.get("speed"),
            Some(&PropertyValue::Integer(80))
        );
        assert_eq!(node.edges.len(), 1);
    }

    #[tokio::test]
    async fn test_time_travel_for_specific_node() {
        let store = make_store();

        let fid = FragmentId {
            start_us: 1000,
            end_us: 2000,
            uuid: uuid::Uuid::new_v4(),
        };
        store
            .register(FragmentMetadata::new(fid.clone(), "test".into()))
            .await
            .unwrap();

        write_fragment_nodes(
            &store,
            &fid,
            &[
                r#"{"id":"node1","timestamp":1500,"properties":{"speed":50}}"#,
                r#"{"id":"node2","timestamp":1500,"properties":{"speed":90}}"#,
            ],
        );

        let qid = NexoraId::from_bytes(b"node1".to_vec());
        let result = execute_time_travel(&store, TimeTravelQuery::at(1600).for_node(qid))
            .await
            .unwrap();
        assert_eq!(result.node_count(), 1);
        assert_eq!(result.nodes[0].id, NexoraId::from_bytes(b"node1".to_vec()));
    }

    #[tokio::test]
    async fn test_consolidate() {
        let store = make_store();

        let fid1 = FragmentId {
            start_us: 1000,
            end_us: 1500,
            uuid: uuid::Uuid::new_v4(),
        };
        let fid2 = FragmentId {
            start_us: 1500,
            end_us: 2000,
            uuid: uuid::Uuid::new_v4(),
        };

        store
            .register(FragmentMetadata::new(fid1.clone(), "test".into()))
            .await
            .unwrap();
        store
            .register(FragmentMetadata::new(fid2.clone(), "test".into()))
            .await
            .unwrap();

        write_fragment_nodes(&store, &fid1, &[r#"{"id":"n1","timestamp":1200}"#]);
        write_fragment_nodes(&store, &fid2, &[r#"{"id":"n2","timestamp":1700}"#]);

        assert_eq!(store.count().await, 2);

        // Consolidate
        let new_id = consolidate_fragments(&store, &[fid1, fid2], 1000, 2000)
            .await
            .unwrap();

        assert_eq!(store.count().await, 1);

        // Verify merged data
        let merged_dir = store.fragment_dir(&new_id);
        let content = std::fs::read_to_string(merged_dir.join("nodes.jsonl")).unwrap();
        assert!(content.contains("n1"));
        assert!(content.contains("n2"));
    }

    /// F1.0: 3-fragment MVCC覆盖语义验证 — 后续fragment覆盖前面数据。
    ///
    /// 场景：
    /// - T1 (1000-2000): node1.version = 1
    /// - T2 (2000-3000): node1.version = 2 (覆盖T1)
    /// - T3 (3000-4000): node1.version = 3 (覆盖T1+T2)
    ///
    /// 查询as_of=2500时，应看到version=2（T2的数据覆盖了T1）。
    /// 查询as_of=3500时，应看到version=3（T3的数据覆盖了T1+T2）。
    #[tokio::test]
    async fn test_time_travel_mvcc_three_fragments() {
        let store = make_store();
        let node_id = NexoraId::from_bytes(b"alice".to_vec());
        let id_hex = node_id.to_hex();

        // T1: 1000-2000, version=1
        let fid1 = FragmentId {
            start_us: 1000,
            end_us: 2000,
            uuid: uuid::Uuid::new_v4(),
        };
        store
            .register(FragmentMetadata::new(fid1.clone(), "test".into()))
            .await
            .unwrap();
        write_fragment_nodes(
            &store,
            &fid1,
            &[&format!(
                r#"{{"id":"{}","timestamp":1500,"properties":{{"version":1}}}}"#,
                id_hex
            )],
        );

        // T2: 2000-3000, version=2（覆盖T1）
        let fid2 = FragmentId {
            start_us: 2000,
            end_us: 3000,
            uuid: uuid::Uuid::new_v4(),
        };
        store
            .register(FragmentMetadata::new(fid2.clone(), "test".into()))
            .await
            .unwrap();
        write_fragment_nodes(
            &store,
            &fid2,
            &[&format!(
                r#"{{"id":"{}","timestamp":2500,"properties":{{"version":2}}}}"#,
                id_hex
            )],
        );

        // T3: 3000-4000, version=3（覆盖T1+T2）
        let fid3 = FragmentId {
            start_us: 3000,
            end_us: 4000,
            uuid: uuid::Uuid::new_v4(),
        };
        store
            .register(FragmentMetadata::new(fid3.clone(), "test".into()))
            .await
            .unwrap();
        write_fragment_nodes(
            &store,
            &fid3,
            &[&format!(
                r#"{{"id":"{}","timestamp":3500,"properties":{{"version":3}}}}"#,
                id_hex
            )],
        );

        // 查询as_of=2500（T2中间）：应看到version=2（T2覆盖了T1的version=1）
        let result = execute_time_travel(&store, TimeTravelQuery::at(2500))
            .await
            .unwrap();
        assert_eq!(result.node_count(), 1, "应找到node1");
        let node = result.find_node(&node_id).unwrap();
        assert_eq!(
            node.properties.get("version"),
            Some(&PropertyValue::Integer(2)),
            "T2应覆盖T1，version=2"
        );
        assert_eq!(node.timestamp, 2500, "timestamp应是T2的2500");

        // 查询as_of=3500（T3中间）：应看到version=3（T3覆盖了T1+T2）
        let result = execute_time_travel(&store, TimeTravelQuery::at(3500))
            .await
            .unwrap();
        assert_eq!(result.node_count(), 1, "应找到node1");
        let node = result.find_node(&node_id).unwrap();
        assert_eq!(
            node.properties.get("version"),
            Some(&PropertyValue::Integer(3)),
            "T3应覆盖T1+T2，version=3"
        );
        assert_eq!(node.timestamp, 3500, "timestamp应是T3的3500");

        // 查询as_of=1500（T1中间）：应看到version=1（只有T1的数据）
        let result = execute_time_travel(&store, TimeTravelQuery::at(1500))
            .await
            .unwrap();
        assert_eq!(result.node_count(), 1, "应找到node1");
        let node = result.find_node(&node_id).unwrap();
        assert_eq!(
            node.properties.get("version"),
            Some(&PropertyValue::Integer(1)),
            "T1时刻只有version=1"
        );
    }
}
