//! B3: Database-level backup & restore.
//!
//! Produces a single self-describing backup artifact: a sequence of node
//! records followed by a SnapshotManifest as the LAST entry (ArcadeDB pattern —
//! a torn backup lacks its trailing manifest, so an incomplete artifact is
//! detected on restore rather than silently loaded). The manifest carries a
//! Blake3 checksum over the payload for corruption detection.

use crate::graph::GraphService;
use crate::snapshot_manifest::{ChecksumKind, SnapshotKind, SnapshotManifest};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{EdgeDirection, HalfEdge, Symbol};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One node's full state in a backup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackupNode {
    pub qid_hex: String,
    pub properties: BTreeMap<String, PropertyValue>,
    pub labels: Vec<String>,
    pub edges: Vec<BackupEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackupEdge {
    pub edge_type: String,
    pub target_hex: String,
    pub direction: String, // "out" | "in"
}

/// The backup payload (before the manifest is appended).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackupPayload {
    /// Backup format version.
    pub version: u32,
    /// All node records.
    pub nodes: Vec<BackupNode>,
    /// Consistency point: last tx/seq id at backup time (0 if unknown).
    pub last_tx_id: u64,
}

/// A complete backup artifact: payload + trailing manifest.
///
/// Serialized as: {payload_json}\n---BACKUP-MANIFEST---\n{manifest_json}
/// The manifest is LAST so a torn write leaves it absent → detected on restore.
pub struct BackupArtifact;

impl BackupArtifact {
    const SEPARATOR: &'static str = "\n---BACKUP-MANIFEST---\n";
    const FORMAT_VERSION: u32 = 1;

    /// Create a backup of the entire graph into a byte buffer.
    ///
    /// Steps:
    /// 1. Flush all nodes to ensure persisted state is current
    /// 2. Enumerate all node IDs (from persistence)
    /// 3. For each node, read properties, labels, and edges
    /// 4. Serialize as JSON payload
    /// 5. Compute Blake3 checksum and append manifest
    ///
    /// Returns a complete backup artifact ready to write to disk.
    pub async fn create(graph: &GraphService, last_tx_id: u64) -> Result<Vec<u8>, String> {
        // Flush first so persisted state is current.
        graph
            .flush_all_nodes()
            .await
            .map_err(|e| format!("flush: {e}"))?;

        let ids = graph
            .all_node_ids()
            .await
            .map_err(|e| format!("enumerate: {e}"))?;

        let mut nodes = Vec::new();
        for qid in ids {
            // Get all properties
            let props = graph
                .get_all_properties(&qid)
                .await
                .map_err(|e| format!("props for {}: {e}", qid))?;

            // Get labels
            let labels_set = graph
                .get_labels(&qid)
                .await
                .map_err(|e| format!("labels for {}: {e}", qid))?;
            let labels: Vec<String> = labels_set.iter().map(|s| s.as_str().to_string()).collect();

            // Get edges
            let edges_raw = graph
                .get_edges(&qid)
                .await
                .map_err(|e| format!("edges for {}: {e}", qid))?;
            let edges: Vec<BackupEdge> = edges_raw
                .iter()
                .map(|he| BackupEdge {
                    edge_type: he.edge_type.as_str().to_string(),
                    target_hex: he.other.to_hex(),
                    direction: match he.direction {
                        EdgeDirection::Out => "out".to_string(),
                        EdgeDirection::In => "in".to_string(),
                    },
                })
                .collect();

            // Skip empty nodes (no properties, labels, or edges)
            if props.is_empty() && labels.is_empty() && edges.is_empty() {
                continue;
            }

            // Convert properties: Symbol keys → String keys, PropertyValue stays as-is
            let properties: BTreeMap<String, PropertyValue> = props
                .into_iter()
                .map(|(k, v)| (k.as_str().to_string(), v))
                .collect();

            nodes.push(BackupNode {
                qid_hex: qid.to_hex(),
                properties,
                labels,
                edges,
            });
        }

        let payload = BackupPayload {
            version: Self::FORMAT_VERSION,
            nodes,
            last_tx_id,
        };
        let payload_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;

        // Manifest over the payload (Blake3 for backup integrity).
        let manifest = SnapshotManifest::for_payload(
            SnapshotKind::DatabaseBackup,
            last_tx_id,
            &payload_bytes,
            ChecksumKind::Blake3,
        );
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(|e| e.to_string())?;

        // Assemble: payload + separator + manifest
        let mut out = payload_bytes;
        out.extend_from_slice(Self::SEPARATOR.as_bytes());
        out.extend_from_slice(&manifest_bytes);
        Ok(out)
    }

    /// Parse + verify a backup artifact. Returns the payload if intact.
    ///
    /// Checks:
    /// 1. Manifest is present (separator found)
    /// 2. Manifest deserializes correctly
    /// 3. Payload length matches manifest
    /// 4. Blake3 checksum matches
    ///
    /// Returns an error if the backup is incomplete (torn write) or corrupted.
    pub fn parse_and_verify(data: &[u8]) -> Result<BackupPayload, String> {
        // Find the separator; split payload | manifest.
        let sep_bytes = Self::SEPARATOR.as_bytes();
        let sep_pos = data
            .windows(sep_bytes.len())
            .position(|window| window == sep_bytes)
            .ok_or_else(|| {
                "incomplete backup (no manifest) — likely a torn write during backup creation"
                    .to_string()
            })?;

        let payload_bytes = &data[..sep_pos];
        let manifest_bytes = &data[sep_pos + sep_bytes.len()..];

        // Deserialize manifest
        let manifest: SnapshotManifest =
            serde_json::from_slice(manifest_bytes).map_err(|e| format!("manifest parse: {e}"))?;

        // Verify payload integrity
        manifest.verify(payload_bytes)?;

        // Deserialize payload
        let payload: BackupPayload =
            serde_json::from_slice(payload_bytes).map_err(|e| format!("payload parse: {e}"))?;

        // Check format version
        if payload.version != Self::FORMAT_VERSION {
            return Err(format!(
                "unsupported backup version: {} (expected {})",
                payload.version,
                Self::FORMAT_VERSION
            ));
        }

        Ok(payload)
    }

    /// Restore a backup into a graph (applies all nodes). The graph should be
    /// empty or the caller accepts merge semantics (set_property is idempotent).
    ///
    /// Returns the number of nodes restored.
    ///
    /// # PITR (Point-in-Time Recovery)
    ///
    /// This function restores a backup to its consistency point (`last_tx_id`).
    /// For full PITR to a specific target timestamp/transaction:
    ///
    /// 1. Restore the backup (establishes the base state at `last_tx_id`)
    /// 2. Replay WAL entries from `last_tx_id + 1` to `target_tx_id`
    ///
    /// The WAL replay infrastructure already exists in `nexora-core/wal/log.rs`.
    /// A complete PITR implementation would:
    /// - Accept a `target_tx_id` parameter
    /// - After restore completes, enumerate WAL entries > payload.last_tx_id
    /// - Apply each WAL mutation in order until target_tx_id is reached
    ///
    /// This is left as a TODO for the next iteration. The backup's `last_tx_id`
    /// already records the consistency cut, so the PITR foundation is in place.
    pub async fn restore(graph: &GraphService, data: &[u8]) -> Result<usize, String> {
        let payload = Self::parse_and_verify(data)?;
        let count = payload.nodes.len();

        tracing::warn!(
            nodes = count,
            last_tx_id = payload.last_tx_id,
            "restoring database backup (destructive operation — existing data may be overwritten)"
        );

        // Use a monotonic request_id for all restore mutations (for idempotency)
        let base_request_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        for (idx, node) in payload.nodes.into_iter().enumerate() {
            let qid = NexoraId::from_hex(&node.qid_hex)
                .map_err(|e| format!("parse qid {}: {e}", node.qid_hex))?;

            let mut request_id = base_request_id.wrapping_add(idx as u64);

            // Restore properties
            for (key, value) in node.properties {
                graph
                    .set_property(&qid, &key, value)
                    .await
                    .map_err(|e| format!("set property {}:{}: {e}", qid, key))?;
            }

            // Restore labels
            for label_str in node.labels {
                let label = Symbol::new(&label_str);
                request_id = request_id.wrapping_add(1);
                graph
                    .add_label(&qid, label, request_id)
                    .await
                    .map_err(|e| format!("add label {}:{}: {e}", qid, label_str))?;
            }

            // Restore edges
            for edge in node.edges {
                let target_qid = NexoraId::from_hex(&edge.target_hex)
                    .map_err(|e| format!("parse edge target {}: {e}", edge.target_hex))?;
                let direction = match edge.direction.as_str() {
                    "out" => EdgeDirection::Out,
                    "in" => EdgeDirection::In,
                    other => {
                        return Err(format!(
                            "invalid edge direction '{}' (expected 'out' or 'in')",
                            other
                        ))
                    }
                };
                let half_edge = HalfEdge::new(Symbol::new(&edge.edge_type), direction, target_qid);
                graph
                    .add_edge(&qid, half_edge)
                    .await
                    .map_err(|e| format!("add edge {}:{}: {e}", qid, edge.edge_type))?;
            }
        }

        tracing::info!(nodes = count, "database backup restored successfully");
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphServiceConfig;
    use crate::persistor::InMemoryPersistor;
    use std::sync::Arc;

    async fn test_graph() -> GraphService {
        let persistor = Arc::new(InMemoryPersistor::new());
        GraphService::new(GraphServiceConfig::default(), persistor)
    }

    #[tokio::test]
    async fn backup_roundtrip_preserves_nodes() {
        let graph1 = test_graph().await;
        let qid = NexoraId::new_random();

        // Write data to first graph
        graph1
            .set_property(&qid, "name", PropertyValue::String("Alice".to_string()))
            .await
            .unwrap();
        graph1
            .set_property(&qid, "age", PropertyValue::Integer(30))
            .await
            .unwrap();
        graph1
            .add_label(&qid, Symbol::new("Person"), 1)
            .await
            .unwrap();

        // Create backup
        let backup_data = BackupArtifact::create(&graph1, 100).await.unwrap();

        // Restore to new graph
        let graph2 = test_graph().await;
        let restored_count = BackupArtifact::restore(&graph2, &backup_data)
            .await
            .unwrap();

        assert_eq!(restored_count, 1);

        // Verify data
        let name = graph2.get_property(&qid, "name").await.unwrap();
        assert_eq!(name, Some(PropertyValue::String("Alice".to_string())));

        let age = graph2.get_property(&qid, "age").await.unwrap();
        assert_eq!(age, Some(PropertyValue::Integer(30)));

        let labels = graph2.get_labels(&qid).await.unwrap();
        assert!(labels.contains(&Symbol::new("Person")));
    }

    #[tokio::test]
    async fn backup_detects_truncation() {
        let graph = test_graph().await;
        let qid = NexoraId::new_random();
        graph
            .set_property(&qid, "test", PropertyValue::Boolean(true))
            .await
            .unwrap();

        let backup_data = BackupArtifact::create(&graph, 42).await.unwrap();

        // Truncate the backup (cut off part of the payload)
        let truncated = &backup_data[..backup_data.len() / 2];
        let result = BackupArtifact::parse_and_verify(truncated);

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Should detect missing manifest (separator not found) or malformed manifest JSON
        assert!(
            err.contains("incomplete")
                || err.contains("no manifest")
                || err.contains("manifest parse"),
            "Expected truncation error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn backup_detects_missing_manifest() {
        let graph = test_graph().await;
        let qid = NexoraId::new_random();
        graph
            .set_property(&qid, "x", PropertyValue::Integer(1))
            .await
            .unwrap();

        // Manually create payload without manifest
        let payload = BackupPayload {
            version: 1,
            nodes: vec![BackupNode {
                qid_hex: qid.to_hex(),
                properties: [("x".to_string(), PropertyValue::Integer(1))]
                    .into_iter()
                    .collect(),
                labels: vec![],
                edges: vec![],
            }],
            last_tx_id: 0,
        };
        let payload_only = serde_json::to_vec(&payload).unwrap();

        let result = BackupArtifact::parse_and_verify(&payload_only);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("incomplete"));
    }

    #[tokio::test]
    async fn backup_detects_corruption() {
        let graph = test_graph().await;
        let qid = NexoraId::new_random();
        graph
            .set_property(&qid, "data", PropertyValue::String("original".to_string()))
            .await
            .unwrap();

        let mut backup_data = BackupArtifact::create(&graph, 99).await.unwrap();

        // Corrupt one byte in the payload (not the manifest)
        if backup_data.len() > 100 {
            backup_data[50] = backup_data[50].wrapping_add(1);
        }

        let result = BackupArtifact::parse_and_verify(&backup_data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("checksum") || err.contains("parse"));
    }

    #[tokio::test]
    async fn backup_preserves_labels_and_edges() {
        let graph1 = test_graph().await;
        let qid1 = NexoraId::new_random();
        let qid2 = NexoraId::new_random();

        // Node 1: two labels, one property
        graph1
            .add_label(&qid1, Symbol::new("Person"), 1)
            .await
            .unwrap();
        graph1
            .add_label(&qid1, Symbol::new("Employee"), 2)
            .await
            .unwrap();
        graph1
            .set_property(&qid1, "name", PropertyValue::String("Bob".to_string()))
            .await
            .unwrap();

        // Node 2: target of an edge
        graph1
            .set_property(&qid2, "city", PropertyValue::String("NYC".to_string()))
            .await
            .unwrap();

        // Add edge: qid1 -[LIVES_IN]-> qid2
        let edge = HalfEdge::out(Symbol::new("LIVES_IN"), qid2.clone());
        graph1.add_edge(&qid1, edge).await.unwrap();

        // Backup + restore
        let backup_data = BackupArtifact::create(&graph1, 200).await.unwrap();
        let graph2 = test_graph().await;
        let count = BackupArtifact::restore(&graph2, &backup_data)
            .await
            .unwrap();
        assert_eq!(count, 2);

        // Verify labels
        let labels = graph2.get_labels(&qid1).await.unwrap();
        assert!(labels.contains(&Symbol::new("Person")));
        assert!(labels.contains(&Symbol::new("Employee")));

        // Verify edges
        let edges = graph2.get_edges(&qid1).await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].edge_type, Symbol::new("LIVES_IN"));
        assert_eq!(edges[0].other, qid2);
        assert_eq!(edges[0].direction, EdgeDirection::Out);
    }

    #[tokio::test]
    async fn backup_empty_graph() {
        let graph1 = test_graph().await;

        // Backup empty graph
        let backup_data = BackupArtifact::create(&graph1, 0).await.unwrap();

        // Restore to new graph
        let graph2 = test_graph().await;
        let count = BackupArtifact::restore(&graph2, &backup_data)
            .await
            .unwrap();

        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn backup_skips_empty_nodes() {
        let graph = test_graph().await;

        // Create a node but don't set any properties/labels/edges
        let qid = NexoraId::new_random();
        // Just touch it (wake it up, but leave empty)
        let _ = graph.get_property(&qid, "nonexistent").await;

        // Backup should skip empty node
        let backup_data = BackupArtifact::create(&graph, 0).await.unwrap();
        let payload = BackupArtifact::parse_and_verify(&backup_data).unwrap();

        // Empty nodes are filtered out
        assert_eq!(payload.nodes.len(), 0);
    }

    #[tokio::test]
    async fn backup_manifest_fields_correct() {
        let graph = test_graph().await;
        let qid = NexoraId::new_random();
        graph
            .set_property(&qid, "k", PropertyValue::Integer(42))
            .await
            .unwrap();

        let backup_data = BackupArtifact::create(&graph, 12345).await.unwrap();

        // Parse to verify manifest structure
        let sep_bytes = BackupArtifact::SEPARATOR.as_bytes();
        let sep_pos = backup_data
            .windows(sep_bytes.len())
            .position(|w| w == sep_bytes)
            .unwrap();
        let manifest_bytes = &backup_data[sep_pos + sep_bytes.len()..];
        let manifest: SnapshotManifest = serde_json::from_slice(manifest_bytes).unwrap();

        assert_eq!(manifest.kind, SnapshotKind::DatabaseBackup);
        assert_eq!(manifest.last_tx_id, 12345);
        assert_eq!(manifest.checksum_kind, ChecksumKind::Blake3);
        assert_eq!(manifest.format_version, 1);
        // Blake3 hex is 64 chars
        assert_eq!(manifest.checksum.len(), 64);
    }
}
