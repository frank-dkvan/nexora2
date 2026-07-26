//! Adapter that bridges GraphService to the GraphHandler trait.
//!
//! This allows a local GraphService to serve remote TCP requests via
//! TcpGraphServer, enabling distributed graph operations.

use crate::distributed_query::WRITE_STAT_COLUMNS;
use crate::fencing::ShardFence;
use crate::tcp_transport::GraphHandler;
use crate::{GraphOperation, GraphResult, RouterError};
use nexora_core::graph::GraphService;
use nexora_id::PropertyValue;
use nexora_value::{EdgeDirection, HalfEdge, Symbol};
use std::sync::Arc;

/// Adapter wrapping GraphService to implement GraphHandler.
///
/// Translates GraphOperation → GraphService method calls,
/// and GraphService results → GraphResult.
pub struct GraphServiceAdapter {
    graph: Arc<GraphService>,
    /// Per-shard epoch fence, shared with the cluster manager. Used to reject
    /// `FencedWrite`s replicated by an owner that failover has since deposed.
    /// `None` in single-node / test setups that never replicate — those never
    /// send `FencedWrite`, so no fence is needed.
    fence: Option<ShardFence>,
    /// Per-shard replication log, shared with the cluster manager. When set,
    /// admitted `FencedWrite`s are recorded by seq so this node can serve — or
    /// request — an incremental catch-up delta. `None` → no delta support.
    replication_log: Option<crate::replication_log::ShardReplicationLog>,
    /// Per-node catch-up barrier, shared with the cluster manager. When any
    /// shard on this node is mid-catch-up the local graph is incomplete, so
    /// serving a whole-graph `ExecuteCypher` read would feed the coordinator a
    /// silent partial. When set and `any_blocked_now()`, the handler errors
    /// instead. `None` → never gate (single node / tests that never fail over).
    catch_up_barrier: Option<crate::catch_up_barrier::CatchUpBarrier>,
    /// Bridge to the local event-table store, injected by the app layer. When
    /// set, `ScanEventTable` ops are served by scanning the local Iceberg event
    /// table and returning Arrow IPC bytes. `None` → event-first is off or this
    /// is a graph-only build; `ScanEventTable` then returns an empty result.
    event_scanner: Option<Arc<dyn crate::EventTableScanner>>,
    /// Bridge to the local ontology manager, injected by the app layer. When
    /// set, `ApplyOntology` ops register the broadcast ontology locally (create
    /// event tables + update routing) without re-broadcasting. `None` → the op
    /// is accepted as a no-op.
    ontology_applier: Option<Arc<dyn crate::OntologyApplier>>,
}

impl GraphServiceAdapter {
    pub fn new(graph: Arc<GraphService>) -> Self {
        Self {
            graph,
            fence: None,
            replication_log: None,
            catch_up_barrier: None,
            event_scanner: None,
            ontology_applier: None,
        }
    }

    /// Attach an event-table scanner so this node can serve `ScanEventTable`
    /// requests for cross-node event queries. Builder-style; chains onto any
    /// constructor. `None` scanner leaves event scans returning empty.
    pub fn with_event_scanner(mut self, scanner: Arc<dyn crate::EventTableScanner>) -> Self {
        self.event_scanner = Some(scanner);
        self
    }

    /// Attach an ontology applier so this node can register ontologies broadcast
    /// by a peer (`ApplyOntology`). Builder-style. `None` leaves `ApplyOntology`
    /// a no-op.
    pub fn with_ontology_applier(mut self, applier: Arc<dyn crate::OntologyApplier>) -> Self {
        self.ontology_applier = Some(applier);
        self
    }

    /// Construct an adapter that enforces epoch fencing on replicated writes,
    /// sharing the cluster manager's [`ShardFence`].
    pub fn with_fence(graph: Arc<GraphService>, fence: ShardFence) -> Self {
        Self {
            graph,
            fence: Some(fence),
            replication_log: None,
            catch_up_barrier: None,
            event_scanner: None,
            ontology_applier: None,
        }
    }

    /// Construct an adapter with both epoch fencing and a shared replication log
    /// (records admitted writes by seq; serves `ExportDelta`).
    pub fn with_fence_and_log(
        graph: Arc<GraphService>,
        fence: ShardFence,
        log: crate::replication_log::ShardReplicationLog,
    ) -> Self {
        Self {
            graph,
            fence: Some(fence),
            replication_log: Some(log),
            catch_up_barrier: None,
            event_scanner: None,
            ontology_applier: None,
        }
    }

    /// Construct a fully cluster-wired adapter: epoch fencing, replication log,
    /// and the catch-up barrier that gates whole-graph reads while any shard on
    /// this node is reconciling after a failover promotion. Serving-path
    /// constructor used by the real binary.
    pub fn with_fence_log_and_barrier(
        graph: Arc<GraphService>,
        fence: ShardFence,
        log: crate::replication_log::ShardReplicationLog,
        barrier: crate::catch_up_barrier::CatchUpBarrier,
    ) -> Self {
        Self {
            graph,
            fence: Some(fence),
            replication_log: Some(log),
            catch_up_barrier: Some(barrier),
            event_scanner: None,
            ontology_applier: None,
        }
    }

    /// Export every node + edge held for a given *cluster* shard as a
    /// [`ShardSnapshot`]. The cluster shard of a key is
    /// `qid.shard_key() % total_shards` (the routing function in
    /// [`crate::shard_map::ShardMap::shard_of`]), which differs from the
    /// GraphService's own internal shard partitioning — so we enumerate all node
    /// ids and keep only those hashing to `shard_id`.
    pub async fn export_shard(
        &self,
        shard_id: usize,
        total_shards: usize,
    ) -> Result<crate::migration::ShardSnapshot, nexora_core::graph::GraphError> {
        use crate::migration::{EdgeEntry, NodeEntry, ShardSnapshot};

        let all_ids = self.graph.all_node_ids().await?;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        for qid in all_ids {
            // Filter to keys owned by this cluster shard.
            if (qid.shard_key() as usize) % total_shards != shard_id {
                continue;
            }

            let props = self.graph.get_all_properties(&qid).await?;
            if !props.is_empty() {
                let properties = props
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), property_to_json(v)))
                    .collect();
                nodes.push(NodeEntry {
                    qid_hex: qid.to_hex(),
                    properties,
                });
            }

            // Only export outgoing edges: each edge is exported once, by its
            // source node's shard owner (matching how writes are routed by
            // source key), so replaying `AddEdge` on the target reconstructs it
            // without double-counting the mirrored in-edge.
            for edge in self.graph.get_edges(&qid).await? {
                if edge.direction != EdgeDirection::Out {
                    continue;
                }
                edges.push(EdgeEntry {
                    source_hex: qid.to_hex(),
                    edge_type: edge.edge_type.as_str().to_string(),
                    direction: "out".to_string(),
                    target_hex: edge.other.to_hex(),
                });
            }
        }

        Ok(ShardSnapshot {
            shard_id,
            epoch: crate::shard_map::OwnerEpoch::new(),
            nodes,
            edges,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })
    }

    /// Apply an admitted `FencedWrite`'s inner mutation directly to the local
    /// graph. The inner op is always a simple mutation (`SetProperty`/`AddEdge`);
    /// this mirrors the corresponding arms of [`GraphHandler::handle`] but as a
    /// plain inherent async fn, so `handle` doesn't recurse into itself (which
    /// gives `#[async_trait]` fragile `Send` inference). Any non-mutation inner
    /// op is a protocol error and is rejected rather than silently ignored.
    async fn apply_inner(&self, inner: GraphOperation) -> Result<GraphResult, RouterError> {
        match inner {
            GraphOperation::SetProperty { qid, key, value } => {
                let pv = json_to_property(value);
                self.graph
                    .set_property(&qid, &key, pv)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                Ok(GraphResult::Status {
                    ok: true,
                    message: "property set".into(),
                })
            }
            GraphOperation::AddEdge {
                source,
                edge_type,
                target,
                direction,
            } => {
                let dir = match direction.as_str() {
                    "in" | "In" | "IN" => EdgeDirection::In,
                    _ => EdgeDirection::Out,
                };
                let edge = HalfEdge::new(Symbol::new(&edge_type), dir, target);
                self.graph
                    .add_edge(&source, edge)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                Ok(GraphResult::Status {
                    ok: true,
                    message: "edge added".into(),
                })
            }
            other => Err(RouterError::Remote(format!(
                "FencedWrite inner must be a mutation (SetProperty/AddEdge), got {other:?}"
            ))),
        }
    }
}

#[async_trait::async_trait]
impl GraphHandler for GraphServiceAdapter {
    async fn handle(
        &self,
        _target_node: &str,
        op: GraphOperation,
    ) -> Result<GraphResult, RouterError> {
        match op {
            GraphOperation::FencedWrite {
                shard_id,
                epoch,
                seq,
                inner,
            } => {
                // Fence check: admit the write only if its epoch is at least the
                // highest this node has seen for the shard. A lower epoch means a
                // deposed owner is still replicating — reject it loudly so the
                // stale write never lands. With no fence configured (single-node
                // / test), admit unconditionally.
                if let Some(fence) = &self.fence {
                    if !fence.admit(shard_id, epoch).await {
                        let current = fence.current(shard_id).await;
                        return Err(RouterError::Remote(format!(
                            "fenced write rejected: shard {shard_id} epoch {} is stale (current {})",
                            epoch.value(),
                            current.map(|e| e.value()).unwrap_or(0),
                        )));
                    }
                }
                // Record in the replica replication log (by owner-assigned seq)
                // so this node can later serve / request an incremental delta.
                // seq==0 means the owner didn't assign one (unlogged path).
                if seq > 0 {
                    if let Some(log) = &self.replication_log {
                        log.record_replica(shard_id, seq, (*inner).clone()).await;
                    }
                }
                // Admitted — apply the inner mutation directly. We deliberately
                // do NOT recurse into `self.handle(..)` here: a self-recursive
                // `#[async_trait]` method has fragile `Send` inference (the boxed
                // future references its own type), and any change to a co-used
                // type tips it into "Send is not general enough". `apply_inner`
                // is a plain inherent async fn, so `handle`'s future no longer
                // references itself.
                self.apply_inner(*inner).await
            }

            GraphOperation::ExportDelta { shard_id, from_seq } => {
                // Serve the incremental replication delta since `from_seq`. With
                // no log, report TooOld so the caller falls back to a snapshot.
                use crate::state_transfer::DeltaResponse;
                let resp = match &self.replication_log {
                    Some(log) => match log.since(shard_id, from_seq).await {
                        crate::replication_log::CatchUp::UpToDate => DeltaResponse::UpToDate,
                        crate::replication_log::CatchUp::TooOld => DeltaResponse::TooOld,
                        crate::replication_log::CatchUp::Incremental(ops) => {
                            DeltaResponse::Delta { ops }
                        }
                    },
                    None => DeltaResponse::TooOld,
                };
                let json = serde_json::to_value(&resp)
                    .map_err(|e| RouterError::Serialization(e.to_string()))?;
                Ok(GraphResult::Property(Some(json)))
            }

            GraphOperation::ExportDigest { shard_id } => {
                // Serve the Merkle digest of this node's replication log for the
                // shard, so a peer's anti-entropy can compare it against its own
                // and detect divergence before pulling ops. With no log, report an
                // empty digest (range (0,0), zero hash) — a caller with real
                // entries then sees a mismatch and falls back to a full catch-up.
                let digest = match &self.replication_log {
                    Some(log) => crate::anti_entropy::compute_shard_digest(log, shard_id as u32)
                        .await
                        .map_err(RouterError::Remote)?,
                    None => crate::anti_entropy::ShardDigest {
                        shard_id: shard_id as u32,
                        seq_range: (0, 0),
                        root_hash: [0u8; 32],
                    },
                };
                let json = serde_json::to_value(&digest)
                    .map_err(|e| RouterError::Serialization(e.to_string()))?;
                Ok(GraphResult::Property(Some(json)))
            }

            GraphOperation::ExportShard {
                shard_id,
                total_shards,
            } => {
                // Export every node + edge this graph holds for the given cluster
                // shard, so a recovering owner/replica can catch up. The cluster
                // shard of a key is `shard_key() % total_shards` — distinct from
                // the GraphService's internal partitioning, so we filter by it.
                let snapshot = self
                    .export_shard(shard_id, total_shards)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                let json = serde_json::to_value(&snapshot)
                    .map_err(|e| RouterError::Serialization(e.to_string()))?;
                Ok(GraphResult::Property(Some(json)))
            }

            GraphOperation::GetProperty { qid, key } => {
                let result = self
                    .graph
                    .get_property(&qid, &key)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                Ok(GraphResult::Property(result.map(property_to_json)))
            }

            GraphOperation::SetProperty { qid, key, value } => {
                let pv = json_to_property(value);
                self.graph
                    .set_property(&qid, &key, pv)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                Ok(GraphResult::Status {
                    ok: true,
                    message: "property set".into(),
                })
            }

            GraphOperation::AddEdge {
                source,
                edge_type,
                target,
                direction,
            } => {
                let dir = match direction.as_str() {
                    "in" | "In" | "IN" => EdgeDirection::In,
                    _ => EdgeDirection::Out,
                };
                let edge = HalfEdge::new(Symbol::new(&edge_type), dir, target);
                self.graph
                    .add_edge(&source, edge)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                Ok(GraphResult::Status {
                    ok: true,
                    message: "edge added".into(),
                })
            }

            GraphOperation::GetEdges { qid, edge_type } => {
                let edges = self
                    .graph
                    .get_edges(&qid)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;

                let filtered: Vec<_> = match edge_type {
                    Some(ref et) => edges
                        .into_iter()
                        .filter(|e| e.edge_type.as_str() == et)
                        .collect(),
                    None => edges,
                };

                let edge_json: Vec<serde_json::Value> = filtered
                    .into_iter()
                    .map(|e| {
                        serde_json::json!({
                            "edge_type": e.edge_type.as_str(),
                            "direction": match e.direction {
                                EdgeDirection::Out => "out",
                                EdgeDirection::In => "in",
                            },
                            "target": e.other.to_hex(),
                        })
                    })
                    .collect();

                Ok(GraphResult::Property(Some(serde_json::Value::Array(
                    edge_json,
                ))))
            }

            GraphOperation::GetAllProperties { qid } => {
                let props = self
                    .graph
                    .get_all_properties(&qid)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;

                let map: serde_json::Map<String, serde_json::Value> = props
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), property_to_json(v)))
                    .collect();

                Ok(GraphResult::Property(Some(serde_json::Value::Object(map))))
            }

            GraphOperation::ExecuteCypher { query } => {
                // Catch-up gate: if any shard on this node is reconciling after a
                // failover promotion, the local graph is incomplete. A
                // scatter-gather read scans the whole local graph, so answering
                // now would feed the coordinator a silent partial. Error instead
                // — the coordinator refuses partial results on an owner error and
                // surfaces a retryable failure rather than an undercount. Catch-up
                // itself uses ExportDelta/snapshot ops, not ExecuteCypher, so it
                // never self-blocks. Uses the non-blocking `any_blocked_now()` so
                // no lock guard is held across the executor await below.
                let blocked = self
                    .catch_up_barrier
                    .as_ref()
                    .is_some_and(|b| b.any_blocked_now());
                if blocked {
                    return Err(RouterError::Remote(
                        "shard catch-up in progress: refusing whole-graph query to \
                         avoid a partial result (retryable)"
                            .to_string(),
                    ));
                }
                // Delegate to nexora-cypher executor
                let result = nexora_cypher::execute_cypher(&self.graph, &query)
                    .await
                    .map_err(|e| RouterError::Remote(e.to_string()))?;
                match result {
                    nexora_cypher::CypherResult::Rows { columns, rows } => {
                        Ok(GraphResult::CypherRows { columns, rows })
                    }
                    nexora_cypher::CypherResult::Write(wr) => {
                        // Return structured write stats so the distributed write
                        // path can SUM them across owners (a lossy string can't be
                        // aggregated). Encoded as a CypherRows with the 8 stat
                        // columns and one integer row — reuses an existing variant.
                        Ok(GraphResult::CypherRows {
                            columns: WRITE_STAT_COLUMNS.iter().map(|s| s.to_string()).collect(),
                            rows: vec![vec![
                                serde_json::json!(wr.nodes_created),
                                serde_json::json!(wr.nodes_deleted),
                                serde_json::json!(wr.relationships_created),
                                serde_json::json!(wr.relationships_deleted),
                                serde_json::json!(wr.properties_set),
                                serde_json::json!(wr.properties_removed),
                                serde_json::json!(wr.labels_added),
                                serde_json::json!(wr.labels_removed),
                            ]],
                        })
                    }
                    nexora_cypher::CypherResult::Empty => Ok(GraphResult::CypherRows {
                        columns: vec![],
                        rows: vec![],
                    }),
                }
            }

            GraphOperation::Ping => Ok(GraphResult::Status {
                ok: true,
                message: "pong".to_string(),
            }),

            GraphOperation::ScanEventTable { table } => {
                // Serve a cross-node event-table scan: scan the local Iceberg
                // event table and return its Arrow IPC bytes, hex-encoded inside
                // a JSON string. No scanner (event-first off / graph-only build)
                // or a missing table → empty string (this node contributes no
                // rows). The coordinator unions every node's contribution.
                let ipc_bytes = match &self.event_scanner {
                    Some(scanner) => scanner
                        .scan_table_ipc(&table)
                        .await
                        .map_err(RouterError::Remote)?,
                    None => Vec::new(),
                };
                let encoded = hex::encode(&ipc_bytes);
                Ok(GraphResult::Property(Some(serde_json::Value::String(
                    encoded,
                ))))
            }

            GraphOperation::ApplyOntology { pkg_json } => {
                // Register an ontology broadcast by a peer. Apply locally without
                // re-broadcasting (the originating node fans out to all peers).
                // No applier (event-first off) → accept as a no-op so the
                // broadcast never fails the originating write.
                match &self.ontology_applier {
                    Some(applier) => {
                        applier
                            .apply_ontology(&pkg_json)
                            .await
                            .map_err(RouterError::Remote)?;
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "ontology applied".to_string(),
                        })
                    }
                    None => Ok(GraphResult::Status {
                        ok: true,
                        message: "ontology ignored (event-first off)".to_string(),
                    }),
                }
            }

            GraphOperation::RemoveOntology { domain } => {
                // Drop a peer-broadcast ontology's routing rules locally. Apply
                // without re-broadcasting. No applier → no-op ack.
                match &self.ontology_applier {
                    Some(applier) => {
                        applier
                            .remove_ontology(&domain)
                            .await
                            .map_err(RouterError::Remote)?;
                        Ok(GraphResult::Status {
                            ok: true,
                            message: "ontology removed".to_string(),
                        })
                    }
                    None => Ok(GraphResult::Status {
                        ok: true,
                        message: "ontology removal ignored (event-first off)".to_string(),
                    }),
                }
            }
        }
    }
}

/// Convert PropertyValue to JSON value.
fn property_to_json(pv: PropertyValue) -> serde_json::Value {
    match pv {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Boolean(b) => serde_json::json!(b),
        PropertyValue::Integer(i) => serde_json::json!(i),
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::String(s) => serde_json::json!(s),
        PropertyValue::Bytes(b) => {
            serde_json::json!(hex::encode(&b))
        }
        PropertyValue::List(list) => {
            serde_json::Value::Array(list.into_iter().map(property_to_json).collect())
        }
        PropertyValue::Map(map) => {
            let m: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, property_to_json(v)))
                .collect();
            serde_json::Value::Object(m)
        }
        other => serde_json::json!(format!("{other:?}")),
    }
}

/// Convert JSON value to PropertyValue.
fn json_to_property(val: serde_json::Value) -> PropertyValue {
    match val {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                PropertyValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s),
        serde_json::Value::Array(arr) => {
            PropertyValue::List(arr.into_iter().map(json_to_property).collect())
        }
        serde_json::Value::Object(map) => {
            let m: std::collections::BTreeMap<String, PropertyValue> = map
                .into_iter()
                .map(|(k, v)| (k, json_to_property(v)))
                .collect();
            PropertyValue::Map(m)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_to_json_roundtrip() {
        let pv = PropertyValue::Integer(42);
        let json = property_to_json(pv.clone());
        let back = json_to_property(json);
        assert_eq!(pv, back);

        let pv = PropertyValue::String("hello".into());
        let json = property_to_json(pv.clone());
        let back = json_to_property(json);
        assert_eq!(pv, back);

        let pv = PropertyValue::Boolean(true);
        let json = property_to_json(pv.clone());
        let back = json_to_property(json);
        assert_eq!(pv, back);

        let pv = PropertyValue::Float(2.5);
        let json = property_to_json(pv.clone());
        let back = json_to_property(json);
        assert_eq!(pv, back);
    }

    #[test]
    fn test_json_to_property_list() {
        let json = serde_json::json!([1, 2, 3]);
        let pv = json_to_property(json);
        match pv {
            PropertyValue::List(items) => assert_eq!(items.len(), 3),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn test_json_to_property_map() {
        let json = serde_json::json!({"a": 1, "b": "hello"});
        let pv = json_to_property(json);
        match pv {
            PropertyValue::Map(m) => {
                assert_eq!(m.len(), 2);
                assert!(m.contains_key("a"));
                assert!(m.contains_key("b"));
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    fn test_graph() -> Arc<GraphService> {
        use nexora_core::{GraphServiceConfig, InMemoryPersistor};
        Arc::new(GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 100,
                node_channel_size: 64,
            },
            Arc::new(InMemoryPersistor::new()),
        ))
    }

    /// A whole-graph read served by an owner reconciling any shard must ERROR,
    /// not return (possibly partial) rows — the coordinator turns that error
    /// into a refused partial result. Read-side counterpart to the write
    /// barrier: an incomplete local graph must never silently undercount.
    #[tokio::test]
    async fn execute_cypher_blocked_during_catch_up() {
        let barrier = crate::catch_up_barrier::CatchUpBarrier::new();
        let adapter = GraphServiceAdapter::with_fence_log_and_barrier(
            test_graph(),
            crate::fencing::ShardFence::new(),
            crate::replication_log::ShardReplicationLog::new(64),
            barrier.clone(),
        );

        // No shard reconciling → read served normally.
        let ok = adapter
            .handle(
                "node",
                GraphOperation::ExecuteCypher {
                    query: "MATCH (n) RETURN count(n)".into(),
                },
            )
            .await;
        assert!(
            ok.is_ok(),
            "read must succeed when no shard is under catch-up"
        );

        // A shard enters catch-up → same read must now be refused.
        barrier.begin(2).await;
        let blocked = adapter
            .handle(
                "node",
                GraphOperation::ExecuteCypher {
                    query: "MATCH (n) RETURN count(n)".into(),
                },
            )
            .await;
        assert!(
            matches!(blocked, Err(RouterError::Remote(_))),
            "read must error while a shard reconciles, got {blocked:?}"
        );

        // Catch-up completes → read served again.
        barrier.end(2).await;
        let reopened = adapter
            .handle(
                "node",
                GraphOperation::ExecuteCypher {
                    query: "MATCH (n) RETURN count(n)".into(),
                },
            )
            .await;
        assert!(reopened.is_ok(), "read must succeed after catch-up ends");
    }

    /// Without a barrier wired (single-node / tests), reads are never gated —
    /// behaviour unchanged from before the fix.
    #[tokio::test]
    async fn execute_cypher_unblocked_without_barrier() {
        let adapter = GraphServiceAdapter::new(test_graph());
        let r = adapter
            .handle(
                "node",
                GraphOperation::ExecuteCypher {
                    query: "MATCH (n) RETURN count(n)".into(),
                },
            )
            .await;
        assert!(r.is_ok(), "no barrier → reads always served");
    }
}
