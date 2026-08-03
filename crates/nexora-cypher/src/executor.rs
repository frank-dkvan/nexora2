//! Cypher executor — bridges cypher-parser's GraphProvider to Nexora's GraphService.
//!
//! Takes an async snapshot of the graph, then runs cypher-parser's
//! synchronous executor against it.

use crate::{CypherError, CypherResult};
use cypher_parser::{CypherValue as CpValue, GraphProvider, ResultSet};
use nexora_core::GraphService;

use nexora_id::{NexoraId, PropertyValue};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Configuration for query execution limits to prevent resource exhaustion.
#[derive(Clone, Debug)]
pub struct QueryLimits {
    /// Maximum number of rows in result set (default: 100,000)
    pub max_result_rows: usize,
    /// Maximum query execution time (default: 30 seconds)
    pub max_execution_time: Duration,
    /// Maximum nodes in snapshot (default: 10,000,000)
    pub max_snapshot_nodes: usize,
    /// Maximum pattern depth in MATCH clauses (default: 10) - H-4 fix
    /// Prevents deeply nested patterns like MATCH (a)-[]->()-[]->()...->()
    pub max_pattern_depth: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_result_rows: 100_000,
            max_execution_time: Duration::from_secs(30),
            max_snapshot_nodes: 10_000_000,
            max_pattern_depth: 10, // H-4: Prevent query complexity attacks
        }
    }
}

/// Execute a Cypher query against a GraphService with default limits.
pub async fn execute(graph: &GraphService, query: &str) -> Result<CypherResult, CypherError> {
    execute_with_limits(graph, query, &QueryLimits::default()).await
}

/// Execute a Cypher query against a GraphService with custom limits.
pub async fn execute_with_limits(
    graph: &GraphService,
    query: &str,
    limits: &QueryLimits,
) -> Result<CypherResult, CypherError> {
    // Parse the query
    let parsed = cypher_parser::parse(query).map_err(|e| CypherError::Parse(e.to_string()))?;

    // H-4: Validate pattern depth to prevent complexity attacks
    validate_pattern_depth(&parsed, limits.max_pattern_depth)?;

    // Wrap execution in timeout
    let result = tokio::time::timeout(limits.max_execution_time, async {
        // Take a snapshot of relevant nodes from the graph
        let snapshot = NexoraGraphSnapshot::from_graph(graph, &parsed, limits.max_snapshot_nodes).await?;

        // Execute using cypher-parser's built-in executor
        let result_set = cypher_parser::execute(&snapshot, &parsed)
            .map_err(|e| CypherError::Execution(e.to_string()))?;

        // Check result set size
        if result_set.rows.len() > limits.max_result_rows {
            return Err(CypherError::Execution(format!(
                "Result set contains {} rows, exceeding limit of {}. Use LIMIT clause to reduce result size.",
                result_set.rows.len(),
                limits.max_result_rows
            )));
        }

        // Convert result
        let mut cypher_result = convert_result(result_set);

        // Post-process: fill in edge properties that cypher-parser doesn't support
        cypher_result = fill_edge_properties(cypher_result, &snapshot, query)?;

        Ok(cypher_result)
    })
    .await;

    match result {
        Ok(inner) => inner,
        Err(_) => Err(CypherError::Execution(format!(
            "Query exceeded maximum execution time of {:?}",
            limits.max_execution_time
        ))),
    }
}

fn convert_result(rs: ResultSet) -> CypherResult {
    if rs.columns.is_empty() && rs.rows.is_empty() {
        return CypherResult::Empty;
    }
    let columns = rs.columns;
    let rows = rs
        .rows
        .iter()
        .map(|row| row.iter().map(cp_value_to_json).collect())
        .collect();
    CypherResult::Rows { columns, rows }
}

fn cp_value_to_json(v: &CpValue) -> serde_json::Value {
    match v {
        CpValue::Null => serde_json::Value::Null,
        CpValue::Bool(b) => serde_json::Value::Bool(*b),
        CpValue::Int(i) => serde_json::json!(i),
        CpValue::Str(s) => serde_json::Value::String(s.clone()),
        CpValue::Node { id, label, name } => {
            serde_json::json!({ "id": id, "label": label, "name": name })
        }
        CpValue::List(items) => {
            serde_json::Value::Array(items.iter().map(cp_value_to_json).collect())
        }
    }
}

// ============================================================
// NexoraGraphSnapshot — implements cypher-parser's GraphProvider
// ============================================================

/// An in-memory snapshot of the graph that implements cypher-parser's GraphProvider.
///
/// This is populated asynchronously from GraphService, then used synchronously
/// by cypher-parser's executor.
pub struct NexoraGraphSnapshot {
    /// u64 index → node data
    nodes: Vec<NodeData>,
    /// u64 index → NexoraId
    idx_to_id: HashMap<u64, NexoraId>,
    /// All known relationship types
    rel_types: Vec<String>,
}

struct NodeData {
    labels: Vec<String>,
    properties: HashMap<String, PropertyValue>,
    outgoing_edges: Vec<EdgeData>, // Edge with properties
}

struct EdgeData {
    edge_type: String,
    target_idx: u64,
    properties: HashMap<String, PropertyValue>,
}

impl NexoraGraphSnapshot {
    /// Take a snapshot from the GraphService.
    ///
    /// If the query includes labels (e.g. MATCH (n:Person)), the label index
    /// is used to get candidate node IDs instead of performing a full
    /// all_node_ids() scan.  This keeps the Cypher executor responsive on
    /// graphs with millions of nodes as long as queries are label-scoped.
    pub async fn from_graph(
        graph: &GraphService,
        query: &cypher_parser::ast::Query,
        max_snapshot_nodes: usize,
    ) -> Result<Self, CypherError> {
        let mut nodes = Vec::new();
        let mut id_to_idx = HashMap::new();
        let mut idx_to_id = HashMap::new();
        let mut rel_types_set = std::collections::HashSet::new();

        // Extract relationship types from the query to scope the snapshot
        let needed_rel_types = extract_rel_types(query);

        // P0.4: Use the label index when the query includes labels.
        // MATCH (n:Person) → node_ids from label index instead of all_node_ids().
        let node_labels = extract_node_labels(query);
        let property_filter = extract_node_property_filter(query);
        let mut node_ids: Vec<NexoraId> =
            if !node_labels.is_empty() {
                graph
                    .label_index
                    .query_all(&node_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                    .await
            } else if let Some((ref prop, ref value)) = property_filter {
                // P0.4: Use property index for exact-match property filter
                graph.query_property_index(prop, value).await.map_err(|e| {
                    CypherError::Execution(format!("Property index query failed: {e}"))
                })?
            } else {
                graph.all_node_ids().await.map_err(|e| {
                    CypherError::Execution(format!("Failed to enumerate nodes: {e}"))
                })?
            };
        node_ids.sort_by_key(NexoraId::to_hex);

        // P0-4 FIX: Lowered snapshot safety threshold to 1M nodes to prevent OOM.
        // At 1M nodes × ~1KB/node ≈ 1GB memory footprint, which is manageable.
        // The previous 10M limit could cause 10GB+ memory usage and coordinator OOM.
        // For workloads exceeding 1M nodes, use label/property filters or implement
        // streaming snapshots (future work).
        const SAFE_SNAPSHOT_LIMIT: usize = 1_000_000;
        let effective_limit = max_snapshot_nodes.min(SAFE_SNAPSHOT_LIMIT);

        if node_ids.len() > effective_limit {
            return Err(CypherError::Execution(format!(
                "Cypher snapshot contains {} nodes, exceeding the safety limit of {effective_limit}. \
                 Consider adding label or property filters to reduce the working set, \
                 or use streaming execution for large result sets.",
                node_ids.len()
            )));
        }

        // Build index
        for (i, qid) in node_ids.iter().enumerate() {
            let idx = i as u64;
            id_to_idx.insert(qid.to_hex(), idx);
            idx_to_id.insert(idx, qid.clone());
        }

        // #1b: Read each node's full state in a single call. For resident nodes
        // this hits the shared projection (a lock-free snapshot clone), so the
        // former three-round-trip-per-node mailbox loop (properties + labels +
        // edges) collapses to one projection read with no actor round-trip.
        // Cold nodes fall back to the wake+snapshot path inside read_node_state.
        //
        // The reads are independent, so we drive up to SNAPSHOT_READ_CONCURRENCY
        // of them at once with `buffered`, which preserves input order — the
        // returned `node_states` still line up 1:1 with `node_ids`, which the
        // index maps and edge wiring below rely on. This turns the snapshot build
        // from a serial await-per-node walk into a concurrent fan-out (cold nodes
        // that hit the wake path benefit most).
        //
        // Adjacency needs every node's edges, so we cache the per-node states
        // and reuse them for edge building rather than re-reading.
        const SNAPSHOT_READ_CONCURRENCY: usize = 64;
        use futures::stream::{StreamExt, TryStreamExt};
        // Each future owns its `NexoraId` (via `cloned`) and captures `graph` by
        // reference-copy. Mapping over owned ids rather than `&NexoraId` avoids a
        // higher-ranked-lifetime inference failure ("FnOnce is not general
        // enough") that otherwise breaks downstream crates calling this path.
        // clippy flags the `.cloned()` as redundant, but it is load-bearing:
        // mapping over owned `NexoraId`s (not `&NexoraId`) is what avoids the
        // higher-ranked-lifetime / `Send` inference failure described above that
        // breaks downstream crates (nexora-zenoh's `#[async_trait]` handler).
        #[allow(clippy::redundant_iter_cloned)]
        let node_states: Vec<std::sync::Arc<nexora_core::graph::NodeReadState>> =
            futures::stream::iter(node_ids.iter().cloned().map(|qid| async move {
                graph
                    .read_node_state(&qid)
                    .await
                    .map_err(|e| CypherError::Execution(format!("Failed to read node: {e}")))
            }))
            .buffered(SNAPSHOT_READ_CONCURRENCY)
            .try_collect()
            .await?;

        for state in &node_states {
            let mut properties: HashMap<String, PropertyValue> = state
                .properties
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            // Labels are first-class on the node; inject them under "labels" so
            // determine_labels finds them (mirrors the previous behavior, but
            // sourced from the projection rather than a separate get_labels
            // round-trip).
            if !state.labels.is_empty() {
                let label_list: Vec<PropertyValue> = state
                    .labels
                    .iter()
                    .map(|s| PropertyValue::String(s.to_string()))
                    .collect();
                properties.insert("labels".to_string(), PropertyValue::List(label_list));
            }
            let labels = determine_labels(&properties);
            nodes.push(NodeData {
                labels,
                properties,
                outgoing_edges: Vec::new(),
            });
        }

        let mut seen_edges = HashSet::new();
        for (owner_idx, state) in node_states.iter().enumerate() {
            for edge in &state.edges {
                let edge_type = edge.edge_type.to_string();
                rel_types_set.insert(edge_type.clone());
                let Some(&other_idx) = id_to_idx.get(&edge.other.to_hex()) else {
                    continue;
                };
                let (source, target) = if edge.direction.is_out() {
                    (owner_idx as u64, other_idx)
                } else {
                    (other_idx, owner_idx as u64)
                };
                if seen_edges.insert((source, edge_type.clone(), target)) {
                    // Get edge properties from the source node's edge_properties map
                    let Some(target_node_id) = idx_to_id.get(&target) else {
                        tracing::warn!("Missing target node ID for index {}", target);
                        continue;
                    };
                    let edge_key = (edge.edge_type.clone(), target_node_id.clone());

                    let properties = state
                        .edge_properties
                        .get(&edge_key)
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect::<HashMap<String, PropertyValue>>();

                    nodes[source as usize].outgoing_edges.push(EdgeData {
                        edge_type,
                        target_idx: target,
                        properties,
                    });
                }
            }
        }

        // Filter rel_types to those used in the query (optimization)
        let rel_types: Vec<String> = if needed_rel_types.is_empty() {
            rel_types_set.into_iter().collect()
        } else {
            needed_rel_types
        };

        tracing::info!(
            "Cypher snapshot: {} nodes, {} rel types",
            nodes.len(),
            rel_types.len()
        );

        Ok(Self {
            nodes,
            idx_to_id,
            rel_types,
        })
    }
}

/// Implement cypher-parser's GraphProvider trait.
/// Uses u64 indices as NodeId (Copy-compatible).
impl GraphProvider for NexoraGraphSnapshot {
    type NodeId = u64;

    fn scan(&self, labels: &[String]) -> Vec<u64> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| labels.is_empty() || labels.iter().any(|l| node.labels.contains(l)))
            .map(|(i, _)| i as u64)
            .collect()
    }

    fn matches_label(&self, node: u64, label: &str) -> bool {
        self.nodes
            .get(node as usize)
            .is_some_and(|n| n.labels.iter().any(|l| l == label))
    }

    fn relationship_types(&self) -> Vec<String> {
        self.rel_types.clone()
    }

    fn expand(&self, node: u64, rel_type: &str) -> Vec<u64> {
        self.nodes
            .get(node as usize)
            .map(|n| {
                n.outgoing_edges
                    .iter()
                    .filter(|edge| edge.edge_type == rel_type)
                    .map(|edge| edge.target_idx)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn rel_sources(&self, rel_type: &str) -> Vec<u64> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                n.outgoing_edges
                    .iter()
                    .any(|edge| edge.edge_type == rel_type)
            })
            .map(|(i, _)| i as u64)
            .collect()
    }

    fn property(&self, node: u64, prop: &str) -> CpValue {
        self.nodes
            .get(node as usize)
            .and_then(|n| n.properties.get(prop))
            .map(pv_to_cp)
            .unwrap_or(CpValue::Null)
    }

    fn node_id(&self, node: u64) -> String {
        self.idx_to_id
            .get(&node)
            .map(|id| id.to_hex())
            .unwrap_or_default()
    }

    fn label(&self, node: u64) -> String {
        self.nodes
            .get(node as usize)
            .and_then(|n| n.labels.first())
            .cloned()
            .unwrap_or_else(|| "Node".into())
    }

    fn name(&self, node: u64) -> String {
        self.nodes
            .get(node as usize)
            .and_then(|n| n.properties.get("name"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.node_id(node))
    }
}

// ============================================================
// Helper functions
// ============================================================

fn determine_labels(props: &HashMap<String, PropertyValue>) -> Vec<String> {
    // P0.1: Labels are first-class on NodeTask.labels, not a synthetic property.
    // The executor receives properties via SnapshotState which now includes the
    // labels field. If labels arrived in-band (e.g. via the "labels" key in a
    // legacy snapshot), parse them; otherwise fall back to "type" or "Node".
    if let Some(PropertyValue::List(labels)) = props.get("labels") {
        return labels
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
    }
    if let Some(PropertyValue::String(t)) = props.get("type") {
        return vec![t.clone()];
    }
    vec!["Node".into()]
}

/// H-4: Validate pattern depth to prevent query complexity attacks.
/// Checks the depth of MATCH patterns like (a)-[]->()-[]->()...->()
fn validate_pattern_depth(query: &cypher_parser::ast::Query, max_depth: usize) -> Result<(), CypherError> {
    for clause in &query.clauses {
        if let cypher_parser::ast::Clause::Match(m) = clause {
            for pattern in &m.patterns {
                // pattern.rest contains the chain of (relationship, node) pairs
                // Depth = 1 (start node) + number of hops
                let depth = 1 + pattern.rest.len();
                if depth > max_depth {
                    return Err(CypherError::Validation(format!(
                        "Pattern depth {} exceeds maximum allowed depth {}. \
                        Deep patterns like MATCH (a)-[]->()-[]->()...->() can cause performance issues.",
                        depth, max_depth
                    )));
                }
            }
        }
    }
    Ok(())
}

fn extract_rel_types(query: &cypher_parser::ast::Query) -> Vec<String> {
    let mut types = Vec::new();
    for clause in &query.clauses {
        if let cypher_parser::ast::Clause::Match(m) = clause {
            for pattern in &m.patterns {
                for (rel, _) in &pattern.rest {
                    types.extend(rel.types.iter().cloned());
                }
            }
        }
    }
    types
}

/// P0.4: Extract node labels referenced in MATCH patterns so the label
/// index can scope the snapshot to only matching nodes.
fn extract_node_labels(query: &cypher_parser::ast::Query) -> Vec<String> {
    let mut labels = Vec::new();
    for clause in &query.clauses {
        if let cypher_parser::ast::Clause::Match(m) = clause {
            for pattern in &m.patterns {
                // Add labels from the start node
                labels.extend(pattern.start.labels.iter().cloned());
                // Add labels from each subsequent node in the path
                for (_, node) in &pattern.rest {
                    labels.extend(node.labels.iter().cloned());
                }
            }
        }
    }
    labels
}

/// P0.4: Extract a single exact-match property filter from a MATCH clause,
/// e.g. MATCH (n {id: 'X'}) → ("id", String("X")).
/// Returns None if no property filter or multiple filters.
fn extract_node_property_filter(
    query: &cypher_parser::ast::Query,
) -> Option<(String, PropertyValue)> {
    for clause in &query.clauses {
        if let cypher_parser::ast::Clause::Match(m) = clause {
            for pattern in &m.patterns {
                let props = &pattern.start.props;
                if !props.is_empty() {
                    let (prop_name, literal) = &props[0];
                    let pv = literal_to_property_value(literal);
                    return Some((prop_name.clone(), pv));
                }
            }
        }
    }
    None
}

/// P0.4: Convert cypher-parser Literal to PropertyValue
fn literal_to_property_value(lit: &cypher_parser::ast::Literal) -> PropertyValue {
    match lit {
        cypher_parser::ast::Literal::Str(s) => PropertyValue::String(s.clone()),
        cypher_parser::ast::Literal::Int(i) => PropertyValue::Integer(*i),
        cypher_parser::ast::Literal::Bool(b) => PropertyValue::Boolean(*b),
        cypher_parser::ast::Literal::Null => PropertyValue::Null,
    }
}

fn pv_to_cp(v: &PropertyValue) -> CpValue {
    match v {
        PropertyValue::Null => CpValue::Null,
        PropertyValue::Boolean(b) => CpValue::Bool(*b),
        PropertyValue::Integer(i) => CpValue::Int(*i),
        // cypher-parser's CypherValue has no dedicated Float variant, so we
        // serialize Float values as strings to preserve precision.
        // Consumers that need numeric operations can parse the string.
        PropertyValue::Float(f) => CpValue::Str(format!("{f}")),
        PropertyValue::String(s) => CpValue::Str(s.clone()),
        other => CpValue::Str(format!("{other}")),
    }
}

/// Post-process query results to fill in edge properties that cypher-parser doesn't support.
///
/// Detects queries like "MATCH (a)-[r:KNOWS]->(b) RETURN id(a), id(b), r.since"
/// and fills in r.since from the snapshot's EdgeData.
fn fill_edge_properties(
    result: CypherResult,
    snapshot: &NexoraGraphSnapshot,
    query: &str,
) -> Result<CypherResult, CypherError> {
    let CypherResult::Rows { columns, rows } = result else {
        return Ok(result);
    };

    // Detect edge property columns: r.property_name
    let edge_prop_pattern = regex::Regex::new(r"\br\.(\w+)")
        .map_err(|e| CypherError::Execution(format!("Invalid regex pattern: {}", e)))?;
    let edge_props: Vec<(usize, String)> = columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            edge_prop_pattern
                .captures(col)
                .and_then(|cap| cap.get(1))
                .map(|m| (idx, m.as_str().to_string()))
        })
        .collect();

    if edge_props.is_empty() {
        return Ok(CypherResult::Rows { columns, rows });
    }

    // Also detect from query string if column names don't have the pattern
    let query_edge_props: Vec<String> = edge_prop_pattern
        .captures_iter(query)
        .filter_map(|cap| cap.get(1))
        .map(|m| m.as_str().to_string())
        .collect();

    if query_edge_props.is_empty() {
        return Ok(CypherResult::Rows { columns, rows });
    }

    // Parse query to extract relationship pattern and find column indices for from_id/to_id
    // For now, assume standard pattern: id(a) AS from_id, id(b) AS to_id, r.prop
    let from_id_idx = columns
        .iter()
        .position(|c| c.contains("from_id") || c == "id(a)");
    let to_id_idx = columns
        .iter()
        .position(|c| c.contains("to_id") || c == "id(b)");

    if from_id_idx.is_none() || to_id_idx.is_none() {
        return Ok(CypherResult::Rows { columns, rows });
    }

    let from_id_idx = from_id_idx.ok_or_else(|| {
        CypherError::Execution("Missing from_id column in query result".to_string())
    })?;
    let to_id_idx = to_id_idx.ok_or_else(|| {
        CypherError::Execution("Missing to_id column in query result".to_string())
    })?;

    // Extract relationship type from query: -[r:TYPE]->
    let rel_type_pattern = regex::Regex::new(r"-\[r:(\w+)\]->")
        .map_err(|e| CypherError::Execution(format!("Invalid regex pattern: {}", e)))?;
    let rel_type = rel_type_pattern
        .captures(query)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str());

    if rel_type.is_none() {
        return Ok(CypherResult::Rows { columns, rows });
    }

    let rel_type = rel_type.ok_or_else(|| {
        CypherError::Execution("Missing relationship type in query pattern".to_string())
    })?;

    // Fill edge properties for each row
    let new_rows: Vec<Vec<serde_json::Value>> =
        rows.into_iter()
            .map(|mut row| {
                // Get from_id and to_id - handle both string IDs and node objects
                let from_hex = row.get(from_id_idx).and_then(|v| {
                    v.as_str()
                        .or_else(|| v.get("id").and_then(|id| id.as_str()))
                });
                let to_hex = row.get(to_id_idx).and_then(|v| {
                    v.as_str()
                        .or_else(|| v.get("id").and_then(|id| id.as_str()))
                });

                if let (Some(from_hex), Some(to_hex)) = (from_hex, to_hex) {
                    // Find source node index
                    if let Some(&source_idx) = snapshot
                        .idx_to_id
                        .iter()
                        .find(|(_, id)| id.to_hex() == from_hex)
                        .map(|(idx, _)| idx)
                    {
                        // Find target node ID
                        if let Some(target_id) = snapshot
                            .idx_to_id
                            .iter()
                            .find(|(_, id)| id.to_hex() == to_hex)
                            .map(|(_, id)| id)
                        {
                            // Get edge properties from snapshot
                            if let Some(node) = snapshot.nodes.get(source_idx as usize) {
                                let target_idx_opt = snapshot
                                    .idx_to_id
                                    .iter()
                                    .find(|(_, id)| *id == target_id)
                                    .map(|(idx, _)| idx);

                                if let Some(&target_idx) = target_idx_opt {
                                    if let Some(edge) = node.outgoing_edges.iter().find(|e| {
                                        e.edge_type == rel_type && e.target_idx == target_idx
                                    }) {
                                        // Fill each edge property column
                                        for prop_name in &query_edge_props {
                                            // Find column index for this property
                                            let col_idx =
                                                columns.iter().position(|c| c.contains(prop_name));
                                            if let Some(col_idx) = col_idx {
                                                if let Some(pv) = edge.properties.get(prop_name) {
                                                    let json_val = cp_value_to_json(&pv_to_cp(pv));
                                                    row[col_idx] = json_val;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                row
            })
            .collect();

    Ok(CypherResult::Rows {
        columns,
        rows: new_rows,
    })
}

/// Extract AS OF timestamp from a Cypher query string.
/// Supports both numeric timestamps and RFC3339 dates.
pub fn extract_as_of(query: &str) -> Option<u64> {
    let upper = query.to_uppercase();
    let idx = upper.find("AS OF ")?;
    let rest: String = upper[idx + 6..].chars().collect();
    let ts_raw = rest.split_whitespace().next()?;
    // Try numeric first
    if let Ok(n) = ts_raw.parse::<u64>() {
        return Some(n);
    }
    // Date format: 2026-01-01 or 2026-01-01T00:00:00Z
    let digits: String = ts_raw.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() >= 8 {
        // Parse YYYYMMDD[HHMMSS...] to approximate microseconds
        let year: u64 = digits[..4].parse().ok()?;
        let month: u64 = digits[4..6].parse().ok()?;
        let day: u64 = digits[6..8].parse().ok()?;
        let hour: u64 = digits.get(8..10).and_then(|s| s.parse().ok()).unwrap_or(0);
        let min: u64 = digits.get(10..12).and_then(|s| s.parse().ok()).unwrap_or(0);
        let sec: u64 = digits.get(12..14).and_then(|s| s.parse().ok()).unwrap_or(0);
        // Days since epoch approximation (2026-01-01 ≈ 56 years * 365.25 days)
        let days = (year - 1970) * 365 + (month - 1) * 30 + day;
        let micros = days * 86400 * 1_000_000
            + hour * 3600 * 1_000_000
            + min * 60 * 1_000_000
            + sec * 1_000_000;
        return Some(micros);
    }
    None
}

#[cfg(test)]
mod tt_tests {
    use super::*;
    use std::collections::HashMap;
    #[test]
    fn test_numeric() {
        assert_eq!(
            extract_as_of("MATCH (n) AS OF 1700000000000000 RETURN n"),
            Some(1700000000000000)
        );
    }
    #[test]
    fn test_rfc3339() {
        let ts = extract_as_of("MATCH (n) AS OF 2026-01-01T00:00:00Z RETURN n");
        assert!(ts.is_some() && ts.unwrap() > 1700000000, "{:?}", ts);
    }
    #[test]
    fn test_none() {
        assert_eq!(extract_as_of("MATCH (n) RETURN n"), None);
    }

    #[test]
    fn reads_reserved_node_labels() {
        // P0.1: Labels are injected as "labels" key (not "__labels")
        let properties = HashMap::from([(
            "labels".to_owned(),
            PropertyValue::List(vec![PropertyValue::String("Person".to_owned())]),
        )]);
        assert_eq!(determine_labels(&properties), vec!["Person"]);
    }
}
