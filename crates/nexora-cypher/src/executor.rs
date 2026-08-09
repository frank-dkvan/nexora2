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
    // Write dispatch: detect CREATE/MERGE/SET/DELETE/REMOVE via nexora-language's
    // AST parser (cypher-parser below only understands read queries: MATCH/WHERE/
    // RETURN/WITH). Without this, a write query would fall through to the read
    // parser and fail with "expected MATCH". Mirror the dispatch in
    // `crate::execute_cypher` so both entry points support writes.
    if let Ok(ast) = nexora_language::Parser::parse(query) {
        let is_write = ast.clauses.iter().any(|c| {
            matches!(
                c,
                nexora_language::Clause::Create { .. }
                    | nexora_language::Clause::Merge { .. }
                    | nexora_language::Clause::Set { .. }
                    | nexora_language::Clause::Remove { .. }
                    | nexora_language::Clause::Delete { .. }
            )
        });
        if is_write {
            let write_result = crate::write_executor::execute_write(graph, &ast).await?;
            return Ok(CypherResult::Write(write_result));
        }
    }

    // Edge-property bridge: cypher-parser 0.5's GraphProvider has no edge-property
    // API, so `MATCH (a)-[r:T]->(b) RETURN r.km` returns null for r.km. Rewrite the
    // query to also surface the edge's endpoint node objects (id carried through),
    // then fill each r.<prop> column from the snapshot's EdgeData and strip the
    // injected helper columns. No-op for queries without edge-property references.
    let mut edge_rw = plan_edge_property_rewrite(query);

    // Parse the (possibly rewritten) query. If the rewrite broke parsing, fall
    // back to the original query with no edge fill — never regress a query that
    // worked before (it just keeps returning null for edge props, as it did).
    let parsed = match cypher_parser::parse(&edge_rw.query) {
        Ok(p) => p,
        Err(_) if !edge_rw.fills.is_empty() => {
            edge_rw = EdgeRewrite {
                query: query.to_string(),
                fills: Vec::new(),
                post_order: None,
            };
            cypher_parser::parse(query).map_err(|e| CypherError::Parse(e.to_string()))?
        }
        Err(e) => return Err(CypherError::Parse(e.to_string())),
    };

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

        // Post-process: fill in edge properties (cypher-parser can't) using the
        // endpoint helper columns injected by plan_edge_property_rewrite, then
        // strip those helper columns so the caller sees only what they asked for.
        cypher_result = fill_edge_properties_v2(cypher_result, &snapshot, &edge_rw);

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

/// One edge variable whose `r.<prop>` columns must be filled from the snapshot,
/// plus the injected helper columns carrying its endpoint node objects.
struct EdgeFill {
    /// The edge variable this fill is for (e.g. `r`).
    evar: String,
    /// Injected RETURN alias holding the edge's SOURCE node object.
    from_alias: String,
    /// Injected RETURN alias holding the edge's TARGET node object.
    to_alias: String,
    /// Relationship type to match (None = any type on that edge var).
    edge_type: Option<String>,
    /// (result column name, edge property name) pairs to fill.
    targets: Vec<(String, String)>,
}

/// One ORDER BY key, resolved to the injected helper column that carries its
/// value after the fill step.
struct PostOrderKey {
    /// Result column (a `__nx_sortN` helper) to sort by.
    col: String,
    ascending: bool,
}

/// ORDER BY / SKIP / LIMIT taken over from cypher-parser and applied in Rust
/// AFTER the edge-property fill. cypher-parser evaluates edge properties as null
/// (its provider is node-only), so letting it sort/paginate on an edge-property
/// key would order rows by null and return the wrong ones. When a trailing
/// ORDER BY references a bridged edge property we strip these clauses from the
/// executed query and reapply them here on the filled rows.
struct PostOrder {
    keys: Vec<PostOrderKey>,
    skip: Option<usize>,
    limit: Option<usize>,
}

/// Plan for surfacing edge properties past cypher-parser's node-only provider.
struct EdgeRewrite {
    /// Query to actually execute (original, or with helper columns injected).
    query: String,
    /// Edge-property fills to apply after execution; empty = no rewrite.
    fills: Vec<EdgeFill>,
    /// Ordering/pagination to apply in Rust after fill (None = leave to cypher-parser).
    post_order: Option<PostOrder>,
}

/// Byte offset just past the RETURN projection list (i.e. the start of the first
/// of ORDER BY / SKIP / LIMIT after RETURN, or end of query). `None` if no RETURN.
fn return_projection_end(query: &str) -> Option<usize> {
    let return_re = regex::Regex::new(r"(?i)\bRETURN\b").ok()?;
    let ret = return_re.find(query)?;
    let after = ret.end();
    let tail_re = regex::Regex::new(r"(?i)\b(ORDER\s+BY|SKIP|LIMIT)\b").ok()?;
    let rel = tail_re.find(&query[after..]).map(|m| after + m.start());
    Some(rel.unwrap_or(query.len()))
}

/// Analyze a read query and, if it references edge properties on a directed,
/// single-hop, variable-named edge, produce a rewrite that also returns the
/// edge's endpoint node objects (so the fill step can locate the edge). No-op
/// (returns the original query, empty fills) for anything it can't handle safely.
fn plan_edge_property_rewrite(query: &str) -> EdgeRewrite {
    let noop = || EdgeRewrite {
        query: query.to_string(),
        fills: Vec::new(),
        post_order: None,
    };

    let Ok(ast) = nexora_language::Parser::parse(query) else {
        return noop();
    };

    use nexora_language::ast::{Clause, EdgeDirection};

    // UNION and other multi-query shapes: leave untouched.
    if ast
        .clauses
        .iter()
        .any(|c| matches!(c, Clause::Union { .. }))
    {
        return noop();
    }

    // Collect edge var -> (source node var, target node var, edge_type) for
    // directed, single-hop edges with named endpoints and a named edge var.
    let mut edges: Vec<(String, String, String, Option<String>)> = Vec::new(); // (evar, src_var, tgt_var, type)
    for clause in &ast.clauses {
        if let Clause::Match { pattern, .. } = clause {
            for part in &pattern.parts {
                let segs = &part.chain.segments;
                for i in 0..segs.len() {
                    let Some(edge) = &segs[i].edge else { continue };
                    let Some(evar) = &edge.variable else { continue };
                    // Skip variable-length paths.
                    if edge.min_hops.is_some() || edge.max_hops.is_some() {
                        continue;
                    }
                    let Some(next) = segs.get(i + 1) else { continue };
                    let (Some(a), Some(b)) = (&segs[i].node.variable, &next.node.variable) else {
                        continue;
                    };
                    let (src, tgt) = match edge.direction {
                        EdgeDirection::Outgoing => (a.clone(), b.clone()),
                        EdgeDirection::Incoming => (b.clone(), a.clone()),
                        EdgeDirection::Either => continue, // ambiguous storage direction
                    };
                    edges.push((evar.clone(), src, tgt, edge.edge_type.clone()));
                }
            }
        }
    }

    if edges.is_empty() {
        return noop();
    }

    let Some(inject_at) = return_projection_end(query) else {
        return noop();
    };

    let mut fills: Vec<EdgeFill> = Vec::new();
    let mut injection = String::new();
    for (evar, src_var, tgt_var, edge_type) in &edges {
        // Find `evar.prop` references in the query text.
        let prop_re = match regex::Regex::new(&format!(r"\b{}\.(\w+)", regex::escape(evar))) {
            Ok(re) => re,
            Err(_) => continue,
        };
        let mut targets: Vec<(String, String)> = Vec::new();
        for cap in prop_re.captures_iter(query) {
            if let Some(m) = cap.get(1) {
                let prop = m.as_str().to_string();
                let col = format!("{}.{}", evar, prop);
                if !targets.iter().any(|(c, _)| c == &col) {
                    targets.push((col, prop));
                }
            }
        }
        if targets.is_empty() {
            continue;
        }
        let from_alias = format!("__nx_{}_from", evar);
        let to_alias = format!("__nx_{}_to", evar);
        injection.push_str(&format!(
            ", {} AS {}, {} AS {}",
            src_var, from_alias, tgt_var, to_alias
        ));
        fills.push(EdgeFill {
            evar: evar.clone(),
            from_alias,
            to_alias,
            edge_type: edge_type.clone(),
            targets,
        });
    }

    if fills.is_empty() {
        return noop();
    }

    // Layer 2: detect a trailing ORDER BY that sorts on a bridged edge property.
    // cypher-parser's provider is node-only, so it evaluates the edge-property key
    // as null and would order/paginate rows by null (returning the wrong ones). When
    // that happens we take ORDER BY + SKIP + LIMIT over and apply them in Rust AFTER
    // the fill. Only when every sort key is a simple `var.prop` and SKIP/LIMIT (if
    // present) are integer literals; otherwise leave ordering to cypher-parser
    // (post_order = None) and just keep the edge-property fills.
    use nexora_language::ast::Expression;
    let edge_vars: std::collections::HashSet<String> =
        fills.iter().map(|f| f.evar.clone()).collect();

    let simple_prop = |e: &Expression| -> Option<(String, String)> {
        if let Expression::Property(obj, prop) = e {
            if let Expression::Variable(v) = obj.as_ref() {
                return Some((v.clone(), prop.clone()));
            }
        }
        None
    };
    let expr_as_usize = |e: &Expression| -> Option<usize> {
        if let Expression::Literal(nexora_id::PropertyValue::Integer(n)) = e {
            if *n >= 0 {
                return Some(*n as usize);
            }
        }
        None
    };

    // The terminal RETURN clause carries the trailing ORDER BY / SKIP / LIMIT.
    let ret_clause = ast.clauses.iter().rev().find_map(|c| match c {
        Clause::Return {
            order_by,
            skip,
            limit,
            ..
        } => Some((order_by, skip, limit)),
        _ => None,
    });

    let mut sort_injection = String::new();
    let mut post_order: Option<PostOrder> = None;

    if let Some((Some(order_by), skip, limit)) = ret_clause {
        let touches_edge = order_by.items.iter().any(|si| {
            simple_prop(&si.expression)
                .map(|(v, _)| edge_vars.contains(&v))
                .unwrap_or(false)
        });
        let all_simple = order_by
            .items
            .iter()
            .all(|si| simple_prop(&si.expression).is_some());
        let skip_val = skip.as_ref().and_then(&expr_as_usize);
        let limit_val = limit.as_ref().and_then(&expr_as_usize);
        let skip_ok = skip.is_none() || skip_val.is_some();
        let limit_ok = limit.is_none() || limit_val.is_some();

        if touches_edge && all_simple && skip_ok && limit_ok {
            let mut keys: Vec<PostOrderKey> = Vec::new();
            for (i, si) in order_by.items.iter().enumerate() {
                // unwrap-safe: all_simple guaranteed every item is a simple prop.
                let (v, prop) = simple_prop(&si.expression).unwrap();
                let sort_col = format!("__nx_sort{}", i);
                // Surface the sort key as a helper projection column.
                sort_injection.push_str(&format!(", {}.{} AS {}", v, prop, sort_col));
                // Edge-property key: cypher-parser returns null for it, so register a
                // fill target so the helper column carries the real value before we
                // sort. Node-property keys are evaluated by cypher-parser directly.
                if edge_vars.contains(&v) {
                    if let Some(f) = fills.iter_mut().find(|f| f.evar == v) {
                        f.targets.push((sort_col.clone(), prop.clone()));
                    }
                }
                keys.push(PostOrderKey {
                    col: sort_col,
                    ascending: si.ascending,
                });
            }
            post_order = Some(PostOrder {
                keys,
                skip: skip_val,
                limit: limit_val,
            });
        }
    }

    let mut rewritten =
        String::with_capacity(query.len() + injection.len() + sort_injection.len() + 1);
    rewritten.push_str(&query[..inject_at]);
    rewritten.push_str(&injection);
    rewritten.push_str(&sort_injection);
    if post_order.is_none() {
        // Ordering left to cypher-parser: keep the trailing ORDER BY / SKIP / LIMIT.
        // Separate the injected projection from the next keyword so they don't glue
        // together (e.g. `__nx_r_toORDER BY`), which would fail to parse and drop
        // the edge-property fills — nulling the very columns we injected for.
        rewritten.push(' ');
        rewritten.push_str(&query[inject_at..]);
    }
    // When post_order is Some the trailing clause is intentionally dropped from the
    // executed query; we reapply ordering/pagination in Rust after the fill.

    EdgeRewrite {
        query: rewritten,
        fills,
        post_order,
    }
}

/// Extract a node's hex id from a result cell that is either a bare hex string
/// or a node object `{"id": "<hex>", ...}`.
fn cell_node_hex(v: &serde_json::Value) -> Option<&str> {
    v.as_str()
        .or_else(|| v.get("id").and_then(|id| id.as_str()))
}

/// Fill edge-property columns from the snapshot using the endpoint helper columns
/// injected by `plan_edge_property_rewrite`, then strip those helper columns.
fn fill_edge_properties_v2(
    result: CypherResult,
    snapshot: &NexoraGraphSnapshot,
    edge_rw: &EdgeRewrite,
) -> CypherResult {
    if edge_rw.fills.is_empty() {
        return result;
    }
    let CypherResult::Rows { columns, mut rows } = result else {
        return result;
    };

    // Resolve column indices for each fill.
    let col_pos = |name: &str| columns.iter().position(|c| c == name);

    for row in rows.iter_mut() {
        for fill in &edge_rw.fills {
            let (Some(from_idx), Some(to_idx)) =
                (col_pos(&fill.from_alias), col_pos(&fill.to_alias))
            else {
                continue;
            };
            let from_hex = row.get(from_idx).and_then(cell_node_hex).map(str::to_string);
            let to_hex = row.get(to_idx).and_then(cell_node_hex).map(str::to_string);
            let (Some(from_hex), Some(to_hex)) = (from_hex, to_hex) else {
                continue;
            };

            // hex -> node index
            let source_idx = snapshot
                .idx_to_id
                .iter()
                .find(|(_, id)| id.to_hex() == from_hex)
                .map(|(idx, _)| *idx);
            let target_idx = snapshot
                .idx_to_id
                .iter()
                .find(|(_, id)| id.to_hex() == to_hex)
                .map(|(idx, _)| *idx);
            let (Some(source_idx), Some(target_idx)) = (source_idx, target_idx) else {
                continue;
            };

            let Some(node) = snapshot.nodes.get(source_idx as usize) else {
                continue;
            };
            let Some(edge) = node.outgoing_edges.iter().find(|e| {
                e.target_idx == target_idx
                    && fill
                        .edge_type
                        .as_ref()
                        .is_none_or(|t| &e.edge_type == t)
            }) else {
                continue;
            };

            for (col_name, prop_name) in &fill.targets {
                if let Some(col_idx) = col_pos(col_name) {
                    if let Some(pv) = edge.properties.get(prop_name) {
                        row[col_idx] = cp_value_to_json(&pv_to_cp(pv));
                    }
                }
            }
        }
    }

    // Layer 2: apply ORDER BY + SKIP + LIMIT in Rust, now that the sort-key helper
    // columns carry real edge-property values. cypher-parser could not do this
    // (it sees edge properties as null), so when a trailing ORDER BY referenced a
    // bridged edge property we stripped those clauses from the executed query and
    // reapply them here on the filled rows.
    if let Some(po) = &edge_rw.post_order {
        let key_idx: Vec<(usize, bool)> = po
            .keys
            .iter()
            .filter_map(|k| col_pos(&k.col).map(|i| (i, k.ascending)))
            .collect();
        if key_idx.len() == po.keys.len() {
            rows.sort_by(|a, b| {
                for &(idx, ascending) in &key_idx {
                    let ord = cmp_json(a.get(idx), b.get(idx));
                    let ord = if ascending { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        // SKIP then LIMIT on the sorted rows.
        if let Some(skip) = po.skip {
            if skip >= rows.len() {
                rows.clear();
            } else {
                rows.drain(0..skip);
            }
        }
        if let Some(limit) = po.limit {
            rows.truncate(limit);
        }
    }

    // Strip every injected helper column (endpoint `__nx_*_from/_to` and sort
    // `__nx_sortN`). Match by the `__nx_` prefix so node-property sort helpers
    // (which are not fill targets) are removed too. Descending index order keeps
    // positions valid as we remove.
    let mut strip: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.starts_with("__nx_"))
        .map(|(i, _)| i)
        .collect();
    strip.sort_unstable();
    strip.dedup();
    let mut columns = columns;
    for &i in strip.iter().rev() {
        columns.remove(i);
        for row in rows.iter_mut() {
            if i < row.len() {
                row.remove(i);
            }
        }
    }

    CypherResult::Rows { columns, rows }
}

/// Total ordering over result cells for Rust-side ORDER BY. Nulls sort last
/// (largest), numbers compare numerically, strings lexically, bools false<true.
/// Mixed/!comparable types fall back to a stable type-rank order so the sort is
/// deterministic rather than panicking.
fn cmp_json(a: Option<&serde_json::Value>, b: Option<&serde_json::Value>) -> std::cmp::Ordering {
    use serde_json::Value;
    use std::cmp::Ordering;
    let rank = |v: Option<&Value>| -> u8 {
        match v {
            Some(Value::Bool(_)) => 0,
            Some(Value::Number(_)) => 1,
            Some(Value::String(_)) => 2,
            Some(Value::Array(_)) => 3,
            Some(Value::Object(_)) => 4,
            // null / absent sort last
            _ => 5,
        }
    };
    match (a, b) {
        (Some(Value::Number(x)), Some(Value::Number(y))) => x
            .as_f64()
            .unwrap_or(f64::NAN)
            .partial_cmp(&y.as_f64().unwrap_or(f64::NAN))
            .unwrap_or(Ordering::Equal),
        (Some(Value::String(x)), Some(Value::String(y))) => x.cmp(y),
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
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
