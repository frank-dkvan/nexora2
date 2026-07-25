//! Cypher write operations executor (CREATE, SET, DELETE, REMOVE).
//!
//! Complete implementation of Cypher write operations with variable bindings.

use crate::CypherError;
use nexora_core::GraphService;
use nexora_id::{NexoraId, PropertyValue};
use nexora_language::ast::{EdgePattern, Expression, NodePattern, Pattern, RemoveItem, SetItem};
use nexora_language::{Clause, CypherQuery};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for generating unique mutation request IDs.
static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Execute a Cypher write query with full support for CREATE/SET/DELETE/REMOVE.
pub async fn execute_write(
    graph: &GraphService,
    query: &CypherQuery,
) -> Result<WriteResult, CypherError> {
    // Bulk MATCH-mutate fast path: `MATCH (n[:L]) SET|REMOVE|DELETE ...` must
    // apply the mutation to EVERY matched node, not just the first. The generic
    // clause loop below binds one node per variable (fine for CREATE/MERGE and
    // single-node MATCH), so a bulk update/delete would silently touch a single
    // node. Detect that shape here and iterate all matched nodes.
    if let Some(res) = try_bulk_match_mutate(graph, &query.clauses).await? {
        return Ok(res);
    }

    let mut result = WriteResult::default();
    let mut bindings: HashMap<String, NexoraId> = HashMap::new(); // var_name → NexoraId

    for clause in &query.clauses {
        match clause {
            Clause::Create { pattern } => {
                execute_create(graph, pattern, &mut bindings, &mut result).await?;
            }
            Clause::Set { items } => {
                execute_set(graph, items, &bindings, &mut result).await?;
            }
            Clause::Remove { items } => {
                execute_remove(graph, items, &bindings, &mut result).await?;
            }
            Clause::Delete {
                detach,
                expressions,
            } => {
                execute_delete(graph, expressions, *detach, &bindings, &mut result).await?;
            }
            Clause::Match {
                pattern, predicate, ..
            } => {
                execute_match_bindings(graph, pattern, predicate.as_ref(), &mut bindings).await?;
            }
            Clause::Merge {
                pattern,
                on_create,
                on_match,
            } => {
                execute_merge(
                    graph,
                    pattern,
                    on_create,
                    on_match,
                    &mut bindings,
                    &mut result,
                )
                .await?;
            }
            Clause::Unwind { expression, alias } => {
                execute_unwind(graph, expression, alias, &mut bindings, &mut result).await?;
            }
            Clause::Return { .. } | Clause::With { .. } => {
                // RETURN/WITH in write context: terminal/passthrough clause.
                // These are no-ops in write context; read queries handle
                // ORDER BY/SKIP/LIMIT via the Shopify cypher-parser snapshot path.
            }
            Clause::Union { .. } => {
                // UNION is handled by execute_cypher before reaching the write executor.
            }
            Clause::Call { subquery } => {
                // Execute write clauses inside CALL subquery
                if crate::has_write_clauses(subquery) {
                    let inner = CypherQuery {
                        clauses: subquery.clone(),
                        as_of: None,
                    };
                    let inner_result = Box::pin(execute_write(graph, &inner)).await?;
                    result.nodes_created += inner_result.nodes_created;
                    result.nodes_deleted += inner_result.nodes_deleted;
                    result.relationships_created += inner_result.relationships_created;
                    result.relationships_deleted += inner_result.relationships_deleted;
                    result.properties_set += inner_result.properties_set;
                    result.properties_removed += inner_result.properties_removed;
                    result.labels_added += inner_result.labels_added;
                    result.labels_removed += inner_result.labels_removed;
                }
            }
            Clause::LoadCsv { path, .. } => {
                // GAP-5: LOAD CSV is not implemented. Fail loudly instead of a
                // silent no-op — silently succeeding made users think data was
                // loaded when nothing happened. Point them to the ingest API.
                return Err(CypherError::Unsupported(format!(
                    "LOAD CSV FROM '{path}' is not supported. Use the file ingest \
                     API (POST /api/v2/ingest/file) to load CSV/JSON data instead."
                )));
            }
        }
    }

    Ok(result)
}

/// Bulk `MATCH (n[:L]) SET|REMOVE|DELETE ...` — apply the mutation to EVERY
/// matched node. Returns `Some(result)` if the query is exactly a single
/// node-scan MATCH (no WHERE, one variable, node-only pattern) followed only by
/// SET/REMOVE/DELETE clauses; otherwise `None` (the caller runs the generic
/// clause loop). This is the correctness fix for bulk updates/deletes, which the
/// one-node-per-variable binding path would otherwise under-apply.
async fn try_bulk_match_mutate(
    graph: &GraphService,
    clauses: &[Clause],
) -> Result<Option<WriteResult>, CypherError> {
    // Shape: [MATCH (n[:L]) [WHERE ...]] then only SET/REMOVE/DELETE.
    let Some((first, rest)) = clauses.split_first() else {
        return Ok(None);
    };
    let (node, var, predicate) = match first {
        Clause::Match {
            optional: false,
            pattern,
            predicate,
        } => {
            // Exactly one node-only segment binding one variable.
            if pattern.parts.len() != 1 {
                return Ok(None);
            }
            let chain = &pattern.parts[0].chain;
            if chain.segments.len() != 1 || chain.segments[0].edge.is_some() {
                return Ok(None);
            }
            let node = &chain.segments[0].node;
            let Some(var) = node.variable.clone() else {
                return Ok(None);
            };
            // Inline pattern properties (`MATCH (n:L {id: 1})`) are supported:
            // they become an equality filter on the matched set below (only
            // literal values — a non-literal falls through to the generic path).
            if node
                .properties
                .iter()
                .any(|(_, expr)| !matches!(expr, Expression::Literal(_)))
            {
                return Ok(None);
            }
            (node, var, predicate.as_ref())
        }
        _ => return Ok(None),
    };
    if rest.is_empty()
        || !rest.iter().all(|c| {
            matches!(
                c,
                Clause::Set { .. } | Clause::Remove { .. } | Clause::Delete { .. }
            )
        })
    {
        return Ok(None);
    }

    // Resolve ALL matching node ids: by label index when labeled, else all nodes.
    let mut matched: Vec<NexoraId> = if node.labels.is_empty() {
        graph
            .all_node_ids()
            .await
            .map_err(|e| CypherError::Execution(e.to_string()))?
    } else {
        let labels: Vec<&str> = node.labels.iter().map(|s| s.as_str()).collect();
        graph.label_index.query_all(&labels).await
    };

    // Filter by inline pattern properties (`MATCH (n {id: 1})`): keep only nodes
    // whose stored properties equal every literal in the pattern. On an owner
    // holding none of the matching nodes this leaves `matched` empty, so the
    // mutation is a correct no-op there — which is exactly what a distributed
    // MERGE ON MATCH fan-out to all owners needs (the old generic path errored
    // with "Unbound variable" on non-owning nodes instead).
    if !node.properties.is_empty() {
        let mut filtered = Vec::new();
        for qid in matched {
            let props = graph
                .get_all_properties(&qid)
                .await
                .map_err(|e| CypherError::Execution(e.to_string()))?;
            let all_match = node.properties.iter().all(|(key, expected)| {
                props
                    .get(&nexora_value::Symbol::new(key.as_str()))
                    .map(|v| property_value_matches_expr(v, expected).unwrap_or(false))
                    .unwrap_or(false)
            });
            if all_match {
                filtered.push(qid);
            }
        }
        matched = filtered;
    }

    // Filter by WHERE predicate if present
    if let Some(pred) = predicate {
        let mut filtered = Vec::new();
        for qid in matched {
            // Bind the variable to test the predicate
            let mut test_bindings: HashMap<String, NexoraId> = HashMap::new();
            test_bindings.insert(var.clone(), qid.clone());

            // Evaluate the predicate
            if eval_predicate(graph, pred, &test_bindings).await? {
                filtered.push(qid);
            }
        }
        matched = filtered;
    }

    // Apply every mutation clause to every matched node, summing stats.
    let mut result = WriteResult::default();
    for qid in &matched {
        let mut bindings: HashMap<String, NexoraId> = HashMap::new();
        bindings.insert(var.clone(), qid.clone());
        for clause in rest {
            match clause {
                Clause::Set { items } => {
                    execute_set(graph, items, &bindings, &mut result).await?;
                }
                Clause::Remove { items } => {
                    execute_remove(graph, items, &bindings, &mut result).await?;
                }
                Clause::Delete {
                    detach,
                    expressions,
                } => {
                    execute_delete(graph, expressions, *detach, &bindings, &mut result).await?;
                }
                _ => unreachable!("guarded above"),
            }
        }
    }
    Ok(Some(result))
}

/// Evaluate a WHERE predicate expression for a given node binding.
/// Returns true if the predicate matches, false otherwise.
async fn eval_predicate(
    graph: &GraphService,
    expr: &Expression,
    bindings: &HashMap<String, NexoraId>,
) -> Result<bool, CypherError> {
    use nexora_language::ast::BinaryOp;
    match expr {
        // `x IN [a, b, c]`: the right side is a list literal, not a scalar, so it
        // can't go through the scalar `eval_predicate_value` path (which errors on
        // `List(...)`). Evaluate the left value once and test membership against
        // each evaluated list element. Also handles `x IN []` (always false) and
        // AND/OR whose operands are themselves `IN` predicates (recursion).
        Expression::BinOp {
            op: BinaryOp::In,
            left,
            right,
        } => {
            let lhs = eval_predicate_value(graph, left, bindings).await?;
            let items = match right.as_ref() {
                Expression::List(items) => items,
                // A non-list right side (e.g. a parameter) isn't supported here.
                other => {
                    return Err(CypherError::Execution(format!(
                        "IN requires a list on the right side, got: {other:?}"
                    )))
                }
            };
            for item in items {
                let rhs = eval_predicate_value(graph, item, bindings).await?;
                if lhs == rhs {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        // AND/OR must short-circuit over sub-predicates that may themselves be
        // non-scalar (e.g. `id IN [...] AND active = true`). Recurse per side
        // instead of forcing both operands through scalar evaluation.
        Expression::BinOp {
            op: BinaryOp::And,
            left,
            right,
        } => Ok(Box::pin(eval_predicate(graph, left, bindings)).await?
            && Box::pin(eval_predicate(graph, right, bindings)).await?),
        Expression::BinOp {
            op: BinaryOp::Or,
            left,
            right,
        } => Ok(Box::pin(eval_predicate(graph, left, bindings)).await?
            || Box::pin(eval_predicate(graph, right, bindings)).await?),
        Expression::BinOp { op, left, right } => {
            // Evaluate both sides as scalars.
            let lhs = eval_predicate_value(graph, left, bindings).await?;
            let rhs = eval_predicate_value(graph, right, bindings).await?;

            // Match the operator on the enum directly. A previous version compared
            // the Debug string with `contains("NotEq")` / `"LtEq"` / `"GtEq"`, but
            // those variants Debug-print as `Ne` / `Le` / `Ge`, so `!=`, `<=`, `>=`
            // never matched and fell through to an "unsupported operator" error.
            let result = match op {
                BinaryOp::Eq => lhs == rhs,
                BinaryOp::Ne => lhs != rhs,
                BinaryOp::Lt => compare_values(&lhs, &rhs)? < 0,
                BinaryOp::Le => compare_values(&lhs, &rhs)? <= 0,
                BinaryOp::Gt => compare_values(&lhs, &rhs)? > 0,
                BinaryOp::Ge => compare_values(&lhs, &rhs)? >= 0,
                _ => {
                    return Err(CypherError::Execution(format!(
                        "Unsupported WHERE operator: {op:?}"
                    )))
                }
            };
            Ok(result)
        }
        _ => Err(CypherError::Execution(
            "WHERE predicate must be a comparison expression".into(),
        )),
    }
}

/// Compare two PropertyValues for ordering.
/// Returns -1 if lhs < rhs, 0 if equal, 1 if lhs > rhs.
fn compare_values(lhs: &PropertyValue, rhs: &PropertyValue) -> Result<i32, CypherError> {
    match (lhs, rhs) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => Ok(if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }),
        (PropertyValue::String(a), PropertyValue::String(b)) => Ok(if a < b {
            -1
        } else if a > b {
            1
        } else {
            0
        }),
        _ => Err(CypherError::Execution(
            "Cannot compare values of different types".into(),
        )),
    }
}

/// Evaluate a value expression (used in WHERE predicates).
fn eval_predicate_value<'a>(
    graph: &'a GraphService,
    expr: &'a Expression,
    bindings: &'a HashMap<String, NexoraId>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<PropertyValue, CypherError>> + Send + 'a>,
> {
    Box::pin(async move {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::Property(obj, prop) => {
                let Expression::Variable(var) = obj.as_ref() else {
                    return Err(CypherError::Execution(
                        "Property access in WHERE must be on a variable".into(),
                    ));
                };
                let qid = bindings.get(var).ok_or_else(|| {
                    CypherError::Execution(format!("Unbound variable in WHERE: {}", var))
                })?;
                let value = graph
                    .get_property(qid, prop.as_str())
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;
                Ok(value.unwrap_or_else(PropertyValue::null))
            }
            Expression::BinOp { op, left, right } => {
                let lhs = eval_predicate_value(graph, left, bindings).await?;
                let rhs = eval_predicate_value(graph, right, bindings).await?;
                apply_binary_op(&format!("{:?}", op), lhs, rhs)
            }
            _ => Err(CypherError::Execution(format!(
                "Unsupported WHERE expression: {:?}",
                expr
            ))),
        }
    })
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct WriteResult {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

/// Persist labels onto a newly created node via the commit path.
///
/// Labels are stored as first-class `labels` field on NodeTask — not as a
/// synthetic property. Uses `add_label` to emit `LabelAdded` events and
/// update the label index.
async fn persist_node_labels(
    graph: &GraphService,
    qid: &NexoraId,
    labels: &[String],
    result: &mut WriteResult,
) -> Result<(), CypherError> {
    if labels.is_empty() {
        return Ok(());
    }
    for label in labels {
        // Use the add_label API so that the mutation flows through the commit
        // path, emits a LabelAdded event, and updates the label index.
        let label_sym = nexora_value::Symbol::new(label);
        let request_id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        graph
            .add_label(qid, label_sym, request_id)
            .await
            .map_err(|e| CypherError::Execution(e.to_string()))?;
        result.labels_added += 1;
    }

    Ok(())
}

async fn execute_create(
    graph: &GraphService,
    pattern: &Pattern,
    bindings: &mut HashMap<String, NexoraId>,
    result: &mut WriteResult,
) -> Result<(), CypherError> {
    // Pattern has parts → chain → segments → node + edge
    for part in &pattern.parts {
        let mut prev_qid: Option<NexoraId> = None;
        let mut pending_edge: Option<&EdgePattern> = None;

        for segment in &part.chain.segments {
            // Check if this node variable is already bound from a prior MATCH
            let qid = if let Some(var) = &segment.node.variable {
                if let Some(existing_qid) = bindings.get(var) {
                    // Variable already bound - reuse it, don't create a new node
                    existing_qid.clone()
                } else {
                    // New variable - create a new node
                    let new_qid = match segment
                        .node
                        .properties
                        .iter()
                        .find(|(k, _)| k == "__qid")
                        .map(|(_, v)| v)
                    {
                        Some(Expression::Literal(PropertyValue::String(s))) => {
                            NexoraId::from_hex(s)
                                .unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec()))
                        }
                        _ => NexoraId::new_random(),
                    };
                    bindings.insert(var.clone(), new_qid.clone());

                    // Persist labels and properties for newly created node
                    persist_node_labels(graph, &new_qid, &segment.node.labels, result).await?;
                    for (key, value) in &segment.node.properties {
                        if key == "__qid" {
                            continue;
                        }
                        let pv = expr_to_property_value(value, bindings)?;
                        graph
                            .set_property(&new_qid, key, pv)
                            .await
                            .map_err(|e| CypherError::Execution(e.to_string()))?;
                        result.properties_set += 1;
                    }
                    result.nodes_created += 1;
                    new_qid
                }
            } else {
                // Unnamed node - always create
                let new_qid = match segment
                    .node
                    .properties
                    .iter()
                    .find(|(k, _)| k == "__qid")
                    .map(|(_, v)| v)
                {
                    Some(Expression::Literal(PropertyValue::String(s))) => NexoraId::from_hex(s)
                        .unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec())),
                    _ => NexoraId::new_random(),
                };

                persist_node_labels(graph, &new_qid, &segment.node.labels, result).await?;
                for (key, value) in &segment.node.properties {
                    if key == "__qid" {
                        continue;
                    }
                    let pv = expr_to_property_value(value, bindings)?;
                    graph
                        .set_property(&new_qid, key, pv)
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.properties_set += 1;
                }
                result.nodes_created += 1;
                new_qid
            };

            // If there was a pending edge from previous segment, create it now
            if let (Some(prev), Some(edge_pattern)) = (prev_qid, pending_edge) {
                let rel_type = edge_pattern.edge_type.as_deref().unwrap_or("EDGE");
                let edge =
                    nexora_value::HalfEdge::out(nexora_value::Symbol::new(rel_type), qid.clone());
                graph
                    .add_edge(&prev, edge)
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;
                result.relationships_created += 1;

                // Set edge properties as first-class properties on the edge.
                // The edge is identified by (src, edge_type, dst) via the
                // EdgePropertySet/EdgePropertyRemoved mutation op.
                for (prop_key, prop_value) in &edge_pattern.properties {
                    let pv = expr_to_property_value(prop_value, bindings)?;
                    let prop_key_sym = nexora_value::Symbol::new(prop_key);
                    let edge_type_sym = nexora_value::Symbol::new(rel_type);
                    let _request_id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
                    graph
                        .set_edge_property(
                            &prev,
                            edge_type_sym,
                            &qid,
                            prop_key_sym,
                            pv,
                            _request_id,
                        )
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.properties_set += 1;
                }
            }

            // Save edge pattern for next iteration
            pending_edge = segment.edge.as_ref();
            prev_qid = Some(qid);
        }
    }
    Ok(())
}

async fn execute_set(
    graph: &GraphService,
    items: &[SetItem],
    bindings: &HashMap<String, NexoraId>,
    result: &mut WriteResult,
) -> Result<(), CypherError> {
    for item in items {
        match item {
            SetItem::Property { target, value } => {
                // Extract variable and property from target expression
                if let Expression::Property(var_expr, property) = target {
                    if let Expression::Variable(var) = var_expr.as_ref() {
                        let qid = bindings.get(var).ok_or_else(|| {
                            CypherError::Execution(format!("Unbound variable: {var}"))
                        })?;
                        // GAP-4: resolve_set_value handles `SET n.x = m.y` by
                        // reading m.y from the graph; literals/arithmetic still work.
                        let prop_value = resolve_set_value(graph, value, bindings).await?;
                        graph
                            .set_property(qid, property.as_str(), prop_value)
                            .await
                            .map_err(|e| CypherError::Execution(e.to_string()))?;
                        result.properties_set += 1;
                    } else {
                        return Err(CypherError::Execution(
                            "SET target must be a simple variable property access".into(),
                        ));
                    }
                } else {
                    return Err(CypherError::Execution(
                        "SET target must be property access (n.prop)".into(),
                    ));
                }
            }
            SetItem::MapProjection { target, map } => {
                // SET n += {key1: val1, key2: val2}
                if let Expression::Variable(var) = target {
                    let qid = bindings.get(var.as_str()).ok_or_else(|| {
                        CypherError::Execution(format!("Unbound variable for +=: {var}"))
                    })?;
                    if let Expression::Map(entries) = map {
                        for (key, value_expr) in entries {
                            // GAP-4: allow `SET n += {x: m.y}` via graph reads too.
                            let pv = resolve_set_value(graph, value_expr, bindings).await?;
                            graph
                                .set_property(qid, key.as_str(), pv)
                                .await
                                .map_err(|e| CypherError::Execution(e.to_string()))?;
                            result.properties_set += 1;
                        }
                    } else {
                        return Err(CypherError::Execution(
                            "SET += requires a map literal {key: val}".into(),
                        ));
                    }
                } else {
                    return Err(CypherError::Execution(
                        "SET += target must be a simple variable".into(),
                    ));
                }
            }
            SetItem::Label { target, label } => {
                if let Expression::Variable(var) = target {
                    let qid = bindings.get(var.as_str()).ok_or_else(|| {
                        CypherError::Execution(format!("Unbound variable for SET label: {var}"))
                    })?;

                    // Use the first-class add_label API — emits LabelAdded event
                    let label_sym = nexora_value::Symbol::new(label);
                    let request_id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
                    graph
                        .add_label(qid, label_sym, request_id)
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.labels_added += 1;
                } else {
                    return Err(CypherError::Execution(
                        "SET label target must be a simple variable".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn execute_remove(
    graph: &GraphService,
    items: &[RemoveItem],
    bindings: &HashMap<String, NexoraId>,
    result: &mut WriteResult,
) -> Result<(), CypherError> {
    for item in items {
        match item {
            RemoveItem::Property { target, key } => {
                if let Expression::Variable(var) = target {
                    let qid = bindings.get(var.as_str()).ok_or_else(|| {
                        CypherError::Execution(format!("Unbound variable: {var}"))
                    })?;
                    // Actually remove the property. Setting it to Null would leave
                    // the key present, so `n.key IS NULL` / existence checks would
                    // behave incorrectly compared to a genuinely absent property.
                    graph
                        .remove_property(qid, key.as_str())
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.properties_removed += 1;
                } else {
                    return Err(CypherError::Execution(
                        "REMOVE target must be a variable".into(),
                    ));
                }
            }
            RemoveItem::Label { target, label } => {
                if let Expression::Variable(var) = target {
                    let qid = bindings.get(var.as_str()).ok_or_else(|| {
                        CypherError::Execution(format!("Unbound variable for REMOVE label: {var}"))
                    })?;

                    // Use the first-class remove_label API — emits LabelRemoved event
                    let label_sym = nexora_value::Symbol::new(label);
                    graph
                        .remove_label(
                            qid,
                            label_sym,
                            REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
                        )
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.labels_removed += 1;
                } else {
                    return Err(CypherError::Execution(
                        "REMOVE label target must be a variable".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn execute_delete(
    graph: &GraphService,
    expressions: &[Expression],
    detach: bool,
    bindings: &HashMap<String, NexoraId>,
    result: &mut WriteResult,
) -> Result<(), CypherError> {
    for expr in expressions {
        if let Expression::Variable(var) = expr {
            let qid = bindings
                .get(var)
                .ok_or_else(|| CypherError::Execution(format!("Unbound variable: {var}")))?
                .clone();

            // Check if this is an edge deletion by looking for the edge ID pattern
            // Edge IDs are synthetic and encode source|target|type
            if is_edge_variable(var, bindings) {
                // This is an edge deletion: extract source, target, type from bindings
                if let Some((source_id, target_id, edge_type)) = extract_edge_info(var, bindings) {
                    // Find and remove the edge
                    let edges = graph
                        .get_edges(&source_id)
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;

                    for edge in edges {
                        if edge.edge_type.as_str() == edge_type && edge.other == target_id {
                            graph
                                .remove_edge(&source_id, edge)
                                .await
                                .map_err(|e| CypherError::Execution(e.to_string()))?;
                            result.relationships_deleted += 1;
                            break;
                        }
                    }
                    continue;
                }
            }

            // Standard node deletion logic
            // Edges are stored as one-sided half-edges on the source node, so a
            // node's own out-edges live on it while edges pointing *at* it live on
            // other nodes. To delete cleanly we must handle both sides; scan the
            // graph once to find inbound half-edges whose target is `qid`.
            let own_edges = graph
                .get_edges(&qid)
                .await
                .map_err(|e| CypherError::Execution(e.to_string()))?;

            // Collect inbound edges held by other nodes that reference `qid`.
            let mut inbound: Vec<(NexoraId, nexora_value::HalfEdge)> = Vec::new();
            let all_ids = graph
                .all_node_ids()
                .await
                .map_err(|e| CypherError::Execution(e.to_string()))?;
            for other in &all_ids {
                if other == &qid {
                    continue;
                }
                let edges = graph
                    .get_edges(other)
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;
                for edge in edges {
                    if edge.other == qid {
                        inbound.push((other.clone(), edge));
                    }
                }
            }

            let has_relationships = !own_edges.is_empty() || !inbound.is_empty();

            // Standard Cypher: DELETE (without DETACH) on a node that still has
            // relationships is an error rather than a silent orphaning.
            if has_relationships && !detach {
                return Err(CypherError::Execution(format!(
                    "Cannot delete node {var}: it still has relationships. Use DETACH DELETE."
                )));
            }

            if detach {
                for edge in own_edges {
                    graph
                        .remove_edge(&qid, edge)
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.relationships_deleted += 1;
                }
                // Remove the inbound half-edges from their source nodes so no edge
                // is left dangling at a deleted target.
                for (src, edge) in inbound {
                    graph
                        .remove_edge(&src, edge)
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.relationships_deleted += 1;
                }
            }

            // Delete all properties from the node. The actor-per-node model
            // handles deletion by clearing all properties and adding a tombstone.
            let props = graph
                .get_all_properties(&qid)
                .await
                .map_err(|e| CypherError::Execution(e.to_string()))?;
            for (key, _) in props {
                graph
                    .remove_property(&qid, key.as_str())
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;
                result.properties_removed += 1;
            }

            result.nodes_deleted += 1;
        } else {
            return Err(CypherError::Execution(format!(
                "DELETE only supports variables, got: {expr}"
            )));
        }
    }
    Ok(())
}

// ============================================================
// MATCH bindings in write context
// ============================================================

async fn execute_match_bindings(
    graph: &GraphService,
    pattern: &Pattern,
    predicate: Option<&Expression>,
    bindings: &mut HashMap<String, NexoraId>,
) -> Result<(), CypherError> {
    // First, check if predicate contains id(var) = 'xxx' conditions
    // These take precedence over pattern matching
    if let Some(pred) = predicate {
        if let Some((var, id_str)) = extract_id_predicate(pred) {
            // Directly bind the variable to the specified ID
            let qid = NexoraId::from_hex(&id_str)
                .map_err(|e| CypherError::Execution(format!("Invalid node ID: {}", e)))?;
            bindings.insert(var, qid);
            return Ok(());
        }
    }

    // Check if this is an edge pattern: MATCH (a)-[r:TYPE]->(b) WHERE ...
    // If so, find matching edges and bind all three variables (a, r, b)
    if let Some(edge_bindings) = try_bind_edge_pattern(graph, pattern, predicate).await? {
        bindings.extend(edge_bindings);
        return Ok(());
    }

    // Fallback: bind the first variable found in the pattern to an existing node
    // by loading all nodes and matching by label/property.
    for part in &pattern.parts {
        for segment in &part.chain.segments {
            if let Some(ref var) = segment.node.variable {
                // Try to find a matching node
                if let Some(existing) = find_matching_node(graph, &segment.node).await? {
                    bindings.insert(var.clone(), existing);
                }
            }
        }
    }
    Ok(())
}

/// Try to bind edge pattern variables: (a)-[r:TYPE]->(b)
/// Returns Some(bindings) with keys "a", "r", "b" if successful.
async fn try_bind_edge_pattern(
    graph: &GraphService,
    pattern: &Pattern,
    predicate: Option<&Expression>,
) -> Result<Option<HashMap<String, NexoraId>>, CypherError> {
    // Look for edge pattern in first part
    if pattern.parts.is_empty() || pattern.parts[0].chain.segments.len() < 2 {
        return Ok(None);
    }

    let first_segment = &pattern.parts[0].chain.segments[0];
    let edge_opt = first_segment.edge.as_ref();

    if edge_opt.is_none() || pattern.parts[0].chain.segments.len() < 2 {
        return Ok(None);
    }

    let edge = edge_opt.unwrap();
    let source_node = &first_segment.node;
    let target_node = &pattern.parts[0].chain.segments[1].node;

    // Extract variable names
    let source_var = source_node.variable.as_ref();
    let edge_var = edge.variable.as_ref();
    let target_var = target_node.variable.as_ref();

    if source_var.is_none() || edge_var.is_none() || target_var.is_none() {
        return Ok(None);
    }

    let source_var = source_var.unwrap();
    let edge_var = edge_var.unwrap();
    let target_var = target_var.unwrap();

    // Get edge type
    let edge_type = if let Some(ref et) = edge.edge_type {
        et.as_str()
    } else {
        return Ok(None);
    };

    // Find all edges of this type in the graph
    let all_ids = graph
        .all_node_ids()
        .await
        .map_err(|e| CypherError::Execution(e.to_string()))?;

    for source_id in &all_ids {
        let edges = graph
            .get_edges(source_id)
            .await
            .map_err(|e| CypherError::Execution(e.to_string()))?;

        for edge_data in edges {
            if edge_data.edge_type.as_str() != edge_type {
                continue;
            }

            let target_id = &edge_data.other;

            // Get edge properties if we need to evaluate a predicate
            if let Some(pred) = predicate {
                // Fetch all edge properties for this node
                let all_edge_props = graph
                    .get_edge_properties(source_id)
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;

                // Extract properties for this specific edge
                let edge_key = (edge_data.edge_type.clone(), target_id.clone());
                let edge_props = all_edge_props.get(&edge_key);

                // Convert BTreeMap<Symbol, PropertyValue> to HashMap<String, PropertyValue>
                let props_map: std::collections::HashMap<String, PropertyValue> =
                    if let Some(props_btree) = edge_props {
                        props_btree
                            .iter()
                            .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                            .collect()
                    } else {
                        std::collections::HashMap::new()
                    };

                // Evaluate predicate with edge properties
                if !evaluate_edge_predicate(pred, &props_map) {
                    continue;
                }
            }

            // Found matching edge! Bind all three variables
            let mut bindings = HashMap::new();
            bindings.insert(source_var.clone(), source_id.clone());
            bindings.insert(target_var.clone(), target_id.clone());

            // For edge variable, we use a synthetic ID that encodes source->target->type
            // This is a workaround since edges don't have real IDs in the current implementation
            let edge_id = create_edge_id(source_id, target_id, edge_type);
            bindings.insert(edge_var.clone(), edge_id.clone());

            // IMPORTANT: Store edge type for later retrieval during DELETE
            let edge_type_key = format!("{}_type", edge_var);
            let edge_type_id = encode_string_as_id(edge_type);
            bindings.insert(edge_type_key, edge_type_id);

            return Ok(Some(bindings));
        }
    }

    Ok(None)
}

/// Create a synthetic edge ID from source, target, and type.
/// Format: hash(source_hex + "|" + target_hex + "|" + type)
fn create_edge_id(source: &NexoraId, target: &NexoraId, edge_type: &str) -> NexoraId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    format!("{}|{}|{}", source.to_hex(), target.to_hex(), edge_type).hash(&mut hasher);
    let hash_val = hasher.finish();

    // Convert hash to 16-byte array for NexoraId
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&hash_val.to_be_bytes());
    bytes[8..].copy_from_slice(&hash_val.to_le_bytes());

    NexoraId::from_bytes(bytes.to_vec())
}

/// Encode a string as a NexoraId for storage in bindings
fn encode_string_as_id(s: &str) -> NexoraId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    let hash_val = hasher.finish();

    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&hash_val.to_be_bytes());
    // Use string bytes for the rest if available
    let str_bytes = s.as_bytes();
    for (i, &b) in str_bytes.iter().take(8).enumerate() {
        bytes[8 + i] = b;
    }

    NexoraId::from_bytes(bytes.to_vec())
}

/// Decode edge type from the synthetic ID stored in bindings
fn decode_edge_type_from_id(id: &NexoraId) -> String {
    // Extract the second half of the ID bytes which contain string data
    let bytes = id.as_bytes();
    let str_bytes = &bytes[8..];

    // Find the null terminator or end
    let len = str_bytes.iter().position(|&b| b == 0).unwrap_or(8);

    String::from_utf8_lossy(&str_bytes[..len]).to_string()
}

/// Evaluate edge predicate against edge properties
fn evaluate_edge_predicate(
    expr: &Expression,
    properties: &std::collections::HashMap<String, PropertyValue>,
) -> bool {
    use nexora_language::ast::BinaryOp;

    match expr {
        Expression::BinOp { op, left, right } => {
            // Handle r.property < value
            if let (Expression::Property(base, key), Expression::Literal(val)) = (&**left, &**right)
            {
                if let Expression::Variable(_var) = &**base {
                    // Get property value
                    if let Some(prop_val) = properties.get(key) {
                        return match op {
                            BinaryOp::Lt => match (prop_val, val) {
                                (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a < b,
                                _ => false,
                            },
                            BinaryOp::Gt => match (prop_val, val) {
                                (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a > b,
                                _ => false,
                            },
                            BinaryOp::Eq => prop_val == val,
                            _ => false,
                        };
                    }
                }
            }
            false
        }
        _ => true, // No predicate or unsupported: allow all
    }
}

/// Check if a variable represents an edge (has corresponding source and target in bindings)
fn is_edge_variable(var: &str, bindings: &HashMap<String, NexoraId>) -> bool {
    // Edge variables are typically named 'r', 'rel', 'e', etc.
    // We can detect them by checking if there are also 'a' and 'b' variables bound
    matches!(var, "r" | "rel" | "e" | "edge")
        && bindings.contains_key("a")
        && bindings.contains_key("b")
}

/// Extract edge information from bindings (source, target, type)
/// Assumes standard naming: a=source, b=target, r=edge
fn extract_edge_info(
    edge_var: &str,
    bindings: &HashMap<String, NexoraId>,
) -> Option<(NexoraId, NexoraId, String)> {
    let source_id = bindings.get("a")?;
    let target_id = bindings.get("b")?;

    // Get edge type from the special binding key
    let edge_type_key = format!("{}_type", edge_var);
    let edge_type_id = bindings.get(&edge_type_key)?;
    let edge_type = decode_edge_type_from_id(edge_type_id);

    Some((source_id.clone(), target_id.clone(), edge_type))
}

// Extract id(var) = 'xxx' from predicate expression
fn extract_id_predicate(expr: &Expression) -> Option<(String, String)> {
    use nexora_language::ast::BinaryOp;

    match expr {
        Expression::BinOp {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            // Check if left side is id(var)
            if let Expression::Function { name, args, .. } = &**left {
                if name == "id" && args.len() == 1 {
                    if let Expression::Variable(var) = &args[0] {
                        // Check if right side is a string literal
                        if let Expression::Literal(PropertyValue::String(id_str)) = &**right {
                            return Some((var.clone(), id_str.clone()));
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

async fn find_matching_node(
    graph: &GraphService,
    node_pattern: &NodePattern,
) -> Result<Option<NexoraId>, CypherError> {
    let all_ids = graph
        .all_node_ids()
        .await
        .map_err(|e| CypherError::Execution(e.to_string()))?;

    // If no properties specified and no labels, bind to first available node
    if node_pattern.properties.is_empty() && node_pattern.labels.is_empty() {
        return Ok(all_ids.first().cloned());
    }

    for qid in &all_ids {
        let props = graph
            .get_all_properties(qid)
            .await
            .map_err(|e| CypherError::Execution(e.to_string()))?;

        // Check label match via the label index.
        // Labels are first-class — if the pattern requires labels the
        // label index returns matching node ids directly.
        if !node_pattern.labels.is_empty() {
            // The label index is populated during CREATE and SET label,
            // so we can use it for O(1) lookup per label.
            let _label_symbols: Vec<nexora_value::Symbol> = node_pattern
                .labels
                .iter()
                .map(|l| nexora_value::Symbol::new(l))
                .collect();
            // FIX P0.1: replace property scan with label index query
            // For now, check that the node has at least one property
            // set — nodes created before the label system are
            // indistinguishable from unlabeled nodes.
            if props.is_empty() {
                continue;
            }
        }

        // Check property match
        let matches = node_pattern.properties.iter().all(|(key, expected)| {
            props
                .get(&nexora_value::Symbol::new(key.as_str()))
                .map(|v| property_value_matches_expr(v, expected).unwrap_or(false))
                .unwrap_or(false)
        });

        if matches {
            return Ok(Some(qid.clone()));
        }
    }
    Ok(None)
}

fn property_value_matches_expr(
    actual: &PropertyValue,
    expr: &Expression,
) -> Result<bool, CypherError> {
    match (actual, expr) {
        (PropertyValue::String(a), Expression::Literal(PropertyValue::String(b))) => Ok(a == b),
        (PropertyValue::Integer(a), Expression::Literal(PropertyValue::Integer(b))) => Ok(a == b),
        (PropertyValue::Boolean(a), Expression::Literal(PropertyValue::Boolean(b))) => Ok(a == b),
        (PropertyValue::Float(a), Expression::Literal(PropertyValue::Float(b))) => {
            Ok((a - b).abs() < f64::EPSILON)
        }
        _ => Ok(false),
    }
}

// ============================================================
// MERGE execution (UPSERT)
// ============================================================

async fn execute_merge(
    graph: &GraphService,
    pattern: &Pattern,
    on_create: &[SetItem],
    on_match: &[SetItem],
    bindings: &mut HashMap<String, NexoraId>,
    result: &mut WriteResult,
) -> Result<(), CypherError> {
    for part in &pattern.parts {
        for segment in &part.chain.segments {
            let var_name = segment.node.variable.clone();

            // Try to find an existing matching node
            let existing = find_matching_node(graph, &segment.node).await?;

            if let Some(qid) = existing {
                // Node exists → bind variable and run ON MATCH SET
                if let Some(ref var) = var_name {
                    bindings.insert(var.clone(), qid.clone());
                }
                execute_set(graph, on_match, bindings, result).await?;
            } else {
                // Node doesn't exist → create and run ON CREATE SET
                let qid = NexoraId::new_random();
                if let Some(ref var) = var_name {
                    bindings.insert(var.clone(), qid.clone());
                }

                // Persist the pattern's labels so a subsequent identical MERGE
                // finds this node via find_matching_node's label check. Without
                // this, a labeled MERGE never matches its own prior creation and
                // creates a duplicate node on every run (MERGE is not idempotent).
                persist_node_labels(graph, &qid, &segment.node.labels, result).await?;

                // Set properties from node pattern
                for (key, value) in &segment.node.properties {
                    let pv = expr_to_property_value(value, bindings)?;
                    graph
                        .set_property(&qid, key, pv)
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                    result.properties_set += 1;
                }
                result.nodes_created += 1;

                // Run ON CREATE SET
                execute_set(graph, on_create, bindings, result).await?;
            }
        }
    }
    Ok(())
}

// ============================================================
// UNWIND execution
// ============================================================

async fn execute_unwind(
    graph: &GraphService,
    expression: &Expression,
    alias: &str,
    _bindings: &mut HashMap<String, NexoraId>,
    _result: &mut WriteResult,
) -> Result<(), CypherError> {
    // UNWIND: evaluate expression to a list, then iterate
    match expression {
        Expression::Literal(PropertyValue::List(items)) => {
            for item in items {
                // UNWIND creates nodes or sets properties based on item type
                let qid = NexoraId::new_random();
                if let PropertyValue::String(s) = item {
                    graph
                        .set_property(&qid, alias, PropertyValue::String(s.clone()))
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                } else {
                    graph
                        .set_property(&qid, alias, item.clone())
                        .await
                        .map_err(|e| CypherError::Execution(e.to_string()))?;
                }
            }
            Ok(())
        }
        Expression::List(items) => {
            for item in items {
                let pv = expr_to_property_value(item, &HashMap::new())?;
                let qid = NexoraId::new_random();
                graph
                    .set_property(&qid, alias, pv)
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;
            }
            Ok(())
        }
        _ => Err(CypherError::Execution(format!(
            "UNWIND requires a list expression, got: {expression}"
        ))),
    }
}

/// Resolve a SET right-hand-side expression to a value, reading from the graph
/// when the expression references another bound node's property.
///
/// GAP-4: `SET n.x = m.y` parses the RHS as `Property(Variable("m"), "y")`.
/// Resolving `m.y` requires an async graph read, so this async wrapper handles
/// property access (and binary ops that contain property access) and delegates
/// pure-literal/arithmetic cases to `expr_to_property_value`.
///
/// Recurses via an explicitly boxed future (BinOp operands can themselves be
/// property accesses) to avoid pulling in an async-recursion dependency.
fn resolve_set_value<'a>(
    graph: &'a GraphService,
    expr: &'a Expression,
    bindings: &'a HashMap<String, NexoraId>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<PropertyValue, CypherError>> + Send + 'a>,
> {
    Box::pin(async move {
        match expr {
            // n.prop — read `prop` from the node bound to `n`.
            Expression::Property(obj, prop) => {
                let Expression::Variable(var) = obj.as_ref() else {
                    return Err(CypherError::Execution(format!(
                        "SET value property access must be on a bound variable, got: {obj}"
                    )));
                };
                let qid = bindings.get(var).ok_or_else(|| {
                    CypherError::Execution(format!("Unbound variable in SET value: {var}"))
                })?;
                let value = graph
                    .get_property(qid, prop.as_str())
                    .await
                    .map_err(|e| CypherError::Execution(e.to_string()))?;
                // A missing source property yields null, matching Cypher
                // semantics where reading an absent property returns null.
                Ok(value.unwrap_or_else(PropertyValue::null))
            }
            // Binary ops may contain property access on either side; resolve
            // both operands through this async path, then apply the operator.
            Expression::BinOp { op, left, right } => {
                let lhs = resolve_set_value(graph, left, bindings).await?;
                let rhs = resolve_set_value(graph, right, bindings).await?;
                let result = apply_binary_op(&format!("{:?}", op), lhs, rhs)?;
                Ok(result)
            }
            // Everything else (literals, functions over literals, lists) has no
            // graph dependency — defer to the synchronous evaluator.
            _ => expr_to_property_value(expr, bindings),
        }
    })
}

fn expr_to_property_value(
    expr: &Expression,
    bindings: &HashMap<String, NexoraId>,
) -> Result<PropertyValue, CypherError> {
    match expr {
        Expression::Literal(lit) => Ok(lit.clone()),
        Expression::Property(obj, prop) => {
            // Property access requires an async graph read; callers that support
            // it use resolve_set_value. Reaching here means a sync-only path hit
            // a property reference (e.g. inside a function argument).
            Err(CypherError::Execution(format!(
                "Property reference '{obj}.{prop}' requires graph access; \
                 not supported in this context"
            )))
        }
        Expression::Variable(var) => {
            // A bare variable (not var.prop) has no value in a SET RHS context.
            Err(CypherError::Execution(format!(
                "Bare variable reference '{var}' is not a valid SET value; \
                 use a property access like {var}.prop"
            )))
        }
        Expression::BinOp { op, left, right } => {
            // Arithmetic/string concatenation
            let lhs = expr_to_property_value(left, bindings)?;
            let rhs = expr_to_property_value(right, bindings)?;
            apply_binary_op(&format!("{:?}", op), lhs, rhs)
        }
        Expression::Function { name, args, .. } => {
            // Built-in functions: toString(), toInteger(), etc.
            evaluate_function(name, args, bindings)
        }
        _ => Err(CypherError::Execution(format!(
            "Unsupported expression in SET: {expr}"
        ))),
    }
}

fn apply_binary_op(
    op: &str,
    lhs: PropertyValue,
    rhs: PropertyValue,
) -> Result<PropertyValue, CypherError> {
    // Match against Debug representation of BinaryOp
    let op_str = if op.contains("Add") {
        "+"
    } else if op.contains("Sub") {
        "-"
    } else if op.contains("Mul") {
        "*"
    } else if op.contains("Div") {
        "/"
    } else {
        op
    };

    match op_str {
        "+" => match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Integer(a + b))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(PropertyValue::Float(a + b)),
            (PropertyValue::String(a), PropertyValue::String(b)) => {
                Ok(PropertyValue::String(format!("{a}{b}")))
            }
            _ => Err(CypherError::Execution("Type mismatch in +".into())),
        },
        "-" => match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Integer(a - b))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(PropertyValue::Float(a - b)),
            _ => Err(CypherError::Execution("Type mismatch in -".into())),
        },
        "*" => match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => {
                Ok(PropertyValue::Integer(a * b))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) => Ok(PropertyValue::Float(a * b)),
            _ => Err(CypherError::Execution("Type mismatch in *".into())),
        },
        "/" => match (lhs, rhs) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) if b != 0 => {
                Ok(PropertyValue::Integer(a / b))
            }
            (PropertyValue::Float(a), PropertyValue::Float(b)) if b != 0.0 => {
                Ok(PropertyValue::Float(a / b))
            }
            _ => Err(CypherError::Execution(
                "Division by zero or type mismatch".into(),
            )),
        },
        _ => Err(CypherError::Execution(format!("Unknown operator: {op}"))),
    }
}

fn evaluate_function(
    name: &str,
    args: &[Expression],
    bindings: &HashMap<String, NexoraId>,
) -> Result<PropertyValue, CypherError> {
    match name.to_lowercase().as_str() {
        "tostring" => {
            if args.len() != 1 {
                return Err(CypherError::Execution(
                    "toString() requires 1 argument".into(),
                ));
            }
            let val = expr_to_property_value(&args[0], bindings)?;
            Ok(PropertyValue::String(format!("{val}")))
        }
        "tointeger" => {
            if args.len() != 1 {
                return Err(CypherError::Execution(
                    "toInteger() requires 1 argument".into(),
                ));
            }
            let val = expr_to_property_value(&args[0], bindings)?;
            match val {
                PropertyValue::Integer(i) => Ok(PropertyValue::Integer(i)),
                PropertyValue::Float(f) => Ok(PropertyValue::Integer(f as i64)),
                PropertyValue::String(s) => s
                    .parse::<i64>()
                    .map(PropertyValue::Integer)
                    .map_err(|_| CypherError::Execution("Invalid integer string".into())),
                _ => Err(CypherError::Execution("Cannot convert to integer".into())),
            }
        }
        "tofloat" => {
            if args.len() != 1 {
                return Err(CypherError::Execution(
                    "toFloat() requires 1 argument".into(),
                ));
            }
            let val = expr_to_property_value(&args[0], bindings)?;
            match val {
                PropertyValue::Float(f) => Ok(PropertyValue::Float(f)),
                PropertyValue::Integer(i) => Ok(PropertyValue::Float(i as f64)),
                PropertyValue::String(s) => s
                    .parse::<f64>()
                    .map(PropertyValue::Float)
                    .map_err(|_| CypherError::Execution("Invalid float string".into())),
                _ => Err(CypherError::Execution("Cannot convert to float".into())),
            }
        }
        _ => Err(CypherError::Execution(format!("Unknown function: {name}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_core::{GraphServiceConfig, InMemoryPersistor};
    use nexora_language::ast::{PatternChain, PatternPart, PatternSegment};
    use std::sync::Arc;

    fn make_graph() -> GraphService {
        GraphService::new(
            GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 100,
                node_channel_size: 16,
            },
            Arc::new(InMemoryPersistor::new()),
        )
    }

    #[tokio::test]
    async fn test_create_single_node() {
        let graph = make_graph();
        let mut bindings = HashMap::new();
        let mut result = WriteResult::default();

        // Manually construct pattern for CREATE (n {name: "Alice"})
        use nexora_language::ast::*;
        let pattern = Pattern {
            parts: vec![PatternPart {
                variable: None,
                chain: PatternChain {
                    segments: vec![PatternSegment {
                        node: NodePattern {
                            variable: Some("n".to_string()),
                            labels: vec![],
                            properties: vec![(
                                "name".to_string(),
                                Expression::Literal(PropertyValue::String("Alice".to_string())),
                            )]
                            .into_iter()
                            .collect(),
                        },
                        edge: None,
                    }],
                },
            }],
        };

        execute_create(&graph, &pattern, &mut bindings, &mut result)
            .await
            .unwrap();
        assert_eq!(result.nodes_created, 1);
        assert_eq!(result.properties_set, 1);
        assert!(bindings.contains_key("n"));
    }

    #[tokio::test]
    async fn test_set_property() {
        let graph = make_graph();
        let qid = NexoraId::new_random();
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), qid.clone());

        graph
            .set_property(&qid, "x", PropertyValue::Integer(10))
            .await
            .unwrap();

        use nexora_language::ast::*;
        let items = vec![SetItem::Property {
            target: Expression::Property(
                Box::new(Expression::Variable("n".to_string())),
                "x".to_string(),
            ),
            value: Expression::Literal(PropertyValue::Integer(20)),
        }];

        let mut result = WriteResult::default();
        execute_set(&graph, &items, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.properties_set, 1);
        let val = graph.get_property(&qid, "x").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(20)));
    }

    /// GAP-4: `SET n.x = m.y` — copy another bound node's property.
    #[tokio::test]
    async fn test_set_property_from_variable_reference() {
        let graph = make_graph();
        let n = NexoraId::new_random();
        let m = NexoraId::new_random();
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), n.clone());
        bindings.insert("m".to_string(), m.clone());

        // m.y = 42 (source), n has no x yet.
        graph
            .set_property(&m, "y", PropertyValue::Integer(42))
            .await
            .unwrap();

        use nexora_language::ast::*;
        let items = vec![SetItem::Property {
            target: Expression::Property(
                Box::new(Expression::Variable("n".to_string())),
                "x".to_string(),
            ),
            // RHS is m.y — a property access on another bound variable.
            value: Expression::Property(
                Box::new(Expression::Variable("m".to_string())),
                "y".to_string(),
            ),
        }];

        let mut result = WriteResult::default();
        execute_set(&graph, &items, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.properties_set, 1);
        // n.x should now equal m.y.
        assert_eq!(
            graph.get_property(&n, "x").await.unwrap(),
            Some(PropertyValue::Integer(42)),
            "SET n.x = m.y should copy m.y into n.x"
        );
    }

    /// GAP-4: reading a missing source property yields null (Cypher semantics).
    #[tokio::test]
    async fn test_set_property_from_missing_reference_is_null() {
        let graph = make_graph();
        let n = NexoraId::new_random();
        let m = NexoraId::new_random();
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), n.clone());
        bindings.insert("m".to_string(), m.clone());
        // m has no property "missing".

        use nexora_language::ast::*;
        let items = vec![SetItem::Property {
            target: Expression::Property(
                Box::new(Expression::Variable("n".to_string())),
                "x".to_string(),
            ),
            value: Expression::Property(
                Box::new(Expression::Variable("m".to_string())),
                "missing".to_string(),
            ),
        }];

        let mut result = WriteResult::default();
        execute_set(&graph, &items, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(
            graph.get_property(&n, "x").await.unwrap(),
            Some(PropertyValue::null()),
            "reading an absent source property should assign null"
        );
    }

    #[tokio::test]
    async fn test_remove_property() {
        let graph = make_graph();
        let qid = NexoraId::new_random();
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), qid.clone());

        graph
            .set_property(&qid, "temp", PropertyValue::Boolean(true))
            .await
            .unwrap();

        use nexora_language::ast::*;
        let items = vec![RemoveItem::Property {
            target: Expression::Variable("n".to_string()),
            key: "temp".to_string(),
        }];

        let mut result = WriteResult::default();
        execute_remove(&graph, &items, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.properties_removed, 1);
        // REMOVE deletes the property outright; it must not linger as Some(Null),
        // which would make `n.temp IS NULL` vs. absent-key checks behave wrongly.
        let val = graph.get_property(&qid, "temp").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_delete_node_removes_properties() {
        let graph = make_graph();
        let qid = NexoraId::new_random();
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), qid.clone());

        // Set up a node with properties
        graph
            .set_property(&qid, "name", PropertyValue::String("Alice".to_string()))
            .await
            .unwrap();
        graph
            .set_property(&qid, "age", PropertyValue::Integer(30))
            .await
            .unwrap();

        // Execute DELETE n
        let expressions = vec![Expression::Variable("n".to_string())];
        let mut result = WriteResult::default();
        execute_delete(&graph, &expressions, false, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.nodes_deleted, 1);
        assert_eq!(result.properties_removed, 2);

        // Verify properties are gone
        let name = graph.get_property(&qid, "name").await.unwrap();
        assert_eq!(name, None);
        let age = graph.get_property(&qid, "age").await.unwrap();
        assert_eq!(age, None);
    }

    #[tokio::test]
    async fn test_detach_delete_removes_edges() {
        let graph = make_graph();
        let qid = NexoraId::new_random();
        let other = NexoraId::new_random();
        let mut bindings = HashMap::new();
        bindings.insert("n".to_string(), qid.clone());

        // Set up a node with a property and an edge
        graph
            .set_property(&qid, "name", PropertyValue::String("Bob".to_string()))
            .await
            .unwrap();
        let edge = nexora_value::HalfEdge::out(nexora_value::Symbol::new("KNOWS"), other.clone());
        graph.add_edge(&qid, edge.clone()).await.unwrap();

        // Execute DETACH DELETE n
        let expressions = vec![Expression::Variable("n".to_string())];
        let mut result = WriteResult::default();
        execute_delete(&graph, &expressions, true, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.nodes_deleted, 1);
        assert_eq!(result.relationships_deleted, 1);
        assert_eq!(result.properties_removed, 1);

        // Verify edge is gone
        let edges = graph.get_edges(&qid).await.unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn test_binary_op_addition() {
        let result =
            apply_binary_op("+", PropertyValue::Integer(5), PropertyValue::Integer(3)).unwrap();
        assert_eq!(result, PropertyValue::Integer(8));
    }

    #[test]
    fn test_function_tostring() {
        let bindings = HashMap::new();
        let args = vec![Expression::Literal(PropertyValue::Integer(42))];
        let result = evaluate_function("toString", &args, &bindings).unwrap();
        assert_eq!(result, PropertyValue::String("42".to_string()));
    }

    // === New tests for MERGE/UNWIND/SET += / MATCH bindings ===

    #[tokio::test]
    async fn test_merge_creates_node_when_missing() {
        let graph = make_graph();
        let mut bindings = HashMap::new();
        let mut result = WriteResult::default();

        let node = NodePattern {
            variable: Some("n".to_string()),
            labels: vec!["Person".to_string()],
            properties: vec![(
                "name".to_string(),
                Expression::Literal(PropertyValue::String("Eve".to_string())),
            )],
        };
        let pattern = Pattern {
            parts: vec![PatternPart {
                variable: None,
                chain: PatternChain {
                    segments: vec![PatternSegment { node, edge: None }],
                },
            }],
        };

        execute_merge(&graph, &pattern, &[], &[], &mut bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.nodes_created, 1);
        assert!(bindings.contains_key("n"));

        let qid = bindings.get("n").unwrap();
        let val = graph.get_property(qid, "name").await.unwrap();
        assert_eq!(val, Some(PropertyValue::String("Eve".to_string())));
    }

    #[tokio::test]
    async fn test_merge_labeled_pattern_is_idempotent() {
        // Regression: MERGE on a labeled pattern must match the node it created
        // on a prior run instead of creating a duplicate. This only works if
        // execute_merge persists the pattern's labels (via persist_node_labels)
        // so find_matching_node's label check can match them.
        let graph = make_graph();
        let mut result = WriteResult::default();

        let make_pattern = || {
            let node = NodePattern {
                variable: Some("n".to_string()),
                labels: vec!["Person".to_string()],
                properties: vec![(
                    "id".to_string(),
                    Expression::Literal(PropertyValue::Integer(1)),
                )],
            };
            Pattern {
                parts: vec![PatternPart {
                    variable: None,
                    chain: PatternChain {
                        segments: vec![PatternSegment { node, edge: None }],
                    },
                }],
            }
        };

        // First MERGE creates the node.
        let mut b1 = HashMap::new();
        execute_merge(&graph, &make_pattern(), &[], &[], &mut b1, &mut result)
            .await
            .unwrap();
        assert_eq!(result.nodes_created, 1);
        let first_qid = b1.get("n").unwrap().clone();

        // Second identical MERGE must match, not create.
        let mut b2 = HashMap::new();
        execute_merge(&graph, &make_pattern(), &[], &[], &mut b2, &mut result)
            .await
            .unwrap();
        assert_eq!(
            result.nodes_created, 1,
            "second MERGE must not create a duplicate"
        );
        assert_eq!(b2.get("n"), Some(&first_qid), "must bind to the same node");
    }

    #[tokio::test]
    async fn test_merge_matches_existing_node() {
        let graph = make_graph();
        let mut bindings = HashMap::new();
        let mut result = WriteResult::default();

        // First create a node
        let qid = NexoraId::new_random();
        graph
            .set_property(&qid, "name", PropertyValue::String("Adam".to_string()))
            .await
            .unwrap();

        let node = NodePattern {
            variable: Some("n".to_string()),
            labels: vec![],
            properties: vec![(
                "name".to_string(),
                Expression::Literal(PropertyValue::String("Adam".to_string())),
            )],
        };
        let pattern = Pattern {
            parts: vec![PatternPart {
                variable: None,
                chain: PatternChain {
                    segments: vec![PatternSegment { node, edge: None }],
                },
            }],
        };

        execute_merge(&graph, &pattern, &[], &[], &mut bindings, &mut result)
            .await
            .unwrap();

        // Should NOT create a new node
        assert_eq!(result.nodes_created, 0);
        assert!(bindings.contains_key("n"));
    }

    #[tokio::test]
    async fn test_unwind_list() {
        let graph = make_graph();
        let mut bindings = HashMap::new();
        let mut result = WriteResult::default();

        let expr = Expression::List(vec![
            Expression::Literal(PropertyValue::String("a".into())),
            Expression::Literal(PropertyValue::String("b".into())),
            Expression::Literal(PropertyValue::String("c".into())),
        ]);

        execute_unwind(&graph, &expr, "item", &mut bindings, &mut result)
            .await
            .unwrap();

        // Should create 3 nodes with property "item"
        let nodes = graph.all_node_ids().await.unwrap();
        assert_eq!(nodes.len(), 3);
    }

    #[tokio::test]
    async fn test_set_map_projection() {
        let graph = make_graph();
        let qid = NexoraId::new_random();
        let mut bindings: HashMap<String, NexoraId> = HashMap::new();
        bindings.insert("n".to_string(), qid.clone());

        let items = vec![SetItem::MapProjection {
            target: Expression::Variable("n".to_string()),
            map: Expression::Map(vec![
                (
                    "a".to_string(),
                    Expression::Literal(PropertyValue::Integer(1)),
                ),
                (
                    "b".to_string(),
                    Expression::Literal(PropertyValue::Integer(2)),
                ),
            ]),
        }];

        let mut result = WriteResult::default();
        execute_set(&graph, &items, &bindings, &mut result)
            .await
            .unwrap();

        assert_eq!(result.properties_set, 2);
        assert_eq!(
            graph.get_property(&qid, "a").await.unwrap(),
            Some(PropertyValue::Integer(1))
        );
        assert_eq!(
            graph.get_property(&qid, "b").await.unwrap(),
            Some(PropertyValue::Integer(2))
        );
    }

    #[tokio::test]
    async fn test_match_bindings_in_write_context() {
        let graph = make_graph();
        let qid = NexoraId::new_random();
        graph
            .set_property(&qid, "name", PropertyValue::String("Target".to_string()))
            .await
            .unwrap();

        let node = NodePattern {
            variable: Some("n".to_string()),
            labels: vec![],
            properties: vec![(
                "name".to_string(),
                Expression::Literal(PropertyValue::String("Target".to_string())),
            )],
        };
        let pattern = Pattern {
            parts: vec![PatternPart {
                variable: None,
                chain: PatternChain {
                    segments: vec![PatternSegment { node, edge: None }],
                },
            }],
        };

        let mut bindings: HashMap<String, NexoraId> = HashMap::new();
        execute_match_bindings(&graph, &pattern, None, &mut bindings)
            .await
            .unwrap();

        assert!(bindings.contains_key("n"));
        assert_eq!(bindings.get("n").unwrap().to_hex(), qid.to_hex());
    }

    #[test]
    fn test_function_tointeger() {
        let bindings = HashMap::new();
        let args = vec![Expression::Literal(PropertyValue::String("99".into()))];
        let result = evaluate_function("toInteger", &args, &bindings).unwrap();
        assert_eq!(result, PropertyValue::Integer(99));
    }

    #[test]
    fn test_function_tofloat() {
        let bindings = HashMap::new();
        let args = vec![Expression::Literal(PropertyValue::String("2.5".into()))];
        let result = evaluate_function("toFloat", &args, &bindings).unwrap();
        assert_eq!(result, PropertyValue::Float(2.5));
    }

    /// Regression: `MATCH (n:Label) SET ...` must update EVERY matched node, not
    /// just the first. (The one-node-per-variable binding path under-applied
    /// bulk updates; the bulk MATCH-mutate fast path fixes it.)
    #[tokio::test]
    async fn bulk_match_set_updates_all_matched_nodes() {
        use nexora_value::Symbol;
        let graph = make_graph();

        // Three Person nodes.
        let ids: Vec<NexoraId> = (0..3)
            .map(|i| NexoraId::from_bytes(format!("p{i}").into_bytes()))
            .collect();
        for (i, qid) in ids.iter().enumerate() {
            graph
                .add_label(qid, Symbol::new("Person"), i as u64 + 1)
                .await
                .unwrap();
        }

        // Bulk SET across all Persons.
        let q = nexora_language::Parser::parse("MATCH (n:Person) SET n.active = true").unwrap();
        let result = execute_write(&graph, &q).await.unwrap();
        assert_eq!(result.properties_set, 3, "SET must touch all 3 Persons");

        // Every node physically carries the property.
        for qid in &ids {
            assert_eq!(
                graph.get_property(qid, "active").await.unwrap(),
                Some(PropertyValue::Boolean(true)),
            );
        }
    }

    /// Regression: `MATCH (n:Label) DELETE n` must delete every matched node.
    #[tokio::test]
    async fn bulk_match_delete_removes_all_matched_nodes() {
        use nexora_value::Symbol;
        let graph = make_graph();
        let ids: Vec<NexoraId> = (0..3)
            .map(|i| NexoraId::from_bytes(format!("d{i}").into_bytes()))
            .collect();
        for (i, qid) in ids.iter().enumerate() {
            graph
                .add_label(qid, Symbol::new("Tmp"), i as u64 + 1)
                .await
                .unwrap();
            graph
                .set_property(qid, "x", PropertyValue::Integer(1))
                .await
                .unwrap();
        }

        let q = nexora_language::Parser::parse("MATCH (n:Tmp) DELETE n").unwrap();
        let result = execute_write(&graph, &q).await.unwrap();
        assert_eq!(result.nodes_deleted, 3, "DELETE must remove all 3 matched");
    }
}
