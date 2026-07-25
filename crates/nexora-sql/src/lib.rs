//! SQL-to-Cypher query translator for Nexora-RS.
//!
//! Uses `sqlparser` for proper SQL parsing, then translates to Cypher.
//!
//! ## Supported SQL Operations
//!
//! ### SELECT
//! - `SELECT * | columns` — column projection
//! - `FROM table` — maps to MATCH (n:table) or MATCH (n)
//! - `WHERE conditions` — filter predicates
//! - `ORDER BY col [ASC|DESC]` — sorting
//! - `LIMIT N` — result limit
//! - `OFFSET N` — result skip (maps to Cypher SKIP)
//! - `GROUP BY col` — grouping (maps to Cypher WITH)
//! - `HAVING cond` — post-aggregation filter
//!
//! ### Aggregation Functions
//! - `COUNT(*)`, `COUNT(col)` → `count(*)`, `count(n.col)`
//! - `SUM(col)` → `sum(n.col)`
//! - `AVG(col)` → `avg(n.col)`
//! - `MIN(col)` → `min(n.col)`
//! - `MAX(col)` → `max(n.col)`
//! - `COLLECT(col)` → `collect(n.col)` (array aggregation)
//!
//! ### WRITE Operations
//! - `INSERT INTO table (col1, col2) VALUES (val1, val2)` → CREATE + SET
//! - `UPDATE table SET col1=val1 WHERE cond` → MATCH + SET
//! - `DELETE FROM table WHERE cond` → DETACH DELETE
//! - `UPSERT` (INSERT ... ON DUPLICATE KEY UPDATE) → MERGE + ON CREATE SET / ON MATCH SET
//!
//! ### EDGE Operations (New in P0.1)
//! - `SELECT * FROM edge_KNOWS` — Query all KNOWS relationships
//! - `SELECT * FROM Person_KNOWS_Person` — Query typed relationships
//! - `INSERT INTO edge_KNOWS (from_id, to_id) VALUES (...)` — Create relationships
//! - `DELETE FROM edge_KNOWS WHERE ...` — Delete relationships

mod edge_table;

pub use edge_table::{classify_table, normalize_table_ident, TableType};

use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    Delete, Expr, GroupByExpr, Insert, OnInsert, Query, SelectItem, SetExpr, Statement,
    TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

// Allowed Cypher functions (whitelist for security)
const ALLOWED_FUNCTIONS: &[&str] = &[
    // Aggregation
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "collect",
    // String
    "tolower",
    "toupper",
    "trim",
    "substring",
    "replace",
    "split",
    "tostring",
    "size",
    "reverse",
    "left",
    "right",
    // Math
    "abs",
    "ceil",
    "floor",
    "round",
    "sqrt",
    "sign",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "atan2",
    "exp",
    "log",
    "log10",
    "pow",
    "rand",
    // Type conversion
    "tointeger",
    "tofloat",
    "toboolean",
    // List
    "head",
    "tail",
    "last",
    "range",
    // Node/Relationship
    "id",
    "type",
    "labels",
    "properties",
    "keys",
    // Temporal (if supported)
    "timestamp",
    "date",
    "datetime",
    "duration",
    "time",
];

fn is_function_allowed(name: &str) -> bool {
    let lowercase = name.to_lowercase();
    ALLOWED_FUNCTIONS.contains(&lowercase.as_str())
}

/// The result of executing a SQL query translated to Cypher.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqlResult {
    /// Column names from the SELECT projection.
    pub columns: Vec<String>,
    /// Row data, each row is a vector of JSON values matching `columns`.
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Total number of rows returned.
    pub row_count: usize,
    /// Wall-clock query execution time in milliseconds.
    pub query_time_ms: u64,
    /// The Cypher query that was executed (translated from the input SQL).
    pub translated_cypher: String,
    /// For write operations: number of rows affected.
    #[serde(default)]
    pub rows_affected: Option<usize>,
}

/// Errors that can occur during SQL translation or execution.
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    /// Failed to parse the input SQL string.
    #[error("parse error: {0}")]
    Parse(String),
    /// The translated Cypher query failed during execution.
    #[error("execution error: {0}")]
    Execution(String),
    /// Unsupported SQL feature.
    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Execute a SQL query against the graph, translating it to Cypher first.
pub async fn execute_sql(
    graph: &nexora_core::GraphService,
    query: &str,
) -> Result<SqlResult, SqlError> {
    let start = std::time::Instant::now();

    // Edge SELECTs with a WHERE clause need SQL-layer filtering. cypher-parser
    // (0.5) exposes only node properties through its GraphProvider, so a
    // relationship-property predicate like `r.since > 2020` pushed into Cypher
    // evaluates to Null and silently drops every row. Fetch the edges with
    // their properties and evaluate the predicate here instead.
    if let Some(res) = try_execute_edge_filtered_select(graph, query, start).await? {
        return Ok(res);
    }

    let (cypher, is_write) = translate_sql_to_cypher(query)?;

    tracing::info!("SQL: {}", query);
    tracing::info!("Translated Cypher: {}", cypher);
    tracing::info!("Is write: {}", is_write);

    if is_write {
        // Execute write operation
        let cypher_result = nexora_cypher::execute_cypher(graph, &cypher)
            .await
            .map_err(|e| SqlError::Execution(e.to_string()))?;

        let rows_affected = match cypher_result {
            nexora_cypher::CypherResult::Write(write_result) => Some(
                write_result.nodes_created
                    + write_result.relationships_created
                    + write_result.nodes_deleted
                    + write_result.relationships_deleted,
            ),
            _ => None,
        };

        Ok(SqlResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            query_time_ms: start.elapsed().as_millis() as u64,
            translated_cypher: cypher,
            rows_affected,
        })
    } else {
        // Execute read operation
        let cypher_result = nexora_cypher::execute_cypher(graph, &cypher)
            .await
            .map_err(|e| SqlError::Execution(e.to_string()))?;

        match cypher_result {
            nexora_cypher::CypherResult::Rows { columns, rows } => Ok(SqlResult {
                columns,
                row_count: rows.len(),
                rows,
                query_time_ms: start.elapsed().as_millis() as u64,
                translated_cypher: cypher,
                rows_affected: None,
            }),
            _ => Ok(SqlResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                query_time_ms: start.elapsed().as_millis() as u64,
                translated_cypher: cypher,
                rows_affected: None,
            }),
        }
    }
}

/// Where an output column of an edge SELECT comes from.
enum EdgeOutCol {
    FromId,
    ToId,
    EdgeType,
    EdgeId,
    /// A relationship property, looked up by name.
    EdgeProp(String),
    /// The aggregate `properties` object (reconstructed from fetched props).
    PropertiesObject,
}

/// Map a projected column name to its edge output source and result column name.
fn classify_edge_out(name: &str) -> (EdgeOutCol, String) {
    match name.to_lowercase().as_str() {
        "from_id" => (EdgeOutCol::FromId, "from_id".to_string()),
        "to_id" => (EdgeOutCol::ToId, "to_id".to_string()),
        "edge_type" | "rel_type" => (EdgeOutCol::EdgeType, "edge_type".to_string()),
        "edge_id" | "rel_id" => (EdgeOutCol::EdgeId, "edge_id".to_string()),
        _ => (EdgeOutCol::EdgeProp(name.to_string()), name.to_string()),
    }
}

/// Try to execute an edge SELECT whose WHERE / ORDER BY touches relationship
/// properties, filtering in the SQL layer.
///
/// cypher-parser (0.5) exposes only node properties through its `GraphProvider`,
/// so a relationship-property predicate pushed into Cypher (`WHERE r.since > 2020`)
/// evaluates to `Null` and silently drops every row. Here we fetch the matching
/// edges together with the referenced properties, then evaluate the predicate,
/// ordering, and paging locally.
///
/// Returns `Ok(None)` whenever this path does not apply (not a rel-typed edge
/// table, no WHERE clause, or a predicate/projection/ordering we cannot evaluate
/// locally) so the caller falls back to the normal translation path.
async fn try_execute_edge_filtered_select(
    graph: &nexora_core::GraphService,
    query: &str,
    start: std::time::Instant,
) -> Result<Option<SqlResult>, SqlError> {
    let dialect = GenericDialect {};
    let ast = match Parser::parse_sql(&dialect, query) {
        Ok(a) => a,
        // Let the normal path surface the parse error verbatim.
        Err(_) => return Ok(None),
    };
    if ast.len() != 1 {
        return Ok(None);
    }
    let q = match &ast[0] {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };
    let select = match &*q.body {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };

    // Single edge table, no joins. A WHERE clause is optional — even without
    // one, routing edge SELECTs here gives clean column names and local
    // ORDER BY, both of which the cypher-parser path gets wrong for edges.
    let selection = select.selection.as_ref();
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    if select.distinct.is_some() || select.having.is_some() {
        return Ok(None);
    }
    match &select.group_by {
        GroupByExpr::Expressions(e, _) if e.is_empty() => {}
        _ => return Ok(None),
    }

    let table_name = select.from[0].relation.to_string();
    // Only rel-typed edge tables: fill_edge_properties keys off `-[r:TYPE]->`,
    // so `_edges` (untyped) can't have its properties resolved and is left to
    // the normal path.
    let match_clause = match classify_table(&table_name) {
        TableType::GenericEdge { rel_type } => format!("MATCH (a)-[r:{}]->(b)", rel_type),
        TableType::TypedEdge {
            source_label,
            rel_type,
            target_label,
        } => format!(
            "MATCH (a:{})-[r:{}]->(b:{})",
            source_label, rel_type, target_label
        ),
        _ => return Ok(None),
    };

    // Bail (before executing) on anything we can't evaluate locally, so we never
    // do the work twice.
    if let Some(sel) = selection {
        if !edge_predicate_supported(sel) {
            return Ok(None);
        }
    }
    // Projection: only wildcard / bare identifiers / aliased identifiers.
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {}
            SelectItem::UnnamedExpr(Expr::Identifier(_)) => {}
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(_),
                ..
            } => {}
            _ => return Ok(None),
        }
    }
    // ORDER BY: only bare identifiers (evaluated locally against fetched props).
    if let Some(ob) = &q.order_by {
        if ob
            .exprs
            .iter()
            .any(|e| !matches!(e.expr, Expr::Identifier(_)))
        {
            return Ok(None);
        }
    }

    // Collect every relationship property referenced by WHERE, ORDER BY, and the
    // projection so the fetch RETURNs them (letting fill_edge_properties resolve
    // each `r.<prop>` column).
    let mut props: Vec<String> = Vec::new();
    if let Some(sel) = selection {
        collect_edge_prop_idents(sel, &mut props);
    }
    if let Some(ob) = &q.order_by {
        for e in &ob.exprs {
            collect_edge_prop_idents(&e.expr, &mut props);
        }
    }
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => push_edge_prop(&id.value, &mut props),
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(id),
                ..
            } => push_edge_prop(&id.value, &mut props),
            _ => {}
        }
    }

    // Fetch all matching edges with their properties (no WHERE in the Cypher).
    let mut returns = vec![
        "id(a) AS from_id".to_string(),
        "id(b) AS to_id".to_string(),
        "type(r) AS edge_type".to_string(),
        "id(r) AS edge_id".to_string(),
    ];
    for p in &props {
        // No `AS` alias: fill_edge_properties (in nexora-cypher) only resolves
        // relationship-property columns whose name literally matches `r.<prop>`.
        // Aliasing them to a bare name makes it skip the fill and leave Null.
        returns.push(format!("r.{}", p));
    }
    let cypher = format!("{} RETURN {}", match_clause, returns.join(", "));

    let cypher_result = nexora_cypher::execute_cypher(graph, &cypher)
        .await
        .map_err(|e| SqlError::Execution(e.to_string()))?;
    let (cols, fetched_rows) = match cypher_result {
        nexora_cypher::CypherResult::Rows { columns, rows } => (columns, rows),
        _ => (Vec::new(), Vec::new()),
    };

    // Resolve a column value for the current row by logical name. Handles both
    // `AS`-aliased columns ("since") and un-aliased ones ("r.since").
    let find_col = |name: &str| -> Option<usize> {
        if let Some(i) = cols.iter().position(|c| c == name) {
            return Some(i);
        }
        let dotted = format!(".{}", name);
        cols.iter()
            .position(|c| c == &format!("r.{}", name) || c.ends_with(&dotted))
    };

    // Filter by the WHERE predicate.
    let mut rows: Vec<Vec<serde_json::Value>> = fetched_rows
        .into_iter()
        .filter(|row| {
            let Some(sel) = selection else {
                return true;
            };
            let get = |name: &str| -> serde_json::Value {
                find_col(name)
                    .and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            };
            eval_edge_predicate(sel, &get)
        })
        .collect();

    // ORDER BY (post-filter, on the resolved property values).
    if let Some(ob) = &q.order_by {
        rows.sort_by(|ra, rb| {
            for e in &ob.exprs {
                if let Expr::Identifier(id) = &e.expr {
                    let va = find_col(&id.value)
                        .and_then(|i| ra.get(i))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let vb = find_col(&id.value)
                        .and_then(|i| rb.get(i))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let mut ord = cmp_json(&va, &vb);
                    if matches!(e.asc, Some(false)) {
                        ord = ord.reverse();
                    }
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // OFFSET then LIMIT (post-filter).
    if let Some(off) = q.offset.as_ref().and_then(|o| expr_to_usize(&o.value)) {
        let drop = off.min(rows.len());
        rows.drain(0..drop);
    }
    if let Some(lim) = q.limit.as_ref().and_then(expr_to_usize) {
        rows.truncate(lim);
    }

    // Project the requested columns.
    let mut out_cols: Vec<String> = Vec::new();
    let mut layout: Vec<EdgeOutCol> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (oc, name) in [
                    (EdgeOutCol::FromId, "from_id"),
                    (EdgeOutCol::ToId, "to_id"),
                    (EdgeOutCol::EdgeType, "edge_type"),
                    (EdgeOutCol::EdgeId, "edge_id"),
                    (EdgeOutCol::PropertiesObject, "properties"),
                ] {
                    layout.push(oc);
                    out_cols.push(name.to_string());
                }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                let (oc, name) = classify_edge_out(&id.value);
                layout.push(oc);
                out_cols.push(name);
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(id),
                alias,
            } => {
                let (oc, _) = classify_edge_out(&id.value);
                layout.push(oc);
                out_cols.push(alias.value.clone());
            }
            _ => return Ok(None),
        }
    }

    let out_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| {
            let get = |name: &str| -> serde_json::Value {
                find_col(name)
                    .and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null)
            };
            layout
                .iter()
                .map(|oc| match oc {
                    EdgeOutCol::FromId => get("from_id"),
                    EdgeOutCol::ToId => get("to_id"),
                    EdgeOutCol::EdgeType => get("edge_type"),
                    EdgeOutCol::EdgeId => get("edge_id"),
                    EdgeOutCol::EdgeProp(p) => get(p),
                    EdgeOutCol::PropertiesObject => {
                        let mut obj = serde_json::Map::new();
                        for p in &props {
                            let v = get(p);
                            if !v.is_null() {
                                obj.insert(p.clone(), v);
                            }
                        }
                        serde_json::Value::Object(obj)
                    }
                })
                .collect()
        })
        .collect();

    Ok(Some(SqlResult {
        columns: out_cols,
        row_count: out_rows.len(),
        rows: out_rows,
        query_time_ms: start.elapsed().as_millis() as u64,
        translated_cypher: cypher,
        rows_affected: None,
    }))
}

/// Add `name` to `out` unless it is a structural edge column or already present.
fn push_edge_prop(name: &str, out: &mut Vec<String>) {
    let lc = name.to_lowercase();
    let structural = matches!(
        lc.as_str(),
        "from_id" | "to_id" | "edge_type" | "rel_type" | "edge_id" | "rel_id"
    );
    if !structural && !out.iter().any(|p| p == name) {
        out.push(name.to_string());
    }
}

/// Collect relationship-property identifiers referenced by an expression.
fn collect_edge_prop_idents(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Identifier(id) => push_edge_prop(&id.value, out),
        Expr::CompoundIdentifier(parts) => {
            if let Some(p) = parts.last() {
                push_edge_prop(&p.value, out);
            }
        }
        Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } => collect_edge_prop_idents(e, out),
        Expr::IsNull(e) | Expr::IsNotNull(e) => collect_edge_prop_idents(e, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_edge_prop_idents(left, out);
            collect_edge_prop_idents(right, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_edge_prop_idents(expr, out);
            for item in list {
                collect_edge_prop_idents(item, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_edge_prop_idents(expr, out);
            collect_edge_prop_idents(low, out);
            collect_edge_prop_idents(high, out);
        }
        Expr::Like { expr, .. } | Expr::ILike { expr, .. } => collect_edge_prop_idents(expr, out),
        _ => {}
    }
}

/// Whether every operand of a predicate is something we can resolve locally.
fn edge_operand_supported(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => true,
        Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } => edge_operand_supported(e),
        _ => false,
    }
}

/// Whether the whole WHERE predicate can be evaluated by [`eval_edge_predicate`].
fn edge_predicate_supported(expr: &Expr) -> bool {
    use sqlparser::ast::BinaryOperator as B;
    match expr {
        Expr::Nested(e) => edge_predicate_supported(e),
        Expr::BinaryOp { left, op, right } => match op {
            B::And | B::Or => edge_predicate_supported(left) && edge_predicate_supported(right),
            B::Eq | B::NotEq | B::Gt | B::Lt | B::GtEq | B::LtEq => {
                edge_operand_supported(left) && edge_operand_supported(right)
            }
            _ => false,
        },
        Expr::IsNull(e) | Expr::IsNotNull(e) => edge_operand_supported(e),
        Expr::InList { expr, list, .. } => {
            edge_operand_supported(expr) && list.iter().all(edge_operand_supported)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            edge_operand_supported(expr)
                && edge_operand_supported(low)
                && edge_operand_supported(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            edge_operand_supported(expr) && matches!(**pattern, Expr::Value(_))
        }
        _ => false,
    }
}

/// Resolve an operand to a JSON value against the current row (via `get`).
fn resolve_edge_value(
    expr: &Expr,
    get: &dyn Fn(&str) -> serde_json::Value,
) -> Option<serde_json::Value> {
    match expr {
        Expr::Identifier(id) => Some(get(&id.value)),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| get(&p.value)),
        Expr::Value(v) => sql_value_to_json(v),
        Expr::Nested(e) => resolve_edge_value(e, get),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => resolve_edge_value(expr, get)
            .as_ref()
            .and_then(json_to_f64)
            .map(|f| serde_json::json!(-f)),
        _ => None,
    }
}

/// Evaluate a supported WHERE predicate against the current row.
fn eval_edge_predicate(expr: &Expr, get: &dyn Fn(&str) -> serde_json::Value) -> bool {
    use sqlparser::ast::BinaryOperator as B;
    use std::cmp::Ordering::{Equal, Greater, Less};
    match expr {
        Expr::Nested(e) => eval_edge_predicate(e, get),
        Expr::BinaryOp { left, op, right } => match op {
            B::And => eval_edge_predicate(left, get) && eval_edge_predicate(right, get),
            B::Or => eval_edge_predicate(left, get) || eval_edge_predicate(right, get),
            B::Eq | B::NotEq | B::Gt | B::Lt | B::GtEq | B::LtEq => {
                let (Some(l), Some(r)) = (
                    resolve_edge_value(left, get),
                    resolve_edge_value(right, get),
                ) else {
                    return false;
                };
                if l.is_null() || r.is_null() {
                    return false;
                }
                let ord = cmp_json(&l, &r);
                match op {
                    B::Eq => ord == Equal,
                    B::NotEq => ord != Equal,
                    B::Gt => ord == Greater,
                    B::Lt => ord == Less,
                    B::GtEq => ord != Less,
                    B::LtEq => ord != Greater,
                    _ => unreachable!(),
                }
            }
            _ => false,
        },
        Expr::IsNull(e) => resolve_edge_value(e, get)
            .map(|v| v.is_null())
            .unwrap_or(true),
        Expr::IsNotNull(e) => resolve_edge_value(e, get)
            .map(|v| !v.is_null())
            .unwrap_or(false),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let Some(v) = resolve_edge_value(expr, get) else {
                return false;
            };
            let found = list.iter().any(|item| {
                resolve_edge_value(item, get)
                    .map(|iv| cmp_json(&v, &iv) == Equal)
                    .unwrap_or(false)
            });
            found ^ negated
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let (Some(v), Some(lo), Some(hi)) = (
                resolve_edge_value(expr, get),
                resolve_edge_value(low, get),
                resolve_edge_value(high, get),
            ) else {
                return false;
            };
            let in_range = cmp_json(&v, &lo) != Less && cmp_json(&v, &hi) != Greater;
            in_range ^ negated
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => eval_edge_like(expr, pattern, *negated, false, get),
        Expr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => eval_edge_like(expr, pattern, *negated, true, get),
        _ => false,
    }
}

/// Evaluate a LIKE / ILIKE predicate against the current row.
fn eval_edge_like(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    get: &dyn Fn(&str) -> serde_json::Value,
) -> bool {
    let (Some(v), Some(p)) = (
        resolve_edge_value(expr, get),
        resolve_edge_value(pattern, get),
    ) else {
        return false;
    };
    let (Some(hay), Some(pat)) = (v.as_str(), p.as_str()) else {
        return false;
    };
    let matched = if case_insensitive {
        like_match(&hay.to_lowercase(), &pat.to_lowercase())
    } else {
        like_match(hay, pat)
    };
    matched ^ negated
}

/// Minimal SQL LIKE matcher supporting `%` at either end (contains / prefix /
/// suffix). Other `%`/`_` positions fall back to a substring test.
fn like_match(haystack: &str, pattern: &str) -> bool {
    let starts = pattern.starts_with('%');
    let ends = pattern.ends_with('%');
    let core = pattern.trim_matches('%');
    match (starts, ends) {
        (true, true) => haystack.contains(core),
        (true, false) => haystack.ends_with(core),
        (false, true) => haystack.starts_with(core),
        (false, false) => haystack == pattern,
    }
}

/// Convert a `sqlparser` literal to a JSON value.
fn sql_value_to_json(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Number(n, _) => n
            .parse::<i64>()
            .map(|i| serde_json::json!(i))
            .or_else(|_| n.parse::<f64>().map(|f| serde_json::json!(f)))
            .ok(),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Some(serde_json::Value::String(s.clone()))
        }
        Value::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        Value::Null => Some(serde_json::Value::Null),
        _ => None,
    }
}

/// Numeric view of a JSON value (numbers, and numeric strings).
fn json_to_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Order two JSON values: numerically when both are numeric, else lexically for
/// strings, with `Null` sorting first.
fn cmp_json(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if let (Some(x), Some(y)) = (json_to_f64(a), json_to_f64(b)) {
        return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
    }
    match (a, b) {
        (serde_json::Value::String(x), serde_json::Value::String(y)) => x.cmp(y),
        (serde_json::Value::Null, serde_json::Value::Null) => Ordering::Equal,
        (serde_json::Value::Null, _) => Ordering::Less,
        (_, serde_json::Value::Null) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

/// Parse a numeric literal expression into a `usize` (for LIMIT / OFFSET).
fn expr_to_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse::<usize>().ok(),
        _ => None,
    }
}

/// Parse SQL and translate to Cypher, returning (cypher, is_write).
pub fn translate_sql_to_cypher(sql: &str) -> Result<(String, bool), SqlError> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;

    if ast.len() != 1 {
        return Err(SqlError::Parse(format!(
            "Expected exactly 1 statement, got {}",
            ast.len()
        )));
    }

    match &ast[0] {
        Statement::Query(q) => {
            let (cypher, _) = translate_select(q)?;
            Ok((cypher, false))
        }
        Statement::Insert(insert) => {
            // Check if it's an UPSERT (INSERT with ON clause)
            let (cypher, _) = if insert.on.is_some() {
                translate_upsert(insert)?
            } else {
                translate_insert(insert)?
            };
            Ok((cypher, true))
        }
        Statement::Update {
            table,
            selection,
            assignments,
            ..
        } => {
            let cypher = translate_update(table, selection, assignments)?;
            Ok((cypher, true))
        }
        Statement::Delete(d) => {
            let cypher = translate_delete(d)?;
            Ok((cypher, true))
        }
        _ => Err(SqlError::Unsupported(format!(
            "Statement type not supported: {:?}",
            ast[0]
        ))),
    }
}

// ============================================================
// SELECT Translation
// ============================================================

fn translate_select(query: &Query) -> Result<(String, Vec<String>), SqlError> {
    let select = match &*query.body {
        SetExpr::Select(s) => s,
        _ => {
            return Err(SqlError::Unsupported(
                "Only SELECT statements supported".into(),
            ))
        }
    };

    // Build MATCH clause - use first table as label
    let (match_clause, _label, is_edge_query) = build_match_clause(&select.from)?;

    // Check for GROUP BY
    let group_by_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            if exprs.is_empty() {
                None
            } else {
                Some(exprs)
            }
        }
        GroupByExpr::All(_) => None,
    };

    let has_group_by = group_by_exprs.is_some();
    let group_by_clause = group_by_exprs
        .as_ref()
        .map(|e| build_group_by_clause(e))
        .transpose()?;

    // Build WHERE clause. This applies in BOTH grouped and non-grouped queries:
    // in Cypher the WHERE filters rows *before* aggregation, exactly like SQL's
    // WHERE (vs HAVING, which filters after). Previously WHERE was dropped for
    // GROUP BY queries, silently ignoring the filter.
    let where_clause = select.selection.as_ref().map(|expr| {
        if is_edge_query {
            format!(" WHERE {}", expr_to_cypher_for_edge(expr))
        } else {
            format!(" WHERE {}", expr_to_cypher(expr))
        }
    });

    // Build ORDER BY clause
    let order_by_clause = match &query.order_by {
        Some(order_by) => build_order_by_clause(&order_by.exprs, is_edge_query, false),
        None => String::new(),
    };

    // Build SKIP/OFFSET clause
    let skip_clause = match &query.offset {
        Some(offset) => format!(" SKIP {}", offset.value),
        None => String::new(),
    };

    // Build LIMIT clause
    let limit_clause = match &query.limit {
        Some(expr) => format!(" LIMIT {}", expr_to_cypher(expr)),
        None => String::new(),
    };

    // Combine into Cypher query
    let cypher = if has_group_by {
        // cypher-parser groups implicitly: a RETURN mixing non-aggregate keys
        // and aggregates groups by the non-aggregate keys. No `WITH <keys>` is
        // needed (the old code emitted a malformed `WITH n.city   RETURN ...`
        // that collapsed every row into one group). `group_by_clause` is still
        // validated for translation errors but doesn't shape the query.
        let _ = &group_by_clause;
        match &select.having {
            Some(having) => {
                // HAVING filters *after* aggregation. cypher-parser rejects an
                // aggregate inside WHERE, so we alias each projected key/aggregate
                // in a WITH stage, filter on the aggregate's alias, then RETURN.
                build_grouped_having_cypher(
                    &match_clause,
                    where_clause.as_deref().unwrap_or(""),
                    &select.projection,
                    having,
                    &order_by_clause,
                    &skip_clause,
                    &limit_clause,
                    is_edge_query,
                )?
            }
            None => {
                let return_clause = build_return_clause(&select.projection, is_edge_query)?;
                format!(
                    "{}{} RETURN {}{}{}{}",
                    match_clause,
                    where_clause.as_deref().unwrap_or(""),
                    return_clause,
                    order_by_clause,
                    skip_clause,
                    limit_clause
                )
            }
        }
    } else {
        // Simple query without GROUP BY - use direct ORDER BY after RETURN
        let return_clause =
            build_return_clause_with_order_by(&select.projection, &query.order_by, is_edge_query)?;
        format!(
            "{}{} RETURN {}{}{}{}",
            match_clause,
            where_clause.as_deref().unwrap_or(""),
            return_clause,
            order_by_clause,
            skip_clause,
            limit_clause
        )
    };

    // Extract column names for result
    let columns = extract_columns_from_projection(&select.projection);

    Ok((cypher, columns))
}

fn build_match_clause(
    tables: &[TableWithJoins],
) -> Result<(String, Option<String>, bool), SqlError> {
    if tables.is_empty() {
        return Ok(("MATCH (n)".to_string(), None, false));
    }

    let table = &tables[0].relation;
    let name = table.to_string();

    // Check if it's a "nodes" table (special case - no label filter)
    if name.to_lowercase() == "nodes" {
        return Ok(("MATCH (n)".to_string(), None, false));
    }

    // Classify table type (node vs edge)
    match classify_table(&name) {
        TableType::Node { label } => {
            // Regular node table
            Ok((format!("MATCH (n:{})", label), Some(label), false))
        }
        TableType::GenericEdge { rel_type } => {
            // edge_KNOWS → MATCH (a)-[r:KNOWS]->(b)
            Ok((format!("MATCH (a)-[r:{}]->(b)", rel_type), None, true))
        }
        TableType::TypedEdge {
            source_label,
            rel_type,
            target_label,
        } => {
            // Person_KNOWS_Person → MATCH (:Person)-[r:KNOWS]->(:Person)
            Ok((
                format!(
                    "MATCH (a:{})-[r:{}]->(b:{})",
                    source_label, rel_type, target_label
                ),
                None,
                true,
            ))
        }
        TableType::AllEdges => {
            // _edges → MATCH (a)-[r]->(b)
            Ok(("MATCH (a)-[r]->(b)".to_string(), None, true))
        }
    }
}

fn build_group_by_clause(group_by: &[Expr]) -> Result<String, SqlError> {
    let parts: Vec<String> = group_by.iter().map(expr_to_cypher).collect();
    Ok(parts.join(", "))
}

fn build_return_clause_with_order_by(
    projection: &[SelectItem],
    order_by: &Option<sqlparser::ast::OrderBy>,
    is_edge_query: bool,
) -> Result<String, SqlError> {
    use std::collections::HashSet;

    // First, build the base return clause
    let base_return = build_return_clause(projection, is_edge_query)?;

    // If no ORDER BY, just return the base
    let order_by = match order_by {
        Some(ob) => ob,
        None => return Ok(base_return),
    };

    // Collect column names already in the projection.
    //
    // A wildcard node projection compiles to a bare `RETURN n`. cypher-parser
    // requires every ORDER BY key to also appear in the projection, so a bare
    // `RETURN n ORDER BY n.age` is rejected. We therefore treat wildcard like an
    // empty projected-column set and let the augmentation below add each ORDER BY
    // key as `n.<col> AS <col>` (yielding `RETURN n, n.age AS age ORDER BY age`).
    let mut projected_cols: HashSet<String> = HashSet::new();
    let mut has_wildcard = false;
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                has_wildcard = true;
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                projected_cols.insert(id.value.clone());
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                projected_cols.insert(alias.value.clone());
            }
            _ => {}
        }
    }
    // For an edge wildcard the base RETURN already spells out from_id/to_id/etc.
    // and edge-property ordering is handled by the SQL-layer edge path, so leave
    // it unchanged. Only the node wildcard needs ORDER BY keys appended.
    if has_wildcard && is_edge_query {
        return Ok(base_return);
    }

    // Collect columns needed by ORDER BY
    let mut order_by_cols: Vec<String> = Vec::new();
    for expr in &order_by.exprs {
        if let Expr::Identifier(id) = &expr.expr {
            let col_name = id.value.clone();
            if !projected_cols.contains(&col_name) {
                order_by_cols.push(col_name);
            }
        }
    }

    // If all ORDER BY columns are already projected, return base
    if order_by_cols.is_empty() {
        return Ok(base_return);
    }

    // Add missing ORDER BY columns to the RETURN clause
    let mut augmented_parts = vec![base_return];
    for col in order_by_cols {
        if is_edge_query {
            augmented_parts.push(format!("r.{} AS {}", col, col));
        } else {
            augmented_parts.push(format!("n.{} AS {}", col, col));
        }
    }

    Ok(augmented_parts.join(", "))
}

fn build_order_by_clause(
    order_by: &[sqlparser::ast::OrderByExpr],
    is_edge_query: bool,
    use_full_refs: bool,
) -> String {
    if order_by.is_empty() {
        return String::new();
    }

    let items: Vec<String> = order_by
        .iter()
        .map(|expr| {
            // For simple column names, decide whether to use aliases or full references
            let col = match &expr.expr {
                // Edge queries have no bare property alias in the RETURN clause
                // (they project from_id/to_id/... or `properties`), so an edge
                // ORDER BY must always reference `r.<prop>` directly.
                Expr::Identifier(ident) if !use_full_refs && !is_edge_query => {
                    // Simple column reference - use as alias (for final ORDER BY)
                    ident.value.clone()
                }
                _ => {
                    // Complex expression or WITH context - translate fully
                    if is_edge_query {
                        expr_to_cypher_for_edge(&expr.expr)
                    } else {
                        expr_to_cypher(&expr.expr)
                    }
                }
            };
            match expr.asc {
                Some(true) | None => col,
                Some(false) => format!("{} DESC", col),
            }
        })
        .collect();

    format!(" ORDER BY {}", items.join(", "))
}

fn build_return_clause(projection: &[SelectItem], is_edge_query: bool) -> Result<String, SqlError> {
    let parts: Vec<String> = projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(Expr::Function(f)) => {
                let func = f.name.to_string().to_lowercase();
                let args = function_args_to_cypher(f);
                let cypher_name = map_function_name(&func);
                format!("{}({})", cypher_name, args)
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                let col_name = &id.value;
                if is_edge_query {
                    // Map edge-specific column names
                    match col_name.as_str() {
                        "from_id" => "id(a) AS from_id".to_string(),
                        "to_id" => "id(b) AS to_id".to_string(),
                        "edge_type" | "rel_type" => "type(r) AS edge_type".to_string(),
                        "edge_id" | "rel_id" => "id(r) AS edge_id".to_string(),
                        _ => format!("r.{}", col_name), // Edge property
                    }
                } else {
                    // Node query
                    match col_name.as_str() {
                        "id" => "id(n) AS id".to_string(),
                        _ => format!("n.{} AS {}", col_name, col_name), // Node property with alias
                    }
                }
            }
            SelectItem::UnnamedExpr(e) => expr_to_cypher(e),
            SelectItem::ExprWithAlias { expr, alias } => {
                format!("{} AS {}", expr_to_cypher(expr), alias.value)
            }
            // Star / wildcard selects. `SELECT *` (Wildcard) and `SELECT t.*`
            // (QualifiedWildcard, as GUI clients emit with a table alias) are
            // equivalent here — the whole node/edge is returned.
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                if is_edge_query {
                    // For edge queries, return relationship with endpoints
                    "id(a) AS from_id, id(b) AS to_id, type(r) AS edge_type, id(r) AS edge_id, properties(r) AS properties".to_string()
                } else {
                    "n".to_string()
                }
            }
        })
        .collect();

    Ok(parts.join(", "))
}

/// Build Cypher for a grouped query that has a HAVING clause.
///
/// cypher-parser rejects an aggregate inside `WHERE`, so HAVING (which filters
/// *after* aggregation) can't be a plain post-RETURN filter. Instead we alias
/// every projected grouping key and aggregate in a `WITH` stage, filter on the
/// aggregate aliases in that stage's `WHERE`, then `RETURN` the aliases. The
/// `WITH` also performs the implicit grouping (grouping keys are the
/// non-aggregate items), so no separate `WITH <keys>` is needed.
///
/// Example:
///   SELECT n.dept, COUNT(*) FROM Employee GROUP BY n.dept HAVING COUNT(*) > 5
/// becomes:
///   MATCH (n:Employee) WITH n.dept AS dept, count(*) AS _agg0
///   WHERE _agg0 > 5 RETURN dept, _agg0
#[allow(clippy::too_many_arguments)]
fn build_grouped_having_cypher(
    match_clause: &str,
    where_clause: &str,
    projection: &[SelectItem],
    having: &Expr,
    order_by_clause: &str,
    skip_clause: &str,
    limit_clause: &str,
    is_edge_query: bool,
) -> Result<String, SqlError> {
    let prefix = if is_edge_query { "r" } else { "n" };

    let mut with_items: Vec<String> = Vec::new();
    let mut return_names: Vec<String> = Vec::new();
    // Map an aggregate's Cypher text (e.g. "count(*)") to the alias it was
    // bound to, so the HAVING predicate can reference the alias instead of
    // repeating the aggregate (which cypher-parser rejects in WHERE).
    let mut agg_aliases: Vec<(String, String)> = Vec::new();
    let mut agg_counter = 0usize;

    let push_aggregate = |agg: String,
                          alias: String,
                          with_items: &mut Vec<String>,
                          return_names: &mut Vec<String>,
                          agg_aliases: &mut Vec<(String, String)>| {
        with_items.push(format!("{agg} AS {alias}"));
        return_names.push(alias.clone());
        agg_aliases.push((agg, alias));
    };

    for item in projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Function(f)) => {
                let func = map_function_name(&f.name.to_string().to_lowercase());
                let agg = format!("{}({})", func, function_args_to_cypher(f));
                let alias = format!("_agg{agg_counter}");
                agg_counter += 1;
                push_aggregate(
                    agg,
                    alias,
                    &mut with_items,
                    &mut return_names,
                    &mut agg_aliases,
                );
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(f),
                alias,
            } => {
                let func = map_function_name(&f.name.to_string().to_lowercase());
                let agg = format!("{}({})", func, function_args_to_cypher(f));
                push_aggregate(
                    agg,
                    alias.value.clone(),
                    &mut with_items,
                    &mut return_names,
                    &mut agg_aliases,
                );
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                with_items.push(format!("{} AS {}", expr_to_cypher(expr), alias.value));
                return_names.push(alias.value.clone());
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                with_items.push(format!("{}.{} AS {}", prefix, id.value, id.value));
                return_names.push(id.value.clone());
            }
            SelectItem::UnnamedExpr(e) => {
                // Compound identifier (n.dept) or other expression: alias by its
                // last path segment so the RETURN name is stable.
                let alias = grouping_key_alias(e);
                with_items.push(format!("{} AS {}", expr_to_cypher(e), alias));
                return_names.push(alias);
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(SqlError::Unsupported(
                    "SELECT * with GROUP BY ... HAVING is not supported; list the grouping \
                     columns and aggregates explicitly"
                        .into(),
                ));
            }
        }
    }

    // Translate the HAVING predicate, then rewrite any aggregate expression to
    // the alias bound in the WITH stage (WHERE cannot contain an aggregate).
    let mut having_cypher = expr_to_cypher(having);
    for (agg, alias) in &agg_aliases {
        having_cypher = having_cypher.replace(agg, alias);
    }

    Ok(format!(
        "{} {} WITH {} WHERE {} RETURN {}{}{}{}",
        match_clause,
        where_clause,
        with_items.join(", "),
        having_cypher,
        return_names.join(", "),
        order_by_clause,
        skip_clause,
        limit_clause
    ))
}

/// Derive a stable RETURN alias for a grouping-key expression: the last path
/// segment of a compound identifier (`n.dept` → `dept`), else a sanitized form.
fn grouping_key_alias(expr: &Expr) -> String {
    match expr {
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| "key".to_string()),
        Expr::Identifier(id) => id.value.clone(),
        _ => "key".to_string(),
    }
}

fn function_args_to_cypher(f: &sqlparser::ast::Function) -> String {
    match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(expr_to_cypher(e))
                }
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    Some("*".to_string())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(", "),
        _ => "*".to_string(),
    }
}

/// Map SQL function names to Cypher equivalents
fn map_function_name(name: &str) -> String {
    match name {
        "upper" => "toUpper".to_string(),
        "lower" => "toLower".to_string(),
        "length" | "len" => "size".to_string(),
        "abs" => "abs".to_string(),
        "collect" | "array_agg" => "collect".to_string(),
        // Aggregation functions are the same in Cypher
        "count" | "sum" | "avg" | "min" | "max" => name.to_string(),
        _ => name.to_string(),
    }
}

fn extract_columns_from_projection(projection: &[SelectItem]) -> Vec<String> {
    projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.clone(),
            SelectItem::UnnamedExpr(Expr::Function(f)) => f.name.to_string().to_lowercase(),
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            _ => "?".to_string(),
        })
        .collect()
}

// ============================================================
// INSERT Translation
// ============================================================

fn translate_insert(insert: &Insert) -> Result<(String, usize), SqlError> {
    let table_name = insert.table_name.to_string();

    // Classify table type
    let table_type = classify_table(&table_name);

    // Extract columns
    let columns: Vec<&str> = insert.columns.iter().map(|c| c.value.as_str()).collect();

    // Handle VALUES clause
    let values_list: Vec<Vec<&Expr>> = match &insert.source {
        Some(source) => match &*source.body {
            SetExpr::Values(values) => values.rows.iter().map(|r| r.iter().collect()).collect(),
            _ => return Err(SqlError::Unsupported("INSERT only supports VALUES".into())),
        },
        None => return Err(SqlError::Unsupported("INSERT requires VALUES".into())),
    };

    match table_type {
        TableType::Node { label } => translate_insert_node(&label, &columns, &values_list),
        TableType::GenericEdge { rel_type } => {
            translate_insert_edge(&rel_type, None, None, &columns, &values_list)
        }
        TableType::TypedEdge {
            source_label,
            rel_type,
            target_label,
        } => translate_insert_edge(
            &rel_type,
            Some(&source_label),
            Some(&target_label),
            &columns,
            &values_list,
        ),
        TableType::AllEdges => Err(SqlError::Unsupported(
            "Cannot INSERT into _edges; use specific edge table".into(),
        )),
    }
}

fn translate_insert_node(
    label: &str,
    columns: &[&str],
    values_list: &[Vec<&Expr>],
) -> Result<(String, usize), SqlError> {
    let mut node_patterns = Vec::new();

    for (row_idx, row_exprs) in values_list.iter().enumerate() {
        let mut properties = Vec::new();
        let mut has_explicit_id = false;

        for (i, expr) in row_exprs.iter().enumerate() {
            if i < columns.len() {
                let col = columns[i];
                let val = expr_to_value_cypher(expr);

                // Check if this column represents a node ID
                // Common patterns: "id", "sensor_id", "vehicle_id", etc.
                let is_id_column = col == "id"
                    || col.ends_with("_id")
                    || col == format!("{}_id", label.to_lowercase());

                if is_id_column && !has_explicit_id {
                    // Extract the raw string value for __qid (will be used as bytes)
                    if let Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) = expr {
                        // Store the string directly - Cypher executor will convert to bytes
                        properties.push(format!("__qid: '{}'", s));
                        has_explicit_id = true;
                    }
                }

                // Always keep the original column as a regular property
                properties.push(format!("{}: {}", col, val));
            }
        }

        // Determine label: use table name if it looks like a label
        let label_clause = if label.to_lowercase() == "nodes" {
            String::new()
        } else {
            format!(":{}", label)
        };

        let property_clause = if properties.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", properties.join(", "))
        };

        // Use unique variable name for each row to avoid "variable already bound" errors
        let var_name = if values_list.len() > 1 {
            format!("n{}", row_idx)
        } else {
            "n".to_string()
        };

        node_patterns.push(format!("({}{}{})", var_name, label_clause, property_clause));
    }

    // Use comma-separated node patterns in a single CREATE clause
    // CREATE (n0:Label {...}), (n1:Label {...}), (n2:Label {...})
    let cypher = format!("CREATE {}", node_patterns.join(", "));
    Ok((cypher, values_list.len()))
}

fn translate_insert_edge(
    rel_type: &str,
    source_label: Option<&str>,
    target_label: Option<&str>,
    columns: &[&str],
    values_list: &[Vec<&Expr>],
) -> Result<(String, usize), SqlError> {
    // Find from_id and to_id columns
    let from_id_idx = columns
        .iter()
        .position(|c| *c == "from_id")
        .ok_or_else(|| SqlError::Unsupported("Edge INSERT requires 'from_id' column".into()))?;

    let to_id_idx = columns
        .iter()
        .position(|c| *c == "to_id")
        .ok_or_else(|| SqlError::Unsupported("Edge INSERT requires 'to_id' column".into()))?;

    let mut cypher_parts = Vec::new();

    for row_exprs in values_list {
        // Extract from_id and to_id
        let from_id = expr_to_value_cypher(row_exprs[from_id_idx]);
        let to_id = expr_to_value_cypher(row_exprs[to_id_idx]);

        // Extract other properties (excluding from_id and to_id)
        let mut properties = Vec::new();
        for (i, expr) in row_exprs.iter().enumerate() {
            if i != from_id_idx && i != to_id_idx && i < columns.len() {
                let col = columns[i];
                let val = expr_to_value_cypher(expr);
                properties.push(format!("{}: {}", col, val));
            }
        }

        // Build MATCH for source and target nodes
        let source_match = if let Some(label) = source_label {
            format!("MATCH (a:{}) WHERE id(a) = {}", label, from_id)
        } else {
            format!("MATCH (a) WHERE id(a) = {}", from_id)
        };

        let target_match = if let Some(label) = target_label {
            format!("MATCH (b:{}) WHERE id(b) = {}", label, to_id)
        } else {
            format!("MATCH (b) WHERE id(b) = {}", to_id)
        };

        // Build CREATE for relationship
        let property_clause = if properties.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", properties.join(", "))
        };

        cypher_parts.push(format!(
            "{} {} CREATE (a)-[r:{}{}]->(b)",
            source_match, target_match, rel_type, property_clause
        ));
    }

    let cypher = cypher_parts.join(" ");
    Ok((cypher, values_list.len()))
}

// ============================================================
// UPDATE Translation
// ============================================================

fn translate_update(
    table: &TableWithJoins,
    selection: &Option<Expr>,
    assignments: &[sqlparser::ast::Assignment],
) -> Result<String, SqlError> {
    // Get table name
    let table_name = table.relation.to_string();

    // Edge tables translate to a relationship MATCH + SET r.<prop>, and must
    // never rewrite the endpoints (from_id/to_id) — that would change graph
    // structure, which UPDATE is not allowed to do.
    let table_type = classify_table(&table_name);
    let is_edge = matches!(
        table_type,
        TableType::GenericEdge { .. } | TableType::TypedEdge { .. } | TableType::AllEdges
    );

    if is_edge {
        for a in assignments {
            let col = assignment_target_to_string(&a.target).to_lowercase();
            if matches!(col.as_str(), "from_id" | "to_id") {
                return Err(SqlError::Unsupported(format!(
                    "cannot UPDATE edge structural column `{col}`; delete and re-insert the edge instead"
                )));
            }
        }

        let set_clause = build_set_clause_with_prefix(assignments, "r");
        let where_clause = match selection {
            Some(expr) => format!(" WHERE {}", expr_to_cypher_for_edge(expr)),
            None => String::new(),
        };
        let match_clause = match table_type {
            TableType::GenericEdge { rel_type } => format!("MATCH (a)-[r:{}]->(b)", rel_type),
            TableType::TypedEdge {
                source_label,
                rel_type,
                target_label,
            } => format!(
                "MATCH (a:{})-[r:{}]->(b:{})",
                source_label, rel_type, target_label
            ),
            TableType::AllEdges => "MATCH (a)-[r]->(b)".to_string(),
            TableType::Node { .. } => unreachable!("is_edge guarantees an edge variant"),
        };
        return Ok(format!("{}{}{}", match_clause, where_clause, set_clause));
    }

    // Node UPDATE.
    let set_clause = build_set_clause_with_prefix(assignments, "n");

    // Determine label filter (without parentheses, just the label name)
    let label_clause = if table_name.to_lowercase() == "nodes" {
        String::new()
    } else {
        format!(":{}", table_name)
    };

    // Build WHERE clause
    let where_clause = match selection {
        Some(expr) => format!(" WHERE {}", expr_to_cypher(expr)),
        None => String::new(),
    };

    let cypher = format!("MATCH (n{}){}{}", label_clause, where_clause, set_clause);

    Ok(cypher)
}

/// Build a `SET var.col = val, ...` clause, prefixing each column with `var`
/// (`n` for nodes, `r` for relationships).
fn build_set_clause_with_prefix(assignments: &[sqlparser::ast::Assignment], var: &str) -> String {
    if assignments.is_empty() {
        return String::new();
    }

    let set_items: Vec<String> = assignments
        .iter()
        .map(|a| {
            let col = assignment_target_to_string(&a.target);
            let val = expr_to_value_cypher(&a.value);
            format!("{}.{} = {}", var, col, val)
        })
        .collect();

    format!(" SET {}", set_items.join(", "))
}

fn assignment_target_to_string(target: &sqlparser::ast::AssignmentTarget) -> String {
    match target {
        sqlparser::ast::AssignmentTarget::ColumnName(name) => name
            .0
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        sqlparser::ast::AssignmentTarget::Tuple(names) => {
            let inner: Vec<String> = names
                .iter()
                .flat_map(|name| name.0.iter().map(|i| i.value.clone()))
                .collect();
            format!("({})", inner.join(", "))
        }
    }
}

// ============================================================
// DELETE Translation
// ============================================================

fn translate_delete(delete: &Delete) -> Result<String, SqlError> {
    // Get table name from tables (MySQL style) or from (Standard SQL)
    let table_name = if !delete.tables.is_empty() {
        delete.tables[0].to_string()
    } else {
        // Get from the FROM clause
        match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                if !tables.is_empty() {
                    tables[0].relation.to_string()
                } else {
                    "nodes".to_string()
                }
            }
        }
    };

    // Classify table type to determine if it's an edge query
    let table_type = classify_table(&table_name);
    let is_edge_query = matches!(
        table_type,
        TableType::GenericEdge { .. } | TableType::TypedEdge { .. } | TableType::AllEdges
    );

    // Build WHERE clause with correct context
    let where_clause = match &delete.selection {
        Some(expr) => {
            if is_edge_query {
                format!(" WHERE {}", expr_to_cypher_for_edge(expr))
            } else {
                format!(" WHERE {}", expr_to_cypher(expr))
            }
        }
        None => {
            return Err(SqlError::Unsupported(
                "DELETE without WHERE clause not supported".into(),
            ))
        }
    };

    match table_type {
        TableType::Node { label } => {
            // Node deletion
            let label_clause = if label.to_lowercase() == "nodes" {
                String::new()
            } else {
                format!(":{}", label)
            };

            // Use DETACH DELETE to remove node and all connected edges
            Ok(format!(
                "MATCH (n{}){} DETACH DELETE n",
                label_clause, where_clause
            ))
        }
        TableType::GenericEdge { rel_type } => {
            // Generic edge deletion: MATCH ()-[r:KNOWS]->() WHERE ... DELETE r
            Ok(format!(
                "MATCH (a)-[r:{}]->(b){} DELETE r",
                rel_type, where_clause
            ))
        }
        TableType::TypedEdge {
            source_label,
            rel_type,
            target_label,
        } => {
            // Typed edge deletion
            Ok(format!(
                "MATCH (a:{})-[r:{}]->(b:{}){} DELETE r",
                source_label, rel_type, target_label, where_clause
            ))
        }
        TableType::AllEdges => {
            // All edges deletion
            Ok(format!("MATCH (a)-[r]->(b){} DELETE r", where_clause))
        }
    }
}

// ============================================================
// UPSERT Translation (MySQL-style INSERT ON DUPLICATE KEY UPDATE)
// ============================================================

/// Build the `ON MATCH SET`-body from an `INSERT`'s ON clause, prefixing each
/// column with `var` (`n` for nodes, `r` for relationships). Returns `None` when
/// there is no update action (a plain conflict-ignore).
fn upsert_on_update_clause(on: Option<&OnInsert>, var: &str) -> Option<String> {
    let assignments = match on? {
        OnInsert::DuplicateKeyUpdate(assignments) => assignments.as_slice(),
        OnInsert::OnConflict(on_conflict) => match &on_conflict.action {
            sqlparser::ast::OnConflictAction::DoUpdate(do_update) => {
                do_update.assignments.as_slice()
            }
            _ => return None,
        },
        #[allow(unreachable_patterns)]
        _ => return None,
    };
    if assignments.is_empty() {
        return None;
    }
    let items: Vec<String> = assignments
        .iter()
        .map(|a| {
            let col = assignment_target_to_string(&a.target);
            let val = expr_to_value_cypher(&a.value);
            format!("{}.{} = {}", var, col, val)
        })
        .collect();
    Some(items.join(", "))
}

fn translate_upsert(insert: &Insert) -> Result<(String, usize), SqlError> {
    let table_name = insert.table_name.to_string();

    // Extract columns
    let columns: Vec<&str> = insert.columns.iter().map(|c| c.value.as_str()).collect();

    // Handle VALUES clause
    let values_list: Vec<Vec<&Expr>> = match &insert.source {
        Some(source) => match &*source.body {
            SetExpr::Values(values) => values.rows.iter().map(|r| r.iter().collect()).collect(),
            _ => return Err(SqlError::Unsupported("UPSERT only supports VALUES".into())),
        },
        None => return Err(SqlError::Unsupported("UPSERT requires VALUES".into())),
    };

    // Edge UPSERT: MATCH both endpoints by id, then MERGE the relationship with
    // ON CREATE / ON MATCH SET for its properties. cypher-parser cannot MERGE a
    // relationship pattern with an inline WHERE, so endpoints are pinned via the
    // MATCH clauses (same shape as translate_insert_edge).
    if let Some((source_label, rel_type, target_label)) = match classify_table(&table_name) {
        TableType::GenericEdge { rel_type } => Some((None, rel_type, None)),
        TableType::TypedEdge {
            source_label,
            rel_type,
            target_label,
        } => Some((Some(source_label), rel_type, Some(target_label))),
        _ => None,
    } {
        let on_update = upsert_on_update_clause(insert.on.as_ref(), "r");
        return translate_upsert_edge(
            source_label.as_deref(),
            &rel_type,
            target_label.as_deref(),
            &columns,
            &values_list,
            &on_update,
        );
    }

    // Determine label filter (without parentheses)
    let label_clause = if table_name.to_lowercase() == "nodes" {
        String::new()
    } else {
        format!(":{}", table_name)
    };

    // Check for ON DUPLICATE KEY UPDATE (node context → `n.` prefix).
    let on_update_clause = upsert_on_update_clause(insert.on.as_ref(), "n");

    // Build MERGE pattern for first row.
    // Cypher MERGE does not accept a WHERE clause; use the node pattern +
    // ON CREATE SET / ON MATCH SET semantics to emulate UPSERT.
    let mut cypher = String::new();

    for row_exprs in &values_list {
        // Build SET items for CREATE
        let mut create_set_items = Vec::new();
        let mut match_conditions = Vec::new();

        for (i, expr) in row_exprs.iter().enumerate() {
            if i < columns.len() {
                let col = columns[i];
                let val = expr_to_value_cypher(expr);
                create_set_items.push(format!("n.{} = {}", col, val));

                // For upsert, match on first column (unique key)
                if i == 0 {
                    match_conditions.push(format!("n.{} = {}", col, val));
                }
            }
        }

        let label_clause_str = if label_clause.is_empty() {
            String::new()
        } else {
            label_clause.clone()
        };

        if let Some(ref on_update) = on_update_clause {
            // UPSERT: MERGE (n:Label) ON CREATE SET n.prop = val, ...
            // followed by ON MATCH SET (for the update branch).
            cypher = format!(
                "MERGE (n{}) ON CREATE SET {} ON MATCH SET {}",
                label_clause_str,
                create_set_items.join(", "),
                on_update
            );
        } else {
            cypher = format!(
                "MERGE (n{}) ON CREATE SET {}",
                label_clause_str,
                create_set_items.join(", ")
            );
        }
    }

    Ok((cypher, values_list.len()))
}

/// Translate an edge UPSERT (`INSERT INTO edge_* ... ON DUPLICATE KEY UPDATE`).
///
/// Emits `MATCH (a) WHERE id(a) = <from> MATCH (b) WHERE id(b) = <to>
/// MERGE (a)-[r:TYPE]->(b) ON CREATE SET ... [ON MATCH SET ...]`, mirroring the
/// node upsert but keyed on the relationship endpoints rather than a node
/// property. Endpoint columns (`from_id`/`to_id`) are used for the MATCH/MERGE
/// pattern and never written as edge properties.
fn translate_upsert_edge(
    source_label: Option<&str>,
    rel_type: &str,
    target_label: Option<&str>,
    columns: &[&str],
    values_list: &[Vec<&Expr>],
    on_update_clause: &Option<String>,
) -> Result<(String, usize), SqlError> {
    let from_idx = columns
        .iter()
        .position(|c| *c == "from_id")
        .ok_or_else(|| SqlError::Unsupported("edge UPSERT requires 'from_id' column".into()))?;
    let to_idx = columns
        .iter()
        .position(|c| *c == "to_id")
        .ok_or_else(|| SqlError::Unsupported("edge UPSERT requires 'to_id' column".into()))?;

    let mut cypher = String::new();
    for row_exprs in values_list {
        let from_id = expr_to_value_cypher(row_exprs[from_idx]);
        let to_id = expr_to_value_cypher(row_exprs[to_idx]);

        // Non-endpoint columns become ON CREATE SET items on the relationship.
        let mut create_set_items = Vec::new();
        for (i, expr) in row_exprs.iter().enumerate() {
            if i == from_idx || i == to_idx || i >= columns.len() {
                continue;
            }
            create_set_items.push(format!("r.{} = {}", columns[i], expr_to_value_cypher(expr)));
        }

        let a_pattern = match source_label {
            Some(l) => format!("MATCH (a:{}) WHERE id(a) = {}", l, from_id),
            None => format!("MATCH (a) WHERE id(a) = {}", from_id),
        };
        let b_pattern = match target_label {
            Some(l) => format!("MATCH (b:{}) WHERE id(b) = {}", l, to_id),
            None => format!("MATCH (b) WHERE id(b) = {}", to_id),
        };

        let create_set = if create_set_items.is_empty() {
            String::new()
        } else {
            format!(" ON CREATE SET {}", create_set_items.join(", "))
        };
        let match_set = match on_update_clause {
            Some(u) => format!(" ON MATCH SET {}", u),
            None => String::new(),
        };

        cypher = format!(
            "{} {} MERGE (a)-[r:{}]->(b){}{}",
            a_pattern, b_pattern, rel_type, create_set, match_set
        );
    }

    Ok((cypher, values_list.len()))
}

// ============================================================
// Expression Translation
// ============================================================

fn expr_to_cypher(expr: &Expr) -> String {
    expr_to_cypher_impl(expr, "n")
}

fn expr_to_cypher_for_edge(expr: &Expr) -> String {
    expr_to_cypher_impl(expr, "r")
}

fn expr_to_cypher_impl(expr: &Expr, var_prefix: &str) -> String {
    match expr {
        Expr::Identifier(ident) => {
            if ident.quote_style.is_none() {
                format!("{}.{}", var_prefix, ident.value)
            } else {
                // Quoted identifiers are still property references and need
                // the prefix for Cypher scope. Escaping is handled above
                // in value_to_cypher; splicing the raw text from the SQL parser
                // verbatim would allow clause injection.
                format!("{}.\"{}\"", var_prefix, ident.value)
            }
        }
        Expr::Value(value) => value_to_cypher(value),
        Expr::BinaryOp { left, op, right } => {
            format!(
                "{} {} {}",
                expr_to_cypher_impl(left, var_prefix),
                op,
                expr_to_cypher_impl(right, var_prefix)
            )
        }
        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|p| p.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();
            let args = function_args_to_cypher_impl(func, var_prefix);

            // Map SQL aggregation functions to Cypher
            let cypher_name: String = match name.as_str() {
                "count" | "sum" | "avg" | "min" | "max" => name,
                "upper" => "toUpper".to_string(),
                "lower" => "toLower".to_string(),
                "length" | "len" => "size".to_string(),
                "abs" => "abs".to_string(),
                "collect" | "array_agg" => "collect".to_string(),
                _ => name,
            };

            // Security: validate function name against whitelist
            if !is_function_allowed(&cypher_name) {
                return format!("/* UNSAFE FUNCTION: {} */", cypher_name.replace("*/", "**"));
            }

            format!("{}({})", cypher_name, args)
        }
        Expr::Nested(expr) => format!("({})", expr_to_cypher_impl(expr, var_prefix)),
        Expr::UnaryOp { op, expr } => format!("{} {}", op, expr_to_cypher_impl(expr, var_prefix)),
        Expr::IsNull(expr) => format!("{} IS NULL", expr_to_cypher_impl(expr, var_prefix)),
        Expr::IsNotNull(expr) => format!("{} IS NOT NULL", expr_to_cypher_impl(expr, var_prefix)),
        Expr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            let list_str = list
                .iter()
                .map(|e| expr_to_cypher_impl(e, var_prefix))
                .collect::<Vec<_>>()
                .join(", ");
            let op = if *negated { "NOT IN" } else { "IN" };
            format!(
                "{} {} [{}]",
                expr_to_cypher_impl(expr, var_prefix),
                op,
                list_str
            )
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
            ..
        } => {
            let rel = if *negated { " OR " } else { " AND " };
            format!(
                "{} >= {} {} {} <= {}",
                expr_to_cypher_impl(expr, var_prefix),
                expr_to_cypher_impl(low, var_prefix),
                rel.trim(),
                expr_to_cypher_impl(expr, var_prefix),
                expr_to_cypher_impl(high, var_prefix)
            )
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let pattern_str = pattern.to_string();
            let inner = pattern_str.trim_matches('\'');

            // Escape BEFORE trimming % to preserve escape sequences
            let escaped_full = inner.replace('\\', "\\\\").replace('\'', "\\'");

            let (starts, ends) = (escaped_full.starts_with('%'), escaped_full.ends_with('%'));
            let trimmed = escaped_full.trim_matches('%');

            let op = if *negated { " NOT " } else { " " };
            if starts && ends {
                format!(
                    "{}{}CONTAINS '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            } else if ends {
                format!(
                    "{}{}STARTS WITH '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            } else if starts {
                format!(
                    "{}{}ENDS WITH '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            } else {
                format!(
                    "{}{}CONTAINS '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            }
        }
        Expr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let pattern_str = pattern.to_string();
            let inner = pattern_str.trim_matches('\'');

            // Escape BEFORE trimming % and converting to lowercase
            let escaped_full = inner.replace('\\', "\\\\").replace('\'', "\\'");

            let (starts, ends) = (escaped_full.starts_with('%'), escaped_full.ends_with('%'));
            let trimmed = escaped_full.trim_matches('%').to_lowercase();

            let op = if *negated { " NOT " } else { " " };
            if starts && ends {
                format!(
                    "toLower({}){}CONTAINS '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            } else if ends {
                format!(
                    "toLower({}){}STARTS WITH '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            } else if starts {
                format!(
                    "toLower({}){}ENDS WITH '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            } else {
                format!(
                    "toLower({}){}CONTAINS '{}'",
                    expr_to_cypher_impl(expr, var_prefix),
                    op,
                    trimmed
                )
            }
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            let inner = expr_to_cypher_impl(expr, var_prefix);
            let type_name = data_type.to_string().to_lowercase();
            match type_name.as_str() {
                "integer" | "int" | "bigint" | "smallint" => format!("toInteger({})", inner),
                "float" | "double" | "real" | "decimal" => format!("toFloat({})", inner),
                "varchar" | "text" | "char" | "string" => format!("toString({})", inner),
                "boolean" | "bool" => inner,
                _ => inner,
            }
        }
        _ => format!("{}", expr),
    }
}

fn function_args_to_cypher_impl(f: &sqlparser::ast::Function, var_prefix: &str) -> String {
    match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(expr_to_cypher_impl(e, var_prefix))
                }
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => {
                    Some("*".to_string())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(", "),
        _ => "*".to_string(),
    }
}

fn expr_to_value_cypher(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => value_to_cypher(value),
        Expr::UnaryOp { op, expr } => format!("{} {}", op, expr_to_value_cypher(expr)),
        Expr::Nested(expr) => format!("({})", expr_to_value_cypher(expr)),
        Expr::Identifier(_) => {
            // Column reference in SET clause needs to be prefixed with n.
            expr_to_cypher(expr)
        }
        Expr::BinaryOp { left, op, right } => {
            // For binary operations in SET, we need to handle column references properly
            // e.g., SET stock = stock - 10 should become SET n.stock = n.stock - 10
            format!(
                "{} {} {}",
                expr_to_value_cypher(left),
                op,
                expr_to_value_cypher(right)
            )
        }
        Expr::Function(func) => {
            let name = func.name.to_string().to_lowercase();

            // Security: validate function name against whitelist
            if !is_function_allowed(&name) {
                return format!("/* UNSAFE FUNCTION: {} */", name.replace("*/", "**"));
            }

            let args = function_args_to_cypher(func);
            format!("{}({})", name, args)
        }
        _ => expr_to_cypher(expr),
    }
}

fn value_to_cypher(value: &Value) -> String {
    match value {
        Value::SingleQuotedString(s) => {
            // Escape both backslashes and single quotes for Cypher
            // string literals. Omitting backslash escaping allows
            // values containing \' to break out of the string and
            // inject arbitrary clauses.
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{}'", escaped)
        }
        Value::DoubleQuotedString(s) => {
            // Double-quoted identifiers become Cypher identifiers,
            // not string literals. They must be prefixed with `n.`
            // and cannot contain arbitrary SQL.
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            format!("n.{}", escaped)
        }
        Value::Number(n, _) => n.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".to_string(),
        _ => format!("{}", value),
    }
}

// ============================================================
// Public API
// ============================================================

/// Translate a SQL query to Cypher (without execution).
pub fn translate_to_cypher(sql: &str) -> Result<String, SqlError> {
    let (cypher, _) = translate_sql_to_cypher(sql)?;
    Ok(cypher)
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ============ SELECT Tests ============

    #[test]
    fn test_basic_select() {
        let cypher = translate_to_cypher("SELECT * FROM nodes LIMIT 10").unwrap();
        assert!(cypher.contains("LIMIT 10"));
        assert!(cypher.contains("MATCH (n)"));
        assert!(cypher.contains("RETURN n"));
    }

    #[test]
    fn test_where_preserves_case() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE n.name = 'Alice'").unwrap();
        assert!(cypher.contains("n.name"));
        assert!(cypher.contains("'Alice'"));
    }

    #[test]
    fn test_table_to_label() {
        let cypher = translate_to_cypher("SELECT * FROM Person WHERE n.age > 30").unwrap();
        assert!(cypher.contains("MATCH (n:Person)"));
    }

    #[test]
    fn test_select_columns() {
        let cypher = translate_to_cypher("SELECT n.name, n.age FROM nodes").unwrap();
        assert!(cypher.contains("n.name"));
        assert!(cypher.contains("n.age"));
    }

    #[test]
    fn test_order_by() {
        let cypher = translate_to_cypher("SELECT * FROM Person ORDER BY n.age DESC").unwrap();
        assert!(cypher.contains("ORDER BY n.age DESC"));
    }

    #[test]
    fn test_order_by_asc() {
        let cypher = translate_to_cypher("SELECT * FROM Person ORDER BY n.name ASC").unwrap();
        assert!(cypher.contains("ORDER BY n.name"));
    }

    #[test]
    fn test_offset() {
        let cypher = translate_to_cypher("SELECT * FROM Person LIMIT 10 OFFSET 20").unwrap();
        assert!(cypher.contains("LIMIT 10"));
        assert!(cypher.contains("SKIP 20"));
    }

    #[test]
    fn test_multiple_order_by() {
        let cypher =
            translate_to_cypher("SELECT * FROM Person ORDER BY n.age DESC, n.name ASC").unwrap();
        assert!(cypher.contains("ORDER BY n.age DESC, n.name"));
    }

    // ============ Aggregation Tests ============

    #[test]
    fn test_aggregation_count() {
        let cypher = translate_to_cypher("SELECT COUNT(*) FROM Person").unwrap();
        assert!(cypher.contains("count(*)"));
    }

    #[test]
    fn test_aggregation_sum() {
        let cypher = translate_to_cypher("SELECT SUM(n.amount) FROM Transaction").unwrap();
        assert!(cypher.contains("sum(n.amount)"));
    }

    #[test]
    fn test_aggregation_avg() {
        let cypher = translate_to_cypher("SELECT AVG(n.price) FROM Product").unwrap();
        assert!(cypher.contains("avg(n.price)"));
    }

    #[test]
    fn test_aggregation_min_max() {
        let cypher = translate_to_cypher("SELECT MIN(n.price), MAX(n.price) FROM Product").unwrap();
        assert!(cypher.contains("min(n.price)"));
        assert!(cypher.contains("max(n.price)"));
    }

    #[test]
    fn test_aggregation_with_alias() {
        let cypher = translate_to_cypher("SELECT COUNT(*) AS total FROM Person").unwrap();
        assert!(cypher.contains("count(*) AS total"));
    }

    // ============ GROUP BY Tests ============

    #[test]
    fn test_group_by() {
        // cypher-parser groups implicitly via RETURN — no malformed `WITH <key>`
        // (which used to collapse every row into a single group).
        let cypher = translate_to_cypher(
            "SELECT n.department, COUNT(*) FROM Employee GROUP BY n.department",
        )
        .unwrap();
        assert!(cypher.contains("RETURN n.department, count(*)"));
        assert!(!cypher.contains("WITH"));
    }

    #[test]
    fn test_group_by_with_aggregation() {
        let cypher = translate_to_cypher(
            "SELECT n.category, SUM(n.amount) FROM Transaction GROUP BY n.category",
        )
        .unwrap();
        assert!(cypher.contains("RETURN n.category, sum(n.amount)"));
        assert!(!cypher.contains("WITH"));
    }

    #[test]
    fn test_group_by_with_order_by() {
        let cypher = translate_to_cypher(
            "SELECT n.dept, COUNT(*) FROM Employee GROUP BY n.dept ORDER BY COUNT(*) DESC",
        )
        .unwrap();
        assert!(cypher.contains("RETURN n.dept, count(*)"));
        assert!(cypher.contains("ORDER BY count(*) DESC"));
    }

    #[test]
    fn test_group_by_multiple_columns() {
        let cypher = translate_to_cypher(
            "SELECT n.dept, n.role, COUNT(*) FROM Employee GROUP BY n.dept, n.role",
        )
        .unwrap();
        assert!(cypher.contains("RETURN n.dept, n.role, count(*)"));
        assert!(!cypher.contains("WITH"));
    }

    // ============ HAVING Tests ============

    #[test]
    fn test_having() {
        // HAVING filters after aggregation. cypher-parser rejects an aggregate
        // inside WHERE, so the aggregate is aliased in a WITH stage and the
        // filter references the alias.
        let cypher = translate_to_cypher(
            "SELECT n.department, COUNT(*) FROM Employee GROUP BY n.department HAVING COUNT(*) > 5",
        )
        .unwrap();
        assert!(
            cypher.contains("WITH"),
            "HAVING must use a WITH stage: {cypher}"
        );
        assert!(
            cypher.contains("count(*) AS"),
            "aggregate must be aliased: {cypher}"
        );
        assert!(
            cypher.contains("> 5"),
            "HAVING predicate must be present: {cypher}"
        );
    }

    #[test]
    fn test_having_with_sum() {
        let cypher = translate_to_cypher("SELECT n.category, SUM(n.amount) FROM Transaction GROUP BY n.category HAVING SUM(n.amount) > 1000").unwrap();
        assert!(
            cypher.contains("WITH"),
            "HAVING must use a WITH stage: {cypher}"
        );
        assert!(
            cypher.contains("sum(n.amount) AS"),
            "aggregate must be aliased: {cypher}"
        );
        assert!(
            cypher.contains("> 1000"),
            "HAVING predicate must be present: {cypher}"
        );
    }

    // ============ Expression Tests ============

    #[test]
    fn test_binary_ops() {
        let cypher =
            translate_to_cypher("SELECT * FROM nodes WHERE n.age > 30 AND n.name = 'Bob'").unwrap();
        assert!(cypher.contains("n.age > 30"));
        assert!(cypher.contains("AND"));
        assert!(cypher.contains("n.name = 'Bob'"));
    }

    #[test]
    fn test_or_condition() {
        let cypher =
            translate_to_cypher("SELECT * FROM nodes WHERE n.age < 20 OR n.age > 60").unwrap();
        assert!(cypher.contains("n.age < 20"));
        assert!(cypher.contains("OR"));
        assert!(cypher.contains("n.age > 60"));
    }

    #[test]
    fn test_is_null() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE n.name IS NULL").unwrap();
        assert!(cypher.contains("n.name IS NULL"));
    }

    #[test]
    fn test_is_not_null() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE n.name IS NOT NULL").unwrap();
        assert!(cypher.contains("n.name IS NOT NULL"));
    }

    #[test]
    fn test_in_list() {
        let cypher =
            translate_to_cypher("SELECT * FROM nodes WHERE n.status IN ('active', 'pending')")
                .unwrap();
        assert!(cypher.contains("n.status IN"));
        assert!(cypher.contains("'active'"));
        assert!(cypher.contains("'pending'"));
    }

    #[test]
    fn test_between() {
        let cypher =
            translate_to_cypher("SELECT * FROM nodes WHERE n.age BETWEEN 18 AND 65").unwrap();
        assert!(cypher.contains("n.age >= 18 AND n.age <= 65"));
    }

    #[test]
    fn test_like() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE n.name LIKE '%john%'").unwrap();
        assert!(cypher.contains("n.name CONTAINS 'john'"), "Got: {}", cypher);
    }

    #[test]
    fn test_cast_integer() {
        let cypher = translate_to_cypher("SELECT CAST(n.price AS INTEGER) FROM Product").unwrap();
        assert!(cypher.contains("toInteger(n.price)"));
    }

    #[test]
    fn test_cast_float() {
        let cypher = translate_to_cypher("SELECT CAST(n.price AS FLOAT) FROM Product").unwrap();
        assert!(cypher.contains("toFloat(n.price)"));
    }

    #[test]
    fn test_nested_expression() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE (n.age + 10) > 50").unwrap();
        assert!(cypher.contains("(n.age + 10) > 50"));
    }

    #[test]
    fn test_bool_literal() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE n.active = true").unwrap();
        assert!(cypher.contains("n.active = true"));
    }

    #[test]
    fn test_numeric_literal() {
        let cypher = translate_to_cypher("SELECT * FROM nodes WHERE n.count > 100.5").unwrap();
        assert!(cypher.contains("n.count > 100.5"));
    }

    // ============ INSERT Tests ============

    #[test]
    fn test_insert() {
        let cypher =
            translate_to_cypher("INSERT INTO Person (name, age) VALUES ('Alice', 30)").unwrap();
        assert!(cypher.contains("CREATE"));
        assert!(cypher.contains("n:Person"));
        assert!(cypher.contains("name: 'Alice'"));
        assert!(cypher.contains("age: 30"));
    }

    #[test]
    fn test_insert_multiple_rows() {
        let cypher =
            translate_to_cypher("INSERT INTO Product (name, price) VALUES ('A', 10), ('B', 20)")
                .unwrap();
        assert!(cypher.contains("CREATE"));
        assert!(cypher.contains("name: 'A'"));
        assert!(cypher.contains("name: 'B'"));
    }

    #[test]
    fn test_insert_into_nodes() {
        let cypher =
            translate_to_cypher("INSERT INTO nodes (name, value) VALUES ('test', 123)").unwrap();
        assert!(cypher.contains("CREATE (n {"));
        assert!(cypher.contains("name: 'test'"));
    }

    // ============ UPDATE Tests ============

    #[test]
    fn test_update_basic() {
        let cypher = translate_to_cypher("UPDATE Person SET name = 'Bob' WHERE id = 1").unwrap();
        assert!(cypher.contains("MATCH (n:Person)"), "Got: {}", cypher);
        assert!(cypher.contains("SET n.name = 'Bob'"));
        assert!(cypher.contains("WHERE n.id = 1"));
    }

    #[test]
    fn test_update_multiple_columns() {
        let cypher =
            translate_to_cypher("UPDATE Person SET name = 'Bob', age = 35 WHERE id = 1").unwrap();
        assert!(cypher.contains("SET n.name = 'Bob', n.age = 35"));
    }

    #[test]
    fn test_update_without_where() {
        // Should allow UPDATE without WHERE (updates all matching nodes)
        let cypher = translate_to_cypher("UPDATE Product SET status = 'inactive'").unwrap();
        assert!(cypher.contains("MATCH (n:Product)"));
        assert!(cypher.contains("SET n.status = 'inactive'"));
    }

    #[test]
    fn test_update_nodes_table() {
        let cypher = translate_to_cypher("UPDATE nodes SET updated = true WHERE id = 5").unwrap();
        assert!(cypher.contains("MATCH (n)"));
        assert!(cypher.contains("SET n.updated = true"));
    }

    // ============ DELETE Tests ============

    #[test]
    fn test_delete() {
        let cypher = translate_to_cypher("DELETE FROM Person WHERE id = 1").unwrap();
        assert!(cypher.contains("MATCH (n:Person)"), "Got: {}", cypher);
        assert!(cypher.contains("DETACH DELETE n"));
        assert!(cypher.contains("WHERE n.id = 1"));
    }

    #[test]
    fn test_delete_rejected_without_where() {
        let result = translate_to_cypher("DELETE FROM Person");
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_from_nodes() {
        let cypher = translate_to_cypher("DELETE FROM nodes WHERE id = 1").unwrap();
        assert!(cypher.contains("MATCH (n)"));
        assert!(cypher.contains("DETACH DELETE n"));
    }

    // ============ UPSERT Tests ============

    #[test]
    fn test_upsert_basic() {
        let cypher = translate_to_cypher("INSERT INTO User (id, name, email) VALUES (1, 'Alice', 'alice@example.com') ON DUPLICATE KEY UPDATE name = VALUES(name)").unwrap();
        assert!(cypher.contains("MERGE"));
        assert!(cypher.contains("n:User"));
        assert!(cypher.contains("ON CREATE SET"));
        assert!(cypher.contains("ON MATCH SET"));
    }

    #[test]
    fn test_upsert_simple() {
        // Simple INSERT without ON DUPLICATE KEY becomes CREATE
        let cypher =
            translate_to_cypher("INSERT INTO Config (key, value) VALUES ('theme', 'dark')")
                .unwrap();
        assert!(cypher.contains("CREATE"));
        assert!(cypher.contains(":Config"));
    }

    // ============ Function Tests ============

    #[test]
    fn test_length_function() {
        let cypher = translate_to_cypher("SELECT LENGTH(n.name) FROM Person").unwrap();
        assert!(cypher.contains("size(n.name)"));
    }

    #[test]
    fn test_collect_function() {
        let cypher = translate_to_cypher("SELECT COLLECT(n.name) FROM Person").unwrap();
        assert!(cypher.contains("collect(n.name)"));
    }

    #[test]
    fn test_upper_lower() {
        let cypher =
            translate_to_cypher("SELECT UPPER(n.name), LOWER(n.name) FROM Person").unwrap();
        assert!(cypher.contains("toUpper(n.name)"), "Got: {}", cypher);
        assert!(cypher.contains("toLower(n.name)"));
    }

    // ============ SQL Injection Defense Tests ============

    #[test]
    fn test_like_escape_backslash() {
        let cypher =
            translate_to_cypher("SELECT * FROM Person WHERE name LIKE '%test\\\\%'").unwrap();
        // Should escape backslash to double backslash
        assert!(
            cypher.contains("\\\\\\\\"),
            "Backslash not properly escaped: {}",
            cypher
        );
    }

    #[test]
    fn test_like_escape_single_quote() {
        let cypher =
            translate_to_cypher("SELECT * FROM Person WHERE name LIKE '%O''Brien%'").unwrap();
        // Should escape single quotes
        assert!(
            cypher.contains("\\'"),
            "Single quote not properly escaped: {}",
            cypher
        );
    }

    #[test]
    fn test_like_injection_attempt() {
        // Test that backslash in pattern is properly escaped
        // A valid SQL pattern with escaped backslash
        let cypher =
            translate_to_cypher("SELECT * FROM Person WHERE name LIKE '%test\\%'").unwrap();
        // The backslash should be double-escaped in the output
        assert!(
            cypher.contains("\\\\"),
            "Backslash not properly escaped: {}",
            cypher
        );
        // Should not contain any unescaped quotes that could break out
        let quote_count = cypher.matches('\'').count();
        // Should have balanced quotes (even number)
        assert_eq!(quote_count % 2, 0, "Unbalanced quotes detected: {}", cypher);
    }

    #[test]
    fn test_string_literal_escape() {
        let cypher = translate_to_cypher("SELECT * FROM Person WHERE name = 'Alice''s'").unwrap();
        // Double single quote should be escaped
        assert!(
            cypher.contains("Alice"),
            "String literal parsing failed: {}",
            cypher
        );
    }

    #[test]
    fn test_like_with_percent_at_ends() {
        let cypher = translate_to_cypher("SELECT * FROM Person WHERE name LIKE '%Alice%'").unwrap();
        // Should preserve % at both ends and convert to CONTAINS
        assert!(
            cypher.contains("CONTAINS"),
            "LIKE not converted to CONTAINS: {}",
            cypher
        );
        assert!(
            cypher.contains("'Alice'"),
            "Pattern incorrectly processed: {}",
            cypher
        );
    }

    #[test]
    fn test_ilike_case_insensitive_with_escape() {
        let cypher =
            translate_to_cypher("SELECT * FROM Person WHERE name ILIKE '%alice%'").unwrap();
        // Should convert to toLower and properly handle pattern
        assert!(
            cypher.contains("toLower"),
            "ILIKE not converted: {}",
            cypher
        );
        assert!(
            cypher.contains("'alice'"),
            "Pattern incorrectly processed: {}",
            cypher
        );
        assert!(
            cypher.contains("CONTAINS"),
            "ILIKE not converted to CONTAINS: {}",
            cypher
        );
    }
}
