//! Nexora Cypher engine — wraps Shopify cypher-parser + extends with write operations.
//!
//! Uses `cypher-parser` for read queries (MATCH, WHERE, RETURN, WITH).
//! Extends with write operations (CREATE, SET, DELETE, REMOVE) via custom AST.
//!
//! ## Architecture
//!
//! ```text
//!   Query string
//!       │
//!       ▼
//!   cypher-parser::parse()  →  Read AST (MATCH/RETURN/WITH)
//!   custom WriteParser      →  Write AST (CREATE/SET/DELETE)
//!       │
//!       ▼
//!   CypherExecutor
//!       │
//!       ├─ Read path:  GraphProvider trait → GraphService
//!       └─ Write path: MutationOps → Commit Path
//! ```

pub mod executor;
pub mod function_rewrite;
pub mod path_variable_rewriter;
pub mod slow_query_log;
pub mod write_ast;
pub mod write_executor;

pub use executor::{execute, execute_with_limits, QueryLimits};
pub use write_executor::{execute_write, WriteResult};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CypherError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("unsupported clause: {0}")]
    Unsupported(String),
    #[error("validation error: {0}")]
    Validation(String), // H-4: Query validation errors (pattern depth, etc.)
    #[error("graph error: {0}")]
    Graph(#[from] nexora_core::GraphError),
}

/// Execute a Cypher query against a GraphService.
pub async fn execute_cypher(
    graph: &nexora_core::GraphService,
    query: &str,
) -> Result<CypherResult, CypherError> {
    // Parse the query to detect write clauses (robust AST-based detection).
    // Falls back to cypher-parser if nexora-language cannot handle the syntax.
    let parsed = nexora_language::Parser::parse(query);

    // Handle UNION queries specially — must come before write detection
    // because sub-queries may mix reads and writes.
    if let Ok(ref q) = parsed {
        if q.clauses.len() == 1 {
            if let nexora_language::Clause::Union { all, queries } = &q.clauses[0] {
                return execute_union(graph, *all, queries).await;
            }
        }
    }

    let is_write = parsed
        .as_ref()
        .map(|q| has_write_clauses(&q.clauses))
        .unwrap_or(false);

    // If parsing succeeded and it's a write query, use custom write executor
    if is_write {
        let query = parsed.map_err(|e| CypherError::Parse(e.to_string()))?;
        let write_result = execute_write(graph, &query).await?;
        return Ok(CypherResult::Write(write_result));
    }

    // Check for CALL or LOAD CSV clauses in read queries.
    // These need special handling because cypher-parser doesn't understand them.
    if let Ok(ref q) = parsed {
        if q.clauses.iter().any(|c| {
            matches!(
                c,
                nexora_language::Clause::Call { .. } | nexora_language::Clause::LoadCsv { .. }
            )
        }) {
            return execute_with_special_clauses(graph, q).await;
        }
    }

    // Analyze query for function calls that need rewriting
    let analysis = function_rewrite::analyze_query(query)?;
    let query_to_execute = if analysis.modified {
        &analysis.rewritten_query
    } else {
        query
    };

    // Rewrite path variables if present (so cypher-parser can handle them)
    let path_rewrite = if let Ok(ref ast) = parsed {
        path_variable_rewriter::rewrite_path_variables(query_to_execute, ast)
    } else {
        path_variable_rewriter::PathRewriteResult {
            query: query_to_execute.to_string(),
            has_path_vars: false,
            path_mappings: Vec::new(),
        }
    };
    let final_query = if path_rewrite.has_path_vars {
        &path_rewrite.query
    } else {
        query_to_execute
    };

    // If nexora-language parsed it but found no write clauses, treat as read query.
    // For queries nexora-language cannot parse at all, fall back to cypher-parser.
    let result = match parsed {
        Ok(_) => {
            // Valid read-only query parsed by nexora-language, use cypher-parser for full execution
            executor::execute(graph, final_query).await?
        }
        Err(_) => {
            // nexora-language couldn't parse → fall back to cypher-parser (read path)
            executor::execute(graph, final_query).await?
        }
    };

    // Post-process results if functions were rewritten
    if analysis.modified {
        match result {
            CypherResult::Rows { columns, rows } => {
                let (new_columns, new_rows) =
                    function_rewrite::post_process_results(columns, rows, &analysis);
                Ok(CypherResult::Rows {
                    columns: new_columns,
                    rows: new_rows,
                })
            }
            other => Ok(other),
        }
    } else {
        Ok(result)
    }
}

/// Execute a UNION / UNION ALL query by running each sub-query independently
/// and merging results. UNION deduplicates; UNION ALL concatenates.
async fn execute_union(
    graph: &nexora_core::GraphService,
    all: bool,
    queries: &[Vec<nexora_language::Clause>],
) -> Result<CypherResult, CypherError> {
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();

    for subquery in queries {
        // Check if this sub-query has write clauses
        if has_write_clauses(subquery) {
            let q = nexora_language::CypherQuery {
                clauses: subquery.clone(),
                as_of: None,
            };
            let _ = execute_write(graph, &q).await?;
            continue;
        }

        // Execute as a read query
        let subquery_str = clauses_to_string(subquery);
        if subquery_str.is_empty() {
            continue;
        }
        match executor::execute(graph, &subquery_str).await? {
            CypherResult::Rows {
                columns: cols,
                rows: r,
            } => {
                if columns.is_empty() {
                    columns = cols;
                }
                rows.extend(r);
            }
            CypherResult::Empty | CypherResult::Write(_) => {}
        }
    }

    if rows.is_empty() && columns.is_empty() {
        return Ok(CypherResult::Empty);
    }

    // Deduplicate for UNION (not UNION ALL)
    if !all {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            let key = serde_json::to_string(row).unwrap_or_default();
            seen.insert(key)
        });
    }

    Ok(CypherResult::Rows { columns, rows })
}

/// Execute a query that contains CALL or LOAD CSV clauses by stripping them
/// (with warnings) and executing the remaining clauses.
async fn execute_with_special_clauses(
    graph: &nexora_core::GraphService,
    query: &nexora_language::CypherQuery,
) -> Result<CypherResult, CypherError> {
    let mut filtered = Vec::new();
    for clause in &query.clauses {
        match clause {
            nexora_language::Clause::Call { subquery } => {
                tracing::warn!("CALL subquery is not fully supported; executing outer query only");
                // If the subquery has write clauses, execute them
                if has_write_clauses(subquery) {
                    let q = nexora_language::CypherQuery {
                        clauses: subquery.clone(),
                        as_of: None,
                    };
                    let _ = execute_write(graph, &q).await?;
                }
            }
            nexora_language::Clause::LoadCsv { path, alias, .. } => {
                // GAP-5: LOAD CSV is not implemented. Fail loudly rather than
                // silently stripping the clause (which made users think the data
                // loaded). Direct them to the file ingest API.
                return Err(CypherError::Unsupported(format!(
                    "LOAD CSV FROM '{path}' AS {alias} is not supported. Use the file \
                     ingest API (POST /api/v2/ingest/file) to load CSV/JSON data instead."
                )));
            }
            other => filtered.push(other.clone()),
        }
    }

    if filtered.is_empty() {
        return Ok(CypherResult::Empty);
    }

    let filtered_str = clauses_to_string(&filtered);
    // If the filtered query can't be parsed by cypher-parser (e.g., a RETURN
    // with no preceding MATCH after LOAD CSV was stripped), return Empty.
    match executor::execute(graph, &filtered_str).await {
        Ok(result) => Ok(result),
        Err(CypherError::Parse(msg)) => {
            tracing::warn!(
                "Filtered query '{}' failed to parse after stripping special clauses: {}",
                filtered_str,
                msg
            );
            Ok(CypherResult::Empty)
        }
        Err(e) => Err(e),
    }
}

/// Convert a slice of clauses back to a query string using their Display impls.
fn clauses_to_string(clauses: &[nexora_language::Clause]) -> String {
    clauses
        .iter()
        .map(|c| format!("{c}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Check if any clause in the query is a write operation.
/// Recursively checks inside UNION sub-queries and CALL subqueries.
fn has_write_clauses(clauses: &[nexora_language::Clause]) -> bool {
    clauses.iter().any(|c| match c {
        nexora_language::Clause::Create { .. }
        | nexora_language::Clause::Merge { .. }
        | nexora_language::Clause::Set { .. }
        | nexora_language::Clause::Remove { .. }
        | nexora_language::Clause::Delete { .. } => true,
        nexora_language::Clause::Union { queries, .. } => {
            queries.iter().any(|sub| has_write_clauses(sub))
        }
        nexora_language::Clause::Call { subquery } => has_write_clauses(subquery),
        _ => false,
    })
}

/// Result of a Cypher query execution.
#[derive(Clone, Debug)]
pub enum CypherResult {
    /// Query returned rows (MATCH ... RETURN)
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    /// Query performed mutations (CREATE/SET/DELETE)
    Write(WriteResult),
    /// Empty result
    Empty,
}
pub use executor::extract_as_of;
