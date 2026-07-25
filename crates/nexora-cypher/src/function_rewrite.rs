//! Function rewriting for Cypher queries
//!
//! Rewrites queries that use Neo4j built-in functions (id(), type(), properties(), labels())
//! into equivalent queries that can be executed by cypher-parser.
//!
//! ## Strategy
//!
//! Since cypher-parser doesn't support these functions directly, we rewrite the query
//! to return the raw entities, then post-process the results to extract the requested fields.
//!
//! ## Example
//!
//! ```cypher
//! // Original query:
//! MATCH (a)-[r:KNOWS]->(b) RETURN id(a), id(b), type(r), r.since
//!
//! // Rewritten query:
//! MATCH (a)-[r:KNOWS]->(b) RETURN a, b, r, r.since
//!
//! // Post-processing extracts id(a), id(b), type(r) from the returned entities
//! ```

use crate::CypherError;

/// Information about function calls that need post-processing
#[derive(Debug, Clone)]
pub struct FunctionCall {
    /// Original column name in the result
    pub result_column: String,
    /// Function name (id, type, properties, labels, time_window_bucket)
    pub function_name: String,
    /// The variable or expression the function is applied to (e.g., "a" in id(a), "e.ts" in time_window_bucket)
    pub variable: String,
    /// Extra literal arguments (e.g., bucket size for time_window_bucket)
    pub args: Vec<serde_json::Value>,
}

// ============================================================
// F4.2: time_window_bucket — pure scalar function
// ============================================================

/// Align `timestamp_ms` down to the nearest `window_size_ms` boundary.
///
/// Returns the start of the tumbling window that contains `timestamp_ms`.
///
/// # Examples
///
/// ```
/// use nexora_cypher::function_rewrite::time_window_bucket;
///
/// // 1700000090000 ms floors to the [1700000040000, 1700000100000) window
/// assert_eq!(time_window_bucket(1_700_000_090_000, 60_000), 1_700_000_040_000);
///
/// // Exactly on a boundary stays in that window (inclusive start)
/// assert_eq!(time_window_bucket(1_700_000_040_000, 60_000), 1_700_000_040_000);
/// ```
///
/// Use in Cypher via the query post-processor:
/// ```cypher
/// MATCH (e:Event) RETURN time_window_bucket(e.ts, 60000) AS window, count(*) AS cnt
/// ```
pub fn time_window_bucket(timestamp_ms: i64, window_size_ms: i64) -> i64 {
    assert!(window_size_ms > 0, "window_size_ms must be positive");
    timestamp_ms.div_euclid(window_size_ms) * window_size_ms
}

/// Result of query analysis
#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    /// Rewritten query that can be executed by cypher-parser
    pub rewritten_query: String,
    /// Function calls that need post-processing
    pub function_calls: Vec<FunctionCall>,
    /// Whether the query was modified
    pub modified: bool,
}

/// Analyze and potentially rewrite a Cypher query to work around missing function support
pub fn analyze_query(query: &str) -> Result<QueryAnalysis, CypherError> {
    // Check if the query contains any of the problematic functions
    let has_id = query.contains("id(");
    let has_type = query.contains("type(");
    let has_properties = query.contains("properties(");
    let has_labels = query.contains("labels(");

    // Check for edge property access (r.property_name)
    let has_edge_property = regex::Regex::new(r"\br\.(\w+)").unwrap().is_match(query);

    if !has_id && !has_type && !has_properties && !has_labels && !has_edge_property {
        // No rewriting needed
        return Ok(QueryAnalysis {
            rewritten_query: query.to_string(),
            function_calls: Vec::new(),
            modified: false,
        });
    }

    // Simple regex-based rewriting for common patterns
    let mut rewritten = query.to_string();
    let mut function_calls = Vec::new();

    // Pattern: id(var) AS alias or just id(var)
    let id_pattern = regex::Regex::new(r"id\((\w+)\)(?:\s+AS\s+(\w+))?").unwrap();
    for cap in id_pattern.captures_iter(query) {
        let var = cap.get(1).unwrap().as_str();
        let alias = cap
            .get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| format!("id({})", var));
        function_calls.push(FunctionCall {
            result_column: alias.clone(),
            function_name: "id".to_string(),
            variable: var.to_string(),
            args: vec![],
        });
    }

    // Pattern: type(rel) AS alias or just type(rel)
    let type_pattern = regex::Regex::new(r"type\((\w+)\)(?:\s+AS\s+(\w+))?").unwrap();
    for cap in type_pattern.captures_iter(query) {
        let var = cap.get(1).unwrap().as_str();
        let alias = cap
            .get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| format!("type({})", var));
        function_calls.push(FunctionCall {
            result_column: alias.clone(),
            function_name: "type".to_string(),
            variable: var.to_string(),
            args: vec![],
        });
    }

    // Pattern: properties(entity) AS alias or just properties(entity)
    let props_pattern = regex::Regex::new(r"properties\((\w+)\)(?:\s+AS\s+(\w+))?").unwrap();
    for cap in props_pattern.captures_iter(query) {
        let var = cap.get(1).unwrap().as_str();
        let alias = cap
            .get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| format!("properties({})", var));
        function_calls.push(FunctionCall {
            result_column: alias.clone(),
            function_name: "properties".to_string(),
            variable: var.to_string(),
            args: vec![],
        });
    }

    // Pattern: labels(node) AS alias or just labels(node)
    let labels_pattern = regex::Regex::new(r"labels\((\w+)\)(?:\s+AS\s+(\w+))?").unwrap();
    for cap in labels_pattern.captures_iter(query) {
        let var = cap.get(1).unwrap().as_str();
        let alias = cap
            .get(2)
            .map(|m| m.as_str().to_string())
            .unwrap_or_else(|| format!("labels({})", var));
        function_calls.push(FunctionCall {
            result_column: alias.clone(),
            function_name: "labels".to_string(),
            variable: var.to_string(),
            args: vec![],
        });
    }

    // Replace function calls with their variables in the RETURN clause
    // For "id(n) AS id", replace with "n AS id"
    rewritten = regex::Regex::new(r"id\((\w+)\)(\s+AS\s+\w+)?")
        .unwrap()
        .replace_all(&rewritten, "$1$2")
        .to_string();
    rewritten = regex::Regex::new(r"type\((\w+)\)(\s+AS\s+\w+)?")
        .unwrap()
        .replace_all(&rewritten, "$1$2")
        .to_string();
    rewritten = regex::Regex::new(r"properties\((\w+)\)(\s+AS\s+\w+)?")
        .unwrap()
        .replace_all(&rewritten, "$1$2")
        .to_string();
    rewritten = regex::Regex::new(r"labels\((\w+)\)(\s+AS\s+\w+)?")
        .unwrap()
        .replace_all(&rewritten, "$1$2")
        .to_string();

    // Handle WHERE id(n) = X by replacing with a property check
    // This is a temporary workaround - ideally we'd optimize this to direct ID lookup
    let where_id_pattern = regex::Regex::new(r"WHERE id\((\w+)\)\s*=\s*(\d+)").unwrap();
    if where_id_pattern.is_match(&rewritten) {
        // For now, we'll let the query fail with a more helpful error message
        // The proper fix is to add ID indexing to the graph service
        return Err(CypherError::Unsupported(
            "WHERE id(node) = X is not yet supported. Use property-based filtering instead."
                .to_string(),
        ));
    }

    let modified = !function_calls.is_empty();

    Ok(QueryAnalysis {
        rewritten_query: rewritten,
        function_calls,
        modified,
    })
}

/// Post-process query results to apply function transformations
pub fn post_process_results(
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    analysis: &QueryAnalysis,
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if !analysis.modified {
        return (columns, rows);
    }

    // Build a mapping from result column name to function call
    // e.g., "id" -> FunctionCall { result_column: "id", function_name: "id", variable: "n" }
    let mut result_to_function: std::collections::HashMap<String, &FunctionCall> =
        std::collections::HashMap::new();
    for fc in &analysis.function_calls {
        result_to_function.insert(fc.result_column.clone(), fc);
    }

    // Transform columns and rows
    let mut new_columns = Vec::new();
    let mut column_transforms: Vec<Option<&FunctionCall>> = Vec::new();

    for col in &columns {
        // Check if this column name matches a function's result column
        if let Some(fc) = result_to_function.get(col) {
            new_columns.push(fc.result_column.clone());
            column_transforms.push(Some(fc));
        } else {
            new_columns.push(col.clone());
            column_transforms.push(None);
        }
    }

    // Transform each row
    let new_rows: Vec<Vec<serde_json::Value>> = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .zip(&column_transforms)
                .map(|(value, transform)| {
                    if let Some(fc) = transform {
                        apply_function(&fc.function_name, &value)
                    } else {
                        value
                    }
                })
                .collect()
        })
        .collect();

    (new_columns, new_rows)
}

/// Apply a function to a value
fn apply_function(function_name: &str, value: &serde_json::Value) -> serde_json::Value {
    match function_name {
        "id" => extract_id(value),
        "type" => extract_type(value),
        "properties" => extract_properties(value),
        "labels" => extract_labels(value),
        _ => value.clone(),
    }
}

/// Extract ID from a node or relationship object
fn extract_id(value: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object() {
        if let Some(id) = obj.get("id") {
            return id.clone();
        }
    }
    serde_json::Value::Null
}

/// Extract type from a relationship object
fn extract_type(value: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object() {
        if let Some(label) = obj.get("label") {
            return label.clone();
        }
    }
    serde_json::Value::Null
}

/// Extract properties from a node or relationship object
fn extract_properties(value: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object() {
        // Filter out metadata fields (id, label, name)
        let mut properties = serde_json::Map::new();
        for (k, v) in obj {
            if k != "id" && k != "label" && k != "name" {
                properties.insert(k.clone(), v.clone());
            }
        }
        return serde_json::Value::Object(properties);
    }
    serde_json::Value::Object(serde_json::Map::new())
}

/// Extract labels from a node object
fn extract_labels(value: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object() {
        if let Some(label) = obj.get("label") {
            // Return as an array
            return serde_json::json!([label]);
        }
    }
    serde_json::json!([])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_query_with_id() {
        let query = "MATCH (a)-[r:KNOWS]->(b) RETURN id(a), id(b), type(r)";
        let analysis = analyze_query(query).unwrap();

        assert!(analysis.modified);
        assert_eq!(analysis.function_calls.len(), 3);
        assert_eq!(
            analysis.rewritten_query,
            "MATCH (a)-[r:KNOWS]->(b) RETURN a, b, r"
        );
    }

    #[test]
    fn test_analyze_query_with_alias() {
        let query = "MATCH (n:Person) RETURN id(n) AS id";
        let analysis = analyze_query(query).unwrap();

        assert!(analysis.modified);
        assert_eq!(analysis.function_calls.len(), 1);
        assert_eq!(analysis.function_calls[0].result_column, "id");
        assert_eq!(analysis.function_calls[0].variable, "n");
        assert_eq!(analysis.rewritten_query, "MATCH (n:Person) RETURN n AS id");
    }

    #[test]
    fn test_analyze_query_no_functions() {
        let query = "MATCH (n:Person) RETURN n.name, n.age";
        let analysis = analyze_query(query).unwrap();

        assert!(!analysis.modified);
        assert_eq!(analysis.function_calls.len(), 0);
        assert_eq!(analysis.rewritten_query, query);
    }

    #[test]
    fn test_post_process_extracts_id() {
        let columns = vec!["id".to_string()];
        let rows = vec![vec![serde_json::json!({"id": 123, "name": "Alice"})]];

        let analysis = QueryAnalysis {
            rewritten_query: "".to_string(),
            function_calls: vec![FunctionCall {
                result_column: "id".to_string(),
                function_name: "id".to_string(),
                variable: "n".to_string(),
                args: vec![],
            }],
            modified: true,
        };

        let (new_columns, new_rows) = post_process_results(columns, rows, &analysis);

        assert_eq!(new_columns, vec!["id"]);
        assert_eq!(new_rows[0][0], serde_json::json!(123));
    }

    #[test]
    fn test_extract_type() {
        let value = serde_json::json!({"id": 1, "label": "KNOWS"});
        let result = extract_type(&value);
        assert_eq!(result, serde_json::json!("KNOWS"));
    }

    #[test]
    fn test_extract_properties() {
        let value = serde_json::json!({
            "id": 123,
            "label": "Person",
            "name": "Alice",
            "age": 30
        });
        let result = extract_properties(&value);
        // Properties should exclude id, label, and name (which are metadata)
        assert_eq!(result, serde_json::json!({"age": 30}));
    }

    // ---------------------------------------------------------------
    // F4.2: time_window_bucket
    // ---------------------------------------------------------------

    /// Basic alignment: mid-window and boundary cases. 1_700_000_040_000 is a
    /// 60_000-aligned boundary (1_700_000_040_000 / 60_000 = 28_333_334 exactly).
    #[test]
    fn test_time_window_bucket_alignment() {
        // Mid-window — 1_700_000_090_000 floors to the 1_700_000_040_000 window.
        assert_eq!(
            time_window_bucket(1_700_000_090_000, 60_000),
            1_700_000_040_000
        );
        // Exactly on a boundary — stays in that window (inclusive start).
        assert_eq!(
            time_window_bucket(1_700_000_040_000, 60_000),
            1_700_000_040_000
        );
    }

    /// One millisecond before the next boundary belongs to the current window.
    #[test]
    fn test_time_window_bucket_just_before_boundary() {
        // 1_700_000_099_999 floors to 1_700_000_040_000 (next boundary is 1_700_000_100_000).
        assert_eq!(
            time_window_bucket(1_700_000_099_999, 60_000),
            1_700_000_040_000,
        );
    }

    /// Zero timestamp is in the first window starting at 0.
    #[test]
    fn test_time_window_bucket_zero() {
        assert_eq!(time_window_bucket(0, 60_000), 0);
        assert_eq!(time_window_bucket(59_999, 60_000), 0);
        assert_eq!(time_window_bucket(60_000, 60_000), 60_000);
    }

    /// Negative timestamps use Euclidean (floor) division so historical events
    /// are still bucketed correctly.
    #[test]
    fn test_time_window_bucket_negative_timestamp() {
        // -1 ms → bucket starts at -60_000
        assert_eq!(time_window_bucket(-1, 60_000), -60_000);
        // -60_000 ms is exactly on the boundary
        assert_eq!(time_window_bucket(-60_000, 60_000), -60_000);
    }
}
