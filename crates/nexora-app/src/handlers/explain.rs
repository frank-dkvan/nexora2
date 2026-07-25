//! EXPLAIN handler — query plan visualization.
#![allow(dead_code)]

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use nexora_core::{query_optimizer::*, IndexStatistics};
use serde::{Deserialize, Serialize};

use super::AppState;

/// Request body for EXPLAIN
#[derive(Debug, Deserialize)]
pub struct ExplainRequest {
    /// Cypher query to explain
    pub query: String,
    /// Include actual execution stats
    #[serde(default)]
    pub analyze: bool,
}

/// Response body for EXPLAIN
#[derive(Debug, Serialize)]
pub struct ExplainResponse {
    /// Original query
    pub query: String,
    /// Execution plan as a tree
    pub plan: ExecutionPlanResponse,
    /// Estimated total cost
    pub estimated_cost: f64,
    /// Estimated result rows
    pub estimated_rows: usize,
    /// Human-readable explanation
    pub explanation: String,
    /// Actual execution stats (if analyze=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_stats: Option<ActualStats>,
}

#[derive(Debug, Serialize)]
pub struct ExecutionPlanResponse {
    /// Starting operation
    pub start_with: String,
    /// Filter predicates
    pub filters: Vec<String>,
    /// Estimated cost
    pub cost: f64,
}

#[derive(Debug, Serialize)]
pub struct ActualStats {
    /// Actual rows returned
    pub actual_rows: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Number of nodes examined
    pub nodes_examined: usize,
}

/// POST /api/v2/query/explain — Generate query execution plan
pub async fn explain_query(
    State(state): State<AppState>,
    Json(req): Json<ExplainRequest>,
) -> impl IntoResponse {
    // Parse the query (simplified — in production, use full Cypher parser)
    let predicates = parse_query_simple(&req.query);

    // Collect statistics from the graph
    let stats = collect_statistics(&state).await;

    // Create optimizer
    let optimizer = QueryOptimizer::new(stats);

    // Generate execution plan
    let plan = optimizer.optimize(predicates);

    // Format response
    let explanation = format_plan(&plan);

    let response = ExplainResponse {
        query: req.query.clone(),
        plan: ExecutionPlanResponse {
            start_with: format!("{:?}", plan.start_with),
            filters: plan
                .then_filter
                .iter()
                .map(|p| format!("{:?}", p))
                .collect(),
            cost: plan.estimated_cost,
        },
        estimated_cost: plan.estimated_cost,
        estimated_rows: plan.estimated_cost as usize,
        explanation,
        actual_stats: if req.analyze {
            // Execute and measure
            let start = std::time::Instant::now();
            let results = optimizer.execute(plan).await;
            let elapsed = start.elapsed();

            results.ok().map(|r| ActualStats {
                actual_rows: r.len(),
                execution_time_ms: elapsed.as_secs_f64() * 1000.0,
                nodes_examined: r.len(),
            })
        } else {
            None
        },
    };

    (StatusCode::OK, Json(response))
}

/// Simple query parser (production version should use nexora-language parser)
fn parse_query_simple(query: &str) -> Vec<FilterPredicate> {
    let mut predicates = Vec::new();

    // Extract label: MATCH (n:Person)
    if let Some(label_start) = query.find("(n:") {
        if let Some(label_end) = query[label_start..].find(')') {
            let label = &query[label_start + 3..label_start + label_end];
            predicates.push(FilterPredicate::HasLabel(label.to_string()));
        }
    }

    // Extract property filter: WHERE n.city = 'Beijing'
    if let Some(where_idx) = query.find("WHERE") {
        let where_clause = &query[where_idx + 5..];
        if let Some(eq_idx) = where_clause.find('=') {
            let prop = where_clause[..eq_idx]
                .trim()
                .trim_start_matches("n.")
                .to_string();
            let value_str = where_clause[eq_idx + 1..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"');

            // Try to parse as different types
            let value = if let Ok(i) = value_str.parse::<i64>() {
                nexora_id::PropertyValue::Integer(i)
            } else if let Ok(f) = value_str.parse::<f64>() {
                nexora_id::PropertyValue::Float(f)
            } else {
                nexora_id::PropertyValue::String(value_str.to_string())
            };

            predicates.push(FilterPredicate::PropertyEquals(prop, value));
        }
    }

    predicates
}

/// Collect graph statistics for optimization
async fn collect_statistics(state: &AppState) -> IndexStatistics {
    let mut stats = IndexStatistics::new();

    // Get total nodes from metrics
    stats.total_nodes = state
        .metrics
        .active_nodes
        .load(std::sync::atomic::Ordering::Relaxed) as usize;

    // TODO: Collect label and property cardinality from indexes
    // For now, use placeholders
    stats
        .label_cardinality
        .insert("Person".to_string(), stats.total_nodes / 10);
    stats
        .label_cardinality
        .insert("Product".to_string(), stats.total_nodes / 20);

    stats
        .property_cardinality
        .insert("name".to_string(), stats.total_nodes);
    stats.property_cardinality.insert("age".to_string(), 100);
    stats.property_cardinality.insert("city".to_string(), 50);

    stats
}

/// Format execution plan as human-readable text
fn format_plan(plan: &ExecutionPlan) -> String {
    let mut output = String::new();

    output.push_str("Query Execution Plan:\n");
    output.push_str(&format!(
        "  1. Start with: {:?} (estimated cost: {:.2})\n",
        plan.start_with, plan.estimated_cost
    ));

    for (i, filter) in plan.then_filter.iter().enumerate() {
        output.push_str(&format!("  {}. Apply filter: {:?}\n", i + 2, filter));
    }

    output.push_str(&format!(
        "\nEstimated total cost: {:.2}\n",
        plan.estimated_cost
    ));

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let query = "MATCH (n:Person) WHERE n.city = 'Beijing' RETURN n";
        let predicates = parse_query_simple(query);

        assert_eq!(predicates.len(), 2);

        match &predicates[0] {
            FilterPredicate::HasLabel(label) => assert_eq!(label, "Person"),
            _ => panic!("Expected HasLabel"),
        }

        match &predicates[1] {
            FilterPredicate::PropertyEquals(prop, _) => assert_eq!(prop, "city"),
            _ => panic!("Expected PropertyEquals"),
        }
    }

    #[test]
    fn test_format_plan() {
        let plan = ExecutionPlan {
            start_with: FilterPredicate::HasLabel("Person".to_string()),
            then_filter: vec![FilterPredicate::PropertyEquals(
                "age".to_string(),
                nexora_id::PropertyValue::Integer(30),
            )],
            estimated_cost: 100.0,
        };

        let formatted = format_plan(&plan);
        assert!(formatted.contains("Person"));
        assert!(formatted.contains("age"));
        assert!(formatted.contains("100"));
    }
}
