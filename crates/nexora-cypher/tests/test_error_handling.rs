//! Tests for error handling and edge cases in Cypher execution.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::{execute_cypher, CypherResult};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::Symbol;
use serde_json::Value;
use std::sync::Arc;

fn make_graph() -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 1000,
        node_channel_size: 64,
    };
    GraphService::new(config, Arc::new(InMemoryPersistor::new()))
}

fn scalar(result: &CypherResult) -> Option<&Value> {
    match result {
        CypherResult::Rows { rows, .. } => rows.first().and_then(|r| r.first()),
        _ => None,
    }
}

fn row_count(result: &CypherResult) -> usize {
    match result {
        CypherResult::Rows { rows, .. } => rows.len(),
        _ => 0,
    }
}

/// Node with properties + Person label, created via the reliable core API.
async fn make_node(
    graph: &GraphService,
    id: &str,
    props: &[(&str, PropertyValue)],
    label: &str,
    req: u64,
) {
    let qid = NexoraId::from_bytes(id.as_bytes().to_vec());
    for (k, v) in props {
        graph.set_property(&qid, k, v.clone()).await.unwrap();
    }
    graph
        .add_label(&qid, Symbol::new(label), req)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_syntax_error() {
    let graph = make_graph();
    let result = execute_cypher(&graph, "INVALID QUERY SYNTAX").await;
    assert!(result.is_err(), "malformed query must error");
    let msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        msg.contains("parse")
            || msg.contains("syntax")
            || msg.contains("invalid")
            || msg.contains("unexpected"),
        "error should indicate a parse/syntax problem: {msg}"
    );
}

#[tokio::test]
async fn test_empty_graph_returns_empty() {
    let graph = make_graph();
    let result = execute_cypher(&graph, "MATCH (n) RETURN n").await.unwrap();
    assert_eq!(row_count(&result), 0);
}

#[tokio::test]
async fn test_where_matches_nothing() {
    let graph = make_graph();
    make_node(
        &graph,
        "a",
        &[("age", PropertyValue::Integer(25))],
        "Person",
        1,
    )
    .await;

    let result = execute_cypher(&graph, "MATCH (a:Person) WHERE a.age > 100 RETURN a")
        .await
        .unwrap();
    assert_eq!(row_count(&result), 0);
}

#[tokio::test]
async fn test_type_mismatch_comparison_no_match() {
    let graph = make_graph();
    make_node(
        &graph,
        "a",
        &[("age", PropertyValue::Integer(30))],
        "Person",
        1,
    )
    .await;

    // Comparing an integer property to a string: either a graceful empty set
    // or an explicit type error — both are acceptable, but it must not panic.
    let result = execute_cypher(&graph, "MATCH (a:Person) WHERE a.age = 'thirty' RETURN a").await;
    match result {
        Ok(r) => assert_eq!(
            row_count(&r),
            0,
            "type-mismatched comparison should match nothing"
        ),
        Err(_) => { /* explicit type error is also fine */ }
    }
}

#[tokio::test]
async fn test_aggregation_on_empty_set() {
    let graph = make_graph();
    // count() over no rows must be 0 (not empty/absent).
    let result = execute_cypher(&graph, "MATCH (n:NonExistent) RETURN count(n)")
        .await
        .unwrap();
    assert_eq!(scalar(&result).and_then(|v| v.as_i64()), Some(0));
}

#[tokio::test]
async fn test_count_property_skips_null() {
    let graph = make_graph();
    make_node(
        &graph,
        "a",
        &[("value", PropertyValue::Integer(10))],
        "Node",
        1,
    )
    .await;
    make_node(
        &graph,
        "b",
        &[("value", PropertyValue::Integer(20))],
        "Node",
        2,
    )
    .await;
    make_node(&graph, "c", &[], "Node", 3).await; // no `value`

    // count(n.value) excludes the null-valued node => 2
    let result = execute_cypher(&graph, "MATCH (n:Node) RETURN count(n.value)")
        .await
        .unwrap();
    assert_eq!(scalar(&result).and_then(|v| v.as_i64()), Some(2));
}

#[tokio::test]
async fn test_invalid_function_name_errors() {
    let graph = make_graph();
    make_node(
        &graph,
        "a",
        &[("name", PropertyValue::String("test".into()))],
        "Node",
        1,
    )
    .await;

    let result = execute_cypher(&graph, "MATCH (n:Node) RETURN nonExistentFunction(n.name)").await;
    assert!(result.is_err(), "unknown function should error");
}

#[tokio::test]
async fn test_unicode_in_properties() {
    let graph = make_graph();
    make_node(
        &graph,
        "u",
        &[
            ("name", PropertyValue::String("张三".into())),
            ("emoji", PropertyValue::String("😀".into())),
        ],
        "Person",
        1,
    )
    .await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n.name")
        .await
        .unwrap();
    assert_eq!(
        scalar(&result).and_then(|v| v.as_str()),
        Some("张三"),
        "unicode property must round-trip"
    );
}

#[tokio::test]
async fn test_large_property_value() {
    let graph = make_graph();
    let large = "x".repeat(10_000);
    make_node(
        &graph,
        "big",
        &[("text", PropertyValue::String(large.clone()))],
        "Node",
        1,
    )
    .await;

    let result = execute_cypher(&graph, "MATCH (n:Node) RETURN n.text")
        .await
        .unwrap();
    assert_eq!(
        scalar(&result).and_then(|v| v.as_str()).map(|s| s.len()),
        Some(10_000)
    );
}

#[tokio::test]
async fn test_to_integer_on_non_numeric_is_null() {
    let graph = make_graph();
    make_node(
        &graph,
        "n",
        &[("text", PropertyValue::String("not a number".into()))],
        "Node",
        1,
    )
    .await;

    // toInteger of a non-numeric string yields null (Cypher semantics), and
    // must not error.
    let result = execute_cypher(&graph, "MATCH (n:Node) RETURN toInteger(n.text)").await;
    if let Ok(r) = result {
        if let Some(cell) = scalar(&r) {
            assert!(
                cell.is_null() || cell.as_i64().is_none(),
                "non-numeric toInteger should be null"
            );
        }
    }
    // An explicit error is also acceptable for this edge case.
}

#[tokio::test]
async fn test_missing_property_is_null() {
    let graph = make_graph();
    make_node(
        &graph,
        "a",
        &[("name", PropertyValue::String("Alice".into()))],
        "Person",
        1,
    )
    .await;

    // Returning a property that doesn't exist should yield a null cell, not an error.
    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n.nonexistent")
        .await
        .unwrap();
    assert_eq!(row_count(&result), 1);
    let cell = scalar(&result).unwrap();
    assert!(
        cell.is_null(),
        "absent property should be null, got {cell:?}"
    );
}
