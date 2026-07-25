//! Tests for relationship traversal via Cypher.
//!
//! The graph (nodes + edges) is built with the core `GraphService` API
//! (`set_property`, `add_label`, `add_edge`) — the same reliable setup the
//! integration suite uses — and the Cypher *read/traversal* path is exercised
//! and asserted. This targets the previously-untested relationship pattern
//! matching, directions, and endpoint filtering.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::{execute_cypher, CypherResult};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

fn make_graph() -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 1000,
        node_channel_size: 64,
    };
    GraphService::new(config, Arc::new(InMemoryPersistor::new()))
}

fn scalar_i64(result: &CypherResult) -> Option<i64> {
    match result {
        CypherResult::Rows { rows, .. } => rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64()),
        _ => None,
    }
}

fn row_count(result: &CypherResult) -> usize {
    match result {
        CypherResult::Rows { rows, .. } => rows.len(),
        _ => 0,
    }
}

/// Create a `Person` node with a name and optional age.
async fn person(graph: &GraphService, name: &str, age: Option<i64>, req: u64) -> NexoraId {
    let qid = NexoraId::from_bytes(name.as_bytes().to_vec());
    graph
        .set_property(&qid, "name", PropertyValue::String(name.to_string()))
        .await
        .unwrap();
    if let Some(a) = age {
        graph
            .set_property(&qid, "age", PropertyValue::Integer(a))
            .await
            .unwrap();
    }
    graph
        .add_label(&qid, Symbol::new("Person"), req)
        .await
        .unwrap();
    qid
}

async fn edge(graph: &GraphService, src: &NexoraId, ty: &str, dst: &NexoraId) {
    graph
        .add_edge(src, HalfEdge::out(Symbol::new(ty), dst.clone()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_traverse_specific_relationship_type() {
    let graph = make_graph();
    let alice = person(&graph, "Alice", None, 1).await;
    let bob = person(&graph, "Bob", None, 2).await;
    let carol = person(&graph, "Carol", None, 3).await;
    edge(&graph, &alice, "KNOWS", &bob).await;
    edge(&graph, &bob, "KNOWS", &carol).await;
    edge(&graph, &alice, "WORKS_WITH", &carol).await;

    // KNOWS chain: Alice->Bob, Bob->Carol => 2
    let r = execute_cypher(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN count(*)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&r), Some(2));

    // WORKS_WITH: Alice->Carol => 1
    let r = execute_cypher(
        &graph,
        "MATCH (a:Person)-[:WORKS_WITH]->(b:Person) RETURN count(*)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&r), Some(1));
}

#[tokio::test]
async fn test_traverse_outgoing_direction() {
    let graph = make_graph();
    let alice = person(&graph, "Alice", None, 1).await;
    let bob = person(&graph, "Bob", None, 2).await;
    edge(&graph, &alice, "FOLLOWS", &bob).await;

    // Alice follows someone => 1
    let out = execute_cypher(
        &graph,
        "MATCH (a:Person {name: 'Alice'})-[:FOLLOWS]->(b) RETURN count(b)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&out), Some(1));

    // Bob follows no one (edge points Alice->Bob) => 0
    let none = execute_cypher(
        &graph,
        "MATCH (a:Person {name: 'Bob'})-[:FOLLOWS]->(b) RETURN count(b)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&none), Some(0));
}

#[tokio::test]
async fn test_count_outgoing_from_hub() {
    let graph = make_graph();
    let hub = person(&graph, "Hub", None, 1).await;
    for (i, name) in ["A", "B", "C"].iter().enumerate() {
        let n = person(&graph, name, None, (i + 2) as u64).await;
        edge(&graph, &hub, "KNOWS", &n).await;
    }

    let r = execute_cypher(
        &graph,
        "MATCH (hub:Person {name: 'Hub'})-[:KNOWS]->(friend) RETURN count(friend)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&r), Some(3));
}

#[tokio::test]
async fn test_relationship_endpoint_where_filter() {
    let graph = make_graph();
    let alice = person(&graph, "Alice", Some(30), 1).await;
    let bob = person(&graph, "Bob", Some(25), 2).await;
    let carol = person(&graph, "Carol", Some(35), 3).await;
    edge(&graph, &alice, "KNOWS", &bob).await;
    edge(&graph, &alice, "KNOWS", &carol).await;

    // Only Carol (35) passes age > 30.
    let r = execute_cypher(
        &graph,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) WHERE b.age > 30 RETURN count(b)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&r), Some(1));
}

#[tokio::test]
async fn test_match_missing_relationship_is_empty() {
    let graph = make_graph();
    person(&graph, "Solo", None, 1).await;

    let r = execute_cypher(&graph, "MATCH (a:Person)-[:KNOWS]->(b) RETURN b")
        .await
        .unwrap();
    assert_eq!(row_count(&r), 0);
}

#[tokio::test]
async fn test_multiple_edges_same_source() {
    let graph = make_graph();
    let alice = person(&graph, "Alice", None, 1).await;
    let bob = person(&graph, "Bob", None, 2).await;
    let carol = person(&graph, "Carol", None, 3).await;
    let dave = person(&graph, "Dave", None, 4).await;
    edge(&graph, &alice, "KNOWS", &bob).await;
    edge(&graph, &alice, "KNOWS", &carol).await;
    edge(&graph, &alice, "KNOWS", &dave).await;

    let r = execute_cypher(
        &graph,
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN count(b)",
    )
    .await
    .unwrap();
    assert_eq!(scalar_i64(&r), Some(3));
}
