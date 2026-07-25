//! Cypher 集成测试 — 端到端：创建图 → 休眠节点 → 执行 Cypher 查询 → 验证结果

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::execute_cypher;
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;

fn make_graph() -> (GraphService, Arc<InMemoryPersistor>) {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 1000,
        node_channel_size: 64,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let svc = GraphService::new(config, persistor.clone());
    (svc, persistor)
}

#[tokio::test]
async fn test_cypher_basic_query() {
    let (graph, _persistor) = make_graph();
    let qid = NexoraId::from_bytes(b"alice".to_vec());

    // Create a node with properties
    graph
        .set_property(&qid, "name", PropertyValue::String("Alice".into()))
        .await
        .unwrap();
    graph
        .set_property(&qid, "age", PropertyValue::Integer(30))
        .await
        .unwrap();
    // P0.4: labels are first-class on NodeTask.labels, not a synthetic property.
    // The test must use add_label() to populate the label index.
    let request_id = 1u64;
    graph
        .add_label(&qid, nexora_value::Symbol::new("Person"), request_id)
        .await
        .unwrap();

    // Active nodes must be visible without forcing them to sleep.
    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n")
        .await
        .unwrap();

    match result {
        nexora_cypher::CypherResult::Rows { columns, rows } => {
            assert!(!columns.is_empty(), "Should have columns");
            assert!(
                !rows.is_empty(),
                "Should have at least one row for Person node"
            );
        }
        nexora_cypher::CypherResult::Empty => panic!("active Person node should be visible"),
        other => panic!("Unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_cypher_float_property() {
    // Float properties should be returned correctly
    let (graph, _) = make_graph();
    let qid = NexoraId::from_bytes(b"float-node".to_vec());
    graph
        .set_property(&qid, "score", PropertyValue::Float(1.75))
        .await
        .unwrap();

    // Query should succeed
    let result = execute_cypher(&graph, "MATCH (n) RETURN n.score").await;
    assert!(result.is_ok(), "Float property query should succeed");
}

#[tokio::test]
async fn test_cypher_write_create() {
    // CREATE now works via write_executor
    let (graph, _) = make_graph();
    let result = execute_cypher(&graph, "CREATE (n:Person {name: 'Alice'})").await;
    assert!(result.is_ok(), "CREATE should succeed: {:?}", result.err());
}

#[tokio::test]
async fn test_cypher_empty_graph() {
    let (graph, _) = make_graph();

    let result = execute_cypher(&graph, "MATCH (n) RETURN n").await.unwrap();
    match result {
        nexora_cypher::CypherResult::Rows { rows, .. } => {
            assert!(rows.is_empty(), "Empty graph should return no rows");
        }
        nexora_cypher::CypherResult::Empty => {}
        other => panic!("Unexpected result: {other:?}"),
    }
}

#[tokio::test]
async fn test_cypher_parse_error() {
    let (graph, _) = make_graph();
    let result = execute_cypher(&graph, "INVALID QUERY SYNTAX").await;
    assert!(result.is_err(), "Invalid syntax should fail");
}

/// Regression for the concurrent (order-preserving) snapshot build: a traversal
/// over many nodes+edges must return the same result as the old serial read
/// loop. The snapshot reads run via `buffered`, so if ordering were not
/// preserved the `idx_to_id` / edge index wiring would mismatch and the
/// traversal would drop or misroute edges.
#[tokio::test]
async fn test_cypher_traversal_snapshot_order_preserved() {
    use nexora_value::{HalfEdge, Symbol};

    let (graph, _) = make_graph();

    // Build a chain of 200 Person nodes: n0 -KNOWS-> n1 -KNOWS-> ... -> n199.
    // 200 > SNAPSHOT_READ_CONCURRENCY (64), so the fan-out spans several batches.
    let ids: Vec<NexoraId> = (0..200)
        .map(|i| NexoraId::from_bytes(format!("person-{i:03}").into_bytes()))
        .collect();
    for (i, id) in ids.iter().enumerate() {
        graph
            .set_property(id, "idx", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        graph
            .add_label(id, Symbol::new("Person"), i as u64 + 1)
            .await
            .unwrap();
    }
    for w in ids.windows(2) {
        graph
            .add_edge(&w[0], HalfEdge::out(Symbol::new("KNOWS"), w[1].clone()))
            .await
            .unwrap();
    }

    // All Person nodes are visible.
    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n")
        .await
        .unwrap();
    match result {
        nexora_cypher::CypherResult::Rows { rows, .. } => {
            assert_eq!(
                rows.len(),
                200,
                "all 200 Person nodes must be in the snapshot"
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }

    // Traversal must resolve edges correctly across the concurrent snapshot:
    // every node except the last has exactly one KNOWS successor → 199 pairs.
    let traversal = execute_cypher(&graph, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b")
        .await
        .unwrap();
    match traversal {
        nexora_cypher::CypherResult::Rows { rows, .. } => {
            assert_eq!(
                rows.len(),
                199,
                "chain of 200 nodes must yield 199 KNOWS edges after concurrent snapshot"
            );
        }
        nexora_cypher::CypherResult::Empty => panic!("traversal must not be empty"),
        other => panic!("unexpected result: {other:?}"),
    }
}
