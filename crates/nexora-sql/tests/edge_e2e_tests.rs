//! End-to-end tests for edge operations with actual graph execution.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::NexoraId;
use nexora_sql::execute_sql;
use std::sync::Arc;

fn setup_test_graph() -> Arc<GraphService> {
    Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ))
}

#[tokio::test]
async fn test_edge_workflow_generic() {
    let graph = setup_test_graph();

    // Step 1: Create nodes
    let result = execute_sql(
        &graph,
        "INSERT INTO Person (name, age) VALUES ('Alice', 30)",
    )
    .await
    .unwrap();
    assert_eq!(result.rows_affected, Some(1));

    let result = execute_sql(&graph, "INSERT INTO Person (name, age) VALUES ('Bob', 25)")
        .await
        .unwrap();
    assert_eq!(result.rows_affected, Some(1));

    // Step 2: Get node IDs
    let result = execute_sql(&graph, "SELECT id, name FROM Person ORDER BY name").await;
    if let Err(e) = &result {
        println!("Error: {:?}", e);
    }
    let result = result.unwrap();
    println!("Translated Cypher: {}", result.translated_cypher);
    assert_eq!(result.row_count, 2);

    let alice_id = result.rows[0][0].as_str().unwrap();
    let bob_id = result.rows[1][0].as_str().unwrap();

    // Step 3: Create edge using edge_KNOWS table
    let sql = format!(
        "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES ('{}', '{}', 2020)",
        alice_id, bob_id
    );
    let result = execute_sql(&graph, &sql).await.unwrap();
    println!("Edge insert Cypher: {}", result.translated_cypher);
    println!("Rows affected: {:?}", result.rows_affected);

    // Debug: Check if edge was actually created with properties
    let alice_qid = NexoraId::from_hex(alice_id).unwrap();
    let edges = graph.get_edges(&alice_qid).await.unwrap();
    println!("Alice's edges after insert: {:?}", edges);

    assert_eq!(result.rows_affected, Some(1));

    // DEBUG: Check if edge properties were actually saved
    let alice_qid = NexoraId::from_hex(alice_id).unwrap();
    let state = graph.read_node_state(&alice_qid).await.unwrap();
    println!(
        "Alice's edge_properties after insert: {:?}",
        state.edge_properties
    );

    // Step 3.5: Test querying edges without properties first
    let result_simple = execute_sql(&graph, "SELECT from_id, to_id FROM edge_KNOWS")
        .await
        .unwrap();
    println!("Simple edge query (no props): {:?}", result_simple.rows[0]);

    // Step 4: Query edges with properties
    let result = execute_sql(&graph, "SELECT from_id, to_id, since FROM edge_KNOWS")
        .await
        .unwrap();
    println!("Edge query Cypher: {}", result.translated_cypher);

    assert_eq!(result.row_count, 1);
    println!("Edge query result: {:?}", result.rows[0]);
    assert_eq!(result.rows[0][0].as_str().unwrap(), alice_id);
    assert_eq!(result.rows[0][1].as_str().unwrap(), bob_id);
    assert_eq!(result.rows[0][2].as_i64().unwrap(), 2020);

    // Step 5: Query with wildcard
    let result = execute_sql(&graph, "SELECT * FROM edge_KNOWS")
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert!(result.columns.contains(&"from_id".to_string()));
    assert!(result.columns.contains(&"to_id".to_string()));
    assert!(result.columns.contains(&"edge_type".to_string()));

    // Step 6: Delete edge
    let result = execute_sql(&graph, "DELETE FROM edge_KNOWS WHERE since < 2025").await;
    if let Err(e) = &result {
        println!("Delete error: {:?}", e);
    }
    let result = result.unwrap();
    assert!(result.rows_affected.is_some());

    // Step 7: Verify edge deleted
    let result = execute_sql(&graph, "SELECT * FROM edge_KNOWS")
        .await
        .unwrap();
    assert_eq!(result.row_count, 0);
}

#[tokio::test]
async fn test_edge_workflow_typed() {
    let graph = setup_test_graph();

    // Create nodes
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('Charlie')")
        .await
        .unwrap();
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('David')")
        .await
        .unwrap();

    // Get IDs
    let result = execute_sql(&graph, "SELECT id FROM Person ORDER BY name")
        .await
        .unwrap();
    let charlie_id = result.rows[0][0].as_str().unwrap();
    let david_id = result.rows[1][0].as_str().unwrap();

    // Create typed edge
    let sql = format!(
        "INSERT INTO Person_KNOWS_Person (from_id, to_id, relationship) VALUES ('{}', '{}', 'colleague')",
        charlie_id, david_id
    );
    execute_sql(&graph, &sql).await.unwrap();

    // Query typed edges
    let result = execute_sql(&graph, "SELECT * FROM Person_KNOWS_Person")
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
    println!("Typed edge result: {:?}", result);
}

#[tokio::test]
async fn test_all_edges_query() {
    let graph = setup_test_graph();

    // Create nodes
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('Eve')")
        .await
        .unwrap();
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('Frank')")
        .await
        .unwrap();
    execute_sql(&graph, "INSERT INTO Company (name) VALUES ('Acme')")
        .await
        .unwrap();

    // Get IDs
    let persons = execute_sql(&graph, "SELECT id FROM Person ORDER BY name")
        .await
        .unwrap();
    let eve_id = persons.rows[0][0].as_str().unwrap();
    let frank_id = persons.rows[1][0].as_str().unwrap();

    let companies = execute_sql(&graph, "SELECT id FROM Company").await.unwrap();
    let acme_id = companies.rows[0][0].as_str().unwrap();

    // Create multiple edge types
    let sql1 = format!(
        "INSERT INTO edge_KNOWS (from_id, to_id) VALUES ('{}', '{}')",
        eve_id, frank_id
    );
    execute_sql(&graph, &sql1).await.unwrap();

    let sql2 = format!(
        "INSERT INTO edge_WORKS_AT (from_id, to_id) VALUES ('{}', '{}')",
        eve_id, acme_id
    );
    execute_sql(&graph, &sql2).await.unwrap();

    // Query all edges
    let result = execute_sql(&graph, "SELECT edge_type FROM _edges")
        .await
        .unwrap();

    assert_eq!(result.row_count, 2);
    println!("All edges: {:?}", result);
}

#[tokio::test]
async fn test_edge_with_filter() {
    let graph = setup_test_graph();

    // Setup
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('Grace')")
        .await
        .unwrap();
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('Henry')")
        .await
        .unwrap();
    execute_sql(&graph, "INSERT INTO Person (name) VALUES ('Iris')")
        .await
        .unwrap();

    let persons = execute_sql(&graph, "SELECT id FROM Person ORDER BY name")
        .await
        .unwrap();
    let grace_id = persons.rows[0][0].as_str().unwrap();
    let henry_id = persons.rows[1][0].as_str().unwrap();
    let iris_id = persons.rows[2][0].as_str().unwrap();

    // Create edges with different years
    execute_sql(
        &graph,
        &format!(
            "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES ('{}', '{}', 2015)",
            grace_id, henry_id
        ),
    )
    .await
    .unwrap();

    execute_sql(
        &graph,
        &format!(
            "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES ('{}', '{}', 2022)",
            henry_id, iris_id
        ),
    )
    .await
    .unwrap();

    // Filter edges by year
    let result = execute_sql(&graph, "SELECT * FROM edge_KNOWS WHERE since > 2020")
        .await
        .unwrap();

    println!("Filter result: {:?}", result);
    println!("Translated Cypher: {}", result.translated_cypher);
    println!("Row count: {}", result.row_count);
    assert_eq!(result.row_count, 1);
}
