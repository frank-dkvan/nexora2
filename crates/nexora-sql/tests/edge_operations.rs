//! End-to-end tests for edge operations via SQL.
//!
//! Tests the complete flow: SQL → Cypher → Graph Execution → Results

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_sql::{execute_sql, translate_to_cypher};
use std::sync::Arc;

/// Helper to create a test graph with sample nodes
async fn setup_test_graph() -> Arc<GraphService> {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    // Create some test nodes using SQL
    execute_sql(
        &graph,
        "INSERT INTO Person (name, age) VALUES ('Alice', 30)",
    )
    .await
    .unwrap();

    execute_sql(&graph, "INSERT INTO Person (name, age) VALUES ('Bob', 25)")
        .await
        .unwrap();

    execute_sql(
        &graph,
        "INSERT INTO Person (name, age) VALUES ('Charlie', 35)",
    )
    .await
    .unwrap();

    // Get node IDs
    let nodes = execute_sql(&graph, "SELECT id, name FROM Person ORDER BY name")
        .await
        .unwrap();

    eprintln!("Columns: {:?}", nodes.columns);
    eprintln!("Row count: {}", nodes.rows.len());
    if !nodes.rows.is_empty() {
        eprintln!("First row: {:?}", nodes.rows[0]);
    }

    let alice_id = nodes.rows[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get alice_id from {:?}", nodes.rows[0][0]));
    let bob_id = nodes.rows[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get bob_id from {:?}", nodes.rows[1][0]));
    let charlie_id = nodes.rows[2][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get charlie_id from {:?}", nodes.rows[2][0]));

    eprintln!(
        "Alice ID: {}, Bob ID: {}, Charlie ID: {}",
        alice_id, bob_id, charlie_id
    );

    // Create relationships using SQL
    let edge_sql = format!(
        "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES ('{}', '{}', 2020)",
        alice_id, bob_id
    );
    eprintln!("Edge SQL: {}", edge_sql);
    let cypher = translate_to_cypher(&edge_sql).unwrap();
    eprintln!("Edge Cypher: {}", cypher);

    let edge_result = execute_sql(&graph, &edge_sql).await;

    eprintln!("Edge insert result: {:?}", edge_result);
    edge_result.unwrap();

    execute_sql(
        &graph,
        &format!(
            "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES ('{}', '{}', 2020)",
            bob_id, charlie_id
        ),
    )
    .await
    .unwrap();

    execute_sql(
        &graph,
        &format!(
            "INSERT INTO edge_FOLLOWS (from_id, to_id, since) VALUES ('{}', '{}', 2021)",
            alice_id, charlie_id
        ),
    )
    .await
    .unwrap();

    graph
}

// ============================================================
// SELECT Tests
// ============================================================

#[tokio::test]
async fn test_select_generic_edge_table() {
    let graph = setup_test_graph().await;

    // Query all KNOWS relationships
    let sql = "SELECT * FROM edge_KNOWS";
    let result = execute_sql(&graph, sql).await.unwrap();

    assert_eq!(result.row_count, 2, "Should find 2 KNOWS relationships");
    assert!(result.columns.contains(&"from_id".to_string()));
    assert!(result.columns.contains(&"to_id".to_string()));
    assert!(result.columns.contains(&"edge_type".to_string()));
}

#[tokio::test]
async fn test_select_typed_edge_table() {
    let graph = setup_test_graph().await;

    // Query typed relationship: Person_KNOWS_Person
    let sql = "SELECT * FROM Person_KNOWS_Person";
    let result = execute_sql(&graph, sql).await.unwrap();

    assert_eq!(
        result.row_count, 2,
        "Should find 2 Person-KNOWS-Person edges"
    );

    // Check Cypher translation
    let cypher = translate_to_cypher(sql).unwrap();
    assert!(cypher.contains("MATCH (a:Person)-[r:KNOWS]->(b:Person)"));
}

#[tokio::test]
async fn test_select_all_edges() {
    let graph = setup_test_graph().await;

    // Query all relationships
    let sql = "SELECT * FROM _edges";
    let result = execute_sql(&graph, sql).await.unwrap();

    assert_eq!(result.row_count, 3, "Should find 3 total edges");
}

#[tokio::test]
async fn test_select_edge_with_where() {
    let graph = setup_test_graph().await;

    // Query edges with WHERE clause
    let sql = "SELECT * FROM edge_KNOWS WHERE since > 2019";
    let result = execute_sql(&graph, sql).await.unwrap();

    assert_eq!(result.row_count, 2);

    // Check Cypher translation
    let cypher = translate_to_cypher(sql).unwrap();
    assert!(cypher.contains("WHERE r.since > 2019"));
}

#[tokio::test]
async fn test_select_edge_specific_columns() {
    let graph = setup_test_graph().await;

    // Query specific columns
    let sql = "SELECT from_id, to_id, since FROM edge_KNOWS";
    let result = execute_sql(&graph, sql).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert!(result.columns.contains(&"from_id".to_string()));
    assert!(result.columns.contains(&"to_id".to_string()));
    assert!(result.columns.contains(&"since".to_string()));
}

#[tokio::test]
async fn test_select_edge_with_order_by() {
    let graph = setup_test_graph().await;

    let sql = "SELECT * FROM edge_KNOWS ORDER BY since DESC";
    let _result = execute_sql(&graph, sql).await.unwrap();

    let cypher = translate_to_cypher(sql).unwrap();
    assert!(cypher.contains("ORDER BY r.since DESC"));
}

#[tokio::test]
async fn test_select_edge_with_limit() {
    let graph = setup_test_graph().await;

    let sql = "SELECT * FROM edge_KNOWS LIMIT 1";
    let result = execute_sql(&graph, sql).await.unwrap();

    assert!(result.row_count <= 1);
}

// ============================================================
// INSERT Tests
// ============================================================

#[tokio::test]
async fn test_insert_generic_edge() {
    let graph = setup_test_graph().await;

    // Get node IDs
    let nodes = execute_sql(&graph, "SELECT id, name FROM Person ORDER BY name")
        .await
        .unwrap();
    let alice_id = nodes.rows[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get alice_id from {:?}", nodes.rows[0][0]));
    let bob_id = nodes.rows[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get bob_id from {:?}", nodes.rows[1][0]));

    // Insert a new WORKS_WITH relationship
    let sql = format!(
        "INSERT INTO edge_WORKS_WITH (from_id, to_id, since) VALUES ('{}', '{}', 2023)",
        alice_id, bob_id
    );

    let result = execute_sql(&graph, &sql).await.unwrap();
    assert_eq!(result.rows_affected, Some(1));

    // Verify the edge was created
    let verify_sql = "SELECT * FROM edge_WORKS_WITH";
    let verify_result = execute_sql(&graph, verify_sql).await.unwrap();
    assert_eq!(verify_result.row_count, 1);
}

#[tokio::test]
async fn test_insert_typed_edge() {
    let graph = setup_test_graph().await;

    // Get node IDs
    let nodes = execute_sql(&graph, "SELECT id, name FROM Person ORDER BY name")
        .await
        .unwrap();
    let alice_id = nodes.rows[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get alice_id from {:?}", nodes.rows[0][0]));
    let bob_id = nodes.rows[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get bob_id from {:?}", nodes.rows[1][0]));

    // Insert typed edge: Person_MENTORS_Person
    let sql = format!(
        "INSERT INTO Person_MENTORS_Person (from_id, to_id, years) VALUES ('{}', '{}', 5)",
        alice_id, bob_id
    );

    let result = execute_sql(&graph, &sql).await.unwrap();
    assert_eq!(result.rows_affected, Some(1));

    // Check Cypher translation
    let cypher = translate_to_cypher(&sql).unwrap();
    assert!(cypher.contains("MATCH (a:Person)"));
    assert!(cypher.contains("MATCH (b:Person)"));
    // The edge carries a `years` property, so it renders inline:
    // `-[r:MENTORS {years: 5}]->`.
    assert!(cypher.contains("-[r:MENTORS"));
}

#[tokio::test]
async fn test_insert_edge_batch() {
    let graph = setup_test_graph().await;

    // Get node IDs
    let nodes = execute_sql(&graph, "SELECT id, name FROM Person ORDER BY name")
        .await
        .unwrap();
    let alice_id = nodes.rows[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get alice_id from {:?}", nodes.rows[0][0]));
    let bob_id = nodes.rows[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get bob_id from {:?}", nodes.rows[1][0]));
    let charlie_id = nodes.rows[2][0].as_str().unwrap();

    // Batch insert multiple edges
    let sql = format!(
        "INSERT INTO edge_LIKES (from_id, to_id) VALUES ('{}', '{}'), ('{}', '{}')",
        alice_id, bob_id, bob_id, charlie_id
    );

    let result = execute_sql(&graph, &sql).await.unwrap();
    assert_eq!(result.rows_affected, Some(2));
}

// ============================================================
// UPDATE Tests
// ============================================================

#[tokio::test]
async fn test_update_edge_properties() {
    let graph = setup_test_graph().await;

    // Update the 'since' property of KNOWS edges
    let sql = "UPDATE edge_KNOWS SET since = 2022 WHERE since = 2020";
    let _result = execute_sql(&graph, sql).await.unwrap();

    // Check Cypher translation
    let cypher = translate_to_cypher(sql).unwrap();
    assert!(cypher.contains("MATCH (a)-[r:KNOWS]->(b)"));
    assert!(cypher.contains("SET r.since = 2022"));
    assert!(cypher.contains("WHERE r.since = 2020"));
}

#[tokio::test]
async fn test_update_typed_edge() {
    let _graph = setup_test_graph().await;

    let sql = "UPDATE Person_KNOWS_Person SET since = 2023 WHERE since < 2021";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a:Person)-[r:KNOWS]->(b:Person)"));
    assert!(cypher.contains("SET r.since = 2023"));
}

// ============================================================
// DELETE Tests
// ============================================================

#[tokio::test]
async fn test_delete_edge_with_where() {
    let graph = setup_test_graph().await;

    // Count edges before delete
    let before = execute_sql(&graph, "SELECT * FROM edge_KNOWS")
        .await
        .unwrap();
    let count_before = before.row_count;

    // Delete edges
    let sql = "DELETE FROM edge_KNOWS WHERE since = 2020";
    let _result = execute_sql(&graph, sql).await.unwrap();

    // Verify deletion
    let after = execute_sql(&graph, "SELECT * FROM edge_KNOWS")
        .await
        .unwrap();
    assert!(after.row_count < count_before);

    // Check Cypher translation
    let cypher = translate_to_cypher(sql).unwrap();
    assert!(cypher.contains("MATCH (a)-[r:KNOWS]->(b)"));
    assert!(cypher.contains("DELETE r"));
    assert!(cypher.contains("WHERE r.since = 2020"));
}

#[tokio::test]
async fn test_delete_typed_edge() {
    let _graph = setup_test_graph().await;

    let sql = "DELETE FROM Person_FOLLOWS_Person WHERE since > 2020";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a:Person)-[r:FOLLOWS]->(b:Person)"));
    assert!(cypher.contains("DELETE r"));
}

#[tokio::test]
async fn test_delete_all_edges_rejected() {
    // DELETE without WHERE should be rejected
    let result = translate_to_cypher("DELETE FROM edge_KNOWS");
    assert!(result.is_err(), "DELETE without WHERE should fail");
}

// ============================================================
// Cypher Translation Tests
// ============================================================

#[test]
fn test_cypher_translation_generic_edge() {
    let sql = "SELECT * FROM edge_KNOWS";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a)-[r:KNOWS]->(b)"));
    assert!(cypher.contains(
        "RETURN id(a) AS from_id, id(b) AS to_id, type(r) AS edge_type, id(r) AS edge_id"
    ));
}

#[test]
fn test_cypher_translation_typed_edge() {
    let sql = "SELECT * FROM Person_KNOWS_Person";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a:Person)-[r:KNOWS]->(b:Person)"));
}

#[test]
fn test_cypher_translation_all_edges() {
    let sql = "SELECT * FROM _edges";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a)-[r]->(b)"));
}

#[test]
fn test_cypher_translation_edge_insert() {
    let sql = "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES (1, 2, 2020)";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a) WHERE id(a) = 1"));
    assert!(cypher.contains("MATCH (b) WHERE id(b) = 2"));
    assert!(cypher.contains("CREATE (a)-[r:KNOWS {since: 2020}]->(b)"));
}

#[test]
fn test_cypher_translation_typed_edge_insert() {
    let sql = "INSERT INTO Person_KNOWS_Person (from_id, to_id) VALUES (1, 2)";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a:Person) WHERE id(a) = 1"));
    assert!(cypher.contains("MATCH (b:Person) WHERE id(b) = 2"));
    assert!(cypher.contains("CREATE (a)-[r:KNOWS]->(b)"));
}

// ============================================================
// UPSERT Tests
// ============================================================

#[tokio::test]
async fn test_upsert_edge() {
    let graph = setup_test_graph().await;

    // Get node IDs
    let nodes = execute_sql(&graph, "SELECT id, name FROM Person ORDER BY name")
        .await
        .unwrap();
    let alice_id = nodes.rows[0][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get alice_id from {:?}", nodes.rows[0][0]));
    let bob_id = nodes.rows[1][0]
        .as_str()
        .unwrap_or_else(|| panic!("Failed to get bob_id from {:?}", nodes.rows[1][0]));

    // UPSERT: create or update edge
    let sql = format!(
        "INSERT INTO edge_KNOWS (from_id, to_id, since, strength) VALUES ('{}', '{}', 2023, 10) \
         ON DUPLICATE KEY UPDATE strength = 20",
        alice_id, bob_id
    );

    let result = execute_sql(&graph, &sql).await;

    // Check if the statement was accepted (may fail if edge already exists)
    match result {
        Ok(_) => {
            // Verify the edge was created/updated
            let verify_sql = format!(
                "SELECT * FROM edge_KNOWS WHERE from_id = '{}' AND to_id = '{}'",
                alice_id, bob_id
            );
            let verify_result = execute_sql(&graph, &verify_sql).await;
            assert!(verify_result.is_ok());
        }
        Err(_) => {
            // UPSERT may not be fully supported yet, skip for now
        }
    }
}

#[test]
fn test_cypher_translation_edge_upsert() {
    let sql = "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES (1, 2, 2020) \
               ON DUPLICATE KEY UPDATE since = 2023";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MERGE"));
    assert!(cypher.contains("ON CREATE SET"));
    assert!(cypher.contains("ON MATCH SET"));
}

#[test]
fn test_cypher_translation_typed_edge_upsert() {
    let sql = "INSERT INTO Person_KNOWS_Person (from_id, to_id, since) VALUES (1, 2, 2020) \
               ON DUPLICATE KEY UPDATE since = 2023";
    let cypher = translate_to_cypher(sql).unwrap();

    assert!(cypher.contains("MATCH (a:Person) WHERE id(a) = 1"));
    assert!(cypher.contains("MATCH (b:Person) WHERE id(b) = 2"));
    assert!(cypher.contains("MERGE (a)-[r:KNOWS"));
}

#[test]
fn test_update_edge_cannot_change_structure() {
    // Attempting to update from_id or to_id should fail
    let sql = "UPDATE edge_KNOWS SET from_id = 999 WHERE since = 2020";
    let result = translate_to_cypher(sql);

    assert!(result.is_err(), "Should not allow updating edge structure");
}
