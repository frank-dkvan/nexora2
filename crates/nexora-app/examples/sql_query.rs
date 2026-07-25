//! Example 6: SQL query execution.
//!
//! Run with: `cargo run --example sql_query -p nexora-app`
//!
//! This example demonstrates:
//! - Creating nodes via Cypher
//! - Querying with SQL (translated to Cypher internally)
//! - Viewing the translated Cypher query

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 10_000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    println!("=== Nexora-RS SQL Query Example ===\n");

    // First, create some test data using Cypher
    let create_queries = vec![
        r#"CREATE (n:Person {name: 'Alice', age: 30, department: 'Engineering'})"#,
        r#"CREATE (n:Person {name: 'Bob', age: 25, department: 'Sales'})"#,
        r#"CREATE (n:Person {name: 'Charlie', age: 35, department: 'Engineering'})"#,
        r#"CREATE (n:Person {name: 'Diana', age: 28, department: 'Marketing'})"#,
        r#"CREATE (n:Person {name: 'Eve', age: 32, department: 'Engineering'})"#,
    ];

    println!("Creating test data...");
    for q in &create_queries {
        let _ = nexora_cypher::execute_cypher(&graph, q).await;
    }
    println!("Created 5 Person nodes\n");

    // SQL SELECT query
    println!("--- SQL: SELECT all persons ---");
    let result =
        nexora_sql::execute_sql(&graph, "SELECT name, age FROM Person ORDER BY age DESC").await?;

    println!("Columns: {:?}", result.columns);
    println!("Rows: {}", result.row_count);
    println!("Query time: {}ms", result.query_time_ms);
    if !result.translated_cypher.is_empty() {
        println!("Translated Cypher: {}", result.translated_cypher);
    }
    for row in &result.rows {
        println!("  {:?}", row);
    }

    // SQL with WHERE clause
    println!("\n--- SQL: Engineering department only ---");
    let result = nexora_sql::execute_sql(
        &graph,
        "SELECT name, age FROM Person WHERE department = 'Engineering' ORDER BY age",
    )
    .await?;

    println!("Rows: {}", result.row_count);
    for row in &result.rows {
        println!("  {:?}", row);
    }

    // SQL with aggregation
    println!("\n--- SQL: Count by department ---");
    let result = nexora_sql::execute_sql(
        &graph,
        "SELECT department, COUNT(*) as count FROM Person GROUP BY department",
    )
    .await?;

    println!("Rows: {}", result.row_count);
    for row in &result.rows {
        println!("  {:?}", row);
    }

    println!("\n=== SQL Query Example Complete ===");
    Ok(())
}
