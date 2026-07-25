//! Example 2: Cypher query execution.
//!
//! Run with: `cargo run --example cypher_query -p nexora-app`
//!
//! This example demonstrates:
//! - Creating nodes via Cypher CREATE
//! - Querying with MATCH/WHERE/RETURN
//! - Using aggregation functions
//! - Union queries
//! - Case When expressions

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

    println!("=== Nexora-RS Cypher Query Example ===\n");

    // Create nodes
    let queries = vec![
        r#"CREATE (n:Person {name: 'Alice', age: 30})"#,
        r#"CREATE (n:Person {name: 'Bob', age: 25})"#,
        r#"CREATE (n:Person {name: 'Charlie', age: 35})"#,
        r#"CREATE (n:Person {name: 'Diana', age: 28})"#,
    ];

    for q in &queries {
        match nexora_cypher::execute_cypher(&graph, q).await {
            Ok(_result) => println!("Executed: {}", q),
            Err(e) => println!("Error: {}", e),
        }
    }

    // Query all persons
    println!("\n--- All Persons ---");
    let result = nexora_cypher::execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC",
    )
    .await?;

    if let nexora_cypher::CypherResult::Rows { columns, rows } = result {
        println!("Columns: {:?}", columns);
        for row in &rows {
            println!("  {:?}", row);
        }
    }

    // Aggregation
    println!("\n--- Aggregation ---");
    let result = nexora_cypher::execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN COUNT(n), AVG(n.age), MIN(n.age), MAX(n.age)",
    )
    .await?;

    if let nexora_cypher::CypherResult::Rows { rows, .. } = result {
        for row in &rows {
            println!("  Count, Avg, Min, Max: {:?}", row);
        }
    }

    // Filter with WHERE
    println!("\n--- Filter: age > 28 ---");
    let result = nexora_cypher::execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.age > 28 RETURN n.name, n.age",
    )
    .await?;

    if let nexora_cypher::CypherResult::Rows { rows, .. } = result {
        for row in &rows {
            println!("  {:?}", row);
        }
    }

    println!("\n=== Cypher Query Example Complete ===");
    Ok(())
}
