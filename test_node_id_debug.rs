use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_sql::execute_sql;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    // Insert a node
    execute_sql(&graph, "INSERT INTO Person (name, age) VALUES ('Alice', 30)")
        .await
        .unwrap();

    // Try to select with id
    let result = execute_sql(&graph, "SELECT id, name FROM Person")
        .await
        .unwrap();

    println!("Columns: {:?}", result.columns);
    println!("Number of rows: {}", result.rows.len());

    if !result.rows.is_empty() {
        println!("First row length: {}", result.rows[0].len());
        for (i, val) in result.rows[0].iter().enumerate() {
            println!("  Column {}: {:?}", i, val);
        }

        // Try to get as_i64
        if let Some(first_val) = result.rows[0].get(0) {
            println!("First value: {:?}", first_val);
            println!("Is number: {}", first_val.is_number());
            println!("Is i64: {}", first_val.is_i64());
            println!("Is u64: {}", first_val.is_u64());
            println!("Is object: {}", first_val.is_object());

            if let Some(id) = first_val.as_i64() {
                println!("Got ID as i64: {}", id);
            } else {
                println!("Failed to convert to i64");
                println!("Raw value: {}", first_val);
            }
        }
    }
}
