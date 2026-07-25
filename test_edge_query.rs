// Quick test to see if cypher-parser can handle relationship queries

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
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

    // Create two nodes
    let cypher1 = "CREATE (a:Person {name: 'Alice', age: 30})";
    let result1 = nexora_cypher::execute_cypher(&graph, cypher1).await;
    println!("Create Alice: {:?}", result1);

    let cypher2 = "CREATE (b:Person {name: 'Bob', age: 25})";
    let result2 = nexora_cypher::execute_cypher(&graph, cypher2).await;
    println!("Create Bob: {:?}", result2);

    // Get their IDs
    let query_ids = "MATCH (n:Person) RETURN n";
    let result_ids = nexora_cypher::execute_cypher(&graph, query_ids).await;
    println!("Query nodes: {:?}", result_ids);

    // Try to create a relationship using SQL
    let sql_insert = "INSERT INTO edge_KNOWS (from_id, to_id, since) VALUES (1, 2, 2020)";
    let translated = nexora_sql::translate_sql_to_cypher(sql_insert);
    println!("\nSQL translation: {:?}", translated);

    if let Ok((cypher, _)) = translated {
        println!("Translated Cypher: {}", cypher);

        // Try to execute it
        let exec_result = nexora_cypher::execute_cypher(&graph, &cypher).await;
        println!("Execution result: {:?}", exec_result);
    }

    // Try edge query
    let edge_query = "MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b";
    println!("\n\nTrying edge query: {}", edge_query);
    let edge_result = nexora_cypher::execute_cypher(&graph, edge_query).await;
    println!("Edge query result: {:?}", edge_result);
}
