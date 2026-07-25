//! Test to understand cypher-parser's relationship handling

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::execute_cypher;
use std::sync::Arc;

#[tokio::test]
async fn test_relationship_query_capability() {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    // Create nodes
    execute_cypher(&graph, "CREATE (a:Person {name: 'Alice'})")
        .await
        .unwrap();
    execute_cypher(&graph, "CREATE (b:Person {name: 'Bob'})")
        .await
        .unwrap();

    // Create a relationship using write operation
    let create_rel = "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[r:KNOWS {since: 2020}]->(b)";
    let create_result = execute_cypher(&graph, create_rel).await;
    println!("Create relationship result: {:?}", create_result);

    // Try simple relationship query
    let rel_query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b";
    let rel_result = execute_cypher(&graph, rel_query).await;
    println!("Relationship query result: {:?}", rel_result);

    // Try with just returning the relationship
    let rel_only_query = "MATCH (a)-[r:KNOWS]->(b) RETURN r";
    let rel_only_result = execute_cypher(&graph, rel_only_query).await;
    println!("Relationship-only query result: {:?}", rel_only_result);

    // Try returning relationship properties
    let rel_prop_query = "MATCH (a)-[r:KNOWS]->(b) RETURN r.since";
    let rel_prop_result = execute_cypher(&graph, rel_prop_query).await;
    println!("Relationship property query result: {:?}", rel_prop_result);
}
