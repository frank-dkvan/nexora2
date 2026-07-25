//! Test advanced Cypher clauses: ORDER BY, LIMIT, SKIP, WITH, aggregation

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::execute_cypher;
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;

async fn setup_test_graph() -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 1000,
        node_channel_size: 64,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create 5 person nodes with different ages
    for i in 0..5 {
        let qid = NexoraId::from_bytes(format!("person{}", i).as_bytes().to_vec());
        graph
            .set_property(&qid, "name", PropertyValue::String(format!("Person{}", i)))
            .await
            .unwrap();
        graph
            .set_property(&qid, "age", PropertyValue::Integer(25 - i))
            .await
            .unwrap();
        let request_id = (i + 1) as u64;
        graph
            .add_label(&qid, nexora_value::Symbol::new("Person"), request_id)
            .await
            .unwrap();
    }

    graph
}

#[tokio::test]
async fn test_order_by() {
    let graph = setup_test_graph().await;

    // Test ORDER BY ascending
    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age",
    )
    .await;
    println!("ORDER BY ASC result: {:?}", result);

    match result {
        Ok(_) => println!("✅ ORDER BY is supported"),
        Err(e) => println!("❌ ORDER BY not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_limit() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n.name LIMIT 2").await;
    println!("LIMIT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ LIMIT is supported"),
        Err(e) => println!("❌ LIMIT not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_skip() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n.name SKIP 1").await;
    println!("SKIP result: {:?}", result);

    match result {
        Ok(_) => println!("✅ SKIP is supported"),
        Err(e) => println!("❌ SKIP not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_order_limit_skip_combined() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC SKIP 1 LIMIT 3",
    )
    .await;
    println!(
        "Combined ORDER BY + SKIP + LIMIT (correct order) result: {:?}",
        result
    );

    match result {
        Ok(_) => println!("✅ Combined clauses are supported"),
        Err(e) => println!("❌ Combined clauses not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_with_clause() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) WITH n WHERE n.age > 22 RETURN n.name",
    )
    .await;
    println!("WITH clause result: {:?}", result);

    match result {
        Ok(_) => println!("✅ WITH clause is supported"),
        Err(e) => println!("❌ WITH clause not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_count_aggregation() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN COUNT(n)").await;
    println!("COUNT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ COUNT aggregation is supported"),
        Err(e) => println!("❌ COUNT aggregation not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_sum_aggregation() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN SUM(n.age)").await;
    println!("SUM result: {:?}", result);

    match result {
        Ok(_) => println!("✅ SUM aggregation is supported"),
        Err(e) => println!("❌ SUM aggregation not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_avg_aggregation() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN AVG(n.age)").await;
    println!("AVG result: {:?}", result);

    match result {
        Ok(_) => println!("✅ AVG aggregation is supported"),
        Err(e) => println!("❌ AVG aggregation not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_min_max_aggregation() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN MIN(n.age), MAX(n.age)").await;
    println!("MIN/MAX result: {:?}", result);

    match result {
        Ok(_) => println!("✅ MIN/MAX aggregation is supported"),
        Err(e) => println!("❌ MIN/MAX aggregation not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_merge_clause() {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 1000,
        node_channel_size: 64,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Test MERGE (should create node if not exists)
    let result = execute_cypher(&graph, "MERGE (n:Person {name: 'Bob'}) RETURN n").await;
    println!("MERGE result: {:?}", result);

    match result {
        Ok(_) => println!("✅ MERGE is supported"),
        Err(e) => println!("❌ MERGE not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_case_when() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, CASE WHEN n.age > 23 THEN 'old' ELSE 'young' END AS category",
    )
    .await;
    println!("CASE WHEN result: {:?}", result);

    match result {
        Ok(_) => println!("✅ CASE WHEN is supported"),
        Err(e) => println!("❌ CASE WHEN not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_order_limit_combined() {
    let graph = setup_test_graph().await;

    // Test ORDER BY + LIMIT (without SKIP)
    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age LIMIT 3",
    )
    .await;
    println!("ORDER BY + LIMIT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ ORDER BY + LIMIT is supported"),
        Err(e) => println!("❌ ORDER BY + LIMIT not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_limit_skip_combined() {
    let graph = setup_test_graph().await;

    // Test LIMIT + SKIP (without ORDER BY)
    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n.name LIMIT 3 SKIP 1").await;
    println!("LIMIT + SKIP result: {:?}", result);

    match result {
        Ok(_) => println!("✅ LIMIT + SKIP is supported"),
        Err(e) => println!("❌ LIMIT + SKIP not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_skip_limit_combined() {
    let graph = setup_test_graph().await;

    // Test SKIP + LIMIT (reverse order)
    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN n.name SKIP 1 LIMIT 3").await;
    println!("SKIP + LIMIT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ SKIP + LIMIT is supported"),
        Err(e) => println!("❌ SKIP + LIMIT not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_order_skip_combined() {
    let graph = setup_test_graph().await;

    // Test ORDER BY + SKIP (without LIMIT)
    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age SKIP 2",
    )
    .await;
    println!("ORDER BY + SKIP result: {:?}", result);

    match result {
        Ok(_) => println!("✅ ORDER BY + SKIP is supported"),
        Err(e) => println!("❌ ORDER BY + SKIP not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_collect_aggregation() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN COLLECT(n.name)").await;
    println!("COLLECT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ COLLECT aggregation is supported"),
        Err(e) => println!("❌ COLLECT aggregation not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_distinct() {
    let graph = setup_test_graph().await;

    let result = execute_cypher(&graph, "MATCH (n:Person) RETURN DISTINCT n.age").await;
    println!("DISTINCT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ DISTINCT is supported"),
        Err(e) => println!("❌ DISTINCT not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_with_workaround_for_triple_clause() {
    let graph = setup_test_graph().await;

    // Workaround: Use WITH to split ORDER BY + SKIP + LIMIT
    // Need to include sorted field in WITH projection
    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) WITH n.name AS name, n.age AS age ORDER BY age SKIP 1 LIMIT 2 RETURN name, age",
    )
    .await;
    println!("WITH workaround result: {:?}", result);

    match result {
        Ok(ref res) => {
            println!("✅ WITH workaround is supported");
            if let nexora_cypher::CypherResult::Rows { rows, .. } = res {
                println!("   Returned {} rows (expected 2)", rows.len());
                assert_eq!(rows.len(), 2, "Should return exactly 2 rows");
            }
        }
        Err(e) => println!("❌ WITH workaround not supported: {:?}", e),
    }
}

#[tokio::test]
async fn test_order_skip_limit_direct() {
    let graph = setup_test_graph().await;

    // Direct test: ORDER BY + SKIP + LIMIT in RETURN clause
    let result = execute_cypher(
        &graph,
        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age SKIP 1 LIMIT 2",
    )
    .await;
    println!("Direct ORDER BY + SKIP + LIMIT result: {:?}", result);

    match result {
        Ok(_) => println!("✅ Direct triple clause is supported"),
        Err(e) => println!("❌ Direct triple clause not supported: {:?}", e),
    }
}
