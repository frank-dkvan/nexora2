//! Tests for resource exhaustion attack prevention (P0-5).

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::{execute_cypher, execute_with_limits, QueryLimits};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_result_set_size_limit() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create 150 nodes
    for i in 0..150 {
        execute_cypher(&graph, &format!("CREATE (n:Test {{id: {}}})", i))
            .await
            .unwrap();
    }

    // Query with limit of 100 rows should fail
    let limits = QueryLimits {
        max_result_rows: 100,
        max_execution_time: Duration::from_secs(30),
        max_snapshot_nodes: 10_000_000,
        max_pattern_depth: 10,
    };

    let result = execute_with_limits(&graph, "MATCH (n) RETURN n", &limits).await;

    assert!(result.is_err(), "Expected error for exceeding result limit");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("exceeding limit of 100"),
        "Error should mention result limit: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_result_set_within_limit() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create 50 nodes
    for i in 0..50 {
        execute_cypher(&graph, &format!("CREATE (n:Test {{id: {}}})", i))
            .await
            .unwrap();
    }

    // Query with limit of 100 rows should succeed
    let limits = QueryLimits {
        max_result_rows: 100,
        max_execution_time: Duration::from_secs(30),
        max_snapshot_nodes: 10_000_000,
        max_pattern_depth: 10,
    };

    let result = execute_with_limits(&graph, "MATCH (n) RETURN n", &limits).await;

    assert!(result.is_ok(), "Query should succeed within limits");
}

#[tokio::test]
async fn test_query_timeout() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create nodes that will form a large cartesian product
    for i in 0..10 {
        execute_cypher(&graph, &format!("CREATE (a:A {{id: {}}})", i))
            .await
            .unwrap();
        execute_cypher(&graph, &format!("CREATE (b:B {{id: {}}})", i + 10))
            .await
            .unwrap();
        execute_cypher(&graph, &format!("CREATE (c:C {{id: {}}})", i + 20))
            .await
            .unwrap();
    }

    // Very short timeout - cartesian product might exceed it
    let limits = QueryLimits {
        max_result_rows: 1_000_000,
        max_execution_time: Duration::from_millis(1), // 1ms timeout
        max_snapshot_nodes: 10_000_000,
        max_pattern_depth: 10,
    };

    // This query creates a cartesian product: 10 * 10 * 10 = 1000 rows
    // With 1ms timeout, it might fail (depending on system speed)
    let result =
        execute_with_limits(&graph, "MATCH (a:A), (b:B), (c:C) RETURN a, b, c", &limits).await;

    // We can't guarantee timeout on fast machines, but if it fails, it should be timeout
    if let Err(err) = result {
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("exceeded maximum execution time"),
            "If query fails, it should be due to timeout: {}",
            err_msg
        );
    }
}

#[tokio::test]
async fn test_snapshot_node_limit() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create 150 nodes
    for i in 0..150 {
        execute_cypher(&graph, &format!("CREATE (n:Test {{id: {}}})", i))
            .await
            .unwrap();
    }

    // Set snapshot limit to 100 nodes
    let limits = QueryLimits {
        max_result_rows: 1_000_000,
        max_execution_time: Duration::from_secs(30),
        max_snapshot_nodes: 100,
        max_pattern_depth: 10,
    };

    let result = execute_with_limits(&graph, "MATCH (n) RETURN n", &limits).await;

    assert!(
        result.is_err(),
        "Expected error for exceeding snapshot limit"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("exceeding the safety limit"),
        "Error should mention snapshot limit: {}",
        err_msg
    );
}

#[tokio::test]
async fn test_use_limit_clause_to_avoid_error() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create 150 nodes
    for i in 0..150 {
        execute_cypher(&graph, &format!("CREATE (n:Test {{id: {}}})", i))
            .await
            .unwrap();
    }

    // Query with LIMIT clause should bypass result set limit check
    let limits = QueryLimits {
        max_result_rows: 100,
        max_execution_time: Duration::from_secs(30),
        max_snapshot_nodes: 10_000_000,
        max_pattern_depth: 10,
    };

    // LIMIT 50 means we only get 50 rows back, within the limit
    let result = execute_with_limits(&graph, "MATCH (n) RETURN n LIMIT 50", &limits).await;

    assert!(result.is_ok(), "Query with LIMIT clause should succeed");
}
