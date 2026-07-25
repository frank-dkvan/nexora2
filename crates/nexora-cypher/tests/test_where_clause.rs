//! Tests for WHERE clause functionality (comparison, logical operators, NULL handling).

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::{execute_cypher, CypherResult};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::Symbol;
use serde_json::Value;
use std::sync::Arc;

fn make_graph() -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 1000,
        node_channel_size: 64,
    };
    GraphService::new(config, Arc::new(InMemoryPersistor::new()))
}

/// Extract the single scalar cell of a one-row, one-column result (e.g. `count(n)`).
fn scalar(result: &CypherResult) -> Option<&Value> {
    match result {
        CypherResult::Rows { rows, .. } => rows.first().and_then(|r| r.first()),
        _ => None,
    }
}

fn row_count(result: &CypherResult) -> usize {
    match result {
        CypherResult::Rows { rows, .. } => rows.len(),
        _ => 0,
    }
}

/// Create a labeled node with integer/bool/string properties.
async fn make_person(graph: &GraphService, seed: &[u8], props: &[(&str, PropertyValue)], req: u64) {
    let qid = NexoraId::from_bytes(seed.to_vec());
    for (k, v) in props {
        graph.set_property(&qid, k, v.clone()).await.unwrap();
    }
    graph
        .add_label(&qid, Symbol::new("Person"), req)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_where_comparison_operators() {
    let graph = make_graph();
    for i in 1..=10u64 {
        make_person(
            &graph,
            format!("n{i}").as_bytes(),
            &[
                ("age", PropertyValue::Integer((i * 10) as i64)),
                ("name", PropertyValue::String(format!("User{i}"))),
            ],
            i,
        )
        .await;
    }

    // equals: exactly one node has age 50
    let r = execute_cypher(&graph, "MATCH (n:Person) WHERE n.age = 50 RETURN count(n)")
        .await
        .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 1);

    // not equals: 9 of 10
    let r = execute_cypher(&graph, "MATCH (n:Person) WHERE n.age <> 50 RETURN count(n)")
        .await
        .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 9);

    // less than 40: ages 10,20,30 => 3
    let r = execute_cypher(&graph, "MATCH (n:Person) WHERE n.age < 40 RETURN count(n)")
        .await
        .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 3);

    // >= 70: ages 70,80,90,100 => 4
    let r = execute_cypher(&graph, "MATCH (n:Person) WHERE n.age >= 70 RETURN count(n)")
        .await
        .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 4);
}

#[tokio::test]
async fn test_where_logical_operators() {
    let graph = make_graph();
    for i in 1..=10u64 {
        make_person(
            &graph,
            format!("n{i}").as_bytes(),
            &[
                ("age", PropertyValue::Integer((i * 10) as i64)),
                ("active", PropertyValue::Boolean(i % 2 == 0)),
            ],
            i,
        )
        .await;
    }

    // AND: age>30 (40..100 => 7) AND active (even tens: 40,60,80,100 among those) => 4
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.age > 30 AND n.active = true RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 4);

    // OR: age<30 (10,20) OR age>80 (90,100) => 4
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.age < 30 OR n.age > 80 RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 4);

    // NOT active: odd i => 5
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE NOT n.active RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 5);
}

#[tokio::test]
async fn test_where_null_handling() {
    let graph = make_graph();
    for i in 1..=5u64 {
        let mut props = vec![("name", PropertyValue::String(format!("User{i}")))];
        if i % 2 == 0 {
            props.push(("email", PropertyValue::String(format!("user{i}@x.com"))));
        }
        make_person(&graph, format!("n{i}").as_bytes(), &props, i).await;
    }

    // IS NOT NULL: emails on i=2,4 => 2
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.email IS NOT NULL RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 2);

    // IS NULL: i=1,3,5 => 3
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.email IS NULL RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 3);

    // Short-circuit: IS NOT NULL guards CONTAINS — must not error on null rows.
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.email IS NOT NULL AND n.email CONTAINS 'user2' RETURN n.name",
    )
    .await;
    assert!(
        r.is_ok(),
        "short-circuit AND should not error on null email"
    );
}

#[tokio::test]
async fn test_where_string_matching() {
    let graph = make_graph();
    for (i, name) in ["Alice", "Bob", "Charlie", "David", "Eve"]
        .iter()
        .enumerate()
    {
        make_person(
            &graph,
            format!("n{i}").as_bytes(),
            &[("name", PropertyValue::String((*name).to_string()))],
            i as u64,
        )
        .await;
    }

    // STARTS WITH 'A' => Alice
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 1);

    // ENDS WITH 'e' => Alice, Charlie, Eve => 3
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.name ENDS WITH 'e' RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 3);

    // CONTAINS 'ar' => Charlie
    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.name CONTAINS 'ar' RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_where_in_operator() {
    let graph = make_graph();
    for i in 1..=10u64 {
        make_person(
            &graph,
            format!("n{i}").as_bytes(),
            &[("id", PropertyValue::Integer(i as i64))],
            i,
        )
        .await;
    }

    let r = execute_cypher(
        &graph,
        "MATCH (n:Person) WHERE n.id IN [2, 4, 6, 8] RETURN count(n)",
    )
    .await
    .unwrap();
    assert_eq!(scalar(&r).unwrap().as_i64().unwrap(), 4);
}

#[tokio::test]
async fn test_where_no_match_returns_empty() {
    let graph = make_graph();
    make_person(&graph, b"solo", &[("age", PropertyValue::Integer(25))], 1).await;

    let r = execute_cypher(&graph, "MATCH (n:Person) WHERE n.age > 100 RETURN n")
        .await
        .unwrap();
    assert_eq!(row_count(&r), 0);
}
