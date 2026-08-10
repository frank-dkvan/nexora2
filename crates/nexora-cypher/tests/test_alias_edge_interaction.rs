//! Interaction between the non-ASCII alias bridge and the edge-property bridge.
//!
//! `RETURN r.distance AS <alias>` exercises BOTH bridges at once: the alias
//! bridge rewrites the alias to an ASCII placeholder, and the edge-property
//! bridge must fill `r.distance` from the snapshot. The two must compose so the
//! aliased column carries the real edge-property value.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::{execute, CypherResult};
use std::sync::Arc;

fn make_graph() -> GraphService {
    GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 16,
        },
        Arc::new(InMemoryPersistor::new()),
    )
}

async fn rows(graph: &GraphService, q: &str) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    match execute(graph, q).await.expect("query ok") {
        CypherResult::Rows { columns, rows } => (columns, rows),
        other => panic!("expected rows, got {:?}", other),
    }
}

async fn seed(graph: &GraphService) {
    execute(
        graph,
        r#"CREATE (pvg:Airport {code:'PVG'}),
                 (lax:Airport {code:'LAX'}),
                 (pvg)-[:ROUTE_TO {distance: 11000}]->(lax)"#,
    )
    .await
    .expect("create ok");
}

/// Baseline: edge property with NO alias fills correctly (defect-1 fix).
#[tokio::test]
async fn edge_prop_no_alias_fills() {
    let graph = make_graph();
    seed(&graph).await;
    let (cols, r) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN r.distance",
    )
    .await;
    assert_eq!(cols, vec!["r.distance"]);
    assert_eq!(r[0][0], serde_json::json!(11000));
}

/// English alias on an edge property: column must be the alias AND carry the
/// real value. (Guards the pre-existing edge-fill-vs-alias gap.)
#[tokio::test]
async fn edge_prop_english_alias_fills() {
    let graph = make_graph();
    seed(&graph).await;
    let (cols, r) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN r.distance AS dist",
    )
    .await;
    assert_eq!(cols, vec!["dist"], "column should be the alias");
    assert_eq!(
        r[0][0],
        serde_json::json!(11000),
        "aliased edge-property column must carry the real value, not null"
    );
}

/// Non-ASCII alias on an edge property: both bridges compose — the column is the
/// original non-ASCII alias and carries the real edge-property value.
#[tokio::test]
async fn edge_prop_non_ascii_alias_fills() {
    let graph = make_graph();
    seed(&graph).await;
    let (cols, r) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code AS 代码, r.distance AS 距离",
    )
    .await;
    assert_eq!(cols, vec!["代码".to_string(), "距离".to_string()]);
    assert_eq!(r[0][0], serde_json::json!("PVG"));
    assert_eq!(
        r[0][1],
        serde_json::json!(11000),
        "non-ASCII aliased edge-property column must carry the real value"
    );
}
