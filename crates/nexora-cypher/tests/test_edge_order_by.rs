//! Isolate the ORDER-BY-on-edge-property defect with a single-rewrite path
//! (no HTTP double-rewrite). Creates a tiny graph, then compares edge-property
//! projection with and without ORDER BY referencing the edge property.

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

#[tokio::test]
async fn edge_property_order_by() {
    let graph = make_graph();

    // Build: two ROUTE_TO edges from PVG so ORDER BY has something to sort.
    // Insert the SMALLER distance first (JFK=9500 before LAX=11000) so that
    // insertion order is ASCending. Then ORDER BY r.distance DESC can only yield
    // [11000, 9500] if it genuinely sorts — insertion-order passthrough would
    // give [9500, 11000] and fail the assertion below. This disambiguates a real
    // sort from the fill merely restoring values in original row order.
    execute(
        &graph,
        r#"CREATE (pvg:Airport {code:'PVG'}),
                 (lax:Airport {code:'LAX'}),
                 (jfk:Airport {code:'JFK'}),
                 (pvg)-[:ROUTE_TO {distance: 9500}]->(jfk),
                 (pvg)-[:ROUTE_TO {distance: 11000}]->(lax)"#,
    )
    .await
    .expect("create ok");

    // A) No ORDER BY — known-good baseline.
    let (_c, r_plain) =
        rows(&graph, "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance").await;
    println!("PLAIN rows = {:?}", r_plain);
    let plain_distances: Vec<&serde_json::Value> = r_plain.iter().map(|row| &row[2]).collect();
    println!("PLAIN distances = {:?}", plain_distances);

    // B) ORDER BY the edge property — the failing case.
    let (_c2, r_ord) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance ORDER BY r.distance DESC",
    )
    .await;
    println!("ORDER rows = {:?}", r_ord);
    let ord_distances: Vec<&serde_json::Value> = r_ord.iter().map(|row| &row[2]).collect();
    println!("ORDER distances = {:?}", ord_distances);

    // Baseline must have real values.
    assert!(
        plain_distances.iter().all(|v| !v.is_null()),
        "baseline (no ORDER BY) should have non-null distances"
    );

    // Regression: ORDER BY on an edge property must still return real values.
    // The rewriter injects endpoint helper columns before the trailing ORDER BY
    // clause; a missing separator space used to glue "__nx_r_to" onto "ORDER",
    // breaking the parse and silently dropping the fill (all distances null).
    assert!(
        ord_distances.iter().all(|v| !v.is_null()),
        "ORDER BY edge-property should return non-null distances, got {:?}",
        ord_distances
    );

    // ORDER BY DESC must also actually sort: 11000 before 9500.
    assert_eq!(
        ord_distances,
        vec![&serde_json::json!(11000), &serde_json::json!(9500)],
        "ORDER BY r.distance DESC should sort edges high-to-low"
    );

    // C) SKIP/LIMIT is the other trailing clause the injection can glue onto.
    let (_c3, r_lim) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance ORDER BY r.distance DESC LIMIT 1",
    )
    .await;
    println!("LIMIT rows = {:?}", r_lim);
    assert_eq!(r_lim.len(), 1, "LIMIT 1 should return one row");
    assert_eq!(
        r_lim[0][2],
        serde_json::json!(11000),
        "LIMIT after ORDER BY DESC should keep the highest distance with its real value"
    );

    // D) ASC direction must sort low-to-high with real values.
    let (_c4, r_asc) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance ORDER BY r.distance ASC",
    )
    .await;
    let asc_distances: Vec<&serde_json::Value> = r_asc.iter().map(|row| &row[2]).collect();
    assert_eq!(
        asc_distances,
        vec![&serde_json::json!(9500), &serde_json::json!(11000)],
        "ORDER BY r.distance ASC should sort low-to-high, got {:?}",
        asc_distances
    );

    // E) SKIP + LIMIT together (Rust-side pagination path).
    let (_c5, r_skip) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance ORDER BY r.distance DESC SKIP 1 LIMIT 1",
    )
    .await;
    assert_eq!(r_skip.len(), 1, "SKIP 1 LIMIT 1 should return one row");
    assert_eq!(
        r_skip[0][2],
        serde_json::json!(9500),
        "SKIP 1 after DESC should drop 11000 and keep 9500"
    );

    // F) ORDER BY a NODE property (not an edge property) must NOT trigger the
    // Rust-side takeover — it stays on cypher-parser's path. Edge props still fill.
    let (_c6, r_node) = rows(
        &graph,
        "MATCH (a:Airport)-[r:ROUTE_TO]->(b:Airport) RETURN a.code, b.code, r.distance ORDER BY b.code ASC",
    )
    .await;
    println!("NODE-ORDER rows = {:?}", r_node);
    // b.code sorted ASC → JFK before LAX; edge distances still non-null and correct.
    assert_eq!(
        r_node.iter().map(|row| &row[1]).collect::<Vec<_>>(),
        vec![&serde_json::json!("JFK"), &serde_json::json!("LAX")],
        "ORDER BY b.code ASC should sort by node property"
    );
    assert!(
        r_node.iter().all(|row| !row[2].is_null()),
        "edge distance must remain filled even when ordering by a node property"
    );
}
