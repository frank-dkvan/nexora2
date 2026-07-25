//! Query performance benchmarks
//!
//! Tests:
//! - Cypher MATCH query latency across different graph sizes
//! - Property filtering performance
//! - Graph traversal performance (1-hop, 2-hop, 3-hop)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_cypher::execute_cypher;
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

/// Create a test graph with N nodes
async fn create_graph_with_nodes(num_nodes: usize) -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: num_nodes / 4,
        node_channel_size: 256,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);

    // Create nodes with properties
    for i in 0..num_nodes {
        let qid = NexoraId::from_bytes(format!("node-{:06}", i).into_bytes());

        graph
            .set_property(&qid, "id", PropertyValue::Integer(i as i64))
            .await
            .unwrap();

        graph
            .set_property(&qid, "name", PropertyValue::String(format!("Node{}", i)))
            .await
            .unwrap();

        graph
            .set_property(&qid, "category", PropertyValue::Integer((i % 10) as i64))
            .await
            .unwrap();

        graph
            .set_property(
                &qid,
                "labels",
                PropertyValue::List(vec![PropertyValue::String("TestNode".into())]),
            )
            .await
            .unwrap();

        // Create edges (chain structure)
        if i > 0 {
            let prev_qid = NexoraId::from_bytes(format!("node-{:06}", i - 1).into_bytes());
            let edge = HalfEdge::out(Symbol::new("NEXT"), qid.clone());
            graph.add_edge(&prev_qid, edge).await.unwrap();
        }
    }

    graph
}

/// Benchmark: MATCH all nodes query
fn bench_match_all_nodes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("match_all_nodes");

    for num_nodes in [100, 1_000, 10_000].iter() {
        let graph = rt.block_on(create_graph_with_nodes(*num_nodes));

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = execute_cypher(&graph, "MATCH (n:TestNode) RETURN n").await;
                    black_box(result.unwrap());
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: MATCH with property filter
fn bench_match_with_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("match_with_filter");

    for num_nodes in [100, 1_000, 10_000].iter() {
        let graph = rt.block_on(create_graph_with_nodes(*num_nodes));

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result =
                        execute_cypher(&graph, "MATCH (n:TestNode) WHERE n.category = 5 RETURN n")
                            .await;
                    black_box(result.unwrap());
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Property-only query (no graph traversal)
fn bench_property_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("property_query");

    for num_nodes in [100, 1_000, 10_000].iter() {
        let graph = rt.block_on(create_graph_with_nodes(*num_nodes));

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result =
                        execute_cypher(&graph, "MATCH (n:TestNode) RETURN n.id, n.name LIMIT 100")
                            .await;
                    black_box(result.unwrap());
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Aggregation query
fn bench_aggregation_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("aggregation_query");

    for num_nodes in [100, 1_000, 10_000].iter() {
        let graph = rt.block_on(create_graph_with_nodes(*num_nodes));

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = execute_cypher(
                        &graph,
                        "MATCH (n:TestNode) RETURN COUNT(n), AVG(n.id), MAX(n.id)",
                    )
                    .await;
                    black_box(result.unwrap());
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Sorting and pagination
fn bench_order_limit_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("order_limit_query");

    for num_nodes in [100, 1_000, 10_000].iter() {
        let graph = rt.block_on(create_graph_with_nodes(*num_nodes));

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let result = execute_cypher(
                        &graph,
                        "MATCH (n:TestNode) RETURN n.id ORDER BY n.id DESC SKIP 10 LIMIT 20",
                    )
                    .await;
                    black_box(result.unwrap());
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: CREATE query (write performance)
fn bench_create_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("create_query");

    group.bench_function("create_10_nodes", |b| {
        b.iter(|| {
            rt.block_on(async {
                let config = GraphServiceConfig {
                    num_shards: 4,
                    max_nodes_per_shard: 1000,
                    node_channel_size: 64,
                };
                let persistor = Arc::new(InMemoryPersistor::new());
                let graph = GraphService::new(config, persistor);

                for i in 0..10 {
                    let query = format!("CREATE (n:TestNode {{id: {}, name: 'Node{}'}})", i, i);
                    let result: Result<_, _> = execute_cypher(&graph, &query).await;
                    black_box(result.unwrap());
                }
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_match_all_nodes,
    bench_match_with_filter,
    bench_property_query,
    bench_aggregation_query,
    bench_order_limit_query,
    bench_create_query,
);
criterion_main!(benches);
