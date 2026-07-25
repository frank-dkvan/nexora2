//! Property index performance benchmarks
//!
//! Compares query performance with and without indexing

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::{
    GraphService, GraphServiceConfig, InMemoryPersistor, IndexConfig, PropertyIndex,
};
use nexora_cypher::execute_cypher;
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;

async fn create_indexed_graph(num_nodes: usize) -> (GraphService, PropertyIndex) {
    let config = GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: num_nodes / 4,
        node_channel_size: 256,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = GraphService::new(config, persistor);
    let index = PropertyIndex::new();

    // Create nodes and build index
    for i in 0..num_nodes {
        let qid = NexoraId::from_bytes(format!("node-{:06}", i).into_bytes());

        graph
            .set_property(&qid, "id", PropertyValue::Integer(i as i64))
            .await
            .unwrap();

        graph
            .set_property(&qid, "category", PropertyValue::Integer((i % 10) as i64))
            .await
            .unwrap();

        // Build index
        index
            .insert("id", PropertyValue::Integer(i as i64), qid.clone())
            .await
            .unwrap();
        index
            .insert(
                "category",
                PropertyValue::Integer((i % 10) as i64),
                qid.clone(),
            )
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
    }

    (graph, index)
}

/// Benchmark: Index lookup vs full scan
fn bench_index_vs_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("index_vs_scan");

    for num_nodes in [1_000, 10_000].iter() {
        let (graph, index) = rt.block_on(create_indexed_graph(*num_nodes));

        // With index
        group.bench_with_input(
            BenchmarkId::new("with_index", num_nodes),
            num_nodes,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let nodes = index
                            .query("category", &PropertyValue::Integer(5))
                            .await
                            .unwrap();
                        black_box(nodes);
                    });
                });
            },
        );

        // Without index (full Cypher scan)
        group.bench_with_input(
            BenchmarkId::new("cypher_scan", num_nodes),
            num_nodes,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = execute_cypher(
                            &graph,
                            "MATCH (n:TestNode) WHERE n.category = 5 RETURN n",
                        )
                        .await;
                        black_box(result.unwrap());
                    });
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Range query performance
fn bench_range_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("range_query");

    for num_nodes in [1_000, 10_000].iter() {
        let (_graph, index) = rt.block_on(create_indexed_graph(*num_nodes));

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let nodes = index
                        .range_query(
                            "id",
                            &PropertyValue::Integer(100),
                            &PropertyValue::Integer(200),
                        )
                        .await
                        .unwrap();
                    black_box(nodes);
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Cache hit rates (L1 vs L2)
fn bench_cache_tiers(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("cache_tiers");

    let config = IndexConfig {
        l1_promotion_threshold: 3,
        ..Default::default()
    };
    let index = PropertyIndex::new_with_config(config);

    // Populate index
    rt.block_on(async {
        for i in 0..1000 {
            let qid = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
            index
                .insert("id", PropertyValue::Integer(i), qid.clone())
                .await
                .unwrap();
        }

        // Warm up L1 cache for certain keys
        for _ in 0..5 {
            for i in 0..10 {
                index.query("id", &PropertyValue::Integer(i)).await.unwrap();
            }
        }
    });

    // Benchmark hot keys (L1 hits)
    group.bench_function("l1_hot_keys", |b| {
        b.iter(|| {
            rt.block_on(async {
                for i in 0..10 {
                    let nodes = index.query("id", &PropertyValue::Integer(i)).await.unwrap();
                    black_box(nodes);
                }
            });
        });
    });

    // Benchmark warm keys (L2 hits)
    group.bench_function("l2_warm_keys", |b| {
        b.iter(|| {
            rt.block_on(async {
                for i in 100..110 {
                    let nodes = index.query("id", &PropertyValue::Integer(i)).await.unwrap();
                    black_box(nodes);
                }
            });
        });
    });

    group.finish();
}

/// Benchmark: Index insert throughput
fn bench_insert_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("insert_throughput");

    group.bench_function("1000_inserts", |b| {
        b.iter(|| {
            rt.block_on(async {
                let index = PropertyIndex::new();
                for i in 0..1000 {
                    let qid = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
                    index
                        .insert("id", PropertyValue::Integer(i), qid.clone())
                        .await
                        .unwrap();
                }
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_index_vs_scan,
    bench_range_query,
    bench_cache_tiers,
    bench_insert_throughput,
);
criterion_main!(benches);
