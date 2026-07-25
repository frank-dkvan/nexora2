//! Ingest + Standing Query performance benchmark.
//!
//! Measures end-to-end throughput: JSON → Graph → SQ evaluation → Output.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_standing_query::{
    pattern::{FilterCondition, StandingQueryPattern},
    StandingQueryManager,
};
use std::sync::Arc;
use std::time::Duration;

fn benchmark_ingest_no_sq(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest");
    group.throughput(Throughput::Elements(1000));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("ingest_1000_nodes", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter(|| async {
            let graph = setup_graph();
            for i in 0..1000 {
                let qid = NexoraId::from_bytes(format!("node-{i:04}").into_bytes());
                graph
                    .set_property(&qid, "speed", PropertyValue::Float(i as f64 % 100.0))
                    .await
                    .unwrap();
                graph
                    .set_property(&qid, "zone", PropertyValue::String(format!("Z{}", i % 5)))
                    .await
                    .unwrap();
            }
            black_box(graph)
        });
    });

    group.finish();
}

fn benchmark_ingest_with_sq(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_with_sq");
    group.throughput(Throughput::Elements(1000));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("ingest_1000_with_sq", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter(|| async {
            let graph = setup_graph();
            let sqm = Arc::new(StandingQueryManager::new(1024));

            // Register SQ: speed > 50
            sqm.register(
                "fast",
                StandingQueryPattern::property("speed", FilterCondition::GreaterThan(50.0)),
            )
            .await;

            // Ingest with manual SQ evaluation
            for i in 0..1000 {
                let qid = NexoraId::from_bytes(format!("node-{i:04}").into_bytes());
                graph
                    .set_property(&qid, "speed", PropertyValue::Float(i as f64 % 100.0))
                    .await
                    .unwrap();

                // Manually trigger SQ
                let all_props = graph.get_all_properties(&qid).await.unwrap();
                let props_map: std::collections::HashMap<String, PropertyValue> = all_props
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect();
                if let Some(value) = props_map.get("speed") {
                    sqm.on_property_change(&qid, "speed", value, &props_map)
                        .await;
                }
            }

            black_box((graph, sqm))
        });
    });

    group.finish();
}

fn benchmark_wal_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Elements(100));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("wal_100_writes", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.to_async(&rt).iter(|| async {
            let temp_dir = tempfile::tempdir().unwrap();
            let config = GraphServiceConfig {
                num_shards: 4,
                max_nodes_per_shard: 1000,
                node_channel_size: 64,
            };
            let persistor = Arc::new(InMemoryPersistor::new());
            let graph = Arc::new(
                GraphService::new_with_wal(config, persistor, temp_dir.path().to_path_buf(), None)
                    .unwrap(),
            );

            for i in 0..100 {
                let qid = NexoraId::from_bytes(format!("wal-{i:03}").into_bytes());
                graph
                    .set_property(&qid, "val", PropertyValue::Integer(i))
                    .await
                    .unwrap();
            }

            black_box((graph, temp_dir))
        });
    });

    group.finish();
}

fn setup_graph() -> Arc<GraphService> {
    let config = GraphServiceConfig {
        num_shards: 16,
        max_nodes_per_shard: 10_000,
        node_channel_size: 64,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    Arc::new(GraphService::new(config, persistor))
}

criterion_group!(
    benches,
    benchmark_ingest_no_sq,
    benchmark_ingest_with_sq,
    benchmark_wal_overhead
);
criterion_main!(benches);
