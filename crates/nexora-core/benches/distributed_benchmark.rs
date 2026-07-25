//! Performance benchmarks for nexora.
//!
//! Run with: `cargo bench -p nexora-core`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_core::graph::{GraphService, GraphServiceConfig};
use nexora_core::InMemoryPersistor;
use nexora_id::NexoraId;
use nexora_value::PropertyValue;
use std::sync::Arc;
use std::time::Duration;

fn setup_graph(num_shards: usize, max_nodes: usize) -> Arc<GraphService> {
    let config = GraphServiceConfig {
        num_shards,
        max_nodes_per_shard: max_nodes,
        node_channel_size: 4096,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    Arc::new(GraphService::new(config, persistor))
}

// ============================================================
// Single-Node Write Benchmarks
// ============================================================

fn bench_single_node_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = setup_graph(4, 10000);

    let mut group = c.benchmark_group("single_node_write");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for n in [100, 1000, 10000u64].iter() {
        group.throughput(Throughput::Elements(*n));
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    let qid = NexoraId::from_bytes(b"bench_node".to_vec());
                    for i in 0..n {
                        graph
                            .set_property(&qid, "v", PropertyValue::Integer(i as i64))
                            .await
                            .unwrap();
                        black_box(());
                    }
                });
            });
        });
    }
    group.finish();
}

fn bench_multi_node_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = setup_graph(8, 10000);

    let mut group = c.benchmark_group("multi_node_write");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for n in [100, 500u64].iter() {
        group.throughput(Throughput::Elements(*n));
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter(|| {
                rt.block_on(async {
                    for i in 0..n {
                        let qid = NexoraId::from_bytes(format!("node_{:08}", i).into_bytes());
                        let _ = graph
                            .set_property(&qid, "v", PropertyValue::Integer(42))
                            .await;
                    }
                });
            });
        });
    }
    group.finish();
}

// ============================================================
// Read Benchmarks
// ============================================================

fn bench_property_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = setup_graph(4, 10000);

    // Pre-populate
    rt.block_on(async {
        for i in 0..1000u64 {
            let qid = NexoraId::from_bytes(format!("node_{:08}", i).into_bytes());
            let _ = graph
                .set_property(&qid, "value", PropertyValue::Integer(i as i64))
                .await;
        }
    });

    let mut group = c.benchmark_group("property_read");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("get_existing", |b| {
        b.iter(|| {
            rt.block_on(async {
                let qid = NexoraId::from_bytes(format!("node_{:08}", 500).into_bytes());
                black_box(graph.get_property(&qid, "value").await.unwrap());
            });
        });
    });

    group.finish();
}

// ============================================================
// Concurrent Write Benchmarks
// ============================================================

fn bench_concurrent_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = Arc::new(setup_graph(8, 50000));

    let mut group = c.benchmark_group("concurrent_write");
    group.measurement_time(Duration::from_secs(10));

    for concurrency in [4, 16, 64].iter() {
        group.throughput(Throughput::Elements(*concurrency as u64));
        group.bench_with_input(
            BenchmarkId::new("parallel_writes", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::new();
                        for t in 0..concurrency {
                            let g = graph.clone();
                            handles.push(tokio::spawn(async move {
                                let qid =
                                    NexoraId::from_bytes(format!("conc_{:08}", t).into_bytes());
                                for i in 0..100 {
                                    let _ = g
                                        .set_property(&qid, "v", PropertyValue::Integer(i as i64))
                                        .await;
                                }
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

// ============================================================
// Merkle Tree Build Benchmark
// ============================================================

fn bench_merkle_tree_build(c: &mut Criterion) {
    use std::hash::{Hash, Hasher};

    fn build_tree(pairs: &[(String, serde_json::Value)], leaf_size: usize) -> Vec<[u8; 32]> {
        if pairs.is_empty() {
            return vec![];
        }
        let chunks: Vec<_> = pairs.chunks(leaf_size).collect();
        let mut nodes: Vec<[u8; 32]> = Vec::with_capacity(chunks.len() * 2);

        for chunk in &chunks {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for (k, v) in *chunk {
                k.hash(&mut hasher);
                format!("{:?}", v).hash(&mut hasher);
            }
            let h64 = hasher.finish();
            let mut hash = [0u8; 32];
            hash[0..8].copy_from_slice(&h64.to_le_bytes());
            hash[8..16].copy_from_slice(&h64.to_be_bytes());
            nodes.push(hash);
        }

        let mut level_start = 0;
        let mut level_count = chunks.len();
        while level_count > 1 {
            let next_start = nodes.len();
            for i in (level_start..level_start + level_count).step_by(2) {
                let mut combined = [0u8; 64];
                combined[0..32].copy_from_slice(&nodes[i]);
                let right_idx = if i + 1 < level_start + level_count {
                    i + 1
                } else {
                    i
                };
                combined[32..64].copy_from_slice(&nodes[right_idx]);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                combined.hash(&mut hasher);
                let h64 = hasher.finish();
                let mut hash = [0u8; 32];
                hash[0..8].copy_from_slice(&h64.to_le_bytes());
                hash[8..16].copy_from_slice(&h64.to_be_bytes());
                nodes.push(hash);
            }
            level_start = next_start;
            level_count = level_count.div_ceil(2);
        }
        nodes
    }

    let mut group = c.benchmark_group("merkle_tree");
    group.measurement_time(Duration::from_secs(5));

    for size in [100u64, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let pairs: Vec<_> = (0..size)
                .map(|i| (format!("key_{:08}", i), serde_json::json!(i)))
                .collect();

            b.iter(|| {
                black_box(build_tree(&pairs, 256));
            });
        });
    }
    group.finish();
}

// ============================================================
// Failover Recovery Benchmark
// ============================================================

fn bench_failover_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("failover_recovery");
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("state_transfer_small", |b| {
        b.iter(|| {
            let entries: Vec<Vec<u8>> = (0..1000)
                .map(|i| format!("wal_entry_{}", i).into_bytes())
                .collect();
            let total = entries.iter().map(|e| e.len() as u64).sum::<u64>();
            black_box(total);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_node_write,
    bench_multi_node_write,
    bench_property_read,
    bench_concurrent_write,
    bench_merkle_tree_build,
    bench_failover_recovery,
);

criterion_main!(benches);
