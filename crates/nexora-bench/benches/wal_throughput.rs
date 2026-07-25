//! WAL write throughput benchmarks — sequential writes, concurrent writes, recovery.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_bench::util::{generate_random_ids, BenchConfig};
use nexora_core::{GraphService, InMemoryPersistor};
use nexora_id::PropertyValue;
use std::sync::Arc;
use tempfile::TempDir;

fn bench_sequential_wal_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_sequential_writes");
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[1_000usize, 5_000, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let config = BenchConfig {
                    num_shards: 4,
                    max_nodes_per_shard: 100_000,
                    node_channel_size: 128,
                };
                async move {
                    let tmp = TempDir::new().unwrap();
                    let persistor = Arc::new(InMemoryPersistor::new());
                    let svc = GraphService::new_with_wal(
                        config.to_graph_config(),
                        persistor,
                        tmp.path().to_path_buf(),
                        None,
                    )
                    .unwrap();

                    let ids = generate_random_ids(n);
                    for (i, qid) in ids.iter().enumerate() {
                        svc.mutate_set_property(
                            qid,
                            "counter",
                            PropertyValue::Integer(i as i64),
                            i as u64,
                        )
                        .await
                        .unwrap();
                    }
                    black_box(&svc);
                }
            });
        });
    }
    group.finish();
}

fn bench_concurrent_wal_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_concurrent_writes");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 5_000usize;
    for &workers in &[1usize, 4, 8] {
        let total = n as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(
            BenchmarkId::new("workers", workers),
            &workers,
            |b, &workers| {
                b.to_async(&rt).iter(|| {
                    let config = BenchConfig {
                        num_shards: 16,
                        max_nodes_per_shard: 100_000,
                        node_channel_size: 128,
                    };
                    async move {
                        let tmp = TempDir::new().unwrap();
                        let persistor = Arc::new(InMemoryPersistor::new());
                        let svc = Arc::new(
                            GraphService::new_with_wal(
                                config.to_graph_config(),
                                persistor,
                                tmp.path().to_path_buf(),
                                None,
                            )
                            .unwrap(),
                        );

                        let ids = Arc::new(generate_random_ids(n));
                        let chunk = n / workers;
                        let mut handles = Vec::new();
                        for w in 0..workers {
                            let svc = svc.clone();
                            let ids = ids.clone();
                            handles.push(tokio::spawn(async move {
                                let start = w * chunk;
                                let end = if w == workers - 1 { n } else { start + chunk };
                                for i in start..end {
                                    let qid = &ids[i];
                                    svc.mutate_set_property(
                                        qid,
                                        "counter",
                                        PropertyValue::Integer(i as i64),
                                        i as u64,
                                    )
                                    .await
                                    .unwrap();
                                }
                            }));
                        }
                        for h in handles {
                            h.await.unwrap();
                        }
                        black_box(());
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_wal_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_recovery");
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[500usize, 2_000, 5_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| {
                let config = BenchConfig {
                    num_shards: 4,
                    max_nodes_per_shard: 100_000,
                    node_channel_size: 128,
                };
                async move {
                    let tmp = TempDir::new().unwrap();

                    // Phase 1: Write data
                    {
                        let persistor = Arc::new(InMemoryPersistor::new());
                        let svc = GraphService::new_with_wal(
                            config.to_graph_config(),
                            persistor,
                            tmp.path().to_path_buf(),
                            None,
                        )
                        .unwrap();
                        let ids = generate_random_ids(n);
                        for (i, qid) in ids.iter().enumerate() {
                            svc.mutate_set_property(
                                qid,
                                "val",
                                PropertyValue::Integer(i as i64),
                                i as u64,
                            )
                            .await
                            .unwrap();
                        }
                        svc.shutdown().await.unwrap();
                    }

                    // Phase 2: Reopen and replay
                    let persistor = Arc::new(InMemoryPersistor::new());
                    let svc = GraphService::new_with_wal(
                        config.to_graph_config(),
                        persistor,
                        tmp.path().to_path_buf(),
                        None,
                    )
                    .unwrap();
                    let replayed = svc.replay_all_wals().await.unwrap();
                    black_box(replayed);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_wal_writes,
    bench_concurrent_wal_writes,
    bench_wal_recovery,
);
criterion_main!(benches);
