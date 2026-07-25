//! Concurrency benchmarks — 1/4/8/16/32 concurrent workers doing mixed read/write.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_bench::util::{generate_random_ids, make_test_graph, populate_nodes, BenchConfig};
use nexora_id::PropertyValue;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

fn bench_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency_mixed");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 5_000usize;
    let ops_per_worker = 500usize;

    // Pre-build graph
    let svc = Arc::new(make_test_graph(&BenchConfig {
        num_shards: 16,
        max_nodes_per_shard: 100_000,
        node_channel_size: 256,
    }));
    let ids = Arc::new(generate_random_ids(n));
    rt.block_on(populate_nodes(&svc, &ids));

    for &workers in &[1usize, 4, 8, 16, 32] {
        let total_ops = (workers * ops_per_worker) as u64;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.to_async(&rt).iter(|| {
                    let svc = svc.clone();
                    let ids = ids.clone();
                    async move {
                        let mut handles = Vec::new();
                        for w in 0..workers {
                            let svc = svc.clone();
                            let ids = ids.clone();
                            handles.push(tokio::spawn(async move {
                                let mut rng = StdRng::seed_from_u64(w as u64);
                                for _ in 0..ops_per_worker {
                                    let qid = ids.choose(&mut rng).unwrap();
                                    // 50% read, 50% write
                                    if rng.gen_bool(0.5) {
                                        let _ = svc.get_property(qid, "name").await.unwrap();
                                    } else {
                                        svc.set_property(
                                            qid,
                                            "score",
                                            PropertyValue::Float(rng.gen()),
                                        )
                                        .await
                                        .unwrap();
                                    }
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

fn bench_concurrent_writes_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency_writes");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 5_000usize;
    let ops_per_worker = 500usize;

    for &workers in &[1usize, 4, 8, 16] {
        let total_ops = (workers * ops_per_worker) as u64;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.to_async(&rt).iter(|| {
                    let config = BenchConfig {
                        num_shards: 16,
                        max_nodes_per_shard: 100_000,
                        node_channel_size: 256,
                    };
                    async move {
                        let svc = Arc::new(make_test_graph(&config));
                        let ids = Arc::new(generate_random_ids(n));
                        populate_nodes(&svc, &ids).await;

                        let mut handles = Vec::new();
                        for w in 0..workers {
                            let svc = svc.clone();
                            let ids = ids.clone();
                            handles.push(tokio::spawn(async move {
                                let mut rng = StdRng::seed_from_u64(w as u64);
                                for _ in 0..ops_per_worker {
                                    let qid = ids.choose(&mut rng).unwrap();
                                    svc.set_property(
                                        qid,
                                        "counter",
                                        PropertyValue::Integer(rng.gen()),
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

fn bench_concurrent_reads_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency_reads");
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 5_000usize;
    let ops_per_worker = 500usize;

    // Pre-build graph
    let svc = Arc::new(make_test_graph(&BenchConfig {
        num_shards: 16,
        max_nodes_per_shard: 100_000,
        node_channel_size: 256,
    }));
    let ids = Arc::new(generate_random_ids(n));
    rt.block_on(populate_nodes(&svc, &ids));

    for &workers in &[1usize, 4, 8, 16, 32] {
        let total_ops = (workers * ops_per_worker) as u64;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.to_async(&rt).iter(|| {
                    let svc = svc.clone();
                    let ids = ids.clone();
                    async move {
                        let mut handles = Vec::new();
                        for w in 0..workers {
                            let svc = svc.clone();
                            let ids = ids.clone();
                            handles.push(tokio::spawn(async move {
                                let mut rng = StdRng::seed_from_u64(w as u64);
                                for _ in 0..ops_per_worker {
                                    let qid = ids.choose(&mut rng).unwrap();
                                    let _ = svc.get_property(qid, "name").await.unwrap();
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

criterion_group!(
    benches,
    bench_mixed_read_write,
    bench_concurrent_writes_only,
    bench_concurrent_reads_only,
);
criterion_main!(benches);
