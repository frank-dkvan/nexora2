//! Concurrent write throughput benchmarks
//!
//! Tests multi-threaded write performance with different concurrency levels:
//! - 1 thread (baseline)
//! - 4 threads
//! - 8 threads
//! - 16 threads

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;

/// Create a test graph service
fn make_service() -> GraphService {
    GraphService::new(
        GraphServiceConfig {
            num_shards: 16,
            max_nodes_per_shard: 10_000,
            node_channel_size: 512,
        },
        Arc::new(InMemoryPersistor::new()),
    )
}

/// Benchmark: Concurrent property writes
fn bench_concurrent_property_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_property_writes");
    group.sample_size(20);

    let thread_counts = [1, 4, 8, 16];
    let writes_per_thread = 100;

    for num_threads in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_threads)
                    .build()
                    .unwrap();

                b.iter(|| {
                    rt.block_on(async {
                        let graph = Arc::new(make_service());
                        let mut handles = Vec::new();

                        for thread_id in 0..num_threads {
                            let g = graph.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..writes_per_thread {
                                    let qid = NexoraId::from_bytes(
                                        format!("t{}-n{}", thread_id, i).into_bytes(),
                                    );
                                    g.set_property(&qid, "value", PropertyValue::Integer(i as i64))
                                        .await
                                        .unwrap();
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

/// Benchmark: Concurrent edge creation
fn bench_concurrent_edge_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_edge_creation");
    group.sample_size(20);

    let thread_counts = [1, 4, 8, 16];
    let edges_per_thread = 50;

    for num_threads in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_threads)
                    .build()
                    .unwrap();

                b.iter(|| {
                    rt.block_on(async {
                        let graph = Arc::new(make_service());
                        let mut handles = Vec::new();

                        for thread_id in 0..num_threads {
                            let g = graph.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..edges_per_thread {
                                    let from = NexoraId::from_bytes(
                                        format!("edge-from-t{}-{}", thread_id, i).into_bytes(),
                                    );
                                    let to = NexoraId::from_bytes(
                                        format!("edge-to-t{}-{}", thread_id, i).into_bytes(),
                                    );
                                    let edge = nexora_value::HalfEdge::out(
                                        nexora_value::Symbol::new("LINK"),
                                        to,
                                    );
                                    g.add_edge(&from, edge).await.unwrap();
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

/// Benchmark: Mixed read/write workload
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(20);

    let thread_counts = [1, 4, 8, 16];
    let ops_per_thread = 100;

    for num_threads in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_threads)
                    .build()
                    .unwrap();

                b.iter(|| {
                    rt.block_on(async {
                        let graph = Arc::new(make_service());
                        let mut handles = Vec::new();

                        for thread_id in 0..num_threads {
                            let g = graph.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..ops_per_thread {
                                    let qid = NexoraId::from_bytes(
                                        format!("mixed-t{}-n{}", thread_id, i).into_bytes(),
                                    );

                                    // Write
                                    g.set_property(&qid, "x", PropertyValue::Integer(i as i64))
                                        .await
                                        .unwrap();

                                    // Read
                                    let _ = g.get_property(&qid, "x").await.unwrap();

                                    // Write again
                                    g.set_property(&qid, "y", PropertyValue::Integer(i as i64 * 2))
                                        .await
                                        .unwrap();
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

/// Benchmark: Contention on hot nodes (worst case)
fn bench_hot_node_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_node_contention");
    group.sample_size(20);

    let thread_counts = [1, 4, 8, 16];
    let updates_per_thread = 50;

    for num_threads in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_threads)
                    .build()
                    .unwrap();

                b.iter(|| {
                    rt.block_on(async {
                        let graph = Arc::new(make_service());
                        let hot_node = NexoraId::from_bytes(b"hot-node".to_vec());
                        let mut handles = Vec::new();

                        for thread_id in 0..num_threads {
                            let g = graph.clone();
                            let qid = hot_node.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..updates_per_thread {
                                    let key = format!("prop-t{}-{}", thread_id, i);
                                    g.set_property(&qid, &key, PropertyValue::Integer(i as i64))
                                        .await
                                        .unwrap();
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

/// Benchmark: Shard-distributed writes (best case)
fn bench_distributed_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_writes");
    group.sample_size(20);

    let thread_counts = [1, 4, 8, 16];
    let writes_per_thread = 100;

    for num_threads in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(num_threads)
                    .build()
                    .unwrap();

                b.iter(|| {
                    rt.block_on(async {
                        let graph = Arc::new(make_service());
                        let mut handles = Vec::new();

                        for thread_id in 0..num_threads {
                            let g = graph.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..writes_per_thread {
                                    // Generate node IDs that distribute across shards
                                    let qid = NexoraId::from_bytes(
                                        format!("dist-{:08x}", thread_id * 10000 + i).into_bytes(),
                                    );
                                    g.set_property(&qid, "data", PropertyValue::Integer(i as i64))
                                        .await
                                        .unwrap();
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

criterion_group!(
    benches,
    bench_concurrent_property_writes,
    bench_concurrent_edge_creation,
    bench_mixed_workload,
    bench_hot_node_contention,
    bench_distributed_writes,
);
criterion_main!(benches);
