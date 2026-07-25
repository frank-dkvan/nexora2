//! Performance benchmarks for nexora-core.
//!
//! Covers: WAL write throughput, property read/write latency,
//! edge operations, shard concurrency, standing query matching rate.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::{
    event::{NodeChangeEvent, TimedEvent},
    wal::{WalOperation, WalSyncPolicy, WriteAheadLog},
    GraphService, GraphServiceConfig, InMemoryPersistor,
};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use std::sync::Arc;
use tempfile::tempdir;

/// Benchmark: WAL append throughput (ops/sec)
fn bench_wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    for num_records in [100, 1_000, 10_000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_records),
            num_records,
            |b, &n| {
                let dir = tempdir().unwrap();
                let mut wal = WriteAheadLog::open_with_policy(
                    dir.path(),
                    WalSyncPolicy::Never, // Fastest for benchmarking
                )
                .unwrap();

                b.iter(|| {
                    for i in 0..n {
                        wal.append(WalOperation::NodeEvent {
                            qid: NexoraId::from_bytes(format!("bench-{}", i).into_bytes()),
                            event: TimedEvent::new(
                                NodeChangeEvent::PropertySet {
                                    key: Symbol::new("v"),
                                    value: PropertyValue::Integer(i as i64),
                                },
                                EventTime::from_micros(i as u64),
                            ),
                        })
                        .unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: WAL replay throughput
fn bench_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_replay");
    for num_records in [100, 1_000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_records),
            num_records,
            |b, &n| {
                let dir = tempdir().unwrap();
                {
                    let mut wal =
                        WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
                    for i in 0..n {
                        wal.append(WalOperation::NodeEvent {
                            qid: NexoraId::from_bytes(format!("rbench-{}", i).into_bytes()),
                            event: TimedEvent::new(
                                NodeChangeEvent::PropertySet {
                                    key: Symbol::new("v"),
                                    value: PropertyValue::Integer(i as i64),
                                },
                                EventTime::from_micros(i as u64),
                            ),
                        })
                        .unwrap();
                    }
                }
                let mut wal = WriteAheadLog::open(dir.path()).unwrap();
                b.iter(|| {
                    wal.replay().unwrap();
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Node property set + get roundtrip
fn bench_property_rw(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = make_service();

    c.bench_function("property_set_get", |b| {
        let qid = NexoraId::from_bytes(b"prop-bench".to_vec());
        b.iter(|| {
            rt.block_on(async {
                graph
                    .set_property(&qid, "val", PropertyValue::Integer(42))
                    .await
                    .unwrap();
                let _ = graph.get_property(&qid, "val").await.unwrap();
            });
        });
    });
}

/// Benchmark: Edge creation throughput
fn bench_edge_creation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = make_service();
    let counter = std::sync::atomic::AtomicU64::new(0);

    c.bench_function("add_edge", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rt.block_on(async {
                let from = NexoraId::from_bytes(format!("e-from-{}", i).into_bytes());
                let to = NexoraId::from_bytes(format!("e-to-{}", i).into_bytes());
                let edge = nexora_value::HalfEdge::out(Symbol::new("BENCH_EDGE"), to);
                graph.add_edge(&from, edge).await.unwrap();
            });
        });
    });
}

/// Benchmark: Concurrent multi-node operations
fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = Arc::new(make_service());

    c.bench_function("concurrent_50_nodes", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();
                for i in 0..50 {
                    let g = graph.clone();
                    handles.push(tokio::spawn(async move {
                        let qid = NexoraId::from_bytes(format!("conc-bench-{:03}", i).into_bytes());
                        g.set_property(&qid, "x", PropertyValue::Integer(i as i64))
                            .await
                            .unwrap();
                        g.set_property(&qid, "y", PropertyValue::Integer(i as i64 * 2))
                            .await
                            .unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });
}

/// Benchmark: Shard-level routing throughput
fn bench_shard_routing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = make_service();

    c.bench_function("shard_routing_100_nodes", |b| {
        b.iter(|| {
            rt.block_on(async {
                for i in 0..100 {
                    let qid = NexoraId::from_bytes(format!("shard-{:04}", i).into_bytes());
                    graph
                        .set_property(&qid, "s", PropertyValue::Integer(i as i64))
                        .await
                        .unwrap();
                }
            });
        });
    });
}

fn make_service() -> GraphService {
    GraphService::new(
        GraphServiceConfig {
            num_shards: 8,
            max_nodes_per_shard: 10_000,
            node_channel_size: 256,
        },
        Arc::new(InMemoryPersistor::new()),
    )
}

criterion_group!(
    benches,
    bench_wal_append,
    bench_wal_replay,
    bench_property_rw,
    bench_edge_creation,
    bench_concurrent_operations,
    bench_shard_routing,
);
criterion_main!(benches);
