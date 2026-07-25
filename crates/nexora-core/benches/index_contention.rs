//! Index lock-contention benchmark — isolates the three secondary indexes
//! (label / edge / property) from the rest of the engine so the effect of
//! sharding their locks is directly measurable.
//!
//! Each scenario spawns N concurrent tasks that write to *distinct* keys. Under
//! a single graph-wide `RwLock` these serialize on the index-update phase; under
//! the sharded `DashMap` they contend only on per-segment locks and should scale
//! with the worker count. To A/B the sharding: run this on the current tree,
//! then `git stash` the three index files (reverting to the global-lock design)
//! and run again — compare the multi-worker rows.
//!
//! Run: `cargo bench -p nexora-core --bench index_contention`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_core::{EdgeIndex, LabelIndex, PropertyIndex};
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;

const WORKER_COUNTS: [usize; 4] = [1, 4, 8, 16];
const WRITES_PER_WORKER: usize = 500;

fn rt(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.max(1))
        .build()
        .unwrap()
}

/// Concurrent label inserts to distinct labels (one label namespace per worker).
fn bench_label_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_contention_label_add");
    for &workers in WORKER_COUNTS.iter() {
        group.throughput(Throughput::Elements((workers * WRITES_PER_WORKER) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                let runtime = rt(workers);
                b.iter(|| {
                    runtime.block_on(async {
                        let index = Arc::new(LabelIndex::new());
                        let mut handles = Vec::with_capacity(workers);
                        for w in 0..workers {
                            let index = index.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..WRITES_PER_WORKER {
                                    let node =
                                        NexoraId::from_bytes(format!("w{w}-n{i}").into_bytes());
                                    index.add_label(format!("Label{w}"), node).await;
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

/// Concurrent edge inserts to distinct edge types (one type per worker).
fn bench_edge_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_contention_edge_add");
    for &workers in WORKER_COUNTS.iter() {
        group.throughput(Throughput::Elements((workers * WRITES_PER_WORKER) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                let runtime = rt(workers);
                b.iter(|| {
                    runtime.block_on(async {
                        let index = Arc::new(EdgeIndex::new());
                        let mut handles = Vec::with_capacity(workers);
                        for w in 0..workers {
                            let index = index.clone();
                            handles.push(tokio::spawn(async move {
                                let et = format!("TYPE{w}");
                                for i in 0..WRITES_PER_WORKER {
                                    let src =
                                        NexoraId::from_bytes(format!("w{w}-s{i}").into_bytes());
                                    let dst =
                                        NexoraId::from_bytes(format!("w{w}-d{i}").into_bytes());
                                    index.add_edge(&et, src, dst).await;
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

/// Concurrent property inserts to distinct properties (one property per worker).
fn bench_property_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_contention_property_insert");
    for &workers in WORKER_COUNTS.iter() {
        group.throughput(Throughput::Elements((workers * WRITES_PER_WORKER) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                let runtime = rt(workers);
                b.iter(|| {
                    runtime.block_on(async {
                        let index = Arc::new(PropertyIndex::new());
                        let mut handles = Vec::with_capacity(workers);
                        for w in 0..workers {
                            let index = index.clone();
                            handles.push(tokio::spawn(async move {
                                let prop = format!("prop{w}");
                                for i in 0..WRITES_PER_WORKER {
                                    let node =
                                        NexoraId::from_bytes(format!("w{w}-n{i}").into_bytes());
                                    index
                                        .insert(
                                            prop.clone(),
                                            PropertyValue::Integer(i as i64),
                                            node,
                                        )
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

/// Mixed read/write on the label index: half the workers query while the other
/// half write. This is where moving the query counter off the write lock (atomic
/// vs `stats.write()` per query) shows up — readers no longer serialize writers.
fn bench_label_mixed_rw(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_contention_label_mixed_rw");
    for &workers in WORKER_COUNTS.iter() {
        group.throughput(Throughput::Elements((workers * WRITES_PER_WORKER) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                let runtime = rt(workers);
                b.iter(|| {
                    runtime.block_on(async {
                        let index = Arc::new(LabelIndex::new());
                        // Seed a shared label so readers have something to hit.
                        index
                            .add_label("Shared", NexoraId::from_bytes(b"seed".to_vec()))
                            .await;
                        let mut handles = Vec::with_capacity(workers);
                        for w in 0..workers {
                            let index = index.clone();
                            handles.push(tokio::spawn(async move {
                                for i in 0..WRITES_PER_WORKER {
                                    if w % 2 == 0 {
                                        let node =
                                            NexoraId::from_bytes(format!("w{w}-n{i}").into_bytes());
                                        index.add_label(format!("Label{w}"), node).await;
                                    } else {
                                        let _ = index.query("Shared").await;
                                    }
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
    bench_label_contention,
    bench_edge_contention,
    bench_property_contention,
    bench_label_mixed_rw
);
criterion_main!(benches);
