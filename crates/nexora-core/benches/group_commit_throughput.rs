//! Group-commit throughput A/B: `WalSyncPolicy::Always` vs `Group`.
//!
//! This is the before/after proof for the group-commit change (#3). It drives
//! writes end-to-end through `GraphService` (so the durable barrier and the
//! background flusher are exercised), against a durable RocksDB persistor + WAL.
//!
//! The win shows up under **concurrent** writers: `Always` fsyncs once per
//! write and serializes on that syscall, while `Group` lets one fsync amortize
//! all writes that accumulated during it. The single-writer case is included as
//! a control — group commit should not regress it materially.
//!
//! Run: `cargo bench -p nexora-core --bench group_commit_throughput`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_core::wal::WalSyncPolicy;
use nexora_core::{GraphService, GraphServiceConfig};
use nexora_id::{NexoraId, PropertyValue};
use nexora_persistor_rocksdb::RocksDbPersistor;
use std::sync::Arc;
use std::time::Duration;

fn config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: 100_000,
        node_channel_size: 256,
    }
}

fn group() -> WalSyncPolicy {
    WalSyncPolicy::Group {
        max_ops: 256,
        max_delay: Duration::from_micros(500),
        max_bytes: None,
    }
}

/// Build a fresh durable service (RocksDB + WAL) under the given policy.
/// Returns the service plus the tempdir guard (kept alive for the run).
///
/// Constructed inside the runtime context (`rt.enter()`) because the
/// group-commit flusher is `tokio::spawn`ed at construction time and needs a
/// live reactor — mirroring production, where the service is built inside the
/// server's async runtime.
fn make_service(
    rt: &tokio::runtime::Runtime,
    policy: WalSyncPolicy,
) -> (Arc<GraphService>, tempfile::TempDir) {
    let _guard = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let persistor = Arc::new(RocksDbPersistor::open(dir.path().join("db")).unwrap());
    let svc = GraphService::new_with_wal_policy(
        config(),
        persistor,
        dir.path().join("wal"),
        None,
        policy,
    )
    .unwrap();
    (Arc::new(svc), dir)
}

/// Concurrent writers hammering distinct nodes. This is where group commit
/// should pull decisively ahead of per-write fsync.
fn bench_concurrent_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group_bench = c.benchmark_group("group_commit/concurrent");

    // Each iteration issues `writers * per_writer` durable writes.
    let writers = 32usize;
    let per_writer = 8usize;
    let total = (writers * per_writer) as u64;
    group_bench.throughput(Throughput::Elements(total));
    group_bench.sample_size(20);

    for (name, policy) in [("Always", WalSyncPolicy::Always), ("Group", group())] {
        group_bench.bench_with_input(BenchmarkId::from_parameter(name), &policy, |b, &policy| {
            let (svc, _dir) = make_service(&rt, policy);
            let counter = std::sync::atomic::AtomicU64::new(0);
            b.iter(|| {
                let base = counter.fetch_add(total, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(writers);
                    for w in 0..writers {
                        let svc = svc.clone();
                        handles.push(tokio::spawn(async move {
                            for k in 0..per_writer {
                                let n = base + (w * per_writer + k) as u64;
                                let qid = NexoraId::from_bytes(format!("cw-{n}").into_bytes());
                                svc.set_property(&qid, "v", PropertyValue::Integer(n as i64))
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
        });
    }
    group_bench.finish();
}

/// Single-writer control: sequential durable writes. Group commit must not
/// materially regress this (its added latency is bounded by `max_delay`).
fn bench_sequential_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group_bench = c.benchmark_group("group_commit/sequential");

    let n_writes = 200u64;
    group_bench.throughput(Throughput::Elements(n_writes));
    group_bench.sample_size(20);

    for (name, policy) in [("Always", WalSyncPolicy::Always), ("Group", group())] {
        group_bench.bench_with_input(BenchmarkId::from_parameter(name), &policy, |b, &policy| {
            let (svc, _dir) = make_service(&rt, policy);
            let counter = std::sync::atomic::AtomicU64::new(0);
            b.iter(|| {
                let base = counter.fetch_add(n_writes, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    for i in 0..n_writes {
                        let n = base + i;
                        let qid = NexoraId::from_bytes(format!("sw-{n}").into_bytes());
                        svc.set_property(&qid, "v", PropertyValue::Integer(n as i64))
                            .await
                            .unwrap();
                    }
                });
            });
        });
    }
    group_bench.finish();
}

criterion_group!(benches, bench_concurrent_writes, bench_sequential_writes);
criterion_main!(benches);
