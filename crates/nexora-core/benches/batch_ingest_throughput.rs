//! block B throughput A/B: serial per-write ingest vs concurrent `write_batch`.
//!
//! The old ingest pattern is a serial loop — `for record { set_property().await }`
//! — which under group commit pays one fsync per write (no batching, since only
//! one write is ever in flight). `write_batch` fans the same writes out across
//! shards so one `F_FULLFSYNC` amortizes the whole batch.
//!
//! All variants write N distinct nodes to a durable RocksDB+WAL service:
//!   serial               = the old loop (WaitDurable, one in-flight)
//!   batch/wait_durable   = write_batch, concurrency 64, ack=durable
//!   batch/relaxed        = write_batch, concurrency 64, ack=buffered
//!
//! Expected: batch >> serial (fsync amortization); relaxed >= wait_durable.
//!
//! Run: `cargo bench -p nexora-core --bench batch_ingest_throughput`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_core::{
    BatchDurability, GraphService, GraphServiceConfig, MutationOp, WriteBatchOptions,
};
use nexora_id::{NexoraId, PropertyValue};
use nexora_persistor_rocksdb::RocksDbPersistor;
use nexora_value::Symbol;
use std::sync::Arc;

const N: usize = 1_000;

fn config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: 2_000_000,
        node_channel_size: 256,
    }
}

fn make_service(rt: &tokio::runtime::Runtime) -> (Arc<GraphService>, tempfile::TempDir) {
    let _g = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let persistor = Arc::new(RocksDbPersistor::open(dir.path().join("db")).unwrap());
    let svc =
        GraphService::new_with_wal(config(), persistor, dir.path().join("wal"), None).unwrap();
    (Arc::new(svc), dir)
}

fn batch_items(base: u64) -> Vec<(NexoraId, Vec<MutationOp>)> {
    (0..N)
        .map(|i| {
            let n = base + i as u64;
            (
                NexoraId::from_bytes(format!("bi-{n}").into_bytes()),
                vec![MutationOp::SetProperty {
                    key: Symbol::new("v"),
                    value: PropertyValue::Integer(n as i64),
                }],
            )
        })
        .collect()
}

fn bench_ingest(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut g = c.benchmark_group("batch_ingest");
    g.throughput(Throughput::Elements(N as u64));
    g.sample_size(10);

    // Serial: the old per-write loop.
    {
        let (svc, _dir) = make_service(&rt);
        let counter = std::sync::atomic::AtomicU64::new(0);
        g.bench_function(BenchmarkId::from_parameter("serial"), |b| {
            b.iter(|| {
                let base = counter.fetch_add(N as u64, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    for i in 0..N as u64 {
                        let n = base + i;
                        let qid = NexoraId::from_bytes(format!("bi-{n}").into_bytes());
                        svc.set_property(&qid, "v", PropertyValue::Integer(n as i64))
                            .await
                            .unwrap();
                    }
                });
            });
        });
    }

    // Batch, ack=durable.
    {
        let (svc, _dir) = make_service(&rt);
        let counter = std::sync::atomic::AtomicU64::new(0);
        g.bench_function(BenchmarkId::from_parameter("batch/wait_durable"), |b| {
            b.iter(|| {
                let base = counter.fetch_add(N as u64, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    svc.write_batch(
                        batch_items(base),
                        WriteBatchOptions {
                            concurrency: 64,
                            durability: BatchDurability::WaitDurable,
                        },
                    )
                    .await
                    .unwrap();
                });
            });
        });
    }

    // Batch, ack=buffered (relaxed).
    {
        let (svc, _dir) = make_service(&rt);
        let counter = std::sync::atomic::AtomicU64::new(0);
        g.bench_function(BenchmarkId::from_parameter("batch/relaxed"), |b| {
            b.iter(|| {
                let base = counter.fetch_add(N as u64, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    svc.write_batch(
                        batch_items(base),
                        WriteBatchOptions {
                            concurrency: 64,
                            durability: BatchDurability::Relaxed,
                        },
                    )
                    .await
                    .unwrap();
                });
            });
        });
    }

    g.finish();
}

criterion_group!(benches, bench_ingest);
criterion_main!(benches);
