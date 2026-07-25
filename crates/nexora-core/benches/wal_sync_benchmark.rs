//! WAL sync policy comparison benchmark
//!
//! Compares throughput and latency across different WAL sync strategies:
//! - Always: fsync on every write (safest, slowest)
//! - EveryN: batch fsync every N writes (balanced)
//! - Never: no fsync (fastest, least durable)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::{
    event::{NodeChangeEvent, TimedEvent},
    wal::{WalOperation, WalSyncPolicy, WriteAheadLog},
};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use tempfile::tempdir;

/// Benchmark WAL append with different sync policies
fn bench_wal_sync_policies(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_sync_policy");

    let policies = vec![
        ("Never", WalSyncPolicy::Never),
        ("Every10", WalSyncPolicy::EveryN(10)),
        ("Every100", WalSyncPolicy::EveryN(100)),
        ("Always", WalSyncPolicy::Always),
    ];

    for (name, policy) in policies {
        group.bench_with_input(BenchmarkId::new("1000_writes", name), &name, |b, _| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let mut wal = WriteAheadLog::open_with_policy(dir.path(), policy).unwrap();

                for i in 0..1000 {
                    wal.append(WalOperation::NodeEvent {
                        qid: NexoraId::from_bytes(format!("sync-{}", i).into_bytes()),
                        event: TimedEvent::new(
                            NodeChangeEvent::PropertySet {
                                key: Symbol::new("value"),
                                value: PropertyValue::Integer(i),
                            },
                            EventTime::from_micros(i as u64),
                        ),
                    })
                    .unwrap();
                }
            });
        });
    }

    group.finish();
}

/// Benchmark WAL latency percentiles (P50, P90, P99)
fn bench_wal_latency_percentiles(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_latency");
    group.sample_size(100);

    let policies = vec![
        ("Never", WalSyncPolicy::Never),
        ("Every10", WalSyncPolicy::EveryN(10)),
        ("Always", WalSyncPolicy::Always),
    ];

    for (name, policy) in policies {
        group.bench_with_input(BenchmarkId::new("single_write", name), &name, |b, _| {
            let dir = tempdir().unwrap();
            let mut wal = WriteAheadLog::open_with_policy(dir.path(), policy).unwrap();
            let mut counter = 0u64;

            b.iter(|| {
                wal.append(WalOperation::NodeEvent {
                    qid: NexoraId::from_bytes(format!("lat-{}", counter).into_bytes()),
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("v"),
                            value: PropertyValue::Integer(counter as i64),
                        },
                        EventTime::from_micros(counter),
                    ),
                })
                .unwrap();
                counter += 1;
            });
        });
    }

    group.finish();
}

/// Benchmark WAL throughput (ops/sec) with different policies
fn bench_wal_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_throughput");
    group.sample_size(50);

    for batch_size in [100, 1_000, 10_000].iter() {
        for &policy in [
            WalSyncPolicy::Never,
            WalSyncPolicy::EveryN(100),
            WalSyncPolicy::Always,
        ]
        .iter()
        {
            let policy_name = match policy {
                WalSyncPolicy::Never => "Never",
                WalSyncPolicy::EveryN(n) => {
                    if n == 100 {
                        "Every100"
                    } else {
                        "EveryN"
                    }
                }
                WalSyncPolicy::Always => "Always",
                WalSyncPolicy::Group { .. } => "Group",
            };

            group.bench_with_input(
                BenchmarkId::new(format!("{}_{}", policy_name, batch_size), batch_size),
                batch_size,
                |b, &batch_size| {
                    b.iter(|| {
                        let dir = tempdir().unwrap();
                        let mut wal = WriteAheadLog::open_with_policy(dir.path(), policy).unwrap();

                        for i in 0..batch_size {
                            wal.append(WalOperation::NodeEvent {
                                qid: NexoraId::from_bytes(format!("tput-{}", i).into_bytes()),
                                event: TimedEvent::new(
                                    NodeChangeEvent::PropertySet {
                                        key: Symbol::new("data"),
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
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_sync_policies,
    bench_wal_latency_percentiles,
    bench_wal_throughput,
);
criterion_main!(benches);
