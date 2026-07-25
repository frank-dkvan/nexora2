//! WAL overhead benchmark — measures pure WAL append throughput.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use nexora_core::{
    event::{NodeChangeEvent, TimedEvent},
    wal::{WalOperation, WriteAheadLog},
};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use std::time::Duration;

fn benchmark_wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Elements(1000));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("wal_append_1000", |b| {
        b.iter(|| {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut wal = WriteAheadLog::open(temp_dir.path()).unwrap();

            for i in 0..1000 {
                let qid = NexoraId::from_bytes(format!("n{i:04}").into_bytes());
                wal.append(WalOperation::NodeEvent {
                    qid,
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("val"),
                            value: PropertyValue::Integer(i),
                        },
                        EventTime::from_micros(i as u64 * 1000),
                    ),
                })
                .unwrap();
            }

            black_box(wal)
        });
    });

    group.finish();
}

fn benchmark_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    group.throughput(Throughput::Elements(1000));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("wal_replay_1000", |b| {
        // Pre-create WAL with 1000 records
        let temp_dir = tempfile::tempdir().unwrap();
        {
            let mut wal = WriteAheadLog::open(temp_dir.path()).unwrap();
            for i in 0..1000 {
                let qid = NexoraId::from_bytes(format!("n{i:04}").into_bytes());
                wal.append(WalOperation::NodeEvent {
                    qid,
                    event: TimedEvent::new(
                        NodeChangeEvent::PropertySet {
                            key: Symbol::new("val"),
                            value: PropertyValue::Integer(i),
                        },
                        EventTime::from_micros(i as u64 * 1000),
                    ),
                })
                .unwrap();
            }
        }

        b.iter(|| {
            let mut wal = WriteAheadLog::open(temp_dir.path()).unwrap();
            let records = wal.replay().unwrap();
            black_box(records)
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_wal_append, benchmark_wal_replay);
criterion_main!(benches);
