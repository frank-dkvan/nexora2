//! #1 read-snapshot bypass: full-scan and point-read latency via the projection.
//!
//! Measures the read paths that the shared projection targets:
//!   - `read_node_state` across all resident nodes (the per-node work a Cypher
//!     full scan does), and
//!   - single-node point reads (`get_property`).
//!
//! Both now hit the lock-free projection for resident nodes instead of a
//! mailbox round-trip. There is no in-tree "before" harness to A/B against
//! (the getters were rewritten in place), so this benchmark establishes the
//! post-change latency curve at 1k/10k resident nodes — the baseline for any
//! future regression and the evidence for the projection's scan cost.
//!
//! Run: `cargo bench -p nexora-core --bench scan_projection`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;

/// Build a service with `n` resident nodes, each carrying a couple properties.
/// `max_nodes_per_shard` is kept above `n` so every node stays resident (the
/// projection's fast path); this isolates read cost from wake/evict churn.
fn make_populated(rt: &tokio::runtime::Runtime, n: usize) -> (Arc<GraphService>, Vec<NexoraId>) {
    let config = GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: n, // n nodes / 8 shards each, all resident
        node_channel_size: 256,
    };
    let svc = Arc::new(GraphService::new(
        config,
        Arc::new(InMemoryPersistor::new()),
    ));
    let ids: Vec<NexoraId> = (0..n)
        .map(|i| NexoraId::from_bytes(format!("scan-{i:07}").into_bytes()))
        .collect();
    rt.block_on(async {
        for (i, qid) in ids.iter().enumerate() {
            svc.set_property(qid, "v", PropertyValue::Integer(i as i64))
                .await
                .unwrap();
            svc.set_property(qid, "k", PropertyValue::String("x".into()))
                .await
                .unwrap();
        }
    });
    (svc, ids)
}

/// Full scan: read every resident node's state once (the per-node cost a
/// `MATCH (n)` snapshot build pays).
fn bench_full_scan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("scan_projection/full_scan");

    for &n in [1_000usize, 10_000].iter() {
        let (svc, ids) = make_populated(&rt, n);
        group.throughput(Throughput::Elements(n as u64));
        group.sample_size(20);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    for qid in &ids {
                        let state = svc.read_node_state(qid).await.unwrap();
                        criterion::black_box(state.properties.len());
                    }
                });
            });
        });
    }
    group.finish();
}

/// Point read: single-node `get_property`, projection fast path.
fn bench_point_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("scan_projection/point_read");

    let (svc, ids) = make_populated(&rt, 10_000);
    group.sample_size(50);
    group.bench_function("get_property_10k_resident", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let qid = &ids[i % ids.len()];
            i += 1;
            rt.block_on(async {
                let v = svc.get_property(qid, "v").await.unwrap();
                criterion::black_box(v);
            });
        });
    });
    group.finish();
}

criterion_group!(benches, bench_full_scan, bench_point_read);
criterion_main!(benches);
