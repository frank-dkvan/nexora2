//! #4a: EdgeIndex insert cost vs index size.
//!
//! `add_edge` used to recompute stats on every insert by scanning every set
//! (`total_edges = forward.values().map(len).sum()`), so per-insert cost grew
//! with the number of edges already indexed — O(N) per insert, O(N^2) to build
//! N edges. Stats are now maintained incrementally, so per-insert cost should
//! be flat regardless of how many edges are already present.
//!
//! This benchmark inserts one more edge into an index pre-populated with
//! `preload` edges. Flat timings across `preload` sizes confirm the O(1) fix;
//! the old code's timings rose with `preload`.
//!
//! Run: `cargo bench -p nexora-core --bench edge_index_scaling`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::EdgeIndex;
use nexora_id::NexoraId;

fn node(i: u64) -> NexoraId {
    NexoraId::from_bytes(format!("n-{i}").into_bytes())
}

/// Build an index pre-loaded with `n` distinct edges under one edge type.
fn preloaded(rt: &tokio::runtime::Runtime, n: u64) -> EdgeIndex {
    let idx = EdgeIndex::new();
    rt.block_on(async {
        for i in 0..n {
            // Distinct (src, dst) pairs so each insert adds a new edge.
            idx.add_edge("KNOWS", node(i), node(i.wrapping_add(1_000_000)))
                .await;
        }
    });
    idx
}

fn bench_add_edge_vs_size(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("edge_index/add_edge_at_size");

    // Per-insert cost should be flat across these preload sizes after the fix.
    for &preload in [1_000u64, 10_000, 100_000].iter() {
        let idx = preloaded(&rt, preload);
        let counter = std::sync::atomic::AtomicU64::new(0);
        group.bench_with_input(BenchmarkId::from_parameter(preload), &preload, |b, _| {
            b.iter(|| {
                // Insert a fresh distinct edge each iteration so it is a real
                // insert (not a dedup no-op), measuring the add path at the
                // given index size.
                let k = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    idx.add_edge("KNOWS", node(2_000_000 + k), node(3_000_000 + k))
                        .await;
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_add_edge_vs_size);
criterion_main!(benches);
