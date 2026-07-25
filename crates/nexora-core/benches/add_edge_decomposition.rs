//! Diagnostic: per-write cost of the graph write path under real RocksDB is
//! dominated by the durability fsync, not by any per-write CPU component.
//!
//! ## What this benchmark set out to do vs. what it found
//!
//! The intent was to decompose a durable write into its parts — mailbox
//! round-trip, WAL commit, edge-index update, mutation callback, cold-node wake
//! — and size each share. The measured result is that they are **not
//! separable**, because one term swamps all the others:
//!
//!   - `steady_state/{wal_group, wal_always, callback}` and
//!     `cold_vs_resident/{cold, resident}` all land at ~4.3 ms per sequential
//!     write. Adding the mutation callback, switching WAL policy, or paying a
//!     cold `wake_node` moves the number by less than the run-to-run noise.
//!
//! ## Why: macOS `sync_data()` issues `F_FULLFSYNC` (~4.3 ms on Apple SSD)
//!
//! Rust's `File::sync_data()` maps to `F_FULLFSYNC` on macOS, which forces the
//! drive to flush its cache to stable media — ~4–5 ms on this hardware. That is
//! the ~4.3 ms floor every *sequential* durable write pays. Against it, the
//! other components are rounding error: edge-index insert ~815 ns (see
//! `edge_index_scaling`), a no-op mutation callback and the mailbox round-trip
//! well under 10 µs. Even a cold-node wake (RocksDB snapshot miss + task spawn)
//! doesn't stand out, because it too is under one fsync.
//!
//! (An `os.fsync`-based probe reports ~20 µs on the same disk — that is a plain
//! `fsync`, which does *not* flush the drive cache. It understates the real
//! `F_FULLFSYNC` cost by ~200x, which is why an earlier estimate here was wrong.)
//!
//! ## Consequence
//!
//! There is no per-write CPU component in the edge/write path worth optimizing
//! in isolation — the lever is fsync *amortization*, i.e. group commit (already
//! implemented). Group commit's win only appears under **concurrent** writers,
//! where one `F_FULLFSYNC` covers a whole batch; a sequential single-writer
//! micro-bench (this one) cannot batch, so all variants collapse to the fsync
//! floor. See `group_commit_throughput` for the concurrent A/B where the
//! batching actually pays off (~2.4x).
//!
//! This benchmark is kept as a platform/fsync-floor probe and a guard against
//! anyone "optimizing" a component that the fsync already dominates.
//!
//! Groups (all values are expected to sit near the single-write fsync floor):
//!   steady_state/wal_group    = mailbox + WAL(group, buffered commit)
//!   steady_state/wal_always   = mailbox + WAL(always, per-write F_FULLFSYNC)
//!   steady_state/callback     = mailbox + WAL(group) + no-op mutation callback
//!   cold_vs_resident/cold     = fresh node each iter (includes wake_node)
//!   cold_vs_resident/resident = pre-warmed resident node (no wake)
//!
//! Run: `cargo bench -p nexora-core --bench add_edge_decomposition`

use criterion::{criterion_group, criterion_main, Criterion};
use nexora_core::event::NodeChangeEvent;
use nexora_core::wal::WalSyncPolicy;
use nexora_core::{GraphService, GraphServiceConfig};
use nexora_id::{NexoraId, PropertyValue};
use std::sync::Arc;
use std::time::Duration;

const POOL: u64 = 4096;

fn config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: 2_000_000, // keep the whole pool resident
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

fn noop_callback() -> nexora_core::graph::GraphMutationCallback {
    Arc::new(|_id: NexoraId, _ev: NodeChangeEvent| {
        Box::pin(async {}) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
    })
}

fn make_service(
    rt: &tokio::runtime::Runtime,
    policy: WalSyncPolicy,
    with_cb: bool,
) -> (Arc<GraphService>, tempfile::TempDir) {
    let _guard = rt.enter();
    let dir = tempfile::tempdir().unwrap();
    let persistor =
        Arc::new(nexora_persistor_rocksdb::RocksDbPersistor::open(dir.path().join("db")).unwrap());
    let mut svc = GraphService::new_with_wal_policy(
        config(),
        persistor,
        dir.path().join("wal"),
        None,
        policy,
    )
    .unwrap();
    if with_cb {
        svc = svc.with_mutation_callback(noop_callback());
    }
    (Arc::new(svc), dir)
}

fn pool_id(i: u64) -> NexoraId {
    NexoraId::from_bytes(format!("pool-{}", i % POOL).into_bytes())
}

/// Pre-warm POOL nodes so they are resident (in memory + published in the
/// projection). Subsequent set_property on them takes the resident path.
fn prewarm(rt: &tokio::runtime::Runtime, svc: &GraphService) {
    rt.block_on(async {
        for i in 0..POOL {
            svc.set_property(&pool_id(i), "v", PropertyValue::Integer(0))
                .await
                .unwrap();
        }
    });
}

fn bench_steady_state(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut g = c.benchmark_group("add_edge_decomposition/steady_state");
    g.sample_size(50);

    let variants: [(&str, WalSyncPolicy, bool); 3] = [
        ("wal_group", group(), false),
        ("wal_always", WalSyncPolicy::Always, false),
        ("callback", group(), true),
    ];

    for (name, policy, with_cb) in variants {
        let (svc, _dir) = make_service(&rt, policy, with_cb);
        prewarm(&rt, &svc);
        let counter = std::sync::atomic::AtomicU64::new(0);
        g.bench_function(name, |b| {
            b.iter(|| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    // Reuse a resident node (no wake); overwrite one property
                    // (no state growth).
                    svc.set_property(&pool_id(n), "v", PropertyValue::Integer(n as i64))
                        .await
                        .unwrap();
                });
            });
        });
    }
    g.finish();
}

fn bench_cold_vs_resident(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut g = c.benchmark_group("add_edge_decomposition/cold_vs_resident");
    g.sample_size(30);

    // Cold: a fresh node id each iteration → wake_node (RocksDB snapshot miss +
    // task spawn) on every write.
    {
        let (svc, _dir) = make_service(&rt, group(), false);
        let counter = std::sync::atomic::AtomicU64::new(0);
        g.bench_function("cold", |b| {
            b.iter(|| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    let qid = NexoraId::from_bytes(format!("cold-{n}").into_bytes());
                    svc.set_property(&qid, "v", PropertyValue::Integer(n as i64))
                        .await
                        .unwrap();
                });
            });
        });
    }

    // Resident: pre-warmed pool, reused → no wake.
    {
        let (svc, _dir) = make_service(&rt, group(), false);
        prewarm(&rt, &svc);
        let counter = std::sync::atomic::AtomicU64::new(0);
        g.bench_function("resident", |b| {
            b.iter(|| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                rt.block_on(async {
                    svc.set_property(&pool_id(n), "v", PropertyValue::Integer(n as i64))
                        .await
                        .unwrap();
                });
            });
        });
    }
    g.finish();
}

criterion_group!(benches, bench_steady_state, bench_cold_vs_resident);
criterion_main!(benches);
