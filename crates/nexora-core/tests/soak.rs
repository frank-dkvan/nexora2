//! E2: Long-running soak test framework.
//!
//! Runs sustained mixed load (writes + reads + edges) against a GraphService
//! while periodically injecting faults (node sleep/eviction churn), tracking a
//! liveness/consistency invariant throughout, and sampling resident-node counts
//! to surface unbounded memory growth (leak detection).
//!
//! ## Duration is configurable
//!
//! The default run is CI-friendly (a few seconds) so `cargo test` exercises the
//! harness on every build. For a real 72h+ soak, set the env var:
//!
//! ```sh
//! NEXORA_SOAK_SECS=259200 cargo test -p nexora-core --test soak -- --ignored --nocapture
//! ```
//!
//! The long variants are `#[ignore]` so they never run in the normal suite.
//! Both short and long share the SAME loop body, so the short run genuinely
//! validates the soak logic (not a separate toy path).

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Soak configuration: how long to run, how often to inject faults, and the
/// working-set size (bounds resident memory so leak detection is meaningful).
struct SoakConfig {
    duration: Duration,
    fault_interval: Duration,
    /// Distinct node ids the load cycles through (working set).
    key_space: u64,
    /// Max resident nodes before we consider it a leak (working set + slack).
    max_resident: usize,
}

/// Outcome of a soak run — the numbers an operator would inspect.
#[derive(Debug, Default)]
struct SoakReport {
    writes: u64,
    reads: u64,
    read_mismatches: u64,
    faults_injected: u64,
    peak_resident: usize,
}

fn make_service(num_shards: usize, max_nodes_per_shard: usize) -> Arc<GraphService> {
    Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards,
            max_nodes_per_shard,
            node_channel_size: 256,
        },
        Arc::new(InMemoryPersistor::new()),
    ))
}

/// Run the soak loop against `graph` under `config`. Returns a report; panics
/// (fails the test) only on an invariant violation — a read that returns a
/// value the writer never wrote, or resident memory exceeding the leak bound.
async fn run_soak(graph: Arc<GraphService>, config: SoakConfig) -> SoakReport {
    let mut report = SoakReport::default();
    let start = Instant::now();
    let mut last_fault = Instant::now();
    // Monotonic per-key version counter, so a read can verify it sees a value
    // that was actually written (read-your-writes under churn).
    let versions: Arc<Vec<AtomicU64>> =
        Arc::new((0..config.key_space).map(|_| AtomicU64::new(0)).collect());

    let mut iter: u64 = 0;
    while start.elapsed() < config.duration {
        let key_idx = iter % config.key_space;
        let qid = NexoraId::from_bytes(format!("soak-{key_idx}").into_bytes());

        // Write: bump this key's version and store it.
        let v = versions[key_idx as usize].fetch_add(1, Ordering::SeqCst) + 1;
        graph
            .set_property(&qid, "v", PropertyValue::Integer(v as i64))
            .await
            .expect("soak write must succeed");
        report.writes += 1;

        // Occasionally add an edge to exercise the edge path + grow state.
        if iter.is_multiple_of(5) {
            let target = NexoraId::from_bytes(
                format!("soak-{}", (key_idx + 1) % config.key_space).into_bytes(),
            );
            graph
                .add_edge(&qid, HalfEdge::out(Symbol::new("NEXT"), target))
                .await
                .expect("soak edge write must succeed");
        }

        // Read back: must see a version >= the one we just wrote for this key
        // (a concurrent-free single loop means exactly our write or newer). A
        // value below our write, or a missing value, is a correctness failure.
        let got = graph
            .get_property(&qid, "v")
            .await
            .expect("soak read must succeed");
        report.reads += 1;
        match got {
            Some(PropertyValue::Integer(read_v)) if read_v as u64 >= v => {}
            other => {
                // Record then fail — read_mismatches is part of the report
                // contract even though this path fails the test immediately.
                report.read_mismatches += 1;
                let _ = &report;
                panic!(
                    "soak read-your-writes violated for key {key_idx}: wrote v={v}, read {other:?}"
                );
            }
        }

        // Periodic fault injection: force sleep/eviction churn on a band of keys
        // so the wake path is exercised continuously (cold-read recovery).
        if last_fault.elapsed() >= config.fault_interval {
            for k in 0..config.key_space.min(16) {
                let victim = NexoraId::from_bytes(format!("soak-{k}").into_bytes());
                // sleep_node persists + evicts; next read must wake it correctly.
                let _ = graph.sleep_node(&victim).await;
            }
            report.faults_injected += 1;
            last_fault = Instant::now();
        }

        // Leak detection: resident nodes must stay bounded by the working set +
        // shard slack. An unbounded climb signals a projection/eviction leak.
        if iter.is_multiple_of(256) {
            let resident = resident_count(&graph).await;
            report.peak_resident = report.peak_resident.max(resident);
            assert!(
                resident <= config.max_resident,
                "resident nodes {resident} exceeded leak bound {} at iter {iter}",
                config.max_resident
            );
        }

        iter += 1;
        // Yield periodically so the runtime isn't monopolized on a single-thread
        // executor during long runs.
        if iter.is_multiple_of(1000) {
            tokio::task::yield_now().await;
        }
    }

    // Final resident sample for the report.
    report.peak_resident = report.peak_resident.max(resident_count(&graph).await);
    report
}

/// Count resident nodes across all shards (best-effort; used for leak trend).
async fn resident_count(graph: &GraphService) -> usize {
    graph.active_node_count().await
}

/// Short, always-on soak — validates the harness + invariants on every build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_short_smoke() {
    let secs = std::env::var("NEXORA_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(3);
    let graph = make_service(8, 500);
    let report = run_soak(
        graph,
        SoakConfig {
            duration: Duration::from_secs(secs),
            fault_interval: Duration::from_millis(200),
            key_space: 200,
            // working set 200 spread over 8 shards, max 500/shard → generous bound
            max_resident: 4000,
        },
    )
    .await;

    println!("soak report (short): {report:?}");
    assert!(report.writes > 0, "soak must have done work");
    assert_eq!(report.read_mismatches, 0, "no read-your-writes violations");
    assert!(report.faults_injected > 0, "faults must have been injected");
}

/// Long soak (72h+ when configured) — `#[ignore]` so it never runs in the
/// normal suite. Run explicitly with NEXORA_SOAK_SECS set.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "E2: long soak — run explicitly with NEXORA_SOAK_SECS set"]
async fn soak_long_running() {
    let secs = std::env::var("NEXORA_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(3600); // default 1h when invoked without env override
    println!("starting long soak for {secs}s");
    let graph = make_service(64, 2000);
    let report = run_soak(
        graph,
        SoakConfig {
            duration: Duration::from_secs(secs),
            fault_interval: Duration::from_secs(5),
            key_space: 10_000,
            max_resident: 200_000,
        },
    )
    .await;

    println!("soak report (long): {report:?}");
    assert_eq!(
        report.read_mismatches, 0,
        "no read-your-writes violations over long soak"
    );
}
