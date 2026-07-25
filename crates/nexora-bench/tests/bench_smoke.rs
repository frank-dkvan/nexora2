//! Smoke tests for the nexora-bench crate.

use nexora_bench::util::{
    generate_random_ids, generate_seeded_ids, make_edge, make_test_graph, populate_edges,
    populate_nodes, timed, BenchConfig, LatencyStats,
};
use nexora_bench::{
    comparison::ComparisonRunner,
    tpc_graph::{TpcGraphBenchmark, TpcScaleFactor},
};
use nexora_id::{NexoraId, PropertyValue};
use std::time::Duration;

#[test]
fn test_generate_random_ids_deterministic() {
    let ids1 = generate_random_ids(100);
    let ids2 = generate_random_ids(100);
    assert_eq!(ids1, ids2, "generate_random_ids should be deterministic");
    assert_eq!(ids1.len(), 100);
}

#[test]
fn test_generate_seeded_ids_unique() {
    let ids = generate_seeded_ids(1000, 42);
    let set: std::collections::HashSet<&NexoraId> = ids.iter().collect();
    assert_eq!(set.len(), 1000, "All seeded IDs should be unique");
}

#[test]
fn test_make_edge() {
    let target = NexoraId::new_random();
    let edge = make_edge("KNOWS", target.clone());
    assert_eq!(edge.edge_type.as_str(), "KNOWS");
    assert!(edge.direction.is_out());
    assert_eq!(edge.other, target);
}

#[test]
fn test_timed() {
    let (val, elapsed) = timed(|| 42);
    assert_eq!(val, 42);
    // elapsed may be zero on very fast machines or under QEMU/CI
    // where the timer granularity is coarse. Relax to >= instead of >.
    assert!(elapsed >= Duration::ZERO);
}

#[test]
fn test_latency_stats_empty() {
    let mut empty = Vec::new();
    let stats = LatencyStats::from_durations(&mut empty);
    assert_eq!(stats.count, 0);
    assert_eq!(stats.p50_us, 0.0);
}

#[test]
fn test_latency_stats_basic() {
    let mut durations: Vec<Duration> = (0..100).map(|i| Duration::from_micros(i * 10)).collect();
    let stats = LatencyStats::from_durations(&mut durations);
    assert_eq!(stats.count, 100);
    assert_eq!(stats.min_us, 0.0);
    assert_eq!(stats.max_us, 990.0);
    assert!(stats.mean_us > 0.0);
    assert!(stats.p50_us > 0.0);
    assert!(stats.p95_us > stats.p50_us);
    assert!(stats.p99_us >= stats.p95_us);
}

#[test]
fn test_bench_config_default() {
    let cfg = BenchConfig::default();
    assert!(cfg.num_shards > 0);
    assert!(cfg.max_nodes_per_shard > 0);
    assert!(cfg.node_channel_size > 0);
}

#[tokio::test]
async fn test_make_test_graph_and_crud() {
    let svc = make_test_graph(&BenchConfig::default());
    let qid = NexoraId::new_random();

    svc.set_property(&qid, "name", PropertyValue::String("test".into()))
        .await
        .unwrap();

    let val = svc.get_property(&qid, "name").await.unwrap();
    assert_eq!(val, Some(PropertyValue::String("test".into())));
}

#[tokio::test]
async fn test_populate_nodes() {
    let svc = make_test_graph(&BenchConfig::default());
    let ids = generate_random_ids(100);
    populate_nodes(&svc, &ids).await;

    let val = svc.get_property(&ids[0], "name").await.unwrap();
    assert!(val.is_some());
}

#[tokio::test]
async fn test_populate_edges() {
    let svc = make_test_graph(&BenchConfig::default());
    let ids = generate_random_ids(50);
    populate_nodes(&svc, &ids).await;
    populate_edges(&svc, &ids, 3).await;

    let edges = svc.get_edges(&ids[0]).await.unwrap();
    assert!(!edges.is_empty());
}

#[test]
fn test_comparison_runner() {
    let runner = ComparisonRunner::new(BenchConfig {
        num_shards: 4,
        max_nodes_per_shard: 10_000,
        node_channel_size: 64,
    });
    // run_all creates its own tokio runtime internally
    let results = runner.run_all();
    assert!(!results.is_empty(), "Should have benchmark results");
    for r in &results {
        assert!(r.operations > 0, "Should have completed operations");
        assert!(
            r.throughput_ops_sec > 0.0,
            "Should have positive throughput"
        );
    }
}

#[tokio::test]
async fn test_tpc_graph_sf1() {
    let mut tpc = TpcGraphBenchmark::new(
        BenchConfig {
            num_shards: 4,
            max_nodes_per_shard: 10_000,
            node_channel_size: 64,
        },
        TpcScaleFactor::Sf1,
    );
    let svc = tpc.generate().await;

    // Verify graph was populated
    let node_ids = svc.all_node_ids().await.unwrap();
    assert!(!node_ids.is_empty(), "Graph should have nodes");

    // Run queries
    let results = tpc.run_all_queries(&svc).await;
    assert_eq!(results.len(), 7, "Should have 7 TPC query results");
    for r in &results {
        assert!(!r.query_name.is_empty());
        assert!(r.iterations > 0);
    }
}

#[test]
fn test_html_report_generation() {
    use nexora_bench::report::generate_html;

    let html = generate_html(&[], &[]);
    assert!(html.contains("<html"));
    assert!(html.contains("nexora"));

    // With data
    let comp_results = vec![nexora_bench::comparison::BenchmarkResult {
        operation: "test_op".into(),
        operations: 1000,
        throughput_ops_sec: 5000.0,
        latency: LatencyStats {
            count: 1000,
            min_us: 1.0,
            max_us: 100.0,
            mean_us: 10.0,
            p50_us: 8.0,
            p95_us: 50.0,
            p99_us: 90.0,
        },
        metadata: std::collections::HashMap::new(),
    }];
    let html = generate_html(&comp_results, &[]);
    assert!(html.contains("test_op"));
    assert!(html.contains("5.0K")); // format_number(5000) -> "5.0K"
}
