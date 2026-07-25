//! Comparison benchmark framework for nexora-core.
//!
//! Runs a standardized set of operations and outputs results as JSON
//! (for cross-database comparison with Neo4j / ArangoDB) and as a
//! formatted table on stderr.
//!
//! Run with: `cargo bench -p nexora-core --bench comparison`
//!
//! JSON is written to `target/comparison_results.json` and also printed
//! to stdout for piping.

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================
// Result structures
// ============================================================

struct BenchResult {
    operation: String,
    latency_p50_ns: f64,
    latency_p99_ns: f64,
    latency_mean_ns: f64,
    throughput_ops_per_sec: f64,
    total_ops: usize,
}

// ============================================================
// Statistics helpers
// ============================================================

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)].as_nanos() as f64
}

fn mean(data: &[Duration]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let total_ns: u128 = data.iter().map(|d| d.as_nanos()).sum();
    total_ns as f64 / data.len() as f64
}

fn finalize(operation: &str, latencies: Vec<Duration>) -> BenchResult {
    let total_ns: f64 = latencies.iter().map(|d| d.as_nanos() as f64).sum();
    let n = latencies.len();
    BenchResult {
        operation: operation.to_string(),
        latency_p50_ns: percentile(&latencies, 0.50),
        latency_p99_ns: percentile(&latencies, 0.99),
        latency_mean_ns: mean(&latencies),
        throughput_ops_per_sec: if total_ns > 0.0 {
            1e9 * n as f64 / total_ns
        } else {
            0.0
        },
        total_ops: n,
    }
}

// ============================================================
// Graph helpers
// ============================================================

fn make_graph(num_shards: usize) -> GraphService {
    GraphService::new(
        GraphServiceConfig {
            num_shards,
            max_nodes_per_shard: 200_000,
            node_channel_size: 256,
        },
        Arc::new(InMemoryPersistor::new()),
    )
}

fn qid(prefix: &str, idx: usize) -> NexoraId {
    NexoraId::from_bytes(format!("{prefix}{idx:010}").into_bytes())
}

// ============================================================
// Individual benchmarks
// ============================================================

async fn bench_property_set(graph: &GraphService, n: usize) -> BenchResult {
    let mut latencies = Vec::with_capacity(n);
    for i in 0..n {
        let id = qid("set", i);
        let start = Instant::now();
        graph
            .set_property(&id, "key", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        latencies.push(start.elapsed());
    }
    latencies.sort();
    finalize("property_set", latencies)
}

async fn bench_property_get(graph: &GraphService, n: usize) -> BenchResult {
    // Pre-populate nodes
    for i in 0..n {
        let id = qid("get", i);
        graph
            .set_property(&id, "val", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }

    let mut latencies = Vec::with_capacity(n);
    for i in 0..n {
        let id = qid("get", i);
        let start = Instant::now();
        let _ = graph.get_property(&id, "val").await.unwrap();
        latencies.push(start.elapsed());
    }
    latencies.sort();
    finalize("property_get", latencies)
}

async fn bench_edge_add(graph: &GraphService, n: usize) -> BenchResult {
    // Pre-create source nodes
    for i in 0..n {
        let id = qid("eadd_src", i);
        graph
            .set_property(&id, "id", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }

    let mut latencies = Vec::with_capacity(n);
    for i in 0..n {
        let src = qid("eadd_src", i);
        let target = qid("eadd_tgt", i);
        let edge = HalfEdge::out(Symbol::new("LINK"), target);
        let start = Instant::now();
        graph.add_edge(&src, edge).await.unwrap();
        latencies.push(start.elapsed());
    }
    latencies.sort();
    finalize("edge_add", latencies)
}

async fn bench_edge_get(graph: &GraphService, n: usize, degree: usize) -> BenchResult {
    let node = qid("eget_node", 0);
    graph
        .set_property(&node, "id", PropertyValue::Integer(0))
        .await
        .unwrap();
    for i in 0..degree {
        let target = qid("eget_tgt", i);
        let edge = HalfEdge::out(Symbol::new("EDGE"), target);
        graph.add_edge(&node, edge).await.unwrap();
    }

    let mut latencies = Vec::with_capacity(n);
    for _ in 0..n {
        let start = Instant::now();
        let edges = graph.get_edges(&node).await.unwrap();
        latencies.push(start.elapsed());
        std::hint::black_box(edges.len());
    }
    latencies.sort();
    finalize("edge_get", latencies)
}

async fn bench_get_all_properties(graph: &GraphService, n: usize) -> BenchResult {
    // Pre-populate nodes with 5 properties each
    for i in 0..n {
        let id = qid("allprop", i);
        for k in 0..5u8 {
            let key = format!("k{k}");
            graph
                .set_property(&id, &key, PropertyValue::Integer(i as i64 + k as i64))
                .await
                .unwrap();
        }
    }

    let mut latencies = Vec::with_capacity(n);
    for i in 0..n {
        let id = qid("allprop", i);
        let start = Instant::now();
        let props = graph.get_all_properties(&id).await.unwrap();
        latencies.push(start.elapsed());
        std::hint::black_box(props.len());
    }
    latencies.sort();
    finalize("get_all_properties", latencies)
}

async fn bench_bulk_load(graph: &GraphService, n: usize) -> BenchResult {
    let start = Instant::now();
    for i in 0..n {
        let id = qid("bulk", i);
        graph
            .set_property(&id, "id", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        graph
            .set_property(&id, "name", PropertyValue::String(format!("Node{i}")))
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();

    BenchResult {
        operation: format!("bulk_load_{n}"),
        latency_p50_ns: elapsed.as_nanos() as f64 / n as f64,
        latency_p99_ns: elapsed.as_nanos() as f64 / n as f64,
        latency_mean_ns: elapsed.as_nanos() as f64 / n as f64,
        throughput_ops_per_sec: 1e9 * n as f64 / elapsed.as_nanos() as f64,
        total_ops: n,
    }
}

async fn bench_chain_traverse(
    graph: &GraphService,
    chain_len: usize,
    iterations: usize,
) -> BenchResult {
    // Build chain
    let chain_ids: Vec<NexoraId> = (0..chain_len).map(|i| qid("chain", i)).collect();
    for (i, id) in chain_ids.iter().enumerate() {
        graph
            .set_property(id, "index", PropertyValue::Integer(i as i64))
            .await
            .unwrap();
        if i + 1 < chain_len {
            let edge = HalfEdge::out(Symbol::new("NEXT"), chain_ids[i + 1].clone());
            graph.add_edge(id, edge).await.unwrap();
        }
    }

    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let mut current = chain_ids[0].clone();
        for _ in 0..(chain_len - 1) {
            let edges = graph.get_edges(&current).await.unwrap();
            let next = edges
                .iter()
                .find(|e| e.edge_type.as_str() == "NEXT" && e.direction.is_out())
                .expect("missing NEXT edge");
            current = next.other.clone();
        }
        latencies.push(start.elapsed());
        std::hint::black_box(current);
    }
    latencies.sort();
    finalize("chain_traverse", latencies)
}

// ============================================================
// Output helpers
// ============================================================

fn results_to_json(results: &[BenchResult]) -> serde_json::Value {
    let arr: Vec<_> = results
        .iter()
        .map(|r| {
            json!({
                "operation": r.operation,
                "latency_p50_ns": r.latency_p50_ns.round(),
                "latency_p99_ns": r.latency_p99_ns.round(),
                "latency_mean_ns": r.latency_mean_ns.round(),
                "throughput_ops_per_sec": r.throughput_ops_per_sec.round(),
                "total_ops": r.total_ops,
            })
        })
        .collect();

    json!({
        "database": "nexora",
        "version": env!("CARGO_PKG_VERSION"),
        "results": arr,
    })
}

fn print_table(results: &[BenchResult]) {
    eprintln!();
    eprintln!(
        "{:<25} {:>12} {:>12} {:>12} {:>15} {:>8}",
        "Operation", "P50 (us)", "P99 (us)", "Mean (us)", "Throughput", "Ops"
    );
    eprintln!("{}", "-".repeat(86));
    for r in results {
        eprintln!(
            "{:<25} {:>12.2} {:>12.2} {:>12.2} {:>13.0}/s {:>8}",
            r.operation,
            r.latency_p50_ns / 1000.0,
            r.latency_p99_ns / 1000.0,
            r.latency_mean_ns / 1000.0,
            r.throughput_ops_per_sec,
            r.total_ops,
        );
    }
    eprintln!();
}

// ============================================================
// Main
// ============================================================

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    eprintln!("=== nexora Comparison Benchmarks ===");
    eprintln!();

    let results: Vec<BenchResult> = rt.block_on(async {
        let graph = make_graph(8);

        eprintln!("[1/7] property_set ...");
        let r1 = bench_property_set(&graph, 5000).await;

        eprintln!("[2/7] property_get ...");
        let r2 = bench_property_get(&graph, 5000).await;

        eprintln!("[3/7] edge_add ...");
        let r3 = bench_edge_add(&graph, 2000).await;

        eprintln!("[4/7] edge_get ...");
        let r4 = bench_edge_get(&graph, 2000, 100).await;

        eprintln!("[5/7] get_all_properties ...");
        let r5 = bench_get_all_properties(&graph, 3000).await;

        eprintln!("[6/7] bulk_load ...");
        let r6 = bench_bulk_load(&graph, 5000).await;

        eprintln!("[7/7] chain_traverse ...");
        let r7 = bench_chain_traverse(&graph, 100, 200).await;

        vec![r1, r2, r3, r4, r5, r6, r7]
    });

    // JSON output
    let json_value = results_to_json(&results);
    let json_str = serde_json::to_string_pretty(&json_value).unwrap();

    // Print JSON to stdout
    println!("{json_str}");

    // Write JSON to file — use CARGO_MANIFEST_DIR to locate workspace target/
    let json_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/comparison_results.json");
    if let Some(parent) = json_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::write(&json_path, &json_str) {
        Ok(()) => eprintln!("JSON results written to {}", json_path.display()),
        Err(e) => eprintln!("Warning: could not write JSON file: {e}"),
    }

    // Table output to stderr
    print_table(&results);
}
