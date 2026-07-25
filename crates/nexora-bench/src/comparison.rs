//! Comparison framework — runs graph operations and outputs JSON results
//! suitable for comparison with Neo4j/ArangoDB.

use nexora_core::GraphService;
use nexora_id::{NexoraId, PropertyValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tokio::runtime::Runtime;

use crate::util::{
    generate_random_ids, make_edge, make_test_graph, populate_edges, populate_nodes, BenchConfig,
    LatencyStats,
};

/// A single benchmark result entry, serializable to JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Name of the operation (e.g., "create_node", "get_property").
    pub operation: String,
    /// Number of operations completed.
    pub operations: usize,
    /// Throughput in ops/sec.
    pub throughput_ops_sec: f64,
    /// Latency statistics.
    pub latency: LatencyStats,
    /// Additional metadata (e.g., node count, concurrency level).
    pub metadata: HashMap<String, serde_json::Value>,
}

/// The comparison runner — executes graph operations and collects results.
pub struct ComparisonRunner {
    config: BenchConfig,
}

impl ComparisonRunner {
    pub fn new(config: BenchConfig) -> Self {
        Self { config }
    }

    /// Run all comparison benchmarks and return JSON-serializable results.
    pub fn run_all(&self) -> Vec<BenchmarkResult> {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut results = Vec::new();

            // Node creation
            results.extend(self.bench_create_nodes(10_000).await);
            results.extend(self.bench_create_nodes(50_000).await);

            // Property read/write
            results.extend(self.bench_property_ops(10_000).await);

            // Edge operations
            results.extend(self.bench_edge_ops(10_000, 5).await);

            // Traversal
            results.extend(self.bench_traversal(10_000, 5, 2).await);

            results
        })
    }

    /// Benchmark node creation at different scales.
    async fn bench_create_nodes(&self, n: usize) -> Vec<BenchmarkResult> {
        let svc = make_test_graph(&self.config);
        let ids = generate_random_ids(n);

        let mut latencies = Vec::with_capacity(n);
        let start = Instant::now();
        for qid in &ids {
            let t0 = Instant::now();
            svc.set_property(qid, "name", PropertyValue::String("bench".into()))
                .await
                .unwrap();
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        let throughput = n as f64 / elapsed.as_secs_f64();

        vec![BenchmarkResult {
            operation: "create_node".to_string(),
            operations: n,
            throughput_ops_sec: throughput,
            latency: stats,
            metadata: HashMap::from([("node_count".into(), n.into())]),
        }]
    }

    /// Benchmark property read/write on existing nodes.
    async fn bench_property_ops(&self, n: usize) -> Vec<BenchmarkResult> {
        let svc = make_test_graph(&self.config);
        let ids = generate_random_ids(n);
        populate_nodes(&svc, &ids).await;

        // Write benchmark
        let mut latencies = Vec::with_capacity(n);
        let start = Instant::now();
        for (i, qid) in ids.iter().enumerate() {
            let t0 = Instant::now();
            svc.set_property(qid, "score", PropertyValue::Float(i as f64 * 1.5))
                .await
                .unwrap();
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();
        let mut lat = latencies;
        let write_stats = LatencyStats::from_durations(&mut lat);
        let write_throughput = n as f64 / elapsed.as_secs_f64();

        // Read benchmark
        let mut latencies = Vec::with_capacity(n);
        let start = Instant::now();
        for qid in &ids {
            let t0 = Instant::now();
            let _ = svc.get_property(qid, "name").await.unwrap();
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();
        let mut lat = latencies;
        let read_stats = LatencyStats::from_durations(&mut lat);
        let read_throughput = n as f64 / elapsed.as_secs_f64();

        vec![
            BenchmarkResult {
                operation: "set_property".to_string(),
                operations: n,
                throughput_ops_sec: write_throughput,
                latency: write_stats,
                metadata: HashMap::from([("node_count".into(), n.into())]),
            },
            BenchmarkResult {
                operation: "get_property".to_string(),
                operations: n,
                throughput_ops_sec: read_throughput,
                latency: read_stats,
                metadata: HashMap::from([("node_count".into(), n.into())]),
            },
        ]
    }

    /// Benchmark edge creation and retrieval.
    async fn bench_edge_ops(&self, n: usize, edges_per_node: usize) -> Vec<BenchmarkResult> {
        let svc = make_test_graph(&self.config);
        let ids = generate_random_ids(n);
        populate_nodes(&svc, &ids).await;

        // Edge creation
        let mut latencies = Vec::with_capacity(n * edges_per_node);
        let start = Instant::now();
        for qid in &ids {
            for j in 0..edges_per_node {
                let target = &ids[(j + 1) % n];
                let edge = make_edge("LINKS_TO", target.clone());
                let t0 = Instant::now();
                svc.add_edge(qid, edge).await.unwrap();
                latencies.push(t0.elapsed());
            }
        }
        let elapsed = start.elapsed();
        let total_edges = n * edges_per_node;
        let mut lat = latencies;
        let write_stats = LatencyStats::from_durations(&mut lat);
        let write_throughput = total_edges as f64 / elapsed.as_secs_f64();

        // Edge retrieval
        let mut latencies = Vec::with_capacity(n);
        let start = Instant::now();
        for qid in &ids {
            let t0 = Instant::now();
            let _ = svc.get_edges(qid).await.unwrap();
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();
        let mut lat = latencies;
        let read_stats = LatencyStats::from_durations(&mut lat);
        let read_throughput = n as f64 / elapsed.as_secs_f64();

        vec![
            BenchmarkResult {
                operation: "add_edge".to_string(),
                operations: total_edges,
                throughput_ops_sec: write_throughput,
                latency: write_stats,
                metadata: HashMap::from([
                    ("node_count".into(), n.into()),
                    ("edges_per_node".into(), edges_per_node.into()),
                ]),
            },
            BenchmarkResult {
                operation: "get_edges".to_string(),
                operations: n,
                throughput_ops_sec: read_throughput,
                latency: read_stats,
                metadata: HashMap::from([("node_count".into(), n.into())]),
            },
        ]
    }

    /// Benchmark multi-hop traversal.
    async fn bench_traversal(
        &self,
        n: usize,
        edges_per_node: usize,
        depth: usize,
    ) -> Vec<BenchmarkResult> {
        let svc = make_test_graph(&self.config);
        let ids = generate_random_ids(n);
        populate_nodes(&svc, &ids).await;
        populate_edges(&svc, &ids, edges_per_node).await;

        let sample_size = 100.min(n);
        let mut latencies = Vec::with_capacity(sample_size);

        let start = Instant::now();
        for qid in ids.iter().take(sample_size) {
            let t0 = Instant::now();
            let _ = traverse_hop(&svc, qid, depth).await;
            latencies.push(t0.elapsed());
        }
        let elapsed = start.elapsed();

        let mut lat = latencies;
        let stats = LatencyStats::from_durations(&mut lat);
        let throughput = sample_size as f64 / elapsed.as_secs_f64();

        vec![BenchmarkResult {
            operation: format!("traversal_{depth}_hop"),
            operations: sample_size,
            throughput_ops_sec: throughput,
            latency: stats,
            metadata: HashMap::from([
                ("node_count".into(), n.into()),
                ("edges_per_node".into(), edges_per_node.into()),
                ("depth".into(), depth.into()),
            ]),
        }]
    }
}

/// Traverse `depth` hops from a starting node, collecting visited node IDs.
async fn traverse_hop(svc: &GraphService, start: &NexoraId, depth: usize) -> Vec<NexoraId> {
    let mut visited = vec![start.clone()];
    let mut frontier = vec![start.clone()];
    let mut visited_set = std::collections::HashSet::new();
    visited_set.insert(start.clone());

    for _ in 0..depth {
        let mut next_frontier = Vec::new();
        for node in &frontier {
            let edges = svc.get_edges(node).await.unwrap();
            for edge in edges {
                if !visited_set.contains(&edge.other) {
                    visited_set.insert(edge.other.clone());
                    visited.push(edge.other.clone());
                    next_frontier.push(edge.other);
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    visited
}
