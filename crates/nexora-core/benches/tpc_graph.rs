//! TPC-H style graph query benchmark suite for nexora-core.
//!
//! Covers: bulk load, chain traversal, star-graph fan-out, concurrent
//! read-heavy / write-heavy workloads, edge enumeration, mixed workloads,
//! and shard scale testing.
//!
//! Run with: `cargo bench -p nexora-core --bench tpc_graph`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================
// Helpers
// ============================================================

/// Create a graph service with the given shard count.
fn make_service(num_shards: usize) -> GraphService {
    GraphService::new(
        GraphServiceConfig {
            num_shards,
            max_nodes_per_shard: 200_000,
            node_channel_size: 256,
        },
        Arc::new(InMemoryPersistor::new()),
    )
}

/// Create an `Arc<GraphService>` for concurrent benchmarks.
fn make_service_arc(num_shards: usize) -> Arc<GraphService> {
    Arc::new(make_service(num_shards))
}

/// Generate a deterministic `NexoraId` from a prefix string and an index.
fn qid(prefix: &str, idx: usize) -> NexoraId {
    NexoraId::from_bytes(format!("{prefix}{idx:010}").into_bytes())
}

/// Follow a chain of `NEXT` edges starting from `start` for `depth` hops.
async fn traverse_chain(graph: &GraphService, start: &NexoraId, depth: usize) -> NexoraId {
    let mut current = start.clone();
    for _ in 0..depth {
        let edges = graph.get_edges(&current).await.unwrap();
        let next = edges
            .iter()
            .find(|e| e.edge_type.as_str() == "NEXT" && e.direction.is_out())
            .expect("chain node missing outgoing NEXT edge");
        current = next.other.clone();
    }
    current
}

// ============================================================
// 1. Bulk Load Benchmark
// ============================================================

fn bench_bulk_load(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpc_bulk_load");
    group.sample_size(10);

    for &n in &[1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let graph = make_service(8);
                    let start = Instant::now();
                    rt.block_on(async {
                        for i in 0..n {
                            let id = qid("bulk", i);
                            graph
                                .set_property(&id, "id", PropertyValue::Integer(i as i64))
                                .await
                                .unwrap();
                            graph
                                .set_property(
                                    &id,
                                    "name",
                                    PropertyValue::String(format!("Node{i}")),
                                )
                                .await
                                .unwrap();
                        }
                    });
                    total += start.elapsed();
                }
                total
            });
        });
    }
    group.finish();
}

// ============================================================
// 2. Graph Traversal Benchmark (chain A→B→C→…→N)
// ============================================================

fn bench_chain_traversal(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpc_chain_traversal");

    let chain_len = 1001usize; // 1001 nodes → 1000 edges (max depth = 1000)
    let graph = make_service(8);
    let chain_ids: Vec<NexoraId> = (0..chain_len).map(|i| qid("chain", i)).collect();

    // Build the chain
    rt.block_on(async {
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
    });

    for &depth in &[10usize, 100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter(|| {
                rt.block_on(async {
                    let end = traverse_chain(&graph, &chain_ids[0], depth).await;
                    black_box(end);
                });
            });
        });
    }
    group.finish();
}

// ============================================================
// 3. Multi-Hop Query Benchmark (star graph: 1 center, N leaves)
// ============================================================

fn bench_star_graph(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpc_star_graph");

    for &num_leaves in &[100usize, 1_000, 10_000] {
        let graph = make_service(8);
        let center = qid("star_center", 0);
        let leaves: Vec<NexoraId> = (0..num_leaves).map(|i| qid("leaf", i)).collect();

        rt.block_on(async {
            graph
                .set_property(&center, "type", PropertyValue::String("center".into()))
                .await
                .unwrap();
            for (i, leaf) in leaves.iter().enumerate() {
                graph
                    .set_property(leaf, "id", PropertyValue::Integer(i as i64))
                    .await
                    .unwrap();
                let edge = HalfEdge::out(Symbol::new("CONNECTED"), leaf.clone());
                graph.add_edge(&center, edge).await.unwrap();
            }
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(num_leaves),
            &num_leaves,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let edges = graph.get_edges(&center).await.unwrap();
                        for edge in &edges {
                            let _ = graph.get_property(&edge.other, "id").await.unwrap();
                        }
                        black_box(edges.len());
                    });
                });
            },
        );
    }
    group.finish();
}

// ============================================================
// 4. Concurrent Read-Heavy Benchmark (90% reads / 10% writes)
// ============================================================

fn bench_read_heavy(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("tpc_read_heavy");
    group.sample_size(20);

    let num_nodes = 1000usize;
    let ops_per_task = 200usize;
    let num_tasks = 8usize;

    let graph = make_service_arc(16);

    // Pre-populate nodes
    rt.block_on(async {
        for i in 0..num_nodes {
            let id = qid("rh", i);
            graph
                .set_property(&id, "val", PropertyValue::Integer(i as i64))
                .await
                .unwrap();
        }
    });

    group.bench_function("90_read_10_write", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();
                for t in 0..num_tasks {
                    let g = graph.clone();
                    handles.push(tokio::spawn(async move {
                        for i in 0..ops_per_task {
                            let idx = (t * ops_per_task + i) % num_nodes;
                            let id = qid("rh", idx);
                            if i % 10 < 9 {
                                // 90% reads
                                let _ = g.get_property(&id, "val").await.unwrap();
                            } else {
                                // 10% writes
                                g.set_property(&id, "val", PropertyValue::Integer(i as i64))
                                    .await
                                    .unwrap();
                            }
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================
// 5. Concurrent Write-Heavy Benchmark (90% writes / 10% reads)
// ============================================================

fn bench_write_heavy(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("tpc_write_heavy");
    group.sample_size(20);

    let num_nodes = 1000usize;
    let ops_per_task = 200usize;
    let num_tasks = 8usize;

    let graph = make_service_arc(16);

    // Pre-populate nodes
    rt.block_on(async {
        for i in 0..num_nodes {
            let id = qid("wh", i);
            graph
                .set_property(&id, "val", PropertyValue::Integer(i as i64))
                .await
                .unwrap();
        }
    });

    group.bench_function("90_write_10_read", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();
                for t in 0..num_tasks {
                    let g = graph.clone();
                    handles.push(tokio::spawn(async move {
                        for i in 0..ops_per_task {
                            let idx = (t * ops_per_task + i) % num_nodes;
                            let id = qid("wh", idx);
                            if i % 10 < 9 {
                                // 90% writes
                                g.set_property(&id, "val", PropertyValue::Integer(i as i64))
                                    .await
                                    .unwrap();
                            } else {
                                // 10% reads
                                let _ = g.get_property(&id, "val").await.unwrap();
                            }
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================
// 6. Edge Traversal Benchmark (varying degree per node)
// ============================================================

fn bench_edge_traversal(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpc_edge_traversal");

    for &degree in &[1usize, 10, 100, 1000] {
        let graph = make_service(8);
        let node = qid("edge_node", 0);

        rt.block_on(async {
            graph
                .set_property(&node, "id", PropertyValue::Integer(0))
                .await
                .unwrap();
            for i in 0..degree {
                let target = qid("target", i);
                let edge = HalfEdge::out(Symbol::new("EDGE"), target);
                graph.add_edge(&node, edge).await.unwrap();
            }
        });

        group.bench_with_input(BenchmarkId::from_parameter(degree), &degree, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let edges = graph.get_edges(&node).await.unwrap();
                    black_box(edges.len());
                });
            });
        });
    }
    group.finish();
}

// ============================================================
// 7. Mixed Workload Benchmark
// 40% read, 30% write, 20% traverse, 10% edge ops
// ============================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("tpc_mixed_workload");
    group.sample_size(20);

    let num_nodes = 1000usize;
    let ops_per_task = 200usize;
    let num_tasks = 8usize;

    let graph = make_service_arc(16);

    // Pre-populate nodes with properties and chain edges
    rt.block_on(async {
        for i in 0..num_nodes {
            let id = qid("mw", i);
            graph
                .set_property(&id, "val", PropertyValue::Integer(i as i64))
                .await
                .unwrap();
            if i + 1 < num_nodes {
                let next = qid("mw", i + 1);
                let edge = HalfEdge::out(Symbol::new("NEXT"), next);
                graph.add_edge(&id, edge).await.unwrap();
            }
        }
    });

    group.bench_function("40r_30w_20t_10e", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();
                for t in 0..num_tasks {
                    let g = graph.clone();
                    handles.push(tokio::spawn(async move {
                        for i in 0..ops_per_task {
                            let idx = (t * ops_per_task + i) % num_nodes;
                            let id = qid("mw", idx);
                            let bucket = i % 10;
                            if bucket < 4 {
                                // 40% read
                                let _ = g.get_property(&id, "val").await.unwrap();
                            } else if bucket < 7 {
                                // 30% write
                                g.set_property(&id, "val", PropertyValue::Integer(i as i64))
                                    .await
                                    .unwrap();
                            } else if bucket < 9 {
                                // 20% traverse
                                let edges = g.get_edges(&id).await.unwrap();
                                if let Some(first) = edges.first() {
                                    let _ = g.get_property(&first.other, "val").await.unwrap();
                                }
                            } else {
                                // 10% edge ops
                                let target_idx = (idx + 1) % num_nodes;
                                let target = qid("mw", target_idx);
                                let edge = HalfEdge::out(Symbol::new("EXTRA"), target);
                                let _ = g.add_edge(&id, edge).await;
                            }
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

// ============================================================
// 8. Scale Test (varying shard counts)
// ============================================================

fn bench_scale_test(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("tpc_scale_test");
    group.sample_size(20);

    let num_ops = 1000usize;

    for &num_shards in &[1usize, 8, 32, 128, 256] {
        let graph = make_service(num_shards);

        // Pre-populate nodes
        rt.block_on(async {
            for i in 0..num_ops {
                let id = qid("scale", i);
                graph
                    .set_property(&id, "val", PropertyValue::Integer(i as i64))
                    .await
                    .unwrap();
            }
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(num_shards),
            &num_shards,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..num_ops {
                            let id = qid("scale", i);
                            let _ = graph.get_property(&id, "val").await.unwrap();
                        }
                    });
                });
            },
        );
    }
    group.finish();
}

// ============================================================
// Criterion entry point
// ============================================================

criterion_group!(
    benches,
    bench_bulk_load,
    bench_chain_traversal,
    bench_star_graph,
    bench_read_heavy,
    bench_write_heavy,
    bench_edge_traversal,
    bench_mixed_workload,
    bench_scale_test,
);
criterion_main!(benches);
