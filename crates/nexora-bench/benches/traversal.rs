//! Graph traversal benchmarks — 1-4 hop neighbor traversal, BFS/DFS, pattern matching.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_bench::util::{
    generate_random_ids, make_test_graph, populate_edges, populate_nodes, BenchConfig,
};
use nexora_core::GraphService;
use nexora_id::NexoraId;
use std::collections::{HashSet, VecDeque};

fn bench_hop_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("hop_traversal");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 10_000usize;
    let svc = make_test_graph(&config);
    let ids = generate_random_ids(n);
    rt.block_on(populate_nodes(&svc, &ids));
    rt.block_on(populate_edges(&svc, &ids, 5));

    for &depth in &[1usize, 2, 3, 4] {
        group.throughput(Throughput::Elements(100));
        group.bench_with_input(BenchmarkId::new("bfs", depth), &depth, |b, &depth| {
            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    let mut total = 0usize;
                    for qid in ids.iter().take(100) {
                        total += bfs_traverse(svc, qid, depth).await.len();
                    }
                    black_box(total);
                }
            });
        });
    }
    group.finish();
}

fn bench_bfs_by_graph_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("bfs_by_size");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[1_000usize, 10_000] {
        let svc = make_test_graph(&config);
        let ids = generate_random_ids(n);
        rt.block_on(populate_nodes(&svc, &ids));
        rt.block_on(populate_edges(&svc, &ids, 5));

        group.throughput(Throughput::Elements(50));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    let mut total = 0usize;
                    for qid in ids.iter().take(50) {
                        total += bfs_traverse(svc, qid, 3).await.len();
                    }
                    black_box(total);
                }
            });
        });
    }
    group.finish();
}

fn bench_dfs_by_graph_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("dfs_by_size");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[1_000usize, 10_000] {
        let svc = make_test_graph(&config);
        let ids = generate_random_ids(n);
        rt.block_on(populate_nodes(&svc, &ids));
        rt.block_on(populate_edges(&svc, &ids, 5));

        group.throughput(Throughput::Elements(50));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    let mut total = 0usize;
                    for qid in ids.iter().take(50) {
                        total += dfs_traverse(svc, qid, 3).await.len();
                    }
                    black_box(total);
                }
            });
        });
    }
    group.finish();
}

fn bench_find_triangles(c: &mut Criterion) {
    let mut group = c.benchmark_group("pattern_triangles");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 1_000usize;
    let svc = make_test_graph(&config);
    let ids = generate_random_ids(n);
    rt.block_on(populate_nodes(&svc, &ids));
    rt.block_on(populate_edges(&svc, &ids, 5));

    group.throughput(Throughput::Elements(1));
    group.bench_function("find_triangles_1k", |b| {
        b.to_async(&rt).iter(|| {
            let svc = &svc;
            let ids = &ids;
            async move {
                let count = find_triangles(svc, ids).await;
                black_box(count);
            }
        });
    });
    group.finish();
}

fn bench_shortest_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("shortest_path");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 1_000usize;
    let svc = make_test_graph(&config);
    let ids = generate_random_ids(n);
    rt.block_on(populate_nodes(&svc, &ids));
    rt.block_on(populate_edges(&svc, &ids, 5));

    group.throughput(Throughput::Elements(20));
    group.bench_function("bfs_shortest_path_1k", |b| {
        b.to_async(&rt).iter(|| {
            let svc = &svc;
            let ids = &ids;
            async move {
                let mut found = 0usize;
                for i in 0..20 {
                    let src = &ids[i];
                    let dst = &ids[(i + n / 2) % n];
                    if bfs_shortest_path(svc, src, dst, 6).await.is_some() {
                        found += 1;
                    }
                }
                black_box(found);
            }
        });
    });
    group.finish();
}

// ====== Traversal algorithms ======

async fn bfs_traverse(svc: &GraphService, start: &NexoraId, depth: usize) -> Vec<NexoraId> {
    let mut visited = HashSet::new();
    visited.insert(start.clone());
    let mut frontier = vec![start.clone()];
    let mut result = vec![start.clone()];

    for _ in 0..depth {
        let mut next = Vec::new();
        for node in &frontier {
            let edges = svc.get_edges(node).await.unwrap();
            for e in edges {
                if visited.insert(e.other.clone()) {
                    result.push(e.other.clone());
                    next.push(e.other);
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
    }
    result
}

async fn dfs_traverse(svc: &GraphService, start: &NexoraId, max_depth: usize) -> Vec<NexoraId> {
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    let mut stack = vec![(start.clone(), 0usize)];

    while let Some((node, depth)) = stack.pop() {
        if !visited.insert(node.clone()) {
            continue;
        }
        result.push(node.clone());
        if depth >= max_depth {
            continue;
        }
        let edges = svc.get_edges(&node).await.unwrap();
        for e in edges {
            if !visited.contains(&e.other) {
                stack.push((e.other, depth + 1));
            }
        }
    }
    result
}

async fn find_triangles(svc: &GraphService, ids: &[NexoraId]) -> usize {
    let mut count = 0;
    let sample: &[NexoraId] = &ids[..ids.len().min(200)];
    for a in sample {
        let edges_a = svc.get_edges(a).await.unwrap();
        let neighbors_a: HashSet<NexoraId> = edges_a.into_iter().map(|e| e.other).collect();
        for b in &neighbors_a {
            // Use byte comparison to avoid duplicate triangles (a < b < c)
            if b.as_bytes() > a.as_bytes() {
                let edges_b = svc.get_edges(b).await.unwrap();
                for e in edges_b {
                    if neighbors_a.contains(&e.other) && e.other.as_bytes() > b.as_bytes() {
                        count += 1;
                    }
                }
            }
        }
    }
    count
}

async fn bfs_shortest_path(
    svc: &GraphService,
    src: &NexoraId,
    dst: &NexoraId,
    max_depth: usize,
) -> Option<usize> {
    let mut visited = HashSet::new();
    visited.insert(src.clone());
    let mut queue = VecDeque::new();
    queue.push_back((src.clone(), 0usize));

    while let Some((node, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        let edges = svc.get_edges(&node).await.unwrap();
        for e in edges {
            if &e.other == dst {
                return Some(depth + 1);
            }
            if visited.insert(e.other.clone()) {
                queue.push_back((e.other, depth + 1));
            }
        }
    }
    None
}

criterion_group!(
    benches,
    bench_hop_traversal,
    bench_bfs_by_graph_size,
    bench_dfs_by_graph_size,
    bench_find_triangles,
    bench_shortest_path,
);
criterion_main!(benches);
