//! CRUD throughput benchmarks — node creation, property updates, edge creation, reads, deletes.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexora_bench::util::{
    generate_random_ids, make_edge, make_test_graph, populate_edges, populate_nodes, BenchConfig,
};
use nexora_id::PropertyValue;

fn bench_node_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_creation");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[10_000usize, 50_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let svc = make_test_graph(&config);
                let ids = generate_random_ids(n);
                for qid in &ids {
                    svc.set_property(qid, "name", PropertyValue::String("bench".into()))
                        .await
                        .unwrap();
                }
                black_box(&svc);
            });
        });
    }
    group.finish();
}

fn bench_property_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("property_update");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[10_000usize, 50_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let svc = make_test_graph(&config);
            let ids = generate_random_ids(n);
            rt.block_on(populate_nodes(&svc, &ids));

            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    for (i, qid) in ids.iter().enumerate() {
                        svc.set_property(qid, "score", PropertyValue::Float(i as f64 * 1.5))
                            .await
                            .unwrap();
                    }
                    black_box(());
                }
            });
        });
    }
    group.finish();
}

fn bench_edge_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_creation");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[1_000usize, 10_000] {
        let edges_per_node = 5;
        let total = (n * edges_per_node) as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let svc = make_test_graph(&config);
            let ids = generate_random_ids(n);
            rt.block_on(populate_nodes(&svc, &ids));

            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    for (i, qid) in ids.iter().enumerate() {
                        for j in 0..edges_per_node {
                            let target = &ids[(i + j + 1) % n];
                            let edge = make_edge("LINKS_TO", target.clone());
                            svc.add_edge(qid, edge).await.unwrap();
                        }
                    }
                    black_box(());
                }
            });
        });
    }
    group.finish();
}

fn bench_read_property(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_property");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[10_000usize, 50_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let svc = make_test_graph(&config);
            let ids = generate_random_ids(n);
            rt.block_on(populate_nodes(&svc, &ids));

            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    let mut sum = 0i64;
                    for qid in ids {
                        let val = svc.get_property(qid, "id").await.unwrap();
                        if let Some(PropertyValue::Integer(v)) = val {
                            sum += v;
                        }
                    }
                    black_box(sum);
                }
            });
        });
    }
    group.finish();
}

fn bench_read_edges(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_edges");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    for &n in &[1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let svc = make_test_graph(&config);
            let ids = generate_random_ids(n);
            rt.block_on(populate_nodes(&svc, &ids));
            rt.block_on(populate_edges(&svc, &ids, 5));

            b.to_async(&rt).iter(|| {
                let svc = &svc;
                let ids = &ids;
                async move {
                    let mut total = 0usize;
                    for qid in ids {
                        let edges = svc.get_edges(qid).await.unwrap();
                        total += edges.len();
                    }
                    black_box(total);
                }
            });
        });
    }
    group.finish();
}

fn bench_delete_edge(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_edge");
    let config = BenchConfig::default();
    let rt = tokio::runtime::Runtime::new().unwrap();

    let n = 1_000usize;
    group.throughput(Throughput::Elements(n as u64));
    group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
        b.to_async(&rt).iter(|| async move {
            let svc = make_test_graph(&config);
            let ids = generate_random_ids(n);
            populate_nodes(&svc, &ids).await;
            for (i, qid) in ids.iter().enumerate() {
                let target = &ids[(i + 1) % n];
                let edge = make_edge("LINKS_TO", target.clone());
                svc.add_edge(qid, edge.clone()).await.unwrap();
                svc.remove_edge(qid, edge).await.unwrap();
            }
            black_box(());
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_node_creation,
    bench_property_update,
    bench_edge_creation,
    bench_read_property,
    bench_read_edges,
    bench_delete_edge,
);
criterion_main!(benches);
