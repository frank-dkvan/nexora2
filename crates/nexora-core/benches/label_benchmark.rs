//! Label index performance benchmarks

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use nexora_core::LabelIndex;
use nexora_id::NexoraId;

/// Benchmark: Single label query
fn bench_single_label_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("label_query_single");

    for num_nodes in [1_000, 10_000, 100_000].iter() {
        let index = LabelIndex::new();

        // Populate index
        rt.block_on(async {
            for i in 0..*num_nodes {
                let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
                index.add_label("Person", node.clone()).await;
            }
        });

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let nodes = index.query("Person").await;
                    black_box(nodes);
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Multiple label intersection query
fn bench_intersection_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("label_query_intersection");

    for num_nodes in [1_000, 10_000, 100_000].iter() {
        let index = LabelIndex::new();

        // Populate with overlapping labels
        rt.block_on(async {
            for i in 0..*num_nodes {
                let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
                index.add_label("Person", node.clone()).await;

                if i % 2 == 0 {
                    index.add_label("Employee", node.clone()).await;
                }

                if i % 3 == 0 {
                    index.add_label("Manager", node.clone()).await;
                }
            }
        });

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let nodes = index.query_all(&["Person", "Employee", "Manager"]).await;
                    black_box(nodes);
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Union query
fn bench_union_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("label_query_union");

    for num_nodes in [1_000, 10_000, 100_000].iter() {
        let index = LabelIndex::new();

        // Create separate label groups
        rt.block_on(async {
            for i in 0..num_nodes / 3 {
                let node = NexoraId::from_bytes(format!("person-{}", i).into_bytes());
                index.add_label("Person", node).await;
            }

            for i in 0..num_nodes / 3 {
                let node = NexoraId::from_bytes(format!("product-{}", i).into_bytes());
                index.add_label("Product", node).await;
            }

            for i in 0..num_nodes / 3 {
                let node = NexoraId::from_bytes(format!("company-{}", i).into_bytes());
                index.add_label("Company", node).await;
            }
        });

        group.bench_with_input(BenchmarkId::from_parameter(num_nodes), num_nodes, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let nodes = index.query_any(&["Person", "Product", "Company"]).await;
                    black_box(nodes);
                });
            });
        });
    }

    group.finish();
}

/// Benchmark: Add label throughput
fn bench_add_label(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("label_add");

    group.bench_function("1000_inserts", |b| {
        b.iter(|| {
            rt.block_on(async {
                let index = LabelIndex::new();
                for i in 0..1000 {
                    let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
                    index.add_label("Person", node).await;
                }
            });
        });
    });

    group.finish();
}

/// Benchmark: Has label check
fn bench_has_label(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let index = LabelIndex::new();
    let nodes: Vec<_> = rt.block_on(async {
        let mut nodes = Vec::new();
        for i in 0..10_000 {
            let node = NexoraId::from_bytes(format!("node-{}", i).into_bytes());
            index.add_label("Person", node.clone()).await;
            nodes.push(node);
        }
        nodes
    });

    c.bench_function("has_label_10k", |b| {
        b.iter(|| {
            rt.block_on(async {
                for node in &nodes {
                    let result = index.has_label("Person", node).await;
                    black_box(result);
                }
            });
        });
    });
}

/// Benchmark: Remove node
fn bench_remove_node(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("label_remove_node");

    group.bench_function("remove_with_3_labels", |b| {
        b.iter(|| {
            rt.block_on(async {
                let index = LabelIndex::new();
                let node = NexoraId::from_bytes(b"node1".to_vec());

                index
                    .add_labels(&["Person", "Employee", "Manager"], node.clone())
                    .await;
                index.remove_node(&node).await;
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_label_query,
    bench_intersection_query,
    bench_union_query,
    bench_add_label,
    bench_has_label,
    bench_remove_node,
);
criterion_main!(benches);
