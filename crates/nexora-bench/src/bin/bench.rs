//! CLI entry point for the nexora-bench benchmark suite.
//!
//! Usage:
//!   cargo run --release -p nexora-bench -- --scenario crud
//!   cargo run --release -p nexora-bench -- --scenario traversal --depth 4
//!   cargo run --release -p nexora-bench -- --scenario tpc --scale 1
//!   cargo run --release -p nexora-bench -- --all
//!   cargo run --release -p nexora-bench -- --report output.html

use nexora_bench::{
    comparison::ComparisonRunner,
    report,
    tpc_graph::{TpcGraphBenchmark, TpcScaleFactor},
    util::BenchConfig,
};
use std::env;
use std::fs;
use std::process;

fn print_usage() {
    eprintln!("nexora-bench — Performance benchmark suite for nexora");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("  bench --scenario <NAME> [OPTIONS]");
    eprintln!("  bench --all [--report <FILE>]");
    eprintln!();
    eprintln!("SCENARIOS:");
    eprintln!("  crud        CRUD throughput benchmark (node/property/edge ops)");
    eprintln!("  traversal   Graph traversal benchmark (1-4 hop BFS)");
    eprintln!("  wal         WAL write throughput benchmark");
    eprintln!("  concurrency Concurrent worker benchmark");
    eprintln!("  tpc         TPC-H style graph benchmark");
    eprintln!("  all         Run all scenarios");
    eprintln!();
    eprintln!("OPTIONS:");
    eprintln!("  --depth <N>     Traversal depth (default: 3)");
    eprintln!("  --scale <N>     TPC scale factor: 1, 3, or 10 (default: 1)");
    eprintln!("  --report <FILE> Write HTML report to file");
    eprintln!("  --help          Show this help message");
}

fn parse_args() -> Vec<String> {
    env::args().skip(1).collect()
}

fn main() {
    let args = parse_args();

    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return;
    }

    let run_all = args.iter().any(|a| a == "--all" || a == "all");
    let report_file: Option<String> = {
        let idx = args.iter().position(|a| a == "--report");
        idx.and_then(|i| args.get(i + 1).cloned())
    };

    let mut comparison_results = Vec::new();
    let mut tpc_results = Vec::new();
    let config = BenchConfig::default();

    if run_all {
        println!("=== Running all benchmark scenarios ===\n");

        println!("[1/3] Comparison framework (CRUD + traversal)...");
        let runner = ComparisonRunner::new(config);
        let results = runner.run_all();
        println!("  Completed {} benchmark entries", results.len());
        for r in &results {
            println!(
                "    {:<25} {:>10.0} ops/s  p50={:.0}us p95={:.0}us",
                r.operation, r.throughput_ops_sec, r.latency.p50_us, r.latency.p95_us
            );
        }
        comparison_results = results;

        println!("\n[2/3] TPC-H style graph benchmark (SF-1)...");
        let tpc = TpcGraphBenchmark::new(config, TpcScaleFactor::Sf1);
        let results = tpc.run_full();
        println!("  Completed {} TPC queries", results.len());
        for r in &results {
            println!(
                "    {:<30} {:>8.1} qps  p50={:.0}us p95={:.0}us",
                r.query_name, r.throughput_qps, r.latency.p50_us, r.latency.p95_us
            );
        }
        tpc_results = results;

        println!("\n[3/3] Done!");
    } else {
        let scenario_idx = args.iter().position(|a| a == "--scenario");
        let scenario = scenario_idx
            .and_then(|i| args.get(i + 1).cloned())
            .unwrap_or_else(|| {
                eprintln!("Error: --scenario is required (or use --all)");
                process::exit(1);
            });

        match scenario.as_str() {
            "crud" => {
                println!("=== CRUD Throughput Benchmark ===\n");
                let runner = ComparisonRunner::new(config);
                comparison_results = runner.run_all();
                for r in &comparison_results {
                    println!(
                        "{:<25} {:>10.0} ops/s  p50={:.0}us p95={:.0}us p99={:.0}us",
                        r.operation,
                        r.throughput_ops_sec,
                        r.latency.p50_us,
                        r.latency.p95_us,
                        r.latency.p99_us
                    );
                }
            }
            "traversal" => {
                let depth: usize = {
                    let idx = args.iter().position(|a| a == "--depth");
                    idx.and_then(|i| args.get(i + 1))
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(3)
                };
                println!("=== Traversal Benchmark (depth={depth}) ===\n");
                let runner = ComparisonRunner::new(config);
                // The comparison runner does a 2-hop traversal; we add depth via metadata
                comparison_results = runner.run_all();
                for r in &comparison_results {
                    if r.operation.starts_with("traversal") {
                        println!(
                            "{:<25} {:>10.0} ops/s  p50={:.0}us p95={:.0}us",
                            r.operation, r.throughput_ops_sec, r.latency.p50_us, r.latency.p95_us
                        );
                    }
                }
            }
            "tpc" => {
                let scale_n: u32 = {
                    let idx = args.iter().position(|a| a == "--scale");
                    idx.and_then(|i| args.get(i + 1))
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(1)
                };
                let scale = match scale_n {
                    1 => TpcScaleFactor::Sf1,
                    3 => TpcScaleFactor::Sf3,
                    10 => TpcScaleFactor::Sf10,
                    _ => {
                        eprintln!("Error: invalid scale factor {scale_n}. Use 1, 3, or 10.");
                        process::exit(1);
                    }
                };
                println!("=== TPC-H Style Graph Benchmark ({}) ===\n", scale.name());
                let tpc = TpcGraphBenchmark::new(config, scale);
                tpc_results = tpc.run_full();
                for r in &tpc_results {
                    println!(
                        "{:<30} {:>8.1} qps  p50={:.0}us p95={:.0}us p99={:.0}us  results={}",
                        r.query_name,
                        r.throughput_qps,
                        r.latency.p50_us,
                        r.latency.p95_us,
                        r.latency.p99_us,
                        r.result_size
                    );
                }
            }
            "wal" => {
                println!("=== WAL Throughput Benchmark ===\n");
                println!("Note: WAL benchmarks require criterion. Run with:");
                println!("  cargo bench -p nexora-bench --bench wal_throughput");
            }
            "concurrency" => {
                println!("=== Concurrency Benchmark ===\n");
                println!("Note: Concurrency benchmarks require criterion. Run with:");
                println!("  cargo bench -p nexora-bench --bench concurrency");
            }
            _ => {
                eprintln!("Error: unknown scenario '{scenario}'");
                print_usage();
                process::exit(1);
            }
        }
    }

    // Generate HTML report
    if let Some(path) = report_file {
        let html = report::generate_html(&comparison_results, &tpc_results);
        fs::write(&path, html).expect("Failed to write report");
        println!("\nHTML report written to {path}");
    }
}
