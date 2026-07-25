//! nexora-bench — Performance benchmark suite for the nexora graph database.
//!
//! This crate provides:
//! - Criterion micro-benchmarks for CRUD, traversal, WAL, and concurrency
//! - A TPC-H style synthetic social network benchmark
//! - HTML report generation with charts
//! - A CLI runner for orchestrating benchmark scenarios

pub mod comparison;
pub mod report;
pub mod tpc_graph;
pub mod util;

pub use comparison::{BenchmarkResult, ComparisonRunner};
pub use tpc_graph::{TpcGraphBenchmark, TpcScaleFactor};
pub use util::{
    generate_random_ids, make_edge, make_test_graph, percentile, timed, BenchConfig, LatencyStats,
};
