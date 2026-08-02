//! Prometheus metrics registry for Nexora
//!
//! Provides centralized metrics collection for:
//! - Raft operations (proposals, commits, log size)
//! - Graph queries (latency, throughput, cache hits)
//! - Storage operations (RocksDB, WAL, checkpoints)
//! - Network (RPC calls, bytes transferred)

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};

/// Centralized metrics registry
pub struct MetricsRegistry {
    registry: Registry,

    // Raft metrics
    pub raft_proposals_total: Counter,
    pub raft_commits_total: Counter,
    pub raft_log_size: Gauge,
    pub raft_leader_changes_total: Counter,
    pub raft_heartbeat_latency: Histogram,

    // Graph query metrics
    pub query_duration_seconds: HistogramVec,
    pub query_total: CounterVec,
    pub query_errors_total: CounterVec,
    pub active_queries: Gauge,

    // Storage metrics
    pub rocksdb_read_bytes: Counter,
    pub rocksdb_write_bytes: Counter,
    pub rocksdb_cache_hit_rate: Gauge,
    pub wal_sync_duration_seconds: Histogram,
    pub wal_fsync_total: Counter,
    pub checkpoint_duration_seconds: Histogram,

    // Network metrics
    pub rpc_requests_total: CounterVec,
    pub rpc_duration_seconds: HistogramVec,
    pub rpc_bytes_sent: Counter,
    pub rpc_bytes_received: Counter,
}

impl MetricsRegistry {
    /// Create new metrics registry with all standard metrics
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();

        // Raft metrics
        let raft_proposals_total = Counter::with_opts(Opts::new(
            "nexora_raft_proposals_total",
            "Total number of Raft proposals submitted",
        ))?;
        registry.register(Box::new(raft_proposals_total.clone()))?;

        let raft_commits_total = Counter::with_opts(Opts::new(
            "nexora_raft_commits_total",
            "Total number of Raft log entries committed",
        ))?;
        registry.register(Box::new(raft_commits_total.clone()))?;

        let raft_log_size = Gauge::with_opts(Opts::new(
            "nexora_raft_log_size",
            "Current Raft log size in entries",
        ))?;
        registry.register(Box::new(raft_log_size.clone()))?;

        let raft_leader_changes_total = Counter::with_opts(Opts::new(
            "nexora_raft_leader_changes_total",
            "Total number of Raft leader changes",
        ))?;
        registry.register(Box::new(raft_leader_changes_total.clone()))?;

        let raft_heartbeat_latency = Histogram::with_opts(HistogramOpts::new(
            "nexora_raft_heartbeat_latency_seconds",
            "Raft heartbeat round-trip latency",
        ))?;
        registry.register(Box::new(raft_heartbeat_latency.clone()))?;

        // Graph query metrics
        let query_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "nexora_query_duration_seconds",
                "Query execution duration in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["query_type"],
        )?;
        registry.register(Box::new(query_duration_seconds.clone()))?;

        let query_total = CounterVec::new(
            Opts::new("nexora_query_total", "Total number of queries executed"),
            &["query_type", "status"],
        )?;
        registry.register(Box::new(query_total.clone()))?;

        let query_errors_total = CounterVec::new(
            Opts::new("nexora_query_errors_total", "Total number of query errors"),
            &["query_type", "error_type"],
        )?;
        registry.register(Box::new(query_errors_total.clone()))?;

        let active_queries = Gauge::with_opts(Opts::new(
            "nexora_active_queries",
            "Number of currently executing queries",
        ))?;
        registry.register(Box::new(active_queries.clone()))?;

        // Storage metrics
        let rocksdb_read_bytes = Counter::with_opts(Opts::new(
            "nexora_rocksdb_read_bytes_total",
            "Total bytes read from RocksDB",
        ))?;
        registry.register(Box::new(rocksdb_read_bytes.clone()))?;

        let rocksdb_write_bytes = Counter::with_opts(Opts::new(
            "nexora_rocksdb_write_bytes_total",
            "Total bytes written to RocksDB",
        ))?;
        registry.register(Box::new(rocksdb_write_bytes.clone()))?;

        let rocksdb_cache_hit_rate = Gauge::with_opts(Opts::new(
            "nexora_rocksdb_cache_hit_rate",
            "RocksDB block cache hit rate (0-1)",
        ))?;
        registry.register(Box::new(rocksdb_cache_hit_rate.clone()))?;

        let wal_sync_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "nexora_wal_sync_duration_seconds",
                "WAL fsync duration in seconds",
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        )?;
        registry.register(Box::new(wal_sync_duration_seconds.clone()))?;

        let wal_fsync_total = Counter::with_opts(Opts::new(
            "nexora_wal_fsync_total",
            "Total number of WAL fsync operations",
        ))?;
        registry.register(Box::new(wal_fsync_total.clone()))?;

        let checkpoint_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "nexora_checkpoint_duration_seconds",
                "Checkpoint creation duration in seconds",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]),
        )?;
        registry.register(Box::new(checkpoint_duration_seconds.clone()))?;

        // Network metrics
        let rpc_requests_total = CounterVec::new(
            Opts::new("nexora_rpc_requests_total", "Total number of RPC requests"),
            &["method", "status"],
        )?;
        registry.register(Box::new(rpc_requests_total.clone()))?;

        let rpc_duration_seconds = HistogramVec::new(
            HistogramOpts::new("nexora_rpc_duration_seconds", "RPC request duration")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]),
            &["method"],
        )?;
        registry.register(Box::new(rpc_duration_seconds.clone()))?;

        let rpc_bytes_sent = Counter::with_opts(Opts::new(
            "nexora_rpc_bytes_sent_total",
            "Total bytes sent via RPC",
        ))?;
        registry.register(Box::new(rpc_bytes_sent.clone()))?;

        let rpc_bytes_received = Counter::with_opts(Opts::new(
            "nexora_rpc_bytes_received_total",
            "Total bytes received via RPC",
        ))?;
        registry.register(Box::new(rpc_bytes_received.clone()))?;

        Ok(Self {
            registry,
            raft_proposals_total,
            raft_commits_total,
            raft_log_size,
            raft_leader_changes_total,
            raft_heartbeat_latency,
            query_duration_seconds,
            query_total,
            query_errors_total,
            active_queries,
            rocksdb_read_bytes,
            rocksdb_write_bytes,
            rocksdb_cache_hit_rate,
            wal_sync_duration_seconds,
            wal_fsync_total,
            checkpoint_duration_seconds,
            rpc_requests_total,
            rpc_duration_seconds,
            rpc_bytes_sent,
            rpc_bytes_received,
        })
    }

    /// Export metrics in Prometheus text format
    pub fn export(&self) -> anyhow::Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    /// Get underlying registry for custom metrics
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new().expect("Failed to create metrics registry")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = MetricsRegistry::new().unwrap();

        // Increment some metrics
        metrics.raft_proposals_total.inc();
        metrics.query_total.with_label_values(&["cypher", "success"]).inc();

        // Export should succeed
        let output = metrics.export().unwrap();
        assert!(output.contains("nexora_raft_proposals_total"));
        assert!(output.contains("nexora_query_total"));
    }

    #[test]
    fn test_query_metrics() {
        let metrics = MetricsRegistry::new().unwrap();

        metrics.query_total.with_label_values(&["cypher", "success"]).inc();
        metrics.query_duration_seconds.with_label_values(&["cypher"]).observe(0.123);

        let output = metrics.export().unwrap();
        assert!(output.contains("query_type=\"cypher\""));
    }
}
