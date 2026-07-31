//! Prometheus metrics export for DeepStreaming.
//! Exposes counter, gauge, and histogram metrics at /metrics.

use axum::response::Json;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global metrics registry
pub struct Metrics {
    pub active_nodes: AtomicU64,
    pub standing_queries: AtomicU64,
    pub events_total: AtomicU64,
    pub sq_matches_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub fragment_count: AtomicU64,
    pub wal_append_total: AtomicU64,
    pub wal_append_sum_us: AtomicU64,
    /// Total number of Cypher queries executed.
    pub queries_total: AtomicU64,
    /// Sum of query durations in microseconds.
    pub query_duration_sum_us: AtomicU64,
    /// Number of slow queries (exceeded slow query threshold).
    pub slow_queries_total: AtomicU64,
    /// F4: current event-time watermark on the ingestion stream (milliseconds
    /// since epoch). 0 when watermarking is disabled or no event seen yet.
    pub watermark_current_ms: AtomicU64,
    /// F4: total events dropped for arriving beyond the allowed lateness.
    pub late_events_dropped_total: AtomicU64,
    /// F4: total event-time windows emitted (fired).
    pub window_fired_total: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active_nodes: AtomicU64::new(0),
            standing_queries: AtomicU64::new(0),
            events_total: AtomicU64::new(0),
            sq_matches_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            fragment_count: AtomicU64::new(0),
            wal_append_total: AtomicU64::new(0),
            wal_append_sum_us: AtomicU64::new(0),
            queries_total: AtomicU64::new(0),
            query_duration_sum_us: AtomicU64::new(0),
            slow_queries_total: AtomicU64::new(0),
            watermark_current_ms: AtomicU64::new(0),
            late_events_dropped_total: AtomicU64::new(0),
            window_fired_total: AtomicU64::new(0),
        })
    }

    /// F4: publish the current event-time watermark (milliseconds since epoch).
    pub fn set_watermark_ms(&self, ms: u64) {
        self.watermark_current_ms.store(ms, Ordering::Relaxed);
    }
    /// F4: record `n` late events dropped beyond the allowed lateness.
    ///
    /// F4: record `n` property writes/removals dropped by event-time LWW —
    /// late/out-of-order arrivals rejected because a newer value already exists.
    /// Wired from the bulk-ingest path, which sums `CommitReceipt::late_dropped`
    /// across the batch. (The event-time *windowing* path can also report here
    /// once it is driven by the app runtime.)
    pub fn inc_late_dropped(&self, n: u64) {
        self.late_events_dropped_total
            .fetch_add(n, Ordering::Relaxed);
    }
    /// F4: record `n` tumbling windows emitted. Wired from the tumbling-window
    /// analytics endpoint (`POST /api/v2/analytics/tumbling-window`), which sums
    /// the windows produced by each aggregate call.
    pub fn inc_windows_fired(&self, n: u64) {
        self.window_fired_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_events(&self, n: u64) {
        self.events_total.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_sq_matches(&self, n: u64) {
        self.sq_matches_total.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_errors(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_active_nodes(&self, n: u64) {
        self.active_nodes.store(n, Ordering::Relaxed);
    }
    pub fn set_sq_count(&self, n: u64) {
        self.standing_queries.store(n, Ordering::Relaxed);
    }

    /// Record a completed query: increment counter and add duration.
    pub fn record_query(&self, duration_us: u64) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.query_duration_sum_us
            .fetch_add(duration_us, Ordering::Relaxed);
    }

    /// Record a slow query.
    pub fn inc_slow_query(&self) {
        self.slow_queries_total.fetch_add(1, Ordering::Relaxed);
    }
}

/// Generate Prometheus text format metrics.
pub fn render_metrics(metrics: &Metrics) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "# HELP deepstreaming_active_nodes Number of active graph nodes\n# TYPE deepstreaming_active_nodes gauge\ndeepstreaming_active_nodes {}\n",
        metrics.active_nodes.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_standing_queries Number of registered standing queries\n# TYPE deepstreaming_standing_queries gauge\ndeepstreaming_standing_queries {}\n",
        metrics.standing_queries.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_events_total Total events ingested\n# TYPE deepstreaming_events_total counter\ndeepstreaming_events_total {}\n",
        metrics.events_total.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_sq_matches_total Total SQ match events\n# TYPE deepstreaming_sq_matches_total counter\ndeepstreaming_sq_matches_total {}\n",
        metrics.sq_matches_total.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_errors_total Total error count\n# TYPE deepstreaming_errors_total counter\ndeepstreaming_errors_total {}\n",
        metrics.errors_total.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_fragment_count Number of time fragments\n# TYPE deepstreaming_fragment_count gauge\ndeepstreaming_fragment_count {}\n",
        metrics.fragment_count.load(Ordering::Relaxed)
    ));
    let total = metrics.wal_append_total.load(Ordering::Relaxed);
    let sum_us = metrics.wal_append_sum_us.load(Ordering::Relaxed);
    let avg_us = sum_us.checked_div(total).unwrap_or(0);
    out.push_str(&format!(
        "# HELP deepstreaming_wal_append_total Total WAL append count\n# TYPE deepstreaming_wal_append_total counter\ndeepstreaming_wal_append_total {}\n",
        total
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_wal_append_avg_us Average WAL append latency (microseconds)\n# TYPE deepstreaming_wal_append_avg_us gauge\ndeepstreaming_wal_append_avg_us {}\n",
        avg_us
    ));
    let q_total = metrics.queries_total.load(Ordering::Relaxed);
    let q_sum_us = metrics.query_duration_sum_us.load(Ordering::Relaxed);
    let q_avg_us = q_sum_us.checked_div(q_total).unwrap_or(0);
    out.push_str(&format!(
        "# HELP deepstreaming_queries_total Total Cypher queries executed\n# TYPE deepstreaming_queries_total counter\ndeepstreaming_queries_total {}\n",
        q_total
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_query_avg_us Average query latency (microseconds)\n# TYPE deepstreaming_query_avg_us gauge\ndeepstreaming_query_avg_us {}\n",
        q_avg_us
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_slow_queries_total Total slow queries (exceeded threshold)\n# TYPE deepstreaming_slow_queries_total counter\ndeepstreaming_slow_queries_total {}\n",
        metrics.slow_queries_total.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_watermark_current_ms Current event-time watermark (ms since epoch)\n# TYPE deepstreaming_watermark_current_ms gauge\ndeepstreaming_watermark_current_ms {}\n",
        metrics.watermark_current_ms.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_late_events_dropped_total Events dropped beyond allowed lateness\n# TYPE deepstreaming_late_events_dropped_total counter\ndeepstreaming_late_events_dropped_total {}\n",
        metrics.late_events_dropped_total.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "# HELP deepstreaming_window_fired_total Total event-time windows emitted\n# TYPE deepstreaming_window_fired_total counter\ndeepstreaming_window_fired_total {}\n",
        metrics.window_fired_total.load(Ordering::Relaxed)
    ));
    out
}

/// Generate JSON format metrics for the frontend.
pub fn render_json(metrics: &Metrics) -> Json<serde_json::Value> {
    let total = metrics.wal_append_total.load(Ordering::Relaxed);
    let sum_us = metrics.wal_append_sum_us.load(Ordering::Relaxed);
    let q_total = metrics.queries_total.load(Ordering::Relaxed);
    let q_sum_us = metrics.query_duration_sum_us.load(Ordering::Relaxed);
    Json(json!({
        "active_nodes": metrics.active_nodes.load(Ordering::Relaxed),
        "standing_queries": metrics.standing_queries.load(Ordering::Relaxed),
        "events_total": metrics.events_total.load(Ordering::Relaxed),
        "sq_matches_total": metrics.sq_matches_total.load(Ordering::Relaxed),
        "errors_total": metrics.errors_total.load(Ordering::Relaxed),
        "fragment_count": metrics.fragment_count.load(Ordering::Relaxed),
        "wal_append_total": total,
        "wal_append_avg_us": sum_us.checked_div(total).unwrap_or(0),
        "queries_total": q_total,
        "query_avg_us": q_sum_us.checked_div(q_total).unwrap_or(0),
        "slow_queries_total": metrics.slow_queries_total.load(Ordering::Relaxed),
        "watermark_current_ms": metrics.watermark_current_ms.load(Ordering::Relaxed),
        "late_events_dropped_total": metrics.late_events_dropped_total.load(Ordering::Relaxed),
        "window_fired_total": metrics.window_fired_total.load(Ordering::Relaxed),
    }))
}
