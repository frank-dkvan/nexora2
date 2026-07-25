//! Slow query logging for performance analysis and optimization.
//!
//! Tracks query execution time and logs queries exceeding a configured threshold.
//! Useful for identifying performance bottlenecks in production workloads.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Configuration for slow query logging
#[derive(Debug, Clone)]
pub struct SlowQueryConfig {
    /// Threshold for logging slow queries
    pub threshold: Duration,
    /// Whether slow query logging is enabled
    pub enabled: bool,
    /// Maximum number of recent slow queries to keep in memory
    pub max_history: usize,
}

impl Default for SlowQueryConfig {
    fn default() -> Self {
        Self {
            threshold: Duration::from_secs(1),
            enabled: true,
            max_history: 100,
        }
    }
}

/// A recorded slow query
#[derive(Debug, Clone)]
pub struct SlowQueryRecord {
    /// Query text
    pub query: String,
    /// Execution duration
    pub duration: Duration,
    /// Timestamp when the query started
    pub started_at: std::time::SystemTime,
    /// Number of rows returned
    pub rows_returned: usize,
    /// Execution plan summary (if available)
    pub plan_summary: Option<String>,
}

/// Slow query logger
pub struct SlowQueryLogger {
    config: RwLock<SlowQueryConfig>,
    history: RwLock<Vec<SlowQueryRecord>>,
}

impl SlowQueryLogger {
    pub fn new(config: SlowQueryConfig) -> Arc<Self> {
        Arc::new(Self {
            config: RwLock::new(config),
            history: RwLock::new(Vec::new()),
        })
    }

    /// Start tracking a query execution
    pub fn start_query(&self, query: String) -> QueryTimer {
        QueryTimer {
            query,
            started: Instant::now(),
            started_at: std::time::SystemTime::now(),
        }
    }

    /// Record a completed query if it exceeds the threshold
    pub async fn record(
        &self,
        timer: QueryTimer,
        rows_returned: usize,
        plan_summary: Option<String>,
    ) {
        let duration = timer.started.elapsed();
        let config = self.config.read().await;

        if !config.enabled || duration < config.threshold {
            return;
        }

        let record = SlowQueryRecord {
            query: timer.query.clone(),
            duration,
            started_at: timer.started_at,
            rows_returned,
            plan_summary: plan_summary.clone(),
        };

        // Log to tracing
        tracing::warn!(
            query = %timer.query,
            duration_ms = duration.as_millis(),
            rows = rows_returned,
            plan = ?plan_summary,
            "Slow query detected"
        );

        // Store in history
        let mut history = self.history.write().await;
        history.push(record);

        // Keep only the most recent max_history entries
        if history.len() > config.max_history {
            history.remove(0);
        }
    }

    /// Get recent slow queries
    pub async fn get_recent(&self, limit: usize) -> Vec<SlowQueryRecord> {
        let history = self.history.read().await;
        let start = history.len().saturating_sub(limit);
        history[start..].to_vec()
    }

    /// Clear all recorded slow queries
    pub async fn clear_history(&self) {
        self.history.write().await.clear();
    }

    /// Update configuration
    pub async fn update_config(&self, config: SlowQueryConfig) {
        *self.config.write().await = config;
    }

    /// Get current configuration
    pub async fn get_config(&self) -> SlowQueryConfig {
        self.config.read().await.clone()
    }

    /// Get statistics about slow queries
    pub async fn get_stats(&self) -> SlowQueryStats {
        let history = self.history.read().await;

        if history.is_empty() {
            return SlowQueryStats::default();
        }

        let total_count = history.len();
        let total_duration: Duration = history.iter().map(|r| r.duration).sum();
        let avg_duration = total_duration / total_count as u32;

        let mut durations: Vec<Duration> = history.iter().map(|r| r.duration).collect();
        durations.sort();

        let p50 = durations[total_count / 2];
        let p95 = durations[(total_count * 95) / 100];
        let p99 = durations[(total_count * 99) / 100];

        SlowQueryStats {
            total_count,
            avg_duration,
            p50_duration: p50,
            p95_duration: p95,
            p99_duration: p99,
        }
    }
}

/// Query execution timer
pub struct QueryTimer {
    query: String,
    started: Instant,
    started_at: std::time::SystemTime,
}

/// Statistics about slow queries
#[derive(Debug, Default, Clone)]
pub struct SlowQueryStats {
    pub total_count: usize,
    pub avg_duration: Duration,
    pub p50_duration: Duration,
    pub p95_duration: Duration,
    pub p99_duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slow_query_logging() {
        let config = SlowQueryConfig {
            threshold: Duration::from_millis(100),
            enabled: true,
            max_history: 10,
        };
        let logger = SlowQueryLogger::new(config);

        // Fast query - should not be logged
        let timer = logger.start_query("SELECT * FROM nodes".to_string());
        logger.record(timer, 10, None).await;

        let history = logger.get_recent(100).await;
        assert_eq!(history.len(), 0);

        // Slow query - should be logged
        let timer = logger.start_query("SELECT * FROM huge_table".to_string());
        tokio::time::sleep(Duration::from_millis(150)).await;
        logger
            .record(timer, 1000, Some("Sequential scan".to_string()))
            .await;

        let history = logger.get_recent(100).await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].query, "SELECT * FROM huge_table");
        assert!(history[0].duration >= Duration::from_millis(150));
    }

    #[tokio::test]
    async fn test_history_limit() {
        let config = SlowQueryConfig {
            threshold: Duration::from_millis(1),
            enabled: true,
            max_history: 3,
        };
        let logger = SlowQueryLogger::new(config);

        // Add 5 slow queries
        for i in 0..5 {
            let timer = logger.start_query(format!("Query {}", i));
            tokio::time::sleep(Duration::from_millis(2)).await;
            logger.record(timer, 10, None).await;
        }

        let history = logger.get_recent(100).await;
        assert_eq!(history.len(), 3); // Only last 3 retained
        assert_eq!(history[0].query, "Query 2");
        assert_eq!(history[2].query, "Query 4");
    }
}
