//! Replication monitoring — tracks lag, failover events, and catch-up progress.
//!
//! This module periodically updates production-critical metrics that help operators
//! detect replication issues, monitor failover health, and track recovery progress.

use crate::metrics::Metrics;
use nexora_raft::RaftLogReplicator;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::interval;

/// Tracks replication state for monitoring purposes.
pub struct ReplicationMonitor {
    metrics: Arc<Metrics>,
    /// Per-shard Raft replicators.
    replicators: Arc<RwLock<Vec<Arc<RaftLogReplicator>>>>,
    /// Failover tracking state.
    failover_state: Arc<RwLock<FailoverState>>,
}

#[derive(Clone, Debug)]
struct FailoverState {
    /// Total failover attempts since start.
    total_attempts: u64,
    /// Total successful failovers since start.
    total_success: u64,
    /// Catch-up in progress flag.
    catch_up_in_progress: bool,
    /// Start time of current catch-up operation.
    catch_up_start: Option<Instant>,
}

impl Default for FailoverState {
    fn default() -> Self {
        Self {
            total_attempts: 0,
            total_success: 0,
            catch_up_in_progress: false,
            catch_up_start: None,
        }
    }
}

impl ReplicationMonitor {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            replicators: Arc::new(RwLock::new(Vec::new())),
            failover_state: Arc::new(RwLock::new(FailoverState::default())),
        }
    }

    /// Register a shard replicator for monitoring.
    pub async fn register_replicator(&self, replicator: Arc<RaftLogReplicator>) {
        let mut reps = self.replicators.write().await;
        reps.push(replicator);
        tracing::debug!(
            shard_id = reps.len() - 1,
            "Registered replicator for monitoring"
        );
    }

    /// Record a failover attempt.
    pub async fn record_failover_attempt(&self) {
        let mut state = self.failover_state.write().await;
        state.total_attempts += 1;
        self.metrics.inc_failover();
        tracing::info!(
            total_attempts = state.total_attempts,
            "Failover attempt recorded"
        );
    }

    /// Record a successful failover completion.
    pub async fn record_failover_success(&self) {
        let mut state = self.failover_state.write().await;
        state.total_success += 1;
        self.metrics.inc_failover_success();
        tracing::info!(
            total_success = state.total_success,
            success_rate = format!(
                "{:.1}%",
                (state.total_success as f64 / state.total_attempts.max(1) as f64) * 100.0
            ),
            "Failover succeeded"
        );
    }

    /// Mark catch-up operation as started.
    pub async fn start_catch_up(&self) {
        let mut state = self.failover_state.write().await;
        state.catch_up_in_progress = true;
        state.catch_up_start = Some(Instant::now());
        self.metrics.set_catch_up_in_progress(true);
        tracing::info!("Catch-up operation started");
    }

    /// Mark catch-up operation as completed and record duration.
    pub async fn complete_catch_up(&self) {
        let mut state = self.failover_state.write().await;
        state.catch_up_in_progress = false;
        if let Some(start) = state.catch_up_start.take() {
            let duration_ms = start.elapsed().as_millis() as u64;
            self.metrics.set_catch_up_duration_ms(duration_ms);
            tracing::info!(
                duration_ms = duration_ms,
                "Catch-up operation completed"
            );
        }
        self.metrics.set_catch_up_in_progress(false);
    }

    /// Compute maximum replication lag across all shards.
    async fn compute_max_replication_lag(&self) -> u64 {
        let replicators = self.replicators.read().await;
        let mut max_lag_ms = 0u64;

        for (shard_id, replicator) in replicators.iter().enumerate() {
            let commit_index = replicator.commit_index().await;
            let last_applied = replicator.last_applied().await;

            if last_applied > commit_index {
                let lag_entries = last_applied - commit_index;
                // Estimate: assume ~1ms per entry (rough heuristic for lag estimation)
                let estimated_lag_ms = lag_entries;

                if estimated_lag_ms > max_lag_ms {
                    max_lag_ms = estimated_lag_ms;
                    tracing::trace!(
                        shard_id = shard_id,
                        commit_index = commit_index,
                        last_applied = last_applied,
                        lag_entries = lag_entries,
                        "Replication lag detected"
                    );
                }
            }
        }

        max_lag_ms
    }

    /// Start the monitoring loop that periodically updates metrics.
    pub fn start_monitoring_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;

                // Update replication lag metric
                let max_lag_ms = self.compute_max_replication_lag().await;
                self.metrics.set_replication_lag_ms(max_lag_ms);

                if max_lag_ms > 1000 {
                    tracing::warn!(
                        max_lag_ms = max_lag_ms,
                        "High replication lag detected"
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_failover_tracking() {
        let metrics = Metrics::new();
        let monitor = ReplicationMonitor::new(metrics.clone());

        // Record 3 attempts, 2 successes
        monitor.record_failover_attempt().await;
        monitor.record_failover_success().await;

        monitor.record_failover_attempt().await;
        monitor.record_failover_success().await;

        monitor.record_failover_attempt().await;

        let state = monitor.failover_state.read().await;
        assert_eq!(state.total_attempts, 3);
        assert_eq!(state.total_success, 2);
    }

    #[tokio::test]
    async fn test_catch_up_duration() {
        let metrics = Metrics::new();
        let monitor = ReplicationMonitor::new(metrics.clone());

        monitor.start_catch_up().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        monitor.complete_catch_up().await;

        let duration = metrics.catch_up_duration_ms.load(std::sync::atomic::Ordering::Relaxed);
        assert!(duration >= 100, "Expected duration >= 100ms, got {}", duration);

        let in_progress = metrics.catch_up_in_progress.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(in_progress, 0);
    }
}
