// P1-5: 复制延迟可观测性
//
// Prometheus 指标暴露：
// - 复制延迟（p50/p95/p99）
// - 复制成功率
// - 分片落后程度（lag）
// - 失败重试次数

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 复制延迟直方图桶（毫秒）
#[allow(dead_code)] // C2: Replication metrics stub, will be used for histogram registration
const LATENCY_BUCKETS: &[f64] = &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

/// 复制指标收集器
#[derive(Clone)]
pub struct ReplicationMetrics {
    inner: Arc<ReplicationMetricsInner>,
}

struct ReplicationMetricsInner {
    /// 复制尝试总数
    attempts: AtomicU64,
    /// 复制成功总数
    successes: AtomicU64,
    /// 复制失败总数
    failures: AtomicU64,
    /// 法定人数成功总数
    quorum_successes: AtomicU64,
    /// 法定人数失败总数
    quorum_failures: AtomicU64,
    /// 缺失确认总数
    missing_acks: AtomicU64,
    /// 追随者拒绝总数
    follower_nacks: AtomicU64,
    /// 延迟样本（简化版，生产环境应使用 prometheus 的 Histogram）
    latency_samples: std::sync::Mutex<Vec<Duration>>,
}

impl ReplicationMetrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ReplicationMetricsInner {
                attempts: AtomicU64::new(0),
                successes: AtomicU64::new(0),
                failures: AtomicU64::new(0),
                quorum_successes: AtomicU64::new(0),
                quorum_failures: AtomicU64::new(0),
                missing_acks: AtomicU64::new(0),
                follower_nacks: AtomicU64::new(0),
                latency_samples: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    /// 记录一次复制尝试
    pub fn record_attempt(&self) {
        self.inner.attempts.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次复制成功
    pub fn record_success(&self) {
        self.inner.successes.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次复制失败
    pub fn record_failure(&self) {
        self.inner.failures.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次法定人数成功
    pub fn record_quorum_success(&self) {
        self.inner.quorum_successes.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次法定人数失败
    pub fn record_quorum_failure(&self) {
        self.inner.quorum_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录缺失确认数
    pub fn record_missing_acks(&self, count: u64) {
        self.inner.missing_acks.fetch_add(count, Ordering::Relaxed);
    }

    /// 记录追随者拒绝数
    pub fn record_follower_nacks(&self, count: u64) {
        self.inner
            .follower_nacks
            .fetch_add(count, Ordering::Relaxed);
    }

    /// 记录复制延迟
    pub fn record_latency(&self, latency: Duration) {
        let mut samples = self.inner.latency_samples.lock().unwrap();
        samples.push(latency);

        // 限制样本数量（滑动窗口，保留最近 10000 个）
        if samples.len() > 10_000 {
            samples.drain(0..5_000);
        }
    }

    /// 获取快照（用于测试和导出）
    pub fn snapshot(&self) -> MetricsSnapshot {
        let samples = self.inner.latency_samples.lock().unwrap();
        let mut sorted_samples: Vec<Duration> = samples.clone();
        sorted_samples.sort();

        let p50 = percentile(&sorted_samples, 0.50);
        let p95 = percentile(&sorted_samples, 0.95);
        let p99 = percentile(&sorted_samples, 0.99);

        MetricsSnapshot {
            attempts: self.inner.attempts.load(Ordering::Relaxed),
            successes: self.inner.successes.load(Ordering::Relaxed),
            failures: self.inner.failures.load(Ordering::Relaxed),
            quorum_ok: self.inner.quorum_successes.load(Ordering::Relaxed),
            quorum_failed: self.inner.quorum_failures.load(Ordering::Relaxed),
            missing_acks_total: self.inner.missing_acks.load(Ordering::Relaxed),
            follower_nacks: self.inner.follower_nacks.load(Ordering::Relaxed),
            latency_p50: p50,
            latency_p95: p95,
            latency_p99: p99,
        }
    }

    /// 导出为 Prometheus 文本格式
    pub fn export_prometheus(&self) -> String {
        let snapshot = self.snapshot();

        format!(
            r#"# HELP replication_attempts_total Total number of replication attempts
# TYPE replication_attempts_total counter
replication_attempts_total {}

# HELP replication_successes_total Total number of successful replications
# TYPE replication_successes_total counter
replication_successes_total {}

# HELP replication_failures_total Total number of failed replications
# TYPE replication_failures_total counter
replication_failures_total {}

# HELP replication_quorum_ok_total Total number of quorum-successful writes
# TYPE replication_quorum_ok_total counter
replication_quorum_ok_total {}

# HELP replication_quorum_failed_total Total number of quorum-failed writes
# TYPE replication_quorum_failed_total counter
replication_quorum_failed_total {}

# HELP replication_missing_acks_total Total missing acknowledgments
# TYPE replication_missing_acks_total counter
replication_missing_acks_total {}

# HELP replication_follower_nacks_total Total follower rejections
# TYPE replication_follower_nacks_total counter
replication_follower_nacks_total {}

# HELP replication_latency_seconds Replication latency percentiles
# TYPE replication_latency_seconds summary
replication_latency_seconds{{quantile="0.5"}} {:.6}
replication_latency_seconds{{quantile="0.95"}} {:.6}
replication_latency_seconds{{quantile="0.99"}} {:.6}
"#,
            snapshot.attempts,
            snapshot.successes,
            snapshot.failures,
            snapshot.quorum_ok,
            snapshot.quorum_failed,
            snapshot.missing_acks_total,
            snapshot.follower_nacks,
            snapshot.latency_p50.as_secs_f64(),
            snapshot.latency_p95.as_secs_f64(),
            snapshot.latency_p99.as_secs_f64(),
        )
    }
}

impl Default for ReplicationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// 指标快照
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub quorum_ok: u64,
    pub quorum_failed: u64,
    pub missing_acks_total: u64,
    pub follower_nacks: u64,
    pub latency_p50: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
}

/// 计算百分位数
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::from_millis(0);
    }

    let idx = ((sorted.len() as f64) * p).floor() as usize;
    let idx = idx.min(sorted.len() - 1);
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_recording() {
        let metrics = ReplicationMetrics::new();

        metrics.record_attempt();
        metrics.record_success();
        metrics.record_quorum_success();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.attempts, 1);
        assert_eq!(snapshot.successes, 1);
        assert_eq!(snapshot.quorum_ok, 1);
    }

    #[test]
    fn test_latency_percentiles() {
        let metrics = ReplicationMetrics::new();

        // 记录一些延迟样本
        for i in 1..=100 {
            metrics.record_latency(Duration::from_millis(i));
        }

        let snapshot = metrics.snapshot();
        assert!(snapshot.latency_p50 >= Duration::from_millis(49));
        assert!(snapshot.latency_p50 <= Duration::from_millis(51));
        assert!(snapshot.latency_p95 >= Duration::from_millis(94));
        assert!(snapshot.latency_p99 >= Duration::from_millis(98));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = ReplicationMetrics::new();

        metrics.record_attempt();
        metrics.record_success();
        metrics.record_latency(Duration::from_millis(10));

        let output = metrics.export_prometheus();
        assert!(output.contains("replication_attempts_total 1"));
        assert!(output.contains("replication_successes_total 1"));
        assert!(output.contains("replication_latency_seconds"));
    }

    #[test]
    fn test_missing_acks_and_nacks() {
        let metrics = ReplicationMetrics::new();

        metrics.record_missing_acks(3);
        metrics.record_follower_nacks(2);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.missing_acks_total, 3);
        assert_eq!(snapshot.follower_nacks, 2);
    }

    #[test]
    fn test_empty_latency_samples() {
        let metrics = ReplicationMetrics::new();
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.latency_p50, Duration::from_millis(0));
        assert_eq!(snapshot.latency_p95, Duration::from_millis(0));
        assert_eq!(snapshot.latency_p99, Duration::from_millis(0));
    }
}
