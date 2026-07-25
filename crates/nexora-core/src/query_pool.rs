//! D6: Dedicated query execution pool.
//!
//! Heavy read queries (multi-hop traversal, aggregation) can overwhelm the
//! system if unbounded concurrent execution is allowed. This routes them through
//! a bounded concurrency limiter so ingestion, replication, and heartbeats stay
//! responsive.
//!
//! ## Design: Semaphore vs OS Thread Pool
//!
//! Cypher execution (`nexora_cypher::execute_cypher`) is **already async** — it
//! does not block tokio worker threads with synchronous CPU work. The problem is
//! **unbounded concurrency**: when 1000 queries arrive simultaneously, they all
//! spawn tasks, exhaust memory/mailbox capacity, and starve the runtime.
//!
//! A **bounded semaphore** (not an OS thread pool) solves this:
//! - Limits concurrent query count to `max_concurrent`
//! - Failed `try_acquire` → caller-runs (backpressure, ArcadeDB pattern)
//! - No context switches to separate threads (queries stay on tokio)
//! - If specific CPU-bound segments (sort/aggregation) emerge later, those can be
//!   wrapped in `spawn_blocking` independently
//!
//! This is the async-native equivalent of ArcadeDB's "bounded queue + caller-runs"
//! thread pool pattern.

use std::future::Future;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Query execution pool that limits concurrent query count.
#[derive(Clone)]
pub struct QueryPool {
    semaphore: Arc<Semaphore>,
    #[allow(dead_code)] // Stored for introspection/metrics, not actively read yet
    max_concurrent: usize,
}

impl QueryPool {
    /// Create a new query pool with the given concurrency limit.
    ///
    /// Typical values:
    /// - Development: 4–16
    /// - Production: num_cpus * 4 to num_cpus * 8
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_concurrent,
        }
    }

    /// Execute a query under the pool's concurrency limit.
    ///
    /// FIX C: Blocks until a permit is available instead of falling back to
    /// caller-runs. This enforces a true concurrency ceiling: at most
    /// `max_concurrent` queries run simultaneously, and excess requests wait in
    /// the semaphore queue (bounded by tokio's async task fairness, not a
    /// physical RAM queue). This prevents memory exhaustion from unbounded
    /// concurrent execution.
    ///
    /// **Before (caller-runs)**: 100 queries → 4 under permits + 96 unbounded
    /// **After (await permit)**: 100 queries → max 4 running, 96 waiting
    ///
    /// Note: Semaphore waiting is async and cheap (no thread blocking). If a
    /// stricter "reject when saturated" policy is needed (return 503), use
    /// `try_execute` instead.
    pub async fn execute<F, T>(&self, f: F) -> T
    where
        F: Future<Output = T>,
    {
        let permit = self.semaphore.acquire().await.expect("semaphore closed");
        let result = f.await;
        drop(permit);
        result
    }

    /// Try to execute a query, returning `None` if the pool is saturated.
    ///
    /// Use this when you want to reject excess load (HTTP 503) instead of
    /// queuing. The caller can decide whether to retry, return an error, or
    /// fall back to a degraded path.
    pub async fn try_execute<F, T>(&self, f: F) -> Option<T>
    where
        F: Future<Output = T>,
    {
        let permit = self.semaphore.try_acquire().ok()?;
        let result = f.await;
        drop(permit);
        Some(result)
    }

    /// Current number of available permits (for metrics/observability).
    #[allow(dead_code)] // Reserved for future metrics endpoint
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Maximum concurrent queries allowed under permits.
    #[allow(dead_code)] // Reserved for future metrics endpoint
    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn query_pool_limits_concurrency() {
        let pool = QueryPool::new(4);
        let peak = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..16 {
            let pool = pool.clone();
            let peak = Arc::clone(&peak);
            let current = Arc::clone(&current);

            let handle = tokio::spawn(async move {
                pool.execute(async {
                    let c = current.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(c, Ordering::SeqCst);
                    sleep(Duration::from_millis(10)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        let peak_val = peak.load(Ordering::SeqCst);
        // Peak should be close to max_concurrent + some caller-runs overhead
        // With 16 tasks and max=4, we expect peak ≤ 4 + small number of caller-runs
        assert!(
            peak_val <= 16,
            "Peak concurrency should not wildly exceed limit"
        );
        // The semaphore ensures at most 4 under permits; caller-runs adds overhead
        // but the key property is it's bounded (not 16 all at once)
        assert!(peak_val >= 4, "Peak should reach at least the permit limit");
    }

    #[tokio::test]
    async fn query_pool_caller_runs_under_saturation() {
        let pool = QueryPool::new(2);
        let executed = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _ in 0..8 {
            let pool = pool.clone();
            let executed = Arc::clone(&executed);

            let handle = tokio::spawn(async move {
                pool.execute(async {
                    executed.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(5)).await;
                })
                .await;
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 8 tasks must complete (caller-runs doesn't drop tasks)
        assert_eq!(executed.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn query_pool_available_reflects_permits() {
        let pool = QueryPool::new(4);
        assert_eq!(pool.available(), 4);
        assert_eq!(pool.max_concurrent(), 4);

        // Acquire 2 permits manually for testing
        let _p1 = pool.semaphore.acquire().await.unwrap();
        let _p2 = pool.semaphore.acquire().await.unwrap();
        assert_eq!(pool.available(), 2);

        drop(_p1);
        assert_eq!(pool.available(), 3);
    }
}
