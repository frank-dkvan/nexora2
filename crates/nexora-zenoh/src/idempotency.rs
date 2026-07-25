//! Idempotency tracking for distributed writes.
//!
//! Provides request deduplication to prevent duplicate writes from client retries
//! or internal retry logic, ensuring exactly-once semantics for streaming workloads.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Time-to-live for request IDs in the deduplication cache
const DEFAULT_TTL: Duration = Duration::from_secs(300); // 5 minutes
/// How often to run GC on the idempotency cache
const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60); // 1 minute

/// Tracks recently processed request IDs to detect and reject duplicates
pub struct IdempotencyTracker {
    /// Map of request_id -> (result, timestamp)
    cache: Arc<RwLock<HashMap<String, (WriteResult, Instant)>>>,
    /// How long to keep request IDs in the cache
    ttl: Duration,
    /// Background GC task handle (P1-1 fix: automatically clean expired entries)
    _gc_task: Option<JoinHandle<()>>,
}

/// Cached result of a previously processed write
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WriteResult {
    /// Write succeeded with the given sequence number
    Committed { seq: u64 },
    /// Write failed with the given error message
    Failed { error: String },
}

impl IdempotencyTracker {
    /// Create a new idempotency tracker with default TTL and automatic GC
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_TTL)
    }

    /// Create a new idempotency tracker with custom TTL and automatic GC
    pub fn with_ttl(ttl: Duration) -> Self {
        Self::with_ttl_and_gc(ttl, DEFAULT_GC_INTERVAL)
    }

    /// Create a new idempotency tracker with custom TTL and GC interval
    ///
    /// P1-1 fix: Automatically spawns a background task to clean expired entries.
    /// Pass `gc_interval = Duration::ZERO` to disable automatic GC (testing only).
    pub fn with_ttl_and_gc(ttl: Duration, gc_interval: Duration) -> Self {
        let cache = Arc::new(RwLock::new(HashMap::new()));

        let gc_task = if gc_interval > Duration::ZERO {
            let cache_clone = cache.clone();
            let ttl_clone = ttl;
            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(gc_interval);
                interval.tick().await; // skip first immediate tick
                loop {
                    interval.tick().await;
                    let now = Instant::now();
                    let mut cache = cache_clone.write().await;
                    let before = cache.len();
                    cache.retain(|_, (_, timestamp)| now.duration_since(*timestamp) < ttl_clone);
                    let evicted = before.saturating_sub(cache.len());
                    if evicted > 0 {
                        tracing::debug!("Idempotency GC: evicted {evicted} expired entries (cache size: {} -> {})", before, cache.len());
                    }
                }
            }))
        } else {
            None
        };

        Self {
            cache,
            ttl,
            _gc_task: gc_task,
        }
    }

    /// Check if a request ID has been seen before
    ///
    /// Returns:
    /// - `Some(WriteResult)` if this is a duplicate request (return cached result)
    /// - `None` if this is a new request (proceed with write)
    pub async fn check_duplicate(&self, request_id: &str) -> Option<WriteResult> {
        let cache = self.cache.read().await;
        cache.get(request_id).map(|(result, _)| result.clone())
    }

    /// Record a successful write for deduplication
    pub async fn record_success(&self, request_id: String, seq: u64) {
        let mut cache = self.cache.write().await;
        cache.insert(request_id, (WriteResult::Committed { seq }, Instant::now()));
    }

    /// Record a failed write for deduplication
    pub async fn record_failure(&self, request_id: String, error: String) {
        let mut cache = self.cache.write().await;
        cache.insert(request_id, (WriteResult::Failed { error }, Instant::now()));
    }

    /// Remove expired entries from the cache (garbage collection)
    pub async fn gc(&self) {
        let now = Instant::now();
        let mut cache = self.cache.write().await;
        cache.retain(|_, (_, timestamp)| now.duration_since(*timestamp) < self.ttl);
    }

    /// Get the current cache size (for metrics/debugging)
    pub async fn cache_size(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Clear all entries (for testing)
    #[cfg(test)]
    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }
}

impl Default for IdempotencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_request_not_duplicate() {
        let tracker = IdempotencyTracker::new();
        assert!(tracker.check_duplicate("req-123").await.is_none());
    }

    #[tokio::test]
    async fn test_duplicate_request_detected() {
        let tracker = IdempotencyTracker::new();

        // Record a successful write
        tracker.record_success("req-123".to_string(), 42).await;

        // Check for duplicate
        let result = tracker.check_duplicate("req-123").await;
        assert!(result.is_some());

        if let Some(WriteResult::Committed { seq }) = result {
            assert_eq!(seq, 42);
        } else {
            panic!("Expected Committed result");
        }
    }

    #[tokio::test]
    async fn test_failed_request_cached() {
        let tracker = IdempotencyTracker::new();

        // Record a failed write
        tracker
            .record_failure("req-456".to_string(), "timeout".to_string())
            .await;

        // Check for duplicate
        let result = tracker.check_duplicate("req-456").await;
        assert!(result.is_some());

        if let Some(WriteResult::Failed { error }) = result {
            assert_eq!(error, "timeout");
        } else {
            panic!("Expected Failed result");
        }
    }

    #[tokio::test]
    async fn test_gc_removes_expired_entries() {
        let tracker = IdempotencyTracker::with_ttl(Duration::from_millis(50));

        tracker.record_success("req-old".to_string(), 1).await;
        assert_eq!(tracker.cache_size().await, 1);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(100)).await;

        tracker.gc().await;
        assert_eq!(tracker.cache_size().await, 0);
    }

    #[tokio::test]
    async fn test_gc_keeps_fresh_entries() {
        let tracker = IdempotencyTracker::with_ttl(Duration::from_secs(10));

        tracker.record_success("req-fresh".to_string(), 1).await;
        tracker.gc().await;

        assert_eq!(tracker.cache_size().await, 1);
        assert!(tracker.check_duplicate("req-fresh").await.is_some());
    }

    #[tokio::test]
    async fn test_multiple_requests() {
        let tracker = IdempotencyTracker::new();

        tracker.record_success("req-1".to_string(), 10).await;
        tracker.record_success("req-2".to_string(), 20).await;
        tracker
            .record_failure("req-3".to_string(), "error".to_string())
            .await;

        assert_eq!(tracker.cache_size().await, 3);
        assert!(tracker.check_duplicate("req-1").await.is_some());
        assert!(tracker.check_duplicate("req-2").await.is_some());
        assert!(tracker.check_duplicate("req-3").await.is_some());
        assert!(tracker.check_duplicate("req-4").await.is_none());
    }
}
