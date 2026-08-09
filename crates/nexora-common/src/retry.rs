//! Retry framework with exponential backoff for network operations.
//!
//! Provides a unified retry mechanism for all external service calls (S3, Kafka,
//! Kinesis, HTTP APIs) to handle transient failures gracefully. Works in conjunction
//! with circuit breakers (P1-2) for comprehensive failure handling.
//!
//! # Design Principles
//!
//! - **Exponential backoff**: 100ms → 200ms → 400ms → 800ms → 1600ms
//! - **Jitter**: ±25% randomization to prevent thundering herd
//! - **Max retries**: 3 by default (configurable per operation)
//! - **Timeout**: Per-attempt timeout (not total timeout)
//! - **Idempotency**: Only retry idempotent operations by default
//!
//! # Usage Examples
//!
//! ```rust
//! use nexora_common::retry::{retry_with_backoff, RetryConfig};
//!
//! // Simple retry with defaults (3 attempts, exponential backoff)
//! let result = retry_with_backoff(|| async {
//!     external_service.call().await
//! }).await?;
//!
//! // Custom retry config
//! let config = RetryConfig {
//!     max_attempts: 5,
//!     base_delay_ms: 200,
//!     max_delay_ms: 5000,
//!     timeout_per_attempt: Some(Duration::from_secs(10)),
//! };
//! let result = retry_with_backoff_config(config, || async {
//!     external_service.call().await
//! }).await?;
//! ```

use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Retry configuration for exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of attempts (including the first try).
    pub max_attempts: u32,
    /// Base delay in milliseconds (first retry waits this long).
    pub base_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential growth).
    pub max_delay_ms: u64,
    /// Optional timeout per individual attempt (not total timeout).
    pub timeout_per_attempt: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 100,
            max_delay_ms: 5000,
            timeout_per_attempt: None,
        }
    }
}

impl RetryConfig {
    /// Conservative config for critical operations (more retries, longer waits).
    pub fn conservative() -> Self {
        Self {
            max_attempts: 5,
            base_delay_ms: 200,
            max_delay_ms: 10_000,
            timeout_per_attempt: Some(Duration::from_secs(30)),
        }
    }

    /// Aggressive config for fast-fail scenarios (fewer retries, shorter waits).
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 2,
            base_delay_ms: 50,
            max_delay_ms: 500,
            timeout_per_attempt: Some(Duration::from_secs(5)),
        }
    }

    /// Calculate the delay for a given attempt (0-indexed).
    ///
    /// Uses exponential backoff: `base * 2^attempt`, capped at `max_delay_ms`,
    /// with ±25% jitter to prevent thundering herd.
    fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base = self.base_delay_ms;
        let exponential = base.saturating_mul(2u64.saturating_pow(attempt));
        let capped = exponential.min(self.max_delay_ms);

        // Add ±25% jitter
        let jitter_range = capped / 4; // 25%
        let jitter = rand::random::<u64>() % (jitter_range * 2 + 1);
        let with_jitter = capped.saturating_sub(jitter_range).saturating_add(jitter);

        Duration::from_millis(with_jitter)
    }
}

/// Retry an async operation with exponential backoff (default config).
///
/// **Important**: Only use this for **idempotent** operations. Non-idempotent
/// operations (e.g., financial transactions) should not be retried automatically.
///
/// # Example
///
/// ```rust
/// let data = retry_with_backoff(|| async {
///     s3_client.get_object("bucket", "key").await
/// }).await?;
/// ```
pub async fn retry_with_backoff<F, Fut, T, E>(operation: F) -> Result<T, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    retry_with_backoff_config(RetryConfig::default(), operation).await
}

/// Retry an async operation with custom config.
///
/// # Example
///
/// ```rust
/// let config = RetryConfig::conservative();
/// let data = retry_with_backoff_config(config, || async {
///     kinesis_client.get_records(iterator).await
/// }).await?;
/// ```
pub async fn retry_with_backoff_config<F, Fut, T, E>(
    config: RetryConfig,
    operation: F,
) -> Result<T, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;

    loop {
        attempt += 1;

        // Execute the operation (with optional timeout)
        let result = if let Some(timeout) = config.timeout_per_attempt {
            match tokio::time::timeout(timeout, operation()).await {
                Ok(r) => r.map_err(|e| e.to_string()),
                Err(_) => {
                    warn!(
                        attempt = attempt,
                        timeout_ms = timeout.as_millis(),
                        "Operation timed out"
                    );
                    // Continue to retry logic below
                    Err(format!("timeout after {}ms", timeout.as_millis()))
                }
            }
        } else {
            operation().await.map_err(|e| e.to_string())
        };

        match result {
            Ok(value) => {
                if attempt > 1 {
                    debug!(attempt = attempt, "Operation succeeded after retry");
                }
                return Ok(value);
            }
            Err(err) => {
                if attempt >= config.max_attempts {
                    warn!(
                        attempt = attempt,
                        error = %err,
                        "Operation failed after all retry attempts"
                    );
                    return Err(err);
                }

                let delay = config.delay_for_attempt(attempt - 1);
                warn!(
                    attempt = attempt,
                    max_attempts = config.max_attempts,
                    error = %err,
                    retry_after_ms = delay.as_millis(),
                    "Operation failed, retrying"
                );

                sleep(delay).await;
            }
        }
    }
}

/// Check if an error is retryable (transient network/service errors).
///
/// Use this to conditionally retry only specific error types:
///
/// ```rust
/// if is_retryable_error(&err) {
///     retry_with_backoff(operation).await?
/// } else {
///     return Err(err); // Fail fast on permanent errors
/// }
/// ```
pub fn is_retryable_error(err: &str) -> bool {
    let err_lower = err.to_lowercase();

    // Network errors (transient)
    err_lower.contains("timeout")
        || err_lower.contains("connection refused")
        || err_lower.contains("connection reset")
        || err_lower.contains("broken pipe")
        || err_lower.contains("network unreachable")
        || err_lower.contains("host unreachable")

        // HTTP 5xx (server errors, transient)
        || err_lower.contains("500")
        || err_lower.contains("502")
        || err_lower.contains("503")
        || err_lower.contains("504")

        // AWS throttling (transient)
        || err_lower.contains("throttl")
        || err_lower.contains("too many requests")
        || err_lower.contains("rate limit")
        || err_lower.contains("slow down")

        // Kafka/Kinesis transient errors
        || err_lower.contains("not available")
        || err_lower.contains("leader not available")
        || err_lower.contains("not coordinator")

        // S3 transient errors
        || err_lower.contains("slow down")
        || err_lower.contains("internal error")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_succeeds_on_first_attempt() {
        let result = retry_with_backoff(|| async { Ok::<i32, String>(42) }).await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(move || {
            let c = counter_clone.clone();
            async move {
                let attempt = c.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err("transient error".to_string())
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn test_retry_exhausts_attempts() {
        let result = retry_with_backoff_config(
            RetryConfig {
                max_attempts: 2,
                base_delay_ms: 10,
                ..Default::default()
            },
            || async { Err::<i32, String>("permanent error".to_string()) },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "permanent error");
    }

    #[tokio::test]
    async fn test_exponential_backoff() {
        let config = RetryConfig {
            max_attempts: 4,
            base_delay_ms: 100,
            max_delay_ms: 1000,
            timeout_per_attempt: None,
        };

        // Delays should be: ~100ms, ~200ms, ~400ms (with jitter)
        let d0 = config.delay_for_attempt(0);
        let d1 = config.delay_for_attempt(1);
        let d2 = config.delay_for_attempt(2);

        // Check approximate doubling (within jitter range)
        assert!(d0.as_millis() >= 75 && d0.as_millis() <= 125); // ~100ms ±25%
        assert!(d1.as_millis() >= 150 && d1.as_millis() <= 250); // ~200ms ±25%
        assert!(d2.as_millis() >= 300 && d2.as_millis() <= 500); // ~400ms ±25%
    }

    #[tokio::test]
    async fn test_max_delay_cap() {
        let config = RetryConfig {
            max_attempts: 10,
            base_delay_ms: 100,
            max_delay_ms: 500,
            timeout_per_attempt: None,
        };

        // Even at attempt 10 (which would be 100 * 2^10 = 102400ms),
        // delay should be capped at max_delay_ms
        let delay = config.delay_for_attempt(10);
        assert!(delay.as_millis() <= 625); // 500ms + 25% jitter
    }

    #[test]
    fn test_is_retryable_error() {
        // Retryable errors
        assert!(is_retryable_error("connection timeout"));
        assert!(is_retryable_error("500 Internal Server Error"));
        assert!(is_retryable_error("503 Service Unavailable"));
        assert!(is_retryable_error("Throttling Exception"));
        assert!(is_retryable_error("Too Many Requests"));
        assert!(is_retryable_error("Connection refused"));
        assert!(is_retryable_error("Broken pipe"));

        // Non-retryable errors
        assert!(!is_retryable_error("404 Not Found"));
        assert!(!is_retryable_error("401 Unauthorized"));
        assert!(!is_retryable_error("Invalid input"));
        assert!(!is_retryable_error("Parse error"));
    }

    #[tokio::test]
    async fn test_timeout_per_attempt() {
        let config = RetryConfig {
            max_attempts: 2,
            base_delay_ms: 10,
            max_delay_ms: 100,
            timeout_per_attempt: Some(Duration::from_millis(50)),
        };

        let result = retry_with_backoff_config(config, || async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<i32, String>(42)
        })
        .await;

        // Should timeout and retry, then timeout again and fail
        assert!(result.is_err());
    }
}
