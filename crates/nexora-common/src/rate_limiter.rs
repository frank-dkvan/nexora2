//! P1-4: Rate limiting with token bucket algorithm
//!
//! Provides both global and per-client rate limiting for API endpoints.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Configuration for rate limiting
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Global rate limit (requests per second)
    pub global_limit: u64,
    /// Per-client rate limit (requests per second)
    pub per_client_limit: u64,
    /// Bucket refill interval
    pub refill_interval: Duration,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            global_limit: 100_000,                       // 100K req/s global
            per_client_limit: 1_000,                     // 1K req/s per client
            refill_interval: Duration::from_millis(100), // Refill every 100ms
        }
    }
}

/// Token bucket for rate limiting
struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        let new_tokens = elapsed * self.refill_rate;

        self.tokens = (self.tokens + new_tokens).min(self.capacity);
        self.last_refill = now;
    }

    fn try_consume(&mut self, count: f64) -> bool {
        self.refill();

        if self.tokens >= count {
            self.tokens -= count;
            true
        } else {
            false
        }
    }
}

/// Rate limiter with global and per-client limits
pub struct RateLimiter {
    config: RateLimiterConfig,
    global_bucket: Arc<Mutex<TokenBucket>>,
    client_buckets: Arc<Mutex<HashMap<IpAddr, TokenBucket>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimiterConfig) -> Self {
        let global_bucket =
            TokenBucket::new(config.global_limit as f64, config.global_limit as f64);

        Self {
            config,
            global_bucket: Arc::new(Mutex::new(global_bucket)),
            client_buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check if a request from the given client IP should be allowed
    pub async fn check_rate_limit(&self, client_ip: IpAddr) -> Result<(), RateLimitError> {
        // Check global limit first
        let mut global = self.global_bucket.lock().await;
        if !global.try_consume(1.0) {
            return Err(RateLimitError::GlobalLimitExceeded);
        }
        drop(global);

        // Check per-client limit
        let mut clients = self.client_buckets.lock().await;
        let bucket = clients.entry(client_ip).or_insert_with(|| {
            TokenBucket::new(
                self.config.per_client_limit as f64,
                self.config.per_client_limit as f64,
            )
        });

        if !bucket.try_consume(1.0) {
            return Err(RateLimitError::ClientLimitExceeded(client_ip));
        }

        Ok(())
    }

    /// Cleanup stale client buckets (call periodically)
    pub async fn cleanup_stale_clients(&self) {
        let mut clients = self.client_buckets.lock().await;
        let now = Instant::now();

        clients.retain(|_, bucket| {
            // Remove clients that haven't been accessed in 5 minutes
            now.duration_since(bucket.last_refill) < Duration::from_secs(300)
        });
    }
}

/// Rate limit error types
#[derive(Debug, Clone)]
pub enum RateLimitError {
    GlobalLimitExceeded,
    ClientLimitExceeded(IpAddr),
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RateLimitError::GlobalLimitExceeded => {
                write!(f, "Global rate limit exceeded")
            }
            RateLimitError::ClientLimitExceeded(ip) => {
                write!(f, "Rate limit exceeded for client {}", ip)
            }
        }
    }
}

impl std::error::Error for RateLimitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10.0, 10.0);

        // Should succeed
        assert!(bucket.try_consume(5.0));
        assert!((bucket.tokens - 5.0).abs() < 0.1);

        // Should succeed
        assert!(bucket.try_consume(5.0));
        assert!(bucket.tokens < 0.1);

        // Should fail
        assert!(!bucket.try_consume(1.0));
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10.0, 10.0);

        // Consume all tokens
        assert!(bucket.try_consume(10.0));
        assert_eq!(bucket.tokens, 0.0);

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(500)).await;
        bucket.refill();

        // Should have ~5 tokens (10 tokens/sec * 0.5 sec)
        assert!(bucket.tokens >= 4.5 && bucket.tokens <= 5.5);
        assert!(bucket.try_consume(5.0));
    }

    #[tokio::test]
    async fn test_rate_limiter_per_client() {
        let config = RateLimiterConfig {
            global_limit: 100_000,
            per_client_limit: 5,
            refill_interval: Duration::from_millis(100),
        };

        let limiter = RateLimiter::new(config);
        let client_ip: IpAddr = "127.0.0.1".parse().unwrap();

        // First 5 requests should succeed
        for _ in 0..5 {
            assert!(limiter.check_rate_limit(client_ip).await.is_ok());
        }

        // 6th request should fail
        let result = limiter.check_rate_limit(client_ip).await;
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(RateLimitError::ClientLimitExceeded(_))
        ));
    }

    #[tokio::test]
    async fn test_rate_limiter_global() {
        let config = RateLimiterConfig {
            global_limit: 3,
            per_client_limit: 1000,
            refill_interval: Duration::from_millis(100),
        };

        let limiter = RateLimiter::new(config);
        let client1: IpAddr = "127.0.0.1".parse().unwrap();
        let client2: IpAddr = "127.0.0.2".parse().unwrap();

        // First 3 requests should succeed
        assert!(limiter.check_rate_limit(client1).await.is_ok());
        assert!(limiter.check_rate_limit(client2).await.is_ok());
        assert!(limiter.check_rate_limit(client1).await.is_ok());

        // 4th request should fail (global limit)
        let result = limiter.check_rate_limit(client2).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(RateLimitError::GlobalLimitExceeded)));
    }

    #[tokio::test]
    async fn test_cleanup_stale_clients() {
        let config = RateLimiterConfig::default();
        let limiter = RateLimiter::new(config);

        let client1: IpAddr = "127.0.0.1".parse().unwrap();
        let client2: IpAddr = "127.0.0.2".parse().unwrap();

        // Make requests from both clients
        limiter.check_rate_limit(client1).await.ok();
        limiter.check_rate_limit(client2).await.ok();

        // Should have 2 clients
        {
            let clients = limiter.client_buckets.lock().await;
            assert_eq!(clients.len(), 2);
        }

        // Cleanup shouldn't remove active clients
        limiter.cleanup_stale_clients().await;
        {
            let clients = limiter.client_buckets.lock().await;
            assert_eq!(clients.len(), 2);
        }
    }
}
