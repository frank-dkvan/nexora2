//! Common utilities shared across Nexora crates.
//!
//! This crate provides foundational utilities for error handling, retry logic,
//! and other cross-cutting concerns used throughout the Nexora platform.

pub mod rate_limiter;
pub mod retry;

pub use rate_limiter::{RateLimitError, RateLimiter, RateLimiterConfig};
pub use retry::{is_retryable_error, retry_with_backoff, retry_with_backoff_config, RetryConfig};
