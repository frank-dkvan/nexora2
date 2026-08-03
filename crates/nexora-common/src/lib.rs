//! Common utilities shared across Nexora crates.
//!
//! This crate provides foundational utilities for error handling, retry logic,
//! and other cross-cutting concerns used throughout the Nexora platform.

pub mod retry;
pub mod rate_limiter;

pub use retry::{retry_with_backoff, retry_with_backoff_config, RetryConfig, is_retryable_error};
pub use rate_limiter::{RateLimiter, RateLimiterConfig, RateLimitError};
