//! Circuit breaker implementation for stream sources
//!
//! Protects external service calls (Kafka, Kinesis, MQTT, etc.) from cascading failures.

use anyhow::Result;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{error, warn};

#[derive(Debug, Error)]
pub enum CircuitBreakerError {
    #[error("Circuit breaker is open for service: {0}")]
    CircuitOpen(String),

    #[error("Operation failed: {0}")]
    OperationFailed(#[from] anyhow::Error),
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for stream source operations
pub struct StreamCircuitBreaker {
    service_name: String,
    failure_count: AtomicUsize,
    success_count: AtomicUsize,
    last_failure_time: Arc<Mutex<Option<Instant>>>,
    failure_threshold: usize,
    success_threshold: usize,
    timeout: Duration,
}

impl StreamCircuitBreaker {
    /// Create a new circuit breaker for a stream source
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            failure_count: AtomicUsize::new(0),
            success_count: AtomicUsize::new(0),
            last_failure_time: Arc::new(Mutex::new(None)),
            failure_threshold: 5,
            success_threshold: 2,
            timeout: Duration::from_secs(30),
        }
    }

    /// Get the current state of the circuit breaker
    fn get_state(&self) -> State {
        let failures = self.failure_count.load(Ordering::Relaxed);

        if failures >= self.failure_threshold {
            let last_failure = self.last_failure_time.lock();
            if let Some(last) = *last_failure {
                if last.elapsed() > self.timeout {
                    return State::HalfOpen;
                }
            }
            return State::Open;
        }

        State::Closed
    }

    /// Execute an operation with circuit breaker protection
    pub async fn call<F, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: Into<anyhow::Error>,
    {
        // Check if circuit is open
        let state = self.get_state();
        if state == State::Open {
            error!(
                service = %self.service_name,
                "Circuit breaker is OPEN, rejecting call"
            );
            return Err(CircuitBreakerError::CircuitOpen(self.service_name.clone()));
        }

        // Execute the operation
        match operation.await {
            Ok(result) => {
                // Success - reset failure count and increment success count
                self.failure_count.store(0, Ordering::Relaxed);

                if state == State::HalfOpen {
                    let successes = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if successes >= self.success_threshold {
                        // Transition back to Closed
                        self.success_count.store(0, Ordering::Relaxed);
                        *self.last_failure_time.lock() = None;
                    }
                }

                Ok(result)
            }
            Err(err) => {
                // Failure - increment failure count
                let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                *self.last_failure_time.lock() = Some(Instant::now());
                self.success_count.store(0, Ordering::Relaxed);

                warn!(
                    service = %self.service_name,
                    failures = failures,
                    threshold = self.failure_threshold,
                    "Operation failed"
                );

                Err(CircuitBreakerError::OperationFailed(err.into()))
            }
        }
    }

    /// Get the current state of the circuit breaker as a string
    pub fn state(&self) -> &str {
        match self.get_state() {
            State::Closed => "closed",
            State::Open => "open",
            State::HalfOpen => "half_open",
        }
    }

    /// Check if the circuit breaker is open
    pub fn is_open(&self) -> bool {
        self.get_state() == State::Open
    }
}
