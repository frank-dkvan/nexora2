//! Circuit Breaker for EventLogStore operations
//!
//! Protects against cascading failures when S3/Iceberg operations fail repeatedly.
//! Uses the failsafe crate to implement the circuit breaker pattern.

use anyhow::{Context, Result};
use failsafe::{
    backoff::{self, Exponential},
    failure_policy::{self, ConsecutiveFailures},
    futures::CircuitBreaker,
    Config, Error as FailsafeError, Instrument, StateMachine,
};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::time::Duration;
use tracing::{error, info, warn};

/// Circuit breaker configuration for event store operations
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    pub failure_threshold: usize,
    /// Number of consecutive successes (in half-open state) before closing
    pub success_threshold: usize,
    /// How long to wait before attempting to close an open circuit
    pub timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            timeout: Duration::from_secs(30),
        }
    }
}

/// State observer to track circuit breaker state
#[derive(Clone)]
struct StateObserver {
    current_state: Arc<AtomicU8>,
}

impl StateObserver {
    fn new() -> Self {
        Self {
            current_state: Arc::new(AtomicU8::new(0)), // 0 = Closed
        }
    }

    fn get_state(&self) -> CircuitState {
        match self.current_state.load(Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

impl Instrument for StateObserver {
    fn on_call_rejected(&self) {
        // Called when a request is rejected due to open circuit
    }

    fn on_open(&self) {
        self.current_state.store(1, Ordering::SeqCst);
    }

    fn on_half_open(&self) {
        self.current_state.store(2, Ordering::SeqCst);
    }

    fn on_closed(&self) {
        self.current_state.store(0, Ordering::SeqCst);
    }
}

/// Circuit breaker wrapper for protecting against cascading failures
pub struct EventStoreCircuitBreaker {
    circuit: StateMachine<ConsecutiveFailures<Exponential>, StateObserver>,
    observer: StateObserver,
    service_name: String,
}

impl EventStoreCircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(service_name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        let observer = StateObserver::new();

        let circuit = Config::new()
            .failure_policy(failure_policy::consecutive_failures(
                config.failure_threshold as u32,
                backoff::exponential(Duration::from_millis(100), Duration::from_secs(5)),
            ))
            .instrument(observer.clone())
            .build();

        let service_name = service_name.into();

        info!(
            service = %service_name,
            failure_threshold = config.failure_threshold,
            success_threshold = config.success_threshold,
            timeout_secs = config.timeout.as_secs(),
            "Circuit breaker initialized"
        );

        Self {
            circuit,
            observer,
            service_name,
        }
    }

    /// Execute an operation with circuit breaker protection
    ///
    /// If the circuit is open (too many failures), returns an error immediately
    /// without calling the operation. Otherwise, executes the operation and
    /// records the result (success/failure) in the circuit breaker state.
    pub async fn call<F, T, E>(&self, operation: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        match self.circuit.call(operation).await {
            Ok(result) => Ok(result),
            Err(FailsafeError::Rejected) => {
                error!(
                    service = %self.service_name,
                    "Circuit breaker is OPEN - rejecting request"
                );
                anyhow::bail!(
                    "Service '{}' is currently unavailable (circuit breaker open)",
                    self.service_name
                )
            }
            Err(FailsafeError::Inner(e)) => {
                warn!(
                    service = %self.service_name,
                    error = %e,
                    "Operation failed"
                );
                anyhow::bail!("Operation failed: {}", e)
            }
        }
    }

    /// Get the current state of the circuit breaker
    pub fn state(&self) -> CircuitState {
        self.observer.get_state()
    }

    /// Get a human-readable state description
    pub fn state_description(&self) -> &'static str {
        match self.state() {
            CircuitState::Closed => "closed (healthy)",
            CircuitState::Open => "open (failing)",
            CircuitState::HalfOpen => "half-open (testing)",
        }
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - requests pass through normally
    Closed,
    /// Circuit is open - requests are rejected immediately
    Open,
    /// Circuit is half-open - allowing test requests to check if service recovered
    HalfOpen,
}

impl CircuitState {
    /// Convert to numeric representation for metrics (0=closed, 1=open, 2=half-open)
    pub fn as_metric(&self) -> u8 {
        match self {
            CircuitState::Closed => 0,
            CircuitState::Open => 1,
            CircuitState::HalfOpen => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
        };
        let cb = EventStoreCircuitBreaker::new("test", config);

        // Initial state should be closed
        assert_eq!(cb.state(), CircuitState::Closed);

        // Trigger 3 consecutive failures
        for _ in 0..3 {
            let result = cb
                .call(async { Err::<(), _>("simulated failure") })
                .await;
            assert!(result.is_err());
        }

        // Circuit should now be open
        assert_eq!(cb.state(), CircuitState::Open);

        // Next call should be rejected immediately
        let result = cb.call(async { Ok::<(), String>(()) }).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("circuit breaker open"));
    }

    #[tokio::test]
    async fn test_circuit_breaker_stays_closed_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
        };
        let cb = EventStoreCircuitBreaker::new("test", config);

        // Execute 5 successful operations
        for _ in 0..5 {
            let result = cb.call(async { Ok::<(), String>(()) }).await;
            assert!(result.is_ok());
        }

        // Circuit should remain closed
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovers_after_timeout() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 1,
            timeout: Duration::from_millis(50),
        };
        let cb = EventStoreCircuitBreaker::new("test", config);

        // Trigger 2 failures to open circuit
        for _ in 0..2 {
            let _ = cb.call(async { Err::<(), _>("failure") }).await;
        }

        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Circuit should transition to half-open and allow test request
        let result = cb.call(async { Ok::<(), String>(()) }).await;
        assert!(result.is_ok());

        // After successful test, circuit should be closed
        assert_eq!(cb.state(), CircuitState::Closed);
    }
}
