//! Sink registry — centralized management of output sinks.
//!
//! The registry holds all configured sinks and provides a unified interface
//! for routing Standing Query results to multiple outputs simultaneously.

use crate::sink_trait::OutputSink;
use nexora_standing_query::StandingQueryResult;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Central registry for all output sinks.
///
/// Responsibilities:
/// - Register sinks at startup or dynamically at runtime
/// - Route SQ results to all active sinks
/// - Track sink health and provide status reporting
/// - Handle sink failures gracefully (log + continue)
pub struct SinkRegistry {
    sinks: Arc<RwLock<HashMap<String, Arc<dyn OutputSink>>>>,
    /// Total number of results processed (all sinks combined)
    total_processed: Arc<std::sync::atomic::AtomicU64>,
    /// Total number of errors encountered (all sinks combined)
    total_errors: Arc<std::sync::atomic::AtomicU64>,
}

impl SinkRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            sinks: Arc::new(RwLock::new(HashMap::new())),
            total_processed: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_errors: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Register a sink. Replaces any existing sink with the same name.
    pub async fn register(&self, sink: Arc<dyn OutputSink>) {
        let name = sink.name().to_string();
        self.sinks.write().await.insert(name.clone(), sink);
        tracing::info!("Registered output sink: {name}");
    }

    /// Unregister a sink by name.
    pub async fn unregister(&self, name: &str) -> bool {
        let removed = self.sinks.write().await.remove(name).is_some();
        if removed {
            tracing::info!("Unregistered output sink: {name}");
        }
        removed
    }

    /// Get a sink by name (for testing or direct access).
    pub async fn get(&self, name: &str) -> Option<Arc<dyn OutputSink>> {
        self.sinks.read().await.get(name).cloned()
    }

    /// List all registered sink names.
    pub async fn list_names(&self) -> Vec<String> {
        self.sinks.read().await.keys().cloned().collect()
    }

    /// Get the number of registered sinks.
    pub async fn count(&self) -> usize {
        self.sinks.read().await.len()
    }

    /// Route a StandingQueryResult to all registered sinks in parallel.
    ///
    /// Errors are logged but do not block other sinks. Returns the number
    /// of sinks that successfully processed the result.
    pub async fn route(&self, result: &StandingQueryResult) -> usize {
        let sinks = self.sinks.read().await;
        if sinks.is_empty() {
            return 0;
        }

        let mut tasks = Vec::new();
        for (name, sink) in sinks.iter() {
            let sink = sink.clone();
            let result = result.clone();
            let name = name.clone();
            let total_errors = self.total_errors.clone();

            tasks.push(tokio::spawn(async move {
                match sink.process(&result).await {
                    Ok(()) => true,
                    Err(e) => {
                        total_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            sink = %name,
                            error = %e,
                            "Sink failed to process SQ result"
                        );
                        false
                    }
                }
            }));
        }

        let results: Vec<Result<bool, tokio::task::JoinError>> =
            futures::future::join_all(tasks).await;
        let success_count = results
            .iter()
            .filter(|r| r.as_ref().ok() == Some(&true))
            .count();

        self.total_processed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        success_count
    }

    /// Get registry statistics.
    pub fn stats(&self) -> RegistryStats {
        RegistryStats {
            total_processed: self
                .total_processed
                .load(std::sync::atomic::Ordering::Relaxed),
            total_errors: self.total_errors.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Get detailed status of all sinks.
    pub async fn sink_statuses(&self) -> HashMap<String, String> {
        let sinks = self.sinks.read().await;
        let mut statuses = HashMap::new();
        for (name, sink) in sinks.iter() {
            let status = format!("{:?}", sink.status());
            statuses.insert(name.clone(), status);
        }
        statuses
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RegistryStats {
    pub total_processed: u64,
    pub total_errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink_trait::OutputError;
    use crate::sink_trait::OutputStatus;
    use async_trait::async_trait;

    struct MockSink {
        name: String,
        should_fail: bool,
    }

    #[async_trait]
    impl OutputSink for MockSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn process(&self, _result: &StandingQueryResult) -> Result<(), OutputError> {
            if self.should_fail {
                Err(OutputError::Sink("mock failure".into()))
            } else {
                Ok(())
            }
        }

        fn status(&self) -> OutputStatus {
            OutputStatus::Active
        }
    }

    #[tokio::test]
    async fn test_registry_register_unregister() {
        let registry = SinkRegistry::new();
        assert_eq!(registry.count().await, 0);

        let sink = Arc::new(MockSink {
            name: "test-sink".to_string(),
            should_fail: false,
        });
        registry.register(sink).await;
        assert_eq!(registry.count().await, 1);

        let removed = registry.unregister("test-sink").await;
        assert!(removed);
        assert_eq!(registry.count().await, 0);
    }

    #[tokio::test]
    async fn test_registry_route_success() {
        let registry = SinkRegistry::new();
        let sink = Arc::new(MockSink {
            name: "success-sink".to_string(),
            should_fail: false,
        });
        registry.register(sink).await;

        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "test-sq",
            nexora_id::NexoraId::new_random(),
            std::collections::HashMap::new(),
            nexora_standing_query::ResultType::Matched,
            chrono::Utc::now(),
        );

        let success_count = registry.route(&result).await;
        assert_eq!(success_count, 1);

        let stats = registry.stats();
        assert_eq!(stats.total_processed, 1);
        assert_eq!(stats.total_errors, 0);
    }

    #[tokio::test]
    async fn test_registry_route_failure() {
        let registry = SinkRegistry::new();
        let sink = Arc::new(MockSink {
            name: "fail-sink".to_string(),
            should_fail: true,
        });
        registry.register(sink).await;

        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "test-sq",
            nexora_id::NexoraId::new_random(),
            std::collections::HashMap::new(),
            nexora_standing_query::ResultType::Matched,
            chrono::Utc::now(),
        );

        let success_count = registry.route(&result).await;
        assert_eq!(success_count, 0);

        let stats = registry.stats();
        assert_eq!(stats.total_processed, 1);
        assert_eq!(stats.total_errors, 1);
    }

    #[tokio::test]
    async fn test_registry_route_mixed() {
        let registry = SinkRegistry::new();
        registry
            .register(Arc::new(MockSink {
                name: "sink-ok".to_string(),
                should_fail: false,
            }))
            .await;
        registry
            .register(Arc::new(MockSink {
                name: "sink-fail".to_string(),
                should_fail: true,
            }))
            .await;

        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "test-sq",
            nexora_id::NexoraId::new_random(),
            std::collections::HashMap::new(),
            nexora_standing_query::ResultType::Matched,
            chrono::Utc::now(),
        );

        let success_count = registry.route(&result).await;
        assert_eq!(success_count, 1); // one succeeded, one failed

        let stats = registry.stats();
        assert_eq!(stats.total_processed, 1);
        assert_eq!(stats.total_errors, 1);
    }
}
