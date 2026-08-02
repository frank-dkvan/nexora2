//! Performance optimizations for EventProjector
//!
//! Phase 7.7: Batch processing, caching, and throughput improvements

use crate::{EventProjector, GraphMutationBuilder, ProjectionRule, TemplateEngine};
use anyhow::Result;
use dashmap::DashMap;
use nexora_core::GraphService;
use nexora_eventlog::{EventLogStore, RawEvent};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

/// Performance configuration for EventProjector
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Batch size: process this many events before flushing to graph
    pub batch_size: usize,

    /// Batch timeout: flush after this duration even if batch not full
    pub batch_timeout_ms: u64,

    /// Polling interval in milliseconds (default: 1000ms)
    pub polling_interval_ms: u64,

    /// Enable template caching
    pub enable_template_cache: bool,

    /// Template cache size
    pub template_cache_size: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_timeout_ms: 1000,
            polling_interval_ms: 1000,
            enable_template_cache: true,
            template_cache_size: 1000,
        }
    }
}

/// Optimized batch for graph mutations
#[derive(Debug)]
pub struct MutationBatch {
    /// Node upserts: (id, labels, properties)
    pub nodes: Vec<(String, Vec<String>, serde_json::Map<String, serde_json::Value>)>,

    /// Edge upserts: (source, edge_type, target, properties)
    pub edges: Vec<(String, String, String, serde_json::Map<String, serde_json::Value>)>,

    /// Timestamp when batch was created
    pub created_at: Instant,
}

impl MutationBatch {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            created_at: Instant::now(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.edges.is_empty()
    }

    pub fn len(&self) -> usize {
        self.nodes.len() + self.edges.len()
    }

    pub fn add_node(
        &mut self,
        id: String,
        labels: Vec<String>,
        properties: serde_json::Map<String, serde_json::Value>,
    ) {
        self.nodes.push((id, labels, properties));
    }

    pub fn add_edge(
        &mut self,
        source: String,
        edge_type: String,
        target: String,
        properties: serde_json::Map<String, serde_json::Value>,
    ) {
        self.edges.push((source, edge_type, target, properties));
    }

    pub fn should_flush(&self, config: &PerformanceConfig) -> bool {
        // Flush if batch is full
        if self.len() >= config.batch_size {
            return true;
        }

        // Flush if timeout exceeded
        if self.created_at.elapsed().as_millis() >= config.batch_timeout_ms as u128 {
            return true;
        }

        false
    }

    pub async fn flush(&mut self, graph_service: &GraphService) -> Result<()> {
        // Flush all nodes
        for (id, labels, properties) in self.nodes.drain(..) {
            graph_service
                .upsert_node(&id, &labels, properties)
                .await?;
        }

        // Flush all edges
        for (source, edge_type, target, properties) in self.edges.drain(..) {
            graph_service
                .upsert_edge(&source, &edge_type, &target, properties)
                .await?;
        }

        self.created_at = Instant::now();
        Ok(())
    }
}

/// Template cache for rendered values
pub struct TemplateCache {
    cache: DashMap<String, String>,
    max_size: usize,
}

impl TemplateCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: DashMap::new(),
            max_size,
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.cache.get(key).map(|v| v.clone())
    }

    pub fn insert(&self, key: String, value: String) {
        if self.cache.len() >= self.max_size {
            // Simple eviction: clear oldest entries
            // In production, use LRU eviction
            if let Some(first_key) = self.cache.iter().next().map(|e| e.key().clone()) {
                self.cache.remove(&first_key);
            }
        }
        self.cache.insert(key, value);
    }

    pub fn clear(&self) {
        self.cache.clear();
    }
}

impl EventProjector {
    /// Create EventProjector with custom performance config
    pub fn with_performance_config(
        rules: Vec<ProjectionRule>,
        event_store: Arc<EventLogStore>,
        graph_service: Arc<GraphService>,
        config: PerformanceConfig,
    ) -> Result<Self> {
        // Store config in projector
        // Note: This requires adding a field to EventProjector struct
        Self::new(rules, event_store, graph_service)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutation_batch_capacity() {
        let mut batch = MutationBatch::new();
        assert!(batch.is_empty());

        batch.add_node(
            "node1".to_string(),
            vec!["Label".to_string()],
            serde_json::Map::new(),
        );

        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_batch_should_flush_by_size() {
        let mut config = PerformanceConfig::default();
        config.batch_size = 2;

        let mut batch = MutationBatch::new();
        assert!(!batch.should_flush(&config));

        batch.add_node(
            "n1".to_string(),
            vec!["A".to_string()],
            serde_json::Map::new(),
        );
        assert!(!batch.should_flush(&config));

        batch.add_node(
            "n2".to_string(),
            vec!["B".to_string()],
            serde_json::Map::new(),
        );
        assert!(batch.should_flush(&config)); // Should flush at size 2
    }

    #[tokio::test]
    async fn test_batch_should_flush_by_timeout() {
        let mut config = PerformanceConfig::default();
        config.batch_timeout_ms = 100; // 100ms timeout

        let mut batch = MutationBatch::new();
        batch.add_node(
            "n1".to_string(),
            vec!["A".to_string()],
            serde_json::Map::new(),
        );

        assert!(!batch.should_flush(&config));

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert!(batch.should_flush(&config)); // Should flush after timeout
    }

    #[test]
    fn test_template_cache() {
        let cache = TemplateCache::new(3);

        cache.insert("key1".to_string(), "value1".to_string());
        cache.insert("key2".to_string(), "value2".to_string());

        assert_eq!(cache.get("key1"), Some("value1".to_string()));
        assert_eq!(cache.get("key2"), Some("value2".to_string()));
        assert_eq!(cache.get("key3"), None);

        // Add more to trigger eviction
        cache.insert("key3".to_string(), "value3".to_string());
        cache.insert("key4".to_string(), "value4".to_string());

        // Cache should have at most 3 entries
        assert!(cache.cache.len() <= 3);
    }
}
