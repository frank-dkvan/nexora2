//! Event projector: core streaming engine
//!
//! Subscribes to event streams from nexora-eventlog and applies projection rules
//! to automatically update the graph.

use crate::{GraphMutationBuilder, GraphStreamingError, ProjectionRule, Result, TemplateEngine};
use dashmap::DashMap;
use nexora_core::{GraphService, RawEvent};
use nexora_eventlog::EventLogStore;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Metrics for a single projection
#[derive(Debug, Clone, Default)]
pub struct ProjectionMetrics {
    pub events_processed: u64,
    pub nodes_created: u64,
    pub edges_created: u64,
    pub errors: u64,
}

/// Core event projection engine
pub struct EventProjector {
    /// Projection rules to apply
    rules: Vec<ProjectionRule>,

    /// Event store to stream from
    event_store: Arc<EventLogStore>,

    /// Graph service to update
    graph_service: Arc<GraphService>,

    /// Template engine for rendering
    template_engine: TemplateEngine,

    /// Graph mutation builder
    mutation_builder: GraphMutationBuilder,

    /// Active projection tasks
    tasks: DashMap<String, JoinHandle<()>>,

    /// Metrics per projection rule
    metrics: Arc<DashMap<String, ProjectionMetrics>>,
}

impl EventProjector {
    /// Create a new event projector
    pub fn new(
        rules: Vec<ProjectionRule>,
        event_store: Arc<EventLogStore>,
        graph_service: Arc<GraphService>,
    ) -> Self {
        let mutation_builder = GraphMutationBuilder::new(graph_service.clone());
        let template_engine = TemplateEngine::new();

        Self {
            rules,
            event_store,
            graph_service,
            template_engine,
            mutation_builder,
            tasks: DashMap::new(),
            metrics: Arc::new(DashMap::new()),
        }
    }

    /// Start all projection tasks
    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting EventProjector with {} rules", self.rules.len());

        for rule in &self.rules {
            self.start_projection(rule.clone()).await?;
        }

        Ok(())
    }

    /// Start a single projection task
    async fn start_projection(&self, rule: ProjectionRule) -> Result<()> {
        let rule_name = rule.name.clone();
        let topic = rule.source_topic.clone();

        // Initialize metrics
        self.metrics
            .insert(rule_name.clone(), ProjectionMetrics::default());

        // Spawn projection task
        let event_store = self.event_store.clone();
        let template_engine = self.template_engine.clone();
        let mutation_builder = GraphMutationBuilder::new(self.graph_service.clone());
        let metrics = self.metrics.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::projection_loop(
                rule,
                event_store,
                template_engine,
                mutation_builder,
                metrics,
            )
            .await
            {
                tracing::error!("Projection task failed: {}", e);
            }
        });

        self.tasks.insert(rule_name.clone(), handle);

        tracing::info!(
            "Started projection '{}' for topic '{}'",
            rule_name,
            topic
        );

        Ok(())
    }

    /// Main projection loop for a single rule
    async fn projection_loop(
        rule: ProjectionRule,
        event_store: Arc<EventLogStore>,
        template_engine: TemplateEngine,
        mutation_builder: GraphMutationBuilder,
        metrics: Arc<DashMap<String, ProjectionMetrics>>,
    ) -> Result<()> {
        let rule_name = rule.name.clone();
        let topic = rule.source_topic.clone();

        tracing::info!("Projection loop started for rule: {}", rule_name);

        // Stream events from EventLogStore
        let mut rx = event_store
            .stream_topic(&topic)
            .await
            .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;

        while let Some(event) = rx.recv().await {
            match Self::process_event(&event, &rule, &template_engine, &mutation_builder).await {
                Ok(stats) => {
                    Self::update_metrics(&rule_name, stats, &metrics);
                }
                Err(e) => {
                    tracing::error!("Failed to process event for rule '{}': {}", rule_name, e);
                    Self::increment_error(&rule_name, &metrics);
                }
            }
        }

        tracing::info!("Projection loop ended for rule: {}", rule_name);

        Ok(())
    }

    /// Process a single event through a projection rule
    async fn process_event(
        event: &RawEvent,
        rule: &ProjectionRule,
        template_engine: &TemplateEngine,
        mutation_builder: &GraphMutationBuilder,
    ) -> Result<ProcessStats> {
        let mut stats = ProcessStats::default();

        // 1. Parse event payload
        let context = TemplateEngine::extract_context(&event.data)?;

        // 2. Check event filter
        if !rule.matches_filter(&context) {
            return Ok(stats); // Skip this event
        }

        // 3. Render node ID
        let node_id = template_engine.render(&rule.node.id, &context)?;

        // 4. Render node properties
        let node_properties = template_engine.render_map(&rule.node.properties, &context)?;

        // 5. Upsert node
        mutation_builder
            .upsert_node(&node_id, &rule.node.labels, node_properties)
            .await?;

        stats.nodes_updated += 1;

        // 6. If edge projection exists, upsert edge
        if let Some(edge_proj) = &rule.edge {
            let target_id = template_engine.render(&edge_proj.target_id, &context)?;
            let edge_properties = template_engine.render_map(&edge_proj.properties, &context)?;

            mutation_builder
                .upsert_edge(&node_id, &edge_proj.edge_type, &target_id, edge_properties)
                .await?;

            stats.edges_updated += 1;
        }

        stats.events_processed = 1;
        Ok(stats)
    }

    /// Update metrics for a projection
    fn update_metrics(rule_name: &str, stats: ProcessStats, metrics: &DashMap<String, ProjectionMetrics>) {
        metrics.entry(rule_name.to_string()).and_modify(|m| {
            m.events_processed += stats.events_processed;
            m.nodes_created += stats.nodes_updated;
            m.edges_created += stats.edges_updated;
        });
    }

    /// Increment error count
    fn increment_error(rule_name: &str, metrics: &DashMap<String, ProjectionMetrics>) {
        metrics.entry(rule_name.to_string()).and_modify(|m| {
            m.errors += 1;
        });
    }

    /// Stop all projection tasks
    pub async fn stop(&self) {
        tracing::info!("Stopping EventProjector");

        for entry in self.tasks.iter() {
            let rule_name = entry.key();
            let handle = entry.value();

            handle.abort();
            tracing::info!("Stopped projection: {}", rule_name);
        }

        self.tasks.clear();
    }

    /// Get metrics for all projections
    pub fn get_metrics(&self) -> Vec<(String, ProjectionMetrics)> {
        self.metrics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Get metrics for a specific projection
    pub fn get_projection_metrics(&self, rule_name: &str) -> Option<ProjectionMetrics> {
        self.metrics.get(rule_name).map(|m| m.clone())
    }

    /// Get list of active projection names
    pub fn get_active_projections(&self) -> Vec<String> {
        self.tasks.iter().map(|entry| entry.key().clone()).collect()
    }
}

/// Statistics from processing a single event
#[derive(Debug, Default)]
struct ProcessStats {
    events_processed: u64,
    nodes_updated: u64,
    edges_updated: u64,
}

impl Drop for EventProjector {
    fn drop(&mut self) {
        // Abort all tasks on drop
        for entry in self.tasks.iter() {
            entry.value().abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_core::{GraphServiceConfig, InMemoryPersistor};
    use nexora_eventlog::StorageConfig;
    use tempfile::TempDir;

    async fn setup_test_services() -> (Arc<EventLogStore>, Arc<GraphService>) {
        let temp_dir = TempDir::new().unwrap();

        let event_store = Arc::new(
            EventLogStore::new_with_config(
                StorageConfig::local_fs(temp_dir.path().to_str().unwrap())
            )
            .await
            .unwrap(),
        );

        let config = GraphServiceConfig::default();
        let persistor = Arc::new(InMemoryPersistor::new());
        let graph_service = Arc::new(
            GraphService::new(config, persistor)
                .await
                .unwrap(),
        );

        (event_store, graph_service)
    }

    #[tokio::test]
    async fn test_projector_creation() {
        let (event_store, graph_service) = setup_test_services().await;

        let rules = vec![ProjectionRule {
            name: "test_rule".to_string(),
            source_topic: "test.topic".to_string(),
            event_filter: None,
            node: crate::NodeProjection {
                id: "{{id}}".to_string(),
                labels: vec!["TestNode".to_string()],
                properties: Default::default(),
            },
            edge: None,
        }];

        let projector = EventProjector::new(rules, event_store, graph_service);

        assert_eq!(projector.rules.len(), 1);
        assert_eq!(projector.get_active_projections().len(), 0); // Not started yet
    }

    #[tokio::test]
    async fn test_process_event() {
        let (event_store, graph_service) = setup_test_services().await;

        let rule = ProjectionRule {
            name: "cargo_rule".to_string(),
            source_topic: "nexora.cargo".to_string(),
            event_filter: None,
            node: crate::NodeProjection {
                id: "{{cargo_id}}".to_string(),
                labels: vec!["Cargo".to_string()],
                properties: {
                    let mut props = std::collections::HashMap::new();
                    props.insert("status".to_string(), "{{status}}".to_string());
                    props
                },
            },
            edge: None,
        };

        let event = RawEvent {
            event_time: 1000000,
            ingest_time: 1000000,
            source: "test".to_string(),
            topic: "nexora.cargo".to_string(),
            partition: 0,
            offset: 0,
            key: None,
            data: r#"{"cargo_id":"CARGO-123","status":"IN_TRANSIT"}"#.to_string(),
        };

        let template_engine = TemplateEngine::new();
        let mutation_builder = GraphMutationBuilder::new(graph_service.clone());

        let stats = EventProjector::process_event(
            &event,
            &rule,
            &template_engine,
            &mutation_builder,
        )
        .await
        .unwrap();

        assert_eq!(stats.events_processed, 1);
        assert_eq!(stats.nodes_updated, 1);
    }

    #[tokio::test]
    async fn test_metrics() {
        let (event_store, graph_service) = setup_test_services().await;

        let rules = vec![ProjectionRule {
            name: "test_rule".to_string(),
            source_topic: "test.topic".to_string(),
            event_filter: None,
            node: crate::NodeProjection {
                id: "{{id}}".to_string(),
                labels: vec!["TestNode".to_string()],
                properties: Default::default(),
            },
            edge: None,
        }];

        let projector = EventProjector::new(rules, event_store, graph_service);

        // Manually insert metrics for testing
        projector.metrics.insert(
            "test_rule".to_string(),
            ProjectionMetrics {
                events_processed: 10,
                nodes_created: 8,
                edges_created: 5,
                errors: 2,
            },
        );

        let metrics = projector.get_projection_metrics("test_rule").unwrap();
        assert_eq!(metrics.events_processed, 10);
        assert_eq!(metrics.nodes_created, 8);
        assert_eq!(metrics.edges_created, 5);
        assert_eq!(metrics.errors, 2);
    }
}
