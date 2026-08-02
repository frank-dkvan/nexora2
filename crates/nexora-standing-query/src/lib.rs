//! Standing Query engine — incremental pattern matching with result propagation.
//!
//! Standing Queries live in the graph and automatically propagate incremental
//! results as data changes. They are the core differentiator of Nexora.
//!
//! Architecture:
//! ```ignore
//!   SQ Registration → Node Subscription → Property Change → Match Check → Result
//! ```ignore

pub mod pattern;
pub mod persist;
pub mod result;
pub mod sink_registry;
pub mod watermark;
pub mod webhook_sink;

use nexora_id::{NexoraId, PropertyValue};
use pattern::StandingQueryPattern;
pub use result::StandingQueryResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

pub use pattern::EdgeTraversalGraph;
pub use sink_registry::{
    FileSinkConfig, KafkaSinkConfig, RegisteredSink, SinkConfig, SinkRegistry, WebhookSinkConfig,
};
pub use webhook_sink::{WebhookSink, WebhookSinkRunner};

/// A registered Standing Query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandingQuery {
    pub id: Uuid,
    pub name: String,
    pub pattern: StandingQueryPattern,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// P1.3: Rule version (incremented on every update)
    pub version: u64,
    /// P1.3: Rule metadata (domain, tags, author, description)
    pub metadata: HashMap<String, String>,
}

/// Standing Query Manager — manages registration and result propagation.
pub struct StandingQueryManager {
    /// Registered standing queries.
    queries: Arc<RwLock<HashMap<Uuid, StandingQuery>>>,
    /// Result broadcast channel (all SQ results flow through here).
    result_tx: broadcast::Sender<StandingQueryResult>,
    /// Per-node SQ match state: (sq_id, qid) → match state
    match_states: Arc<RwLock<HashMap<(Uuid, NexoraId), MatchState>>>,
    /// Optional graph reference for edge traversal in SQ patterns.
    graph: Arc<RwLock<Option<Arc<dyn EdgeTraversalGraph>>>>,
    /// Optional control-plane store for SQ definitions + match state (A1).
    /// When set, definition changes (register/update/remove) are written
    /// through so registered SQs survive a restart. Shares the same physical
    /// store as the shard map and MV definitions. Injected after construction
    /// because the manager is built before the store is available.
    store: Arc<RwLock<Option<Arc<dyn nexora_core::control_plane_store::ControlPlaneStore>>>>,
}

/// State of a single SQ match on a single node.
#[derive(Clone, Debug)]
pub struct MatchState {
    /// Whether the node currently matches the SQ pattern.
    is_matching: bool,
    /// The matched properties (for result reporting).
    matched_properties: HashMap<String, PropertyValue>,
    /// Last check time.
    last_checked: chrono::DateTime<chrono::Utc>,
}

impl StandingQueryManager {
    pub fn new(result_buffer: usize) -> Self {
        let (result_tx, _) = broadcast::channel(result_buffer);
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
            result_tx,
            match_states: Arc::new(RwLock::new(HashMap::new())),
            graph: Arc::new(RwLock::new(None)),
            store: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the graph reference for edge traversal in SQ patterns.
    pub async fn set_graph(&self, graph: Arc<dyn EdgeTraversalGraph>) {
        *self.graph.write().await = Some(graph);
    }

    /// Inject the control-plane store (A1). Once set, definition changes
    /// (register/update/remove) are written through so registered SQs survive
    /// a restart. Called from startup after the store is built. Shares the same
    /// physical backend as the shard map and MV definitions.
    pub async fn set_store(
        &self,
        store: Arc<dyn nexora_core::control_plane_store::ControlPlaneStore>,
    ) {
        *self.store.write().await = Some(store);
    }

    /// Restore SQ definitions and match states from the control-plane store.
    /// No-op returning `Ok(0)` when no store is set or nothing was persisted.
    /// Returns the number of SQ definitions restored.
    pub async fn restore(&self) -> Result<usize, String> {
        let store = self.store.read().await.clone();
        let Some(store) = store else {
            return Ok(0);
        };
        let Some((queries, match_states)) = persist::restore_state(store.as_ref())? else {
            return Ok(0);
        };
        let count = queries.len();
        {
            let mut q = self.queries.write().await;
            for sq in queries {
                q.insert(sq.id, sq);
            }
        }
        {
            let mut ms = self.match_states.write().await;
            for (k, v) in match_states {
                ms.insert(k, v);
            }
        }
        Ok(count)
    }

    /// Persist current SQ definitions + match states. No-op when no store is
    /// set. Called after every definition change; failures are logged but do
    /// not fail the originating operation (the in-memory state is authoritative
    /// for this run; persistence is a durability best-effort on top).
    async fn persist_now(&self) {
        let store = self.store.read().await.clone();
        let Some(store) = store else {
            return;
        };
        let queries: Vec<StandingQuery> = self.queries.read().await.values().cloned().collect();
        let match_states = self.match_states.read().await.clone();
        if let Err(e) = persist::persist_state(store.as_ref(), &queries, &match_states) {
            tracing::warn!(error = %e, "failed to persist Standing Query state");
        }
    }

    /// Register a new Standing Query.
    pub async fn register(&self, name: &str, pattern: StandingQueryPattern) -> Uuid {
        let id = Uuid::new_v4();
        let sq = StandingQuery {
            id,
            name: name.to_string(),
            pattern,
            created_at: chrono::Utc::now(),
            version: 1,
            metadata: HashMap::new(),
        };

        self.queries.write().await.insert(id, sq);
        tracing::info!(sq_id = %id, name = name, "Standing Query registered");
        self.persist_now().await;
        id
    }

    /// P1.3: Register with metadata (domain, tags, etc.)
    pub async fn register_with_metadata(
        &self,
        name: &str,
        pattern: StandingQueryPattern,
        metadata: HashMap<String, String>,
    ) -> Uuid {
        let id = Uuid::new_v4();
        let sq = StandingQuery {
            id,
            name: name.to_string(),
            pattern,
            created_at: chrono::Utc::now(),
            version: 1,
            metadata,
        };

        self.queries.write().await.insert(id, sq);
        tracing::info!(sq_id = %id, name = name, "Standing Query registered with metadata");
        self.persist_now().await;
        id
    }

    /// P1.3: Update a registered query (increments version for audit).
    pub async fn update(&self, id: Uuid, name: &str, pattern: StandingQueryPattern) -> bool {
        let mut queries = self.queries.write().await;
        let Some(sq) = queries.get_mut(&id) else {
            return false;
        };
        sq.name = name.to_string();
        sq.pattern = pattern;
        sq.version += 1;
        tracing::info!(sq_id = %id, version = sq.version, "Standing Query updated");
        drop(queries);
        self.persist_now().await;
        true
    }

    /// Remove a Standing Query.
    pub async fn remove(&self, id: Uuid) -> bool {
        let removed = self.queries.write().await.remove(&id).is_some();
        if removed {
            // Clean up match states
            self.match_states
                .write()
                .await
                .retain(|(sq_id, _), _| *sq_id != id);
            tracing::info!(sq_id = %id, "Standing Query removed");
            self.persist_now().await;
        }
        removed
    }

    /// List all registered Standing Queries.
    pub async fn list(&self) -> Vec<StandingQuery> {
        self.queries.read().await.values().cloned().collect()
    }

    /// Subscribe to Standing Query results.
    pub fn subscribe(&self) -> broadcast::Receiver<StandingQueryResult> {
        self.result_tx.subscribe()
    }

    /// Publish a result directly to the broadcast channel. Test-only helper for
    /// exercising downstream subscribers (e.g. the SQ→MV bridge) without driving
    /// a full match through `on_property_change`. Returns the number of active
    /// receivers, or an error if there are none.
    #[doc(hidden)]
    pub fn publish_result_for_test(
        &self,
        result: StandingQueryResult,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        self.result_tx.send(result).map_err(|e| e.into())
    }

    /// Notify the engine that a node's properties have changed.
    /// The engine evaluates all registered SQs against the new state.
    /// Returns the number of new matches found.
    pub async fn on_property_change(
        &self,
        qid: &NexoraId,
        key: &str,
        value: &PropertyValue,
        properties: &HashMap<String, PropertyValue>,
    ) -> usize {
        let queries: Vec<_> = self.queries.read().await.values().cloned().collect();

        let mut new_matches = 0;
        for sq in &queries {
            if self.evaluate(sq, qid, key, value, properties).await {
                new_matches += 1;
            }
        }
        new_matches
    }

    /// Evaluate a single Standing Query against a node.
    /// Returns true if a new match was found (transition from not-matching to matching).
    async fn evaluate(
        &self,
        sq: &StandingQuery,
        qid: &NexoraId,
        _changed_key: &str,
        _changed_value: &PropertyValue,
        properties: &HashMap<String, PropertyValue>,
    ) -> bool {
        // Check if pattern requires edge traversal
        let needs_edges = Self::pattern_needs_edges(&sq.pattern);

        let matches = if needs_edges {
            // Async path: fetch edges and use matches_with_edges
            let graph_opt = self.graph.read().await.clone();
            if let Some(graph) = graph_opt {
                let edges = graph.get_node_edges(qid).await.unwrap_or_default();
                sq.pattern
                    .matches_with_edges(qid, properties, &edges, graph.as_ref())
                    .await
            } else {
                // No graph reference — fall back to sync (will return false for edge patterns)
                sq.pattern.matches(properties)
            }
        } else {
            // Sync path: no edge traversal needed
            sq.pattern.matches(properties)
        };

        let state_key = (sq.id, qid.clone());

        let mut states = self.match_states.write().await;
        let state = states.entry(state_key.clone()).or_insert(MatchState {
            is_matching: false,
            matched_properties: HashMap::new(),
            last_checked: chrono::Utc::now(),
        });

        if matches && !state.is_matching {
            // New match — emit result
            state.is_matching = true;
            state.matched_properties = properties.clone();
            state.last_checked = chrono::Utc::now();

            let result = StandingQueryResult {
                sq_id: sq.id,
                sq_name: sq.name.clone(),
                qid: qid.clone(),
                matched_properties: properties.clone(),
                result_type: ResultType::Matched,
                timestamp: chrono::Utc::now(),
                sq_version: sq.version,
                hit_id: Uuid::new_v4().to_string(),
                trigger_source: "property_change".to_string(),
                matching_edge_types: Self::extract_edge_types(&sq.pattern),
                suppressed: false,
            };

            if let Err(e) = self.result_tx.send(result) {
                tracing::warn!(
                    sq_id = %sq.id,
                    node = %qid,
                    dropped_sq_id = %e.0.sq_id,
                    "SQ result dropped - no active subscribers or lagging receivers"
                );
            } else {
                tracing::debug!(sq_id = %sq.id, node = %qid, "SQ matched");
            }
            true
        } else if !matches && state.is_matching {
            // Match lost — emit unmatch
            state.is_matching = false;
            state.matched_properties.clear();
            state.last_checked = chrono::Utc::now();

            let result = StandingQueryResult {
                sq_id: sq.id,
                sq_name: sq.name.clone(),
                qid: qid.clone(),
                matched_properties: HashMap::new(),
                result_type: ResultType::Unmatched,
                timestamp: chrono::Utc::now(),
                sq_version: sq.version,
                hit_id: Uuid::new_v4().to_string(),
                trigger_source: "property_change".to_string(),
                matching_edge_types: Self::extract_edge_types(&sq.pattern),
                suppressed: false,
            };

            if let Err(e) = self.result_tx.send(result) {
                tracing::warn!(
                    sq_id = %sq.id,
                    node = %qid,
                    dropped_sq_id = %e.0.sq_id,
                    "SQ unmatch result dropped - no active subscribers or lagging receivers"
                );
            } else {
                tracing::debug!(sq_id = %sq.id, node = %qid, "SQ unmatched");
            }
            false
        } else {
            false
        }
    }

    /// Get match count for a specific SQ.
    pub async fn match_count(&self, sq_id: Uuid) -> usize {
        self.match_states
            .read()
            .await
            .iter()
            .filter(|((id, _), state)| *id == sq_id && state.is_matching)
            .count()
    }

    /// Clean up match states for a deleted node.
    /// Should be called when a node is removed from the graph.
    pub async fn cleanup_node(&self, qid: &NexoraId) {
        let mut states = self.match_states.write().await;
        let before = states.len();
        states.retain(|(_, node_id), _| node_id != qid);
        let removed = before - states.len();
        if removed > 0 {
            tracing::debug!(node = %qid, cleaned = removed, "Cleaned up SQ match states for deleted node");
        }
    }

    /// Periodic cleanup of stale match states.
    /// Removes entries for nodes that are no longer matching and haven't been checked recently.
    pub async fn cleanup_stale(&self, max_age_secs: i64) {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_secs);
        let mut states = self.match_states.write().await;
        let before = states.len();
        states.retain(|_, state| state.is_matching || state.last_checked > cutoff);
        let removed = before - states.len();
        if removed > 0 {
            tracing::debug!(cleaned = removed, "Cleaned up stale SQ match states");
        }
    }

    /// P0.5: Re-evaluate standing queries when a label is added to a node.
    /// Returns the number of new matches found.
    pub async fn on_label_added(&self, qid: &NexoraId, label: &str) -> usize {
        let queries: Vec<_> = self.queries.read().await.values().cloned().collect();
        let mut new_matches = 0;
        for sq in &queries {
            if Self::pattern_matches_label(&sq.pattern, label) {
                // Label-triggered patterns should re-evaluate: fetch properties and call evaluate_with_state
                if let Some(graph) = self.graph.read().await.as_ref() {
                    if let Ok(props_map) = graph.get_node_properties(qid).await.map(|m| {
                        m.into_iter()
                            .map(|(k, v)| (k.to_string(), v))
                            .collect::<HashMap<_, _>>()
                    }) {
                        if self.evaluate_with_state(sq, qid, &props_map).await {
                            new_matches += 1;
                        }
                    }
                }
            }
        }
        new_matches
    }

    /// P0.5: Re-evaluate standing queries when an edge is added.
    pub async fn on_edge_added(
        &self,
        qid: &NexoraId,
        edge_type: &str,
        _target: &NexoraId,
    ) -> usize {
        let queries: Vec<_> = self.queries.read().await.values().cloned().collect();
        let mut new_matches = 0;
        for sq in &queries {
            if Self::pattern_matches_edge(&sq.pattern, edge_type) {
                if let Some(graph) = self.graph.read().await.as_ref() {
                    if let Ok(props_map) = graph.get_node_properties(qid).await.map(|m| {
                        m.into_iter()
                            .map(|(k, v)| (k.to_string(), v))
                            .collect::<HashMap<_, _>>()
                    }) {
                        if self.evaluate_with_state(sq, qid, &props_map).await {
                            new_matches += 1;
                        }
                    }
                }
            }
        }
        new_matches
    }

    /// P0.5: Re-evaluate standing queries when a label is removed from a node.
    /// Checking all SQs for this node — removed label may cause unmatch.
    pub async fn on_label_removed(&self, qid: &NexoraId, _label: &str) -> usize {
        // When a label is removed, re-evaluate ALL SQs for this node.
        // Any SQ that matched previously based on this label will flip to
        // unmatched via evaluate_with_state.
        let queries: Vec<_> = self.queries.read().await.values().cloned().collect();
        let mut changes = 0;
        if let Some(graph) = self.graph.read().await.as_ref() {
            if let Ok(props_map) = graph.get_node_properties(qid).await.map(|m| {
                m.into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect::<HashMap<_, _>>()
            }) {
                for sq in &queries {
                    if self.evaluate_with_state(sq, qid, &props_map).await {
                        changes += 1;
                    }
                }
            }
        }
        changes
    }

    /// P0.5: Re-evaluate when an edge is removed — may cause SQ unmatch.
    pub async fn on_edge_removed(
        &self,
        qid: &NexoraId,
        _edge_type: &str,
        _target: &NexoraId,
    ) -> usize {
        let queries: Vec<_> = self.queries.read().await.values().cloned().collect();
        let mut changes = 0;
        if let Some(graph) = self.graph.read().await.as_ref() {
            if let Ok(props_map) = graph.get_node_properties(qid).await.map(|m| {
                m.into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect::<HashMap<_, _>>()
            }) {
                for sq in &queries {
                    if Self::pattern_needs_edges(&sq.pattern) {
                        // Edge-removed events are relevant to SQs with edge patterns
                        if self.evaluate_with_state(sq, qid, &props_map).await {
                            changes += 1;
                        }
                    }
                }
            }
        }
        changes
    }

    /// P0.5: Re-evaluate without specific changed_key/value — used for label/edge triggers.
    async fn evaluate_with_state(
        &self,
        sq: &StandingQuery,
        qid: &NexoraId,
        properties: &HashMap<String, PropertyValue>,
    ) -> bool {
        let needs_edges = Self::pattern_needs_edges(&sq.pattern);

        let matches = if needs_edges {
            let graph_opt = self.graph.read().await.clone();
            if let Some(graph) = graph_opt {
                let edges = graph.get_node_edges(qid).await.unwrap_or_default();
                sq.pattern
                    .matches_with_edges(qid, properties, &edges, graph.as_ref())
                    .await
            } else {
                sq.pattern.matches(properties)
            }
        } else {
            sq.pattern.matches(properties)
        };

        let state_key = (sq.id, qid.clone());
        let mut states = self.match_states.write().await;
        let state = states.entry(state_key.clone()).or_insert(MatchState {
            is_matching: false,
            matched_properties: HashMap::new(),
            last_checked: chrono::Utc::now(),
        });

        if matches && !state.is_matching {
            state.is_matching = true;
            state.matched_properties = properties.clone();
            state.last_checked = chrono::Utc::now();

            let result = StandingQueryResult {
                sq_id: sq.id,
                sq_name: sq.name.clone(),
                qid: qid.clone(),
                matched_properties: properties.clone(),
                result_type: ResultType::Matched,
                timestamp: chrono::Utc::now(),
                sq_version: sq.version,
                hit_id: Uuid::new_v4().to_string(),
                trigger_source: "edge_added".to_string(),
                matching_edge_types: Self::extract_edge_types(&sq.pattern),
                suppressed: false,
            };
            if let Err(e) = self.result_tx.send(result) {
                tracing::warn!(
                    sq_id = %sq.id,
                    node = %qid,
                    dropped_sq_id = %e.0.sq_id,
                    "SQ result dropped (edge_added) - no active subscribers or lagging receivers"
                );
            } else {
                tracing::debug!(sq_id = %sq.id, node = %qid, "SQ matched (via P0.5 trigger)");
            }
            true
        } else if !matches && state.is_matching {
            state.is_matching = false;
            state.matched_properties.clear();
            state.last_checked = chrono::Utc::now();

            let result = StandingQueryResult {
                sq_id: sq.id,
                sq_name: sq.name.clone(),
                qid: qid.clone(),
                matched_properties: HashMap::new(),
                result_type: ResultType::Unmatched,
                timestamp: chrono::Utc::now(),
                sq_version: sq.version,
                hit_id: Uuid::new_v4().to_string(),
                trigger_source: "edge_added".to_string(),
                matching_edge_types: Self::extract_edge_types(&sq.pattern),
                suppressed: false,
            };
            if let Err(e) = self.result_tx.send(result) {
                tracing::warn!(
                    sq_id = %sq.id,
                    node = %qid,
                    dropped_sq_id = %e.0.sq_id,
                    "SQ unmatch result dropped (edge_added) - no active subscribers or lagging receivers"
                );
            } else {
                tracing::debug!(sq_id = %sq.id, node = %qid, "SQ unmatched (via P0.5 trigger)");
            }
            false
        } else {
            false
        }
    }

    /// P0.5: Check if an SQ pattern references a specific label.
    fn pattern_matches_label(pattern: &StandingQueryPattern, label: &str) -> bool {
        match pattern {
            StandingQueryPattern::LabelFilter(labels) => labels.iter().any(|l| l == label),
            StandingQueryPattern::And(ps) | StandingQueryPattern::Or(ps) => {
                ps.iter().any(|p| Self::pattern_matches_label(p, label))
            }
            StandingQueryPattern::Not(p) => Self::pattern_matches_label(p, label),
            _ => false,
        }
    }

    /// P0.5: Check if an SQ pattern references a specific edge type.
    fn pattern_matches_edge(pattern: &StandingQueryPattern, edge_type: &str) -> bool {
        match pattern {
            StandingQueryPattern::EdgePattern(e) => e.edge_type.as_str() == edge_type,
            StandingQueryPattern::And(ps) | StandingQueryPattern::Or(ps) => {
                ps.iter().any(|p| Self::pattern_matches_edge(p, edge_type))
            }
            StandingQueryPattern::Not(p) => Self::pattern_matches_edge(p, edge_type),
            _ => false,
        }
    }

    /// P1.3: Extract all edge type names from a pattern (for explain reporting).
    fn extract_edge_types(pattern: &StandingQueryPattern) -> Vec<String> {
        match pattern {
            StandingQueryPattern::EdgePattern(e) => {
                let mut types = vec![e.edge_type.clone()];
                if let Some(next) = &e.next {
                    types.extend(Self::extract_edge_types(
                        &StandingQueryPattern::EdgePattern(*next.clone()),
                    ));
                }
                types
            }
            StandingQueryPattern::And(ps) | StandingQueryPattern::Or(ps) => {
                ps.iter().flat_map(Self::extract_edge_types).collect()
            }
            StandingQueryPattern::Not(p) => Self::extract_edge_types(p),
            _ => vec![],
        }
    }

    /// P1.3: Get version history for a query.
    pub async fn get_version(&self, sq_id: Uuid) -> Option<u64> {
        self.queries.read().await.get(&sq_id).map(|sq| sq.version)
    }

    /// Check if a pattern requires edge traversal (contains EdgePattern).
    fn pattern_needs_edges(pattern: &StandingQueryPattern) -> bool {
        match pattern {
            StandingQueryPattern::EdgePattern(_) => true,
            StandingQueryPattern::And(patterns) | StandingQueryPattern::Or(patterns) => {
                patterns.iter().any(Self::pattern_needs_edges)
            }
            StandingQueryPattern::Not(p) => Self::pattern_needs_edges(p),
            _ => false,
        }
    }
}

/// Type of Standing Query result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ResultType {
    /// Node now matches the pattern.
    Matched,
    /// Node no longer matches.
    Unmatched,
}

// Re-export

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{FilterCondition, StandingQueryPattern};

    #[tokio::test]
    async fn test_sq_trigger_fires() {
        let sqm = StandingQueryManager::new(100);
        let qid = NexoraId::from_bytes(b"test".to_vec());

        // Register SQ: speed > 100
        sqm.register(
            "high-speed",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

        // Build properties map: speed = 150
        let mut props = HashMap::new();
        props.insert("speed".to_string(), PropertyValue::Float(150.0));

        // Trigger
        sqm.on_property_change(&qid, "speed", &PropertyValue::Float(150.0), &props)
            .await;

        // Should match
        assert_eq!(
            sqm.match_count(sqm.list().await[0].id).await,
            1,
            "SQ should match speed=150 > 100"
        );
    }

    #[tokio::test]
    async fn test_sq_no_match_below_threshold() {
        let sqm = StandingQueryManager::new(100);
        let qid = NexoraId::from_bytes(b"test".to_vec());

        sqm.register(
            "high-speed",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

        let mut props = HashMap::new();
        props.insert("speed".to_string(), PropertyValue::Float(50.0));
        sqm.on_property_change(&qid, "speed", &PropertyValue::Float(50.0), &props)
            .await;

        assert_eq!(
            sqm.match_count(sqm.list().await[0].id).await,
            0,
            "SQ should NOT match speed=50 < 100"
        );
    }

    #[tokio::test]
    async fn test_sq_integer_value() {
        let sqm = StandingQueryManager::new(100);
        let qid = NexoraId::from_bytes(b"test".to_vec());

        sqm.register(
            "high-speed",
            StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
        )
        .await;

        // Integer value (as sent by JSON API)
        let mut props = HashMap::new();
        props.insert("speed".to_string(), PropertyValue::Integer(150));
        sqm.on_property_change(&qid, "speed", &PropertyValue::Integer(150), &props)
            .await;

        assert_eq!(
            sqm.match_count(sqm.list().await[0].id).await,
            1,
            "SQ should match Integer(150) > 100.0"
        );
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::pattern::{FilterCondition, StandingQueryPattern};
    use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_e2e_property_change_triggers_sq() {
        // Setup: GraphService + SQ Manager
        let config = GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 100,
            node_channel_size: 16,
        };
        let persistor = Arc::new(InMemoryPersistor::new());
        let graph = Arc::new(GraphService::new(config, persistor));
        let sqm = StandingQueryManager::new(100);

        let qid = NexoraId::from_bytes(b"forklift-042".to_vec());

        // Register SQ: speed > 100
        let sq_id = sqm
            .register(
                "high-speed",
                StandingQueryPattern::property("speed", FilterCondition::GreaterThan(100.0)),
            )
            .await;

        // Set property via GraphService
        graph
            .set_property(&qid, "speed", PropertyValue::Float(150.0))
            .await
            .unwrap();

        // Get all properties (same as handler does)
        let all_props = graph.get_all_properties(&qid).await.unwrap();
        let props_map: HashMap<String, PropertyValue> = all_props
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        // Verify props_map has "speed"
        assert!(
            props_map.contains_key("speed"),
            "props_map should contain 'speed'"
        );
        assert_eq!(props_map.get("speed"), Some(&PropertyValue::Float(150.0)));

        // Trigger SQ
        let value_ref = props_map.get("speed").cloned().unwrap();
        sqm.on_property_change(&qid, "speed", &value_ref, &props_map)
            .await;

        // Verify match
        assert_eq!(
            sqm.match_count(sq_id).await,
            1,
            "SQ should match after property change"
        );
    }
}
