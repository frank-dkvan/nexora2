//! Standing Query pattern definitions and matching logic.

use nexora_id::{NexoraId, PropertyValue};
use nexora_value::HalfEdge;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A Standing Query pattern that matches against node properties and edges.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StandingQueryPattern {
    /// Match nodes where a property satisfies a condition.
    PropertyFilter(PropertyFilter),
    /// Match nodes that have all specified labels.
    LabelFilter(Vec<String>),
    /// Match nodes with an outgoing edge of a specific type.
    EdgePattern(EdgePattern),
    /// Combine multiple patterns with AND logic.
    And(Vec<StandingQueryPattern>),
    /// Combine multiple patterns with OR logic.
    Or(Vec<StandingQueryPattern>),
    /// Negate a pattern.
    Not(Box<StandingQueryPattern>),
}

/// An edge pattern for traversal-based standing queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgePattern {
    /// Edge type/label (e.g., "KNOWS", "FOLLOWS").
    pub edge_type: String,
    /// Direction: "out", "in", or "both".
    pub direction: EdgeDirection,
    /// Optional pattern to match on the target node.
    pub target_pattern: Option<Box<StandingQueryPattern>>,
    /// P1.1: Optional chain to another edge pattern for multi-hop traversal.
    pub next: Option<Box<EdgePattern>>,
    /// P1.1: For variable-length paths, min depth (1-based). None = exact single-hop.
    pub min_hops: Option<usize>,
    /// P1.1: For variable-length paths, max depth (inclusive). None = use next instead.
    pub max_hops: Option<usize>,
}

/// Edge direction for traversal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EdgeDirection {
    Out,
    In,
    Both,
}

/// A filter condition on a single property.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyFilter {
    pub key: String,
    pub condition: FilterCondition,
}

/// Filter conditions for property values.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FilterCondition {
    /// Property exists (any value).
    Exists,
    /// Property equals a specific value.
    Equals(PropertyValue),
    /// Property does not equal a value.
    NotEquals(PropertyValue),
    /// Numeric greater than.
    GreaterThan(f64),
    /// Numeric less than.
    LessThan(f64),
    /// String starts with.
    StartsWith(String),
    /// String contains.
    Contains(String),
    /// String ends with.
    EndsWith(String),
    /// Property is null.
    IsNull,
    /// Property is not null.
    IsNotNull,
}

impl StandingQueryPattern {
    /// Check if a set of properties matches this pattern.
    /// For edge patterns, you must use `matches_with_context` instead.
    pub fn matches(&self, properties: &HashMap<String, PropertyValue>) -> bool {
        match self {
            Self::PropertyFilter(filter) => filter.matches(properties),
            Self::LabelFilter(labels) => {
                // Check if the node has all of the specified labels.
                if let Some(PropertyValue::List(node_labels)) = properties.get("labels") {
                    labels.iter().all(|label| {
                        node_labels
                            .iter()
                            .any(|v| v.as_str() == Some(label.as_str()))
                    })
                } else {
                    false
                }
            }
            Self::EdgePattern(_) => {
                // Edge patterns require GraphService context for traversal
                false
            }
            Self::And(patterns) => patterns.iter().all(|p| p.matches(properties)),
            Self::Or(patterns) => patterns.iter().any(|p| p.matches(properties)),
            Self::Not(pattern) => !pattern.matches(properties),
        }
    }

    /// Check if a node matches this pattern with edge traversal support.
    /// Requires access to the graph to follow edges.
    pub async fn matches_with_edges(
        &self,
        qid: &NexoraId,
        properties: &HashMap<String, PropertyValue>,
        edges: &[HalfEdge],
        graph: &dyn EdgeTraversalGraph,
    ) -> bool {
        self.matches_impl_boxed(qid, properties, edges, graph).await
    }

    fn matches_impl_boxed<'a>(
        &'a self,
        qid: &'a NexoraId,
        properties: &'a HashMap<String, PropertyValue>,
        edges: &'a [HalfEdge],
        graph: &'a dyn EdgeTraversalGraph,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            match self {
                Self::PropertyFilter(filter) => filter.matches(properties),
                Self::LabelFilter(labels) => {
                    if let Some(PropertyValue::List(node_labels)) = properties.get("labels") {
                        labels.iter().all(|label| {
                            node_labels
                                .iter()
                                .any(|v| v.as_str() == Some(label.as_str()))
                        })
                    } else {
                        false
                    }
                }
                Self::EdgePattern(edge_pattern) => edge_pattern.matches(qid, edges, graph).await,
                Self::And(patterns) => {
                    for p in patterns {
                        if !p.matches_impl_boxed(qid, properties, edges, graph).await {
                            return false;
                        }
                    }
                    true
                }
                Self::Or(patterns) => {
                    for p in patterns {
                        if p.matches_impl_boxed(qid, properties, edges, graph).await {
                            return true;
                        }
                    }
                    false
                }
                Self::Not(pattern) => {
                    !pattern
                        .matches_impl_boxed(qid, properties, edges, graph)
                        .await
                }
            }
        })
    }

    /// Create a property filter pattern.
    pub fn property(key: &str, condition: FilterCondition) -> Self {
        Self::PropertyFilter(PropertyFilter {
            key: key.to_string(),
            condition,
        })
    }

    /// Create a label filter pattern.
    pub fn label(label: &str) -> Self {
        Self::LabelFilter(vec![label.to_string()])
    }

    /// Create an edge pattern (existence check only).
    pub fn edge(edge_type: &str, direction: EdgeDirection) -> Self {
        Self::EdgePattern(EdgePattern {
            edge_type: edge_type.to_string(),
            direction,
            target_pattern: None,
            next: None,
            min_hops: None,
            max_hops: None,
        })
    }

    /// Create an edge pattern with a target node condition.
    pub fn edge_with_target(
        edge_type: &str,
        direction: EdgeDirection,
        target_pattern: StandingQueryPattern,
    ) -> Self {
        Self::EdgePattern(EdgePattern {
            edge_type: edge_type.to_string(),
            direction,
            target_pattern: Some(Box::new(target_pattern)),
            next: None,
            min_hops: None,
            max_hops: None,
        })
    }

    /// Combine patterns with AND.
    pub fn and(patterns: Vec<StandingQueryPattern>) -> Self {
        Self::And(patterns)
    }

    /// Combine patterns with OR.
    pub fn or(patterns: Vec<StandingQueryPattern>) -> Self {
        Self::Or(patterns)
    }
}

impl PropertyFilter {
    fn matches(&self, properties: &HashMap<String, PropertyValue>) -> bool {
        let value = properties.get(&self.key);

        match &self.condition {
            FilterCondition::Exists => value.is_some(),
            FilterCondition::Equals(expected) => value == Some(expected),
            FilterCondition::NotEquals(expected) => value != Some(expected),
            FilterCondition::GreaterThan(threshold) => value
                .and_then(|v| v.as_f64())
                .is_some_and(|n| n > *threshold),
            FilterCondition::LessThan(threshold) => value
                .and_then(|v| v.as_f64())
                .is_some_and(|n| n < *threshold),
            FilterCondition::StartsWith(prefix) => value
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.starts_with(prefix.as_str())),
            FilterCondition::Contains(substring) => value
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.contains(substring.as_str())),
            FilterCondition::EndsWith(suffix) => value
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.ends_with(suffix.as_str())),
            FilterCondition::IsNull => value.is_none_or(|v| v.is_null()),
            FilterCondition::IsNotNull => value.as_ref().is_some_and(|v| !v.is_null()),
        }
    }
}

/// Trait for graph services that support edge traversal in standing queries.
#[async_trait::async_trait]
pub trait EdgeTraversalGraph: Send + Sync {
    /// Get properties of a node.
    async fn get_node_properties(
        &self,
        qid: &NexoraId,
    ) -> Result<HashMap<String, PropertyValue>, String>;

    /// Get edges of a node.
    async fn get_node_edges(&self, qid: &NexoraId) -> Result<Vec<HalfEdge>, String>;
}

/// Implement EdgeTraversalGraph for GraphService so SQ patterns can traverse edges.
#[async_trait::async_trait]
impl EdgeTraversalGraph for nexora_core::GraphService {
    async fn get_node_properties(
        &self,
        qid: &NexoraId,
    ) -> Result<HashMap<String, PropertyValue>, String> {
        let props = self
            .get_all_properties(qid)
            .await
            .map_err(|e| e.to_string())?;
        Ok(props.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    async fn get_node_edges(&self, qid: &NexoraId) -> Result<Vec<HalfEdge>, String> {
        self.get_edges(qid).await.map_err(|e| e.to_string())
    }
}

impl EdgePattern {
    /// Check if this edge pattern matches against a node's edges.
    /// P1.1: Supports multi-hop chain matching via `next` field and
    /// variable-length path matching via `min_hops`/`max_hops`.
    pub async fn matches(
        &self,
        _qid: &NexoraId,
        edges: &[HalfEdge],
        graph: &dyn EdgeTraversalGraph,
    ) -> bool {
        if let (Some(min), Some(max)) = (self.min_hops, self.max_hops) {
            self.matches_variable_length(_qid, edges, graph, min, max)
                .await
        } else {
            self.matches_single_hop(_qid, edges, graph).await
        }
    }

    /// P1.1: Single-hop matching logic (exact one edge).
    async fn matches_single_hop(
        &self,
        _qid: &NexoraId,
        edges: &[HalfEdge],
        graph: &dyn EdgeTraversalGraph,
    ) -> bool {
        for edge in edges {
            if !edge_matches(edge, &self.edge_type, &self.direction) {
                continue;
            }

            let target_qid = edge.other.clone();
            if let Some(target_pattern) = &self.target_pattern {
                let target_props = graph.get_node_properties(&target_qid).await;
                if let Ok(target_props) = target_props {
                    if !target_pattern.matches(&target_props) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            // P1.1: If there's a `next` hop, traverse from target
            if let Some(next_pattern) = &self.next {
                if let Ok(target_edges) = graph.get_node_edges(&target_qid).await {
                    let next_match =
                        Box::pin(next_pattern.matches(&target_qid, &target_edges, graph));
                    if next_match.await {
                        return true;
                    }
                }
            } else {
                return true;
            }
        }
        false
    }

    /// P1.1: Variable-length path matching (min..max hops).
    async fn matches_variable_length(
        &self,
        start_qid: &NexoraId,
        _start_edges: &[HalfEdge],
        graph: &dyn EdgeTraversalGraph,
        min: usize,
        max: usize,
    ) -> bool {
        // BFS: (qid, depth)
        let mut visited: std::collections::HashSet<NexoraId> = std::collections::HashSet::new();
        let mut queue: Vec<(NexoraId, usize)> = vec![(start_qid.clone(), 0)];
        visited.insert(start_qid.clone());

        while let Some((current_qid, depth)) = queue.pop() {
            // Check target pattern at current depth (if >= min)
            if depth >= min {
                if let Some(target_pattern) = &self.target_pattern {
                    if let Ok(target_props) = graph.get_node_properties(&current_qid).await {
                        if target_pattern.matches(&target_props) {
                            return true;
                        }
                    }
                } else if depth >= min {
                    return true;
                }
            }

            if depth >= max {
                continue;
            }

            // Expand outgoing edges
            let Ok(edges) = graph.get_node_edges(&current_qid).await else {
                continue;
            };
            for edge in &edges {
                if !edge_matches(edge, &self.edge_type, &self.direction) {
                    continue;
                }
                let next_qid = edge.other.clone();
                if visited.insert(next_qid.clone()) {
                    queue.push((next_qid, depth + 1));
                }
            }
        }
        false
    }

    /// P1.1: Create a simple multi-hop chain: -[e1]->-[e2]->...->-[eN]->
    pub fn chain(edge_types: Vec<String>, target: Option<Box<StandingQueryPattern>>) -> Self {
        let mut head: Option<EdgePattern> = None;
        for et in edge_types.into_iter().rev() {
            let next = head.map(Box::new);
            head = Some(EdgePattern {
                edge_type: et,
                direction: EdgeDirection::Out,
                target_pattern: None,
                next,
                min_hops: None,
                max_hops: None,
            });
        }
        // Attach target pattern to the last hop
        if let Some(ref mut h) = head {
            if let Some(ref mut last) = {
                let l = h.last_mut();
                Some(l)
            } {
                last.target_pattern = target;
            } else {
                h.target_pattern = target;
            }
        }
        head.unwrap_or(EdgePattern {
            edge_type: String::new(),
            direction: EdgeDirection::Out,
            target_pattern: None,
            next: None,
            min_hops: None,
            max_hops: None,
        })
    }

    /// Walk the `next` chain to find the last edge pattern.
    fn last_mut(&mut self) -> &mut Self {
        let mut current = self;
        while current.next.is_some() {
            // SAFETY: we just checked the Option
            let next = current.next.as_mut().unwrap();
            current = next;
        }
        current
    }

    /// P1.1: Create a variable-length edge pattern: -[e*min..max]->
    pub fn variable(
        edge_type: String,
        min: usize,
        max: usize,
        target: Option<Box<StandingQueryPattern>>,
    ) -> Self {
        EdgePattern {
            edge_type,
            direction: EdgeDirection::Out,
            target_pattern: target,
            next: None,
            min_hops: Some(min),
            max_hops: Some(max),
        }
    }
}

/// Helper: check if a HalfEdge matches an edge type and direction.
fn edge_matches(edge: &HalfEdge, edge_type: &str, direction: &EdgeDirection) -> bool {
    if edge.edge_type.as_str() != edge_type {
        return false;
    }
    match direction {
        EdgeDirection::Out => edge.direction.is_out(),
        EdgeDirection::In => edge.direction.is_in(),
        EdgeDirection::Both => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, PropertyValue)]) -> HashMap<String, PropertyValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn test_property_filter_exists() {
        let p = StandingQueryPattern::property("name", FilterCondition::Exists);
        assert!(p.matches(&props(&[("name", PropertyValue::String("Alice".into()))])));
        assert!(!p.matches(&props(&[])));
    }

    #[test]
    fn test_property_filter_gt() {
        let p = StandingQueryPattern::property("age", FilterCondition::GreaterThan(30.0));
        assert!(p.matches(&props(&[("age", PropertyValue::Integer(35))])));
        assert!(!p.matches(&props(&[("age", PropertyValue::Integer(25))])));
    }

    #[test]
    fn test_property_filter_contains() {
        let p = StandingQueryPattern::property("name", FilterCondition::Contains("lic".into()));
        assert!(p.matches(&props(&[("name", PropertyValue::String("Alice".into()))])));
        assert!(!p.matches(&props(&[("name", PropertyValue::String("Bob".into()))])));
    }

    #[test]
    fn test_and_pattern() {
        let p = StandingQueryPattern::and(vec![
            StandingQueryPattern::property("age", FilterCondition::GreaterThan(20.0)),
            StandingQueryPattern::property("name", FilterCondition::IsNotNull),
        ]);
        assert!(p.matches(&props(&[
            ("age", PropertyValue::Integer(30)),
            ("name", PropertyValue::String("Alice".into())),
        ])));
        assert!(!p.matches(&props(&[("age", PropertyValue::Integer(15))])));
    }

    #[test]
    fn test_label_filter() {
        let p = StandingQueryPattern::label("Person");
        assert!(p.matches(&props(&[(
            "labels",
            PropertyValue::List(vec![PropertyValue::String("Person".into()),])
        ),])));
    }
}
