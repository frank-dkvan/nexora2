//! Graph mutation builder: converts events to graph operations
//!
//! This module handles the actual graph updates: creating nodes, edges, and setting properties.

use crate::{GraphStreamingError, Result};
use nexora_core::GraphService;
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::{HalfEdge, Symbol};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Monotonic request-id source for commit-path mutations (`add_label`,
/// `set_edge_property`, `delete_node`). Mirrors the counter convention used by
/// the Cypher write executor.
static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_request_id() -> u64 {
    REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Derive a `NexoraId` from an arbitrary external id string. Hex ids (the HTTP
/// wire format) decode directly; anything else (e.g. `"CARGO-123"`) is used
/// verbatim as the id's byte content. Same rule the Cypher write path uses.
fn id_from_str(s: &str) -> NexoraId {
    NexoraId::from_hex(s).unwrap_or_else(|_| NexoraId::from_bytes(s.as_bytes().to_vec()))
}

/// Builder for graph mutations based on projected events
pub struct GraphMutationBuilder {
    graph_service: Arc<GraphService>,
}

impl GraphMutationBuilder {
    /// Create a new graph mutation builder
    pub fn new(graph_service: Arc<GraphService>) -> Self {
        Self { graph_service }
    }

    /// Upsert a node: create if not exists, update properties if exists.
    ///
    /// Node creation is implicit — `set_property` creates the node if absent —
    /// and `add_label` is idempotent (the label index dedups), so this always
    /// applies labels + properties without a prior existence check.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The node ID (from rendered template)
    /// * `labels` - Labels to apply to the node
    /// * `properties` - Properties to set (rendered from templates)
    pub async fn upsert_node(
        &self,
        node_id_str: &str,
        labels: &[String],
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let node_id = id_from_str(node_id_str);
        let prop_count = properties.len();

        // Set properties first — this implicitly creates the node if absent.
        for (key, value) in properties {
            let prop_value = Self::parse_property_value(&value);
            self.graph_service
                .set_property(&node_id, &key, prop_value)
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
        }

        // Apply labels (idempotent via the label index).
        for label in labels {
            let label_sym = Symbol::new(label);
            self.graph_service
                .add_label(&node_id, label_sym, next_request_id())
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
        }

        tracing::debug!(
            "Upserted node: {} with {} properties",
            node_id_str,
            prop_count
        );

        Ok(())
    }

    /// Upsert an edge: create if not exists, update properties if exists.
    ///
    /// `add_edge` is idempotent (the edge index dedups on duplicate add), so the
    /// edge is always (re)added and its properties set.
    ///
    /// # Arguments
    ///
    /// * `src_id_str` - Source node ID
    /// * `edge_type` - Edge type
    /// * `target_id_str` - Target node ID
    /// * `properties` - Edge properties to set
    pub async fn upsert_edge(
        &self,
        src_id_str: &str,
        edge_type: &str,
        target_id_str: &str,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let src_id = id_from_str(src_id_str);
        let target_id = id_from_str(target_id_str);
        let edge_type_sym = Symbol::new(edge_type);
        let prop_count = properties.len();

        // Add the outgoing edge (idempotent via the edge index).
        self.graph_service
            .add_edge(
                &src_id,
                HalfEdge::out(edge_type_sym.clone(), target_id.clone()),
            )
            .await
            .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;

        // Set edge properties via the commit path.
        for (key, value) in properties {
            let key_sym = Symbol::new(&key);
            let prop_value = Self::parse_property_value(&value);
            self.graph_service
                .set_edge_property(
                    &src_id,
                    edge_type_sym.clone(),
                    &target_id,
                    key_sym,
                    prop_value,
                    next_request_id(),
                )
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
        }

        tracing::debug!(
            "Upserted edge: {} -[{}]-> {} with {} properties",
            src_id_str,
            edge_type,
            target_id_str,
            prop_count
        );

        Ok(())
    }

    /// Delete a node (soft delete - adds tombstone)
    pub async fn delete_node(&self, node_id_str: &str) -> Result<()> {
        let node_id = id_from_str(node_id_str);

        self.graph_service
            .delete_node(&node_id, next_request_id(), Some("graphstreaming"), None)
            .await
            .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;

        tracing::debug!("Deleted node: {}", node_id_str);
        Ok(())
    }

    /// Delete an edge
    pub async fn delete_edge(
        &self,
        src_id_str: &str,
        edge_type: &str,
        target_id_str: &str,
    ) -> Result<()> {
        let src_id = id_from_str(src_id_str);
        let target_id = id_from_str(target_id_str);
        let edge_type_sym = Symbol::new(edge_type);

        self.graph_service
            .remove_edge(&src_id, HalfEdge::out(edge_type_sym, target_id))
            .await
            .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;

        tracing::debug!(
            "Deleted edge: {} -[{}]-> {}",
            src_id_str,
            edge_type,
            target_id_str
        );
        Ok(())
    }

    /// Parse a string value into a PropertyValue
    ///
    /// Attempts to parse as number or boolean, falls back to string.
    fn parse_property_value(value: &str) -> PropertyValue {
        // Try parsing as integer
        if let Ok(i) = value.parse::<i64>() {
            return PropertyValue::Integer(i);
        }

        // Try parsing as float
        if let Ok(f) = value.parse::<f64>() {
            return PropertyValue::Float(f);
        }

        // Try parsing as boolean
        if let Ok(b) = value.parse::<bool>() {
            return PropertyValue::Boolean(b);
        }

        // Default to string
        PropertyValue::String(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_property_value_integer() {
        let value = GraphMutationBuilder::parse_property_value("42");
        assert_eq!(value, PropertyValue::Integer(42));
    }

    #[test]
    fn test_parse_property_value_float() {
        let value = GraphMutationBuilder::parse_property_value("3.14");
        assert_eq!(value, PropertyValue::Float(3.14));
    }

    #[test]
    fn test_parse_property_value_boolean() {
        let value_true = GraphMutationBuilder::parse_property_value("true");
        assert_eq!(value_true, PropertyValue::Boolean(true));

        let value_false = GraphMutationBuilder::parse_property_value("false");
        assert_eq!(value_false, PropertyValue::Boolean(false));
    }

    #[test]
    fn test_parse_property_value_string() {
        let value = GraphMutationBuilder::parse_property_value("hello");
        assert_eq!(value, PropertyValue::String("hello".to_string()));
    }

    #[test]
    fn test_parse_property_value_string_looks_like_number() {
        // Numbers with units should be strings
        let value = GraphMutationBuilder::parse_property_value("42px");
        assert_eq!(value, PropertyValue::String("42px".to_string()));
    }
}
