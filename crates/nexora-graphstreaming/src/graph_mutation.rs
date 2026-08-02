//! Graph mutation builder: converts events to graph operations
//!
//! This module handles the actual graph updates: creating nodes, edges, and setting properties.

use crate::{GraphStreamingError, Result};
use nexora_core::{GraphService, NodeCommand};
use nexora_id::{NexoraId, PropertyValue};
use nexora_value::Symbol;
use std::collections::HashMap;
use std::sync::Arc;

/// Builder for graph mutations based on projected events
pub struct GraphMutationBuilder {
    graph_service: Arc<GraphService>,
}

impl GraphMutationBuilder {
    /// Create a new graph mutation builder
    pub fn new(graph_service: Arc<GraphService>) -> Self {
        Self { graph_service }
    }

    /// Upsert a node: create if not exists, update properties if exists
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
        // Parse node ID
        let node_id = NexoraId::from_string(node_id_str);

        // Check if node exists
        let exists = self.graph_service.get_node(&node_id).await.is_ok();

        if !exists {
            // Create new node
            tracing::debug!("Creating new node: {}", node_id_str);

            // Send CreateNode command
            self.graph_service
                .send_command(
                    &node_id,
                    NodeCommand::CreateNode {
                        id: node_id.clone(),
                    },
                )
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;

            // Add labels
            for label in labels {
                let label_sym = Symbol::new(label);
                self.graph_service
                    .send_command(&node_id, NodeCommand::AddLabel { label: label_sym })
                    .await
                    .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
            }
        }

        // Set properties (create or update)
        for (key, value) in properties {
            let key_sym = Symbol::new(&key);
            let prop_value = Self::parse_property_value(&value);

            self.graph_service
                .send_command(
                    &node_id,
                    NodeCommand::SetProperty {
                        key: key_sym,
                        value: prop_value,
                    },
                )
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
        }

        tracing::debug!(
            "Upserted node: {} with {} properties",
            node_id_str,
            properties.len()
        );

        Ok(())
    }

    /// Upsert an edge: create if not exists, update properties if exists
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
        let src_id = NexoraId::from_string(src_id_str);
        let target_id = NexoraId::from_string(target_id_str);
        let edge_type_sym = Symbol::new(edge_type);

        // Check if edge exists
        let edge_exists = self
            .graph_service
            .get_node(&src_id)
            .await
            .ok()
            .and_then(|node| {
                node.outgoing_edges()
                    .iter()
                    .find(|e| e.edge_type() == edge_type_sym && e.target() == &target_id)
            })
            .is_some();

        if !edge_exists {
            // Create edge
            tracing::debug!(
                "Creating edge: {} -[{}]-> {}",
                src_id_str,
                edge_type,
                target_id_str
            );

            self.graph_service
                .send_command(
                    &src_id,
                    NodeCommand::AddEdge {
                        edge_type: edge_type_sym.clone(),
                        target: target_id.clone(),
                    },
                )
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
        }

        // Set edge properties
        for (key, value) in properties {
            let key_sym = Symbol::new(&key);
            let prop_value = Self::parse_property_value(&value);

            self.graph_service
                .send_command(
                    &src_id,
                    NodeCommand::SetEdgeProperty {
                        edge_type: edge_type_sym.clone(),
                        target: target_id.clone(),
                        key: key_sym,
                        value: prop_value,
                    },
                )
                .await
                .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;
        }

        tracing::debug!(
            "Upserted edge: {} -[{}]-> {} with {} properties",
            src_id_str,
            edge_type,
            target_id_str,
            properties.len()
        );

        Ok(())
    }

    /// Delete a node (soft delete - adds tombstone)
    pub async fn delete_node(&self, node_id_str: &str) -> Result<()> {
        let node_id = NexoraId::from_string(node_id_str);

        self.graph_service
            .send_command(&node_id, NodeCommand::DeleteNode)
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
        let src_id = NexoraId::from_string(src_id_str);
        let target_id = NexoraId::from_string(target_id_str);
        let edge_type_sym = Symbol::new(edge_type);

        self.graph_service
            .send_command(
                &src_id,
                NodeCommand::RemoveEdge {
                    edge_type: edge_type_sym,
                    target: target_id,
                },
            )
            .await
            .map_err(|e| GraphStreamingError::GraphError(e.to_string()))?;

        tracing::debug!("Deleted edge: {} -[{}]-> {}", src_id_str, edge_type, target_id_str);
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
