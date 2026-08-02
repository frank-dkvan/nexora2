//! Projection rule definitions and parser
//!
//! Projection rules define how events from nexora-eventlog are mapped to graph nodes and edges.

use crate::{GraphStreamingError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

/// A complete projection rule defining how to map events to graph elements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionRule {
    /// Unique name for this projection rule
    pub name: String,

    /// Source topic to subscribe to in nexora-eventlog
    pub source_topic: String,

    /// Optional event filter: only process events matching these criteria
    /// Example: {"status": ["IN_TRANSIT", "DELIVERED"]}
    #[serde(default)]
    pub event_filter: Option<HashMap<String, Vec<String>>>,

    /// Node projection: how to create/update the node
    pub node: NodeProjection,

    /// Optional edge projection: how to create/update an edge from this node
    #[serde(default)]
    pub edge: Option<EdgeProjection>,
}

/// Node projection definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProjection {
    /// Node ID template (e.g., "{{cargo_id}}")
    pub id: String,

    /// Labels to apply to the node
    pub labels: Vec<String>,

    /// Property templates (key -> template)
    /// Example: {"status": "{{status}}", "temperature": "{{temperature}}"}
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Edge projection definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeProjection {
    /// Edge type (e.g., "LOCATED_AT")
    pub edge_type: String,

    /// Target node ID template (e.g., "{{location_code}}")
    pub target_id: String,

    /// Edge property templates
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl ProjectionRule {
    /// Load a single projection rule from a YAML file
    pub async fn load_from_file(path: impl AsRef<Path>) -> Result<Vec<Self>> {
        let content = fs::read_to_string(path.as_ref()).await?;
        Self::load_from_str(&content)
    }

    /// Load projection rules from a YAML string
    pub fn load_from_str(yaml: &str) -> Result<Vec<Self>> {
        let wrapper: ProjectionRuleFile = serde_yaml::from_str(yaml)?;

        // Validate rules
        for rule in &wrapper.projections {
            rule.validate()?;
        }

        Ok(wrapper.projections)
    }

    /// Load all projection rules from a directory
    pub async fn load_from_dir(dir: impl AsRef<Path>) -> Result<Vec<Self>> {
        let mut all_rules = Vec::new();
        let mut entries = fs::read_dir(dir.as_ref()).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("yaml")
                || path.extension().and_then(|s| s.to_str()) == Some("yml")
            {
                match Self::load_from_file(&path).await {
                    Ok(rules) => all_rules.extend(rules),
                    Err(e) => {
                        tracing::warn!("Failed to load projection rule from {:?}: {}", path, e);
                    }
                }
            }
        }

        Ok(all_rules)
    }

    /// Validate this projection rule
    fn validate(&self) -> Result<()> {
        // Name must not be empty
        if self.name.is_empty() {
            return Err(GraphStreamingError::InvalidRule(
                "Projection rule name cannot be empty".to_string(),
            ));
        }

        // Source topic must not be empty
        if self.source_topic.is_empty() {
            return Err(GraphStreamingError::InvalidRule(
                "Source topic cannot be empty".to_string(),
            ));
        }

        // Node ID must not be empty
        if self.node.id.is_empty() {
            return Err(GraphStreamingError::InvalidRule(
                "Node ID template cannot be empty".to_string(),
            ));
        }

        // Node must have at least one label
        if self.node.labels.is_empty() {
            return Err(GraphStreamingError::InvalidRule(
                "Node must have at least one label".to_string(),
            ));
        }

        // If edge is present, validate it
        if let Some(edge) = &self.edge {
            if edge.edge_type.is_empty() {
                return Err(GraphStreamingError::InvalidRule(
                    "Edge type cannot be empty".to_string(),
                ));
            }
            if edge.target_id.is_empty() {
                return Err(GraphStreamingError::InvalidRule(
                    "Edge target ID template cannot be empty".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Check if an event matches this rule's filter
    pub fn matches_filter(&self, event_data: &serde_json::Value) -> bool {
        let Some(filter) = &self.event_filter else {
            return true; // No filter = match all
        };

        for (key, allowed_values) in filter {
            let event_value = event_data.get(key);

            match event_value {
                Some(serde_json::Value::String(s)) => {
                    if !allowed_values.contains(s) {
                        return false;
                    }
                }
                Some(serde_json::Value::Number(n)) => {
                    let n_str = n.to_string();
                    if !allowed_values.contains(&n_str) {
                        return false;
                    }
                }
                _ => return false, // Missing or wrong type
            }
        }

        true
    }
}

/// Wrapper for YAML file format
#[derive(Debug, Deserialize)]
struct ProjectionRuleFile {
    projections: Vec<ProjectionRule>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_simple_rule() {
        let yaml = r#"
projections:
  - name: cargo_node
    source_topic: nexora.cargo
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
      properties:
        cargo_id: "{{cargo_id}}"
        status: "{{status}}"
"#;

        let rules = ProjectionRule::load_from_str(yaml).unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].name, "cargo_node");
        assert_eq!(rules[0].source_topic, "nexora.cargo");
        assert_eq!(rules[0].node.labels, vec!["Cargo"]);
        assert_eq!(rules[0].node.properties.len(), 2);
        assert!(rules[0].edge.is_none());
    }

    #[test]
    fn test_parse_rule_with_edge() {
        let yaml = r#"
projections:
  - name: cargo_location
    source_topic: nexora.cargo
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
      properties:
        status: "{{status}}"
    edge:
      edge_type: LOCATED_AT
      target_id: "{{location_code}}"
      properties:
        timestamp: "{{event_time}}"
"#;

        let rules = ProjectionRule::load_from_str(yaml).unwrap();
        assert_eq!(rules.len(), 1);

        let edge = rules[0].edge.as_ref().unwrap();
        assert_eq!(edge.edge_type, "LOCATED_AT");
        assert_eq!(edge.target_id, "{{location_code}}");
        assert_eq!(edge.properties.get("timestamp").unwrap(), "{{event_time}}");
    }

    #[test]
    fn test_parse_rule_with_filter() {
        let yaml = r#"
projections:
  - name: active_cargo
    source_topic: nexora.cargo
    event_filter:
      status: ["IN_TRANSIT", "DELIVERED"]
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
"#;

        let rules = ProjectionRule::load_from_str(yaml).unwrap();
        assert_eq!(rules.len(), 1);

        let filter = rules[0].event_filter.as_ref().unwrap();
        assert_eq!(filter.get("status").unwrap(), &vec!["IN_TRANSIT", "DELIVERED"]);
    }

    #[test]
    fn test_validate_empty_name() {
        let yaml = r#"
projections:
  - name: ""
    source_topic: nexora.cargo
    node:
      id: "{{id}}"
      labels: ["Test"]
"#;

        let result = ProjectionRule::load_from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_no_labels() {
        let yaml = r#"
projections:
  - name: test
    source_topic: nexora.cargo
    node:
      id: "{{id}}"
      labels: []
"#;

        let result = ProjectionRule::load_from_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_matches_filter() {
        let yaml = r#"
projections:
  - name: test
    source_topic: nexora.test
    event_filter:
      status: ["ACTIVE", "PENDING"]
      priority: ["1", "2"]
    node:
      id: "{{id}}"
      labels: ["Test"]
"#;

        let rules = ProjectionRule::load_from_str(yaml).unwrap();
        let rule = &rules[0];

        // Match
        let event1 = json!({"status": "ACTIVE", "priority": "1"});
        assert!(rule.matches_filter(&event1));

        // No match - wrong status
        let event2 = json!({"status": "INACTIVE", "priority": "1"});
        assert!(!rule.matches_filter(&event2));

        // No match - missing field
        let event3 = json!({"status": "ACTIVE"});
        assert!(!rule.matches_filter(&event3));
    }

    #[test]
    fn test_no_filter_matches_all() {
        let yaml = r#"
projections:
  - name: test
    source_topic: nexora.test
    node:
      id: "{{id}}"
      labels: ["Test"]
"#;

        let rules = ProjectionRule::load_from_str(yaml).unwrap();
        let rule = &rules[0];

        let event = json!({"anything": "goes"});
        assert!(rule.matches_filter(&event));
    }

    #[test]
    fn test_multiple_rules() {
        let yaml = r#"
projections:
  - name: rule1
    source_topic: topic1
    node:
      id: "{{id}}"
      labels: ["Type1"]
  - name: rule2
    source_topic: topic2
    node:
      id: "{{id}}"
      labels: ["Type2"]
"#;

        let rules = ProjectionRule::load_from_str(yaml).unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].name, "rule1");
        assert_eq!(rules[1].name, "rule2");
    }
}
