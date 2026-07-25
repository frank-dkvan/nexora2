//! NOTE: This crate is planned for future integration.
//! Recipe interpreter — YAML declarative pipeline definitions.
//!
//! A Recipe defines a complete data processing pipeline:
//! - Ingest sources
//! - Standing Queries
//! - Output sinks
//! - UI configuration

use serde::{Deserialize, Serialize};

/// A complete Nexora recipe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Recipe {
    /// Recipe name.
    pub name: String,
    /// Recipe version.
    pub version: Option<String>,
    /// Recipe description.
    pub description: Option<String>,
    /// Standing queries to register.
    #[serde(default)]
    pub standing_queries: Vec<StandingQueryRecipe>,
    /// Ingest sources to start.
    #[serde(default)]
    pub ingest_sources: Vec<IngestSourceRecipe>,
    /// Output configurations.
    #[serde(default)]
    pub outputs: Vec<OutputRecipe>,
    /// Status quo configuration.
    #[serde(default)]
    pub status: Option<StatusRecipe>,
}

/// Explicit recipe status for API and E2E testing.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecipeStatus {
    #[serde(rename = "draft")]
    Draft,
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "paused")]
    Paused,
    #[serde(rename = "error")]
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StandingQueryRecipe {
    pub name: String,
    pub pattern: serde_json::Value,
    #[serde(default)]
    pub outputs: Vec<OutputRecipe>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestSourceRecipe {
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(flatten)]
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputRecipe {
    pub name: String,
    #[serde(rename = "type")]
    pub output_type: String,
    #[serde(flatten)]
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusRecipe {
    pub enabled: bool,
    pub interval_ms: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecipeError {
    #[error("parse error: {0}")]
    Parse(String),
    #[error("validation error: {0}")]
    Validation(String),
}

/// Parse a recipe from YAML string.
pub fn parse_recipe(yaml: &str) -> Result<Recipe, RecipeError> {
    serde_yaml::from_str(yaml).map_err(|e| RecipeError::Parse(e.to_string()))
}

/// Parse a recipe from JSON string.
pub fn parse_recipe_json(json: &str) -> Result<Recipe, RecipeError> {
    serde_json::from_str(json).map_err(|e| RecipeError::Parse(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_recipe() {
        let yaml = r#"
name: test-recipe
version: "1.0"
description: A test recipe
"#;
        let recipe = parse_recipe(yaml).unwrap();
        assert_eq!(recipe.name, "test-recipe");
        assert_eq!(recipe.version.as_deref(), Some("1.0"));
    }

    #[test]
    fn test_parse_recipe_with_ingest() {
        let yaml = r#"
name: kafka-recipe
ingest_sources:
  - name: my-kafka
    type: kafka
    brokers: "localhost:9092"
    topic: "events"
    format: json
"#;
        let recipe = parse_recipe(yaml).unwrap();
        assert_eq!(recipe.ingest_sources.len(), 1);
        assert_eq!(recipe.ingest_sources[0].source_type, "kafka");
    }

    #[test]
    fn test_parse_recipe_with_sq() {
        let yaml = r#"
name: sq-recipe
standing_queries:
  - name: high-speed-alert
    pattern:
      type: PropertyFilter
      key: speed
      condition:
        GreaterThan: 100.0
    outputs:
      - name: console
        type: console
"#;
        let recipe = parse_recipe(yaml).unwrap();
        assert_eq!(recipe.standing_queries.len(), 1);
        assert_eq!(recipe.standing_queries[0].name, "high-speed-alert");
    }
}
pub mod executor;
pub use executor::execute_recipe;
pub use executor::execute_recipe_legacy;
