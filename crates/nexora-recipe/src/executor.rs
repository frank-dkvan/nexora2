//! Recipe executor — applies a Recipe to configure the system.
//!
//! The executor:
//! 1. Registers all standing queries from the recipe, deserializing each
//!    pattern from its JSON representation into a `StandingQueryPattern`.
//! 2. Validates ingest source configurations (returns names for the caller
//!    to start the actual ingest streams).
//! 3. Validates output configurations.
//! 4. Returns a summary of what was registered.

use crate::{IngestSourceRecipe, OutputRecipe, Recipe};
use nexora_standing_query::pattern::StandingQueryPattern;
use nexora_standing_query::StandingQueryManager;
use std::sync::Arc;

/// Result of executing a recipe.
#[derive(Debug)]
pub struct RecipeExecution {
    /// IDs of registered standing queries, in order.
    pub sq_ids: Vec<uuid::Uuid>,
    /// Validated ingest source names.
    pub ingest_sources: Vec<String>,
    /// Validated output names.
    pub outputs: Vec<String>,
}

/// Parse a `StandingQueryPattern` from a `serde_json::Value`.
///
/// Supports two JSON forms:
///
/// 1. **Serde enum form** (externally tagged, what serde produces):
///    ```json
///    {"PropertyFilter": {"key": "speed", "condition": {"GreaterThan": 100.0}}}
///    ```
///
/// 2. **Recipe-style** (flat with `type` field, what YAML recipes use):
///    ```json
///    {"type": "PropertyFilter", "key": "speed", "condition": {"GreaterThan": 100.0}}
///    ```
fn parse_pattern(value: &serde_json::Value) -> Result<StandingQueryPattern, String> {
    // First try direct serde deserialization (externally-tagged enum form).
    if let Ok(pattern) = serde_json::from_value::<StandingQueryPattern>(value.clone()) {
        return Ok(pattern);
    }

    // Recipe-style: {"type": "PropertyFilter", "key": ..., "condition": ...}
    if let Some(obj) = value.as_object() {
        if let Some(p_type) = obj.get("type").and_then(|v| v.as_str()) {
            let converted = convert_recipe_style_pattern(p_type, obj)?;
            return serde_json::from_value::<StandingQueryPattern>(converted)
                .map_err(|e| format!("failed to deserialize converted pattern: {e}"));
        }
    }

    Err(format!(
        "could not parse standing query pattern from JSON: {value}"
    ))
}

/// Convert recipe-style pattern JSON (flat with "type" field) into the
/// externally-tagged enum representation that serde expects.
fn convert_recipe_style_pattern(
    p_type: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value, String> {
    match p_type {
        "PropertyFilter" => {
            let key = obj
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or("PropertyFilter requires 'key'")?;
            let condition = obj
                .get("condition")
                .ok_or("PropertyFilter requires 'condition'")?;
            Ok(serde_json::json!({
                "PropertyFilter": {
                    "key": key,
                    "condition": condition,
                }
            }))
        }
        "LabelFilter" => {
            // Accept either a single string or an array of strings.
            let labels = if let Some(arr) = obj.get("labels").and_then(|v| v.as_array()) {
                serde_json::Value::Array(arr.clone())
            } else if let Some(s) = obj.get("label").and_then(|v| v.as_str()) {
                serde_json::json!([s])
            } else if let Some(s) = obj.get("labels").and_then(|v| v.as_str()) {
                serde_json::json!([s])
            } else {
                return Err("LabelFilter requires 'labels' or 'label'".to_string());
            };
            Ok(serde_json::json!({ "LabelFilter": labels }))
        }
        "EdgePattern" => {
            let edge_type = obj
                .get("edge_type")
                .or_else(|| obj.get("edgeType"))
                .and_then(|v| v.as_str())
                .ok_or("EdgePattern requires 'edge_type'")?;
            let direction = obj
                .get("direction")
                .and_then(|v| v.as_str())
                .unwrap_or("Out");
            let direction = match direction {
                s if s.eq_ignore_ascii_case("out") => "Out",
                s if s.eq_ignore_ascii_case("in") => "In",
                s if s.eq_ignore_ascii_case("both") => "Both",
                other => return Err(format!("invalid edge direction: {other}")),
            };
            let target_val = obj
                .get("target_pattern")
                .or_else(|| obj.get("targetPattern"));
            Ok(serde_json::json!({
                "EdgePattern": {
                    "edge_type": edge_type,
                    "direction": direction,
                    "target_pattern": target_val,
                }
            }))
        }
        "And" => {
            let patterns = obj
                .get("patterns")
                .and_then(|v| v.as_array())
                .ok_or("And requires 'patterns' array")?;
            let converted: Vec<serde_json::Value> = patterns
                .iter()
                .map(convert_nested_pattern)
                .collect::<Result<_, _>>()?;
            Ok(serde_json::json!({ "And": converted }))
        }
        "Or" => {
            let patterns = obj
                .get("patterns")
                .and_then(|v| v.as_array())
                .ok_or("Or requires 'patterns' array")?;
            let converted: Vec<serde_json::Value> = patterns
                .iter()
                .map(convert_nested_pattern)
                .collect::<Result<_, _>>()?;
            Ok(serde_json::json!({ "Or": converted }))
        }
        "Not" => {
            let inner = obj.get("pattern").ok_or("Not requires 'pattern'")?;
            let converted = convert_nested_pattern(inner)?;
            Ok(serde_json::json!({ "Not": converted }))
        }
        other => Err(format!("unknown pattern type: {other}")),
    }
}

/// Convert a nested pattern value — tries direct serde first, then recipe-style.
fn convert_nested_pattern(value: &serde_json::Value) -> Result<serde_json::Value, String> {
    // If it's already in serde enum form, return as-is.
    if serde_json::from_value::<StandingQueryPattern>(value.clone()).is_ok() {
        return Ok(value.clone());
    }
    // Otherwise convert from recipe-style.
    if let Some(obj) = value.as_object() {
        if let Some(t) = obj.get("type").and_then(|v| v.as_str()) {
            return convert_recipe_style_pattern(t, obj);
        }
    }
    Err(format!("could not convert nested pattern: {value}"))
}

/// Validate an ingest source configuration.
fn validate_ingest_source(source: &IngestSourceRecipe) -> Result<(), String> {
    match source.source_type.as_str() {
        "file" => {
            let path = source
                .config
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("file ingest requires 'path' in config")?;
            if path.is_empty() {
                return Err("file ingest 'path' must not be empty".to_string());
            }
            let format = source
                .config
                .get("format")
                .and_then(|v| v.as_str())
                .unwrap_or("json");
            if !["json", "jsonl", "csv"].contains(&format) {
                return Err(format!("unsupported ingest format: {format}"));
            }
            Ok(())
        }
        "stdin" => Ok(()),
        "kafka" => {
            let brokers = source
                .config
                .get("brokers")
                .and_then(|v| v.as_str())
                .ok_or("kafka ingest requires 'brokers' in config")?;
            let topic = source
                .config
                .get("topic")
                .and_then(|v| v.as_str())
                .ok_or("kafka ingest requires 'topic' in config")?;
            if brokers.is_empty() || topic.is_empty() {
                return Err("kafka brokers and topic must not be empty".to_string());
            }
            Ok(())
        }
        other => Err(format!("unsupported ingest source type: {other}")),
    }
}

/// Validate an output configuration.
fn validate_output(output: &OutputRecipe) -> Result<(), String> {
    match output.output_type.as_str() {
        "console" => Ok(()),
        "webhook" => {
            let url = output
                .config
                .get("url")
                .and_then(|v| v.as_str())
                .ok_or("webhook output requires 'url' in config")?;
            if url.is_empty() {
                return Err("webhook output 'url' must not be empty".to_string());
            }
            Ok(())
        }
        "file" => {
            let path = output
                .config
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("file output requires 'path' in config")?;
            if path.is_empty() {
                return Err("file output 'path' must not be empty".to_string());
            }
            Ok(())
        }
        other => Err(format!("unsupported output type: {other}")),
    }
}

/// Result of executing a recipe with enhanced context.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecipeExecutionResult {
    /// Unique run identifier.
    pub run_id: String,
    /// Recipe name.
    pub recipe_name: String,
    /// Execution status.
    pub status: String,
    /// IDs of registered standing queries.
    pub sq_ids: Vec<String>,
    /// Timestamp when execution started.
    pub started_at: String,
    /// Timestamp when execution completed.
    pub finished_at: String,
    /// Any error message.
    pub error: Option<String>,
}

/// Execute a Recipe: register all standing queries and validate ingest/output configs.
///
/// This is the enhanced version that includes graph context and returns detailed results.
pub async fn execute_recipe(
    _graph: &Arc<nexora_core::GraphService>,
    sq_manager: &Arc<StandingQueryManager>,
    recipe: &Recipe,
) -> Result<RecipeExecutionResult, String> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let started_at = chrono::Utc::now().to_rfc3339();

    let mut sq_ids = Vec::new();
    let mut error = None;

    // Register standing queries
    for sq_recipe in &recipe.standing_queries {
        let pattern = parse_pattern(&sq_recipe.pattern)
            .map_err(|e| format!("SQ '{}': {e}", sq_recipe.name))?;

        let id = sq_manager.register(&sq_recipe.name, pattern).await;
        sq_ids.push(id.to_string());
        tracing::info!("Recipe: registered SQ '{}' ({})", sq_recipe.name, id);
    }

    // Validate ingest sources
    for source in &recipe.ingest_sources {
        if let Err(e) = validate_ingest_source(source) {
            error = Some(format!("ingest '{}': {e}", source.name));
        }
    }

    // Validate outputs
    for output in &recipe.outputs {
        if let Err(e) = validate_output(output) {
            error = Some(format!("output '{}': {e}", output.name));
        }
    }

    for sq_recipe in &recipe.standing_queries {
        for output in &sq_recipe.outputs {
            if let Err(e) = validate_output(output) {
                if error.is_none() {
                    error = Some(format!(
                        "SQ '{}' output '{}': {e}",
                        sq_recipe.name, output.name
                    ));
                }
            }
        }
    }

    let status = if error.is_some() {
        "partial_success"
    } else {
        "success"
    };
    let finished_at = chrono::Utc::now().to_rfc3339();

    Ok(RecipeExecutionResult {
        run_id,
        recipe_name: recipe.name.clone(),
        status: status.to_string(),
        sq_ids,
        started_at,
        finished_at,
        error,
    })
}

// Keep the old function for backward compatibility with existing tests
/// Deprecated: use the 3-argument `execute_recipe` instead.
pub async fn execute_recipe_legacy(
    recipe: &Recipe,
    sq_manager: &Arc<StandingQueryManager>,
) -> Result<RecipeExecution, String> {
    let mut sq_ids = Vec::new();
    let mut ingest_sources = Vec::new();
    let mut outputs = Vec::new();

    // Register standing queries
    for sq_recipe in &recipe.standing_queries {
        let pattern = parse_pattern(&sq_recipe.pattern)
            .map_err(|e| format!("SQ '{}': {e}", sq_recipe.name))?;

        let id = sq_manager.register(&sq_recipe.name, pattern).await;
        sq_ids.push(id);
        tracing::info!("Recipe: registered SQ '{}' ({})", sq_recipe.name, id);

        // Validate per-SQ outputs
        for output in &sq_recipe.outputs {
            validate_output(output)
                .map_err(|e| format!("SQ '{}' output '{}': {e}", sq_recipe.name, output.name))?;
        }
    }

    // Validate ingest sources
    for source in &recipe.ingest_sources {
        validate_ingest_source(source).map_err(|e| format!("ingest '{}': {e}", source.name))?;
        ingest_sources.push(source.name.clone());
    }

    // Validate top-level outputs
    for output in &recipe.outputs {
        validate_output(output).map_err(|e| format!("output '{}': {e}", output.name))?;
        outputs.push(output.name.clone());
    }

    Ok(RecipeExecution {
        sq_ids,
        ingest_sources,
        outputs,
    })
}

// Old execute_recipe entry point for existing tests
pub async fn execute_recipe_old(
    recipe: &Recipe,
    sq_manager: &Arc<StandingQueryManager>,
) -> Result<RecipeExecution, String> {
    execute_recipe_legacy(recipe, sq_manager).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StandingQueryRecipe;

    #[test]
    fn test_parse_pattern_property_filter_serde_form() {
        let json = serde_json::json!({
            "PropertyFilter": {
                "key": "speed",
                "condition": {"GreaterThan": 100.0}
            }
        });
        let pattern = parse_pattern(&json).unwrap();
        assert!(matches!(pattern, StandingQueryPattern::PropertyFilter(_)));
    }

    #[test]
    fn test_parse_pattern_property_filter_recipe_style() {
        let json = serde_json::json!({
            "type": "PropertyFilter",
            "key": "speed",
            "condition": {"GreaterThan": 100.0}
        });
        let pattern = parse_pattern(&json).unwrap();
        assert!(matches!(pattern, StandingQueryPattern::PropertyFilter(_)));
    }

    #[test]
    fn test_parse_pattern_label_filter_recipe_style() {
        let json = serde_json::json!({
            "type": "LabelFilter",
            "labels": ["Person", "User"]
        });
        let pattern = parse_pattern(&json).unwrap();
        assert!(matches!(pattern, StandingQueryPattern::LabelFilter(_)));
    }

    #[test]
    fn test_parse_pattern_edge_recipe_style() {
        let json = serde_json::json!({
            "type": "EdgePattern",
            "edge_type": "KNOWS",
            "direction": "out"
        });
        let pattern = parse_pattern(&json).unwrap();
        assert!(matches!(pattern, StandingQueryPattern::EdgePattern(_)));
    }

    #[test]
    fn test_parse_pattern_and_recipe_style() {
        let json = serde_json::json!({
            "type": "And",
            "patterns": [
                {"type": "PropertyFilter", "key": "speed", "condition": {"GreaterThan": 100.0}},
                {"type": "PropertyFilter", "key": "name", "condition": "Exists"}
            ]
        });
        let pattern = parse_pattern(&json).unwrap();
        assert!(matches!(pattern, StandingQueryPattern::And(_)));
    }

    #[test]
    fn test_parse_pattern_not_recipe_style() {
        let json = serde_json::json!({
            "type": "Not",
            "pattern": {"type": "PropertyFilter", "key": "status", "condition": {"Equals": {"String": "inactive"}}}
        });
        let pattern = parse_pattern(&json).unwrap();
        assert!(matches!(pattern, StandingQueryPattern::Not(_)));
    }

    #[test]
    fn test_parse_pattern_invalid() {
        let json = serde_json::json!({"foo": "bar"});
        assert!(parse_pattern(&json).is_err());
    }

    #[test]
    fn test_validate_ingest_file() {
        let source = IngestSourceRecipe {
            name: "test".to_string(),
            source_type: "file".to_string(),
            config: serde_json::json!({"path": "/tmp/data.jsonl", "format": "jsonl"}),
        };
        assert!(validate_ingest_source(&source).is_ok());
    }

    #[test]
    fn test_validate_ingest_file_missing_path() {
        let source = IngestSourceRecipe {
            name: "test".to_string(),
            source_type: "file".to_string(),
            config: serde_json::json!({}),
        };
        assert!(validate_ingest_source(&source).is_err());
    }

    #[test]
    fn test_validate_ingest_kafka() {
        let source = IngestSourceRecipe {
            name: "test".to_string(),
            source_type: "kafka".to_string(),
            config: serde_json::json!({"brokers": "localhost:9092", "topic": "events"}),
        };
        assert!(validate_ingest_source(&source).is_ok());
    }

    #[test]
    fn test_validate_output_console() {
        let output = OutputRecipe {
            name: "console".to_string(),
            output_type: "console".to_string(),
            config: serde_json::json!({}),
        };
        assert!(validate_output(&output).is_ok());
    }

    #[test]
    fn test_validate_output_webhook() {
        let output = OutputRecipe {
            name: "hook".to_string(),
            output_type: "webhook".to_string(),
            config: serde_json::json!({"url": "http://example.com/hook"}),
        };
        assert!(validate_output(&output).is_ok());
    }

    #[test]
    fn test_validate_output_webhook_missing_url() {
        let output = OutputRecipe {
            name: "hook".to_string(),
            output_type: "webhook".to_string(),
            config: serde_json::json!({}),
        };
        assert!(validate_output(&output).is_err());
    }

    #[tokio::test]
    async fn test_execute_recipe_end_to_end() {
        let recipe = Recipe {
            name: "test-recipe".to_string(),
            version: Some("1.0".to_string()),
            description: None,
            standing_queries: vec![StandingQueryRecipe {
                name: "high-speed".to_string(),
                pattern: serde_json::json!({
                    "type": "PropertyFilter",
                    "key": "speed",
                    "condition": {"GreaterThan": 100.0}
                }),
                outputs: vec![OutputRecipe {
                    name: "console".to_string(),
                    output_type: "console".to_string(),
                    config: serde_json::json!({}),
                }],
            }],
            ingest_sources: vec![IngestSourceRecipe {
                name: "file-1".to_string(),
                source_type: "file".to_string(),
                config: serde_json::json!({"path": "/tmp/data.jsonl", "format": "jsonl"}),
            }],
            outputs: vec![],
            status: None,
        };

        let sq_manager = Arc::new(StandingQueryManager::new(64));
        let result = execute_recipe_old(&recipe, &sq_manager).await.unwrap();

        assert_eq!(result.sq_ids.len(), 1);
        assert_eq!(result.ingest_sources, vec!["file-1"]);
        assert!(result.outputs.is_empty());
    }
}
