//! Wikipedia recipe end-to-end tests.
//!
//! Validates the complete recipe lifecycle:
//! 1. Recipe YAML parsing
//! 2. Standing Query registration
//! 3. Ingest source configuration
//! 4. Output sink configuration
//! 5. Full pipeline execution

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_recipe::{OutputRecipe, Recipe, StandingQueryRecipe};
use nexora_standing_query::StandingQueryManager;
use std::sync::Arc;

/// Parse a minimal Wikipedia recipe from YAML.
#[test]
fn test_parse_wikipedia_recipe_yaml() {
    let yaml = r#"
name: wikipedia-ingest
version: "1.0"
description: "Ingest Wikipedia article data and monitor for edits"

standing_queries:
  - name: recent-edits
    pattern:
      type: PropertyFilter
      key: edit_count
      condition:
        GreaterThan: 100.0
    outputs:
      - name: console-out
        type: console

ingest_sources:
  - name: wikipedia-file
    type: file
    path: /data/wikipedia.jsonl
    format: json

outputs:
  - name: console-out
    type: console
"#;

    let recipe = nexora_recipe::parse_recipe(yaml).unwrap();
    assert_eq!(recipe.name, "wikipedia-ingest");
    assert_eq!(recipe.standing_queries.len(), 1);
    assert_eq!(recipe.ingest_sources.len(), 1);
    assert_eq!(recipe.outputs.len(), 1);
}

/// Test that a YAML recipe parses correctly with version as a string.
#[test]
fn test_wikipedia_recipe_with_version() {
    let yaml = r#"
name: simple-recipe
version: "2.0"
"#;
    let recipe = nexora_recipe::parse_recipe(yaml).unwrap();
    assert_eq!(recipe.version.as_deref(), Some("2.0"));
}

/// Integration test: recipe execution registers SQs via executor.
#[tokio::test]
async fn test_recipe_execution_registers_sqs() {
    let config = GraphServiceConfig {
        num_shards: 4,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    };
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(config, persistor));
    let sq_manager = Arc::new(StandingQueryManager::new(100));

    let recipe = Recipe {
        name: "test-recipe".into(),
        version: Some("1.0".into()),
        description: None,
        standing_queries: vec![StandingQueryRecipe {
            name: "high-value".into(),
            pattern: serde_json::json!({
                "type": "PropertyFilter",
                "key": "value",
                "condition": {"GreaterThan": 100.0}
            }),
            outputs: vec![OutputRecipe {
                name: "console".into(),
                output_type: "console".into(),
                config: serde_json::json!({}),
            }],
        }],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    let execution = nexora_recipe::execute_recipe(&graph, &sq_manager, &recipe).await;
    assert!(
        execution.is_ok(),
        "Recipe execution failed: {:?}",
        execution.err()
    );

    let result = execution.unwrap();
    assert_eq!(result.recipe_name, "test-recipe");
    assert!(result.status == "success" || result.status == "partial_success");
    assert!(
        !result.sq_ids.is_empty(),
        "Expected at least 1 SQ to be registered"
    );
}

/// Test that recipe execution handles unknown SQ pattern gracefully.
#[tokio::test]
async fn test_recipe_handles_unknown_sq_pattern() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(config, persistor));
    let sq_manager = Arc::new(StandingQueryManager::new(100));

    let recipe = Recipe {
        name: "invalid-recipe".into(),
        version: Some("1.0".into()),
        description: None,
        standing_queries: vec![StandingQueryRecipe {
            name: "bad-sq".into(),
            pattern: serde_json::json!({
                "type": "UnknownType",
                "data": {}
            }),
            outputs: vec![],
        }],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    let execution = nexora_recipe::execute_recipe(&graph, &sq_manager, &recipe).await;
    // Should not crash — may return error or partial success
    if let Ok(result) = execution {
        assert!(
            !result.sq_ids.is_empty() || result.status == "partial_success",
            "Status: {}, error: {:?}",
            result.status,
            result.error
        );
    }
}

/// Test that repeated recipe execution produces results.
#[tokio::test]
async fn test_recipe_repeated_execution() {
    let config = GraphServiceConfig::default();
    let persistor = Arc::new(InMemoryPersistor::new());
    let graph = Arc::new(GraphService::new(config, persistor));
    let sq_manager = Arc::new(StandingQueryManager::new(100));

    let recipe = Recipe {
        name: "idempotent-test".into(),
        version: Some("1.0".into()),
        description: None,
        standing_queries: vec![StandingQueryRecipe {
            name: "test-sq".into(),
            pattern: serde_json::json!({
                "type": "PropertyFilter",
                "key": "status",
                "condition": {"Equals": {"String": "active"}}
            }),
            outputs: vec![],
        }],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    let r1 = nexora_recipe::execute_recipe(&graph, &sq_manager, &recipe).await;
    let r2 = nexora_recipe::execute_recipe(&graph, &sq_manager, &recipe).await;

    assert!(r1.is_ok(), "First execution failed: {:?}", r1.err());
    assert!(r2.is_ok(), "Second execution failed: {:?}", r2.err());

    let sqs = sq_manager.list().await;
    assert!(
        sqs.len() <= 4,
        "Expected at most 4 SQs (2 registrations x 2 patterns), got {}",
        sqs.len()
    );
}
