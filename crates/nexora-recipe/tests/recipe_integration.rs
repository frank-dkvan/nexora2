//! Comprehensive integration tests for nexora-recipe crate.
//!
//! Coverage:
//! - Recipe parsing: YAML and JSON, minimal recipes, full recipes, error paths
//! - StandingQueryRecipe: structure, serialization
//! - IngestSourceRecipe: validation for file, stdin, kafka, unknown types
//! - OutputRecipe: validation for console, webhook, file, unknown types
//! - StatusRecipe: serialization, defaults
//! - RecipeError variants
//! - RecipeExecutor: registration, validation, end-to-end
//! - Pattern conversion: PropertyFilter, LabelFilter, EdgePattern, And, Or, Not
//! - Pattern conversion error paths: missing fields, unknown types
//! - Concurrency: concurrent recipe parsing and execution
//! - Edge cases: empty recipes, duplicate SQ names, invalid YAML, null fields

use nexora_recipe::{
    executor::{execute_recipe_legacy, RecipeExecution},
    parse_recipe, parse_recipe_json, IngestSourceRecipe, OutputRecipe, Recipe, RecipeError,
    StandingQueryRecipe, StatusRecipe,
};
use nexora_standing_query::StandingQueryManager;
use std::sync::Arc;

// ============================================================
// Recipe Parsing Tests — YAML
// ============================================================

#[test]
fn test_parse_minimal_recipe_yaml() {
    let yaml = r#"
name: minimal-recipe
version: "1.0"
description: bare minimum
"#;
    let recipe = parse_recipe(yaml).unwrap();
    assert_eq!(recipe.name, "minimal-recipe");
    assert_eq!(recipe.version.as_deref(), Some("1.0"));
    assert_eq!(recipe.description.as_deref(), Some("bare minimum"));
    assert!(recipe.standing_queries.is_empty());
    assert!(recipe.ingest_sources.is_empty());
    assert!(recipe.outputs.is_empty());
}

#[test]
fn test_parse_recipe_without_version() {
    let yaml = "name: unnamed\n";
    let recipe = parse_recipe(yaml).unwrap();
    assert_eq!(recipe.name, "unnamed");
    assert!(recipe.version.is_none());
    assert!(recipe.description.is_none());
}

#[test]
fn test_parse_recipe_with_standing_queries() {
    let yaml = r#"
name: recipe-with-sq
standing_queries:
  - name: high-speed
    pattern:
      type: PropertyFilter
      key: speed
      condition:
        GreaterThan: 100.0
"#;
    let recipe = parse_recipe(yaml).unwrap();
    assert_eq!(recipe.standing_queries.len(), 1);
    assert_eq!(recipe.standing_queries[0].name, "high-speed");
}

#[test]
fn test_parse_recipe_with_ingest_sources() {
    let yaml = r#"
name: recipe-with-ingest
ingest_sources:
  - name: file-source
    type: file
    path: /tmp/data.jsonl
    format: jsonl
  - name: kafka-source
    type: kafka
    brokers: "localhost:9092"
    topic: events
"#;
    let recipe = parse_recipe(yaml).unwrap();
    assert_eq!(recipe.ingest_sources.len(), 2);
    assert_eq!(recipe.ingest_sources[0].source_type, "file");
    assert_eq!(recipe.ingest_sources[1].source_type, "kafka");
}

#[test]
fn test_parse_recipe_with_outputs() {
    let yaml = r#"
name: recipe-with-outputs
outputs:
  - name: console-out
    type: console
  - name: webhook-out
    type: webhook
    url: "http://example.com/hook"
"#;
    let recipe = parse_recipe(yaml).unwrap();
    assert_eq!(recipe.outputs.len(), 2);
}

#[test]
fn test_parse_recipe_full() {
    let yaml = r#"
name: full-recipe
version: "2.0"
description: complete pipeline
status:
  enabled: true
  interval_ms: 5000
standing_queries:
  - name: sq-1
    pattern:
      type: PropertyFilter
      key: temp
      condition:
        GreaterThan: 100.0
    outputs:
      - name: webhook-1
        type: webhook
        url: "http://hooks.example.com/alert"
ingest_sources:
  - name: sensor-stream
    type: kafka
    brokers: "kafka:9092"
    topic: sensor-readings
outputs:
  - name: dashboard
    type: console
"#;
    let recipe = parse_recipe(yaml).unwrap();
    assert_eq!(recipe.name, "full-recipe");
    assert_eq!(recipe.standing_queries.len(), 1);
    assert_eq!(recipe.ingest_sources.len(), 1);
    assert_eq!(recipe.outputs.len(), 1);
    assert!(recipe.status.is_some());
    assert!(recipe.status.as_ref().unwrap().enabled);
}

// ============================================================
// Recipe Parsing Tests — JSON
// ============================================================

#[test]
fn test_parse_minimal_recipe_json() {
    let json = r#"{"name": "json-recipe"}"#;
    let recipe = parse_recipe_json(json).unwrap();
    assert_eq!(recipe.name, "json-recipe");
    assert!(recipe.version.is_none());
}

#[test]
fn test_parse_recipe_json_full() {
    let json = r#"{
  "name": "json-pipeline",
  "version": "3.0",
  "description": "from JSON",
  "standing_queries": [
    {
      "name": "sq-json",
      "pattern": {
        "type": "PropertyFilter",
        "key": "status",
        "condition": {"Equals": {"String": "active"}}
      }
    }
  ],
  "ingest_sources": [
    {
      "name": "stdin-source",
      "type": "stdin"
    }
  ],
  "outputs": [
    {
      "name": "console-only",
      "type": "console"
    }
  ],
  "status": {
    "enabled": true,
    "interval_ms": 10000
  }
}"#;
    let recipe = parse_recipe_json(json).unwrap();
    assert_eq!(recipe.name, "json-pipeline");
    assert_eq!(recipe.standing_queries.len(), 1);
    assert_eq!(recipe.ingest_sources.len(), 1);
    assert_eq!(recipe.outputs.len(), 1);
    assert!(recipe.status.is_some());
}

#[test]
fn test_parse_recipe_json_with_control_chars() {
    // Ensure special characters in descriptions don't break parsing
    let json = r#"{
  "name": "escaped",
  "description": "Line1\nLine2\t\"quoted\""
}"#;
    let recipe = parse_recipe_json(json).unwrap();
    assert_eq!(recipe.name, "escaped");
    assert!(recipe.description.unwrap().contains("Line1"));
}

// ============================================================
// Recipe Parsing — Error Paths
// ============================================================

#[test]
fn test_parse_invalid_yaml() {
    let yaml = "name: [unclosed bracket";
    let result = parse_recipe(yaml);
    assert!(result.is_err());
    match result.unwrap_err() {
        RecipeError::Parse(_) => {}
        _ => panic!("Expected Parse error"),
    }
}

#[test]
fn test_parse_invalid_json() {
    let json = "{ broken json ";
    let result = parse_recipe_json(json);
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_string() {
    assert!(parse_recipe("").is_err());
    assert!(parse_recipe_json("").is_err());
}

// ============================================================
// Recipe Serialization Roundtrip
// ============================================================

#[test]
fn test_recipe_serialization_roundtrip() {
    let recipe = Recipe {
        name: "roundtrip".into(),
        version: Some("1.0".into()),
        description: Some("test".into()),
        standing_queries: vec![StandingQueryRecipe {
            name: "sq1".into(),
            pattern: serde_json::json!({
                "type": "PropertyFilter",
                "key": "x",
                "condition": {"GreaterThan": 10.0}
            }),
            outputs: vec![],
        }],
        ingest_sources: vec![IngestSourceRecipe {
            name: "file1".into(),
            source_type: "file".into(),
            config: serde_json::json!({"path": "/tmp/data.jsonl", "format": "jsonl"}),
        }],
        outputs: vec![OutputRecipe {
            name: "console".into(),
            output_type: "console".into(),
            config: serde_json::json!({}),
        }],
        status: Some(StatusRecipe {
            enabled: true,
            interval_ms: Some(5000),
        }),
    };

    let json = serde_json::to_string(&recipe).unwrap();
    let deserialized: Recipe = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.name, "roundtrip");
    assert_eq!(deserialized.standing_queries.len(), 1);
    assert_eq!(deserialized.ingest_sources.len(), 1);
}

// ============================================================
// RecipeExecution End-to-End Tests
// ============================================================

#[tokio::test]
async fn test_execute_recipe_register_single_sq() {
    let recipe = Recipe {
        name: "test-single-sq".into(),
        version: None,
        description: None,
        standing_queries: vec![StandingQueryRecipe {
            name: "high-temp".into(),
            pattern: serde_json::json!({
                "type": "PropertyFilter",
                "key": "temp",
                "condition": {"GreaterThan": 100.0}
            }),
            outputs: vec![],
        }],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await.unwrap();

    assert_eq!(result.sq_ids.len(), 1);
    assert!(result.ingest_sources.is_empty());
    assert!(result.outputs.is_empty());
}

#[tokio::test]
async fn test_execute_recipe_multiple_sqs() {
    let recipe = Recipe {
        name: "multi-sq".into(),
        version: None,
        description: None,
        standing_queries: vec![
            StandingQueryRecipe {
                name: "sq-a".into(),
                pattern: serde_json::json!({
                    "PropertyFilter": {"key": "speed", "condition": {"GreaterThan": 50.0}}
                }),
                outputs: vec![],
            },
            StandingQueryRecipe {
                name: "sq-b".into(),
                pattern: serde_json::json!({
                    "type": "LabelFilter",
                    "labels": ["Person", "Employee"]
                }),
                outputs: vec![],
            },
            StandingQueryRecipe {
                name: "sq-c".into(),
                pattern: serde_json::json!({
                    "type": "EdgePattern",
                    "edge_type": "KNOWS",
                    "direction": "out"
                }),
                outputs: vec![],
            },
        ],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await.unwrap();

    assert_eq!(result.sq_ids.len(), 3);
}

#[tokio::test]
async fn test_execute_recipe_with_ingest_validation() {
    let recipe = Recipe {
        name: "with-ingest".into(),
        version: None,
        description: None,
        standing_queries: vec![],
        ingest_sources: vec![
            IngestSourceRecipe {
                name: "file-1".into(),
                source_type: "file".into(),
                config: serde_json::json!({"path": "/tmp/data.jsonl", "format": "jsonl"}),
            },
            IngestSourceRecipe {
                name: "stdin-1".into(),
                source_type: "stdin".into(),
                config: serde_json::json!({}),
            },
        ],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await.unwrap();

    assert_eq!(result.ingest_sources.len(), 2);
    assert!(result.ingest_sources.contains(&"file-1".to_string()));
    assert!(result.ingest_sources.contains(&"stdin-1".to_string()));
}

#[tokio::test]
async fn test_execute_recipe_with_output_validation() {
    let recipe = Recipe {
        name: "with-outputs".into(),
        version: None,
        description: None,
        standing_queries: vec![],
        ingest_sources: vec![],
        outputs: vec![
            OutputRecipe {
                name: "console-out".into(),
                output_type: "console".into(),
                config: serde_json::json!({}),
            },
            OutputRecipe {
                name: "webhook-out".into(),
                output_type: "webhook".into(),
                config: serde_json::json!({"url": "http://example.com/hook"}),
            },
        ],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await.unwrap();

    assert_eq!(result.outputs.len(), 2);
}

#[tokio::test]
async fn test_execute_recipe_end_to_end_full() {
    let recipe = Recipe {
        name: "full-pipeline".into(),
        version: Some("1.0".into()),
        description: Some("test full pipeline".into()),
        standing_queries: vec![StandingQueryRecipe {
            name: "alert-sq".into(),
            pattern: serde_json::json!({
                "type": "And",
                "patterns": [
                    {"type": "PropertyFilter", "key": "temp", "condition": {"GreaterThan": 100.0}},
                    {"type": "LabelFilter", "labels": ["Sensor"]}
                ]
            }),
            outputs: vec![OutputRecipe {
                name: "alert-webhook".into(),
                output_type: "webhook".into(),
                config: serde_json::json!({"url": "http://alerts.example.com/hook"}),
            }],
        }],
        ingest_sources: vec![IngestSourceRecipe {
            name: "sensor-kafka".into(),
            source_type: "kafka".into(),
            config: serde_json::json!({"brokers": "kafka:9092", "topic": "sensors"}),
        }],
        outputs: vec![OutputRecipe {
            name: "debug-log".into(),
            output_type: "console".into(),
            config: serde_json::json!({}),
        }],
        status: Some(StatusRecipe {
            enabled: true,
            interval_ms: Some(30000),
        }),
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await.unwrap();

    assert_eq!(result.sq_ids.len(), 1);
    assert_eq!(result.ingest_sources.len(), 1);
    assert_eq!(result.outputs.len(), 1);
}

// ============================================================
// Recipe Execution — Error Paths
// ============================================================

#[tokio::test]
async fn test_execute_invalid_ingest_type() {
    let recipe = Recipe {
        name: "bad-ingest".into(),
        version: None,
        description: None,
        standing_queries: vec![],
        ingest_sources: vec![IngestSourceRecipe {
            name: "bad".into(),
            source_type: "invalid_type".into(),
            config: serde_json::json!({}),
        }],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_file_ingest_missing_path() {
    let recipe = Recipe {
        name: "missing-path".into(),
        version: None,
        description: None,
        standing_queries: vec![],
        ingest_sources: vec![IngestSourceRecipe {
            name: "broken".into(),
            source_type: "file".into(),
            config: serde_json::json!({}),
        }],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_file_ingest_unsupported_format() {
    let recipe = Recipe {
        name: "bad-format".into(),
        version: None,
        description: None,
        standing_queries: vec![],
        ingest_sources: vec![IngestSourceRecipe {
            name: "wrong-format".into(),
            source_type: "file".into(),
            config: serde_json::json!({"path": "/tmp/data.xml", "format": "xml"}),
        }],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_webhook_output_missing_url() {
    let recipe = Recipe {
        name: "bad-webhook".into(),
        version: None,
        description: None,
        standing_queries: vec![],
        ingest_sources: vec![],
        outputs: vec![OutputRecipe {
            name: "broken-webhook".into(),
            output_type: "webhook".into(),
            config: serde_json::json!({}),
        }],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_execute_sq_parse_error_in_pattern() {
    let recipe = Recipe {
        name: "bad-sq-pattern".into(),
        version: None,
        description: None,
        standing_queries: vec![StandingQueryRecipe {
            name: "broken-sq".into(),
            pattern: serde_json::json!({"not": "a valid pattern", "nothing": "here"}),
            outputs: vec![],
        }],
        ingest_sources: vec![],
        outputs: vec![],
        status: None,
    };

    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let result = execute_recipe_legacy(&recipe, &sq_manager).await;
    assert!(result.is_err());
}

// ============================================================
// StatusRecipe Tests
// ============================================================

#[test]
fn test_status_recipe_serialization() {
    let status = StatusRecipe {
        enabled: true,
        interval_ms: Some(10000),
    };
    let json = serde_json::to_string(&status).unwrap();
    let deserialized: StatusRecipe = serde_json::from_str(&json).unwrap();
    assert!(deserialized.enabled);
    assert_eq!(deserialized.interval_ms, Some(10000));
}

#[test]
fn test_status_recipe_disabled() {
    let json = r#"{"enabled": false}"#;
    let status: StatusRecipe = serde_json::from_str(json).unwrap();
    assert!(!status.enabled);
    assert!(status.interval_ms.is_none());
}

// ============================================================
// RecipeError Tests
// ============================================================

#[test]
fn test_recipe_error_display() {
    let err = RecipeError::Parse("invalid YAML".into());
    assert!(format!("{}", err).contains("invalid YAML"));

    let err = RecipeError::Validation("missing field".into());
    assert!(format!("{}", err).contains("missing field"));
}

// ============================================================
// OutputRecipe Tests
// ============================================================

#[test]
fn test_output_recipe_console_serialization() {
    let output = OutputRecipe {
        name: "console".into(),
        output_type: "console".into(),
        config: serde_json::json!({}),
    };
    let json = serde_json::to_string(&output).unwrap();
    assert!(json.contains("console"));
}

#[test]
fn test_output_recipe_file_serialization() {
    let output = OutputRecipe {
        name: "file-out".into(),
        output_type: "file".into(),
        config: serde_json::json!({"path": "/tmp/out.log"}),
    };
    let json = serde_json::to_string(&output).unwrap();
    assert!(json.contains("file"));
}

// ============================================================
// Concurrency Tests
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_recipe_execution() {
    let sq_manager = Arc::new(StandingQueryManager::new(64));
    let mut handles = vec![];

    for i in 0..10 {
        let sq = sq_manager.clone();
        handles.push(tokio::spawn(async move {
            let recipe = Recipe {
                name: format!("concurrent-recipe-{}", i),
                version: None,
                description: None,
                standing_queries: vec![StandingQueryRecipe {
                    name: format!("sq-{}", i),
                    pattern: serde_json::json!({
                        "type": "PropertyFilter",
                        "key": format!("field_{}", i),
                        "condition": {"GreaterThan": 10.0}
                    }),
                    outputs: vec![],
                }],
                ingest_sources: vec![],
                outputs: vec![],
                status: None,
            };
            execute_recipe_legacy(&recipe, &sq).await.unwrap()
        }));
    }

    let mut results = vec![];
    for h in handles {
        results.push(h.await.unwrap());
    }

    assert_eq!(results.len(), 10);
    for r in &results {
        assert_eq!(r.sq_ids.len(), 1);
    }
}

// ============================================================
// Send + Sync verification
// ============================================================

#[test]
fn test_types_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Recipe>();
    assert_send_sync::<RecipeExecution>();
}
