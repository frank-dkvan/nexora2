//! Integration tests for nexora-client.
//!
//! These tests verify the client's construction, configuration, and type
//! serialization — they do not require a running nexora server.

use nexora_client::*;
use serde_json::json;
use std::time::Duration;

// ============================================================
// Construction & Configuration
// ============================================================

#[test]
fn test_default_construction() {
    let client = NexoraClient::new().unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080");
}

#[test]
fn test_builder_with_custom_url() {
    let client = NexoraClient::builder()
        .base_url("http://graph.example.com:9090")
        .build()
        .unwrap();
    assert_eq!(client.base_url(), "http://graph.example.com:9090");
}

#[test]
fn test_builder_with_bearer_token() {
    let client = NexoraClient::builder()
        .bearer_token("my-secret-token")
        .build()
        .unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080");
}

#[test]
fn test_builder_with_timeout() {
    let client = NexoraClient::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080");
}

#[test]
fn test_builder_with_max_retries() {
    let client = NexoraClient::builder().max_retries(5).build().unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080");
}

#[test]
fn test_builder_invalid_url() {
    let result = NexoraClient::builder().base_url("not a valid url").build();
    assert!(result.is_err());
}

#[test]
fn test_client_clone_shares_state() {
    let client = NexoraClient::new().unwrap();
    let cloned = client.clone();
    assert_eq!(client.base_url(), cloned.base_url());
}

// ============================================================
// Type Serialization
// ============================================================

#[test]
fn test_cypher_request_serialization() {
    let req = CypherRequest::new("MATCH (n) RETURN n LIMIT 10");
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["query"], "MATCH (n) RETURN n LIMIT 10");
}

#[test]
fn test_sql_request_serialization() {
    let req = SqlRequest::new("SELECT * FROM nodes LIMIT 10");
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["query"], "SELECT * FROM nodes LIMIT 10");
}

#[test]
fn test_add_edge_request_serialization() {
    let req = AddEdgeRequest::new("KNOWS", "61626364", "out");
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["edge_type"], "KNOWS");
    assert_eq!(json["target"], "61626364");
    assert_eq!(json["direction"], "out");
}

#[test]
fn test_vector_search_request_serialization() {
    let req = VectorSearchRequest {
        vector: vec![0.1, 0.2, 0.3],
        k: 5,
    };
    let json = serde_json::to_value(&req).unwrap();
    let arr = json["vector"].as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // f32 → f64 promotion introduces small precision differences,
    // so compare with tolerance instead of exact equality.
    assert!((arr[0].as_f64().unwrap() - 0.1).abs() < 1e-6);
    assert!((arr[1].as_f64().unwrap() - 0.2).abs() < 1e-6);
    assert!((arr[2].as_f64().unwrap() - 0.3).abs() < 1e-6);
    assert_eq!(json["k"], 5);
}

#[test]
fn test_create_sq_request_serialization() {
    let req = CreateSqRequest {
        name: "my-sq".to_string(),
        pattern: SqPatternRequest {
            pattern_type: "PropertyFilter".to_string(),
            key: Some("age".to_string()),
            condition: Some(json!({"type": "GreaterThan", "value": 18})),
            labels: None,
        },
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "my-sq");
    assert_eq!(json["pattern"]["type"], "PropertyFilter");
    assert_eq!(json["pattern"]["key"], "age");
}

#[test]
fn test_create_recipe_request_serialization() {
    let req = CreateRecipeRequest {
        name: "my-recipe".to_string(),
        description: Some("A test recipe".to_string()),
        steps: vec![RecipeStepDef {
            query: "MATCH (n) RETURN n".to_string(),
            description: None,
        }],
        trigger: None,
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "my-recipe");
    assert_eq!(json["description"], "A test recipe");
    assert_eq!(json["steps"][0]["query"], "MATCH (n) RETURN n");
}

#[test]
fn test_file_ingest_request_serialization() {
    let req = FileIngestRequest {
        path: "data.json".to_string(),
        id_field: "id".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["path"], "data.json");
    assert_eq!(json["id_field"], "id");
}

#[test]
fn test_kafka_stream_request_serialization() {
    let req = KafkaStreamRequest {
        brokers: "localhost:9092".to_string(),
        topic: "events".to_string(),
        group_id: "my-group".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["brokers"], "localhost:9092");
    assert_eq!(json["topic"], "events");
    assert_eq!(json["group_id"], "my-group");
}

#[test]
fn test_token_request_serialization() {
    let req = TokenRequest {
        user_id: "alice".to_string(),
        role: "admin".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["user_id"], "alice");
    assert_eq!(json["role"], "admin");
}

#[test]
fn test_create_materialized_view_request_serialization() {
    let req = CreateMaterializedViewRequest {
        name: "high_speed".to_string(),
        query: "MATCH (n) WHERE n.speed > 100 RETURN n.id, n.speed".to_string(),
        refresh_mode: "incremental".to_string(),
        schema: vec![
            ColumnDefRequest {
                name: "id".to_string(),
                data_type: "string".to_string(),
            },
            ColumnDefRequest {
                name: "speed".to_string(),
                data_type: "float".to_string(),
            },
        ],
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "high_speed");
    assert_eq!(json["refresh_mode"], "incremental");
    assert_eq!(json["schema"][0]["name"], "id");
    assert_eq!(json["schema"][1]["data_type"], "float");
}

#[test]
fn test_udf_register_request_serialization() {
    let req = UdfRegisterRequest {
        name: "my_udf".to_string(),
        code: r#"{"expression":"x + 1","arity":1}"#.to_string(),
        language: "native".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "my_udf");
    assert_eq!(json["language"], "native");
}

// ============================================================
// Type Deserialization
// ============================================================

#[test]
fn test_health_response_deserialization() {
    let json = json!({
        "status": "healthy",
        "mode": "single-node",
        "profile": "lite-ephemeral",
        "active_nodes": 42,
        "shards": 4,
        "standing_queries": 2,
        "readiness": "ready",
        "liveness": "alive",
        "durability": "ephemeral",
        "version": "0.1.0",
        "uptime_seconds": 3600
    });
    let resp: HealthResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, "healthy");
    assert_eq!(resp.active_nodes, 42);
    assert_eq!(resp.shards, 4);
    assert_eq!(resp.version, "0.1.0");
}

#[test]
fn test_cypher_response_deserialization() {
    let json = json!({
        "columns": ["name", "age"],
        "rows": [["Alice", 30]],
        "error": null,
        "as_of": null
    });
    let resp: CypherResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.columns, vec!["name", "age"]);
    assert_eq!(resp.rows.len(), 1);
    assert_eq!(resp.rows[0][0], "Alice");
    assert!(resp.error.is_none());
}

#[test]
fn test_cypher_response_with_write_stats() {
    let json = json!({
        "columns": [],
        "rows": [],
        "error": null,
        "as_of": null,
        "write_stats": {
            "nodes_created": 1,
            "nodes_deleted": 0,
            "properties_set": 2,
            "relationships_created": 0,
            "relationships_deleted": 0,
            "labels_added": 1,
            "labels_removed": 0
        }
    });
    let resp: CypherResponse = serde_json::from_value(json).unwrap();
    let stats = resp.write_stats.unwrap();
    assert_eq!(stats.nodes_created, 1);
    assert_eq!(stats.properties_set, 2);
    assert_eq!(stats.labels_added, 1);
}

#[test]
fn test_vector_search_response_deserialization() {
    let json = json!({
        "query": [0.1, 0.2, 0.3],
        "k": 5,
        "neighbors": [
            {"qid": "61626364", "distance": 0.5},
            {"qid": "65666768", "distance": 0.8}
        ]
    });
    let resp: VectorSearchResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.k, 5);
    assert_eq!(resp.neighbors.len(), 2);
    assert_eq!(resp.neighbors[0].qid, "61626364");
    assert_eq!(resp.neighbors[1].distance, 0.8);
}

#[test]
fn test_list_standing_queries_response_deserialization() {
    let json = json!({
        "standing_queries": [
            {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "my-sq",
                "created_at": "2025-01-01T00:00:00Z",
                "match_count": 42
            }
        ]
    });
    let resp: ListStandingQueriesResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.standing_queries.len(), 1);
    assert_eq!(resp.standing_queries[0].name, "my-sq");
    assert_eq!(resp.standing_queries[0].match_count, 42);
}

#[test]
fn test_list_recipes_response_deserialization() {
    let json = json!({
        "recipes": [
            {
                "name": "my-recipe",
                "description": "Test recipe",
                "version": "1.0",
                "num_standing_queries": 2,
                "num_ingest_sources": 1,
                "num_outputs": 1,
                "has_trigger": true
            }
        ],
        "count": 1
    });
    let resp: ListRecipesResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.count, 1);
    assert_eq!(resp.recipes[0].name, "my-recipe");
    assert!(resp.recipes[0].has_trigger);
}

#[test]
fn test_storage_status_response_deserialization() {
    let json = json!({
        "backend": "memory",
        "total_objects": 0,
        "total_size_bytes": 0,
        "hot_objects": 0,
        "warm_objects": 0,
        "cold_objects": 0,
        "note": "Tiered storage not enabled."
    });
    let resp: StorageStatusResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.backend, "memory");
    assert!(resp.note.is_some());
}

#[test]
fn test_system_info_response_deserialization() {
    let json = json!({
        "version": "0.1.0",
        "mode": "single-node",
        "num_shards": 4,
        "max_nodes_per_shard": 1000,
        "rocksdb_path": null,
        "wal_dir": null
    });
    let resp: SystemInfoResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.version, "0.1.0");
    assert_eq!(resp.num_shards, 4);
}

#[test]
fn test_token_response_deserialization() {
    let json = json!({
        "token": "eyJhbGciOiJIUzI1NiJ9...",
        "user_id": "alice",
        "role": "admin"
    });
    let resp: TokenResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.token, "eyJhbGciOiJIUzI1NiJ9...");
    assert_eq!(resp.user_id, "alice");
    assert_eq!(resp.role, "admin");
}

// ============================================================
// Error Handling
// ============================================================

#[test]
fn test_error_status_code_extraction() {
    let err = NexoraClientError::Status {
        status: 404,
        body: "Not Found".to_string(),
    };
    assert_eq!(err.status_code(), Some(404));
    assert!(!err.is_server_error());
    assert!(!err.is_retryable());
}

#[test]
fn test_error_server_error_detection() {
    let err = NexoraClientError::Status {
        status: 503,
        body: "Service Unavailable".to_string(),
    };
    assert!(err.is_server_error());
    assert!(err.is_retryable());
}

#[test]
fn test_error_too_many_requests_is_retryable() {
    let err = NexoraClientError::Status {
        status: 429,
        body: "Too Many Requests".to_string(),
    };
    assert!(err.is_retryable());
    assert!(!err.is_server_error());
}

#[test]
fn test_error_display() {
    let err = NexoraClientError::Status {
        status: 400,
        body: "Bad Request".to_string(),
    };
    assert_eq!(format!("{err}"), "HTTP 400: Bad Request");
}

// ============================================================
// Additional Error Type Conversions & Variants
// ============================================================

#[test]
fn test_error_url_variant() {
    let err = NexoraClientError::Url("bad scheme".into());
    assert_eq!(err.status_code(), None);
    assert!(!err.is_server_error());
    assert!(!err.is_retryable());
    assert_eq!(format!("{err}"), "Invalid URL: bad scheme");
}

#[test]
fn test_error_timeout_variant() {
    let dur = Duration::from_secs(30);
    let err = NexoraClientError::Timeout(dur);
    assert_eq!(err.status_code(), None);
    assert!(!err.is_server_error());
    assert!(err.is_retryable());
}

#[test]
fn test_error_max_retries_variant() {
    let err = NexoraClientError::MaxRetriesExceeded(5);
    assert_eq!(err.status_code(), None);
    assert!(!err.is_server_error());
    assert!(!err.is_retryable());
    assert_eq!(format!("{err}"), "Max retries (5) exceeded");
}

#[test]
fn test_error_other_variant() {
    let err = NexoraClientError::Other("something went wrong".into());
    assert_eq!(err.status_code(), None);
    assert!(!err.is_server_error());
    assert!(!err.is_retryable());
    assert_eq!(format!("{err}"), "something went wrong");
}

#[test]
fn test_error_deserialize_variant() {
    let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let err = NexoraClientError::Deserialize(json_err);
    assert_eq!(err.status_code(), None);
    assert!(!err.is_server_error());
    assert!(!err.is_retryable());
}

#[test]
fn test_error_status_code_boundaries() {
    // 499 is not a server error
    let err_499 = NexoraClientError::Status {
        status: 499,
        body: String::new(),
    };
    assert!(!err_499.is_server_error());
    assert!(!err_499.is_retryable());

    // 500 is a server error and retryable
    let err_500 = NexoraClientError::Status {
        status: 500,
        body: String::new(),
    };
    assert!(err_500.is_server_error());
    assert!(err_500.is_retryable());

    // 599 is a server error and retryable
    let err_599 = NexoraClientError::Status {
        status: 599,
        body: String::new(),
    };
    assert!(err_599.is_server_error());
    assert!(err_599.is_retryable());

    // 400 is not retryable and not a server error
    let err_400 = NexoraClientError::Status {
        status: 400,
        body: String::new(),
    };
    assert!(!err_400.is_server_error());
    assert!(!err_400.is_retryable());

    // 403 is not retryable
    let err_403 = NexoraClientError::Status {
        status: 403,
        body: String::new(),
    };
    assert!(!err_403.is_retryable());
}

// ============================================================
// Builder Edge Cases
// ============================================================

#[test]
fn test_builder_zero_timeout() {
    // timeout=0 should still build successfully (reqwest allows it)
    let client = NexoraClient::builder()
        .timeout(Duration::from_secs(0))
        .build();
    assert!(client.is_ok());
}

#[test]
fn test_builder_zero_retries() {
    // max_retries=0 means no retries — should still build
    let client = NexoraClient::builder().max_retries(0).build();
    assert!(client.is_ok());
}

#[test]
fn test_builder_large_retries() {
    let client = NexoraClient::builder().max_retries(u32::MAX).build();
    assert!(client.is_ok());
}

#[test]
fn test_builder_chained_methods() {
    let client = NexoraClient::builder()
        .base_url("http://10.0.0.1:9999")
        .bearer_token("abc-123")
        .timeout(Duration::from_secs(120))
        .max_retries(10)
        .build()
        .unwrap();
    assert_eq!(client.base_url(), "http://10.0.0.1:9999");
}

#[test]
fn test_builder_default_impl() {
    // Default builder should use the default base URL
    let client = NexoraClientBuilder::default().build().unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080");
}

#[test]
fn test_builder_empty_bearer_token() {
    // Empty string is still a valid token
    let client = NexoraClient::builder().bearer_token("").build();
    assert!(client.is_ok());
}

#[test]
fn test_builder_with_https_url() {
    let client = NexoraClient::builder()
        .base_url("https://nexora.example.com")
        .build()
        .unwrap();
    assert_eq!(client.base_url(), "https://nexora.example.com");
}

#[test]
fn test_builder_with_base_url_with_path() {
    let client = NexoraClient::builder()
        .base_url("http://localhost:8080/api")
        .build()
        .unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080/api");
}

#[test]
fn test_client_default_impl() {
    let client = NexoraClient::default();
    assert_eq!(client.base_url(), "http://localhost:8080");
}

#[test]
fn test_client_with_base_url_trailing_slash() {
    let client = NexoraClient::with_base_url("http://localhost:8080/").unwrap();
    assert_eq!(client.base_url(), "http://localhost:8080/");
}

// ============================================================
// Additional Request Type Serialization
// ============================================================

#[test]
fn test_explain_request_serialization() {
    let req = ExplainRequest {
        query: "MATCH (n) RETURN n".to_string(),
        analyze: true,
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["query"], "MATCH (n) RETURN n");
    assert_eq!(json["analyze"], true);
}

#[test]
fn test_explain_request_default_analyze() {
    let req = ExplainRequest {
        query: "MATCH (n) RETURN n".to_string(),
        analyze: false,
    };
    let json = serde_json::to_value(&req).unwrap();
    // analyze field should still be present (serde default)
    assert_eq!(json["analyze"], false);
}

#[test]
fn test_set_property_request_serialization() {
    let req = SetPropertyRequest {
        value: json!({"name": "Alice", "age": 30}),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["value"]["name"], "Alice");
    assert_eq!(json["value"]["age"], 30);
}

#[test]
fn test_vector_insert_request_serialization() {
    let req = VectorInsertRequest {
        qid: "node-123".to_string(),
        vector: vec![1.0, 2.0, 3.0],
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["qid"], "node-123");
    assert_eq!(json["vector"].as_array().unwrap().len(), 3);
}

#[test]
fn test_link_sq_request_serialization() {
    let req = LinkSqRequest {
        sq_id: "sq-abc".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["sq_id"], "sq-abc");
}

#[test]
fn test_sql_ddl_request_serialization() {
    let req = SqlDdlRequest {
        sql: "CREATE MATERIALIZED VIEW v AS SELECT * FROM t".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["sql"], "CREATE MATERIALIZED VIEW v AS SELECT * FROM t");
}

#[test]
fn test_column_def_request_serialization() {
    let req = ColumnDefRequest {
        name: "speed".to_string(),
        data_type: "float".to_string(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "speed");
    assert_eq!(json["data_type"], "float");
}

#[test]
fn test_recipe_step_def_serialization() {
    let req = RecipeStepDef {
        query: "MATCH (n) RETURN n".to_string(),
        description: None,
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["query"], "MATCH (n) RETURN n");
    // description is None and uses skip_serializing_if
    assert!(json.get("description").is_none() || json["description"].is_null());
}

#[test]
fn test_recipe_step_def_with_description_serialization() {
    let req = RecipeStepDef {
        query: "MATCH (n) RETURN n".to_string(),
        description: Some("A step".to_string()),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["description"], "A step");
}

#[test]
fn test_recipe_trigger_def_serialization() {
    let req = RecipeTriggerDef {
        event_type: "node_created".to_string(),
        filter: Some(json!({"label": "Person"})),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["event_type"], "node_created");
    assert_eq!(json["filter"]["label"], "Person");
}

#[test]
fn test_recipe_trigger_def_no_filter() {
    let req = RecipeTriggerDef {
        event_type: "node_created".to_string(),
        filter: None,
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["event_type"], "node_created");
    assert!(json.get("filter").is_none() || json["filter"].is_null());
}

#[test]
fn test_udf_execute_request_serialization() {
    let req = UdfExecuteRequest {
        name: "my_udf".to_string(),
        args: Some(json!({"x": 1})),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "my_udf");
    assert_eq!(json["args"]["x"], 1);
}

#[test]
fn test_udf_execute_request_no_args() {
    let req = UdfExecuteRequest {
        name: "my_udf".to_string(),
        args: None,
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "my_udf");
    // args is None with skip_serializing_if
    assert!(json.get("args").is_none() || json["args"].is_null());
}

#[test]
fn test_create_recipe_request_minimal() {
    let req = CreateRecipeRequest {
        name: "minimal".to_string(),
        description: None,
        steps: vec![],
        trigger: None,
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "minimal");
    // description should be skipped (None + skip_serializing_if)
    assert!(json.get("description").is_none());
    assert_eq!(json["steps"].as_array().unwrap().len(), 0);
}

#[test]
fn test_create_sq_request_with_labels() {
    let req = CreateSqRequest {
        name: "labeled-sq".to_string(),
        pattern: SqPatternRequest {
            pattern_type: "Node".to_string(),
            key: None,
            condition: None,
            labels: Some(vec!["Person".to_string(), "Active".to_string()]),
        },
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["pattern"]["type"], "Node");
    assert_eq!(json["pattern"]["labels"][0], "Person");
    assert_eq!(json["pattern"]["labels"][1], "Active");
}

#[test]
fn test_create_materialized_view_request_minimal() {
    let req = CreateMaterializedViewRequest {
        name: "simple".to_string(),
        query: "SELECT 1".to_string(),
        refresh_mode: String::new(),
        schema: vec![],
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "simple");
    assert_eq!(json["query"], "SELECT 1");
    assert_eq!(json["refresh_mode"], "");
    assert_eq!(json["schema"].as_array().unwrap().len(), 0);
}

// ============================================================
// Additional Response Type Deserialization
// ============================================================

#[test]
fn test_readiness_response_deserialization() {
    let json = json!({"ready": true, "shards": 4});
    let resp: ReadinessResponse = serde_json::from_value(json).unwrap();
    assert!(resp.ready);
    assert_eq!(resp.shards, 4);
}

#[test]
fn test_liveness_response_deserialization() {
    let json = json!({"alive": true});
    let resp: LivenessResponse = serde_json::from_value(json).unwrap();
    assert!(resp.alive);
}

#[test]
fn test_health_response_with_null_profile() {
    let json = json!({
        "status": "healthy",
        "mode": "single-node",
        "profile": null,
        "active_nodes": 0,
        "shards": 1,
        "standing_queries": 0,
        "readiness": "ready",
        "liveness": "alive",
        "durability": "memory",
        "version": "0.1.0",
        "uptime_seconds": 0
    });
    let resp: HealthResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, "healthy");
    assert!(resp.profile.is_none());
    assert_eq!(resp.uptime_seconds, 0);
}

#[test]
fn test_sql_response_deserialization() {
    let json = json!({
        "columns": ["id", "name"],
        "rows": [{"id": 1, "name": "Alice"}],
        "row_count": 1,
        "query_time_ms": 42,
        "translated_cypher": "MATCH (n) RETURN n",
        "error": null
    });
    let resp: SqlResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.columns, vec!["id", "name"]);
    assert_eq!(resp.row_count, 1);
    assert_eq!(resp.query_time_ms, 42);
    assert!(resp.translated_cypher.is_some());
    assert!(resp.error.is_none());
}

#[test]
fn test_sql_response_deserialization_with_error() {
    let json = json!({
        "columns": [],
        "rows": [],
        "row_count": 0,
        "query_time_ms": 5,
        "translated_cypher": null,
        "error": "syntax error near 'SELEKT'"
    });
    let resp: SqlResponse = serde_json::from_value(json).unwrap();
    assert!(resp.error.is_some());
    assert_eq!(resp.error.unwrap(), "syntax error near 'SELEKT'");
}

#[test]
fn test_explain_response_deserialization() {
    let json = json!({
        "query": "MATCH (n) RETURN n",
        "plan": {
            "start_with": "NodeScan",
            "filters": ["n.age > 18"],
            "cost": 1.5
        },
        "estimated_cost": 1.5,
        "estimated_rows": 100,
        "explanation": "Full scan on nodes",
        "actual_stats": null
    });
    let resp: ExplainResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.query, "MATCH (n) RETURN n");
    assert_eq!(resp.plan.start_with, "NodeScan");
    assert_eq!(resp.plan.filters.len(), 1);
    assert_eq!(resp.estimated_rows, 100);
    assert!(resp.actual_stats.is_none());
}

#[test]
fn test_explain_response_with_actual_stats() {
    let json = json!({
        "query": "MATCH (n) RETURN n",
        "plan": {
            "start_with": "NodeScan",
            "filters": [],
            "cost": 0.5
        },
        "estimated_cost": 0.5,
        "estimated_rows": 10,
        "explanation": "Simple scan",
        "actual_stats": {
            "actual_rows": 8,
            "execution_time_ms": 2.5,
            "nodes_examined": 8
        }
    });
    let resp: ExplainResponse = serde_json::from_value(json).unwrap();
    let stats = resp.actual_stats.unwrap();
    assert_eq!(stats.actual_rows, 8);
    assert!((stats.execution_time_ms - 2.5).abs() < 1e-6);
    assert_eq!(stats.nodes_examined, 8);
}

#[test]
fn test_get_property_response_deserialization() {
    let json = json!({
        "node_id": "61626364",
        "key": "name",
        "value": "Alice",
        "not_found": false
    });
    let resp: GetPropertyResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.node_id, "61626364");
    assert_eq!(resp.key, "name");
    assert_eq!(resp.value, "Alice");
    assert_eq!(resp.not_found, Some(false));
}

#[test]
fn test_get_property_response_not_found() {
    let json = json!({
        "node_id": "61626364",
        "key": "missing",
        "value": null,
        "not_found": true
    });
    let resp: GetPropertyResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.not_found, Some(true));
    assert!(resp.value.is_null());
}

#[test]
fn test_get_edges_response_deserialization() {
    let json = json!({
        "edges": [
            {"edge_type": "KNOWS", "direction": "out", "other": "62636465"},
            {"edge_type": "FOLLOWS", "direction": "in", "other": "65666768"}
        ]
    });
    let resp: GetEdgesResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.edges.len(), 2);
    assert_eq!(resp.edges[0].edge_type, "KNOWS");
    assert_eq!(resp.edges[1].direction, "in");
}

#[test]
fn test_create_sq_response_deserialization() {
    let json = json!({"id": "sq-001", "name": "my-sq"});
    let resp: CreateSqResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.id, "sq-001");
    assert_eq!(resp.name, "my-sq");
}

#[test]
fn test_vector_index_response_deserialization() {
    let json = json!({"status": "indexed", "qid": "node-1", "index_size": 42});
    let resp: VectorIndexResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, "indexed");
    assert_eq!(resp.qid, "node-1");
    assert_eq!(resp.index_size, 42);
}

#[test]
fn test_vector_get_response_deserialization() {
    let json = json!({"qid": "node-1", "vector": [0.1, 0.2, 0.3]});
    let resp: VectorGetResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.qid, "node-1");
    assert_eq!(resp.vector.len(), 3);
}

#[test]
fn test_vector_delete_response_deserialization() {
    let json = json!({"status": "deleted", "qid": "node-1", "index_size": 41});
    let resp: VectorDeleteResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, "deleted");
    assert_eq!(resp.index_size, 41);
}

#[test]
fn test_file_ingest_response_deserialization() {
    let json = json!({"status": "started", "path": "/data/file.json", "name": "file.json"});
    let resp: FileIngestResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, "started");
    assert_eq!(resp.path, "/data/file.json");
    assert_eq!(resp.name, "file.json");
}

#[test]
fn test_list_ingests_response_deserialization() {
    let json = json!({"ingests": ["task1", "task2"], "count": 2});
    let resp: ListIngestsResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.ingests, vec!["task1", "task2"]);
    assert_eq!(resp.count, 2);
}

#[test]
fn test_delete_ingest_response_deserialization() {
    let json = json!({"status": "cancelled", "name": "task1", "error": null});
    let resp: DeleteIngestResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, Some("cancelled".to_string()));
    assert_eq!(resp.name, Some("task1".to_string()));
    assert!(resp.error.is_none());
}

#[test]
fn test_list_streams_response_deserialization() {
    let json = json!({
        "streams": [{
            "name": "kafka-1",
            "source_type": "kafka",
            "topic": "events",
            "brokers": "localhost:9092",
            "started_at": "2025-01-01T00:00:00Z"
        }],
        "count": 1
    });
    let resp: ListStreamsResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.count, 1);
    assert_eq!(resp.streams[0].name, "kafka-1");
    assert_eq!(resp.streams[0].source_type, "kafka");
}

#[test]
fn test_delete_stream_response_deserialization() {
    let json = json!({"status": "stopped", "name": "kafka-1", "error": null});
    let resp: DeleteStreamResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, Some("stopped".to_string()));
}

#[test]
fn test_create_recipe_response_deserialization() {
    let json = json!({"status": "created", "name": "my-recipe", "recipe_count": 1});
    let resp: CreateRecipeResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, "created");
    assert_eq!(resp.name, "my-recipe");
    assert_eq!(resp.recipe_count, 1);
}

#[test]
fn test_delete_recipe_response_deserialization() {
    let json = json!({"status": "deleted", "name": "my-recipe", "error": null});
    let resp: DeleteRecipeResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, Some("deleted".to_string()));
}

#[test]
fn test_execute_recipe_response_deserialization() {
    let json = json!({
        "run_id": "run-001",
        "recipe": "my-recipe",
        "status": "completed",
        "result": {"rows": 5},
        "error": null
    });
    let resp: ExecuteRecipeResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.run_id, "run-001");
    assert_eq!(resp.status, "completed");
    assert!(resp.result.is_some());
}

#[test]
fn test_recipe_run_record_deserialization() {
    let json = json!({
        "run_id": "run-001",
        "recipe_name": "my-recipe",
        "started_at": "2025-01-01T00:00:00Z",
        "finished_at": "2025-01-01T00:01:00Z",
        "status": "completed",
        "result": null,
        "error": null
    });
    let resp: RecipeRunRecord = serde_json::from_value(json).unwrap();
    assert_eq!(resp.run_id, "run-001");
    assert_eq!(resp.recipe_name, "my-recipe");
    assert!(resp.finished_at.is_some());
}

#[test]
fn test_get_recipe_runs_response_deserialization() {
    let json = json!({
        "recipe": "my-recipe",
        "runs": [],
        "total_runs": 0
    });
    let resp: GetRecipeRunsResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.recipe, "my-recipe");
    assert_eq!(resp.total_runs, 0);
    assert!(resp.runs.is_empty());
}

#[test]
fn test_udf_info_deserialization() {
    let json = json!({"name": "my_udf", "language": "native"});
    let resp: UdfInfo = serde_json::from_value(json).unwrap();
    assert_eq!(resp.name, "my_udf");
    assert_eq!(resp.language, "native");
}

#[test]
fn test_create_materialized_view_response_deserialization() {
    let json = json!({"view_id": "mv-001", "name": "high_speed", "status": "created"});
    let resp: CreateMaterializedViewResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.view_id, "mv-001");
    assert_eq!(resp.name, "high_speed");
    assert_eq!(resp.status, "created");
}

#[test]
fn test_materialized_view_summary_deserialization() {
    let json = json!({
        "id": "mv-001",
        "name": "high_speed",
        "query": "MATCH (n) RETURN n",
        "refresh_mode": "incremental",
        "created_at": "2025-01-01T00:00:00Z",
        "last_refreshed": "2025-01-01T01:00:00Z"
    });
    let resp: MaterializedViewSummary = serde_json::from_value(json).unwrap();
    assert_eq!(resp.id, "mv-001");
    assert_eq!(resp.refresh_mode, "incremental");
    assert!(resp.last_refreshed.is_some());
}

#[test]
fn test_materialized_view_summary_no_refresh() {
    let json = json!({
        "id": "mv-002",
        "name": "never_refreshed",
        "query": "MATCH (n) RETURN n",
        "refresh_mode": "manual",
        "created_at": "2025-01-01T00:00:00Z",
        "last_refreshed": null
    });
    let resp: MaterializedViewSummary = serde_json::from_value(json).unwrap();
    assert!(resp.last_refreshed.is_none());
}

#[test]
fn test_materialized_row_response_deserialization() {
    let json = json!({
        "key": "row-1",
        "values": {"col1": 42},
        "version": 3,
        "updated_at": "2025-01-01T00:00:00Z"
    });
    let resp: MaterializedRowResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.key, "row-1");
    assert_eq!(resp.version, 3);
}

#[test]
fn test_query_materialized_view_response_deserialization() {
    let json = json!({
        "view_id": "mv-001",
        "rows": [],
        "count": 0
    });
    let resp: QueryMaterializedViewResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.view_id, "mv-001");
    assert_eq!(resp.count, 0);
}

#[test]
fn test_storage_migrate_response_deserialization() {
    let json = json!({"status": "completed", "migrated_objects": 42, "error": null});
    let resp: StorageMigrateResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.status, Some("completed".to_string()));
    assert_eq!(resp.migrated_objects, Some(42));
}

#[test]
fn test_system_config_response_deserialization() {
    let json = json!({
        "version": "0.1.0",
        "host": "0.0.0.0",
        "port": 8080
    });
    let resp: SystemConfigResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.version, "0.1.0");
    assert_eq!(resp.host, "0.0.0.0");
    assert_eq!(resp.port, 8080);
}

#[test]
fn test_time_travel_response_deserialization() {
    let json = json!({"active_nodes": 100, "time_travel": "enabled"});
    let resp: TimeTravelResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.active_nodes, 100);
    assert_eq!(resp.time_travel, "enabled");
}

#[test]
fn test_write_stats_deserialization() {
    let json = json!({
        "nodes_created": 5,
        "nodes_deleted": 2,
        "properties_set": 10,
        "relationships_created": 3,
        "relationships_deleted": 1,
        "labels_added": 4,
        "labels_removed": 1
    });
    let resp: WriteStats = serde_json::from_value(json).unwrap();
    assert_eq!(resp.nodes_created, 5);
    assert_eq!(resp.nodes_deleted, 2);
    assert_eq!(resp.properties_set, 10);
    assert_eq!(resp.relationships_created, 3);
    assert_eq!(resp.relationships_deleted, 1);
    assert_eq!(resp.labels_added, 4);
    assert_eq!(resp.labels_removed, 1);
}

#[test]
fn test_empty_cypher_response() {
    let json = json!({
        "columns": [],
        "rows": [],
        "error": null,
        "as_of": null
    });
    let resp: CypherResponse = serde_json::from_value(json).unwrap();
    assert!(resp.columns.is_empty());
    assert!(resp.rows.is_empty());
    assert!(resp.write_stats.is_none());
}

// ============================================================
// API Method Signature Verification (compile-time)
// ============================================================

#[test]
#[allow(clippy::let_underscore_future)]
fn test_api_method_signatures_compile() {
    // This test verifies that all API method signatures compile
    // with the expected parameter types. We don't call .await
    // (no server), but the compiler verifies types are correct.

    let client = NexoraClient::new().unwrap();

    // Health methods
    let _ = client.health();
    let _ = client.readiness();
    let _ = client.liveness();

    // Query methods
    let _ = client.execute_cypher("MATCH (n) RETURN n");
    let cypher_req = CypherRequest::new("MATCH (n) RETURN n");
    let _ = client.execute_cypher_request(&cypher_req);
    let _ = client.execute_sql("SELECT * FROM nodes");
    let _ = client.explain_query("MATCH (n) RETURN n", true);

    // Graph property methods
    let _ = client.get_property("node-id", "key");
    let _ = client.set_property("node-id", "key", json!({"val": 1}));

    // Graph edge methods
    let _ = client.get_edges("node-id");
    let _ = client.add_edge("node-id", "KNOWS", "target", "out");

    // Time travel
    let _ = client.time_travel();

    // Standing queries
    let _ = client.list_standing_queries();
    let sq_req = CreateSqRequest {
        name: "test".to_string(),
        pattern: SqPatternRequest {
            pattern_type: "Node".to_string(),
            key: None,
            condition: None,
            labels: None,
        },
    };
    let _ = client.create_standing_query(&sq_req);
    let _ = client.get_standing_query("sq-id");
    let _ = client.delete_standing_query("sq-id");

    // Vector search
    let _ = client.vector_index("node-id", vec![1.0, 2.0]);
    let _ = client.vector_search(vec![1.0, 2.0], 5);
    let _ = client.vector_get("node-id");
    let _ = client.vector_delete("node-id");

    // Ingest
    let _ = client.start_file_ingest("/path/to/file");
    let _ = client.start_file_ingest_with_id_field("/path/to/file", "uuid");
    let _ = client.list_ingests();
    let _ = client.delete_ingest("task-name");

    // Streams
    let _ = client.list_streams();
    let _ = client.start_kafka_stream("localhost:9092", "topic");
    let _ = client.start_kafka_stream_with_group("localhost:9092", "topic", "group-1");
    let _ = client.delete_stream("stream-name");

    // Recipes
    let _ = client.list_recipes();
    let recipe_req = CreateRecipeRequest {
        name: "test".to_string(),
        description: None,
        steps: vec![],
        trigger: None,
    };
    let _ = client.create_recipe(&recipe_req);
    let _ = client.get_recipe("recipe-name");
    let _ = client.delete_recipe("recipe-name");
    let _ = client.execute_recipe("recipe-name");
    let _ = client.get_recipe_runs("recipe-name");

    // UDFs
    let udf_req = UdfRegisterRequest {
        name: "udf".to_string(),
        code: "{}".to_string(),
        language: "native".to_string(),
    };
    let _ = client.udf_register(&udf_req);
    let _ = client.udf_list();
    let udf_exec_req = UdfExecuteRequest {
        name: "udf".to_string(),
        args: None,
    };
    let _ = client.udf_execute(&udf_exec_req);
    let _ = client.udf_delete("udf-name");
    let _ = client.udf_execute_by_name("udf-name", Some(json!({"x": 1})));

    // Materialized views
    let mv_req = CreateMaterializedViewRequest {
        name: "mv".to_string(),
        query: "SELECT 1".to_string(),
        refresh_mode: "incremental".to_string(),
        schema: vec![],
    };
    let _ = client.create_materialized_view(&mv_req);
    let _ = client.list_materialized_views();
    let _ = client.get_materialized_view("view-id");
    let _ = client.drop_materialized_view("view-id");
    let _ = client.query_materialized_view("view-id", Some(10));
    let _ = client.query_materialized_view("view-id", None);
    let _ = client.refresh_materialized_view("view-id");
    let _ = client.link_sq_to_materialized_view("view-id", "sq-id");

    // SQL DDL
    let _ = client.execute_sql_ddl("CREATE MATERIALIZED VIEW v AS SELECT 1");

    // Storage
    let _ = client.storage_status();
    let _ = client.storage_migrate();

    // System
    let _ = client.system_info();
    let _ = client.system_config();

    // Auth
    let _ = client.generate_token("alice", "admin");

    // Cluster
    let _ = client.cluster_stats();
    let _ = client.raft_status();

    // Metrics
    let _ = client.metrics_json();
    let _ = client.metrics_prometheus();

    // If we got here, all method signatures compiled correctly.
}
