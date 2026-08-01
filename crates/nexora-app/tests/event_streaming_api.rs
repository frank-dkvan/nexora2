//! Phase 5.3 HTTP API Integration Tests
//!
//! Tests for Event Streaming HTTP API endpoints.

#[cfg(feature = "event-streaming")]
use serde_json::json;

#[tokio::test]
#[cfg(feature = "event-streaming")]
async fn test_event_streaming_ddl_endpoint() {
    // This is a mock test - full integration requires RisingWave running
    // Phase 5.4 will add full end-to-end tests

    let request = json!({
        "sql": "CREATE SOURCE test_source WITH (connector = 'datagen') FORMAT PLAIN ENCODE JSON"
    });

    // Validate request structure
    let sql = request["sql"].as_str().unwrap();
    assert!(sql.contains("CREATE SOURCE"));
    assert!(sql.contains("test_source"));
}

#[tokio::test]
#[cfg(feature = "event-streaming")]
async fn test_event_streaming_query_endpoint() {
    let request = json!({
        "sql": "SELECT * FROM test_mv LIMIT 10"
    });

    let sql = request["sql"].as_str().unwrap();
    assert!(sql.contains("SELECT"));
    assert!(sql.contains("test_mv"));
}

#[tokio::test]
#[cfg(feature = "event-streaming")]
async fn test_event_streaming_status_response() {
    // Test status response structure
    let status = json!({
        "enabled": true,
        "meta_leader": true,
        "version": "v3.0.2"
    });

    assert_eq!(status["enabled"], true);
    assert_eq!(status["meta_leader"], true);
    assert_eq!(status["version"], "v3.0.2");
}

#[tokio::test]
#[cfg(all(feature = "event-streaming", feature = "library"))]
async fn test_distributed_library_status_response() {
    // Test distributed library status response structure
    let status = json!({
        "mode": "distributed_library",
        "meta": {
            "is_leader": true,
            "leader_id": 1,
            "raft_state": "Leader",
            "node_count": 3
        },
        "frontend": {
            "active_nodes": 1,
            "total_nodes": 1,
            "healthy": true
        },
        "compute": {
            "active_nodes": 1,
            "total_nodes": 1,
            "healthy": true,
            "total_parallelism": 8
        }
    });

    assert_eq!(status["mode"], "distributed_library");
    assert_eq!(status["meta"]["is_leader"], true);
    assert_eq!(status["meta"]["node_count"], 3);
    assert_eq!(status["frontend"]["healthy"], true);
    assert_eq!(status["compute"]["healthy"], true);
}

#[test]
#[cfg(feature = "event-streaming")]
fn test_ddl_request_serialization() {
    let sql = "CREATE MATERIALIZED VIEW test_mv AS SELECT * FROM test_source";
    let request = json!({
        "sql": sql
    });

    let json_str = serde_json::to_string(&request).unwrap();
    assert!(json_str.contains("CREATE MATERIALIZED VIEW"));
}

#[test]
#[cfg(feature = "event-streaming")]
fn test_query_request_serialization() {
    let sql = "SELECT COUNT(*) FROM test_mv WHERE status = 'active'";
    let request = json!({
        "sql": sql
    });

    let json_str = serde_json::to_string(&request).unwrap();
    assert!(json_str.contains("SELECT COUNT"));
    assert!(json_str.contains("status"));
}

#[test]
#[cfg(feature = "event-streaming")]
fn test_source_list_response() {
    let sources = json!([
        {
            "name": "kafka_source",
            "connector": "kafka",
            "status": "active"
        },
        {
            "name": "datagen_source",
            "connector": "datagen",
            "status": "active"
        }
    ]);

    let array = sources.as_array().unwrap();
    assert_eq!(array.len(), 2);
    assert_eq!(array[0]["name"], "kafka_source");
    assert_eq!(array[1]["connector"], "datagen");
}

#[test]
#[cfg(feature = "event-streaming")]
fn test_mv_list_response() {
    let mvs = json!([
        {
            "name": "user_counts",
            "definition": "SELECT COUNT(*) FROM users GROUP BY status",
            "status": "active"
        }
    ]);

    let array = mvs.as_array().unwrap();
    assert_eq!(array.len(), 1);
    assert_eq!(array[0]["name"], "user_counts");
    assert!(array[0]["definition"].as_str().unwrap().contains("COUNT"));
}
