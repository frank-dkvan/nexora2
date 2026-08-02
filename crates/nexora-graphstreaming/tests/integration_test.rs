//! Integration tests for GraphStreaming end-to-end pipeline
//!
//! Tests the complete flow: EventLogStore → EventProjector → Graph

#![cfg(all(feature = "event-first", feature = "event-streaming"))]

use nexora_core::GraphService;
use nexora_eventlog::EventLogStore;
use nexora_graphstreaming::{EventProjector, ProjectionRule};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Helper: Create in-memory EventLogStore for testing
async fn setup_event_store() -> Arc<EventLogStore> {
    let config = nexora_eventlog::StorageConfig::local_fs("./test_data/integration");
    Arc::new(
        EventLogStore::new_with_config(config)
            .await
            .expect("Failed to create event store"),
    )
}

/// Helper: Create in-memory GraphService for testing
fn setup_graph_service() -> Arc<GraphService> {
    Arc::new(GraphService::new_in_memory(256, 10_000))
}

/// Test: Full pipeline from event ingestion to graph projection
#[tokio::test]
#[ignore] // Requires external setup
async fn test_full_pipeline() {
    let event_store = setup_event_store().await;
    let graph_service = setup_graph_service();

    // 1. Define projection rule
    let rule_yaml = r#"
projections:
  - name: test_cargo_node
    source_topic: test.cargo
    node:
      id: "{{cargo_id}}"
      labels: ["Cargo"]
      properties:
        status: "{{status}}"
        temperature: "{{temperature}}"
    edge:
      edge_type: LOCATED_AT
      target_id: "{{location_code}}"
      properties:
        arrival_time: "{{event_time}}"
"#;

    let rules = ProjectionRule::parse_yaml(rule_yaml).expect("Failed to parse rule");

    // 2. Create projector
    let projector = EventProjector::new(rules, event_store.clone(), graph_service.clone())
        .expect("Failed to create projector");

    // 3. Start projection (background task)
    projector.start().await.expect("Failed to start projector");

    // 4. Ingest test event
    let event_json = serde_json::json!({
        "cargo_id": "CARGO-123",
        "status": "IN_TRANSIT",
        "temperature": 28,
        "location_code": "LAX",
        "event_time": "2026-08-02T10:00:00Z"
    });

    event_store
        .append_raw_event(
            "test.cargo",
            serde_json::to_string(&event_json).unwrap(),
        )
        .await
        .expect("Failed to append event");

    // 5. Wait for projection (polling-based, needs time)
    sleep(Duration::from_secs(3)).await;

    // 6. Verify node was created in graph
    let node_result = graph_service
        .get_node("CARGO-123")
        .await
        .expect("Failed to get node");

    assert!(node_result.is_some(), "Node CARGO-123 should exist");

    let node = node_result.unwrap();
    assert_eq!(node.labels, vec!["Cargo"]);
    assert_eq!(
        node.properties.get("status").unwrap().as_str(),
        Some("IN_TRANSIT")
    );
    assert_eq!(
        node.properties.get("temperature").unwrap().as_i64(),
        Some(28)
    );

    // 7. Verify edge was created
    let edges = graph_service
        .get_outgoing_edges("CARGO-123")
        .await
        .expect("Failed to get edges");

    assert!(!edges.is_empty(), "Should have at least one edge");

    let located_edge = edges
        .iter()
        .find(|e| e.edge_type == "LOCATED_AT")
        .expect("LOCATED_AT edge should exist");

    assert_eq!(located_edge.target_id, "LAX");

    // 8. Check metrics
    let metrics = projector.get_metrics();
    assert_eq!(metrics.len(), 1);

    let rule_metrics = metrics.get("test_cargo_node").unwrap();
    assert_eq!(rule_metrics.events_processed, 1);
    assert_eq!(rule_metrics.nodes_created, 1);
    assert_eq!(rule_metrics.edges_created, 1);
    assert_eq!(rule_metrics.errors, 0);
}

/// Test: Multiple events projecting concurrently
#[tokio::test]
#[ignore]
async fn test_concurrent_projections() {
    let event_store = setup_event_store().await;
    let graph_service = setup_graph_service();

    let rule_yaml = r#"
projections:
  - name: user_activity
    source_topic: test.users
    node:
      id: "{{user_id}}"
      labels: ["User"]
      properties:
        last_action: "{{action}}"
        timestamp: "{{timestamp}}"
"#;

    let rules = ProjectionRule::parse_yaml(rule_yaml).unwrap();
    let projector = EventProjector::new(rules, event_store.clone(), graph_service.clone()).unwrap();
    projector.start().await.unwrap();

    // Ingest 10 events
    for i in 0..10 {
        let event = serde_json::json!({
            "user_id": format!("USER-{}", i),
            "action": "login",
            "timestamp": format!("2026-08-02T10:00:{:02}Z", i),
        });

        event_store
            .append_raw_event("test.users", serde_json::to_string(&event).unwrap())
            .await
            .unwrap();
    }

    sleep(Duration::from_secs(3)).await;

    // Verify all nodes created
    for i in 0..10 {
        let node_id = format!("USER-{}", i);
        let node = graph_service
            .get_node(&node_id)
            .await
            .unwrap()
            .expect(&format!("Node {} should exist", node_id));

        assert_eq!(node.labels, vec!["User"]);
        assert_eq!(
            node.properties.get("last_action").unwrap().as_str(),
            Some("login")
        );
    }

    let metrics = projector.get_metrics();
    let user_metrics = metrics.get("user_activity").unwrap();
    assert_eq!(user_metrics.events_processed, 10);
    assert_eq!(user_metrics.nodes_created, 10);
}

/// Test: Event filtering
#[tokio::test]
#[ignore]
async fn test_event_filtering() {
    let event_store = setup_event_store().await;
    let graph_service = setup_graph_service();

    let rule_yaml = r#"
projections:
  - name: high_temp_only
    source_topic: test.sensors
    event_filter:
      alert_level: ["HIGH", "CRITICAL"]
    node:
      id: "{{sensor_id}}"
      labels: ["Sensor"]
      properties:
        temperature: "{{temperature}}"
        alert_level: "{{alert_level}}"
"#;

    let rules = ProjectionRule::parse_yaml(rule_yaml).unwrap();
    let projector = EventProjector::new(rules, event_store.clone(), graph_service.clone()).unwrap();
    projector.start().await.unwrap();

    // Ingest 3 events: 2 match filter, 1 doesn't
    let events = vec![
        serde_json::json!({"sensor_id": "S1", "temperature": 85, "alert_level": "HIGH"}),
        serde_json::json!({"sensor_id": "S2", "temperature": 50, "alert_level": "NORMAL"}),
        serde_json::json!({"sensor_id": "S3", "temperature": 95, "alert_level": "CRITICAL"}),
    ];

    for event in events {
        event_store
            .append_raw_event("test.sensors", serde_json::to_string(&event).unwrap())
            .await
            .unwrap();
    }

    sleep(Duration::from_secs(3)).await;

    // Only S1 and S3 should be created
    assert!(graph_service.get_node("S1").await.unwrap().is_some());
    assert!(graph_service.get_node("S2").await.unwrap().is_none());
    assert!(graph_service.get_node("S3").await.unwrap().is_some());

    let metrics = projector.get_metrics();
    let filter_metrics = metrics.get("high_temp_only").unwrap();
    assert_eq!(filter_metrics.nodes_created, 2); // Only 2 passed filter
}

/// Test: Error handling for invalid events
#[tokio::test]
#[ignore]
async fn test_error_handling() {
    let event_store = setup_event_store().await;
    let graph_service = setup_graph_service();

    let rule_yaml = r#"
projections:
  - name: strict_schema
    source_topic: test.strict
    node:
      id: "{{required_field}}"
      labels: ["Test"]
      properties:
        value: "{{value}}"
"#;

    let rules = ProjectionRule::parse_yaml(rule_yaml).unwrap();
    let projector = EventProjector::new(rules, event_store.clone(), graph_service.clone()).unwrap();
    projector.start().await.unwrap();

    // Valid event
    event_store
        .append_raw_event(
            "test.strict",
            r#"{"required_field": "ID-1", "value": 100}"#.to_string(),
        )
        .await
        .unwrap();

    // Invalid event (missing required_field)
    event_store
        .append_raw_event("test.strict", r#"{"value": 200}"#.to_string())
        .await
        .unwrap();

    sleep(Duration::from_secs(3)).await;

    // First event should succeed
    assert!(graph_service.get_node("ID-1").await.unwrap().is_some());

    // Check metrics show 1 error
    let metrics = projector.get_metrics();
    let strict_metrics = metrics.get("strict_schema").unwrap();
    assert_eq!(strict_metrics.events_processed, 2);
    assert_eq!(strict_metrics.nodes_created, 1);
    assert_eq!(strict_metrics.errors, 1);
}
