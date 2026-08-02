//! Phase 6.4: End-to-End Event Pipeline Tests
//!
//! Tests the complete pipeline: Kafka → RisingWave → EventLogSink → Iceberg

#![cfg(all(feature = "event-streaming", feature = "event-first"))]

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Test helper: Start EventLogSink and verify it writes to Iceberg
#[tokio::test]
#[ignore] // Requires running Kafka + RisingWave
async fn test_kafka_to_risingwave_to_eventlog() {
    // 1. Setup: Start Kafka, RisingWave, Nexora EventLogStore
    let event_store = setup_event_store().await;
    let rw = setup_risingwave().await;
    let kafka_producer = setup_kafka().await;

    // 2. Create Kafka source in RisingWave
    rw.execute_ddl(
        "CREATE SOURCE raw_events WITH (
            connector = 'kafka',
            topic = 'test.events',
            properties.bootstrap.server = 'localhost:9092'
         ) FORMAT PLAIN ENCODE JSON;"
    ).await.expect("Failed to create source");

    // 3. Create materialized view
    rw.execute_ddl(
        "CREATE MATERIALIZED VIEW enriched_events AS
         SELECT
            data->>'id' as event_id,
            data->>'type' as event_type,
            data->>'value' as value,
            NOW() as processing_time
         FROM raw_events;"
    ).await.expect("Failed to create MV");

    // 4. Start EventLogSink
    let sink = nexora_risingwave::EventLogSink::new(
        event_store.clone(),
        rw.clone(),
    );

    let handle = tokio::spawn(async move {
        sink.start_sync("enriched_events", "test.enriched").await
    });

    // 5. Publish test events to Kafka
    for i in 0..10 {
        kafka_producer.send(&serde_json::json!({
            "id": format!("event-{}", i),
            "type": "test",
            "value": i * 10,
        })).await.expect("Failed to send event");
    }

    // 6. Wait for events to propagate
    sleep(Duration::from_secs(5)).await;

    // 7. Query Iceberg table to verify events arrived
    let events = event_store.query("SELECT * FROM test.enriched ORDER BY event_id")
        .await
        .expect("Failed to query events");

    assert_eq!(events.len(), 10, "Expected 10 events in Iceberg");

    // 8. Verify event content
    let first_event = &events[0];
    assert_eq!(first_event["event_id"], "event-0");
    assert_eq!(first_event["event_type"], "test");
    assert_eq!(first_event["value"], 0);

    // 9. Cleanup
    handle.abort();
    cleanup_test_resources().await;
}

/// Test MV with JOIN enrichment
#[tokio::test]
#[ignore] // Requires running RisingWave
async fn test_mv_enrichment_pipeline() {
    let event_store = setup_event_store().await;
    let rw = setup_risingwave().await;

    // 1. Create lookup table
    rw.execute_ddl(
        "CREATE TABLE location_lookup (
            code VARCHAR PRIMARY KEY,
            city VARCHAR,
            country VARCHAR
         );"
    ).await.expect("Failed to create table");

    // Insert lookup data
    rw.execute_ddl(
        "INSERT INTO location_lookup VALUES
         ('LAX', 'Los Angeles', 'USA'),
         ('JFK', 'New York', 'USA'),
         ('LHR', 'London', 'UK');"
    ).await.expect("Failed to insert data");

    // 2. Create source
    rw.execute_ddl(
        "CREATE SOURCE cargo_events WITH (
            connector = 'kafka',
            topic = 'logistics.cargo',
            properties.bootstrap.server = 'localhost:9092'
         ) FORMAT PLAIN ENCODE JSON;"
    ).await.expect("Failed to create source");

    // 3. Create enriched MV with JOIN
    rw.execute_ddl(
        "CREATE MATERIALIZED VIEW enriched_cargo AS
         SELECT
            c.data->>'cargo_id' as cargo_id,
            c.data->>'location_code' as location_code,
            l.city,
            l.country,
            c.data->>'temperature' as temperature
         FROM cargo_events c
         LEFT JOIN location_lookup l ON c.data->>'location_code' = l.code;"
    ).await.expect("Failed to create MV");

    // 4. Start sink
    let sink = nexora_risingwave::EventLogSink::new(event_store.clone(), rw.clone());
    let handle = tokio::spawn(async move {
        sink.start_sync("enriched_cargo", "logistics.enriched").await
    });

    // 5. Publish event
    let kafka = setup_kafka().await;
    kafka.send(&serde_json::json!({
        "cargo_id": "CARGO-123",
        "location_code": "LAX",
        "temperature": 28,
    })).await.expect("Failed to send");

    sleep(Duration::from_secs(3)).await;

    // 6. Verify enrichment
    let events = event_store.query("SELECT * FROM logistics.enriched WHERE cargo_id = 'CARGO-123'")
        .await
        .expect("Query failed");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["city"], "Los Angeles");
    assert_eq!(events[0]["country"], "USA");

    handle.abort();
    cleanup_test_resources().await;
}

/// Test MV with aggregation (GROUP BY)
#[tokio::test]
#[ignore] // Requires running RisingWave
async fn test_mv_aggregation_pipeline() {
    let event_store = setup_event_store().await;
    let rw = setup_risingwave().await;

    // 1. Create source
    rw.execute_ddl(
        "CREATE SOURCE user_events WITH (
            connector = 'kafka',
            topic = 'analytics.events',
            properties.bootstrap.server = 'localhost:9092'
         ) FORMAT PLAIN ENCODE JSON;"
    ).await.expect("Failed to create source");

    // 2. Create aggregation MV
    rw.execute_ddl(
        "CREATE MATERIALIZED VIEW user_activity_counts AS
         SELECT
            data->>'user_id' as user_id,
            COUNT(*) as event_count,
            MAX(data->>'timestamp') as last_seen
         FROM user_events
         GROUP BY data->>'user_id';"
    ).await.expect("Failed to create MV");

    // 3. Start sink
    let sink = nexora_risingwave::EventLogSink::new(event_store.clone(), rw.clone());
    let handle = tokio::spawn(async move {
        sink.start_sync("user_activity_counts", "analytics.counts").await
    });

    // 4. Publish multiple events for same user
    let kafka = setup_kafka().await;
    for i in 0..5 {
        kafka.send(&serde_json::json!({
            "user_id": "user-42",
            "action": "click",
            "timestamp": format!("2026-08-02T10:00:0{}Z", i),
        })).await.expect("Failed to send");
    }

    sleep(Duration::from_secs(3)).await;

    // 5. Verify aggregation
    let events = event_store.query("SELECT * FROM analytics.counts WHERE user_id = 'user-42'")
        .await
        .expect("Query failed");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_count"], 5);

    handle.abort();
    cleanup_test_resources().await;
}

/// Test sink restart recovery (no data loss)
#[tokio::test]
#[ignore] // Requires running RisingWave
async fn test_sink_restart_recovery() {
    let event_store = setup_event_store().await;
    let rw = setup_risingwave().await;

    // 1. Create MV
    rw.execute_ddl(
        "CREATE SOURCE test_events WITH (
            connector = 'kafka',
            topic = 'test.restart',
            properties.bootstrap.server = 'localhost:9092'
         ) FORMAT PLAIN ENCODE JSON;"
    ).await.expect("Failed to create source");

    rw.execute_ddl(
        "CREATE MATERIALIZED VIEW test_mv AS
         SELECT data->>'id' as id, data->>'value' as value FROM test_events;"
    ).await.expect("Failed to create MV");

    // 2. Start sink
    let sink1 = nexora_risingwave::EventLogSink::new(event_store.clone(), rw.clone());
    let handle1 = tokio::spawn(async move {
        sink1.start_sync("test_mv", "test.restart_topic").await
    });

    // 3. Publish first batch
    let kafka = setup_kafka().await;
    for i in 0..5 {
        kafka.send(&serde_json::json!({"id": i, "value": i * 10}))
            .await.expect("Failed to send");
    }

    sleep(Duration::from_secs(2)).await;

    // 4. Stop sink
    handle1.abort();
    sleep(Duration::from_secs(1)).await;

    // 5. Publish second batch while sink is down
    for i in 5..10 {
        kafka.send(&serde_json::json!({"id": i, "value": i * 10}))
            .await.expect("Failed to send");
    }

    // 6. Restart sink
    let sink2 = nexora_risingwave::EventLogSink::new(event_store.clone(), rw.clone());
    let handle2 = tokio::spawn(async move {
        sink2.start_sync("test_mv", "test.restart_topic").await
    });

    sleep(Duration::from_secs(3)).await;

    // 7. Verify all events present (no data loss)
    let events = event_store.query("SELECT COUNT(*) as count FROM test.restart_topic")
        .await
        .expect("Query failed");

    assert_eq!(events[0]["count"], 10, "Expected all 10 events");

    handle2.abort();
    cleanup_test_resources().await;
}

/// Test concurrent syncs (multiple MVs)
#[tokio::test]
#[ignore] // Requires running RisingWave
async fn test_concurrent_syncs() {
    let event_store = setup_event_store().await;
    let rw = setup_risingwave().await;

    // 1. Create 3 MVs
    for i in 0..3 {
        rw.execute_ddl(&format!(
            "CREATE SOURCE test_source_{} WITH (
                connector = 'kafka',
                topic = 'test.concurrent.{}',
                properties.bootstrap.server = 'localhost:9092'
             ) FORMAT PLAIN ENCODE JSON;", i, i
        )).await.expect("Failed to create source");

        rw.execute_ddl(&format!(
            "CREATE MATERIALIZED VIEW test_mv_{} AS
             SELECT data->>'id' as id FROM test_source_{};", i, i
        )).await.expect("Failed to create MV");
    }

    // 2. Start 3 sinks concurrently
    let mut handles = vec![];
    for i in 0..3 {
        let sink = nexora_risingwave::EventLogSink::new(
            event_store.clone(),
            rw.clone(),
        );
        let handle = tokio::spawn(async move {
            sink.start_sync(
                &format!("test_mv_{}", i),
                &format!("test.topic_{}", i),
            ).await
        });
        handles.push(handle);
    }

    // 3. Publish events to all 3 topics
    let kafka = setup_kafka().await;
    for i in 0..3 {
        for j in 0..5 {
            kafka.send_to_topic(
                &format!("test.concurrent.{}", i),
                &serde_json::json!({"id": j}),
            ).await.expect("Failed to send");
        }
    }

    sleep(Duration::from_secs(3)).await;

    // 4. Verify all sinks processed events
    for i in 0..3 {
        let events = event_store.query(&format!(
            "SELECT COUNT(*) as count FROM test.topic_{}", i
        )).await.expect("Query failed");

        assert_eq!(events[0]["count"], 5, "Expected 5 events for topic {}", i);
    }

    // 5. Cleanup
    for handle in handles {
        handle.abort();
    }
    cleanup_test_resources().await;
}

/// Test HTTP API: Start sync endpoint
#[tokio::test]
async fn test_start_mv_sync_api() {
    let app = setup_test_app().await;

    let response = reqwest::Client::new()
        .post("http://localhost:8080/api/event-streaming/sync/start")
        .json(&serde_json::json!({
            "mv_name": "test_mv",
            "topic": "test.topic"
        }))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.expect("Parse failed");
    assert_eq!(body["status"], "started");
    assert_eq!(body["mv_name"], "test_mv");

    cleanup_test_app(app).await;
}

/// Test HTTP API: Stop sync endpoint
#[tokio::test]
async fn test_stop_mv_sync_api() {
    let app = setup_test_app().await;

    // Start sync first
    reqwest::Client::new()
        .post("http://localhost:8080/api/event-streaming/sync/start")
        .json(&serde_json::json!({
            "mv_name": "test_mv",
            "topic": "test.topic"
        }))
        .send()
        .await
        .expect("Start failed");

    // Stop sync
    let response = reqwest::Client::new()
        .post("http://localhost:8080/api/event-streaming/sync/stop")
        .json(&serde_json::json!({
            "mv_name": "test_mv"
        }))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.expect("Parse failed");
    assert_eq!(body["status"], "stopped");

    cleanup_test_app(app).await;
}

/// Test HTTP API: Status endpoint
#[tokio::test]
async fn test_sync_status_api() {
    let app = setup_test_app().await;

    // Start 2 syncs
    for i in 0..2 {
        reqwest::Client::new()
            .post("http://localhost:8080/api/event-streaming/sync/start")
            .json(&serde_json::json!({
                "mv_name": format!("test_mv_{}", i),
                "topic": format!("test.topic_{}", i)
            }))
            .send()
            .await
            .expect("Start failed");
    }

    // Check status
    let response = reqwest::Client::new()
        .get("http://localhost:8080/api/event-streaming/sync/status")
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.expect("Parse failed");
    assert_eq!(body["total_count"], 2);
    assert_eq!(body["active_syncs"].as_array().unwrap().len(), 2);

    cleanup_test_app(app).await;
}

/// Test duplicate sync prevention
#[tokio::test]
async fn test_duplicate_sync_prevention() {
    let app = setup_test_app().await;

    // Start first sync
    let response1 = reqwest::Client::new()
        .post("http://localhost:8080/api/event-streaming/sync/start")
        .json(&serde_json::json!({
            "mv_name": "test_mv",
            "topic": "test.topic"
        }))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response1.status(), 200);

    // Try to start duplicate sync
    let response2 = reqwest::Client::new()
        .post("http://localhost:8080/api/event-streaming/sync/start")
        .json(&serde_json::json!({
            "mv_name": "test_mv",
            "topic": "test.topic"
        }))
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response2.status(), 400);

    let body: serde_json::Value = response2.json().await.expect("Parse failed");
    assert!(body["error"].as_str().unwrap().contains("already running"));

    cleanup_test_app(app).await;
}

// ============================================================================
// Test Helpers
// ============================================================================

async fn setup_event_store() -> Arc<nexora_eventlog::EventLogStore> {
    Arc::new(
        nexora_eventlog::EventLogStore::new_with_config(
            nexora_eventlog::StorageConfig::local_fs("./test_data/phase6_4")
        )
        .await
        .expect("Failed to create event store")
    )
}

async fn setup_risingwave() -> Arc<nexora_risingwave::EventStreamingModule> {
    let config = nexora_risingwave::EventStreamingConfig::new()
        .with_meta_addr("127.0.0.1:5690".parse().unwrap())
        .with_frontend_addr("127.0.0.1:4566".parse().unwrap());

    Arc::new(
        nexora_risingwave::EventStreamingModule::start(config)
            .await
            .expect("Failed to start RisingWave")
    )
}

struct KafkaProducer {
    // Mock implementation
}

impl KafkaProducer {
    async fn send(&self, _event: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Implement actual Kafka producer
        Ok(())
    }

    async fn send_to_topic(
        &self,
        _topic: &str,
        _event: &serde_json::Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Implement actual Kafka producer with topic selection
        Ok(())
    }
}

async fn setup_kafka() -> KafkaProducer {
    // TODO: Implement actual Kafka setup
    KafkaProducer {}
}

async fn setup_test_app() -> () {
    // TODO: Spawn nexora-app server for API tests
}

async fn cleanup_test_app(_app: ()) {
    // TODO: Cleanup test app
}

async fn cleanup_test_resources() {
    // TODO: Cleanup Kafka topics, RisingWave MVs, Iceberg tables
}
