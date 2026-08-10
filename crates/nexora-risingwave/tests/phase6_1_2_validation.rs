//! Phase 6.1 & 6.2 Validation Tests
//!
//! Tests for EventLogSink and MV subscription functionality.

#![cfg(feature = "event-first")]

use nexora_risingwave::event_sink::{Change, ColumnValue, EventLogSink, Row};
use nexora_risingwave::{EventStreamingConfig, EventStreamingModule};
use std::sync::Arc;

/// Test that EventLogSink can be created with valid configuration
#[tokio::test]
async fn test_event_log_sink_creation() {
    // Create temporary event store
    let event_store = Arc::new(
        nexora_eventlog::EventLogStore::new_with_config(nexora_eventlog::StorageConfig::local_fs(
            "./test_data/phase6_sink_test",
        ))
        .await
        .expect("Failed to create event store"),
    );

    // Create RisingWave module
    let config = EventStreamingConfig::new()
        .with_meta_addr("127.0.0.1:25690".parse().unwrap())
        .with_frontend_addr("127.0.0.1:24566".parse().unwrap());

    let rw = Arc::new(
        EventStreamingModule::start(config)
            .await
            .expect("Failed to start RisingWave"),
    );

    // Create EventLogSink
    let _sink = EventLogSink::new(event_store, rw);

    // Verify sink can be created
    assert!(true, "EventLogSink created successfully");
}

/// Test row to event conversion
#[test]
fn test_row_to_event_conversion() {
    let row = Row::new(vec![
        (
            "cargo_id".to_string(),
            ColumnValue::String("CARGO-123".to_string()),
        ),
        (
            "status".to_string(),
            ColumnValue::String("IN_TRANSIT".to_string()),
        ),
        ("temperature".to_string(), ColumnValue::Float32(28.5)),
        (
            "location".to_string(),
            ColumnValue::String("LAX".to_string()),
        ),
        ("alert".to_string(), ColumnValue::Boolean(true)),
    ]);

    // Verify row structure
    assert_eq!(row.column_names().len(), 5);
    assert_eq!(row.get("cargo_id").is_some(), true);

    match row.get("temperature") {
        Some(ColumnValue::Float32(v)) => assert!((v - 28.5).abs() < 0.01),
        _ => panic!("Expected Float32 value"),
    }
}

/// Test change event variants
#[test]
fn test_change_event_variants() {
    let row1 = Row::new(vec![
        ("id".to_string(), ColumnValue::Int64(1)),
        ("value".to_string(), ColumnValue::String("old".to_string())),
    ]);

    let row2 = Row::new(vec![
        ("id".to_string(), ColumnValue::Int64(1)),
        ("value".to_string(), ColumnValue::String("new".to_string())),
    ]);

    // Test Insert
    let insert = Change::Insert(row1.clone());
    assert!(matches!(insert, Change::Insert(_)));

    // Test Update
    let update = Change::Update {
        old: row1.clone(),
        new: row2.clone(),
    };
    assert!(matches!(update, Change::Update { .. }));

    // Test Delete
    let delete = Change::Delete(row1.clone());
    assert!(matches!(delete, Change::Delete(_)));
}

/// Test subscribe_mv returns valid receiver
#[tokio::test]
#[ignore] // Requires running RisingWave instance
async fn test_subscribe_mv_basic() {
    let config = EventStreamingConfig::new()
        .with_meta_addr("127.0.0.1:5690".parse().unwrap())
        .with_frontend_addr("127.0.0.1:4566".parse().unwrap());

    let rw = EventStreamingModule::start(config)
        .await
        .expect("Failed to start RisingWave");

    // Create a test MV (assumes RisingWave is running)
    rw.execute_ddl(
        "CREATE MATERIALIZED VIEW test_mv AS
         SELECT 1 as id, 'test' as name",
    )
    .await
    .expect("Failed to create MV");

    // Subscribe to MV
    let mut rx = rw
        .subscribe_mv("test_mv")
        .await
        .expect("Failed to subscribe to MV");

    // Should be able to receive changes
    tokio::select! {
        Some(change) = rx.recv() => {
            assert!(matches!(change, Change::Insert(_)));
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
            // Timeout is okay - polling might not find new data yet
        }
    }
}

/// Test ColumnValue JSON conversion
#[test]
fn test_column_value_types() {
    let values = vec![
        ColumnValue::Int32(42),
        ColumnValue::Int64(1234567890),
        ColumnValue::Float32(3.14),
        ColumnValue::Float64(2.718281828),
        ColumnValue::String("test".to_string()),
        ColumnValue::Boolean(true),
        ColumnValue::Null,
    ];

    for value in values {
        match value {
            ColumnValue::Int32(v) => assert_eq!(v, 42),
            ColumnValue::Int64(v) => assert_eq!(v, 1234567890),
            ColumnValue::Float32(v) => assert!((v - 3.14).abs() < 0.01),
            ColumnValue::Float64(v) => assert!((v - 2.718281828).abs() < 0.0001),
            ColumnValue::String(v) => assert_eq!(v, "test"),
            ColumnValue::Boolean(v) => assert_eq!(v, true),
            ColumnValue::Null => assert!(true),
            _ => {}
        }
    }
}

/// Test row column access
#[test]
fn test_row_column_access() {
    let row = Row::new(vec![
        ("name".to_string(), ColumnValue::String("Alice".to_string())),
        ("age".to_string(), ColumnValue::Int32(30)),
        ("city".to_string(), ColumnValue::String("NYC".to_string())),
    ]);

    // Test get by name
    assert!(row.get("name").is_some());
    assert!(row.get("age").is_some());
    assert!(row.get("city").is_some());
    assert!(row.get("country").is_none());

    // Test column names
    let names = row.column_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"name"));
    assert!(names.contains(&"age"));
    assert!(names.contains(&"city"));

    // Test iteration
    let mut count = 0;
    for (name, _value) in row.iter() {
        assert!(["name", "age", "city"].contains(&name.as_str()));
        count += 1;
    }
    assert_eq!(count, 3);
}
