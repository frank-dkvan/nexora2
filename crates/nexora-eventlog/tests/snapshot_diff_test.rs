//! Tests for incremental snapshot diff streaming
//!
//! Validates that stream_topic uses snapshot diff API correctly

use super::*;
use std::sync::Arc;

#[tokio::test]
async fn test_snapshot_delta_read() {
    let config = StorageConfig::local_fs("./test_data/snapshot_delta");
    let store = EventLogStore::new_with_config(config)
        .await
        .expect("Failed to create store");

    // Create table and append initial events
    let events1 = vec![
        RawEvent {
            event_time: 1000000,
            ingest_time: 1000000,
            source: "test".to_string(),
            topic: "delta_test".to_string(),
            partition: 0,
            offset: 0,
            key: None,
            data: r#"{"id": "1", "value": 100}"#.to_string(),
        },
        RawEvent {
            event_time: 2000000,
            ingest_time: 2000000,
            source: "test".to_string(),
            topic: "delta_test".to_string(),
            partition: 0,
            offset: 1,
            key: None,
            data: r#"{"id": "2", "value": 200}"#.to_string(),
        },
    ];

    store
        .append_batch("delta_test", events1)
        .await
        .expect("Failed to append batch 1");

    // Get snapshot 1
    let table1 = store.load_table("delta_test").await.unwrap();
    let snapshot1_id = table1
        .metadata()
        .current_snapshot()
        .unwrap()
        .snapshot_id();

    // Append more events
    let events2 = vec![
        RawEvent {
            event_time: 3000000,
            ingest_time: 3000000,
            source: "test".to_string(),
            topic: "delta_test".to_string(),
            partition: 0,
            offset: 2,
            key: None,
            data: r#"{"id": "3", "value": 300}"#.to_string(),
        },
    ];

    store
        .append_batch("delta_test", events2)
        .await
        .expect("Failed to append batch 2");

    // Get snapshot 2
    let table2 = store.load_table("delta_test").await.unwrap();
    let snapshot2_id = table2
        .metadata()
        .current_snapshot()
        .unwrap()
        .snapshot_id();

    assert_ne!(snapshot1_id, snapshot2_id);

    // Read delta between snapshot1 and snapshot2
    let delta_batches = store
        .read_snapshot_delta("delta_test", Some(snapshot1_id), snapshot2_id)
        .await
        .expect("Failed to read delta");

    // Should only contain 1 event (id=3)
    let mut total_rows = 0;
    for batch in &delta_batches {
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 1, "Delta should contain only 1 new row");

    // Full read should contain 3 events
    let full_batches = store
        .read_snapshot_delta("delta_test", None, snapshot2_id)
        .await
        .expect("Failed to read full");

    let mut full_rows = 0;
    for batch in &full_batches {
        full_rows += batch.num_rows();
    }
    assert_eq!(full_rows, 3, "Full read should contain all 3 rows");
}

#[tokio::test]
async fn test_incremental_streaming() {
    let config = StorageConfig::local_fs("./test_data/incremental_stream");
    let store = Arc::new(
        EventLogStore::new_with_config(config)
            .await
            .expect("Failed to create store"),
    );

    // Start streaming
    let mut rx = store
        .stream_topic("stream_test")
        .await
        .expect("Failed to start stream");

    // Append first batch
    let events1 = vec![RawEvent {
        event_time: 1000000,
        ingest_time: 1000000,
        source: "test".to_string(),
        topic: "stream_test".to_string(),
        partition: 0,
        offset: 0,
        key: None,
        data: r#"{"id": "1", "value": 100}"#.to_string(),
    }];

    store
        .append_batch("stream_test", events1)
        .await
        .expect("Failed to append");

    // Should receive 1 event
    tokio::time::timeout(tokio::time::Duration::from_secs(3), rx.recv())
        .await
        .expect("Timeout waiting for event")
        .expect("Channel closed");

    // Append second batch
    let events2 = vec![RawEvent {
        event_time: 2000000,
        ingest_time: 2000000,
        source: "test".to_string(),
        topic: "stream_test".to_string(),
        partition: 0,
        offset: 1,
        key: None,
        data: r#"{"id": "2", "value": 200}"#.to_string(),
    }];

    store
        .append_batch("stream_test", events2)
        .await
        .expect("Failed to append");

    // Should receive only 1 new event (not both)
    let event2 = tokio::time::timeout(tokio::time::Duration::from_secs(3), rx.recv())
        .await
        .expect("Timeout waiting for second event")
        .expect("Channel closed");

    assert!(event2.data.contains("\"id\": \"2\""));

    // Should not receive duplicate events
    let no_event = tokio::time::timeout(tokio::time::Duration::from_secs(2), rx.recv()).await;
    assert!(no_event.is_err(), "Should not receive more events");
}

#[tokio::test]
async fn test_snapshot_expired_fallback() {
    let config = StorageConfig::local_fs("./test_data/expired_snapshot");
    let store = Arc::new(
        EventLogStore::new_with_config(config)
            .await
            .expect("Failed to create store"),
    );

    // Create table with initial data
    let events = vec![RawEvent {
        event_time: 1000000,
        ingest_time: 1000000,
        source: "test".to_string(),
        topic: "expired_test".to_string(),
        partition: 0,
        offset: 0,
        key: None,
        data: r#"{"id": "1"}"#.to_string(),
    }];

    store
        .append_batch("expired_test", events)
        .await
        .expect("Failed to append");

    // Try to read delta with non-existent from_snapshot
    let table = store.load_table("expired_test").await.unwrap();
    let current_snapshot = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .snapshot_id();

    let invalid_snapshot = 999999i64;

    // Should return error for invalid snapshot
    let result = store
        .read_snapshot_delta("expired_test", Some(invalid_snapshot), current_snapshot)
        .await;

    assert!(result.is_err(), "Should fail with expired snapshot");
    assert!(
        result.unwrap_err().to_string().contains("expired"),
        "Error should mention expired snapshot"
    );
}
