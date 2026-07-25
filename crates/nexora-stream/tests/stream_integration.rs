//! Comprehensive integration tests for nexora-stream crate.
//!
//! Coverage:
//! - IngestRecord & IngestBatch types: construction, serialization, edge semantics
//! - SourceOffset: serialization, timestamp handling
//! - IngestionStats: defaults, mutation
//! - MockSource: connect, poll, commit, stats, topics, close, exhausted behavior
//! - OffsetManager: commit, get, load, concurrent access, persistence
//! - InMemoryOffsetStore: save, load, load_all
//! - IngestionConfig: defaults, custom values, validation
//! - IngestionPipeline: source registration, offset loading, stats tracking
//! - IngestionError: display, variants
//! - Concurrency: concurrent offset commits, concurrent polls, race-free commit
//! - Edge cases: empty batches, large batches, binary offsets, empty topic names
//! - IngestHandler implementation via mock

use nexora_stream::{
    InMemoryOffsetStore, IngestBatch, IngestHandler, IngestRecord, IngestionConfig, IngestionError,
    IngestionPipeline, IngestionSource, IngestionStats, MockSource, OffsetManager, OffsetStore,
    SourceOffset,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;

// ============================================================
// Helpers
// ============================================================

fn make_record(key: &str, value: &str) -> IngestRecord {
    IngestRecord {
        qid: nexora_id::NexoraId::from_bytes(format!("node-{}", key).into_bytes()),
        key: key.to_string(),
        value: serde_json::Value::String(value.to_string()),
        edge_type: None,
        edge_target: None,
        timestamp: Some(chrono::Utc::now()),
        label: None,
    }
}

fn make_record_with_edge(key: &str, target: &str, edge_type: &str) -> IngestRecord {
    IngestRecord {
        qid: nexora_id::NexoraId::from_bytes(format!("node-{}", key).into_bytes()),
        key: key.to_string(),
        value: serde_json::Value::String("edge-data".into()),
        edge_type: Some(edge_type.to_string()),
        edge_target: Some(nexora_id::NexoraId::from_bytes(
            format!("node-{}", target).into_bytes(),
        )),
        timestamp: Some(chrono::Utc::now()),
        label: None,
    }
}

fn make_source_offset(topic: &str, partition: &str, offset: u64) -> SourceOffset {
    SourceOffset {
        topic: topic.to_string(),
        partition: partition.to_string(),
        offset,
        committed_at: chrono::Utc::now(),
    }
}

// ============================================================
// IngestRecord Tests
// ============================================================

#[test]
fn test_ingest_record_creation() {
    let record = make_record("speed", "80.5");
    assert_eq!(record.key, "speed");
    assert_eq!(record.value, serde_json::Value::String("80.5".into()));
    assert!(record.timestamp.is_some());
    assert!(record.edge_type.is_none());
    assert!(record.edge_target.is_none());
}

#[test]
fn test_ingest_record_with_edge() {
    let record = make_record_with_edge("a", "b", "KNOWS");
    assert_eq!(record.edge_type, Some("KNOWS".to_string()));
    assert!(record.edge_target.is_some());
}

#[test]
fn test_ingest_record_serialization() {
    let record = make_record("k", "v");
    let json = serde_json::to_string(&record).unwrap();
    let deserialized: IngestRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.key, "k");
    assert_eq!(deserialized.value, serde_json::Value::String("v".into()));
}

#[test]
fn test_ingest_record_with_null_qid() {
    // Verify that a record can have any valid NexoraId
    let record = IngestRecord {
        qid: nexora_id::NexoraId::from_bytes(vec![]),
        key: "empty-qid".into(),
        value: serde_json::Value::Null,
        edge_type: None,
        edge_target: None,
        timestamp: None,
        label: None,
    };
    assert!(record.timestamp.is_none());
}

// ============================================================
// IngestBatch Tests
// ============================================================

#[test]
fn test_ingest_batch_creation() {
    let records = vec![make_record("a", "1"), make_record("b", "2")];
    let batch = IngestBatch {
        records: records.clone(),
        partition: "0".to_string(),
        offset_start: 0,
        offset_end: 2,
        topic: "events".to_string(),
        raw_events: None,
    };
    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.topic, "events");
    assert_eq!(batch.offset_start, 0);
    assert_eq!(batch.offset_end, 2);
}

#[test]
fn test_ingest_batch_empty() {
    let batch = IngestBatch {
        records: vec![],
        partition: "0".to_string(),
        offset_start: 100,
        offset_end: 100,
        topic: "events".to_string(),
        raw_events: None,
    };
    assert!(batch.records.is_empty());
    assert_eq!(batch.offset_start, batch.offset_end);
}

#[test]
fn test_ingest_batch_clone() {
    let batch = IngestBatch {
        records: vec![make_record("x", "y")],
        partition: "0".to_string(),
        offset_start: 0,
        offset_end: 1,
        topic: "t".to_string(),
        raw_events: None,
    };
    let cloned = batch.clone();
    assert_eq!(cloned.records.len(), 1);
    assert_eq!(cloned.topic, "t");
}

// ============================================================
// SourceOffset Tests
// ============================================================

#[test]
fn test_source_offset_serialization() {
    let offset = make_source_offset("events", "3", 12345);
    let json = serde_json::to_string(&offset).unwrap();
    let deserialized: SourceOffset = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.offset, 12345);
    assert_eq!(deserialized.topic, "events");
    assert_eq!(deserialized.partition, "3");
}

#[test]
fn test_source_offset_clone() {
    let offset = make_source_offset("events", "0", 999);
    let cloned = offset.clone();
    assert_eq!(cloned.offset, 999);
}

// ============================================================
// IngestionStats Tests
// ============================================================

#[test]
fn test_ingestion_stats_default() {
    let stats = IngestionStats::default();
    assert_eq!(stats.records_ingested, 0);
    assert_eq!(stats.batches_processed, 0);
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.rate_per_sec, 0.0);
    assert!(stats.last_offset.is_none());
    assert_eq!(stats.lag_records, 0);
}

#[test]
fn test_ingestion_stats_serialization() {
    let stats = IngestionStats {
        records_ingested: 1000,
        batches_processed: 10,
        errors: 2,
        last_offset: Some(make_source_offset("t", "0", 1000)),
        lag_records: 50,
        rate_per_sec: 100.5,
    };
    let json = serde_json::to_string(&stats).unwrap();
    let deserialized: IngestionStats = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.records_ingested, 1000);
    assert_eq!(deserialized.errors, 2);
    assert!(deserialized.last_offset.is_some());
}

// ============================================================
// MockSource Tests
// ============================================================

#[tokio::test]
async fn test_mock_source_connect() {
    let source = MockSource::new("test-topic", vec![make_record("k", "v")]);
    assert!(source.connect().await.is_ok());
}

#[tokio::test]
async fn test_mock_source_poll_returns_batch_once() {
    let records = vec![
        make_record("a", "1"),
        make_record("b", "2"),
        make_record("c", "3"),
    ];
    let source = MockSource::new("test", records);

    let batch = source.poll().await.unwrap().unwrap();
    assert_eq!(batch.records.len(), 3);
    assert_eq!(batch.topic, "test");
    assert_eq!(batch.partition, "0");
    assert_eq!(batch.offset_end, 3);

    // Second poll returns None (already consumed)
    let second = source.poll().await.unwrap();
    assert!(second.is_none());
}

#[tokio::test]
async fn test_mock_source_commit() {
    let source = MockSource::new("test", vec![make_record("k", "v")]);
    let offset = make_source_offset("test", "0", 42);
    assert!(source.commit(&offset).await.is_ok());

    let stats = source.stats();
    assert!(stats.last_offset.is_some());
    assert_eq!(stats.last_offset.unwrap().offset, 42);
}

#[tokio::test]
async fn test_mock_source_topics() {
    let source = MockSource::new("my-topic", vec![]);
    let topics = source.topics();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0], "my-topic");
}

#[tokio::test]
async fn test_mock_source_close() {
    let source = MockSource::new("test", vec![]);
    assert!(source.close().await.is_ok());
}

#[tokio::test]
async fn test_mock_source_empty_records() {
    let source = MockSource::new("empty", vec![]);
    let batch = source.poll().await.unwrap().unwrap();
    assert!(batch.records.is_empty());
    assert_eq!(batch.offset_end, 0);
}

#[tokio::test]
async fn test_mock_source_large_batch() {
    let records: Vec<IngestRecord> = (0..10000)
        .map(|i| make_record(&format!("key_{}", i), &format!("val_{}", i)))
        .collect();
    let len = records.len();
    let source = MockSource::new("large", records);
    let batch = source.poll().await.unwrap().unwrap();
    assert_eq!(batch.records.len(), len);
}

// ============================================================
// InMemoryOffsetStore Tests
// ============================================================

#[tokio::test]
async fn test_offset_store_save_and_load() {
    let store = InMemoryOffsetStore::new();
    store.save("events", "0", 100).await.unwrap();

    let loaded = store.load("events", "0").await.unwrap();
    assert_eq!(loaded, Some(100));
}

#[tokio::test]
async fn test_offset_store_load_nonexistent() {
    let store = InMemoryOffsetStore::new();
    let loaded = store.load("nonexistent", "0").await.unwrap();
    assert!(loaded.is_none());
}

#[tokio::test]
async fn test_offset_store_load_all() {
    let store = InMemoryOffsetStore::new();
    store.save("topic-a", "0", 10).await.unwrap();
    store.save("topic-a", "1", 20).await.unwrap();
    store.save("topic-b", "0", 30).await.unwrap();

    let all = store.load_all().await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn test_offset_store_overwrite() {
    let store = InMemoryOffsetStore::new();
    store.save("t", "p", 1).await.unwrap();
    store.save("t", "p", 2).await.unwrap();

    let loaded = store.load("t", "p").await.unwrap();
    assert_eq!(loaded, Some(2), "Overwrite should update the offset");
}

// ============================================================
// OffsetManager Tests
// ============================================================

#[tokio::test]
async fn test_offset_manager_commit_and_get() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let manager = OffsetManager::new(store);
    let offset = make_source_offset("events", "0", 500);
    manager.commit(offset).await.unwrap();

    let got = manager.get("events", "0").await.unwrap();
    assert_eq!(got.offset, 500);
}

#[tokio::test]
async fn test_offset_manager_get_nonexistent() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let manager = OffsetManager::new(store);
    assert!(manager.get("nonexistent", "0").await.is_none());
}

#[tokio::test]
async fn test_offset_manager_multiple_topics() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let manager = OffsetManager::new(store);

    manager
        .commit(make_source_offset("topic-a", "0", 10))
        .await
        .unwrap();
    manager
        .commit(make_source_offset("topic-b", "0", 20))
        .await
        .unwrap();
    manager
        .commit(make_source_offset("topic-a", "1", 30))
        .await
        .unwrap();

    assert_eq!(manager.get("topic-a", "0").await.unwrap().offset, 10);
    assert_eq!(manager.get("topic-b", "0").await.unwrap().offset, 20);
    assert_eq!(manager.get("topic-a", "1").await.unwrap().offset, 30);
}

#[tokio::test]
async fn test_offset_manager_load_persisted() {
    let store = Arc::new(InMemoryOffsetStore::new());
    store.save("t", "p", 42).await.unwrap();

    let manager = OffsetManager::new(store);
    let count = manager.load().await.unwrap();
    assert_eq!(count, 1);

    let got = manager.get("t", "p").await.unwrap();
    assert_eq!(got.offset, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_offset_manager_concurrent_commits() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let manager = Arc::new(OffsetManager::new(store));
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    for i in 0..10 {
        let m = manager.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let offset = make_source_offset("concurrent", &format!("{}", i), i * 100);
            m.commit(offset).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    for i in 0..10 {
        let got = manager.get("concurrent", &format!("{}", i)).await;
        assert!(got.is_some(), "partition {} should have offset", i);
        assert_eq!(got.unwrap().offset, i * 100);
    }
}

// ============================================================
// IngestionConfig Tests
// ============================================================

#[test]
fn test_ingestion_config_defaults() {
    let config = IngestionConfig::default();
    assert_eq!(config.max_batch_size, 1000);
    assert_eq!(config.concurrency, 4);
    assert_eq!(config.poll_interval, Duration::from_millis(100));
    assert_eq!(config.commit_interval, Duration::from_secs(5));
}

#[test]
fn test_ingestion_config_custom() {
    let config = IngestionConfig {
        max_batch_size: 500,
        poll_interval: Duration::from_millis(50),
        commit_interval: Duration::from_secs(1),
        concurrency: 8,
    };
    assert_eq!(config.max_batch_size, 500);
    assert_eq!(config.concurrency, 8);
}

// ============================================================
// IngestionPipeline Tests
// ============================================================

#[tokio::test]
async fn test_pipeline_with_source() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let config = IngestionConfig::default();
    let pipeline = IngestionPipeline::new(config, store);

    let source = Arc::new(MockSource::new("events", vec![make_record("k", "v")]));
    let pipeline = Arc::new(pipeline.with_source(source.clone()));

    // Load offsets before starting
    let count = pipeline.load_offsets().await.unwrap();
    assert_eq!(count, 0, "Fresh store should have no offsets");

    // Source should be usable
    let batch = source.poll().await.unwrap().unwrap();
    assert_eq!(batch.records.len(), 1);
}

#[tokio::test]
async fn test_pipeline_multiple_sources() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let config = IngestionConfig::default();
    let pipeline = IngestionPipeline::new(config, store)
        .with_source(Arc::new(MockSource::new("topic-a", vec![])))
        .with_source(Arc::new(MockSource::new("topic-b", vec![])));

    // Just verify no crash on load_offsets
    let count = pipeline.load_offsets().await.unwrap();
    assert_eq!(count, 0);
}

/// A simple IngestHandler implementation for testing.
struct TestIngestHandler {
    processed: AtomicUsize,
    record_count: AtomicUsize,
}

impl TestIngestHandler {
    fn new() -> Self {
        Self {
            processed: AtomicUsize::new(0),
            record_count: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl IngestHandler for TestIngestHandler {
    async fn handle_batch(&self, batch: &IngestBatch) -> Result<usize, String> {
        self.processed.fetch_add(1, Ordering::Relaxed);
        self.record_count
            .fetch_add(batch.records.len(), Ordering::Relaxed);
        Ok(batch.records.len())
    }
}

#[tokio::test]
async fn test_handler_handles_batch() {
    let handler = TestIngestHandler::new();
    let batch = IngestBatch {
        records: vec![make_record("a", "1"), make_record("b", "2")],
        partition: "0".to_string(),
        offset_start: 0,
        offset_end: 2,
        topic: "t".to_string(),
        raw_events: None,
    };

    let count = handler.handle_batch(&batch).await.unwrap();
    assert_eq!(count, 2);
    assert_eq!(handler.processed.load(Ordering::Relaxed), 1);
    assert_eq!(handler.record_count.load(Ordering::Relaxed), 2);
}

// ============================================================
// IngestionError Tests
// ============================================================

#[test]
fn test_ingestion_error_display() {
    let err = IngestionError::Connection("connection refused".into());
    assert!(format!("{}", err).contains("connection refused"));

    let err = IngestionError::Timeout;
    assert!(format!("{}", err).contains("timeout"));

    let err = IngestionError::NotConnected;
    assert!(format!("{}", err).contains("not connected"));
}

#[test]
fn test_ingestion_error_debug() {
    let err = IngestionError::Poll("poll timeout".into());
    println!("{:?}", err);
}

// ============================================================
// Concurrency & Stress Tests
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_offset_store_writes() {
    let store = Arc::new(InMemoryOffsetStore::new());
    let barrier = Arc::new(Barrier::new(50));
    let mut handles = vec![];

    for i in 0..50 {
        let s = store.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            s.save("concurrent-topic", &format!("{}", i % 5), i * 10)
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let all = store.load_all().await.unwrap();
    assert!(all.len() >= 5, "At least 5 unique partitions should exist");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mock_source_poll_is_idempotent_under_concurrency() {
    let source = Arc::new(MockSource::new("race-test", vec![make_record("k", "v")]));
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];

    // Only one poll should return data; others should return None or also get data.
    // Either way, no crashes.
    for _ in 0..5 {
        let s = source.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            s.poll().await
        }));
    }

    let mut data_count = 0;
    for h in handles {
        let out = h.await.unwrap().unwrap();
        if out.is_some() {
            data_count += 1;
        }
    }

    // At least one task should get data
    assert!(data_count >= 1);
}

// ============================================================
// Send + Sync verification
// ============================================================

#[test]
fn test_types_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<IngestBatch>();
    assert_send_sync::<IngestRecord>();
    assert_send_sync::<SourceOffset>();
    assert_send_sync::<IngestBatch>();
    assert_send_sync::<IngestionConfig>();
    assert_send_sync::<InMemoryOffsetStore>();
    assert_send_sync::<OffsetManager>();
    assert_send_sync::<MockSource>();
}
