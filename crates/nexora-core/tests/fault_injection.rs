//! Fault injection, proptest, and chaos testing for nexora-core.
//!
//! Property-based tests, channel overflow, WAL corruption, and
//! malformed data injection tests.
//!
//! Run: cargo test --test fault_injection

use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use proptest::prelude::*;
use std::sync::Arc;

type SharedGraph = Arc<GraphService>;

fn make_test_graph() -> SharedGraph {
    Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 4,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ))
}

// ============================================================
// Proptest: Property set/get invariance
// ============================================================

proptest! {
    #[test]
    fn set_then_get_value(
        key in "[a-z][a-z0-9_]{1,20}",
        int_val in any::<i64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let graph = make_test_graph();
            let qid = NexoraId::from_bytes(b"set-get".to_vec());
            graph.set_property(&qid, &key, PropertyValue::Integer(int_val)).await.unwrap();
            assert_eq!(
                graph.get_property(&qid, &key).await.unwrap(),
                Some(PropertyValue::Integer(int_val))
            );
        });
    }

    #[test]
    fn overwrite_returns_latest(
        key in "[a-z][a-z0-9_]{1,20}",
        first in any::<i64>(),
        second in any::<i64>(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let graph = make_test_graph();
            let qid = NexoraId::from_bytes(b"overwrite".to_vec());
            graph.set_property(&qid, &key, PropertyValue::Integer(first)).await.unwrap();
            graph.set_property(&qid, &key, PropertyValue::Integer(second)).await.unwrap();
            assert_eq!(
                graph.get_property(&qid, &key).await.unwrap(),
                Some(PropertyValue::Integer(second))
            );
        });
    }

    #[test]
    fn arbitrary_key_lengths_no_panic(
        key_len in 0usize..300usize,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let graph = make_test_graph();
            let qid = NexoraId::from_bytes(b"keylen".to_vec());
            let key = "x".repeat(key_len);
            let _ = graph.set_property(&qid, &key, PropertyValue::Integer(1)).await;
        });
    }
}

// ============================================================
// Channel overflow & backpressure
// ============================================================

#[tokio::test]
async fn test_channel_overflow_graceful() {
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 1,
            max_nodes_per_shard: 100,
            node_channel_size: 1,
        },
        Arc::new(InMemoryPersistor::new()),
    ));

    let qid = NexoraId::from_bytes(b"overflow".to_vec());
    let mut handles = Vec::new();
    for i in 0..50 {
        let g = graph.clone();
        let q = qid.clone();
        handles.push(tokio::spawn(async move {
            let _ = g
                .set_property(&q, &format!("f_{i}"), PropertyValue::Integer(i as i64))
                .await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    assert!(graph.get_property(&qid, "f_0").await.is_ok());
}

#[tokio::test]
async fn test_backpressure_no_data_loss() {
    let graph = GraphService::new(
        GraphServiceConfig {
            num_shards: 2,
            max_nodes_per_shard: 100,
            node_channel_size: 8,
        },
        Arc::new(InMemoryPersistor::new()),
    );
    let qid = NexoraId::from_bytes(b"backpressure".to_vec());

    for i in 0..100 {
        graph
            .set_property(&qid, &format!("p_{i}"), PropertyValue::Integer(i as i64))
            .await
            .unwrap();
    }
    for i in 0..100 {
        assert_eq!(
            graph.get_property(&qid, &format!("p_{i}")).await.unwrap(),
            Some(PropertyValue::Integer(i as i64))
        );
    }
}

// ============================================================
// Malformed data injection
// ============================================================

#[tokio::test]
async fn test_deeply_nested_properties() {
    let graph = make_test_graph();
    let qid = NexoraId::from_bytes(b"deep".to_vec());
    let mut nested = PropertyValue::Integer(42);
    for _ in 0..50 {
        nested = PropertyValue::List(vec![nested]);
    }
    let _ = graph.set_property(&qid, "deep", nested).await;
}

#[tokio::test]
async fn test_special_keys() {
    let graph = make_test_graph();
    let qid = NexoraId::from_bytes(b"special-keys".to_vec());
    for key in &["hello_world", "key-with-dashes", "key.with.dots"] {
        let _ = graph
            .set_property(&qid, key, PropertyValue::Integer(1))
            .await;
    }
}

#[tokio::test]
async fn test_empty_node_id() {
    let graph = make_test_graph();
    let qid = NexoraId::from_bytes(vec![]);
    let _ = graph
        .set_property(&qid, "test", PropertyValue::Integer(1))
        .await;
    let _ = graph.get_property(&qid, "test").await;
}

// ============================================================
// WAL corruption tests
// ============================================================

#[tokio::test]
async fn test_wal_mid_record_corruption() {
    use nexora_core::event::{NodeChangeEvent, TimedEvent};
    use nexora_core::wal::{WalOperation, WalSyncPolicy, WriteAheadLog};
    use nexora_value::Symbol;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Never).unwrap();
        for i in 0..5u64 {
            wal.append(WalOperation::NodeEvent {
                qid: NexoraId::from_bytes(format!("n{i}").into_bytes()),
                event: TimedEvent {
                    time: nexora_id::EventTime(i),
                    event: NodeChangeEvent::PropertySet {
                        key: Symbol::new("k"),
                        value: PropertyValue::Integer(i as i64),
                    },
                },
            })
            .unwrap();
        }
        wal.sync().unwrap();
    }
    let wal_path = dir.path().join("current.wal");
    let mut raw = std::fs::read(&wal_path).unwrap();
    let mid = raw.len() / 2;
    if raw.len() > 50 {
        raw[mid] ^= 0xFF;
    }
    std::fs::write(&wal_path, &raw).unwrap();

    let recovered = WriteAheadLog::replay_dir(dir.path()).unwrap();
    assert!(
        !recovered.records.is_empty(),
        "Should recover records before corruption"
    );
}

#[tokio::test]
async fn test_wal_truncated_recovery() {
    use nexora_core::event::{NodeChangeEvent, TimedEvent};
    use nexora_core::wal::{WalOperation, WalSyncPolicy, WriteAheadLog};
    use nexora_value::Symbol;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Never).unwrap();
        for i in 0..3u64 {
            wal.append(WalOperation::NodeEvent {
                qid: NexoraId::from_bytes(format!("n{i}").into_bytes()),
                event: TimedEvent {
                    time: nexora_id::EventTime(i),
                    event: NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(i as i64),
                    },
                },
            })
            .unwrap();
        }
        wal.sync().unwrap();
    }
    let wal_path = dir.path().join("current.wal");
    let raw = std::fs::read(&wal_path).unwrap();
    let at = raw.len() * 2 / 3;
    std::fs::write(&wal_path, &raw[..at]).unwrap();

    let recovered = WriteAheadLog::replay_dir(dir.path()).unwrap();
    assert!(
        !recovered.records.is_empty(),
        "Should recover records from truncated WAL"
    );
}

// ============================================================
// Concurrent stress
// ============================================================

#[tokio::test]
async fn test_concurrent_writes_no_panic() {
    let graph = Arc::new(make_test_graph());
    let qid = NexoraId::from_bytes(b"concurrent".to_vec());
    let mut handles = Vec::new();
    for i in 0..20 {
        let g = graph.clone();
        let q = qid.clone();
        handles.push(tokio::spawn(async move {
            g.set_property(&q, "counter", PropertyValue::Integer(i as i64))
                .await
        }));
    }
    for h in handles {
        assert!(h.await.unwrap().is_ok());
    }
}

#[tokio::test]
async fn test_mixed_operations_sequence() {
    let graph = make_test_graph();
    let qid = NexoraId::from_bytes(b"mixed-ops".to_vec());

    let ops = [
        ("set", "name", PropertyValue::String("Alice".into())),
        ("set", "age", PropertyValue::Integer(30)),
        ("get", "name", PropertyValue::Null),
        ("set", "age", PropertyValue::Integer(31)),
        ("set", "active", PropertyValue::Boolean(true)),
        ("get", "missing", PropertyValue::Null),
        ("set", "score", PropertyValue::Float(98.5)),
    ];

    for (op, key, val) in &ops {
        match *op {
            "set" => {
                let _ = graph.set_property(&qid, key, val.clone()).await;
            }
            "get" => {
                let _ = graph.get_property(&qid, key).await;
            }
            _ => {}
        }
    }

    assert_eq!(
        graph.get_property(&qid, "name").await.unwrap(),
        Some(PropertyValue::String("Alice".into()))
    );
    assert_eq!(
        graph.get_property(&qid, "age").await.unwrap(),
        Some(PropertyValue::Integer(31))
    );
    assert_eq!(
        graph.get_property(&qid, "score").await.unwrap(),
        Some(PropertyValue::Float(98.5))
    );
}
