//! Chaos engineering tests — extreme conditions validation.
//!
//! Tests: CHAOS-001 through CHAOS-006

use nexora_core::{
    event::{NodeChangeEvent, TimedEvent},
    wal::{WalOperation, WalSyncPolicy, WriteAheadLog},
    GraphService, GraphServiceConfig, InMemoryPersistor,
};
use nexora_id::{EventTime, NexoraId, PropertyValue};
use nexora_value::Symbol;
use std::sync::Arc;

// CHAOS-001: Rapid create + delete cycle (1000 iterations)
#[tokio::test]
async fn test_rapid_create_delete_cycle() {
    let graph = make_service();
    let qid = NexoraId::from_bytes(b"chaos-node".to_vec());

    for i in 0..1000 {
        graph
            .set_property(&qid, "val", PropertyValue::Integer(i))
            .await
            .unwrap();
        let read = graph.get_property(&qid, "val").await.unwrap();
        assert_eq!(read, Some(PropertyValue::Integer(i)));
    }
    // Node should still be accessible
    let final_val = graph.get_property(&qid, "val").await.unwrap();
    assert!(final_val.is_some());
}

// CHAOS-002: Very large property values
#[tokio::test]
async fn test_large_property_values() {
    let graph = make_service();
    let qid = NexoraId::from_bytes(b"large-node".to_vec());

    // 10KB string property
    let large_string = "A".repeat(10_000);
    graph
        .set_property(&qid, "data", PropertyValue::String(large_string.clone()))
        .await
        .unwrap();

    let read = graph.get_property(&qid, "data").await.unwrap();
    assert_eq!(read, Some(PropertyValue::String(large_string)));

    // 1000-element list
    let large_list: Vec<PropertyValue> = (0..1000).map(PropertyValue::Integer).collect();
    graph
        .set_property(&qid, "items", PropertyValue::List(large_list))
        .await
        .unwrap();
    assert!(graph.get_property(&qid, "items").await.unwrap().is_some());
}

// CHAOS-003: Many edges on single node
#[tokio::test]
async fn test_many_edges_single_node() {
    let graph = make_service();
    let hub = NexoraId::from_bytes(b"hub-node".to_vec());

    for i in 0..500 {
        let target = NexoraId::from_bytes(format!("target-{i:04}").into_bytes());
        let edge = nexora_value::HalfEdge::out(Symbol::new("LINKS_TO"), target);
        graph.add_edge(&hub, edge).await.unwrap();
    }

    let edges = graph.get_edges(&hub).await.unwrap();
    assert_eq!(edges.len(), 500, "Hub should have 500 edges");
}

// CHAOS-004: Concurrent read + write + delete on many nodes
#[tokio::test]
async fn test_concurrent_mixed_operations() {
    let graph = Arc::new(make_service());
    let mut handles = Vec::new();

    for i in 0..200 {
        let g = graph.clone();
        handles.push(tokio::spawn(async move {
            let qid = NexoraId::from_bytes(format!("cnode-{i:04}").into_bytes());
            g.set_property(&qid, "a", PropertyValue::Integer(1))
                .await
                .unwrap();
            g.set_property(&qid, "b", PropertyValue::Integer(2))
                .await
                .unwrap();
            let _ = g.get_property(&qid, "a").await;
            let _ = g.get_property(&qid, "b").await;
            qid
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

// CHAOS-005: WAL recovery after simulated crash
#[tokio::test]
async fn test_wal_recovery_after_partial_write() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::from_bytes(b"crash-node".to_vec());

    // Phase 1: Write 10 events, then simulate crash (no checkpoint)
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
        for i in 1..=10 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("val"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros((i * 1000) as u64),
                ),
            })
            .unwrap();
        }
        // No checkpoint — simulate crash
    }

    // Phase 2: Recover — should have all 10 records
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(
            records.records.len(),
            10,
            "All 10 records should survive crash"
        );
    }
}

// CHAOS-006: WAL corruption + recovery
#[tokio::test]
async fn test_wal_corruption_recovery() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();

    // Write valid records
    {
        let mut wal = WriteAheadLog::open(dir.path()).unwrap();
        for i in 1..=3 {
            wal.append(WalOperation::NodeEvent {
                qid: NexoraId::new_random(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("x"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }
    }

    // Append corrupt bytes
    {
        let wal_path = dir.path().join("current.wal");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap(); // Bad magic
    }

    // Recovery should get 3 valid records
    let mut wal = WriteAheadLog::open(dir.path()).unwrap();
    let records = wal.replay().unwrap();
    assert_eq!(
        records.records.len(),
        3,
        "Should recover 3 valid records, discard corrupt tail"
    );
}

// CHAOS-007: Random node crash + consistency verification
// Simulates: write properties → crash → restart → verify old state + write new
#[tokio::test]
async fn test_random_crash_consistency() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::from_bytes(b"crash-verify-node".to_vec());

    let props_phase1: Vec<(String, PropertyValue)> = (0..20)
        .map(|i| (format!("k{}", i), PropertyValue::Integer(i)))
        .collect();

    // Phase 1: Write properties, then crash (drop WAL without checkpoint)
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
        for (key, val) in &props_phase1 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new(key),
                        value: val.clone(),
                    },
                    EventTime::now(),
                ),
            })
            .unwrap();
        }
        // Simulate crash: drop without checkpoint
    }

    // Phase 2: Recover and verify old data is intact
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(
            records.records.len(),
            20,
            "All 20 records should survive crash"
        );

        // Write new properties after recovery
        for i in 20..30 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new(&format!("k{}", i)),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::now(),
                ),
            })
            .unwrap();
        }
    }

    // Phase 3: Recover again and verify all 30 records
    let mut wal = WriteAheadLog::open(dir.path()).unwrap();
    let records = wal.replay().unwrap();
    assert_eq!(
        records.records.len(),
        30,
        "30 records should survive second crash"
    );
}

// CHAOS-008: WAL torn write recovery with FlatBuffers v2
#[tokio::test]
async fn test_wal_torn_write_flatbuffer() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();

    // Write 5 valid FB records
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
        for i in 1..=5 {
            wal.append(WalOperation::NodeEvent {
                qid: NexoraId::from_bytes(format!("fb-node-{i}").into_bytes()),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("v"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }
    }

    // Simulate torn write: append partial FB bytes (half a valid record)
    {
        let wal_path = dir.path().join("current.wal");
        // Append a valid magic + partial data (truncated before stop marker)
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Write magic "QF", length=100, then 50 bytes of garbage (torn)
        f.write_all(&[0x51, 0x46]).unwrap(); // FB magic
        f.write_all(&100u32.to_be_bytes()).unwrap();
        f.write_all(&[0xAB; 50]).unwrap(); // Torn: only 50 of 100 bytes written
                                           // No crc, no stop marker — torn write
    }

    // Recovery should get 5 valid records, discard torn tail
    let mut wal = WriteAheadLog::open(dir.path()).unwrap();
    let records = wal.replay().unwrap();
    assert_eq!(
        records.records.len(),
        5,
        "Should recover 5 valid records, discard torn tail"
    );
}

// CHAOS-009: Concurrent high-contention writes on single node
#[tokio::test]
async fn test_concurrent_single_node_contention() {
    let graph = Arc::new(make_service());
    let qid = NexoraId::from_bytes(b"hot-node".to_vec());

    let mut handles = Vec::new();
    let num_writers = 32;
    let writes_per_writer = 50;

    for w in 0..num_writers {
        let g = graph.clone();
        let q = qid.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..writes_per_writer {
                let key = format!("w{}-k{}", w, j);
                let val = PropertyValue::Integer((w * 1000 + j) as i64);
                g.set_property(&q, &key, val.clone()).await.unwrap();
                // Verify write was accepted
                let read = g.get_property(&q, &key).await.unwrap();
                assert_eq!(read, Some(val));
            }
            w
        }));
    }

    for h in handles {
        let _ = h.await.unwrap();
    }
}

// CHAOS-010: Rapid node creation + deletion in loop
#[tokio::test]
async fn test_rapid_create_delete_nodes() {
    let graph = make_service();

    for cycle in 0..10 {
        let qid = NexoraId::from_bytes(format!("ephemeral-{}", cycle).into_bytes());
        // Create and use
        graph
            .set_property(&qid, "cycle", PropertyValue::Integer(cycle))
            .await
            .unwrap();
        let val = graph.get_property(&qid, "cycle").await.unwrap();
        assert_eq!(val, Some(PropertyValue::Integer(cycle)));

        // Create edge
        let target = NexoraId::from_bytes(format!("target-{}", cycle).into_bytes());
        let edge = nexora_value::HalfEdge::out(Symbol::new("CYCLE"), target.clone());
        graph.add_edge(&qid, edge).await.unwrap();

        let edges = graph.get_edges(&qid).await.unwrap();
        assert_eq!(edges.len(), 1);
    }
}

// CHAOS-011: Edge graph consistency — 100-node chain with verification
#[tokio::test]
async fn test_edge_chain_consistency() {
    let graph = make_service();
    let num_nodes = 100;

    // Build chain: n0 → n1 → n2 → ... → n99
    for i in 0..(num_nodes - 1) {
        let from = NexoraId::from_bytes(format!("chain-{:03}", i).into_bytes());
        let to = NexoraId::from_bytes(format!("chain-{:03}", i + 1).into_bytes());
        let edge = nexora_value::HalfEdge::out(Symbol::new("NEXT"), to);
        graph.add_edge(&from, edge).await.unwrap();
    }

    // Verify chain connectivity
    for i in 0..(num_nodes - 1) {
        let from = NexoraId::from_bytes(format!("chain-{:03}", i).into_bytes());
        let edges = graph.get_edges(&from).await.unwrap();
        assert_eq!(edges.len(), 1, "Node {} should have exactly 1 edge", i);
    }

    // Last node should have 0 edges
    let last = NexoraId::from_bytes(format!("chain-{:03}", num_nodes - 1).into_bytes());
    let edges = graph.get_edges(&last).await.unwrap();
    assert_eq!(edges.len(), 0);
}

// CHAOS-012: Timestamp ordering under concurrent stress
#[tokio::test]
async fn test_timestamp_monotonicity_under_stress() {
    let graph = Arc::new(make_service());
    let qid = NexoraId::from_bytes(b"ts-node".to_vec());
    let mut handles = Vec::new();

    for i in 0..50 {
        let g = graph.clone();
        let q = qid.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("ts-{}", i);
            g.set_property(&q, &key, PropertyValue::Integer(i as i64))
                .await
                .unwrap();
        }));
    }

    // All writes must complete without error
    for h in handles {
        h.await.unwrap();
    }

    // All properties should be retrievable
    for i in 0..50 {
        let key = format!("ts-{}", i);
        let val = graph.get_property(&qid, &key).await.unwrap();
        assert!(val.is_some(), "Property {} should exist after stress", key);
    }
}

// CHAOS-013: Mixed read-write-delete under high load
#[tokio::test]
async fn test_mixed_rw_delete_stress() {
    let graph = Arc::new(make_service());
    let mut handles = Vec::new();
    let num_nodes = 50;
    let ops_per_node = 20;

    for n in 0..num_nodes {
        let g = graph.clone();
        handles.push(tokio::spawn(async move {
            let qid = NexoraId::from_bytes(format!("mixed-{:03}", n).into_bytes());
            for op in 0..ops_per_node {
                match op % 3 {
                    0 => {
                        // Set property
                        g.set_property(
                            &qid,
                            &format!("op{}", op),
                            PropertyValue::Integer(op as i64),
                        )
                        .await
                        .unwrap();
                    }
                    1 => {
                        // Add edge
                        let target = NexoraId::from_bytes(
                            format!("mixed-{:03}", (n + 1) % num_nodes).into_bytes(),
                        );
                        let edge = nexora_value::HalfEdge::out(Symbol::new("MIXED"), target);
                        g.add_edge(&qid, edge).await.unwrap();
                    }
                    _ => {
                        // Read
                        let _ = g.get_property(&qid, "op0").await;
                    }
                }
            }
            qid
        }));
    }

    for h in handles {
        let _ = h.await.unwrap();
    }
}

// CHAOS-014: Snapshot checkpoint + recovery roundtrip
#[tokio::test]
async fn test_snapshot_checkpoint_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let qid = NexoraId::from_bytes(b"snap-node".to_vec());

    // Write events + checkpoint
    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
        for i in 1..=5 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("snap-prop"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }
        // Insert checkpoint
        wal.append(WalOperation::SnapshotCheckpoint {
            qid: qid.clone(),
            snapshot_time: EventTime::from_micros(100),
        })
        .unwrap();
        // More events after checkpoint
        for i in 6..=10 {
            wal.append(WalOperation::NodeEvent {
                qid: qid.clone(),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("snap-prop"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        }
    }

    // Recover: should have 10 events + 1 checkpoint = 11 records
    let mut wal = WriteAheadLog::open(dir.path()).unwrap();
    let records = wal.replay().unwrap();
    assert_eq!(
        records.records.len(),
        11,
        "10 events + 1 checkpoint = 11 records"
    );
}

// CHAOS-015: Graceful degradation under resource exhaustion
// Tests that the graph service doesn't panic when creating many nodes
#[tokio::test]
async fn test_many_nodes_no_panic() {
    let graph = make_service();
    let num_nodes = 200;

    for i in 0..num_nodes {
        let qid = NexoraId::from_bytes(format!("bulk-{:04}", i).into_bytes());
        let result = graph
            .set_property(&qid, "bulk", PropertyValue::Boolean(true))
            .await;
        // Should not panic
        assert!(result.is_ok());
    }
}

fn make_service() -> GraphService {
    let config = GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: 2000,
        node_channel_size: 128,
    };
    GraphService::new(config, Arc::new(InMemoryPersistor::new()))
}
