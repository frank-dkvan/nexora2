//! block B: GraphService::write_batch — concurrency correctness + per-session
//! durability semantics.
//!
//! write_batch fans out per-node commits across shards so the WAL group-commit
//! flusher can amortize one fsync over the batch. Contracts:
//!   - **correctness**: every item is applied exactly once, results returned in
//!     input order, no cross-node interference.
//!   - **WaitDurable**: a returned batch implies every write is fsynced — it
//!     survives a crash with no shutdown.
//!   - **Relaxed**: returns once buffered; after a *graceful* shutdown (flusher
//!     drained) the writes are durable. (Crash-loss of un-fsynced Relaxed writes
//!     is by design and recovered via upstream offset replay — not asserted here
//!     since there's no offset source in this unit test.)

use nexora_core::{
    BatchDurability, GraphService, GraphServiceConfig, InMemoryPersistor, MutationOp,
    WriteBatchOptions,
};
use nexora_id::{NexoraId, PropertyValue};
use nexora_persistor_rocksdb::RocksDbPersistor;
use nexora_value::{HalfEdge, Symbol};
use std::sync::Arc;

fn config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 8,
        max_nodes_per_shard: 1_000_000,
        node_channel_size: 64,
    }
}

fn node(i: usize) -> NexoraId {
    NexoraId::from_bytes(format!("b-{i:06}").into_bytes())
}

/// All items applied, results in input order, values correct.
#[tokio::test]
async fn write_batch_applies_all_in_order() {
    let svc = GraphService::new(config(), Arc::new(InMemoryPersistor::new()));

    let n = 500usize;
    let items: Vec<_> = (0..n)
        .map(|i| {
            (
                node(i),
                vec![MutationOp::SetProperty {
                    key: Symbol::new("v"),
                    value: PropertyValue::Integer(i as i64),
                }],
            )
        })
        .collect();

    let receipts = svc
        .write_batch(items, WriteBatchOptions::default())
        .await
        .unwrap();
    assert_eq!(receipts.len(), n, "one receipt per item, in order");
    assert!(
        receipts.iter().all(|r| r.event_count == 1),
        "each single-op commit yields one event"
    );

    // Every node has its value.
    for i in 0..n {
        assert_eq!(
            svc.get_property(&node(i), "v").await.unwrap(),
            Some(PropertyValue::Integer(i as i64)),
            "node {i} must reflect its batch write"
        );
    }
}

/// Multiple ops in one item apply together (atomic per-node commit).
#[tokio::test]
async fn write_batch_multi_op_per_item() {
    let svc = GraphService::new(config(), Arc::new(InMemoryPersistor::new()));
    let a = node(1);
    let b = node(2);

    let items = vec![(
        a.clone(),
        vec![
            MutationOp::SetProperty {
                key: Symbol::new("x"),
                value: PropertyValue::Integer(1),
            },
            MutationOp::AddLabel {
                label: Symbol::new("Person"),
            },
            MutationOp::AddEdge {
                edge: HalfEdge::out(Symbol::new("KNOWS"), b.clone()),
            },
        ],
    )];

    let receipts = svc
        .write_batch(items, WriteBatchOptions::default())
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].event_count, 3, "three ops → three events");

    // Property, label (via projection), and edge all landed.
    assert_eq!(
        svc.get_property(&a, "x").await.unwrap(),
        Some(PropertyValue::Integer(1))
    );
    assert!(svc
        .get_labels(&a)
        .await
        .unwrap()
        .contains(&Symbol::new("Person")));
    assert!(svc
        .get_edges(&a)
        .await
        .unwrap()
        .iter()
        .any(|e| e.edge_type == Symbol::new("KNOWS") && e.other == b));

    // #4a side-effect wiring: edge index updated from the batch path.
    let outgoing = svc.edge_index.query_outgoing("KNOWS").await;
    assert!(
        outgoing.iter().any(|(s, d)| *s == a && *d == b),
        "batch add_edge must maintain the edge index"
    );
    // Label index too.
    let labeled = svc.label_index.query("Person").await;
    assert!(
        labeled.contains(&a),
        "batch add_label must maintain the label index"
    );
}

/// WaitDurable batch survives a crash with no shutdown (ack ⟹ durable).
#[tokio::test]
async fn write_batch_wait_durable_survives_crash() {
    let temp = tempfile::tempdir().unwrap();
    let wal_dir = temp.path().join("wal");
    let db_dir = temp.path().join("db");
    let persistor = Arc::new(RocksDbPersistor::open(&db_dir).unwrap());
    let n = 128usize;

    // Phase 1: durable batch, then "crash" (drop, no shutdown).
    {
        let svc =
            GraphService::new_with_wal(config(), persistor.clone(), wal_dir.clone(), None).unwrap();
        svc.replay_all_wals().await.unwrap();

        let items: Vec<_> = (0..n)
            .map(|i| {
                (
                    node(i),
                    vec![MutationOp::SetProperty {
                        key: Symbol::new("v"),
                        value: PropertyValue::Integer(i as i64),
                    }],
                )
            })
            .collect();
        svc.write_batch(
            items,
            WriteBatchOptions {
                concurrency: 32,
                durability: BatchDurability::WaitDurable,
            },
        )
        .await
        .unwrap();
        // no shutdown — simulate process kill
    }

    // Phase 2: recover, every acked write must be present.
    {
        let svc = GraphService::new_with_wal(config(), persistor, wal_dir, None).unwrap();
        svc.replay_all_wals().await.unwrap();
        for i in 0..n {
            assert_eq!(
                svc.get_property(&node(i), "v").await.unwrap(),
                Some(PropertyValue::Integer(i as i64)),
                "WaitDurable batch write {i} must survive crash"
            );
        }
    }
}

/// Relaxed batch is durable after a graceful shutdown (flusher drained).
#[tokio::test]
async fn write_batch_relaxed_durable_after_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let wal_dir = temp.path().join("wal");
    let db_dir = temp.path().join("db");
    let persistor = Arc::new(RocksDbPersistor::open(&db_dir).unwrap());
    let n = 128usize;

    {
        let svc =
            GraphService::new_with_wal(config(), persistor.clone(), wal_dir.clone(), None).unwrap();
        svc.replay_all_wals().await.unwrap();

        let items: Vec<_> = (0..n)
            .map(|i| {
                (
                    node(i),
                    vec![MutationOp::SetProperty {
                        key: Symbol::new("v"),
                        value: PropertyValue::Integer(i as i64),
                    }],
                )
            })
            .collect();
        svc.write_batch(
            items,
            WriteBatchOptions {
                concurrency: 32,
                durability: BatchDurability::Relaxed,
            },
        )
        .await
        .unwrap();

        // Graceful shutdown drains the flusher → buffered writes are fsynced.
        svc.shutdown().await.unwrap();
    }

    {
        let svc = GraphService::new_with_wal(config(), persistor, wal_dir, None).unwrap();
        svc.replay_all_wals().await.unwrap();
        for i in 0..n {
            assert_eq!(
                svc.get_property(&node(i), "v").await.unwrap(),
                Some(PropertyValue::Integer(i as i64)),
                "Relaxed batch write {i} must be durable after graceful shutdown"
            );
        }
    }
}

/// An empty batch is a no-op.
#[tokio::test]
async fn write_batch_empty_is_noop() {
    let svc = GraphService::new(config(), Arc::new(InMemoryPersistor::new()));
    let receipts = svc
        .write_batch(Vec::new(), WriteBatchOptions::default())
        .await
        .unwrap();
    assert!(receipts.is_empty());
}

/// concurrency = 1 (fully serial) produces the same result as high concurrency.
#[tokio::test]
async fn write_batch_serial_matches_concurrent() {
    let svc = GraphService::new(config(), Arc::new(InMemoryPersistor::new()));
    let n = 200usize;
    let mk = |off: usize| -> Vec<_> {
        (0..n)
            .map(|i| {
                (
                    NexoraId::from_bytes(format!("s-{}-{}", off, i).into_bytes()),
                    vec![MutationOp::SetProperty {
                        key: Symbol::new("v"),
                        value: PropertyValue::Integer(i as i64),
                    }],
                )
            })
            .collect()
    };

    let serial = svc
        .write_batch(
            mk(0),
            WriteBatchOptions {
                concurrency: 1,
                durability: BatchDurability::WaitDurable,
            },
        )
        .await
        .unwrap();
    let concurrent = svc
        .write_batch(
            mk(1),
            WriteBatchOptions {
                concurrency: 128,
                durability: BatchDurability::WaitDurable,
            },
        )
        .await
        .unwrap();

    assert_eq!(serial.len(), n);
    assert_eq!(concurrent.len(), n);
    for i in 0..n {
        assert_eq!(
            svc.get_property(&NexoraId::from_bytes(format!("s-0-{i}").into_bytes()), "v")
                .await
                .unwrap(),
            svc.get_property(&NexoraId::from_bytes(format!("s-1-{i}").into_bytes()), "v")
                .await
                .unwrap(),
            "serial and concurrent batches must agree on node {i}"
        );
    }
}
