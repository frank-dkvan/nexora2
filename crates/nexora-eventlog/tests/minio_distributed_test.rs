// MinIO distributed integration tests for EventLogStore.
// All nodes share the same S3 warehouse prefix + same SQLite catalog,
// matching the production NFS/shared-storage scenario.
//
// Prerequisites:
//   docker run -d -p 9000:9000 -p 9001:9001 \
//     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//     minio/minio server /data --console-address ":9001"

#[cfg(all(test, feature = "olap"))]
mod minio_distributed_tests {
    use nexora_core::RawEvent;
    use nexora_eventlog::{EventLogStore, StorageConfig};
    use std::sync::Arc;
    use tempfile::TempDir;

    const MINIO_ENDPOINT: &str = "http://localhost:9000";
    const MINIO_BUCKET: &str = "nexora-test-cluster";
    const MINIO_ACCESS_KEY: &str = "minioadmin";
    const MINIO_SECRET_KEY: &str = "minioadmin";

    /// Build a StorageConfig pointing all nodes at the same warehouse + catalog.
    fn shared_config(prefix: &str, catalog_path: &str) -> StorageConfig {
        StorageConfig::s3(
            MINIO_ENDPOINT,
            MINIO_BUCKET,
            "us-east-1",
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
            Some(prefix.to_string()),
            true, // path_style required for MinIO
            catalog_path,
        )
    }

    /// Create N EventLogStore instances that all share the same S3 prefix and
    /// SQLite catalog — the correct multi-node setup where every node sees the
    /// same data.
    async fn create_shared_cluster(n: usize, prefix: &str) -> (Vec<Arc<EventLogStore>>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let catalog = tmp.path().join("catalog.db");
        let catalog_str = catalog.to_str().unwrap().to_owned();

        let mut stores = Vec::with_capacity(n);
        for i in 0..n {
            let cfg = shared_config(prefix, &catalog_str);
            let store = Arc::new(
                EventLogStore::new_with_config(cfg)
                    .await
                    .unwrap_or_else(|e| panic!("node-{i} init failed: {e}")),
            );
            stores.push(store);
        }
        (stores, tmp)
    }

    // ── Test 1 ──────────────────────────────────────────────────────────────
    /// Node 0 writes; nodes 1 and 2 must see the same rows immediately.
    #[tokio::test]
    #[ignore] // needs MinIO
    async fn test_basic_write_and_cross_node_read() {
        let (stores, _tmp) = create_shared_cluster(3, "t1").await;

        let events: Vec<RawEvent> = (0u64..10)
            .map(|i| {
                RawEvent::new(
                    1000 + i,
                    2000 + i,
                    "user-1",
                    "basic_test",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"seq": i}),
                )
            })
            .collect();

        stores[0].append(&events).await.expect("write failed");
        eprintln!("✓ node-0 wrote 10 events");

        for (idx, store) in stores.iter().enumerate() {
            let rows: usize = store
                .read_table_batches("basic_test")
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 10, "node-{idx} expected 10 rows, got {rows}");
            eprintln!("✓ node-{idx} reads {rows} rows");
        }
        eprintln!("✅ test_basic_write_and_cross_node_read passed");
    }

    // ── Test 2 ──────────────────────────────────────────────────────────────
    /// Three nodes write concurrently; the union must equal 10 + 20 + 35 = 65.
    #[tokio::test]
    #[ignore] // needs MinIO
    async fn test_concurrent_writes_from_multiple_nodes() {
        let (stores, _tmp) = create_shared_cluster(3, "t2").await;

        let mk = |source: &'static str, start: u64, n: u64| -> Vec<RawEvent> {
            (start..start + n)
                .map(|i| {
                    RawEvent::new(
                        1000 + i,
                        2000 + i,
                        source,
                        "concurrent_test",
                        None,
                        Some(i as i64),
                        None,
                        serde_json::json!({"src": source}),
                    )
                })
                .collect()
        };

        let ev0 = mk("n0", 0, 10);
        let ev1 = mk("n1", 100, 20);
        let ev2 = mk("n2", 200, 35);

        let (r0, r1, r2) = tokio::join!(
            stores[0].append(&ev0),
            stores[1].append(&ev1),
            stores[2].append(&ev2),
        );
        assert!(r0.is_ok(), "n0: {r0:?}");
        assert!(r1.is_ok(), "n1: {r1:?}");
        assert!(r2.is_ok(), "n2: {r2:?}");

        let total: usize = stores[0]
            .read_table_batches("concurrent_test")
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total, 65, "expected 65 (10+20+35), got {total}");
        eprintln!("✅ test_concurrent_writes_from_multiple_nodes: {total} rows");
    }

    // ── Test 3 ──────────────────────────────────────────────────────────────
    /// Two nodes write in parallel for 5 rounds; Iceberg must commit all 1000 rows
    /// without data loss (optimistic concurrency / retry on conflict).
    #[tokio::test]
    #[ignore] // needs MinIO
    async fn test_iceberg_conflict_resolution() {
        let (stores, _tmp) = create_shared_cluster(2, "t3").await;

        let mk = |node: u64, base: u64| -> Vec<RawEvent> {
            (0..100)
                .map(|i| {
                    RawEvent::new(
                        base + i,
                        base + i + 1,
                        "x",
                        "conflict_test",
                        None,
                        Some((base + i) as i64),
                        None,
                        serde_json::json!({"node": node}),
                    )
                })
                .collect()
        };

        for round in 0u64..5 {
            let ev0 = mk(0, round * 200);
            let ev1 = mk(1, round * 200 + 100);
            let (r0, r1) = tokio::join!(stores[0].append(&ev0), stores[1].append(&ev1));
            assert!(r0.is_ok(), "round {round} n0: {r0:?}");
            assert!(r1.is_ok(), "round {round} n1: {r1:?}");
            eprintln!("✓ round {} done", round + 1);
        }

        let total: usize = stores[0]
            .read_table_batches("conflict_test")
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total, 1000, "expected 1000 rows, got {total}");
        eprintln!("✅ test_iceberg_conflict_resolution: {total} rows, no data lost");
    }

    // ── Test 4 ──────────────────────────────────────────────────────────────
    /// Drop the EventLogStore, re-open with the same catalog path, confirm rows
    /// survive (data lives in MinIO, not just in memory).
    #[tokio::test]
    #[ignore] // needs MinIO
    async fn test_data_persistence_across_restarts() {
        let tmp = TempDir::new().unwrap();
        let catalog = tmp.path().join("p.db");
        let catalog_str = catalog.to_str().unwrap().to_owned();

        // session 1: write 5 rows
        {
            let store = Arc::new(
                EventLogStore::new_with_config(shared_config("t4", &catalog_str))
                    .await
                    .unwrap(),
            );
            let evs: Vec<RawEvent> = (0u64..5)
                .map(|i| {
                    RawEvent::new(
                        1000 + i,
                        2000 + i,
                        "u",
                        "persist_topic",
                        None,
                        Some(i as i64),
                        None,
                        serde_json::json!({"i": i}),
                    )
                })
                .collect();
            store.append(&evs).await.unwrap();
            eprintln!("✓ session 1: wrote 5 events");
        } // store dropped

        // session 2: new store instance, same catalog — must read 5 rows
        {
            let store = Arc::new(
                EventLogStore::new_with_config(shared_config("t4", &catalog_str))
                    .await
                    .unwrap(),
            );
            let rows: usize = store
                .read_table_batches("persist_topic")
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 5, "expected 5 persisted rows, got {rows}");
            eprintln!("✅ test_data_persistence_across_restarts: {rows} rows survived");
        }
    }

    // ── Test 5 ──────────────────────────────────────────────────────────────
    /// Stress: 3 nodes × 10 rounds × 50 events = 1500 rows total, all committed.
    #[tokio::test]
    #[ignore] // needs MinIO, slow
    async fn test_high_concurrency_stress() {
        let (stores, _tmp) = create_shared_cluster(3, "t5").await;

        let rounds = 10usize;
        let per_round = 50usize;

        for round in 0..rounds {
            let mk = |node: usize| -> Vec<RawEvent> {
                (0..per_round)
                    .map(|i| {
                        let seq = node * 10000 + round * per_round + i;
                        RawEvent::new(
                            seq as u64,
                            seq as u64 + 1,
                            "x",
                            "stress_test",
                            None,
                            Some(seq as i64),
                            None,
                            serde_json::json!({"n": node, "r": round}),
                        )
                    })
                    .collect()
            };

            let ev0 = mk(0);
            let ev1 = mk(1);
            let ev2 = mk(2);
            let (r0, r1, r2) = tokio::join!(
                stores[0].append(&ev0),
                stores[1].append(&ev1),
                stores[2].append(&ev2),
            );
            assert!(
                r0.is_ok() && r1.is_ok() && r2.is_ok(),
                "round {round} failed"
            );
            eprintln!("✓ round {}/{}", round + 1, rounds);
        }

        let total: usize = stores[0]
            .read_table_batches("stress_test")
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        let expected = rounds * per_round * 3;
        assert_eq!(total, expected, "expected {expected}, got {total}");
        eprintln!(
            "✅ test_high_concurrency_stress: {total} rows ({rounds} rounds × {per_round} × 3)"
        );
    }

    // ── Test 6 ──────────────────────────────────────────────────────────────
    /// Snapshot isolation: writes from node-0 must be visible to node-1 (and
    /// vice versa) once committed — no stale reads.
    #[tokio::test]
    #[ignore] // needs MinIO
    async fn test_snapshot_isolation() {
        let (stores, _tmp) = create_shared_cluster(2, "t6").await;

        let mk = |tag: &'static str, base: u64, n: u64| -> Vec<RawEvent> {
            (0..n)
                .map(|i| {
                    RawEvent::new(
                        base + i,
                        base + i + 1,
                        tag,
                        "snapshot_test",
                        None,
                        Some((base + i) as i64),
                        None,
                        serde_json::json!({"b": tag}),
                    )
                })
                .collect()
        };

        // batch 1: node-0 writes 10
        stores[0].append(&mk("b1", 0, 10)).await.unwrap();
        eprintln!("✓ node-0 wrote batch1 (10)");

        let c1: usize = stores[1]
            .read_table_batches("snapshot_test")
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(c1, 10, "after batch1: expected 10, got {c1}");

        // batch 2: node-1 writes 15
        stores[1].append(&mk("b2", 100, 15)).await.unwrap();
        eprintln!("✓ node-1 wrote batch2 (15)");

        let c2: usize = stores[0]
            .read_table_batches("snapshot_test")
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(c2, 25, "after batch2: expected 25, got {c2}");
        eprintln!("✅ test_snapshot_isolation: {c1}→{c2} rows visible across nodes");
    }
}
