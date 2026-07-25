// Multi-node distributed tests via a REST catalog (Lakekeeper).
//
// Unlike the MinIO+SQLite tests (which had to SHARE one catalog file to make
// nodes see each other's data), here each node uses a FULLY INDEPENDENT config —
// they only agree on the REST catalog URI + warehouse name. This is the real
// production topology: metadata is shared through the catalog service, no NFS
// or shared SQLite file needed.
//
// Prerequisites:
//   1. MinIO on localhost:9000 (minioadmin/minioadmin)
//   2. Lakekeeper on localhost:8181 with warehouse "nexora":
//        docker compose -f scripts/lakekeeper/docker-compose.yml up -d
//        ./scripts/lakekeeper/bootstrap.sh

#[cfg(all(test, feature = "olap"))]
mod lakekeeper_rest_tests {
    use nexora_core::RawEvent;
    use nexora_eventlog::{EventLogStore, StorageConfig};
    use std::sync::Arc;

    const REST_URI: &str = "http://localhost:8181/catalog";
    const WAREHOUSE: &str = "nexora";
    const S3_ENDPOINT: &str = "http://localhost:9000";
    const S3_KEY: &str = "minioadmin";
    const S3_SECRET: &str = "minioadmin";

    /// Each "node" gets a fully independent EventLogStore. They share NOTHING
    /// locally — only the REST catalog URI + warehouse. This is what makes the
    /// REST backend a true multi-node solution.
    async fn make_node() -> Arc<EventLogStore> {
        let cfg = StorageConfig::rest(
            REST_URI,
            WAREHOUSE,
            S3_ENDPOINT,
            "us-east-1",
            S3_KEY,
            S3_SECRET,
            true, // path_style for MinIO
        );
        Arc::new(
            EventLogStore::new_with_config(cfg)
                .await
                .expect("failed to connect to Lakekeeper REST catalog"),
        )
    }

    /// Unique topic per test run so repeated runs don't accumulate.
    fn unique_topic(base: &str) -> String {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("{base}_{ts}")
    }

    fn mk_events(source: &str, topic: &str, start: u64, n: u64) -> Vec<RawEvent> {
        (start..start + n)
            .map(|i| {
                RawEvent::new(
                    1000 + i,
                    2000 + i,
                    source,
                    topic,
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"src": source, "seq": i}),
                )
            })
            .collect()
    }

    // ── Test 1: cross-node read via independent configs ──────────────────────
    #[tokio::test]
    #[ignore] // needs Lakekeeper + MinIO
    async fn test_rest_basic_cross_node_read() {
        let topic = unique_topic("rest_basic");
        let node_a = make_node().await;
        let node_b = make_node().await;
        let node_c = make_node().await;
        eprintln!("✓ 3 independent nodes connected to Lakekeeper");

        node_a
            .append(&mk_events("node-a", &topic, 0, 10))
            .await
            .expect("node-a write failed");
        eprintln!("✓ node-a wrote 10 events");

        // node-b and node-c have SEPARATE local state — they only know about this
        // table through the REST catalog. If they can read it, sharing works.
        for (name, node) in [("node-b", &node_b), ("node-c", &node_c)] {
            let rows: usize = node
                .read_table_batches(&topic)
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(rows, 10, "{name} should see 10 rows via REST catalog, got {rows}");
            eprintln!("✓ {name} reads {rows} rows (no shared local catalog!)");
        }
        eprintln!("✅ test_rest_basic_cross_node_read passed");
    }

    // ── Test 2: concurrent writes from independent nodes ─────────────────────
    #[tokio::test]
    #[ignore] // needs Lakekeeper + MinIO
    async fn test_rest_concurrent_writes() {
        let topic = unique_topic("rest_concurrent");
        let node_a = make_node().await;
        let node_b = make_node().await;
        let node_c = make_node().await;

        // node-a creates the table first (avoids 3-way create race; the
        // create-or-load fallback handles races too, but this is cleaner).
        node_a
            .append(&mk_events("node-a", &topic, 0, 10))
            .await
            .expect("seed write failed");

        let ev_b = mk_events("node-b", &topic, 100, 20);
        let ev_c = mk_events("node-c", &topic, 200, 35);
        let (r1, r2) = tokio::join!(node_b.append(&ev_b), node_c.append(&ev_c));
        assert!(r1.is_ok(), "node-b: {r1:?}");
        assert!(r2.is_ok(), "node-c: {r2:?}");

        let total: usize = node_a
            .read_table_batches(&topic)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total, 65, "expected 10+20+35=65, got {total}");
        eprintln!("✅ test_rest_concurrent_writes: {total} rows across 3 independent nodes");
    }

    // ── Test 3: data persists across node restart ────────────────────────────
    #[tokio::test]
    #[ignore] // needs Lakekeeper + MinIO
    async fn test_rest_persistence_across_restart() {
        let topic = unique_topic("rest_persist");

        {
            let node = make_node().await;
            node.append(&mk_events("writer", &topic, 0, 7))
                .await
                .unwrap();
            eprintln!("✓ wrote 7 events, dropping node");
        } // node dropped — all local state gone

        // brand-new node, zero shared local state
        let fresh = make_node().await;
        let rows: usize = fresh
            .read_table_batches(&topic)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 7, "expected 7 persisted rows, got {rows}");
        eprintln!("✅ test_rest_persistence_across_restart: {rows} rows survived");
    }

    // ── Test 4: snapshot visibility across nodes ─────────────────────────────
    #[tokio::test]
    #[ignore] // needs Lakekeeper + MinIO
    async fn test_rest_snapshot_visibility() {
        let topic = unique_topic("rest_snapshot");
        let node_a = make_node().await;
        let node_b = make_node().await;

        node_a
            .append(&mk_events("node-a", &topic, 0, 10))
            .await
            .unwrap();
        let c1: usize = node_b
            .read_table_batches(&topic)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(c1, 10, "node-b after batch1: expected 10, got {c1}");

        node_b
            .append(&mk_events("node-b", &topic, 100, 15))
            .await
            .unwrap();
        let c2: usize = node_a
            .read_table_batches(&topic)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(c2, 25, "node-a after batch2: expected 25, got {c2}");
        eprintln!("✅ test_rest_snapshot_visibility: {c1}→{c2} rows visible across nodes");
    }
}
