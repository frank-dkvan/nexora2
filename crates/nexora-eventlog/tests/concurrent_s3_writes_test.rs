//! 多节点并发写入 S3 Event Store 测试
//!
//! 测试场景：
//! 1. 两个节点同时写入同一个 S3 Iceberg 表
//! 2. 验证数据无丢失
//! 3. 验证 Iceberg 自动处理冲突
//! 4. 测试高并发场景

#[cfg(all(test, feature = "olap"))]
mod concurrent_s3_writes {
    use nexora_eventlog::{EventLogStore, RawEvent, StorageConfig};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// 创建共享的 S3 配置（模拟两个节点连接同一个 MinIO）
    fn create_shared_s3_config(catalog_path: &str) -> StorageConfig {
        // 注意：这个测试需要真实的 MinIO 实例
        // 运行前启动: docker run -p 9000:9000 minio/minio server /data
        StorageConfig::s3(
            "http://localhost:9000",
            "nexora-test-events",
            "us-east-1",
            "minioadmin",
            "minioadmin",
            Some("test".into()),
            true, // path_style for MinIO
            catalog_path,
        )
    }

    /// 测试：两个节点并发写入同一个表
    #[tokio::test]
    #[ignore] // 需要真实 MinIO，默认跳过
    async fn test_two_nodes_concurrent_writes() {
        // 准备两个独立的 catalog（模拟两个节点）
        let temp_a = TempDir::new().unwrap();
        let temp_b = TempDir::new().unwrap();
        let catalog_a = temp_a.path().join("catalog.db");
        let catalog_b = temp_b.path().join("catalog.db");

        // 两个节点使用相同的 S3 配置
        let config_a = create_shared_s3_config(catalog_a.to_str().unwrap());
        let config_b = create_shared_s3_config(catalog_b.to_str().unwrap());

        let store_a = Arc::new(EventLogStore::new_with_config(config_a).await.unwrap());
        let store_b = Arc::new(EventLogStore::new_with_config(config_b).await.unwrap());

        // 准备两批事件
        let events_a: Vec<RawEvent> = (0..10)
            .map(|i| {
                RawEvent::new(
                    1000000 + i,
                    2000000 + i,
                    "node-a",
                    "test_topic",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"node": "a", "seq": i}),
                )
            })
            .collect();

        let events_b: Vec<RawEvent> = (100..110)
            .map(|i| {
                RawEvent::new(
                    1000000 + i,
                    2000000 + i,
                    "node-b",
                    "test_topic",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"node": "b", "seq": i}),
                )
            })
            .collect();

        // 并发写入
        let (result_a, result_b) = tokio::join!(
            store_a.append(&events_a),
            store_b.append(&events_b),
        );

        // 验证：两次写入都成功
        assert!(result_a.is_ok(), "Node A write failed: {:?}", result_a);
        assert!(result_b.is_ok(), "Node B write failed: {:?}", result_b);
        assert_eq!(result_a.unwrap(), 10);
        assert_eq!(result_b.unwrap(), 10);

        // 验证：总行数正确（从任一节点读取都能看到全部数据）
        let batches = store_a.read_table_batches("test_topic").await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 20,
            "Expected 20 total rows (10 from each node), got {}",
            total_rows
        );

        println!("✅ Two nodes concurrent write test passed");
    }

    /// 测试：高并发写入（5 个节点同时写）
    #[tokio::test]
    #[ignore] // 需要真实 MinIO
    async fn test_high_concurrency_writes() {
        let num_nodes = 5;
        let events_per_node = 20;

        // 创建 5 个节点的 store
        let mut stores = Vec::new();
        let mut temp_dirs = Vec::new();

        for i in 0..num_nodes {
            let temp = TempDir::new().unwrap();
            let catalog_path = temp.path().join(format!("catalog_{}.db", i));
            let config = create_shared_s3_config(catalog_path.to_str().unwrap());
            let store = Arc::new(EventLogStore::new_with_config(config).await.unwrap());
            stores.push(store);
            temp_dirs.push(temp); // 保持 temp_dir 活着
        }

        // 准备每个节点的事件
        let mut tasks = Vec::new();
        for (node_idx, store) in stores.iter().enumerate() {
            let store = store.clone();
            let task = tokio::spawn(async move {
                let events: Vec<RawEvent> = (0..events_per_node)
                    .map(|i| {
                        RawEvent::new(
                            1000000 + (node_idx * events_per_node + i) as u64,
                            2000000 + (node_idx * events_per_node + i) as u64,
                            &format!("node-{}", node_idx),
                            "concurrent_test",
                            None,
                            Some((node_idx * events_per_node + i) as i64),
                            None,
                            serde_json::json!({
                                "node": node_idx,
                                "seq": i
                            }),
                        )
                    })
                    .collect();

                store.append(&events).await
            });
            tasks.push(task);
        }

        // 等待所有写入完成
        let results = futures::future::join_all(tasks).await;

        // 验证：所有写入都成功
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "Node {} task panicked", i);
            let write_result = result.as_ref().unwrap();
            assert!(
                write_result.is_ok(),
                "Node {} write failed: {:?}",
                i,
                write_result
            );
            assert_eq!(write_result.as_ref().unwrap(), &(events_per_node as u64));
        }

        // 验证：总行数正确
        let batches = stores[0]
            .read_table_batches("concurrent_test")
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let expected_total = num_nodes * events_per_node;
        assert_eq!(
            total_rows, expected_total,
            "Expected {} total rows ({} nodes × {} events), got {}",
            expected_total, num_nodes, events_per_node, total_rows
        );

        println!(
            "✅ High concurrency test passed: {} nodes × {} events = {} total rows",
            num_nodes, events_per_node, total_rows
        );
    }

    /// 测试：冲突场景（两个节点几乎同时提交）
    #[tokio::test]
    #[ignore] // 需要真实 MinIO
    async fn test_conflict_resolution() {
        let temp_a = TempDir::new().unwrap();
        let temp_b = TempDir::new().unwrap();
        let catalog_a = temp_a.path().join("catalog.db");
        let catalog_b = temp_b.path().join("catalog.db");

        let config_a = create_shared_s3_config(catalog_a.to_str().unwrap());
        let config_b = create_shared_s3_config(catalog_b.to_str().unwrap());

        let store_a = Arc::new(EventLogStore::new_with_config(config_a).await.unwrap());
        let store_b = Arc::new(EventLogStore::new_with_config(config_b).await.unwrap());

        // 创建更大的批次，增加冲突概率
        let events_a: Vec<RawEvent> = (0..100)
            .map(|i| {
                RawEvent::new(
                    1000000 + i,
                    2000000 + i,
                    "node-a",
                    "conflict_test",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"batch": "a", "seq": i}),
                )
            })
            .collect();

        let events_b: Vec<RawEvent> = (1000..1100)
            .map(|i| {
                RawEvent::new(
                    1000000 + i,
                    2000000 + i,
                    "node-b",
                    "conflict_test",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"batch": "b", "seq": i}),
                )
            })
            .collect();

        // 重复多次以增加冲突概率
        for round in 0..5 {
            let (r1, r2) = tokio::join!(store_a.append(&events_a), store_b.append(&events_b),);

            assert!(r1.is_ok(), "Round {} node-a failed: {:?}", round, r1);
            assert!(r2.is_ok(), "Round {} node-b failed: {:?}", round, r2);

            println!("Round {} completed successfully", round);
        }

        // 验证：所有写入都被保留
        let batches = store_a.read_table_batches("conflict_test").await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let expected = (100 + 100) * 5; // 每轮 200 行，5 轮
        assert_eq!(
            total_rows, expected,
            "Expected {} rows after 5 rounds, got {}",
            expected, total_rows
        );

        println!("✅ Conflict resolution test passed: {} rows", total_rows);
    }

    /// 测试：本地文件模式（不需要 MinIO，总是运行）
    #[tokio::test]
    async fn test_local_fs_concurrent_writes() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("events");

        let config = StorageConfig::local_fs(data_dir.to_str().unwrap());

        // 两个 store 实例共享同一个本地目录（模拟两个节点访问共享 NFS）
        let store_a = Arc::new(EventLogStore::new_with_config(config.clone()).await.unwrap());
        let store_b = Arc::new(EventLogStore::new_with_config(config).await.unwrap());

        // 先通过 store_a 创建表（避免并发创建冲突）
        let init_event = RawEvent::new(
            0,
            0,
            "init",
            "local_test",
            None,
            None,
            None,
            serde_json::json!({"init": true}),
        );
        store_a.append(&[init_event]).await.unwrap();

        let events_a: Vec<RawEvent> = (0..10)
            .map(|i| {
                RawEvent::new(
                    1000000 + i,
                    2000000 + i,
                    "node-a",
                    "local_test",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"node": "a", "seq": i}),
                )
            })
            .collect();

        let events_b: Vec<RawEvent> = (100..110)
            .map(|i| {
                RawEvent::new(
                    1000000 + i,
                    2000000 + i,
                    "node-b",
                    "local_test",
                    None,
                    Some(i as i64),
                    None,
                    serde_json::json!({"node": "b", "seq": i}),
                )
            })
            .collect();

        // 并发写入（表已存在）
        let (r1, r2) = tokio::join!(store_a.append(&events_a), store_b.append(&events_b),);

        // 验证：打印错误以便调试
        if let Err(ref e) = r1 {
            eprintln!("Node A write error: {:?}", e);
        }
        if let Err(ref e) = r2 {
            eprintln!("Node B write error: {:?}", e);
        }

        assert!(r1.is_ok() && r2.is_ok(), "r1={:?}, r2={:?}", r1, r2);

        // 验证
        let batches = store_a.read_table_batches("local_test").await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 21, "Expected 21 rows (1 init + 10 from A + 10 from B)");

        println!("✅ Local FS concurrent write test passed");
    }
}
