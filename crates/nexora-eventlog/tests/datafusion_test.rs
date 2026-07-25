#![cfg(feature = "olap")]
//! 集成测试: DataFusion SQL 查询 Iceberg 表

use nexora_core::RawEvent;
use nexora_eventlog::{DataFusionEventStore, EventLogStore};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

// 注: 阶段 3 DataFusion catalog 集成受 iceberg-datafusion 0.9.1 API 限制暂时受阻
// (IcebergCatalogProvider::try_new 运行时失败)。
// 阶段 4 的 ViewRefresher 已通过 DataFusion MemTable 方式绕过此问题实现聚合查询。
// 这些测试标记为 ignore,待 iceberg-datafusion 0.10+ 或改用直接注册方案后启用。
#[ignore = "iceberg-datafusion 0.9.1 catalog integration blocked - see stage-3 docs"]
#[tokio::test]
async fn test_datafusion_query() {
    let temp_dir = TempDir::new().unwrap();

    // 1. 创建 EventLogStore 并写入数据
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    let events = vec![
        RawEvent::new(
            1_700_000_000_000_000,
            1_700_000_000_000_000,
            "test",
            "sensors",
            None,
            None,
            None,
            json!({"device_id": "D1", "temperature": 23.5}),
        ),
        RawEvent::new(
            1_700_000_001_000_000,
            1_700_000_001_000_000,
            "test",
            "sensors",
            None,
            None,
            None,
            json!({"device_id": "D2", "temperature": 25.0}),
        ),
    ];

    store.append(&events).await.unwrap();

    // 2. 创建 DataFusionEventStore
    let df_store = DataFusionEventStore::new(store).await.unwrap();

    // 3. 执行 SQL 查询
    let batches = df_store
        .execute_query("SELECT * FROM iceberg.nexora_events.sensors")
        .await
        .unwrap();

    // 4. 验证结果
    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    println!("✅ DataFusion query returned {} rows", total_rows);
}

#[ignore = "iceberg-datafusion 0.9.1 catalog integration blocked - see stage-3 docs"]
#[tokio::test]
async fn test_datafusion_filter_query() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 写入多条数据
    for i in 1..=5 {
        let events = vec![RawEvent::new(
            1_700_000_000_000_000 + (i as u64 * 1_000_000),
            1_700_000_000_000_000 + (i as u64 * 1_000_000),
            "test",
            "metrics",
            None,
            None,
            None,
            json!({"value": i * 10}),
        )];
        store.append(&events).await.unwrap();
    }

    let df_store = DataFusionEventStore::new(store).await.unwrap();

    // 带过滤条件的查询
    let batches = df_store
        .execute_query("SELECT value FROM iceberg.nexora_events.metrics WHERE value > 20")
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3); // 30, 40, 50

    println!("✅ Filtered query returned {} rows", total_rows);
}
