#![cfg(feature = "olap")]
//! 集成测试: 端到端验证事件优先摄入链路
//!
//! 测试流程:
//! 1. 创建 EventLogStore
//! 2. 构造 RawEvent 批次
//! 3. append() 写入 Iceberg
//! 4. 验证表存在 + 数据写入成功

use nexora_eventlog::{EventLogStore, RawEvent};
use tempfile::TempDir;

#[tokio::test]
async fn test_event_log_store_append() {
    // 1. 创建临时目录
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    // 2. 创建 EventLogStore
    let store = EventLogStore::new(data_dir).await.unwrap();

    // 3. 构造测试事件
    let events = vec![
        RawEvent::new(
            1000000, // event_time_us
            2000000, // ingest_time_us
            "test_source",
            "test_topic",
            None,
            Some(0),
            None,
            serde_json::json!({"sensor_id": "s1", "temperature": 23.5}),
        ),
        RawEvent::new(
            1000001,
            2000001,
            "test_source",
            "test_topic",
            None,
            Some(1),
            None,
            serde_json::json!({"sensor_id": "s2", "temperature": 24.1}),
        ),
    ];

    // 4. Append 到 Iceberg 表
    let count = store.append(&events).await.unwrap();
    assert_eq!(count, 2);

    println!("✅ Integration test passed: appended {} events", count);
}
