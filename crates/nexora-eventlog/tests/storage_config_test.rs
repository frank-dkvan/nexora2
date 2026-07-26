#![cfg(feature = "olap")]
//! 集成测试: StorageConfig - 存储后端配置 (阶段 5 Phase 1)

use nexora_core::RawEvent;
use nexora_eventlog::{EventLogStore, StorageConfig};
use serde_json::json;
use tempfile::TempDir;

#[tokio::test]
async fn test_new_with_local_fs_config() {
    let temp_dir = TempDir::new().unwrap();

    // 使用 StorageConfig::local_fs 创建
    let config = StorageConfig::local_fs(temp_dir.path().display().to_string());
    let store = EventLogStore::new_with_config(config).await.unwrap();

    // 验证可以正常写入
    let events = vec![RawEvent::new(
        1_700_000_000_000_000,
        1_700_000_000_000_000,
        "test",
        "config_test",
        None,
        None,
        None,
        json!({"key": "value"}),
    )];

    let count = store.append(&events).await.unwrap();
    assert_eq!(count, 1);

    println!("✅ StorageConfig::local_fs works end-to-end");
}

#[tokio::test]
async fn test_new_backward_compatible() {
    // 验证旧的 new() 方法仍然工作 (向后兼容)
    let temp_dir = TempDir::new().unwrap();

    let store = EventLogStore::new(temp_dir.path()).await.unwrap();

    let events = vec![RawEvent::new(
        1_700_000_000_000_000,
        1_700_000_000_000_000,
        "test",
        "compat_test",
        None,
        None,
        None,
        json!({"data": 123}),
    )];

    let count = store.append(&events).await.unwrap();
    assert_eq!(count, 1);

    println!("✅ Backward compatible new() still works");
}

#[test]
fn test_s3_config_construction() {
    // 验证 S3 配置构造 (不需要真实 S3 服务)
    let config = StorageConfig::s3(
        "http://localhost:9000",
        "test-bucket",
        "us-east-1",
        "minioadmin",
        "minioadmin",
        Some("events".into()),
        true,
        "/tmp/test_catalog.db",
    );

    assert!(config.is_s3());
    assert_eq!(
        config.warehouse_location(),
        "s3://test-bucket/events/warehouse"
    );

    let props = config.catalog_props();
    assert_eq!(props.get("s3.endpoint").unwrap(), "http://localhost:9000");
    assert_eq!(props.get("s3.access-key-id").unwrap(), "minioadmin");

    println!("✅ S3 config construction works");
}
