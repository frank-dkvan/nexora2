#![cfg(feature = "olap")]
//! 集成测试: 根据 DomainPackage 创建 Iceberg 表

use nexora_core::{DomainPackage, DomainSchema, LabelDef, PropertyDef};
use nexora_eventlog::EventLogStore;
use tempfile::TempDir;

#[tokio::test]
async fn test_ensure_table_from_domain() {
    // 1. 创建临时目录
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    // 2. 创建 EventLogStore
    let store = EventLogStore::new(data_dir).await.unwrap();

    // 3. 构造 DomainPackage
    let pkg = DomainPackage {
        schema: DomainSchema {
            domain: "iot".into(),
            version: "1.0".into(),
            description: None,
            extends: None,
            labels: vec![LabelDef {
                name: "Sensor".into(),
                description: None,
                properties: vec![
                    PropertyDef {
                        name: "device_id".into(),
                        prop_type: "string".into(),
                        required: Some(true),
                        indexed: None,
                        description: None,
                        enum_values: None,
                        min: None,
                        max: None,
                        default: None,
                    },
                    PropertyDef {
                        name: "temperature".into(),
                        prop_type: "float".into(),
                        required: Some(false),
                        indexed: None,
                        description: None,
                        enum_values: None,
                        min: None,
                        max: None,
                        default: None,
                    },
                ],
                extends: None,
            }],
            edge_types: vec![],
            constraints: vec![],
            indexes: vec![],
        },
        mappings: vec![],
        standing_queries: vec![],
        materialized_views: vec![],
    };

    // 4. 根据 DomainPackage 创建表
    let table = store
        .ensure_table_from_domain("iot_sensors", &pkg)
        .await
        .unwrap();

    // 5. 验证表存在
    assert_eq!(table.identifier().name(), "iot_sensors");

    // 6. 验证 schema 包含业务字段
    let schema = table.metadata().current_schema();
    let field_names: Vec<_> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.as_str())
        .collect();

    assert!(field_names.contains(&"_event_id"));
    assert!(field_names.contains(&"_event_time"));
    assert!(field_names.contains(&"device_id"));
    assert!(field_names.contains(&"temperature"));

    // 7. 验证分区存在
    let partition_spec = table.metadata().default_partition_spec();
    assert!(
        !partition_spec.fields().is_empty(),
        "Should have partition fields"
    );

    println!(
        "✅ Table created from domain with {} fields",
        field_names.len()
    );
}

#[tokio::test]
async fn test_schema_compatibility_check() {
    let temp_dir = TempDir::new().unwrap();
    let store = EventLogStore::new(temp_dir.path()).await.unwrap();

    let pkg = DomainPackage {
        schema: DomainSchema {
            domain: "test".into(),
            version: "1.0".into(),
            description: None,
            extends: None,
            labels: vec![LabelDef {
                name: "TestLabel".into(),
                description: None,
                properties: vec![PropertyDef {
                    name: "field_a".into(),
                    prop_type: "string".into(),
                    required: Some(true),
                    indexed: None,
                    description: None,
                    enum_values: None,
                    min: None,
                    max: None,
                    default: None,
                }],
                extends: None,
            }],
            edge_types: vec![],
            constraints: vec![],
            indexes: vec![],
        },
        mappings: vec![],
        standing_queries: vec![],
        materialized_views: vec![],
    };

    // 首次创建
    let _table1 = store
        .ensure_table_from_domain("test_topic", &pkg)
        .await
        .unwrap();

    // 再次调用应该成功(兼容)
    let _table2 = store
        .ensure_table_from_domain("test_topic", &pkg)
        .await
        .unwrap();

    println!("✅ Schema compatibility check passed");
}
