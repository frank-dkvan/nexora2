//! RawEvent → Arrow RecordBatch 转换
//!
//! 负责:
//! 1. 将 RawEvent 转换为 Arrow RecordBatch
//! 2. Provenance 列优先级保护(不被 payload 覆盖)
//! 3. Object payload → 展平为列
//! 4. 非 object payload → _payload 列

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use nexora_core::RawEvent;
use serde_json::Value;
use std::sync::Arc;

/// Provenance 保留字段列表
const RESERVED_FIELDS: &[&str] = &[
    "_event_id",
    "_event_time",
    "_ingest_time",
    "_source",
    "_topic",
    "_partition",
    "_offset",
    "_subject",
    "_payload",
];

/// 将 RawEvent batch 转换为 Arrow RecordBatch
pub fn raw_events_to_record_batch(events: &[RawEvent]) -> Result<RecordBatch> {
    if events.is_empty() {
        anyhow::bail!("Cannot create RecordBatch from empty events");
    }

    // 提取 provenance 列
    let event_ids: Vec<String> = events
        .iter()
        .map(|e| e.event_id.to_string())
        .collect();

    let event_times: Vec<i64> = events
        .iter()
        .map(|e| e.event_time_us as i64)
        .collect();

    let sources: Vec<&str> = events.iter().map(|e| e.source.as_str()).collect();

    let topics: Vec<&str> = events.iter().map(|e| e.topic.as_str()).collect();

    // 构造 schema 和 columns
    let mut fields = vec![
        Field::new("_event_id", DataType::Utf8, false),
        Field::new(
            "_event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("_source", DataType::Utf8, false),
        Field::new("_topic", DataType::Utf8, false),
    ];

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(event_ids)),
        Arc::new(TimestampMicrosecondArray::from(event_times)),
        Arc::new(StringArray::from(sources)),
        Arc::new(StringArray::from(topics)),
    ];

    // Payload 列处理
    let first_payload = &events[0].payload;

    if let Value::Object(map) = first_payload {
        // Object payload → 展平为列
        let payload_columns = extract_payload_columns(events, map.keys());

        for (field_name, array) in payload_columns {
            fields.push(Field::new(&field_name, DataType::Utf8, true));
            columns.push(array);
        }
    } else {
        // 非 object payload → _payload 列
        let payload_array = serialize_non_object_payloads(events);
        fields.push(Field::new("_payload", DataType::Utf8, true));
        columns.push(payload_array);
    }

    let schema = Arc::new(ArrowSchema::new(fields));

    RecordBatch::try_new(schema, columns).context("Failed to create RecordBatch")
}

/// 从所有事件中提取 payload 列(展平 object payload)
fn extract_payload_columns(
    events: &[RawEvent],
    keys: impl Iterator<Item = impl AsRef<str>>,
) -> Vec<(String, ArrayRef)> {
    keys.filter_map(|k| {
        let key = k.as_ref();

        // Provenance 保护:冲突的字段跳过
        if RESERVED_FIELDS.contains(&key) {
            tracing::warn!(
                "Payload field '{}' collides with provenance, skipped",
                key
            );
            return None;
        }

        // 提取该字段的所有值
        let values: Vec<Option<String>> = events
            .iter()
            .map(|event| {
                if let Value::Object(ref map) = event.payload {
                    map.get(key).map(serialize_json_value)
                } else {
                    None
                }
            })
            .collect();

        let array = StringArray::from(values);
        Some((key.to_string(), Arc::new(array) as ArrayRef))
    })
    .collect()
}

/// 序列化非 object payload 到 _payload 列
fn serialize_non_object_payloads(events: &[RawEvent]) -> ArrayRef {
    let values: Vec<String> = events
        .iter()
        .map(|event| serialize_json_value(&event.payload))
        .collect();

    Arc::new(StringArray::from(values))
}

/// 将 JSON Value 序列化为字符串
fn serialize_json_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn test_object_payload_to_record_batch() {
        let events = vec![
            RawEvent::new(
                1_700_000_000_000_000, // event_time_us
                1_700_000_000_000_000, // ingest_time_us
                "kafka",
                "orders",
                Some(0),
                Some(100),
                None,
                json!({"order_id": "A1", "amount": 99}),
            ),
            RawEvent::new(
                1_700_000_001_000_000,
                1_700_000_001_000_000,
                "kafka",
                "orders",
                Some(0),
                Some(101),
                None,
                json!({"order_id": "A2", "amount": 150}),
            ),
        ];

        let batch = raw_events_to_record_batch(&events).unwrap();

        // 验证行数
        assert_eq!(batch.num_rows(), 2);

        // 验证列数:4 provenance + 2 payload
        assert_eq!(batch.num_columns(), 6);

        // 验证列名
        let schema = batch.schema();
        assert!(schema.column_with_name("_event_id").is_some());
        assert!(schema.column_with_name("_event_time").is_some());
        assert!(schema.column_with_name("_source").is_some());
        assert!(schema.column_with_name("_topic").is_some());
        assert!(schema.column_with_name("order_id").is_some());
        assert!(schema.column_with_name("amount").is_some());

        // 验证数据
        let order_id_col = batch
            .column_by_name("order_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(order_id_col.value(0), "A1");
        assert_eq!(order_id_col.value(1), "A2");
    }

    #[test]
    fn test_non_object_payload_to_record_batch() {
        let events = vec![
            RawEvent::new(
                1_700_000_000_000_000,
                1_700_000_000_000_000,
                "file",
                "raw",
                None,
                None,
                None,
                json!("hello"),
            ),
            RawEvent::new(
                1_700_000_001_000_000,
                1_700_000_001_000_000,
                "file",
                "raw",
                None,
                None,
                None,
                json!(123),
            ),
        ];

        let batch = raw_events_to_record_batch(&events).unwrap();

        // 验证列数:4 provenance + 1 _payload
        assert_eq!(batch.num_columns(), 5);

        // 验证 _payload 列存在
        let schema = batch.schema();
        assert!(schema.column_with_name("_payload").is_some());

        // 验证数据
        let payload_col = batch
            .column_by_name("_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // serialize_json_value 对 String 返回原值(不带引号)
        assert_eq!(payload_col.value(0), "hello");
        assert_eq!(payload_col.value(1), "123");
    }

    #[test]
    fn test_provenance_not_overwritten() {
        // Payload 包含 _event_id 字段(冲突)
        let fake_id = Uuid::new_v4();

        let events = vec![RawEvent::new(
            1_700_000_000_000_000,
            1_700_000_000_000_000,
            "test",
            "test_topic",
            None,
            None,
            None,
            json!({"_event_id": fake_id.to_string(), "data": "123"}),
        )];

        let batch = raw_events_to_record_batch(&events).unwrap();

        // _event_id 列应该是真实的自动生成的 UUID,不是 payload 的 fake_id
        let event_id_col = batch
            .column_by_name("_event_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // 验证 event_id 不是 fake_id
        assert_ne!(event_id_col.value(0), fake_id.to_string());

        // 验证 event_id 是有效的 UUID 格式
        assert!(Uuid::parse_str(event_id_col.value(0)).is_ok());

        // data 列应该存在
        assert!(batch.column_by_name("data").is_some());

        // 不应该有额外的 _event_id 列(payload 的被跳过了)
        assert_eq!(batch.num_columns(), 5); // 4 provenance + 1 data
    }

    #[test]
    fn test_missing_fields_handled_as_null() {
        let events = vec![
            RawEvent::new(
                1_700_000_000_000_000,
                1_700_000_000_000_000,
                "test",
                "test_topic",
                None,
                None,
                None,
                json!({"field_a": "value1", "field_b": "value2"}),
            ),
            RawEvent::new(
                1_700_000_001_000_000,
                1_700_000_001_000_000,
                "test",
                "test_topic",
                None,
                None,
                None,
                json!({"field_a": "value3"}), // field_b 缺失
            ),
        ];

        let batch = raw_events_to_record_batch(&events).unwrap();

        let field_b_col = batch
            .column_by_name("field_b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(field_b_col.value(0), "value2");
        assert!(field_b_col.is_null(1)); // 第二行 field_b 是 null
    }
}
