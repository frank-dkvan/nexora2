//! Dependency + API spike: validates Iceberg 0.9.1 + DataFusion 52.2 stack.
//!
//! **Spike outcome (阶段 0)**:
//! - ✅ **Dependency resolution**: arrow 57.1 + datafusion 52.2 + iceberg 0.9.1 解析成功,无版本冲突
//! - ✅ **Type compatibility**: Arrow/DataFusion/Iceberg 类型能共存,编译通过
//! - ✅ **Catalog + table creation**: MemoryCatalog + OpenDalStorageFactory::Memory 成功创建表
//! - ✅ **Hidden partition**: days(event_time) 隐藏分区声明成功
//! - ✅ **DataFusion query**: IcebergCatalogProvider 能查询空表
//!
//! **关键发现**: iceberg-rust 0.9.1 的 StorageFactory 实现在独立 crate `iceberg-storage-opendal`,
//! 通过 `OpenDalStorageFactory::Fs` / `::Memory` 暴露。不是 API 缺陷,是模块化设计。
//!
//! **阶段 0 判定:完全通过**——依赖地基 + catalog/table API 均可用,可进入阶段 1。

/// Minimal RawEvent→RecordBatch converter for the spike (simplified schema).
/// Real one in phase 1 will handle nested payload, provenance collision, etc.
#[cfg(test)]
use arrow::array::{ArrayRef, StringArray, TimestampMicrosecondArray};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
#[cfg(test)]
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
fn raw_event_to_record_batch(
    event_id: &str,
    event_time_us: i64,
    source: &str,
    topic: &str,
    payload_summary: &str,
) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("_event_id", DataType::Utf8, false),
        Field::new(
            "_event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("_source", DataType::Utf8, false),
        Field::new("_topic", DataType::Utf8, false),
        Field::new("payload_summary", DataType::Utf8, true), // spike: one demo field
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![event_id])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![event_time_us])) as ArrayRef,
            Arc::new(StringArray::from(vec![source])) as ArrayRef,
            Arc::new(StringArray::from(vec![topic])) as ArrayRef,
            Arc::new(StringArray::from(vec![payload_summary])) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spike_record_batch_round_trip() {
        let batch = raw_event_to_record_batch(
            "evt-001",
            1_700_000_000_000_000, // 2023-11-14 22:13:20 UTC
            "test-source",
            "test-topic",
            "sample payload",
        )
        .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().fields().len(), 5);
    }

    /// Type-level validation: proves arrow/datafusion/iceberg can coexist.
    /// This is the MINIMUM spike outcome — if this compiles, the dep tree is good.
    #[test]
    fn spike_types_coexist() {
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use iceberg::spec::Transform;

        // arrow-schema type
        let _schema = Schema::new(vec![Field::new("event_time", DataType::Int64, false)]);
        // datafusion type
        let _ctx = SessionContext::new();
        // arrow type (must be the SAME arrow version iceberg uses)
        let _rb: Option<RecordBatch> = None;
        // iceberg hidden-partition transform — validates iceberg is usable
        let _transform = Transform::Day;

        assert_eq!(DataType::Int64, DataType::Int64);
        assert_eq!(Transform::Day, Transform::Day);
    }

    /// Full roundtrip: MemoryCatalog + table creation + days partition + DataFusion query.
    /// This validates the complete catalog/table/query path with the corrected
    /// `iceberg-storage-opendal` usage.
    #[tokio::test]
    async fn spike_iceberg_datafusion_roundtrip() -> anyhow::Result<()> {
        use datafusion::prelude::SessionContext;
        use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type};
        use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};
        use iceberg_datafusion::IcebergCatalogProvider;
        use iceberg_storage_opendal::OpenDalStorageFactory;
        use std::collections::HashMap;

        // Step 1: Create MemoryCatalog with OpenDalStorageFactory::Memory
        let warehouse_location = tempfile::tempdir()?.path().to_str().unwrap().to_string();

        use iceberg::memory::MemoryCatalogBuilder;
        let props = HashMap::from([("warehouse".to_string(), warehouse_location.clone())]);
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::Memory))
            .load("test_catalog", props)
            .await?;

        // Step 2: Create namespace + Iceberg table with days(event_time) partition
        let namespace = NamespaceIdent::new("events".into());
        catalog.create_namespace(&namespace, HashMap::new()).await?;

        let iceberg_schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "_event_id", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(2, "_event_time", Type::Primitive(PrimitiveType::Timestamp))
                    .into(),
                NestedField::required(3, "_source", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(4, "_topic", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(5, "payload_summary", Type::Primitive(PrimitiveType::String))
                    .into(),
            ])
            .build()?;

        let partition_spec =
            iceberg::spec::PartitionSpec::builder(Arc::new(iceberg_schema.clone()))
                .with_spec_id(0)
                .add_partition_field("_event_time", "_event_time_day", Transform::Day)?
                .build()?;

        let table_creation = TableCreation::builder()
            .name("test_events".into())
            .schema(iceberg_schema)
            .partition_spec(partition_spec)
            .build();

        catalog.create_table(&namespace, table_creation).await?;

        // Step 3: Query via DataFusion IcebergCatalogProvider
        let ctx = SessionContext::new();
        let catalog_provider = IcebergCatalogProvider::try_new(Arc::new(catalog)).await?;
        ctx.register_catalog("iceberg", Arc::new(catalog_provider));

        let df = ctx
            .sql("SELECT _event_id, _source FROM iceberg.events.test_events")
            .await?;
        let results = df.collect().await?;

        // Step 4: Assert empty (no data appended, but query path validated)
        assert_eq!(results.len(), 0, "table should be empty");

        // Step 5: Verify schema round-tripped
        let table = ctx.table("iceberg.events.test_events").await?;
        let schema = table.schema();
        assert_eq!(schema.fields().len(), 5, "should have 5 fields");

        Ok(())
    }
}
