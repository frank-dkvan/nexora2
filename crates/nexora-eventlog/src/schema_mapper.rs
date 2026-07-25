//! SchemaMapper — DomainPackage → Iceberg Schema 映射
//!
//! 负责:
//! 1. 将 DomainSchema 的 label 定义转换为 Iceberg Schema
//! 2. 处理字段类型映射(String, Int, Float, Boolean, Timestamp)
//! 3. 保留 provenance 列 + 添加业务字段

use anyhow::{Context, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use nexora_core::DomainPackage;
use std::sync::Arc;

pub struct SchemaMapper;

impl SchemaMapper {
    /// 从 DomainPackage 生成 Iceberg Schema
    ///
    /// Schema 结构:
    /// - Provenance 列 (固定): _event_id, _event_time, _source, _topic
    /// - 业务字段: 从 DomainSchema.labels 的 properties 提取
    pub fn map_domain_to_schema(pkg: &DomainPackage) -> Result<Arc<IcebergSchema>> {
        let mut fields = vec![
            // Provenance 列 (固定)
            NestedField::required(1, "_event_id", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(
                2,
                "_event_time",
                Type::Primitive(PrimitiveType::Timestamp),
            )
            .into(),
            NestedField::required(3, "_source", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(4, "_topic", Type::Primitive(PrimitiveType::String)).into(),
        ];

        let mut next_id = 5;

        // 从 DomainSchema.labels 提取字段定义
        for label in &pkg.schema.labels {
            for prop in &label.properties {
                let field_type = Self::map_property_type(&prop.prop_type)?;
                fields.push(NestedField::optional(next_id, &prop.name, field_type).into());
                next_id += 1;
            }
        }

        // 如果没有任何业务字段,添加一个 _payload 列作为回退
        if fields.len() == 4 {
            fields.push(
                NestedField::optional(5, "_payload", Type::Primitive(PrimitiveType::String))
                    .into(),
            );
        }

        IcebergSchema::builder()
            .with_fields(fields)
            .build()
            .map(Arc::new)
            .context("Failed to build Iceberg schema")
    }

    /// 映射字符串类型名 → Iceberg Type
    fn map_property_type(type_str: &str) -> Result<Type> {
        match type_str.to_lowercase().as_str() {
            "string" | "str" | "text" => Ok(Type::Primitive(PrimitiveType::String)),
            "int" | "integer" | "long" => Ok(Type::Primitive(PrimitiveType::Long)),
            "float" | "double" | "number" => Ok(Type::Primitive(PrimitiveType::Double)),
            "bool" | "boolean" => Ok(Type::Primitive(PrimitiveType::Boolean)),
            "timestamp" | "datetime" | "date" => Ok(Type::Primitive(PrimitiveType::Timestamp)),
            // 未知类型默认为 String
            _ => {
                tracing::warn!("Unknown property type '{}', defaulting to String", type_str);
                Ok(Type::Primitive(PrimitiveType::String))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_core::{DomainSchema, LabelDef, PropertyDef};

    #[test]
    fn test_map_empty_domain() {
        let pkg = DomainPackage {
            schema: DomainSchema {
                domain: "test".into(),
                version: "1.0".into(),
                description: None,
                extends: None,
                labels: vec![],
                edge_types: vec![],
                constraints: vec![],
                indexes: vec![],
            },
            mappings: vec![],
            standing_queries: vec![],
            materialized_views: vec![],
        };

        let schema = SchemaMapper::map_domain_to_schema(&pkg).unwrap();

        // 4 provenance + 1 _payload (回退)
        assert_eq!(schema.as_struct().fields().len(), 5);
    }

    #[test]
    fn test_map_domain_with_properties() {
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

        let schema = SchemaMapper::map_domain_to_schema(&pkg).unwrap();

        // 4 provenance + 2 业务字段
        assert_eq!(schema.as_struct().fields().len(), 6);

        // 验证字段名
        let field_names: Vec<_> = schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();

        assert!(field_names.contains(&"_event_id"));
        assert!(field_names.contains(&"device_id"));
        assert!(field_names.contains(&"temperature"));
    }
}
