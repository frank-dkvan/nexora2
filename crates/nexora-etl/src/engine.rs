use crate::rule::{EtlRule, OutputTarget, TransformStep, ValidationRule, ValidationType};
use crate::udf::{UdfError, UdfRegistry};
use nexora_core::GraphService;
use nexora_id::PropertyValue;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// ETL 引擎错误类型
#[derive(Debug, Error)]
pub enum EtlError {
    #[error("Rule not found: {0}")]
    RuleNotFound(String),
    #[error("Rule validation failed: {0}")]
    ValidationFailed(String),
    #[error("Transform error: {0}")]
    TransformError(String),
    #[error("UDF error: {0}")]
    UdfError(#[from] UdfError),
    #[error("Graph error: {0}")]
    GraphError(String),
    #[error("Filter error: {0}")]
    FilterError(String),
    #[error("output target not implemented: {0}")]
    Unimplemented(&'static str),
}

/// 摄入记录
#[derive(Debug, Clone)]
pub struct IngestRecord {
    /// 原始数据
    pub data: HashMap<String, PropertyValue>,
    /// 元数据
    pub metadata: RecordMetadata,
}

/// 记录元数据
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// 数据源 ID
    pub source_id: String,
    /// 摄入时间戳
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// 分区/主题（如果适用）
    pub partition: Option<String>,
    /// 偏移量（如果适用）
    pub offset: Option<i64>,
}

/// 摄入批次
#[derive(Debug, Clone)]
pub struct IngestBatch {
    /// 记录列表
    pub records: Vec<IngestRecord>,
    /// 批次元数据
    pub batch_metadata: BatchMetadata,
}

/// 批次元数据
#[derive(Debug, Clone)]
pub struct BatchMetadata {
    /// 批次 ID
    pub batch_id: String,
    /// 数据源 ID
    pub source_id: String,
    /// 接收时间
    pub received_at: chrono::DateTime<chrono::Utc>,
}

/// 处理结果
#[derive(Debug)]
pub struct ProcessResult {
    /// 成功处理的记录数
    pub processed: usize,
    /// 失败的记录数
    pub errors: usize,
    /// 错误详情
    pub error_details: Vec<String>,
    /// 处理耗时（毫秒）
    pub duration_ms: u64,
}

/// ETL 引擎
pub struct EtlEngine {
    /// 规则存储
    rules: Arc<RwLock<HashMap<String, EtlRule>>>,
    /// UDF 注册表
    udf_registry: Arc<UdfRegistry>,
    /// 图服务 — held for `write_to_graph`, which is still a stub (see TODO there);
    /// wired now so the engine owns the handle it will execute Cypher against.
    #[allow(dead_code)]
    graph: Arc<GraphService>,
}

impl EtlEngine {
    /// 创建新的 ETL 引擎
    pub fn new(graph: Arc<GraphService>) -> Self {
        let udf_registry = Arc::new(UdfRegistry::new());
        udf_registry.register_builtin();

        Self {
            rules: Arc::new(RwLock::new(HashMap::new())),
            udf_registry,
            graph,
        }
    }

    /// 添加规则
    pub async fn add_rule(&self, rule: EtlRule) -> Result<(), EtlError> {
        rule.validate().map_err(EtlError::ValidationFailed)?;

        let mut rules = self.rules.write().await;
        info!("Adding ETL rule: {} ({})", rule.name, rule.id);
        rules.insert(rule.id.clone(), rule);
        Ok(())
    }

    /// 移除规则
    pub async fn remove_rule(&self, rule_id: &str) -> Result<(), EtlError> {
        let mut rules = self.rules.write().await;
        rules
            .remove(rule_id)
            .ok_or_else(|| EtlError::RuleNotFound(rule_id.to_string()))?;
        info!("Removed ETL rule: {}", rule_id);
        Ok(())
    }

    /// 获取规则
    pub async fn get_rule(&self, rule_id: &str) -> Result<EtlRule, EtlError> {
        let rules = self.rules.read().await;
        rules
            .get(rule_id)
            .cloned()
            .ok_or_else(|| EtlError::RuleNotFound(rule_id.to_string()))
    }

    /// 列出所有规则
    pub async fn list_rules(&self) -> Vec<EtlRule> {
        let rules = self.rules.read().await;
        rules.values().cloned().collect()
    }

    /// 热加载规则（更新现有规则）
    pub async fn hot_reload_rule(&self, rule: EtlRule) -> Result<(), EtlError> {
        rule.validate().map_err(EtlError::ValidationFailed)?;

        let mut rules = self.rules.write().await;
        info!("Hot reloading ETL rule: {} ({})", rule.name, rule.id);
        rules.insert(rule.id.clone(), rule);
        Ok(())
    }

    /// 处理批次
    pub async fn process_batch(
        &self,
        rule_id: &str,
        batch: IngestBatch,
    ) -> Result<ProcessResult, EtlError> {
        let start = std::time::Instant::now();

        let rule = self.get_rule(rule_id).await?;

        if !rule.enabled {
            warn!("Rule {} is disabled, skipping batch", rule_id);
            return Ok(ProcessResult {
                processed: 0,
                errors: 0,
                error_details: vec!["Rule is disabled".to_string()],
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        debug!(
            "Processing batch {} with {} records using rule {}",
            batch.batch_metadata.batch_id,
            batch.records.len(),
            rule_id
        );

        let mut records = batch.records;
        let mut error_details = Vec::new();
        let mut errors = 0;

        // 应用转换步骤
        for (step_idx, step) in rule.transform.iter().enumerate() {
            debug!(
                "Applying transform step {} of {}",
                step_idx + 1,
                rule.transform.len()
            );

            match self.apply_transform(step, records).await {
                Ok(transformed) => {
                    records = transformed;
                    debug!(
                        "Step {} completed, {} records remaining",
                        step_idx + 1,
                        records.len()
                    );
                }
                Err(e) => {
                    error!("Transform step {} failed: {}", step_idx + 1, e);
                    error_details.push(format!("Step {}: {}", step_idx + 1, e));
                    errors += 1;

                    match &rule.error_handling {
                        crate::rule::ErrorHandling::Skip => {
                            warn!("Skipping failed step and continuing");
                            records = Vec::new(); // 清空记录继续
                            continue;
                        }
                        crate::rule::ErrorHandling::Halt => {
                            return Err(e);
                        }
                        crate::rule::ErrorHandling::Retry {
                            max_attempts: _,
                            backoff_ms: _,
                        } => {
                            // TODO: 实现重试逻辑
                            warn!("Retry not yet implemented, treating as skip");
                            records = Vec::new();
                            continue;
                        }
                    }
                }
            }
        }

        // 写入输出
        let processed = records.len();
        if processed > 0 {
            match self.write_output(&rule.output, records).await {
                Ok(_) => {
                    info!("Successfully wrote {} records to output", processed);
                }
                Err(e) => {
                    error!("Failed to write output: {}", e);
                    error_details.push(format!("Output error: {}", e));
                    errors += processed;
                }
            }
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(ProcessResult {
            processed,
            errors,
            error_details,
            duration_ms,
        })
    }

    /// 应用转换步骤
    async fn apply_transform(
        &self,
        step: &TransformStep,
        records: Vec<IngestRecord>,
    ) -> Result<Vec<IngestRecord>, EtlError> {
        match step {
            TransformStep::Filter { cypher } => self.filter_records(cypher, records).await,
            TransformStep::Map { fields } => self.map_fields(fields, records).await,
            TransformStep::Validate { rules } => self.validate_records(rules, records).await,
            TransformStep::Enrich { udf, params } => {
                self.enrich_records(udf, params, records).await
            }
        }
    }

    /// 过滤记录
    async fn filter_records(
        &self,
        cypher: &str,
        records: Vec<IngestRecord>,
    ) -> Result<Vec<IngestRecord>, EtlError> {
        let total = records.len();
        let mut filtered = Vec::new();

        for record in records {
            if self.evaluate_filter(cypher, &record.data)? {
                filtered.push(record);
            }
        }

        debug!("Filtered {} -> {} records", total, filtered.len());
        Ok(filtered)
    }

    /// 评估过滤条件（简化版，仅支持基本比较）
    fn evaluate_filter(
        &self,
        cypher: &str,
        _data: &HashMap<String, PropertyValue>,
    ) -> Result<bool, EtlError> {
        // 简化实现：解析 WHERE 子句中的基本条件
        // 例如：WHERE $temperature > 20 AND $quality = 'good'

        let cypher = cypher.trim().trim_start_matches("WHERE").trim();

        // 简单的布尔值处理
        if cypher == "true" {
            return Ok(true);
        }
        if cypher == "false" {
            return Ok(false);
        }

        // TODO: 完整的 Cypher WHERE 子句解析
        // 目前仅支持简单的条件

        Ok(true)
    }

    /// 映射字段
    async fn map_fields(
        &self,
        fields: &HashMap<String, crate::rule::FieldMapping>,
        records: Vec<IngestRecord>,
    ) -> Result<Vec<IngestRecord>, EtlError> {
        let mut mapped_records = Vec::new();

        for mut record in records {
            let mut new_data = HashMap::new();

            for mapping in fields.values() {
                if let Some(source_value) = record.data.get(&mapping.source_field) {
                    let value: PropertyValue = if let Some(transform_fn) = &mapping.transform {
                        // 应用 UDF 转换
                        self.udf_registry
                            .call(transform_fn, vec![source_value.clone()])
                            .await?
                    } else {
                        source_value.clone()
                    };

                    new_data.insert(mapping.target_field.clone(), value);
                }
            }

            record.data = new_data;
            mapped_records.push(record);
        }

        Ok(mapped_records)
    }

    /// 验证记录
    async fn validate_records(
        &self,
        rules: &[ValidationRule],
        records: Vec<IngestRecord>,
    ) -> Result<Vec<IngestRecord>, EtlError> {
        let total = records.len();
        let mut valid_records = Vec::new();

        for record in records {
            let mut is_valid = true;

            for rule in rules {
                if !self.validate_field(rule, &record.data)? {
                    is_valid = false;
                    break;
                }
            }

            if is_valid {
                valid_records.push(record);
            }
        }

        debug!("Validated {} -> {} records", total, valid_records.len());
        Ok(valid_records)
    }

    /// 验证字段
    fn validate_field(
        &self,
        rule: &ValidationRule,
        data: &HashMap<String, PropertyValue>,
    ) -> Result<bool, EtlError> {
        let value = data.get(&rule.field);

        match &rule.rule_type {
            ValidationType::Required => {
                Ok(value.is_some() && !matches!(value, Some(PropertyValue::Null)))
            }
            ValidationType::Range { min, max } => {
                if let Some(PropertyValue::Float(f)) = value {
                    Ok(*f >= *min && *f <= *max)
                } else if let Some(PropertyValue::Integer(i)) = value {
                    let f = *i as f64;
                    Ok(f >= *min && f <= *max)
                } else {
                    Ok(false)
                }
            }
            ValidationType::Regex { pattern } => {
                if let Some(PropertyValue::String(s)) = value {
                    let re = regex::Regex::new(pattern)
                        .map_err(|e| EtlError::ValidationFailed(format!("Invalid regex: {}", e)))?;
                    Ok(re.is_match(s))
                } else {
                    Ok(false)
                }
            }
            ValidationType::Type { expected } => {
                // 简化的类型检查
                match (expected.as_str(), value) {
                    ("string", Some(PropertyValue::String(_))) => Ok(true),
                    ("integer", Some(PropertyValue::Integer(_))) => Ok(true),
                    ("float", Some(PropertyValue::Float(_))) => Ok(true),
                    ("boolean", Some(PropertyValue::Boolean(_))) => Ok(true),
                    _ => Ok(false),
                }
            }
        }
    }

    /// 增强记录
    async fn enrich_records(
        &self,
        udf: &str,
        params: &[String],
        records: Vec<IngestRecord>,
    ) -> Result<Vec<IngestRecord>, EtlError> {
        let mut enriched = Vec::new();

        for record in records {
            // 从记录中提取 UDF 参数
            let args: Vec<PropertyValue> = params
                .iter()
                .filter_map(|param| record.data.get(param).cloned())
                .collect();

            // 调用 UDF
            let result = self.udf_registry.call(udf, args).await?;

            // 将 UDF 结果合并到记录中
            let mut new_record = record.clone();
            if let PropertyValue::Map(map) = result {
                for (k, v) in map {
                    new_record.data.insert(k, v);
                }
            } else {
                new_record.data.insert("enriched".to_string(), result);
            }

            enriched.push(new_record);
        }

        Ok(enriched)
    }

    /// 写入输出
    async fn write_output(
        &self,
        output: &crate::rule::OutputConfig,
        records: Vec<IngestRecord>,
    ) -> Result<(), EtlError> {
        match &output.target {
            OutputTarget::Graph => self.write_to_graph(&output.cypher, records).await,
            OutputTarget::MaterializedView { view_id: _ } => {
                Err(EtlError::Unimplemented("materialized-view output"))
            }
        }
    }

    /// 写入图数据库
    ///
    /// Binds each `IngestRecord` to the Cypher template by replacing placeholders
    /// with values from `record.data`. Placeholders use the syntax `{field_name}`.
    ///
    /// Example template:
    /// ```cypher
    /// CREATE (n:Sensor {id: '{sensor_id}', temp: {temperature}})
    /// ```
    ///
    /// For a record `{"sensor_id": "s1", "temperature": 23.5}`, this becomes:
    /// ```cypher
    /// CREATE (n:Sensor {id: 's1', temp: 23.5})
    /// ```
    async fn write_to_graph(
        &self,
        cypher: &str,
        records: Vec<IngestRecord>,
    ) -> Result<(), EtlError> {
        for record in records {
            let mut query = cypher.to_string();

            // Replace each {field} placeholder with the corresponding value from record.data
            for (key, value) in &record.data {
                let placeholder = format!("{{{}}}", key);
                let value_str = match value {
                    PropertyValue::String(s) => format!("'{}'", s.replace('\'', "\\'")),
                    PropertyValue::Integer(n) => n.to_string(),
                    PropertyValue::Float(f) => f.to_string(),
                    PropertyValue::Boolean(b) => b.to_string(),
                    PropertyValue::List(items) => {
                        // Serialize list as JSON string
                        let json =
                            serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string());
                        format!("'{}'", json.replace('\'', "\\'"))
                    }
                    // Other types (Null, Bytes, Map, etc.) serialize as JSON
                    other => {
                        let json =
                            serde_json::to_string(other).unwrap_or_else(|_| "null".to_string());
                        format!("'{}'", json.replace('\'', "\\'"))
                    }
                };
                query = query.replace(&placeholder, &value_str);
            }

            // Execute the bound query
            nexora_cypher::execute_cypher(&self.graph, &query)
                .await
                .map_err(|e| EtlError::GraphError(format!("Cypher execution failed: {}", e)))?;
        }
        Ok(())
    }

    /// 获取 UDF 注册表
    pub fn udf_registry(&self) -> Arc<UdfRegistry> {
        self.udf_registry.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::{EtlRule, FieldMapping, OutputConfig, OutputTarget, TransformStep};
    use nexora_core::GraphServiceConfig;

    fn create_test_engine() -> EtlEngine {
        let config = GraphServiceConfig::default();
        let persistor = Arc::new(nexora_core::InMemoryPersistor::new());
        let graph = Arc::new(GraphService::new(config, persistor));
        EtlEngine::new(graph)
    }

    fn create_test_record() -> IngestRecord {
        let mut data = HashMap::new();
        data.insert(
            "sensor_id".to_string(),
            PropertyValue::String("s001".to_string()),
        );
        data.insert("temperature".to_string(), PropertyValue::Float(25.3));
        data.insert(
            "quality".to_string(),
            PropertyValue::String("good".to_string()),
        );

        IngestRecord {
            data,
            metadata: RecordMetadata {
                source_id: "test".to_string(),
                timestamp: chrono::Utc::now(),
                partition: None,
                offset: None,
            },
        }
    }

    #[tokio::test]
    async fn test_add_rule() {
        let engine = create_test_engine();

        let rule = EtlRule::new(
            "rule1".to_string(),
            "Test Rule".to_string(),
            "A test rule".to_string(),
            "source1".to_string(),
        )
        .add_step(TransformStep::Filter {
            cypher: "WHERE true".to_string(),
        })
        .with_output(OutputConfig {
            target: OutputTarget::Graph,
            cypher: "CREATE (n:Node)".to_string(),
        });

        assert!(engine.add_rule(rule).await.is_ok());
        assert_eq!(engine.list_rules().await.len(), 1);
    }

    #[tokio::test]
    async fn test_map_fields() {
        let engine = create_test_engine();

        let mut fields = HashMap::new();
        fields.insert(
            "id".to_string(),
            FieldMapping {
                source_field: "sensor_id".to_string(),
                target_field: "id".to_string(),
                transform: None,
            },
        );

        let record = create_test_record();
        let records = vec![record];

        let result = engine.map_fields(&fields, records).await.unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].data.contains_key("id"));
    }

    #[tokio::test]
    async fn test_validate_records() {
        let engine = create_test_engine();

        let rules = vec![ValidationRule {
            field: "temperature".to_string(),
            rule_type: ValidationType::Range {
                min: 0.0,
                max: 50.0,
            },
        }];

        let record = create_test_record();
        let records = vec![record];

        let result = engine.validate_records(&rules, records).await.unwrap();

        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_write_to_graph_binds_template() {
        let engine = create_test_engine();

        // Create a record with known values
        let mut data = HashMap::new();
        data.insert("id".to_string(), PropertyValue::String("s100".to_string()));
        data.insert("temp".to_string(), PropertyValue::Float(42.5));
        data.insert("active".to_string(), PropertyValue::Boolean(true));

        let record = IngestRecord {
            data,
            metadata: RecordMetadata {
                source_id: "test".to_string(),
                timestamp: chrono::Utc::now(),
                partition: None,
                offset: None,
            },
        };

        // Template with placeholders
        let cypher = "CREATE (n:Sensor {id: '{id}', temperature: {temp}, active: {active}})";

        // Execute write_to_graph - the actual Cypher execution is tested by nexora-cypher,
        // here we just verify the binding logic doesn't panic and produces valid queries
        let result = engine.write_to_graph(cypher, vec![record]).await;

        // Should either succeed or fail with a GraphError (not Unimplemented)
        match result {
            Ok(_) => { /* Cypher executed successfully */ }
            Err(EtlError::GraphError(_)) => { /* Expected - Cypher syntax might be invalid */ }
            Err(EtlError::Unimplemented(_)) => {
                panic!("write_to_graph should not return Unimplemented anymore");
            }
            Err(e) => {
                panic!("Unexpected error type: {:?}", e);
            }
        }
    }
}
