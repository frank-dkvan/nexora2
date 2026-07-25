use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// ETL 规则定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtlRule {
    /// 规则唯一标识
    pub id: String,
    /// 规则名称
    pub name: String,
    /// 规则描述
    pub description: String,
    /// 数据源 ID
    pub source_id: String,
    /// 是否启用
    pub enabled: bool,
    /// 转换步骤
    pub transform: Vec<TransformStep>,
    /// 输出配置
    pub output: OutputConfig,
    /// 错误处理策略
    pub error_handling: ErrorHandling,
    /// 创建时间
    pub created_at: DateTime<Utc>,
    /// 更新时间
    pub updated_at: DateTime<Utc>,
}

/// 转换步骤
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransformStep {
    /// 过滤：根据条件过滤记录
    Filter {
        /// Cypher WHERE 子句
        cypher: String,
    },
    /// 映射：字段映射和转换
    Map {
        /// 字段映射配置
        fields: HashMap<String, FieldMapping>,
    },
    /// 验证：数据验证
    Validate {
        /// 验证规则列表
        rules: Vec<ValidationRule>,
    },
    /// 增强：使用 UDF 增强数据
    Enrich {
        /// UDF 函数名
        udf: String,
        /// UDF 参数（字段名）
        params: Vec<String>,
    },
}

/// 字段映射配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// 源字段名
    pub source_field: String,
    /// 目标字段名
    pub target_field: String,
    /// UDF 转换函数（可选）
    pub transform: Option<String>,
}

/// 验证规则
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    /// 字段名
    pub field: String,
    /// 验证类型
    #[serde(flatten)]
    pub rule_type: ValidationType,
}

/// 验证类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ValidationType {
    /// 范围验证
    Range { min: f64, max: f64 },
    /// 正则验证
    Regex { pattern: String },
    /// 必填验证
    Required,
    /// 类型验证
    Type { expected: String },
}

/// 输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// 输出目标
    pub target: OutputTarget,
    /// Cypher 写入语句
    pub cypher: String,
}

/// 输出目标
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OutputTarget {
    /// 图数据库
    Graph,
    /// 物化视图
    MaterializedView { view_id: String },
}

/// 错误处理策略
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "lowercase")]
pub enum ErrorHandling {
    /// 跳过错误记录
    Skip,
    /// 重试
    Retry { max_attempts: u32, backoff_ms: u64 },
    /// 停止管道
    Halt,
}

impl EtlRule {
    /// 创建新规则
    pub fn new(id: String, name: String, description: String, source_id: String) -> Self {
        Self {
            id,
            name,
            description,
            source_id,
            enabled: true,
            transform: Vec::new(),
            output: OutputConfig {
                target: OutputTarget::Graph,
                cypher: String::new(),
            },
            error_handling: ErrorHandling::Skip,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    /// 添加转换步骤
    pub fn add_step(mut self, step: TransformStep) -> Self {
        self.transform.push(step);
        self.updated_at = Utc::now();
        self
    }

    /// 设置输出配置
    pub fn with_output(mut self, output: OutputConfig) -> Self {
        self.output = output;
        self.updated_at = Utc::now();
        self
    }

    /// 设置错误处理策略
    pub fn with_error_handling(mut self, error_handling: ErrorHandling) -> Self {
        self.error_handling = error_handling;
        self.updated_at = Utc::now();
        self
    }

    /// 验证规则配置是否有效
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Rule name cannot be empty".to_string());
        }
        if self.source_id.is_empty() {
            return Err("Source ID cannot be empty".to_string());
        }
        if self.transform.is_empty() {
            return Err("At least one transform step is required".to_string());
        }
        if self.output.cypher.is_empty() {
            return Err("Output cypher cannot be empty".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_creation() {
        let rule = EtlRule::new(
            "rule1".to_string(),
            "Test Rule".to_string(),
            "A test rule".to_string(),
            "source1".to_string(),
        );

        assert_eq!(rule.id, "rule1");
        assert_eq!(rule.name, "Test Rule");
        assert!(rule.enabled);
        assert!(rule.transform.is_empty());
    }

    #[test]
    fn test_rule_builder() {
        let rule = EtlRule::new(
            "rule1".to_string(),
            "Test Rule".to_string(),
            "A test rule".to_string(),
            "source1".to_string(),
        )
        .add_step(TransformStep::Filter {
            cypher: "WHERE $temperature > 20".to_string(),
        })
        .add_step(TransformStep::Map {
            fields: HashMap::new(),
        })
        .with_output(OutputConfig {
            target: OutputTarget::Graph,
            cypher: "CREATE (n:Node)".to_string(),
        });

        assert_eq!(rule.transform.len(), 2);
        assert!(!rule.output.cypher.is_empty());
    }

    #[test]
    fn test_rule_validation() {
        let mut rule = EtlRule::new(
            "rule1".to_string(),
            "Test Rule".to_string(),
            "A test rule".to_string(),
            "source1".to_string(),
        );

        // 缺少转换步骤
        assert!(rule.validate().is_err());

        // 添加步骤
        rule.transform.push(TransformStep::Filter {
            cypher: "WHERE true".to_string(),
        });

        // 缺少输出 cypher
        assert!(rule.validate().is_err());

        // 添加输出
        rule.output.cypher = "CREATE (n:Node)".to_string();

        // 现在应该通过验证
        assert!(rule.validate().is_ok());
    }

    #[test]
    fn test_serialization() {
        let rule = EtlRule::new(
            "rule1".to_string(),
            "Test Rule".to_string(),
            "A test rule".to_string(),
            "source1".to_string(),
        )
        .add_step(TransformStep::Filter {
            cypher: "WHERE $temperature > 20".to_string(),
        });

        let json = serde_json::to_string(&rule).unwrap();
        let deserialized: EtlRule = serde_json::from_str(&json).unwrap();

        assert_eq!(rule.id, deserialized.id);
        assert_eq!(rule.name, deserialized.name);
        assert_eq!(rule.transform.len(), deserialized.transform.len());
    }
}
