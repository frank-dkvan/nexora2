//! # Nexora ETL Engine
//!
//! 内置 ETL 引擎，用于实时数据清洗和转换。
//!
//! ## 功能特性
//!
//! - **可视化配置**：通过 Dashboard 配置 ETL 规则
//! - **热加载**：规则修改秒级生效，无需重启服务
//! - **UDF 支持**：内置 10+ 常用函数，支持自定义 Rust/WASM UDF
//! - **错误处理**：支持 Skip/Retry/Halt 三种策略
//! - **性能优化**：批处理、并行处理
//!
//! ## 状态
//!
//! 转换/校验/过滤管线可用。**输出写入尚未实现**：`OutputTarget::Graph` 与
//! `OutputTarget::MaterializedView` 目前返回 `EtlError::Unimplemented`(此前会
//! 静默丢弃所有记录)。下面的示例演示的是规则注册,不涉及输出写入。
//!
//! ## 使用示例
//!
//! ```rust,no_run
//! use nexora_etl::{EtlEngine, EtlRule, TransformStep, OutputConfig, OutputTarget};
//! use nexora_core::{GraphService, GraphServiceConfig, InMemoryPersistor};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = GraphServiceConfig::default();
//!     let persistor = Arc::new(InMemoryPersistor::new());
//!     let graph = Arc::new(GraphService::new(config, persistor));
//!     let engine = EtlEngine::new(graph);
//!
//!     // 创建规则
//!     let rule = EtlRule::new(
//!         "sensor_cleaning".to_string(),
//!         "Sensor Data Cleaning".to_string(),
//!         "Clean IoT sensor data".to_string(),
//!         "mqtt-sensors".to_string(),
//!     )
//!     .add_step(TransformStep::Filter {
//!         cypher: "WHERE $quality = 'good'".to_string(),
//!     })
//!     .with_output(OutputConfig {
//!         target: OutputTarget::Graph,
//!         cypher: "MERGE (s:Sensor {id: $id})".to_string(),
//!     });
//!
//!     // 添加规则
//!     engine.add_rule(rule).await.unwrap();
//! }
//! ```

pub mod engine;
pub mod rule;
pub mod udf;

pub use engine::{
    BatchMetadata, EtlEngine, EtlError, IngestBatch, IngestRecord, ProcessResult, RecordMetadata,
};
pub use rule::{
    ErrorHandling, EtlRule, FieldMapping, OutputConfig, OutputTarget, TransformStep,
    ValidationRule, ValidationType,
};
pub use udf::{
    FunctionSignature, ParamType, ReturnType, UdfError, UdfRegistry, UserDefinedFunction,
};
