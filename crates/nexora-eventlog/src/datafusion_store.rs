//! DataFusionEventStore — Iceberg 表的 SQL 查询层
//!
//! 负责:
//! 1. 包装 EventLogStore,直接注册 Iceberg 表到 DataFusion
//! 2. 提供 SQL 查询接口
//! 3. 自动发现并注册所有表

use crate::event_log_store::EventLogStore;
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;

pub struct DataFusionEventStore {
    event_log_store: Arc<EventLogStore>,
    session_ctx: SessionContext,
}

impl DataFusionEventStore {
    /// 创建 DataFusionEventStore
    ///
    /// 注意: 需要手动调用 register_table() 或 register_all_tables() 来注册表
    pub async fn new(event_log_store: Arc<EventLogStore>) -> Result<Self> {
        let session_ctx = SessionContext::new();

        Ok(Self {
            event_log_store,
            session_ctx,
        })
    }

    /// 注册单个表到 DataFusion
    ///
    /// 由于 iceberg-datafusion 的 IcebergTableProvider 构造函数不公开,
    /// 我们暂时跳过直接集成,留待后续优化
    pub async fn register_table(&self, _table_name: &str) -> Result<()> {
        // TODO: 实现直接注册 Iceberg 表
        // 当前问题: IcebergTableProvider::try_new() 是 pub(crate)
        anyhow::bail!(
            "Direct table registration not yet implemented - waiting for iceberg-datafusion API"
        )
    }

    /// 自动注册所有表
    pub async fn register_all_tables(&self) -> Result<()> {
        let tables = self.event_log_store.list_tables().await?;

        for table_name in tables {
            self.register_table(&table_name).await?;
        }

        Ok(())
    }

    /// 执行 SQL 查询
    ///
    /// 注意: 当前版本由于 iceberg-datafusion 集成问题,此功能暂时不可用
    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        tracing::debug!("Executing SQL: {}", sql);

        let df = self
            .session_ctx
            .sql(sql)
            .await
            .context("Failed to parse SQL")?;

        let batches = df.collect().await.context("Failed to execute query")?;

        tracing::debug!("Query returned {} batches", batches.len());
        Ok(batches)
    }

    /// 获取 EventLogStore 引用 (用于直接访问 Iceberg API)
    pub fn event_log_store(&self) -> &Arc<EventLogStore> {
        &self.event_log_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventLogStore;

    #[tokio::test]
    async fn test_create_datafusion_store() {
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();

        let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());
        let df_store = DataFusionEventStore::new(store.clone()).await.unwrap();

        // 验证创建成功
        assert!(Arc::ptr_eq(df_store.event_log_store(), &store));

        println!("✅ DataFusionEventStore created successfully");
    }
}
