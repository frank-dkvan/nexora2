//! ViewManager - 物化视图管理器
//!
//! 负责:
//! 1. 创建/删除物化视图
//! 2. 触发视图刷新
//! 3. 管理视图元数据

use crate::event_log_store::EventLogStore;
use crate::materialized_view::{MaterializedView, RefreshMode};
use crate::refresh_scheduler::RefreshScheduler;
use crate::view_refresher::ViewRefresher;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Push 模式默认的增量刷新周期(秒)。增量很轻(只读新增 snapshot 文件),
/// 可比 pull 全量刷新更频繁。
const DEFAULT_PUSH_INTERVAL_SECS: u64 = 5;

pub struct ViewManager {
    /// 视图定义 (view_name -> MaterializedView)
    views: Arc<RwLock<HashMap<String, MaterializedView>>>,

    /// 事件日志存储
    event_log_store: Arc<EventLogStore>,

    /// 视图刷新执行器
    refresher: ViewRefresher,

    /// 后台刷新调度器 (Pull 模式)
    scheduler: RefreshScheduler,
}

impl ViewManager {
    /// 创建 ViewManager
    pub fn new(event_log_store: Arc<EventLogStore>) -> Self {
        let refresher = ViewRefresher::new(event_log_store.clone());
        let scheduler = RefreshScheduler::new(event_log_store.clone());
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
            event_log_store,
            refresher,
            scheduler,
        }
    }

    /// 创建物化视图
    pub async fn create_view(&self, view: MaterializedView) -> Result<()> {
        tracing::info!("Creating materialized view '{}'", view.name);

        // 1. 验证源表存在
        self.event_log_store
            .load_table(&view.source_table)
            .await
            .with_context(|| format!("Source table '{}' not found", view.source_table))?;

        // 2. 首次全量刷新 (计算初始聚合结果)
        let row_count = self.refresher.refresh(&view).await?;
        tracing::info!("Initial refresh computed {} rows", row_count);

        // 3. 注册到调度器。
        //    - Pull:定时全量刷新。
        //    - Push:短周期增量刷新(真增量,无新事件时 no-op)。
        //    - Hybrid:push_enabled 时走增量周期,否则退回全量周期。
        match view.refresh_mode {
            RefreshMode::Pull { interval_secs } => {
                self.scheduler.schedule(view.clone(), interval_secs).await?;
            }
            RefreshMode::Push => {
                self.scheduler
                    .schedule_incremental(view.clone(), DEFAULT_PUSH_INTERVAL_SECS)
                    .await?;
            }
            RefreshMode::Hybrid {
                push_enabled,
                pull_interval_secs,
            } => {
                if push_enabled {
                    self.scheduler
                        .schedule_incremental(view.clone(), DEFAULT_PUSH_INTERVAL_SECS)
                        .await?;
                } else {
                    self.scheduler.schedule(view.clone(), pull_interval_secs).await?;
                }
            }
        }

        // 4. 保存视图定义
        self.views.write().await.insert(view.name.clone(), view);

        tracing::info!("Materialized view created successfully");
        Ok(())
    }

    /// 删除物化视图
    pub async fn drop_view(&self, view_name: &str) -> Result<()> {
        tracing::info!("Dropping materialized view '{}'", view_name);

        // 1. 移除视图定义
        let view = self
            .views
            .write()
            .await
            .remove(view_name)
            .ok_or_else(|| anyhow::anyhow!("View '{}' not found", view_name))?;

        // 2. 取消调度任务
        self.scheduler.cancel(view_name).await;

        // 3. 删除目标表 (可选 - 保留数据)
        tracing::info!("View '{}' dropped (target table retained)", view.name);

        Ok(())
    }

    /// 手动刷新视图
    pub async fn refresh_view(&self, view_name: &str) -> Result<u64> {
        tracing::info!("Refreshing materialized view '{}'", view_name);

        let view = {
            let views = self.views.read().await;
            views
                .get(view_name)
                .ok_or_else(|| anyhow::anyhow!("View '{}' not found", view_name))?
                .clone()
        };

        // 执行全量刷新
        let row_count = self.refresher.refresh(&view).await?;
        tracing::info!("Refreshed view '{}' with {} rows", view_name, row_count);

        Ok(row_count)
    }

    /// 增量更新视图 (Push 模式)
    pub async fn incremental_update(
        &self,
        source_table: &str,
        new_events: &[nexora_core::RawEvent],
    ) -> Result<()> {
        // 找到所有基于此源表的 Push 模式视图
        let views_to_update: Vec<MaterializedView> = {
            let views = self.views.read().await;
            views
                .values()
                .filter(|v| {
                    v.source_table == source_table
                        && matches!(
                            v.refresh_mode,
                            RefreshMode::Push | RefreshMode::Hybrid { push_enabled: true, .. }
                        )
                })
                .cloned()
                .collect()
        };

        // 对每个视图执行增量刷新
        for view in views_to_update {
            tracing::debug!("Triggering incremental update for view '{}'", view.name);
            self.refresher.incremental_refresh(&view, new_events).await?;
        }

        Ok(())
    }

    /// 列出所有视图
    pub async fn list_views(&self) -> Vec<String> {
        self.views.read().await.keys().cloned().collect()
    }

    /// 获取视图定义
    pub async fn get_view(&self, view_name: &str) -> Option<MaterializedView> {
        self.views.read().await.get(view_name).cloned()
    }

    /// 读取视图的「最新一代」结果(对外形态:已去重到最新版本、AVG 已还原)。
    ///
    /// 供读回物化视图数据用(委托 [`ViewRefresher::read_latest`])。视图不存在
    /// 时返回 `Err`。
    pub async fn scan_view_latest(
        &self,
        view_name: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let view = self
            .get_view(view_name)
            .await
            .ok_or_else(|| anyhow::anyhow!("View '{}' not found", view_name))?;
        self.refresher.read_latest(&view).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materialized_view::{Aggregation, RefreshMode};
    use nexora_core::RawEvent;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_view() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

        // 先创建源表
        let events = vec![RawEvent::new(
            1_700_000_000_000_000,
            1_700_000_000_000_000,
            "test",
            "sensors",
            None,
            None,
            None,
            serde_json::json!({"device_id": "D1", "temp": 23.5}),
        )];
        store.append(&events).await.unwrap();

        let manager = ViewManager::new(store);

        let view = MaterializedView::aggregate(
            "sensor_avg",
            "sensors",
            vec!["device_id".into()],
            vec![Aggregation::Avg {
                field: "temp".into(),
                alias: "avg_temp".into(),
            }],
            RefreshMode::Pull { interval_secs: 300 },
        );

        manager.create_view(view).await.unwrap();

        // 验证视图已创建
        let views = manager.list_views().await;
        assert_eq!(views.len(), 1);
        assert!(views.contains(&"sensor_avg".to_string()));

        println!("✅ View created successfully");
    }

    #[tokio::test]
    async fn test_drop_view() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

        // 创建源表
        let events = vec![RawEvent::new(
            1_700_000_000_000_000,
            1_700_000_000_000_000,
            "test",
            "sensors",
            None,
            None,
            None,
            serde_json::json!({}),
        )];
        store.append(&events).await.unwrap();

        let manager = ViewManager::new(store);

        let view = MaterializedView::aggregate(
            "test_view",
            "sensors",
            vec![],
            vec![],
            RefreshMode::Push,
        );

        manager.create_view(view).await.unwrap();
        manager.drop_view("test_view").await.unwrap();

        // 验证视图已删除
        assert_eq!(manager.list_views().await.len(), 0);

        println!("✅ View dropped successfully");
    }
}
