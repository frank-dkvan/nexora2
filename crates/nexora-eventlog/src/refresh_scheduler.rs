//! RefreshScheduler - 物化视图后台定时刷新调度器
//!
//! 负责:
//! 1. 为 Pull 模式视图注册定时刷新任务
//! 2. 使用 tokio 异步任务在后台周期性刷新
//! 3. 支持取消调度

use crate::event_log_store::EventLogStore;
use crate::materialized_view::MaterializedView;
use crate::view_refresher::ViewRefresher;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};

pub struct RefreshScheduler {
    /// 运行中的刷新任务 (view_name -> JoinHandle)
    tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,

    /// 事件日志存储 (用于创建 refresher)
    event_log_store: Arc<EventLogStore>,
}

impl RefreshScheduler {
    pub fn new(event_log_store: Arc<EventLogStore>) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            event_log_store,
        }
    }

    /// 调度定时刷新任务
    ///
    /// 每 `interval_secs` 秒执行一次视图刷新
    pub async fn schedule(&self, view: MaterializedView, interval_secs: u64) -> Result<()> {
        let view_name = view.name.clone();

        // 如果已有任务,先取消
        self.cancel(&view_name).await;

        let event_log_store = self.event_log_store.clone();
        let view_clone = view.clone();

        // 启动后台任务
        let task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs));
            let refresher = ViewRefresher::new(event_log_store);

            // 第一次 tick 立即触发,跳过 (create_view 已经首次刷新)
            ticker.tick().await;

            loop {
                ticker.tick().await;

                tracing::info!("Scheduled refresh for view '{}'", view_clone.name);

                match refresher.refresh(&view_clone).await {
                    Ok(row_count) => {
                        tracing::info!(
                            "Scheduled refresh completed for '{}': {} rows",
                            view_clone.name,
                            row_count
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Scheduled refresh failed for '{}': {}",
                            view_clone.name,
                            e
                        );
                    }
                }
            }
        });

        // 保存任务句柄
        self.tasks.write().await.insert(view_name.clone(), task);

        tracing::info!(
            "Scheduled view '{}' to refresh every {} seconds",
            view_name,
            interval_secs
        );

        Ok(())
    }

    /// 调度增量刷新任务(Push / Hybrid 模式)。
    ///
    /// 每 `interval_secs` 秒调一次 `incremental_refresh`:只读源表自上次以来新增的
    /// snapshot 文件并与旧态合并(见 `ViewRefresher::incremental_aggregate`),因此
    /// 可用比全量更短的周期。无新事件时是 no-op(不写新版本)。首个 tick 会立即
    /// 触发一次,以建立初态(Aggregate 首次无旧态 ⇒ 内部回退全量)。
    pub async fn schedule_incremental(
        &self,
        view: MaterializedView,
        interval_secs: u64,
    ) -> Result<()> {
        let view_name = view.name.clone();
        self.cancel(&view_name).await;

        let event_log_store = self.event_log_store.clone();
        let view_clone = view.clone();

        let task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs));
            let refresher = ViewRefresher::new(event_log_store);

            loop {
                ticker.tick().await;
                match refresher.incremental_refresh(&view_clone, &[]).await {
                    Ok(0) => tracing::debug!(
                        "Incremental tick for '{}': up-to-date (no-op)",
                        view_clone.name
                    ),
                    Ok(n) => tracing::info!(
                        "Incremental refresh for '{}': {} groups",
                        view_clone.name,
                        n
                    ),
                    Err(e) => tracing::error!(
                        "Incremental refresh failed for '{}': {}",
                        view_clone.name,
                        e
                    ),
                }
            }
        });

        self.tasks.write().await.insert(view_name.clone(), task);
        tracing::info!(
            "Scheduled view '{}' for incremental refresh every {} seconds",
            view_name,
            interval_secs
        );
        Ok(())
    }

    /// 取消调度任务
    pub async fn cancel(&self, view_name: &str) {
        if let Some(task) = self.tasks.write().await.remove(view_name) {
            task.abort();
            tracing::info!("Cancelled scheduled refresh for view '{}'", view_name);
        }
    }

    /// 列出所有调度中的任务
    pub async fn list_scheduled(&self) -> Vec<String> {
        self.tasks.read().await.keys().cloned().collect()
    }

    /// 取消所有任务
    pub async fn cancel_all(&self) {
        let mut tasks = self.tasks.write().await;
        for (name, task) in tasks.drain() {
            task.abort();
            tracing::debug!("Cancelled task for view '{}'", name);
        }
    }
}

impl Drop for RefreshScheduler {
    fn drop(&mut self) {
        // 尝试清理任务 (best-effort)
        if let Ok(tasks) = self.tasks.try_read() {
            for (_, task) in tasks.iter() {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materialized_view::{Aggregation, RefreshMode};
    use nexora_core::RawEvent;
    use serde_json::json;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_schedule_and_cancel() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

        // 创建源表
        let events = vec![RawEvent::new(
            1_700_000_000_000_000,
            1_700_000_000_000_000,
            "test",
            "metrics",
            None,
            None,
            None,
            json!({"device_id": "D1", "value": 100}),
        )];
        store.append(&events).await.unwrap();

        let scheduler = RefreshScheduler::new(store);

        let view = MaterializedView::aggregate(
            "test_view",
            "metrics",
            vec!["device_id".into()],
            vec![Aggregation::Count {
                field: "*".into(),
                alias: "count".into(),
            }],
            RefreshMode::Pull { interval_secs: 1 },
        );

        // 调度 (每 1 秒)
        scheduler.schedule(view, 1).await.unwrap();

        // 验证任务已注册
        let scheduled = scheduler.list_scheduled().await;
        assert_eq!(scheduled.len(), 1);
        assert!(scheduled.contains(&"test_view".to_string()));

        // 取消
        scheduler.cancel("test_view").await;

        // 验证任务已取消
        let scheduled = scheduler.list_scheduled().await;
        assert_eq!(scheduled.len(), 0);

        println!("✅ Schedule and cancel works");
    }

    #[tokio::test]
    async fn test_schedule_incremental_registers_and_cancels() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());
        store
            .append(&[RawEvent::new(
                1_700_000_000_000_000,
                1_700_000_000_000_000,
                "test",
                "inc_sched",
                None,
                None,
                None,
                json!({"device_id": "D1", "value": 100}),
            )])
            .await
            .unwrap();

        let scheduler = RefreshScheduler::new(store);
        let view = MaterializedView::aggregate(
            "inc_sched_view",
            "inc_sched",
            vec!["device_id".into()],
            vec![Aggregation::Sum {
                field: "value".into(),
                alias: "sm".into(),
            }],
            RefreshMode::Push,
        );

        scheduler.schedule_incremental(view, 1).await.unwrap();
        assert_eq!(scheduler.list_scheduled().await.len(), 1);
        scheduler.cancel("inc_sched_view").await;
        assert_eq!(scheduler.list_scheduled().await.len(), 0);

        println!("✅ schedule_incremental registers and cancels");
    }

    #[tokio::test]
    async fn test_cancel_all() {
        let temp_dir = TempDir::new().unwrap();
        let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

        // 创建源表
        let events = vec![RawEvent::new(
            1_700_000_000_000_000,
            1_700_000_000_000_000,
            "test",
            "data",
            None,
            None,
            None,
            json!({"x": 1}),
        )];
        store.append(&events).await.unwrap();

        let scheduler = RefreshScheduler::new(store);

        // 调度多个视图
        for i in 1..=3 {
            let view = MaterializedView::aggregate(
                format!("view_{}", i),
                "data",
                vec![],
                vec![Aggregation::Count {
                    field: "*".into(),
                    alias: "c".into(),
                }],
                RefreshMode::Pull { interval_secs: 10 },
            );
            scheduler.schedule(view, 10).await.unwrap();
        }

        assert_eq!(scheduler.list_scheduled().await.len(), 3);

        // 取消所有
        scheduler.cancel_all().await;
        assert_eq!(scheduler.list_scheduled().await.len(), 0);

        println!("✅ Cancel all works");
    }
}
