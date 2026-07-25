#![cfg(feature = "olap")]
//! 集成测试: RefreshScheduler 后台定时刷新

use nexora_core::RawEvent;
use nexora_eventlog::{
    Aggregation, EventLogStore, MaterializedView, RefreshMode, RefreshScheduler,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

fn make_event(topic: &str, device: &str, value: i64, time: u64) -> RawEvent {
    RawEvent::new(
        time,
        time,
        "test",
        topic,
        None,
        None,
        None,
        json!({"device_id": device, "value": value}),
    )
}

#[tokio::test]
async fn test_scheduler_schedule_cancel() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 创建源表
    let events = vec![make_event("data", "D1", 100, 1_700_000_000_000_000)];
    store.append(&events).await.unwrap();

    let scheduler = RefreshScheduler::new(store);

    let view = MaterializedView::aggregate(
        "scheduled_view",
        "data",
        vec!["device_id".into()],
        vec![Aggregation::Count {
            field: "*".into(),
            alias: "count".into(),
        }],
        RefreshMode::Pull { interval_secs: 2 },
    );

    // 调度
    scheduler.schedule(view, 2).await.unwrap();

    // 验证已调度
    assert_eq!(scheduler.list_scheduled().await.len(), 1);

    // 取消
    scheduler.cancel("scheduled_view").await;
    assert_eq!(scheduler.list_scheduled().await.len(), 0);

    println!("✅ Scheduler schedule/cancel works");
}

#[tokio::test]
async fn test_scheduler_background_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 创建源表
    let events = vec![
        make_event("metrics", "A", 10, 1_700_000_000_000_000),
        make_event("metrics", "B", 20, 1_700_000_001_000_000),
    ];
    store.append(&events).await.unwrap();

    let scheduler = RefreshScheduler::new(store.clone());

    let view = MaterializedView::aggregate(
        "auto_refresh",
        "metrics",
        vec!["device_id".into()],
        vec![Aggregation::Sum {
            field: "value".into(),
            alias: "total".into(),
        }],
        RefreshMode::Pull { interval_secs: 1 },
    );

    // 调度 (每 1 秒刷新)
    scheduler.schedule(view.clone(), 1).await.unwrap();

    // 等待至少一次调度刷新触发 (第一次 tick 跳过,所以等 2.5 秒)
    sleep(Duration::from_millis(2500)).await;

    // 验证目标表已被后台任务创建
    let target_exists = store.load_table(&view.target_table).await.is_ok();
    assert!(target_exists, "Target table should be created by scheduled refresh");

    // 清理
    scheduler.cancel_all().await;

    println!("✅ Scheduler background refresh works - target table created");
}

/// End-to-end for the DomainMV → SQL MaterializedView path (阶段6 RefreshScheduler
/// 接线):从一个 DomainPackage 里的 DomainMV(自由 SQL 字符串)转成 MaterializedView,
/// 经 ViewRefresher 执行(此前是 stub),验证目标表被真实写入 + 行数正确。
#[tokio::test]
async fn test_domain_mv_sql_refresh_end_to_end() {
    use nexora_core::domain_package::DomainMV;
    use nexora_eventlog::{MaterializedView, ViewRefresher};

    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 源表 "orders":两个 device 各若干事件
    let events = vec![
        make_event("orders", "A", 1, 1_700_000_000_000_000),
        make_event("orders", "A", 1, 1_700_000_001_000_000),
        make_event("orders", "B", 1, 1_700_000_002_000_000),
    ];
    store.append(&events).await.unwrap();

    // DomainMV:自由 SQL 字符串(单源表 orders),pull:1
    let dmv = DomainMV {
        name: "orders_per_device".into(),
        query: "SELECT device_id, COUNT(*) AS c FROM orders GROUP BY device_id".into(),
        refresh_mode: "pull:1".into(),
        schema: vec![],
    };

    // 转换:自由 SQL → 结构化 SQL MaterializedView(源表从 FROM 解析)
    let view = MaterializedView::from_domain_mv(&dmv).expect("from_domain_mv should parse");
    assert_eq!(view.source_table, "orders");

    // 执行刷新(此前 refresh_sql 是 stub,现在真跑 DataFusion)
    let refresher = ViewRefresher::new(store.clone());
    let rows = refresher.refresh(&view).await.expect("SQL refresh should succeed");

    // 两个分组(A、B)→ 2 行
    assert_eq!(rows, 2, "GROUP BY device_id over 3 events → 2 groups");

    // 目标表被真实写入
    let target_exists = store.load_table(&view.target_table).await.is_ok();
    assert!(target_exists, "SQL view must write its target table");

    println!("✅ DomainMV SQL refresh end-to-end works - {} rows written", rows);
}
