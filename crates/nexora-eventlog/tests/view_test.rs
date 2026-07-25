#![cfg(feature = "olap")]
//! 集成测试: 物化视图 (ViewManager + ViewRefresher)

use nexora_core::RawEvent;
use nexora_eventlog::{
    Aggregation, EventLogStore, MaterializedView, RefreshMode, ViewManager, ViewRefresher,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

/// 辅助: 创建测试事件
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
async fn test_view_manager_create_and_list() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 创建源表
    let events = vec![make_event("metrics", "D1", 100, 1_700_000_000_000_000)];
    store.append(&events).await.unwrap();

    // 创建 ViewManager
    let manager = ViewManager::new(store);

    // 定义视图
    let view = MaterializedView::aggregate(
        "device_avg",
        "metrics",
        vec!["device_id".into()],
        vec![Aggregation::Avg {
            field: "value".into(),
            alias: "avg_value".into(),
        }],
        RefreshMode::Pull { interval_secs: 60 },
    );

    // 创建视图
    manager.create_view(view).await.unwrap();

    // 验证
    let views = manager.list_views().await;
    assert_eq!(views.len(), 1);
    assert!(views.contains(&"device_avg".to_string()));

    println!("✅ ViewManager create/list works");
}

#[tokio::test]
async fn test_view_refresher_aggregate() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 写入多条数据 (相同 device, 不同 value)
    let events = vec![
        make_event("sales", "store_A", 100, 1_700_000_000_000_000),
        make_event("sales", "store_A", 200, 1_700_000_001_000_000),
        make_event("sales", "store_B", 300, 1_700_000_002_000_000),
    ];
    store.append(&events).await.unwrap();

    // 创建 ViewRefresher
    let refresher = ViewRefresher::new(store);

    // 定义聚合视图: 按 device_id 分组求 SUM
    let view = MaterializedView::aggregate(
        "sales_by_store",
        "sales",
        vec!["device_id".into()],
        vec![
            Aggregation::Sum {
                field: "value".into(),
                alias: "total".into(),
            },
            Aggregation::Count {
                field: "*".into(),
                alias: "count".into(),
            },
        ],
        RefreshMode::Pull { interval_secs: 60 },
    );

    // 执行刷新
    let row_count = refresher.refresh(&view).await.unwrap();

    // 验证: 应该有 2 个分组 (store_A, store_B)
    assert_eq!(row_count, 2);

    println!("✅ ViewRefresher aggregate computed {} groups", row_count);
}

#[tokio::test]
async fn test_view_refresher_no_groupby() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 写入数据
    let events = vec![
        make_event("events", "A", 10, 1_700_000_000_000_000),
        make_event("events", "B", 20, 1_700_000_001_000_000),
        make_event("events", "C", 30, 1_700_000_002_000_000),
    ];
    store.append(&events).await.unwrap();

    let refresher = ViewRefresher::new(store);

    // 全局聚合 (无分组)
    let view = MaterializedView::aggregate(
        "total_count",
        "events",
        vec![], // 无分组
        vec![Aggregation::Count {
            field: "*".into(),
            alias: "total".into(),
        }],
        RefreshMode::Pull { interval_secs: 60 },
    );

    let row_count = refresher.refresh(&view).await.unwrap();

    // 无分组聚合返回 1 行
    assert_eq!(row_count, 1);

    println!("✅ ViewRefresher global aggregate works");
}

#[tokio::test]
async fn test_view_end_to_end_with_target_table() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 写入源数据
    let events = vec![
        make_event("orders", "shop_A", 100, 1_700_000_000_000_000),
        make_event("orders", "shop_A", 150, 1_700_000_001_000_000),
        make_event("orders", "shop_B", 200, 1_700_000_002_000_000),
    ];
    store.append(&events).await.unwrap();

    let refresher = ViewRefresher::new(store.clone());

    // 创建聚合视图: 按 shop 求 SUM
    let view = MaterializedView::aggregate(
        "orders_summary",
        "orders",
        vec!["device_id".into()],
        vec![Aggregation::Sum {
            field: "value".into(),
            alias: "total".into(),
        }],
        RefreshMode::Pull { interval_secs: 60 },
    );

    // 执行刷新 (聚合 + 写入目标表)
    let row_count = refresher.refresh(&view).await.unwrap();
    assert_eq!(row_count, 2); // shop_A, shop_B

    // 验证目标表已创建并可查询
    let target_table = store.load_table(&view.target_table).await;
    assert!(target_table.is_ok(), "Target table should exist");

    // 读取目标表验证数据
    let tables = store.list_tables().await.unwrap();
    assert!(
        tables.contains(&view.target_table),
        "Target table '{}' should be in table list",
        view.target_table
    );

    println!("✅ End-to-end: view aggregated and written to target table '{}'", view.target_table);
}

#[tokio::test]
async fn test_incremental_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 初始数据
    let events = vec![make_event("counter", "X", 1, 1_700_000_000_000_000)];
    store.append(&events).await.unwrap();

    let refresher = ViewRefresher::new(store);

    let view = MaterializedView::aggregate(
        "counter_sum",
        "counter",
        vec!["device_id".into()],
        vec![Aggregation::Sum {
            field: "value".into(),
            alias: "sum".into(),
        }],
        RefreshMode::Push,
    );

    // 增量刷新 (新事件)
    let new_events = vec![make_event("counter", "X", 5, 1_700_000_001_000_000)];
    let row_count = refresher.incremental_refresh(&view, &new_events).await.unwrap();

    // 增量刷新触发全量重算,应该有 1 个分组
    assert_eq!(row_count, 1);

    println!("✅ Incremental refresh works");
}

/// Phase 0: 版本化写 + 读时取最新 —— 重复刷新不应让旧代堆积可见。
#[tokio::test]
async fn test_read_latest_dedups_repeated_full_refresh() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    let events = vec![
        make_event("m0", "A", 100, 1_700_000_000_000_000),
        make_event("m0", "A", 200, 1_700_000_001_000_000),
        make_event("m0", "B", 300, 1_700_000_002_000_000),
    ];
    store.append(&events).await.unwrap();

    let refresher = ViewRefresher::new(store);
    let view = MaterializedView::aggregate(
        "m0_sum",
        "m0",
        vec!["device_id".into()],
        vec![Aggregation::Sum {
            field: "value".into(),
            alias: "total".into(),
        }],
        RefreshMode::Pull { interval_secs: 60 },
    );

    // 跑三次全量刷新(源数据不变)。旧实现会往目标表 append 3 份 → 读回 6 行。
    refresher.refresh(&view).await.unwrap();
    refresher.refresh(&view).await.unwrap();
    refresher.refresh(&view).await.unwrap();

    // read_latest 只应返回最新一代 = 2 个分组。
    let latest = refresher.read_latest(&view).await.unwrap();
    let total_rows: usize = latest.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "read_latest 应只返回最新一代的 2 个分组,而非堆积的多代"
    );

    // 且对外结果不含内部 _mv_* 列。
    let schema = latest[0].schema();
    for col in ["_mv_version", "_mv_src_snapshot_id", "_mv_updated_at"] {
        assert!(
            schema.column_with_name(col).is_none(),
            "对外结果不应包含内部列 {col}"
        );
    }

    println!("✅ Phase 0: repeated full refresh dedups to latest generation");
}

// ---- Phase 1: 真增量计算 ----

use arrow::array::{Array, Float64Array, Int64Array};

/// 从 read_latest 的结果里,按分组键(String 列)抽出某个 double 度量列的值。
fn extract_metric(
    batches: &[arrow::record_batch::RecordBatch],
    group_col: &str,
    metric_col: &str,
) -> std::collections::HashMap<String, f64> {
    use arrow::array::StringArray;
    let mut out = std::collections::HashMap::new();
    for b in batches {
        let g = b
            .column_by_name(group_col)
            .expect("group col")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("group is string");
        let m = b.column_by_name(metric_col).expect("metric col");
        // 度量列可能是 Int64(count)或 Float64(sum/avg/max/min)。
        let as_f64 = |i: usize| -> f64 {
            if let Some(a) = m.as_any().downcast_ref::<Float64Array>() {
                a.value(i)
            } else if let Some(a) = m.as_any().downcast_ref::<Int64Array>() {
                a.value(i) as f64
            } else {
                panic!("unexpected metric type for {metric_col}");
            }
        };
        for i in 0..b.num_rows() {
            out.insert(g.value(i).to_string(), as_f64(i));
        }
    }
    out
}

/// 增量结果必须等于「把两批事件都写进去后做一次全量」的结果(COUNT/SUM/MAX/MIN/AVG)。
#[tokio::test]
async fn test_incremental_equals_full_all_agg_types() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());

    // 第一批事件。
    let batch_a = vec![
        make_event("inc", "A", 10, 1_700_000_000_000_000),
        make_event("inc", "A", 30, 1_700_000_001_000_000),
        make_event("inc", "B", 50, 1_700_000_002_000_000),
    ];
    store.append(&batch_a).await.unwrap();

    let refresher = ViewRefresher::new(store.clone());
    let make_view = || {
        MaterializedView::aggregate(
            "inc_v",
            "inc",
            vec!["device_id".into()],
            vec![
                Aggregation::Count { field: "*".into(), alias: "cnt".into() },
                Aggregation::Sum { field: "value".into(), alias: "sm".into() },
                Aggregation::Max { field: "value".into(), alias: "mx".into() },
                Aggregation::Min { field: "value".into(), alias: "mn".into() },
                Aggregation::Avg { field: "value".into(), alias: "av".into() },
            ],
            RefreshMode::Push,
        )
    };
    let view = make_view();

    // 全量刷新第一批 → 建立初态。
    refresher.refresh(&view).await.unwrap();

    // 第二批事件(含改变 max/min 的值 + 新分组 C)。
    let batch_b = vec![
        make_event("inc", "A", 5, 1_700_000_003_000_000),   // A min 变 5
        make_event("inc", "B", 90, 1_700_000_004_000_000),  // B max 变 90
        make_event("inc", "C", 42, 1_700_000_005_000_000),  // 新分组 C
    ];
    store.append(&batch_b).await.unwrap();

    // 增量刷新。
    let n = refresher.incremental_refresh(&view, &[]).await.unwrap();
    assert_eq!(n, 3, "增量后应有 3 个分组 A/B/C");

    let inc_result = refresher.read_latest(&view).await.unwrap();

    // 对照组:全新表 + 全量(A∪B 全部事件)。
    let full_dir = TempDir::new().unwrap();
    let full_store = Arc::new(EventLogStore::new(full_dir.path()).await.unwrap());
    let mut all = batch_a.clone();
    all.extend(batch_b.clone());
    full_store.append(&all).await.unwrap();
    let full_refresher = ViewRefresher::new(full_store);
    let full_view = MaterializedView::aggregate(
        "inc_v",
        "inc",
        vec!["device_id".into()],
        vec![
            Aggregation::Count { field: "*".into(), alias: "cnt".into() },
            Aggregation::Sum { field: "value".into(), alias: "sm".into() },
            Aggregation::Max { field: "value".into(), alias: "mx".into() },
            Aggregation::Min { field: "value".into(), alias: "mn".into() },
            Aggregation::Avg { field: "value".into(), alias: "av".into() },
        ],
        RefreshMode::Pull { interval_secs: 60 },
    );
    full_refresher.refresh(&full_view).await.unwrap();
    let full_result = full_refresher.read_latest(&full_view).await.unwrap();

    // 逐度量、逐分组比对。
    for metric in ["cnt", "sm", "mx", "mn", "av"] {
        let inc = extract_metric(&inc_result, "device_id", metric);
        let full = extract_metric(&full_result, "device_id", metric);
        assert_eq!(
            inc.len(),
            full.len(),
            "metric {metric}: 分组数不一致 inc={inc:?} full={full:?}"
        );
        for (k, v) in &full {
            let iv = inc.get(k).unwrap_or_else(|| panic!("metric {metric} 缺分组 {k}"));
            assert!(
                (iv - v).abs() < 1e-9,
                "metric {metric} 分组 {k}: 增量 {iv} != 全量 {v}"
            );
        }
    }

    // 具体值抽查:A: cnt=3(10,30,5), sum=45, max=30, min=5, avg=15。
    let cnt = extract_metric(&inc_result, "device_id", "cnt");
    let mn = extract_metric(&inc_result, "device_id", "mn");
    let av = extract_metric(&inc_result, "device_id", "av");
    assert_eq!(cnt["A"], 3.0);
    assert_eq!(mn["A"], 5.0);
    assert!((av["A"] - 15.0).abs() < 1e-9);

    println!("✅ Phase 1: incremental == full for COUNT/SUM/MAX/MIN/AVG");
}

/// 无新事件时增量应是 no-op:返回 0 且不新增版本。
#[tokio::test]
async fn test_incremental_noop_when_no_new_events() {
    let temp_dir = TempDir::new().unwrap();
    let store = Arc::new(EventLogStore::new(temp_dir.path()).await.unwrap());
    store
        .append(&[make_event("noop", "A", 1, 1_700_000_000_000_000)])
        .await
        .unwrap();

    let refresher = ViewRefresher::new(store);
    let view = MaterializedView::aggregate(
        "noop_v",
        "noop",
        vec!["device_id".into()],
        vec![Aggregation::Sum { field: "value".into(), alias: "sm".into() }],
        RefreshMode::Push,
    );

    refresher.refresh(&view).await.unwrap(); // 建初态
    // 源表无新 append → 增量 no-op。
    let n = refresher.incremental_refresh(&view, &[]).await.unwrap();
    assert_eq!(n, 0, "无新事件应返回 0");

    // 再次增量仍 0。
    let n2 = refresher.incremental_refresh(&view, &[]).await.unwrap();
    assert_eq!(n2, 0);

    let res = refresher.read_latest(&view).await.unwrap();
    let sm = extract_metric(&res, "device_id", "sm");
    assert_eq!(sm["A"], 1.0);

    println!("✅ Phase 1: incremental no-op when source unchanged");
}
