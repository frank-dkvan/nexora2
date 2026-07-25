# 阶段 4 报告 - 物化视图 + 定时刷新

> **✅ 更新 (2026-07-20,真增量已落地)**: 早先版本里 `incremental_refresh` 退化为全量刷新;现已实现**真增量聚合**(`crates/nexora-eventlog/src/view_refresher.rs` `incremental_aggregate`)。做法:目标表版本化(`_mv_version`/`_mv_src_snapshot_id`)+ 读时取最新代;增量只读源表自上次以来**新增的 Iceberg snapshot 文件**(`EventLogStore::read_snapshot_delta`,append-only ⇒ 文件差集即增量),算部分聚合后与旧态 `UNION ALL` 再归约(COUNT→SUM/SUM→SUM/MAX→MAX/MIN→MIN,AVG 用 `sum`+`count` 伴生列合并、读回还原)。Push/Hybrid 视图经 `RefreshScheduler::schedule_incremental` 短周期触发,无新事件时 no-op。集成测试证明**增量结果 == 等价全量结果**(COUNT/SUM/MAX/MIN/AVG 逐分组比对)。
>
> **边界**:真增量仅覆盖结构化 `Aggregate` 视图;任意 `Sql`-transform 视图(含 `DomainMV::from_domain_mv` 产出的)仍退回全量刷新(需查询分析,超范围)。iceberg 0.9.1 的 compaction/snapshot 过期仍需外部工具,见 [PRODUCTION_GAP_REASSESSMENT_2026-07-20](production-planning/PRODUCTION_GAP_REASSESSMENT_2026-07-20.md) B-2。

## 概述

阶段 4 实现了物化视图系统:聚合计算、全量刷新、按接口暴露的"增量"入口(当前退化为全量刷新)、后台定时调度。

---

## ✅ 已完成组件

### Step 1: MaterializedView 定义 (100%)
- `MaterializedView` - 视图定义结构
- `ViewTransform` - Aggregate / Sql
- `Aggregation` - Count/Sum/Avg/Max/Min
- `RefreshMode` - Pull/Push/Hybrid
- Builder API

### Step 2: ViewManager (100%)
- `create_view()` - 创建 + 首次刷新 + 注册调度
- `drop_view()` - 删除 + 取消调度
- `refresh_view()` - 手动刷新
- `incremental_update()` - Push 增量
- `list_views()` / `get_view()`

### Step 3: ViewRefresher (100%)
- `refresh()` - 全量刷新
- `refresh_aggregate()` - DataFusion 聚合计算
- `incremental_refresh()` - 增量刷新
- `build_aggregate_sql()` - SQL 构造 (含 CAST)
- Iceberg Scan API 读取源表

### Step 4: RefreshScheduler (100%)
- `schedule()` - 注册定时刷新任务
- `cancel()` / `cancel_all()` - 取消任务
- `list_scheduled()` - 列出任务
- tokio 后台异步任务
- Drop 时自动清理

### Step 5: 目标表写入 (100%)
- `EventLogStore::write_batch()` - 通用 RecordBatch 写入
- `ensure_table_from_arrow_schema()` - 从 Arrow schema 创建表
- `arrow_to_iceberg_schema()` - Schema 转换
- 聚合结果自动写入目标表

---

## 📊 测试覆盖: 12/12 通过 (100%)

### 阶段 1-3 测试 (5)
```
✅ test_event_log_store_append
✅ test_router_all_graph
✅ test_router_all_both
✅ test_ensure_table_from_domain
✅ test_schema_compatibility_check
```

### 阶段 4 视图测试 (5)
```
✅ test_view_manager_create_and_list      - 视图管理
✅ test_view_refresher_aggregate          - 聚合计算 (SUM+COUNT)
✅ test_view_refresher_no_groupby         - 全局聚合
✅ test_view_end_to_end_with_target_table - 端到端 (聚合→写表)
✅ test_incremental_refresh               - 增量刷新
```

### 阶段 4 调度器测试 (2)
```
✅ test_scheduler_schedule_cancel         - 调度/取消
✅ test_scheduler_background_refresh      - 后台自动刷新 ⭐
```

---

## 🔑 关键技术突破

### 1. DataFusion 内存表聚合
```rust
let mem_table = MemTable::try_new(schema, vec![source_batches])?;
ctx.register_table("source", Arc::new(mem_table))?;
let df = ctx.sql(&aggregate_sql).await?;
```

### 2. 类型转换 (CAST)
payload 字段存储为 String,数值聚合需要 CAST:
```sql
SUM(CAST(value AS DOUBLE)) AS total
```

### 3. Arrow → Iceberg Schema 转换
```rust
fn arrow_to_iceberg_schema(arrow_schema: &Schema) -> Result<IcebergSchema> {
    // 类型映射 + 字段 ID 分配
}
```

### 4. 后台定时调度
```rust
tokio::spawn(async move {
    let mut ticker = interval(Duration::from_secs(interval_secs));
    loop {
        ticker.tick().await;
        refresher.refresh(&view).await;
    }
});
```

---

## 💡 完整使用示例

```rust
use nexora_eventlog::{ViewManager, MaterializedView, Aggregation, RefreshMode};

// 1. 创建 ViewManager (含调度器)
let manager = ViewManager::new(event_log_store);

// 2. 定义物化视图
let view = MaterializedView::aggregate(
    "sales_summary",
    "orders",                        // 源表
    vec!["store_id".into()],         // 分组
    vec![
        Aggregation::Sum { field: "amount".into(), alias: "total".into() },
        Aggregation::Avg { field: "amount".into(), alias: "avg".into() },
        Aggregation::Count { field: "*".into(), alias: "count".into() },
    ],
    RefreshMode::Pull { interval_secs: 300 },  // 每 5 分钟刷新
);

// 3. 创建视图 (自动: 首次聚合 → 写目标表 → 注册调度)
manager.create_view(view).await?;

// 4. 后台每 5 分钟自动刷新聚合结果到 sales_summary_mv 表

// 5. 手动刷新 (可选)
let rows = manager.refresh_view("sales_summary").await?;

// 6. Push 模式增量更新
manager.incremental_update("orders", &new_events).await?;

// 7. 查询物化视图结果 (预计算,快速)
let mv_table = event_log_store.load_table("sales_summary_mv").await?;
```

---

## 🏗️ 数据流

```
源表 (orders)
    ↓
ViewRefresher.refresh()
    ↓
1. Iceberg Scan → RecordBatch
2. DataFusion MemTable 注册
3. 执行聚合 SQL (SUM/AVG/COUNT + CAST)
4. 结果 RecordBatch
    ↓
EventLogStore.write_batch()
    ↓
1. arrow_to_iceberg_schema (自动建表)
2. write_data_files (Parquet)
3. commit_data_files (事务)
    ↓
目标表 (sales_summary_mv) ← 可查询!

后台调度:
RefreshScheduler → 每 N 秒 → refresh() → 更新目标表
```

---

## 📈 整体进度

```
阶段 0: ████████████████████ 100%  ✅
阶段 1: ████████████████████ 100%  ✅
阶段 2: ████████████████████ 100%  ✅
阶段 3: █████████████████░░░  85%  ⏳ (DataFusion catalog 待优化)
阶段 4: ████████████████████ 100%  ✅ ⭐ 本轮完成!
阶段 5: ░░░░░░░░░░░░░░░░░░░░   0%  📋
阶段 6: ░░░░░░░░░░░░░░░░░░░░   0%  📋

总体: ██████████████░░░░░░ 69% (4.85/7 阶段)
```

---

## 📁 代码统计

### 阶段 4 新增模块
```
materialized_view.rs      180 LOC  ✅
view_manager.rs           200 LOC  ✅
view_refresher.rs         240 LOC  ✅
refresh_scheduler.rs      220 LOC  ✅
event_log_store.rs        +120 LOC (write_batch 等)
```

### nexora-eventlog 总计
```
~2300 LOC
12 个集成测试
100% 测试通过率
```

---

## 🎯 支持的功能

### 聚合函数
- ✅ COUNT (含 COUNT(*))
- ✅ SUM (自动 CAST)
- ✅ AVG (自动 CAST)
- ✅ MAX (自动 CAST)
- ✅ MIN (自动 CAST)

### 刷新模式
- ✅ Pull - 定时后台刷新(近实时全量)
- ⚠️ Push - 接口已暴露,但当前退化为全量刷新(非真增量,见顶部更正)
- ✅ Hybrid - 混合模式

### 其他
- ✅ 多字段分组
- ✅ 全局聚合
- ✅ WHERE 过滤
- ✅ 自动建目标表
- ✅ 结果可查询

---

## 🚀 下一步

**阶段 5:** 分层冷化 + 压实
- Hot/Warm/Cold tier
- Object storage 集成
- Iceberg compaction

**阶段 6:** 本体管理面 (API-first)
- DomainPackage CRUD
- REST + GraphQL

---

## 🎊 总结

**阶段 4 圆满完成!**

- ✅ 完整的物化视图系统
- ✅ 聚合计算引擎 (DataFusion)
- ✅ 全量 + 增量刷新
- ✅ 后台定时调度
- ✅ 目标表自动写入
- ✅ 12/12 测试通过

**核心价值:** 从事件表创建预计算聚合视图,查询性能大幅提升!

---

**最后更新:** 2026-07-20  
**整体进度:** 69% (4.85/7 阶段)
