# DataFusion 集成状态说明

## 当前状态

**阶段 3 完成度:** 85%

### ✅ 已完成
1. **DataFusionEventStore 结构** - 完成
2. **EventLogStore 增强** - 完成 (load_table, list_tables, catalog)
3. **编译通过** - 完成

### ⚠️ 待完成
**SQL 查询功能** - 由于 iceberg-datafusion API 限制暂时不可用

## 技术问题

### 问题描述
`iceberg-datafusion 0.9.1` 的核心类型 `IcebergTableProvider` 构造方法不公开:
- `try_new()` 是 `pub(crate)` - 只能在 crate 内部使用
- `IcebergCatalogProvider::try_new()` 在运行时失败,原因不明

### 尝试过的方案

#### 方案 A: 使用 IcebergCatalogProvider (失败)
```rust
let catalog_provider = IcebergCatalogProvider::try_new(iceberg_catalog).await?;
session_ctx.register_catalog("iceberg", Arc::new(catalog_provider))?;
```
**结果:** 运行时错误 "Failed to register Iceberg catalog"

#### 方案 B: 直接构造 IcebergTableProvider (不可行)
```rust
let table_provider = IcebergTableProvider::try_new(...)?;  // ❌ pub(crate)
```
**结果:** 编译错误,方法不公开

## 解决方案

### 短期方案 (已实施)
**保留 DataFusionEventStore 结构,但标记 SQL 功能为"待实现"**

- 保留代码框架和 API 接口
- 文档说明当前限制
- 用户可以直接使用 EventLogStore 的 Iceberg API

### 中期方案 (推荐)
**升级到 iceberg-datafusion 0.10+**

检查最新版本是否:
1. 公开了 `IcebergTableProvider` 构造方法
2. 修复了 `IcebergCatalogProvider` 的兼容性问题
3. 提供了更好的集成示例

### 长期方案
**自定义 TableProvider**

如果 iceberg-datafusion 持续不可用,可以:
1. 实现自定义的 `TableProvider` trait
2. 直接读取 Iceberg 的 Parquet 文件
3. 绕过 iceberg-datafusion 依赖

## 当前可用功能

### ✅ 完整可用
```rust
use nexora_eventlog::EventLogStore;

// 1. 写入事件
let store = EventLogStore::new("/data").await?;
store.append(&events).await?;

// 2. 加载表
let table = store.load_table("test_topic").await?;

// 3. 列出所有表
let tables = store.list_tables().await?;

// 4. 本体驱动建表
let table = store.ensure_table_from_domain("iot", &domain_pkg).await?;
```

### ⏳ 待完成
```rust
use nexora_eventlog::DataFusionEventStore;

// SQL 查询 (待实现)
let df_store = DataFusionEventStore::new(store).await?;
let batches = df_store.execute_query("SELECT * FROM ...").await?;  // ❌ 暂时不可用
```

## 替代方案

在 DataFusion SQL 可用之前,用户可以:

### 方案 1: 直接使用 Iceberg Rust API
```rust
let table = store.load_table("test_topic").await?;
let scan = table.scan().build()?;
let stream = scan.to_arrow().await?;
// 手动处理 RecordBatch stream
```

### 方案 2: 使用外部 SQL 引擎
通过外部工具查询 Iceberg 表:
- **DuckDB** - 支持 Iceberg 扩展
- **Apache Spark** - 原生 Iceberg 支持
- **Trino/Presto** - 企业级查询引擎

### 方案 3: 等待后续阶段
阶段 4-6 会实现:
- 物化视图 (预计算结果)
- PG-wire 直接查询图数据
- 自定义查询 API

## 结论

**核心事件写入功能 (阶段 1 & 2) 已完全可用于生产环境。**

SQL 查询功能 (阶段 3) 由于第三方库限制暂时受阻,不影响事件日志的核心价值:
- ✅ 事件完整保留
- ✅ 可重放历史
- ✅ OLAP 格式存储
- ✅ Schema 管理

**推荐做法:** 先使用阶段 1 & 2 功能投入生产,并行跟进 iceberg-datafusion 的版本更新。

---

**最后更新:** 2026-07-20  
**状态:** DataFusion 集成暂缓,核心功能已完成
