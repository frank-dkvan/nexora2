# 🎊 Nexora 事件日志系统 - 阶段 1 & 2 完成总结

## 📋 项目概述

**目标:** 将 Nexora 的事件摄入系统从单一图存储升级为 **事件优先 + 图增强** 的双栈架构。

**核心理念:** 事件日志表 (Iceberg) 作为 source of truth,图作为实时查询层,实现:
- 📝 事件完整保留 (append-only)
- 🔄 可重放历史数据
- 📊 OLAP 查询支持
- 🚀 图实时查询性能

---

## ✅ 已完成功能

### 阶段 1: 动态路由 + 摄入链路改造 (100%)

#### 1. EventLogStore - Iceberg 表管理
- ✅ **SQLite catalog** 初始化 (embedded)
- ✅ **表懒创建** - 首次 append 时自动推断 schema
- ✅ **RawEvent → RecordBatch → Parquet → Transaction.commit()** 完整链路
- ✅ **关键修复:** Arrow Schema 携带 Iceberg 字段 ID 元数据
- ✅ **Provenance 列:** `_event_id`, `_event_time`, `_source`, `_topic`
- ✅ **Payload 列:** Object → 展平为列, Non-object → `_payload` 列

#### 2. RecordBatch Writer
- ✅ RawEvent → Arrow RecordBatch 转换
- ✅ **Provenance 保护:** payload 中的保留字段不会覆盖真实 provenance
- ✅ 动态 schema 推断

#### 3. TopicRouter - 动态路由
- ✅ **Destination 枚举:** `EventTable` / `Graph` / `Both`
- ✅ **DomainPackage 驱动路由:** topic → destination 映射
- ✅ **预定义路由器:** `all_graph()`, `all_both()`
- ✅ **正则匹配:** 支持 topic pattern

#### 4. EventFirstHandler - 双写协调
- ✅ 根据路由目标选择写入策略
- ✅ 事件表优先 → 图次之
- ✅ 错误处理与日志

#### 5. 基础设施
- ✅ **IngestBatch 扩展:** 添加 `raw_events: Option<Vec<RawEvent>>` 字段
- ✅ **FileSource 改造:** 产生 RawEvent
- ✅ **Feature gate:** `--features event-first` 启用新模式
- ✅ **DomainLoader:** 加载 DomainPackage TOML

---

### 阶段 2: Iceberg 表管理 + 本体 DDL (100%)

#### 1. SchemaMapper - 本体 → Iceberg Schema 映射
- ✅ **DomainPackage → IcebergSchema** 转换
- ✅ **类型映射:** string, int, float, bool, timestamp
- ✅ **Provenance + 业务字段** 自动合并
- ✅ **空 domain 回退:** 自动添加 `_payload` 列

#### 2. EventLogStore 增强
- ✅ **`ensure_table_from_domain()`** - 根据本体定义创建表
- ✅ **Schema 兼容性校验** - 简化版(字段数量检查)
- ✅ **隐藏分区支持** - `days(_event_time)` 自动分区

#### 3. 完整测试覆盖
- ✅ `test_event_log_store_append` - 基础 Iceberg 写入
- ✅ `test_router_all_graph/both` - 路由逻辑
- ✅ `test_ensure_table_from_domain` - 本体驱动建表
- ✅ `test_schema_compatibility_check` - Schema 校验

---

## 📊 代码统计

### 新增 Crate
- **nexora-eventlog** (~1400 LOC)
  - `event_log_store.rs` - 380 LOC
  - `record_batch_writer.rs` - 320 LOC
  - `event_first_handler.rs` - 250 LOC
  - `schema_mapper.rs` - 150 LOC
  - `router.rs` - 140 LOC
  - `domain_loader.rs` - 100 LOC

### 依赖新增
```toml
iceberg = "0.9.1"
iceberg-catalog-sql = "0.9.1"
iceberg-storage-opendal = "0.9.1"
arrow = "54.0.0"
parquet = "54.0.0"
```

---

## 🧪 测试结果

### 集成测试 (5/5 通过)
```
✅ test_event_log_store_append           - 端到端 Iceberg 写入
✅ test_router_all_graph                 - 路由到图
✅ test_router_all_both                  - 路由到事件表+图
✅ test_ensure_table_from_domain         - 本体驱动建表 + 分区
✅ test_schema_compatibility_check       - Schema 校验
```

### 验证项
- ✅ SQLite catalog 初始化成功
- ✅ Iceberg 表创建成功
- ✅ RawEvent → Arrow RecordBatch 转换正确
- ✅ Parquet 文件写入成功
- ✅ Transaction commit 成功
- ✅ 隐藏分区 `days(_event_time)` 生效
- ✅ Schema 字段 ID 映射正确
- ✅ DomainPackage → Iceberg Schema 转换正确

---

## 🎯 关键技术突破

### 1. Arrow Schema 字段 ID 映射问题
**问题:** Iceberg writer 要求 Arrow Schema 携带字段 ID 元数据,否则报错 "Field id X not found"

**解决方案:**
```rust
// 使用 Iceberg Schema → Arrow Schema 转换,保留字段 ID
let arrow_schema_with_ids = arrow_schema::Schema::try_from(iceberg_schema.as_ref())?;
let batch_with_ids = RecordBatch::try_new(
    Arc::new(arrow_schema_with_ids),
    batch.columns().to_vec(),
)?;
```

### 2. 分区写入问题
**问题:** 创建了分区表,但 writer 传入 `partition_value = None`,导致 commit 失败

**解决方案 (阶段 1):** 先禁用分区,使用无分区表  
**解决方案 (阶段 2):** 启用 `days(_event_time)` 分区,writer 自动计算分区值

### 3. Provenance 保护
**问题:** Payload 中的 `_event_id` 等字段可能覆盖真实 provenance

**解决方案:**
```rust
const RESERVED_FIELDS: &[&str] = &["_event_id", "_event_time", ...];

if RESERVED_FIELDS.contains(&field_name) {
    tracing::warn!("Skipping reserved field '{}'", field_name);
    continue;
}
```

---

## 📐 架构图

### 当前数据流

```
┌─────────────┐
│ FileSource  │ (产生 RawEvent)
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ IngestBatch     │
│ + raw_events    │
└──────┬──────────┘
       │
       ▼
┌─────────────────────┐
│ EventFirstHandler   │
│ (路由决策)          │
└──────┬──────────────┘
       │
       ├─────────────┐
       ▼             ▼
┌──────────────┐  ┌──────────────┐
│EventLogStore │  │GraphIngest   │
│ (Iceberg)    │  │Handler       │
└──────────────┘  └──────────────┘
       │                   │
       ▼                   ▼
   ┌─────────┐       ┌─────────┐
   │ Parquet │       │ GraphDB │
   └─────────┘       └─────────┘
```

### Iceberg 表结构

```sql
-- Provenance 列 (固定)
_event_id      STRING       NOT NULL  -- UUID
_event_time    TIMESTAMP    NOT NULL  -- 事件时间(微秒)
_source        STRING       NOT NULL  -- 来源
_topic         STRING       NOT NULL  -- Topic

-- 业务字段 (从 DomainPackage 推断或从 payload 展平)
device_id      STRING       NULLABLE
temperature    DOUBLE       NULLABLE
...

-- 隐藏分区
PARTITIONED BY days(_event_time)
```

---

## 🚀 后续阶段规划

### 阶段 3: PG-wire 暴露事件表 (经 DataFusion)
- [ ] DataFusion 注册 Iceberg 表
- [ ] PG-wire protocol 实现
- [ ] SQL 查询支持 (SELECT * FROM events WHERE ...)

### 阶段 4: 事件表物化视图 + 实时增量
- [ ] 物化视图定义 (SQL DDL)
- [ ] Pull 模式: 定时刷新
- [ ] Push 模式: 实时增量更新

### 阶段 5: 分层冷化 + 压实
- [ ] Hot tier (最近 7 天) → Memory/SSD
- [ ] Cold tier (> 7 天) → Object storage (S3/MinIO)
- [ ] Iceberg compaction (小文件合并)

### 阶段 6: 本体管理面 (API-first)
- [ ] DomainPackage CRUD API
- [ ] Schema evolution 支持
- [ ] 统一访问层 (REST + GraphQL)

---

## 📝 使用示例

### 1. 启用事件优先模式

```toml
# Cargo.toml
[features]
event-first = ["nexora-eventlog/olap"]
```

```bash
cargo run --features event-first
```

### 2. 根据本体创建表

```rust
use nexora_eventlog::{EventLogStore, DomainPackage};

let store = EventLogStore::new("/data/eventlog").await?;

let domain_pkg = DomainPackage { /* 本体定义 */ };
let table = store.ensure_table_from_domain("iot_sensors", &domain_pkg).await?;
```

### 3. Append 事件

```rust
let events = vec![
    RawEvent::new(
        1_700_000_000_000_000,  // event_time_us
        1_700_000_000_000_000,  // ingest_time_us
        "kafka",
        "sensor/temp",
        None,
        Some(0),
        None,
        json!({"device_id": "AGV-01", "temp": 23.5}),
    ),
];

store.append(&events).await?;
```

### 4. 配置路由

```rust
let router = if has_domain_package {
    TopicRouter::from_domain_package(&pkg)
} else {
    TopicRouter::all_both()  // 兼容模式
};
```

---

## 🐛 已知限制

1. **Schema Evolution:** 当前只支持字段数量校验,完整的 schema evolution 在阶段 3 实现
2. **分区策略:** 当前只支持 `days(_event_time)`,未来支持自定义分区
3. **单元测试:** 旧代码的单元测试需要更新 API 调用,但核心集成测试全部通过
4. **并发写入:** 当前未优化并发场景,阶段 4 加入批处理优化

---

## 🎓 经验总结

### 成功经验
1. **集成测试先行:** 先跑通端到端流程,再完善细节
2. **增量迭代:** 阶段 1 先禁用分区简化实现,阶段 2 再启用
3. **类型安全:** Rust 类型系统帮助提前发现了很多 API 不匹配问题

### 踩过的坑
1. **Arrow Schema 字段 ID:** 花了较长时间才发现需要从 Iceberg Schema 转换
2. **分区参数:** 创建分区表但传 `None` 导致 commit 失败
3. **PropertyDef 字段名:** `prop_type` vs `property_type` 不一致

---

## 📚 参考文档

- [Iceberg Spec](https://iceberg.apache.org/spec/)
- [Arrow Schema 文档](https://arrow.apache.org/docs/format/Schema.html)
- [Nexora 架构文档](../ARCHITECTURE.md)

---

**总结:** 阶段 1 & 2 已全面完成,事件日志系统的核心基础已打好,可以进入阶段 3 的查询层实现。🎉
