# Nexora Materialized View

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** Schema 完整（40%），填充逻辑待实现（P0.3）

---

## 1. 概述

Materialized View (MV) 是预计算并持久化的查询结果，用于加速复杂查询。

### 1.1 使用场景

- **复杂聚合查询** — 多跳路径、COUNT/SUM/AVG
- **高频查询** — 仪表板、报表
- **影响范围分析** — 实时依赖风险视图
- **AI Agent 上下文** — 预计算的知识图谱视图

---

## 2. 核心概念

### 2.1 创建 MV

```cypher
CREATE MATERIALIZED VIEW impacted_objects_view AS
MATCH (e:Exception)-[:AFFECTS]->(obj:Object)-[:DEPENDS_ON*1..3]->(downstream:Object)
WHERE e.severity IN ['P1','P2']
RETURN e.id AS exception_id,
       obj.id AS root_object_id,
       count(downstream) AS impacted_count
```

### 2.2 查询 MV

```cypher
-- 自动重写为查询 MV
SELECT * FROM impacted_objects_view
WHERE exception_id = 'EVT001'
```

---

## 3. 数据结构

```rust
pub struct MaterializedView {
    pub id: String,
    pub name: String,
    pub source_query: String,      // Cypher 查询
    pub schema: Vec<ColumnDef>,    // 列定义
    pub refresh_mode: RefreshMode, // 全量/增量
    pub last_refresh: Option<DateTime<Utc>>,
    pub version: u64,
}

pub enum RefreshMode {
    Full,         // 全量重建
    Incremental,  // 增量更新（P2.2）
    OnDemand,     // 手动触发
}
```

---

## 4. 实现计划

### P0.3 — 基础填充逻辑（3-4天）
- 执行 source_query
- 序列化结果到 RocksDB
- 递归处理嵌套 List/Map

### P2.2 — 增量刷新（4天）
- 订阅 GraphMutation
- Delta 应用
- 版本控制

---

## 5. 示例

### 通用依赖风险视图
```cypher
CREATE MATERIALIZED VIEW dependency_risk_view AS
MATCH (e:Exception)-[:AFFECTS]->(obj:Object)
MATCH (obj)-[:DEPENDS_ON*1..3]->(downstream:Object)
RETURN e.id, e.severity, obj.id, count(downstream) AS risk_score
```

### 航空货站航班风险视图
```cypher
CREATE MATERIALIZED VIEW flight_risk_view AS
MATCH (e:Exception)-[:AFFECTS]->(agv:Device {type:'AGV'})
MATCH (agv)-[:EXECUTING]->(task)-[:MOVES]->(piece)
MATCH (piece)-[:LOADED_IN]->(uld)-[:ASSIGNED_TO]->(flight:Flight)
RETURN flight.id, count(DISTINCT piece) AS affected_pieces, max(e.severity) AS max_severity
```

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
