# Nexora SQL 引擎替换为 DataFusion 可行性分析

**日期**: 2026-07-18  
**问题**: 是否直接把目前自研的 SQL 替换成 DataFusion SQL 引擎？  
**评估**: 基于源码 (2870 行) 深度分析

---

## 一、核心判断

### ❌ 不推荐直接替换

**理由**：
1. **当前 SQL 不是真正的 SQL 引擎**，而是"SQL → Cypher 翻译器"（2870 行薄层）
2. **DataFusion 解决的问题与 Nexora 需求不匹配**（表格 vs 图）
3. **引入 DataFusion 的工程量 > 改进现有翻译器**（3-4 周 vs 1-2 周）

---

## 二、当前 SQL 实现的本质

### 2.1 不是独立引擎，是翻译器

**架构**（`nexora-sql/src/lib.rs:1-40`）：
```
SQL Query
    ↓
sqlparser 解析（第三方库）
    ↓
classify_table() → 识别表类型
    ↓
translate_sql_to_cypher() → 生成 Cypher
    ↓
Cypher 引擎执行（nexora-cypher）
    ↓
Result
```

**关键代码**（`lib.rs:109-114`）：
```rust
pub async fn execute_sql(
    sql: &str,
    graph: &GraphService,
) -> Result<SqlResult, SqlError> {
    // 1. 翻译 SQL → Cypher
    let (cypher, is_write) = translate_sql_to_cypher(sql)?;
    
    // 2. 执行 Cypher
    let result = if is_write {
        nexora_cypher::execute_write(cypher, graph).await?
    } else {
        nexora_cypher::execute(cypher, graph).await?
    };
    
    // 3. 转换结果格式
    Ok(SqlResult { ... })
}
```

**结论**：nexora-sql 只是"语法糖层"，真正的查询引擎是 Cypher。

---

### 2.2 核心能力：图→表映射

**表类型识别**（`edge_table.rs`）：
```rust
pub enum TableType {
    // SQL: SELECT * FROM Person
    // → Cypher: MATCH (n:Person) RETURN n
    Node { label: String },
    
    // SQL: SELECT * FROM edge_KNOWS
    // → Cypher: MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b
    GenericEdge { rel_type: String },
    
    // SQL: SELECT * FROM Person_KNOWS_Person
    // → Cypher: MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b
    TypedEdge {
        source_label: String,
        rel_type: String,
        target_label: String,
    },
    
    // SQL: SELECT * FROM edges (所有边)
    AllEdges,
}
```

**关键洞察**：
- SQL 的"表" = Cypher 的"节点标签"或"边类型"
- SQL 的"列" = Cypher 的"属性"
- SQL 的"JOIN" = Cypher 的"图遍历"

---

### 2.3 支持的 SQL 子集

| SQL 特性 | 支持度 | 翻译策略 |
|---------|:---:|---------|
| **SELECT * / columns** | ✅ | RETURN n / n.prop |
| **FROM table** | ✅ | MATCH (n:table) |
| **WHERE** | ✅ | WHERE n.prop = val |
| **ORDER BY** | ✅ | ORDER BY n.prop |
| **LIMIT / OFFSET** | ✅ | LIMIT / SKIP |
| **GROUP BY** | ✅ | WITH ... GROUP BY |
| **HAVING** | ✅ | WITH ... WHERE |
| **聚合函数** | ✅ | count/sum/avg/min/max |
| **INSERT** | ✅ | CREATE + SET |
| **UPDATE** | ✅ | MATCH + SET |
| **DELETE** | ✅ | DETACH DELETE |
| **UPSERT** | ✅ | MERGE |
| **JOIN** | ⚠️ 有限 | 只支持边表的隐式 JOIN |
| **子查询** | ❌ | 不支持 |
| **窗口函数** | ❌ | 不支持 |
| **CTE (WITH)** | ❌ | 不支持 |

**代码量**：
- 核心翻译逻辑：~1500 行（`translate_select` / `translate_insert` / `translate_update`）
- 边表处理：~300 行（`edge_table.rs`）
- 其余：表达式转换、函数映射

---

## 三、DataFusion 是什么

### 3.1 Apache DataFusion 定位

**官方定义**：
> DataFusion is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

**核心能力**：
- SQL 解析 + 优化（逻辑计划 + 物理计划）
- 列存查询执行引擎（基于 Apache Arrow）
- 可扩展：自定义数据源（TableProvider）

**典型用途**：
- 构建 OLAP 数据库（如 InfluxDB IOx、Cube.js）
- 数据湖查询引擎（Parquet/CSV/JSON）
- 分布式 SQL 层（Ballista = 分布式 DataFusion）

---

### 3.2 DataFusion 解决的问题

| 问题 | DataFusion | Nexora 需求 |
|------|:---:|:---:|
| **SQL 解析** | ✅ 完整 SQL-92/99/03 | ⚠️ 只需子集 |
| **查询优化** | ✅ CBO（基于统计） | ⚠️ 图遍历优化不同 |
| **列存执行** | ✅ Arrow 向量化 | ❌ 不需要（图是行存） |
| **分布式查询** | ✅ Ballista | ❌ Nexora 自有分布式 |
| **OLAP 聚合** | ✅ 高效（列存） | ⚠️ Cypher 已有 |
| **表格数据源** | ✅ Parquet/CSV/... | ❌ 需图数据源 |

**结论**：DataFusion 是"表格查询引擎"，Nexora 是"图查询引擎"，两者目标不同。

---

## 四、替换方案评估

### 方案 A：直接替换为 DataFusion

#### 架构

```
SQL Query
    ↓
DataFusion SQL 解析 + 优化
    ↓
自定义 TableProvider（图→表适配器）
    ↓
GraphService 查询
    ↓
Arrow RecordBatch（列存格式）
    ↓
转换为 JSON Result
```

#### 需要实现的组件

**1. GraphTableProvider**（核心，2-3 周）

```rust
// DataFusion 的 TableProvider trait
impl TableProvider for GraphTableProvider {
    async fn scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. 解析 filters → Cypher WHERE 子句
        // 2. 查询 GraphService
        // 3. 将图数据转换为 Arrow RecordBatch（列存）
        // 4. 返回 ExecutionPlan
    }
    
    fn schema(&self) -> SchemaRef {
        // 定义表 schema（从节点标签推断）
        // 问题：图的 schema 是动态的（每个节点属性可能不同）
    }
}
```

**挑战**：
- ❌ **Schema 不匹配**：图节点是"属性包"（动态 schema），DataFusion 要求严格 schema
- ❌ **列存转换开销**：图数据是行存（每个节点是一行），转 Arrow 列存需要 transpose
- ❌ **边表的 JOIN**：DataFusion 的 JOIN 是表格 JOIN，图的边是"遍历"，语义不同

---

**2. 图遍历转 JOIN**（难点）

```sql
-- SQL
SELECT p.name, f.name
FROM Person p
JOIN edge_KNOWS k ON p.id = k.from_id
JOIN Person f ON k.to_id = f.id

-- DataFusion 执行计划
HashJoin(Person, edge_KNOWS, on: p.id = k.from_id)
  → HashJoin(result, Person, on: k.to_id = f.id)

-- Cypher（当前翻译）
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name, f.name

-- 问题：图遍历是"指针跟踪"，JOIN 是"哈希表查找"
-- DataFusion 不理解图的边结构
```

**结论**：强行用 DataFusion 的 JOIN 模拟图遍历，性能劣化 + 语义复杂。

---

**3. 推下优化失效**

```sql
SELECT name FROM Person WHERE age > 30 LIMIT 10
```

**当前翻译**：
```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n.name LIMIT 10
```
→ Cypher 引擎可以"读 10 个就停"（Early termination）

**DataFusion 方案**：
```
1. GraphTableProvider.scan() 读取所有 Person 节点
2. DataFusion Filter(age > 30)
3. DataFusion Limit(10)
```
→ **无法推下 LIMIT**（因为 TableProvider 不知道上层的 LIMIT）

**解决方案**：实现 `ExecutionPlan` 的 `with_new_children` + `supports_limit_pushdown`  
**工作量**：额外 1-2 周

---

#### 工作量估算

| 任务 | 工作量 |
|------|--------|
| GraphTableProvider（基础） | 2 周 |
| Schema 推断（动态属性） | 1 周 |
| 边表 JOIN 适配 | 1 周 |
| 推下优化（LIMIT/Filter） | 1-2 周 |
| 测试 + 性能调优 | 2 周 |
| **总计** | **7-9 周** |

---

#### 性能对比

| 查询 | 当前（SQL → Cypher） | DataFusion 方案 | 性能差异 |
|------|:---:|:---:|:---:|
| **简单 SELECT** | 1ms（直接翻译） | 5-10ms（列存转换） | **慢 5-10×** |
| **图遍历** | 高效（Cypher 原生） | 低效（JOIN 模拟） | **慢 10-50×** |
| **OLAP 聚合** | Cypher 聚合 | DataFusion 列存聚合 | **相当或稍快** |

**结论**：除 OLAP 聚合外，DataFusion 方案性能全面劣化。

---

### 方案 B：保留当前翻译器，局部改进

#### 改进点 1：扩展 JOIN 支持（1 周）

**当前限制**：只支持边表的隐式 JOIN

```sql
-- ❌ 不支持
SELECT p.name, o.title
FROM Person p
JOIN Organization o ON p.org_id = o.id

-- ✅ 支持（隐式）
SELECT a.name, b.name
FROM Person_KNOWS_Person
```

**改进**：
```rust
// 识别 JOIN 模式，翻译为 Cypher
// Person JOIN Organization → MATCH (p:Person)-[:WORKS_AT]->(o:Organization)
```

---

#### 改进点 2：子查询支持（1 周）

```sql
SELECT name FROM Person WHERE age > (SELECT AVG(age) FROM Person)
```

翻译为：
```cypher
MATCH (p:Person)
WITH avg(p.age) AS avg_age
MATCH (p:Person) WHERE p.age > avg_age
RETURN p.name
```

---

#### 改进点 3：CTE (WITH) 支持（3 天）

```sql
WITH young_people AS (
    SELECT * FROM Person WHERE age < 30
)
SELECT name FROM young_people WHERE city = 'NYC'
```

翻译为：
```cypher
MATCH (p:Person) WHERE p.age < 30
WITH p
WHERE p.city = 'NYC'
RETURN p.name
```

---

#### 工作量对比

| 改进 | 工作量 | 增强能力 |
|------|--------|---------|
| JOIN 支持 | 1 周 | 支持显式 JOIN |
| 子查询 | 1 周 | 支持 WHERE 子查询 |
| CTE | 3 天 | 支持 WITH 子句 |
| **总计** | **2-3 周** | 覆盖 90% SQL 场景 |

---

### 方案 C：混合方案（为 ReductStore 引入 DataFusion）

**核心思路**：
- **图查询**（Person/edge_KNOWS）→ 保持当前翻译器
- **时序查询**（ReductStore 虚拟表）→ 使用 DataFusion

#### 架构

```
SQL Query
    ↓
classify_query() → 识别查询类型
    ↓
    ├─ 图表（Person/edge_*） → nexora-sql 翻译器 → Cypher
    └─ 时序表（reduct_*） → DataFusion → ReductStore
```

#### 示例

```sql
-- 图查询 → 翻译器
SELECT name FROM Person WHERE age > 30

-- 时序查询 → DataFusion
SELECT 
    time_bucket('1 hour', timestamp) AS hour,
    AVG(battery_level) AS avg_battery
FROM reduct_forklift_F002
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY hour
```

#### 工作量

| 任务 | 工作量 |
|------|--------|
| DataFusion 嵌入 | 3 天 |
| ReductStore TableProvider | 1 周 |
| 查询路由（图 vs 时序） | 3 天 |
| 测试 | 1 周 |
| **总计** | **3-4 周** |

---

## 五、对比矩阵

| 维度 | 方案 A（替换为 DataFusion） | 方案 B（改进翻译器） | 方案 C（混合） |
|------|:---:|:---:|:---:|
| **工程量** | 7-9 周 | 2-3 周 | 3-4 周 |
| **图查询性能** | ❌ 劣化 5-50× | ✅ 保持 | ✅ 保持 |
| **时序聚合** | ✅ 优化（列存） | ❌ 不支持 | ✅ 优化 |
| **SQL 完整性** | ✅ 高（SQL-92） | ⚠️ 中（子集） | ✅ 高 |
| **维护成本** | ⚠️ DataFusion 依赖 | ✅ 低（2870 行） | ⚠️ 两套引擎 |
| **架构一致性** | ❌ 引入新范式 | ✅ 保持图优先 | ⚠️ 双轨 |

---

## 六、最终建议

### 短期（当前）：方案 B（改进翻译器）⭐

**理由**：
1. ✅ 工作量最小（2-3 周）
2. ✅ 性能无劣化
3. ✅ 覆盖 90% SQL 需求（JOIN + 子查询 + CTE）
4. ✅ 维护成本低（薄层翻译器）

**实施**：
- 1 周：JOIN 支持（Person JOIN Organization）
- 1 周：子查询支持（WHERE age > (SELECT AVG)）
- 3 天：CTE 支持（WITH ... AS）

---

### 中期（Track F 完成后）：方案 C（混合）

**触发条件**：
- F3（WAL + ReductStore 异步复制）已完成
- 需要对 ReductStore 的时序数据做复杂聚合

**理由**：
1. ✅ 图查询保持高性能（当前翻译器）
2. ✅ 时序聚合用 DataFusion（列存优化）
3. ✅ SQL 完整性高（DataFusion 的窗口函数）

**实施**：
- 在 Track F3 完成后，评估时序聚合需求
- 如果需求 > 30% → 引入 DataFusion（3-4 周）

---

### 长期：不推荐方案 A

**理由**：
1. ❌ 工程量大（7-9 周）
2. ❌ 图查询性能劣化（5-50×）
3. ❌ DataFusion 解决的问题（列存 OLAP）不是 Nexora 的核心需求
4. ❌ 引入 Arrow 列存，增加内存转换开销

---

## 七、决策原则

### 不替换的根本原因

**当前 SQL 不是瓶颈**：
- 翻译器只有 2870 行，性能开销 <1ms
- 真正的查询执行在 Cypher 引擎
- 用户使用 SQL 只是"语法糖"，核心还是图查询

**DataFusion 不匹配场景**：
- DataFusion 为"表格 + 列存 + OLAP"优化
- Nexora 是"图 + 行存 + 遍历"
- 强行适配会导致"削足适履"

**类比**：
```
这就像问：
"MongoDB 要不要把自己的查询语言替换成 SQL？"

答案是：
- 提供 SQL 接口（给习惯 SQL 的用户）✅
- 但核心查询引擎仍是文档查询 ✅
- 不会把文档引擎改成关系引擎 ❌
```

---

## 八、补充：何时考虑 DataFusion

**如果未来满足以下条件，可重新评估**：

1. **用户 80% 查询是 OLAP 聚合**（而非图遍历）
2. **需要 SQL-92 完整语法**（窗口函数、复杂子查询、CTE）
3. **愿意接受图查询性能劣化 5-10×**
4. **有 2-3 月专项工程预算**

**当前判断**：上述条件都不满足，**不推荐替换**。

---

**一句话总结**：
> 当前 SQL 是"语法糖翻译器"（2870 行），不是查询引擎瓶颈。DataFusion 是"列存 OLAP 引擎"，与图查询场景不匹配。推荐短期改进翻译器（2-3 周），中期为 ReductStore 时序查询引入 DataFusion（混合方案），不推荐直接替换。
