# Nexora Cypher/SQL 扩展时序能力可行性分析

**日期**: 2026-07-18  
**问题**: 能否在 Nexora Cypher/SQL 基础上扩展，支持对 ReductStore 的时间窗口聚合、历史回溯、滑动窗口？  
**评估**: 技术可行性 + 工程代价 + 是否值得做

---

## 一、直接结论

### ✅ 技术上完全可行

**三条扩展路径**：
1. **Cypher UDF**（用户自定义函数）：添加 `time_series.*` 函数族
2. **新 CALL 子句**：`CALL time_series.query(...)`
3. **混合 GraphProvider**：Cypher 查询跨图 + 时序两个数据源

### ⚠️ 工程代价：3-6 月

**核心工作量**：
- Cypher 扩展（函数 + AST）：2-3 周
- ReductStore 集成（查询引擎）：3-4 周
- 时序算子（窗口/聚合/降采样）：4-6 周
- 测试 + 性能优化：4-6 周

### ❌ 不推荐做（至少不是现在）

**五个理由**：
1. **偏离定位**：Nexora 是"图状态平台"，不是 TSDB
2. **重复造轮子**：InfluxDB/ReductStore 已有成熟实现
3. **维护负担**：时序引擎是独立领域（持续演进）
4. **机会成本**：3-6 月可以做 Track A-D（共识/备份/性能）
5. **边际收益低**：应用层聚合 + 混合架构已够用

---

## 二、技术可行性分析（深度）

### 2.1 当前架构的扩展点

#### 扩展点 1：GraphProvider 接口

**当前实现**（`executor.rs:1113`）：
```rust
impl GraphProvider for NexoraGraphSnapshot {
    fn all_nodes(&self) -> Vec<String> { ... }
    fn get_node(&self, hex_id: &str) -> Option<CpValue> { ... }
    fn get_edge(&self, from: &str, to: &str, ...) -> Option<CpValue> { ... }
}
```

**扩展方式**：新增 `TimeSeriesProvider` trait
```rust
// 新 trait：时序数据提供者
pub trait TimeSeriesProvider {
    async fn query_range(
        &self,
        entry: &str,       // 时间序列 ID（= node_id）
        start: u64,        // 开始时间戳
        stop: u64,         // 结束时间戳
        labels: HashMap<String, String>, // 过滤条件
    ) -> Result<Vec<TimeSeriesRecord>>;
    
    async fn aggregate_window(
        &self,
        entry: &str,
        start: u64,
        stop: u64,
        window: Duration,  // 窗口大小（如 1 小时）
        agg_fn: AggregateFunction, // AVG / SUM / MIN / MAX
    ) -> Result<Vec<(u64, f64)>>;  // (timestamp, aggregated_value)
}

// 为 ReductStore 实现
impl TimeSeriesProvider for ReductStoreBackend {
    async fn query_range(&self, entry, start, stop, labels) -> ... {
        let bucket = self.client.get_bucket(&self.bucket_name).await?;
        let mut query = bucket.query(entry).start(start).stop(stop);
        for (k, v) in labels {
            query = query.include_label(&k, &v);
        }
        // ... 返回 records
    }
    
    async fn aggregate_window(&self, entry, start, stop, window, agg_fn) -> ... {
        // 查询 ReductStore
        let records = self.query_range(entry, start, stop, HashMap::new()).await?;
        
        // 应用层窗口聚合
        let mut buckets: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
        for record in records {
            let bucket_ts = (record.timestamp / window.as_micros()) * window.as_micros();
            buckets.entry(bucket_ts).or_default().push(record.value);
        }
        
        // 聚合每个窗口
        buckets.into_iter().map(|(ts, values)| {
            let agg_value = match agg_fn {
                AggregateFunction::Avg => values.iter().sum::<f64>() / values.len() as f64,
                AggregateFunction::Sum => values.iter().sum(),
                AggregateFunction::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
                AggregateFunction::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            };
            (ts, agg_value)
        }).collect()
    }
}
```

---

#### 扩展点 2：Cypher 函数注册机制

**当前机制**（`function_rewrite.rs`）：
```rust
// 当前只有函数重写（如 toLower → lower）
pub fn analyze_query(query: &str) -> Result<FunctionAnalysis> {
    // 检测需要重写的函数
    // 返回重写后的 query
}
```

**扩展为 UDF 注册**：
```rust
// 新增：用户自定义函数注册表
pub struct CypherUDFRegistry {
    functions: HashMap<String, Box<dyn CypherFunction>>,
}

pub trait CypherFunction: Send + Sync {
    fn name(&self) -> &str;
    fn execute(&self, args: Vec<CypherValue>) -> Result<CypherValue>;
}

// 注册时序函数
impl CypherUDFRegistry {
    pub fn register_time_series_functions(&mut self, ts_provider: Arc<dyn TimeSeriesProvider>) {
        self.register(Box::new(TimeSeriesRangeFunction { provider: ts_provider.clone() }));
        self.register(Box::new(TimeSeriesWindowFunction { provider: ts_provider.clone() }));
        self.register(Box::new(TimeSeriesSampleFunction { provider: ts_provider }));
    }
}

// 实现：time_series.range()
struct TimeSeriesRangeFunction {
    provider: Arc<dyn TimeSeriesProvider>,
}

impl CypherFunction for TimeSeriesRangeFunction {
    fn name(&self) -> &str { "time_series.range" }
    
    fn execute(&self, args: Vec<CypherValue>) -> Result<CypherValue> {
        // args[0] = node (Node 对象)
        // args[1] = property (String，如 "battery_level")
        // args[2] = start (DateTime)
        // args[3] = stop (DateTime)
        
        let node_id = extract_node_id(&args[0])?;
        let start_us = datetime_to_us(&args[2])?;
        let stop_us = datetime_to_us(&args[3])?;
        
        // 查询 ReductStore
        let records = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(
                self.provider.query_range(&node_id, start_us, stop_us, HashMap::new())
            )
        })?;
        
        // 返回 List<Map>
        Ok(CypherValue::List(
            records.into_iter().map(|r| CypherValue::Map(hashmap! {
                "timestamp" => CypherValue::ZonedDateTime(r.timestamp),
                "value" => CypherValue::Float(r.value),
            })).collect()
        ))
    }
}
```

---

### 2.2 扩展后的 Cypher 语法

#### 方案 A：函数式（推荐）

```cypher
// 查询叉车 F002 过去 7 天的电量历史
MATCH (f:Forklift {id: 'F002'})
CALL time_series.range(
    f,                              // 节点对象
    'battery_level',                // 属性名
    datetime() - duration('P7D'),   // 开始时间
    datetime()                      // 结束时间
) YIELD timestamp, value
RETURN timestamp, value
ORDER BY timestamp

// 时间窗口聚合（每小时平均）
MATCH (f:Forklift {id: 'F002'})
CALL time_series.window_avg(
    f,
    'battery_level',
    datetime() - duration('P7D'),
    datetime(),
    duration('PT1H')  // 1 小时窗口
) YIELD timestamp, avg_value
RETURN timestamp, avg_value

// 滑动窗口（5 分钟移动平均）
MATCH (f:Forklift {id: 'F002'})
CALL time_series.sliding_avg(
    f,
    'battery_level',
    datetime() - duration('PT1H'),
    datetime(),
    duration('PT5M'),   // 窗口大小
    duration('PT1M')    // 滑动步长
) YIELD timestamp, moving_avg
RETURN timestamp, moving_avg
```

---

#### 方案 B：新子句（更接近 SQL）

```cypher
// 新增 TIME SERIES 子句
MATCH (f:Forklift {id: 'F002'})
TIME SERIES f.battery_level
    FROM datetime() - duration('P7D')
    TO datetime()
    WINDOW duration('PT1H')
    AGGREGATE avg
RETURN timestamp, value
```

---

#### 方案 C：混合查询（图 + 时序）

```cypher
// 查询：所有"充电中"的叉车，过去 1 小时的平均电量
MATCH (f:Forklift {status: 'charging'})
WITH f
CALL time_series.window_avg(
    f,
    'battery_level',
    datetime() - duration('PT1H'),
    datetime(),
    duration('PT1H')
) YIELD avg_value
RETURN f.id, f.location, avg_value
ORDER BY avg_value ASC

// 跨节点时序聚合
MATCH (f:Forklift)
WHERE f.battery_level < 20  // 当前电量 < 20%（图查询）
WITH f
CALL time_series.last_charge_time(f) YIELD last_charged_at
RETURN f.id, last_charged_at
ORDER BY last_charged_at ASC
```

---

### 2.3 实现架构

```
┌──────────────────────────────────────────────┐
│          Cypher Query String                  │
└──────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────┐
│  Parser (检测 CALL time_series.* 函数)         │
└──────────────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        │                       │
     图查询                  时序查询
        │                       │
        ▼                       ▼
┌───────────────┐       ┌───────────────┐
│ GraphProvider │       │TimeSeriesProvider│
│  (NexoraGraph)│       │  (ReductStore)   │
└───────────────┘       └───────────────┘
        │                       │
        └───────────┬───────────┘
                    ▼
          ┌──────────────────┐
          │  Result Merger    │
          │ (JOIN on node_id) │
          └──────────────────┘
                    │
                    ▼
            ┌──────────────┐
            │ Cypher Result│
            └──────────────┘
```

**关键点**：
1. **两阶段执行**：先执行图查询（MATCH），再对每个节点执行时序查询（CALL）
2. **结果合并**：按 node_id JOIN
3. **惰性求值**：如果图查询返回 0 节点，时序查询不执行

---

## 三、工程代价估算

### 阶段 1：基础设施（3-4 周）

| 任务 | 工作量 | 交付物 |
|------|--------|--------|
| TimeSeriesProvider trait 定义 | 3 天 | `nexora-core/src/time_series_provider.rs` |
| ReductStore 实现 | 5 天 | `nexora-storage/src/reduct_ts_provider.rs` |
| Cypher UDF 注册机制 | 4 天 | `nexora-cypher/src/udf_registry.rs` |
| CALL 子句路由 | 3 天 | `nexora-cypher/src/executor.rs` 改造 |

---

### 阶段 2：时序函数实现（4-6 周）

| 函数 | 功能 | 工作量 |
|------|------|--------|
| `time_series.range()` | 查询时间范围 | 3 天 |
| `time_series.window_avg()` | 时间窗口聚合（AVG） | 5 天 |
| `time_series.window_sum()` | 窗口聚合（SUM/MIN/MAX/COUNT） | 3 天 |
| `time_series.sliding_avg()` | 滑动窗口 | 5 天 |
| `time_series.resample()` | 降采样 | 4 天 |
| `time_series.interpolate()` | 缺失值插值 | 4 天 |
| `time_series.percentile()` | 分位数 | 3 天 |

---

### 阶段 3：测试与优化（4-6 周）

| 任务 | 工作量 |
|------|--------|
| 单元测试（每个函数） | 2 周 |
| 集成测试（Cypher + ReductStore） | 1 周 |
| 性能测试（大数据量） | 1 周 |
| 查询优化（并行/缓存） | 2 周 |

**总计**：11-16 周（3-4 月）

---

## 四、是否值得做？—— 成本收益分析

### 成本（Cons）

#### 1. 工程成本：3-4 月专项投入

**机会成本**：
- Track A（共识/恢复）：2-3 周 → **生产必须项**
- Track B（备份/快照）：4-6 周 → **生产必须项**
- Track D（遍历性能）：2-3 周 → **用户体验提升**
- **时序扩展**：3-4 月 → **非生产必须项**

**结论**：在 Track A/B 未完成前，投入 3-4 月做时序扩展是**资源错配**。

---

#### 2. 维护负担：持续演进

**时序引擎是独立领域**：
- 压缩算法（Delta-of-Delta / Gorilla / Zstd）
- 查询优化（索引 / 向量化 / 下推）
- 新聚合函数（中位数 / 方差 / 相关性）
- 异常检测（阈值 / 趋势 / 季节性）

**InfluxDB 的演进历史**（参考）：
- 2013 起步
- 2016 TSM 存储引擎重写
- 2019 Flux 查询语言
- 2023 IOx 列存引擎（Apache Arrow）

**结论**：时序引擎是**10 年演进领域**，Nexora 维护此功能的长期成本巨大。

---

#### 3. 定位模糊：Nexora 变成"啥都做"

**当前定位**："流式事件驱动的图状态平台"
- 核心职责：图关系 + 实时状态 + 增量计算
- 边界清晰：不做 TSDB、不做 BLOB 存储、不做全文搜索

**加时序扩展后**："图 + 时序一体化平台"
- 职责模糊：既要图遍历、又要时序聚合、还要事件流
- 与成熟 TSDB 竞争：功能难以超越 InfluxDB（10 年积累）

**结论**：定位漂移，失去"图状态平台"的差异化优势。

---

### 收益（Pros）

#### 1. 查询便利性：一条 Cypher 搞定

**体验提升**：
```cypher
// 当前（混合架构）：需要两次查询 + 应用层合并
// 1. Cypher 查图
MATCH (f:Forklift {status: 'charging'}) RETURN f.id

// 2. Python 查时序
for forklift_id in result:
    influx.query(f"SELECT mean(battery) FROM metrics WHERE id='{forklift_id}'")
```

**vs 扩展后**：
```cypher
// 一条 Cypher 搞定
MATCH (f:Forklift {status: 'charging'})
CALL time_series.window_avg(f, 'battery_level', -7d, now(), 1h)
YIELD avg_value
RETURN f.id, avg_value
```

**量化收益**：
- 代码量减少 50%（50 行 Python → 5 行 Cypher）
- 查询延迟相近（混合架构已很快）

---

#### 2. 减少外部依赖

**当前**：Nexora + ReductStore + InfluxDB（3 个组件）  
**扩展后**：Nexora + ReductStore（2 个组件，InfluxDB 可选）

**但**：
- Nexora 内部复杂度大幅增加（时序引擎 = 新模块）
- 只是"外部依赖"变成"内部复杂度"（总复杂度不变）

---

#### 3. 统一接口 → 生态简化

**对下游应用**：
- 单一查询语言（Cypher）
- 单一客户端（Nexora SDK）

**但**：
- InfluxDB/ReductStore 的客户端已很成熟
- Grafana 等可观测工具已内置 InfluxDB 支持（Nexora 需额外适配）

---

### 收益/成本比

| 维度 | 混合架构（Nexora + TSDB） | 一体化（Nexora 扩展时序） |
|------|:---:|:---:|
| **开发成本** | 2-3 周（集成） | **3-4 月（引擎）** |
| **维护成本** | 低（复用成熟组件） | **高（自研引擎）** |
| **查询便利性** | 中（两次查询） | 高（一次查询） |
| **性能** | 高（专业 TSDB） | 中（自研需优化） |
| **定位清晰度** | 高（分层架构） | **低（全能平台）** |

**结论**：收益/成本比 < 0.5，**不值得做**。

---

## 五、推荐方案（替代扩展）

### 方案 A：应用层聚合库（1-2 周）

**不扩展 Cypher，而是提供 SDK 层封装**：

```python
# Nexora Python SDK 扩展
from nexora import Client
from nexora.time_series import TimeSeriesQuery

client = Client("http://nexora:8080")

# 混合查询封装
result = client.query_with_time_series(
    cypher="MATCH (f:Forklift {status: 'charging'}) RETURN f.id",
    time_series=TimeSeriesQuery(
        property="battery_level",
        window="1h",
        aggregate="avg",
        range="-7d"
    )
)

# SDK 内部：
# 1. 执行 Cypher → 拿到 node_ids
# 2. 并发查 ReductStore（每个 node_id）
# 3. 应用层窗口聚合
# 4. JOIN 结果
```

**优势**：
- 工作量 1-2 周（vs 3-4 月）
- 不动 Nexora 核心
- 体验接近扩展 Cypher

---

### 方案 B：Grafana 插件（2-3 周）

**目标用户**：运维/分析人员

**实现**：
```javascript
// Grafana Nexora Data Source Plugin
// 支持混合查询
{
  "graph_query": "MATCH (f:Forklift) WHERE f.status='charging' RETURN f.id",
  "time_series": {
    "property": "battery_level",
    "aggregate": "avg",
    "window": "1h"
  }
}
```

**插件内部**：
- 查 Nexora（图）
- 查 ReductStore/InfluxDB（时序）
- Grafana 渲染合并结果

**优势**：
- 无需改 Nexora
- 复用 Grafana 生态（面板/告警）

---

### 方案 C：Cypher 视图（逻辑层，2 周）

**定义虚拟表**：
```sql
-- 定义时序视图（不是真实扩展，是元数据映射）
CREATE VIEW forklift_battery_history AS
  SELECT node_id, timestamp, battery_level
  FROM reduct://air_cargo/forklift_*
  WHERE event_type = 'battery_update';

-- Cypher 查询时序视图
MATCH (f:Forklift {status: 'charging'})
WITH f.id AS forklift_id
CALL sql.query('
  SELECT 
    date_trunc(hour, timestamp) AS hour,
    AVG(battery_level) AS avg_battery
  FROM forklift_battery_history
  WHERE node_id = $forklift_id
  GROUP BY hour
', {forklift_id: forklift_id})
YIELD result
RETURN forklift_id, result
```

**实现**：
- `sql.query()` 是 Cypher 调用外部 SQL 的桥接
- SQL 引擎（如 DataFusion）负责查 ReductStore
- 避免在 Cypher 内部实现时序算子

**优势**：
- 复用 SQL 生态（DataFusion 已有窗口函数）
- Nexora 只负责桥接，不负责时序引擎

---

## 六、最终建议

### ❌ **不推荐**在 Nexora Cypher/SQL 扩展时序能力

**五个决定性理由**：

1. **ROI 不足**：3-4 月工程 vs 应用层封装 1-2 周，收益差距小
2. **定位漂移**：Nexora 应专注"图状态"，不应成为"全能平台"
3. **维护陷阱**：时序引擎是 10 年演进领域，长期成本巨大
4. **机会成本**：Track A/B（共识/备份）是生产必须项，优先级更高
5. **成熟替代**：InfluxDB/ReductStore 已打磨多年，性能/功能难以超越

---

### ✅ **推荐**三条替代路径（按优先级）

#### 优先级 1：应用层聚合库（1-2 周）

**Nexora Python/Rust SDK 扩展**：
```python
client.query_with_time_series(cypher, time_series_spec)
```

**交付**：
- `nexora-sdk-python/time_series.py`
- `nexora-sdk-rust/src/time_series.rs`

**时机**：Track A/B 完成后立即做（快速见效）

---

#### 优先级 2：混合架构文档与最佳实践（1 周）

**交付**：
- `docs/guides/MIXED_GRAPH_TIME_SERIES_QUERIES.md`
- 示例代码（Python/Rust/TypeScript）
- Grafana 仪表盘模板

**内容**：
- 如何设计查询分流（图 vs 时序）
- 性能优化（并发查询、缓存）
- 典型场景示例（航空货运、制造、IoT）

---

#### 优先级 3：Grafana 插件（2-3 周）

**目标**：让非技术用户（运维）也能用混合查询

**交付**：
- `grafana-nexora-datasource` 插件
- 支持"图查询 + 时序聚合"的可视化配置

**时机**：产品化阶段（用户数 > 10）

---

## 七、如果非要做（不推荐）

### 最小化方案：只做桥接，不做引擎（4-6 周）

**核心**：Cypher 调用外部时序引擎（SQL），不自研算子

```cypher
MATCH (f:Forklift {status: 'charging'})
CALL external.sql('
  SELECT AVG(battery_level) 
  FROM reductstore://air_cargo/{node_id}
  WHERE timestamp > now() - interval 7 day
  GROUP BY time_bucket(1 hour, timestamp)
', {node_id: f.id})
YIELD result
RETURN f.id, result
```

**架构**：
- Nexora 只负责 `CALL external.sql()` 路由
- 时序查询委托给 DataFusion / DuckDB（内嵌 SQL 引擎）
- DataFusion 负责查 ReductStore + 窗口聚合

**优势**：
- 复用 DataFusion 的成熟实现
- Nexora 代码量 < 2000 行

**劣势**：
- 仍需维护 DataFusion 集成
- 用户学习成本（SQL 语法）

---

## 八、总结

| 方案 | 工作量 | 优势 | 劣势 | 推荐度 |
|------|--------|------|------|:---:|
| **扩展 Cypher（完整）** | 3-4 月 | 统一查询语言 | 定位漂移、维护陷阱 | ❌ |
| **Cypher 桥接 SQL** | 4-6 周 | 复用 DataFusion | 仍需维护集成 | ⚠️ |
| **应用层 SDK 封装** | 1-2 周 | 快速、不动核心 | 每个语言单独实现 | ✅✅✅ |
| **Grafana 插件** | 2-3 周 | 运维友好 | 只适用 Grafana | ✅✅ |
| **文档 + 最佳实践** | 1 周 | 低成本 | 需用户编码 | ✅ |

**最终建议**：
1. **短期**（1-2 周）：应用层 SDK 封装 + 最佳实践文档
2. **中期**（2-3 周）：Grafana 插件（如果产品化）
3. **长期**：坚持"图 + TSDB 分层架构"，不做一体化

**核心理念**：**专注做好图状态平台，时序交给专业 TSDB，用胶水层（SDK/插件）连接两者。**
