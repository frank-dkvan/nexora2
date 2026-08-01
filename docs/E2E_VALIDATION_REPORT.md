# Nexora 2.0 端到端能力验证报告

**日期**: 2026-08-01
**范围**: OLAP 查询、时间旅行、性能基线、集成测试
**方法**: 源码审查 + 运行时 HTTP 探测 (单节点 release 构建) + criterion 基准

---

## 摘要

对用户提出的四项下一步任务进行了验证。核心图引擎与 Graph OLAP (SQL 聚合) **可用且性能良好**；但三项常被文档描述为"已完成"的能力 —— **Iceberg 事件表 OLAP、HTTP 事件摄取、时间旅行查询** —— 经核实在当前代码中**未接线或为桩实现**,无法通过对外 API 使用。本报告如实区分"可用"与"未接线"。

| 能力 | 状态 | 说明 |
|------|------|------|
| Graph SQL OLAP (聚合) | ✅ 可用 | COUNT/SUM/AVG/GROUP BY 正确翻译执行 (针对带标签数据) |
| Cypher 属性聚合 | ✅ 可用 | 不依赖标签, 对任意节点可用 |
| CRUD / 遍历性能 | ✅ 已测 | 见性能基线 |
| Iceberg 事件表 OLAP (HTTP) | ❌ 未接线 | 无直接 SQL 端点; 仅物化视图内部 DataFusion 可用 |
| HTTP 事件摄取到 Iceberg | ❌ 不存在 | 无 `/api/ingest/event` 路由; 仅流式源可写事件表 |
| 时间旅行查询 | ❌ 未接线 | `AS OF` 解析后被丢弃; Fragment 引擎未存入 AppState |

---

## 任务 1: Iceberg OLAP 查询能力验证

### 关键发现: 存在两套独立的 "SQL" 路径

**路径 A — `/api/query/sql` (查询【图】, 非 Iceberg)**
`execute_sql` (handlers.rs:3652) 把 SQL **翻译成 Cypher** 在**内存图**上执行
(`nexora_sql::translate_sql_to_cypher` → `nexora_cypher::execute_cypher`)。
与 Iceberg 事件表**完全无关**。

运行时验证 (✅ 全部通过):
- `SELECT COUNT(*) FROM User` → `MATCH (n:User) RETURN count(*)` → 13 行 ✓
- `SELECT COUNT(*) FROM TestNode` → 1378 行 ✓
- `SELECT action, COUNT(*) FROM Event GROUP BY action` → 正确分组 ✓
- Cypher 属性聚合 `MATCH (n) WHERE n.event_type='user.action' RETURN n.action, count(*), sum(n.amount)`
  → click:13/view:13/purchase:12/scroll:12, sum 正确 ✓

**已知限制**:
- SQL `FROM <Table>` 映射为 `MATCH (n:<Table>)` —— 需节点有**真实标签**。
  `/api/ingest/bulk` 写入的节点**无标签** (`labels()` 显示 "Node" 只是默认显示名,
  `MATCH (n:Node)` 匹配 0 行) → 只能用 Cypher 属性过滤查询。
- 带点号的表名 (如 `FROM user.action`) 被截断; 带 `.` 的属性在 WHERE 中触发解析错误。
- 不支持 JOIN、相关子查询。

**路径 B — Iceberg 事件表 OLAP (无直接 HTTP 端点)**
- 仅通过**物化视图** (`ViewRefresher::refresh_sql` view_refresher.rs:318):
  Iceberg scan → DataFusion `MemTable` → 跑 SQL。代价: 每次全表载入内存, 无下推。
- `DataFusionEventStore::register_table` (datafusion_store.rs:39) 是 **TODO 桩**
  (iceberg-datafusion `IcebergTableProvider::try_new()` 为 pub(crate), 被阻塞)。
- `/api/event-streaming/query` (feature-gated) 委托内嵌 RisingWave, 返回值为简化 String。

### 结论
Graph SQL OLAP 聚合**可用**。直接对 Iceberg 事件表做 HTTP SQL **不可用** —— 这是真实的实现缺口, 非配置问题。

---

## 任务 2: 时间旅行查询

### 结论: 无法通过任何对外 API 使用

存在**三条彼此不连通**的链路:

1. **Cypher `AS OF` 解析器** (handlers.rs:637-666): 能解析 `AS OF <ts>`
   (数值微秒或 YYYYMMDD), 但时间戳被剥离后**丢弃**; `execute_cypher`
   签名 (cypher/src/lib.rs:47) **不接受时间参数**。`as_of` 只原样回显到响应,
   查询始终针对**当前活跃图**执行。

2. **Fragment 回放引擎** (nexora-fragment/src/time_travel.rs, ~596 行):
   唯一真正实现时间旅行语义处 (`execute_time_travel` + `TimeTravelQuery::at`,
   通过重放 nodes.jsonl 重建历史)。但**非测试调用者为零** ——
   `TieredFragmentStore` 创建后未存入 `AppState` (main.rs:1409-1414),
   HTTP handler 访问不到。

3. **`/api/graph/history` 端点** (handlers.rs:3779): 是桩,
   忽略 qid/as_of, 只返回 `{"active_nodes":N,"time_travel":"enabled"}`。

4. **Iceberg 层** (event_log_store.rs:554 `read_snapshot_delta`): 支持按
   snapshot_id 读增量, 但仅供物化视图刷新用; 无"时间戳→snapshot_id"映射,
   不对外暴露 (iceberg 原生有 `snapshot_for_timestamp`, 代码库未用)。

### 要真正启用需要 (三选一/组合)
1. 将 `TieredFragmentStore` 存入 `AppState`;
2. `execute_cypher` handler 在 `as_of_ts` 存在时改走 `execute_time_travel`;
3. Iceberg 层实现时间戳→snapshot 映射。

---

## 任务 3: 性能基线 (已采集)

**方法**: `cargo run --release -p nexora-bench -- --all` (进程内 `InMemoryPersistor`,
不经 HTTP/持久化 WAL —— 测的是核心图引擎上限)。

### CRUD / 遍历 (ops/sec, 延迟 µs)
| 操作 | 吞吐 | p50 | p95 |
|------|------|-----|-----|
| create_node (10K) | 38,368 ops/s | 25 | 37 |
| create_node (50K) | 14,329 ops/s | 69 | 118 |
| set_property | 89,124 ops/s | 10 | 16 |
| get_property | 2,240,415 ops/s | 0 | 1 |
| add_edge | 88,111 ops/s | 11 | 16 |
| get_edges | 2,327,859 ops/s | 0 | 1 |
| traversal_2_hop | 94,347 ops/s | 10 | 14 |

### TPC 图查询 (LDBC 风格, QPS + 延迟 µs)
| 查询 | QPS | p50 | p95 |
|------|-----|-----|-----|
| Q1 friends_of_friends | 26,283 | 36 | 61 |
| Q2 shortest_path | 7,826 | 77 | 317 |
| Q3 degree_centrality | 824,035 | 1 | 1 |
| Q4 common_friends | 189,753 | 4 | 9 |
| Q5 trending_topics | 11,923 | 85 | 159 |
| Q6 shortest_message_path | 733,945 | 1 | 2 |
| Q7 friend_recommendations | 1,738 | 563 | 1089 |

HTML 报告: `/tmp/nexora-perf/baseline.html`

**注意**: 这些是**进程内引擎**数字, 远高于 HTTP 端到端 (E2E 顺序 curl 实测 ~47 ops/s,
受 curl 进程 + 单连接 + 每写一次 fsync 限制)。要对齐 `docs/PERFORMANCE_BENCHMARK.md`
的 ≥1000 QPS/P99 目标, 需一个针对 HTTP/PgWire 端点的负载生成器 —— **该压测工具仓库中尚不存在**。
更高效的批量摄取应走 `POST /api/ingest/bulk` (内部 `write_batch` concurrency=64)。

---

## 任务 4: 集成测试套件 + CI/CD

### 已修正: E2E 测试假阳性
`scripts/test-e2e-full-pipeline.sh` 原 **Phase 4 存在假阳性**:
1. 使用了不存在的 `/api/ingest/event` → 100 个事件全部 404 (无路由);
2. `SELECT COUNT(*) FROM events` 查的是**图**不是 Iceberg;
3. 断言 `grep -q "total"` 匹配的是响应中的**列名** "total", 即使空结果也"通过"。

**修正**: Phase 4 改为诚实测试**可用**的 Graph SQL OLAP 路径 ——
用 Cypher UNWIND 批量建带标签 `Event` 节点 → `SELECT COUNT(*) FROM Event`
(断言 == 节点数) + `GROUP BY action` (断言分组数)。摘要与"下一步"文案也
更新为反映真实实现边界。

### CI/CD 集成 (下一步)
建议将修正后的 E2E 脚本 + `nexora-bench` 基线纳入 CI (见文末)。
