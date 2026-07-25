# Dashboard 与集群模式支持

本文档说明 Nexora Dashboard 及查询接口在多节点集群下的能力边界。
内容基于 3 节点 / 12 分片集群的实测结果。

## 简答

- **Dashboard UI**：任何模式下都可访问。
- **Cypher / SQL 查询**：在多节点集群中，**可分布式子集会自动分布式执行**；
  超出子集的查询返回 **HTTP 501** 并附带可操作的错误提示（不是静默返回错误结果）。
- **per-key REST API**（`/api/v2/graph/node/{qid}`）：按 key 路由，任何模式下都工作。

## 分布式查询能力（多节点集群，实测）

### ✅ 支持并自动分布式执行

| 类别 | 示例 | 机制 |
|------|------|------|
| 节点扫描 | `MATCH (n) RETURN n` | scatter-gather 到各 owner |
| 全局聚合 | `MATCH (n) RETURN count(n), avg(n.age)` | 各分片部分聚合 → 协调器合并 |
| 分组聚合 | `RETURN n.type, count(*)` | 同上 |
| `WITH` 管道 | `MATCH (n) WITH n.age AS a WHERE a>20 RETURN a` | 分布式规划 |
| `UNION` | `... UNION ...` | 分布式规划 |
| 关系遍历 | `MATCH (a)-[:KNOWS]->(b) RETURN a,b` | 跨分区 join（RelJoinSpec）|
| 变长路径 | `MATCH (a)-[:R*1..3]->(b) RETURN a,b` | 跨分区 BFS（PathJoinSpec）|
| 路径变量 | `MATCH p = (a)-[:R]->(b) RETURN p` | 查询重写为组件变量 |
| 节点写 | `CREATE (n:T{...})` / `SET` / `DELETE` / `MERGE` | 按 owner 路由 |

SQL 经 `SELECT`→Cypher 翻译走同一分布式路径，响应带 `execution_mode: "distributed"`。

### ❌ 不支持（需要跨分片事务，返回友好 501）

- **跨分片关系创建**：`CREATE (a)-[:REL]->(b)` —— 源/目标可能在不同 owner。
- **SQL 多表 JOIN / 关联子查询** —— 翻译后无法分布式执行。

超出子集时返回的 501 消息会列出支持/不支持清单和替代方案，Dashboard 会将其结构化渲染。

## 一致性

| 接口 | ReadConcern | 特性 |
|------|-------------|------|
| HTTP（Dashboard/REST） | `Local` | 可读任意副本，可用性优先，最终一致 |
| PG-wire（:5433） | `Majority` + session tracking | read-after-write，强一致，适合生产关键操作 |

## 不同部署模式对照

| 功能 | 单节点 | 单节点集群（分片全本地） | 多节点集群 |
|------|--------|--------------------------|------------|
| Dashboard UI | ✅ | ✅ | ✅ |
| 健康 / 集群监控 | ✅ | ✅ | ✅ |
| 可分布式子集查询 | ✅ 本地 | ✅ 本地 | ✅ 分布式 |
| 跨分片关系 CREATE | ✅ | ✅ | ❌ 501（友好提示）|
| 多表 SQL JOIN | ✅ | ✅ | ❌ 501（友好提示）|
| per-key REST API | ✅ | ✅ | ✅ |

## Dashboard 集群感知

多节点集群下，Dashboard 会：
- 顶部显示集群检测横幅
- 将 501 集群错误结构化渲染（支持项 / 不支持项 / 替代方案）

## 相关实现

见 [CLUSTER_QUERY_SUPPORT_IMPLEMENTATION.md](CLUSTER_QUERY_SUPPORT_IMPLEMENTATION.md)（Phase 1-3 + `--peer` 解析修复的实现细节与实测记录）。

---
*基于 3 节点 / 12 分片集群实测。早期版本曾将 UNION/WITH 误列为不支持，已订正。*
