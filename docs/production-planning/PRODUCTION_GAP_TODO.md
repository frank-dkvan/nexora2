# Nexora 生产级差距与优化清单

**生成时间:** 2026/07/05  
**代码库版本:** main @ 6c16c00  
**扫描范围:** 全部 20+ crates, 338 Rust 文件

---

## 执行摘要

**当前生产就绪度:** ⭐⭐⭐⭐☆ **4.3/5**

### 核心优势
✅ Actor-per-Node 架构稳定（事件溯源 + WAL + RocksDB）  
✅ Cypher 写操作生产级（CREATE/SET/DELETE/MERGE）  
✅ 核心代码完全领域无关（无行业硬编码）  
✅ 测试覆盖良好（Assert-based + 故障注入 + 压测）  
✅ 分布式能力就绪（Zenoh + Raft + PG Wire）

### 关键差距
⚠️ **图数据模型非一等公民** — Label/EdgeProperty 使用 synthetic property 模拟  
⚠️ **无 Tombstone 原语** — 节点删除仅清空属性，无真正删除语义  
⚠️ Cypher 查询依赖全图快照（MAX_SNAPSHOT_NODES = 100,000 硬限制）  
⚠️ Standing Query 触发器缺失（边/标签变化不触发重新评估）  
⚠️ Materialized View 填充逻辑为 stub  
⚠️ 分层存储接口定义但未集成

---

## P0：必须立即实现（生产阻塞）

### P0.1 正式图数据模型重构 ⏱️ 7-10 天

**问题描述:**  
当前图模型存在多处临时实现，不符合生产级图数据库要求：

1. **Label 非一等公民** — 使用 synthetic property `__labels: List<String>` 模拟
2. **EdgeProperty 模拟存储** — 使用 `__rel_<type>_<target>_<prop>` 模式存储边属性
3. **无 Tombstone 原语** — DELETE 仅清空属性，节点 Actor 可能残留内存
4. **GraphEvent 不标准** — 缺少 LabelAdded/LabelRemoved/EdgePropertySet 等事件
5. **无 Namespace/Tenant 隔离** — 多租户支持缺失

**影响范围:**  
- `nexora-core/src/graph/node_task.rs` — NodeTask 数据结构
- `nexora-core/src/event.rs` — NodeChangeEvent 枚举
- `nexora-cypher/src/write_executor.rs:111-131` — Label 操作
- `nexora-cypher/src/write_executor.rs:183-186` — EdgeProperty 操作
- `nexora-cypher/src/write_executor.rs:427-440` — DELETE 语义

**当前实现证据:**

```rust
// nexora-cypher/src/write_executor.rs:111-131
/// Persist a node's labels as the synthetic `__labels` list.
graph.set_property(qid, "__labels", PropertyValue::List(label_list))

// nexora-cypher/src/write_executor.rs:183-186
/// Pattern: `__rel_<type>_<target_id>_<prop_key>`
let synthetic_key = format!("__rel_{}_{}_{}", rel_type, qid.to_hex(), prop_key);

// nexora-cypher/src/write_executor.rs:427-440
// The actor-per-node model has no tombstone primitive, so clearing all 
// properties (including the synthetic `__labels`) is the closest to node 
// deletion available.
```

**解决方案（分阶段）:**

#### 阶段 1：扩展核心数据结构（3 天）

1. **新增 NodeRecord 结构:**
   ```rust
   pub struct NodeRecord {
       pub id: NexoraId,
       pub labels: HashSet<Symbol>,           // 一等公民 Label
       pub properties: BTreeMap<Symbol, PropertyValue>,
       pub created_at: EventTime,
       pub updated_at: EventTime,
       pub version: u64,
       pub namespace: Option<Symbol>,         // 命名空间
       pub tenant_id: Option<Symbol>,         // 租户 ID
       pub tombstone: Option<TombstoneRecord>, // 删除标记
   }
   
   pub struct TombstoneRecord {
       pub deleted_at: EventTime,
       pub deleted_by: Option<String>,
       pub reason: Option<String>,
   }
   ```

2. **新增 EdgeRecord 结构:**
   ```rust
   pub struct EdgeRecord {
       pub src: NexoraId,
       pub edge_type: Symbol,
       pub dst: NexoraId,
       pub properties: BTreeMap<Symbol, PropertyValue>, // 一等公民边属性
       pub created_at: EventTime,
       pub version: u64,
   }
   ```

3. **扩展 GraphEvent:**
   ```rust
   pub enum GraphMutation {
       // Node operations
       NodeCreated { id: NexoraId, labels: Vec<Symbol>, namespace: Option<Symbol> },
       PropertySet { node: NexoraId, key: Symbol, value: PropertyValue, prev: Option<PropertyValue> },
       PropertyRemoved { node: NexoraId, key: Symbol, prev: PropertyValue },
       
       // Label operations (NEW)
       LabelAdded { node: NexoraId, label: Symbol },
       LabelRemoved { node: NexoraId, label: Symbol },
       
       // Edge operations
       EdgeAdded { src: NexoraId, edge_type: Symbol, dst: NexoraId },
       EdgeRemoved { src: NexoraId, edge_type: Symbol, dst: NexoraId },
       
       // Edge property operations (NEW)
       EdgePropertySet { src: NexoraId, edge_type: Symbol, dst: NexoraId, key: Symbol, value: PropertyValue },
       EdgePropertyRemoved { src: NexoraId, edge_type: Symbol, dst: NexoraId, key: Symbol },
       
       // Node deletion (NEW)
       NodeDeleted { id: NexoraId, tombstone: TombstoneRecord },
       NodeRestored { id: NexoraId },
   }
   ```

#### 阶段 2：迁移 Label 存储（2 天）

1. 修改 `NodeTask` 添加 `labels: HashSet<Symbol>` 字段
2. 提供兼容层：读取时同时检查 `labels` 字段和 `__labels` 属性
3. 写入时同时更新两处（过渡期）
4. 添加迁移工具将 `__labels` 迁移到 `labels` 字段

#### 阶段 3：迁移 EdgeProperty 存储（2 天）

1. 修改 `HalfEdge` 添加 `properties: BTreeMap<Symbol, PropertyValue>` 字段
2. 提供兼容层：读取时同时检查边属性和 `__rel_*` 合成属性
3. 添加迁移工具

#### 阶段 4：实现 Tombstone 原语（2-3 天）

1. 修改 NodeTask 添加 `tombstone: Option<TombstoneRecord>` 字段
2. DELETE 操作设置 tombstone 而非清空属性
3. 实现 `GraphService::cleanup_tombstones()` 定期清理
4. 查询自动过滤已删除节点

**验收标准:**  
- [ ] Label 操作触发 `LabelAdded`/`LabelRemoved` 事件
- [ ] EdgeProperty 可直接读写，不依赖合成属性
- [ ] DELETE 后节点有明确 tombstone，查询自动过滤
- [ ] WAL replay 后 Label/EdgeProperty/Tombstone 状态一致
- [ ] 提供 `__labels` → `labels` 迁移脚本
- [ ] 兼容层测试：旧数据可正常读取
- [ ] Namespace/Tenant 隔离测试
- [ ] 单元测试覆盖所有新 GraphMutation 类型

**优先级:** 🔴 **P0** — 图模型基础，影响所有后续功能

**依赖关系:** 此任务完成后才能正确实现 P1.1 (Standing Query 触发器)

---

### P0.2 移除 Cypher 全图快照限制 ⏱️ 5-7 天

**问题描述:**  
当前 Cypher 查询依赖全图快照，节点数超过 100,000 时查询直接失败。

**影响范围:**  
- **文件:** `nexora-cypher/src/executor.rs:104-107`
- **代码片段:**
  ```rust
  const MAX_SNAPSHOT_NODES: usize = 100_000;
  if node_ids.len() > MAX_SNAPSHOT_NODES {
      return Err(format!("Cypher snapshot contains {} nodes, exceeding safety limit"));
  }
  ```

**解决方案:**  
1. **短期 (P0.1a):** 增大限制至 1,000,000 并添加流式处理警告
2. **中期 (P0.1b):** 实现 Lazy Node Loading（只在 RETURN 时加载节点详情）
3. **长期 (P0.1c):** 实现完整查询计划器（索引下推 + Limit 下推）

**验收标准:**  
- [ ] 1M 节点图可执行选择性查询 `MATCH (n:Device {id: "X"}) RETURN n`
- [ ] EXPLAIN 输出显示索引使用情况
- [ ] 查询超时机制（默认 30s）
- [ ] 测试覆盖：100k/500k/1M 节点规模

**优先级:** 🔴 **P0** — 阻塞大规模生产部署

---

### P0.3 完成 Materialized View 填充逻辑 ⏱️ 3-4 天

**问题描述:**  
Materialized View schema 完整但填充逻辑为 TODO stub。

**影响范围:**  
- **文件:** `nexora-app/src/handlers/materialized_view.rs:386`
- **代码片段:**
  ```rust
  // TODO: Execute the source query and populate the view
  ```

**解决方案:**  
1. 集成 Cypher executor 执行 `source_query`
2. 将结果序列化为 RocksDB key-value
3. 实现 PropertyValue::List/Map 递归序列化（line 131-133）
4. 持久化 view metadata 到 `materialized-views` column family

**验收标准:**  
- [ ] `CREATE MATERIALIZED VIEW risk_view AS MATCH (e:Exception)...` 自动填充
- [ ] 嵌套 List/Map 完整序列化
- [ ] RocksDB 崩溃恢复后 view 数据完整
- [ ] API 查询 view 返回正确结果

**优先级:** 🔴 **P0** — Materialized View 功能不可用

---

### P0.4 递归序列化嵌套 PropertyValue ⏱️ 1 天

**问题描述:**  
PropertyValue::List 和 Map 的序列化仅处理一层，嵌套结构被忽略。

**影响范围:**  
- **文件:** `nexora-app/src/handlers/materialized_view.rs:131-133`

**解决方案:**  
```rust
PropertyValue::List(values) => {
    json!(values.iter().map(|v| property_value_to_json(v)).collect::<Vec<_>>())
}
PropertyValue::Map(map) => {
    json!(map.iter().map(|(k, v)| (k.clone(), property_value_to_json(v))).collect::<BTreeMap<_, _>>())
}
```

**验收标准:**  
- [ ] 支持任意深度嵌套 `List<Map<String, List<Integer>>>`
- [ ] 单元测试覆盖 3 层嵌套

**优先级:** 🔴 **P0** — 数据完整性风险

---

## P1：下一阶段实现（完善核心能力）

### P1.1 Standing Query 边变化触发器 ⏱️ 4-5 天

**问题描述:**  
当前 Standing Query 仅订阅属性变化，边增删、标签变化、节点删除不触发重新评估。

**影响范围:**  
- **核心框架:** `nexora-standing-query/src/lib.rs` (完整)
- **触发器集成:** 未实现

**需要实现的触发器:**  
1. **EdgeAdded** — 新边建立时评估受影响的 SQ
2. **EdgeRemoved** — 边删除时评估受影响的 SQ
3. **LabelAdded/LabelRemoved** — `__labels` 属性变化时触发
4. **NodeDeleted** — 自动调用 `StandingQueryManager::cleanup_node()`

**解决方案:**  
修改 `nexora-cypher/src/write_executor.rs`：

```rust
// 在 execute_create_edge() 后添加
if let Some(sq_mgr) = &self.standing_query_manager {
    sq_mgr.on_edge_added(src_id, edge_type, dst_id).await?;
}

// 在 execute_delete() 后添加
if let Some(sq_mgr) = &self.standing_query_manager {
    sq_mgr.cleanup_node(node_id).await?;
}
```

**验收标准:**  
- [ ] `MATCH (a)-[:EXECUTING]->(t)` 当新边建立时自动触发
- [ ] 边删除后 SQ 结果更新
- [ ] `__labels` 增删触发 label 匹配规则
- [ ] 集成测试：创建边 → 验证 SQ 命中 → 删除边 → 验证 SQ 失效

**优先级:** 🟡 **P1** — Standing Query 功能不完整

---

### P1.2 持久化 Standing Query 状态 ⏱️ 3 天

**问题描述:**  
Standing Query 状态仅保存在内存，重启后丢失。

**影响范围:**  
- **已定义 CF:** `standing-queries`, `standing-query-states` (RocksDB column families)
- **未填充数据**

**解决方案:**  
1. SQ 注册时持久化规则到 `standing-queries` CF
2. 每次命中时持久化匹配状态到 `standing-query-states` CF
3. 启动时从 RocksDB 恢复 SQ

**验收标准:**  
- [ ] RocksDB 重启后 SQ 规则自动恢复
- [ ] 命中历史可查询
- [ ] 崩溃恢复测试

**优先级:** 🟡 **P1** — 生产环境需持久化

---

### P1.3 Fixpoint 集成 Standing Query ⏱️ 2-3 天

**问题描述:**  
Fixpoint 增量传递闭包已实现，但未连接到 Standing Query。

**解决方案:**  
当 Fixpoint 计算出 ReachabilityDelta 时，广播到 StandingQueryManager 重新评估受影响的路径模式。

**验收标准:**  
- [ ] `MATCH (a)-[:DEPENDS_ON*1..3]->(b)` 自动增量计算
- [ ] 边增删时只重算受影响的子图
- [ ] 性能测试：10k 节点图边增删延迟 < 100ms

**优先级:** 🟡 **P1** — 性能优化关键

---

### P1.4 查询优化器补全 ⏱️ 2 天

**问题描述:**  
EXPLAIN 输出缺少 label/property 基数统计。

**影响范围:**  
- **文件:** `nexora-app/src/handlers/explain.rs:162`

**解决方案:**  
从 `LabelIndex` 和 `PropertyIndex` 收集基数：

```rust
let label_cardinality = graph.label_index().count_nodes_with_label(label)?;
let property_cardinality = graph.property_index().count_distinct_values(key)?;
```

**验收标准:**  
- [ ] EXPLAIN 输出包含 `label_cardinality` 和 `property_selectivity`
- [ ] 查询计划器根据基数选择最优执行路径

**优先级:** 🟡 **P1** — 查询性能优化

---

## P2：可延后（增强功能）

### P2.1 分层存储集成 ⏱️ 5-7 天

**问题描述:**  
StorageBackend trait 定义完整，但未集成到 GraphService。

**解决方案:**  
1. 实现 Hot (RocksDB) → Warm (Parquet) → Cold (S3) 迁移策略
2. 基于访问频率自动降级冷数据
3. 查询时透明穿透多层存储

**优先级:** 🟢 **P2** — 生产初期不必需

---

### P2.2 Materialized View 增量刷新 ⏱️ 4 天

**问题描述:**  
当前 MV 刷新为全量重建，大规模 view 刷新耗时长。

**解决方案:**  
订阅 GraphMutation 事件，只应用增量 delta 更新 view。

**优先级:** 🟢 **P2** — 性能优化

---

### P2.3 查询重写器增强 ⏱️ 2 天

**问题描述:**  
Query Rewriter 未提取 view 未覆盖的过滤器。

**影响范围:**  
- **文件:** `nexora-app/src/query_rewriter.rs:105`

**优先级:** 🟢 **P2** — 查询优化

---

## Experimental：已实现（生产可用）

以下实验性功能已完整实现，可直接用于生产：

✅ **HNSW 向量搜索** — `nexora-hnsw` crate 完整  
✅ **Zenoh 分布式通信** — 集群模式可用  
✅ **UDF (Rust/WASM/Python)** — 用户自定义函数  
✅ **PG Wire 协议** — PostgreSQL 客户端兼容  
✅ **WAL 加密** — AES-256-GCM 加密  

---

## Test-only：仅测试用（无生产风险）

以下内容仅出现在测试代码中，不影响生产：

- `scenario_forklift.rs` — 叉车车队监控场景测试
- `materialized_view.rs:385` — `high_speed_forklifts` 测试用例

**结论:** ✅ 核心代码完全领域无关

---

## Should-remove：建议删除或重构

**无需删除项** — 代码质量良好，未发现死代码或冗余实现。

---

## 总工作量估算

| 优先级 | 任务数 | 总工作量 | 关键路径 |
|--------|--------|----------|----------|
| P0 | 4 | 16-22 天 | 图模型重构 (10d) + 快照限制 (7d) |
| P1 | 4 | 11-13 天 | SQ 触发器 (5d) + 持久化 (3d) |
| P2 | 3 | 11-13 天 | 分层存储 (7d) |
| **总计** | **11** | **38-48 天** | **约 8-10 周** |

---

## 下一步行动

### 立即开始（第一周）
1. **P0.1 阶段 1** — 扩展核心数据结构（NodeRecord/EdgeRecord/GraphMutation）（3 天）
2. **P0.4** — 递归序列化嵌套 PropertyValue（1 天）
3. **P0.1 阶段 2** — 迁移 Label 存储（2 天）

### 第二周
4. **P0.1 阶段 3** — 迁移 EdgeProperty 存储（2 天）
5. **P0.1 阶段 4** — 实现 Tombstone 原语（2-3 天）
6. **P0.2a** — 临时增大 MAX_SNAPSHOT_NODES 至 1M（1 天）

### 第三周
7. **P0.3** — 完成 Materialized View 填充逻辑（3-4 天）
8. **P0.2b** — 实现 Lazy Node Loading（3-4 天）

### 第四周
9. **P1.1** — Standing Query 边变化触发器（4-5 天）
10. **P1.2** — 持久化 Standing Query 状态（3 天）

---

**文档版本:** v2.0  
**维护者:** Nexora Team  
**最后更新:** 2026/07/05  
**关键变更:** 新增 P0.1 图模型重构任务（附件首要任务）
