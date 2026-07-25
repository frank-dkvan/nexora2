# Nexora 时间旅行与回滚功能深度审查报告

**审查日期**: 2026-07-11  
**审查对象**: 时间旅行查询 (Time Travel Query) 与节点回滚 (Node Rollback)  
**结论**: ⚠️ **部分实现，生产就绪度不足**

---

## 执行摘要

Nexora 声称支持 "时间旅行查询" 和 "审计/回滚" 功能，但实际实现存在**严重的功能缺失和架构割裂**：

### ✅ 已实现的部分
1. **Fragment-based 时间旅行** (`nexora-fragment` crate)
   - 基于不可变时间分片的历史查询
   - 支持查询指定时间点的节点快照
   - 有完整的单元测试覆盖

2. **事件溯源基础设施** (`nexora-core`)
   - 每个 NodeTask 维护 Journal（事件日志）
   - WAL (Write-Ahead Log) 记录所有变更
   - 软删除机制 (Tombstone)

### ❌ 缺失的关键功能
1. **没有 HTTP API 暴露时间旅行查询**
   - `/api/v2/graph/history` 端点是**假的**（只返回状态消息）
   - 无法通过 REST API 查询历史状态
   
2. **没有节点回滚功能**
   - 虽然有 `NodeRestored` 事件类型，但**未实现**
   - 无法恢复已删除的节点
   - 无法将节点状态回滚到历史版本

3. **Journal → Fragment 管道未连接**
   - 活跃图的 Journal 事件**不会自动**归档到 Fragment
   - FragmentSealer 存在但未集成到主应用

4. **分布式场景下的时间旅行未考虑**
   - 跨分片的历史查询语义不明确
   - 缺少全局时间戳协调

---

## 详细分析

### 1. Fragment-based 时间旅行实现

#### 架构设计
```rust
// crates/nexora-fragment/src/time_travel.rs

pub struct TimeTravelQuery {
    pub as_of_us: u64,           // 查询时间点（微秒）
    pub namespace: Option<String>,
    pub node_id: Option<NexoraId>, // 可选：指定节点
    pub property_filter: Option<...>,
}

pub struct NodeSnapshot {
    pub id: NexoraId,
    pub timestamp: u64,
    pub properties: HashMap<String, PropertyValue>,
    pub edges: Vec<EdgeSnapshot>,
}
```

#### 实现原理
```
1. Fragment 存储结构:
   /fragments/
     ├── 1000_2000_uuid1/
     │   └── nodes.jsonl      # 节点变更记录
     ├── 2000_3000_uuid2/
     │   └── nodes.jsonl
     └── ...

2. 查询流程:
   execute_time_travel(store, query) →
     a. 找到所有 <= as_of_us 的 Fragment
     b. 按时间顺序重放所有节点事件
     c. 构建指定时间点的节点快照
     d. 应用过滤条件并返回结果
```

#### 测试验证
```rust
// ✅ 测试通过
#[tokio::test]
async fn test_time_travel_basic() {
    // 节点在 1500us 时 speed=50
    // 节点在 2500us 时 speed=80 + 添加边
    
    // 查询 1600us → 应该看到 speed=50
    let result = execute_time_travel(&store, TimeTravelQuery::at(1600)).await;
    assert_eq!(node.properties.get("speed"), Some(&PropertyValue::Integer(50)));
    
    // 查询 2600us → 应该看到 speed=80 + 边
    let result = execute_time_travel(&store, TimeTravelQuery::at(2600)).await;
    assert_eq!(node.properties.get("speed"), Some(&PropertyValue::Integer(80)));
    assert_eq!(node.edges.len(), 1);
}
```

**评估**: ✅ Fragment层实现完整，逻辑正确

---

### 2. HTTP API 层面的问题

#### 当前端点实现

```rust
// crates/nexora-app/src/handlers.rs

pub async fn time_travel(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "active_nodes": state.graph.active_node_count().await,
        "time_travel": "enabled"  // ⚠️ 这只是一个状态消息!
    }))
}

// crates/nexora-app/src/main.rs
.route("/api/v2/graph/history", get(handlers::time_travel))
```

**问题**:
1. ❌ 没有接受 `as_of` / `node_id` / `timestamp` 参数
2. ❌ 没有调用 `nexora_fragment::execute_time_travel()`
3. ❌ 只返回一个占位符响应

#### 应该有但没有的端点

```rust
// 缺失的 API (应该实现但未实现)

// 1. 查询节点历史状态
GET /api/v2/graph/node/{node_id}/history?at={timestamp}
// 返回: NodeSnapshot (properties, edges, timestamp)

// 2. 查询节点变更历史
GET /api/v2/graph/node/{node_id}/events?from={ts}&to={ts}
// 返回: List<TimedEvent<NodeChangeEvent>>

// 3. 回滚节点到历史版本
POST /api/v2/graph/node/{node_id}/restore
Body: { "at": timestamp }
// 效果: 将节点恢复到指定时间点的状态

// 4. 恢复已删除节点
POST /api/v2/graph/node/{node_id}/undelete
// 效果: 移除 Tombstone，恢复节点

// 5. 全局时间旅行查询（Cypher）
POST /api/v2/query/cypher?at={timestamp}
Body: { "query": "MATCH (n:Person) RETURN n" }
// 效果: 在历史时间点执行 Cypher 查询
```

---

### 3. Journal → Fragment 管道缺失

#### 设计意图
```
活跃图变更 → NodeTask.journal → FragmentSealer → Fragment Store
                                                  → Tiered Store (S3)
```

#### 实际状态
```rust
// ✅ Journal 存在
// crates/nexora-core/src/graph/node_task.rs
struct NodeTask {
    journal: Vec<TimedEvent<NodeChangeEvent>>, // 累积事件
}

// ✅ FragmentSealer 实现完整
// crates/nexora-fragment/src/sealer.rs
impl FragmentSealer {
    pub async fn record(&self, event: SealEvent) { /* ... */ }
    pub async fn seal(&self, end_us: u64) -> Result<Option<FragmentId>, ...> {
        // 将当前窗口序列化为 Fragment
    }
}

// ❌ 但两者未连接！
// crates/nexora-app/src/main.rs 中没有:
// - 创建 FragmentSealer 实例
// - 注册 mutation callback 调用 sealer.record()
// - 启动定时任务调用 sealer.seal()
```

**后果**:
- Journal 事件只存在内存中，重启后丢失
- 无法进行时间旅行查询（因为没有 Fragment 数据）
- WAL 日志也只用于崩溃恢复，不会转换为 Fragment

---

### 4. 节点回滚功能缺失

#### 事件类型已定义
```rust
// crates/nexora-core/src/event.rs
pub enum NodeChangeEvent {
    NodeDeleted { tombstone: TombstoneRecord },  // ✅ 已实现
    NodeRestored,                                // ❌ 定义了但未实现
}

pub struct TombstoneRecord {
    pub deleted_at: EventTime,
    pub deleted_by: Option<String>,
    pub reason: Option<String>,
}
```

#### 缺失的实现
```rust
// ❌ 未找到以下函数:
// 1. 恢复已删除节点
async fn undelete_node(graph: &GraphService, node_id: NexoraId) 
    -> Result<(), GraphError>

// 2. 回滚节点到历史版本
async fn restore_node_to(
    graph: &GraphService, 
    node_id: NexoraId, 
    as_of: u64
) -> Result<(), GraphError>

// 3. 从 Fragment 重建节点
async fn rebuild_node_from_snapshot(
    node_task: &mut NodeTask,
    snapshot: NodeSnapshot
) -> Result<(), NodeError>
```

---

### 5. 分布式时间旅行的问题

#### 场景: 跨分片查询

```cypher
-- 查询历史时间点的关系
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b:Person)
WHERE timestamp = 2026-01-01T00:00:00Z
RETURN a, b
```

**问题**:
1. **时间戳不一致**
   - 不同机器的时钟可能不同步
   - 缺少全局时间戳服务 (如 HLC/TrueTime)
   
2. **分片碎片化**
   - Alice 在分片1，Bob 在分片2
   - 两个分片的 Fragment 时间窗口可能不对齐
   
3. **查询语义不明确**
   - "as of T" 是指每个分片各自的本地时间？
   - 还是全局一致的快照时间？

---

## 功能可用性矩阵

| 功能 | 存储层 | API层 | 测试 | 生产就绪 |
|-----|--------|-------|------|----------|
| **时间旅行查询** (单节点) | ✅ | ❌ | ✅ | ❌ |
| **时间旅行查询** (Cypher) | ✅ | ❌ | ❌ | ❌ |
| **节点历史事件查询** | ⚠️ | ❌ | ❌ | ❌ |
| **节点状态回滚** | ❌ | ❌ | ❌ | ❌ |
| **软删除 (Tombstone)** | ✅ | ⚠️ | ✅ | ⚠️ |
| **节点恢复 (Undelete)** | ❌ | ❌ | ❌ | ❌ |
| **Journal → Fragment 归档** | ⚠️ | ❌ | ❌ | ❌ |
| **分布式时间一致性** | ❌ | ❌ | ❌ | ❌ |
| **审计日志导出** | ⚠️ | ❌ | ❌ | ❌ |

**图例**:
- ✅ 完整实现
- ⚠️ 部分实现/未集成
- ❌ 未实现

---

## 对比：声称的功能 vs 实际能力

### README.md 中的声称

```markdown
| **Time Travel** | Query graph state at any historical point in time |
```

### 实际情况

#### ✅ 能做到的:
```rust
// 通过底层 API 查询 Fragment 历史
use nexora_fragment::{FragmentStore, TimeTravelQuery, execute_time_travel};

let store = FragmentStore::new("/path/to/fragments", "namespace");
let query = TimeTravelQuery::at(1609459200_000000) // 2021-01-01
    .for_node(node_id);
let result = execute_time_travel(&store, query).await?;

// 得到节点快照
for snapshot in result.nodes {
    println!("{:?}", snapshot.properties);
}
```

#### ❌ 不能做到的:
```bash
# HTTP API 查询（不支持）
curl "http://localhost:8080/api/v2/graph/node/abc123/history?at=2021-01-01T00:00:00Z"
# → 404 Not Found

# Cypher 时间旅行（不支持）
POST /api/v2/query/cypher?at=2021-01-01T00:00:00Z
{"query": "MATCH (n) RETURN n"}
# → 忽略 at 参数，查询当前状态

# 节点回滚（不支持）
POST /api/v2/graph/node/abc123/restore
{"at": "2021-01-01T00:00:00Z"}
# → 404 Not Found

# 节点恢复（不支持）
POST /api/v2/graph/node/deleted_node/undelete
# → 404 Not Found
```

---

## 根本原因分析

### 为什么功能只实现了一半？

1. **模块化设计但未集成**
   - `nexora-fragment` 是独立开发的模块
   - 从未与 `nexora-app` 主应用集成
   - 像是"功能原型"而非生产代码

2. **测试驱动但未端到端**
   - Fragment 层有完整单元测试
   - 但缺少集成测试和 API 测试
   - 没有验证用户实际能用上这些功能

3. **文档过度承诺**
   - README 列出了"时间旅行"作为核心特性
   - 但实际只是底层基础设施就位
   - 对用户而言功能**不可用**

---

## 要实现完整时间旅行的待办事项

### P0: 基本时间旅行查询 (预计 2-3 天)

```rust
// 1. 实现 HTTP API 端点
// crates/nexora-app/src/handlers.rs

#[derive(Deserialize)]
pub struct TimeTravelParams {
    node_id: String,
    at: String, // ISO 8601 timestamp
}

pub async fn get_node_history(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Query(params): Query<TimeTravelParams>,
) -> Result<Json<NodeSnapshot>, StatusCode> {
    let qid = NexoraId::from_hex(&node_id)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    let timestamp = parse_timestamp(&params.at)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    
    let query = TimeTravelQuery::at(timestamp).for_node(qid);
    let result = execute_time_travel(&state.fragment_store, query)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    result.find_node(&qid)
        .ok_or(StatusCode::NOT_FOUND)
        .map(|snapshot| Json(snapshot.clone()))
}

// 2. 注册路由
// crates/nexora-app/src/main.rs
.route("/api/v2/graph/node/:node_id/history", 
       get(handlers::get_node_history))
```

### P1: Journal → Fragment 自动归档 (预计 3-4 天)

```rust
// 1. 在 AppState 中添加 FragmentSealer
pub struct AppState {
    pub graph: Arc<GraphService>,
    pub fragment_sealer: Arc<FragmentSealer>, // 新增
    // ...
}

// 2. 注册 mutation callback
// crates/nexora-app/src/main.rs
let sealer = Arc::new(FragmentSealer::new(
    fragment_store.clone(),
    "default",
    EventTime::now().as_micros()
));

graph.register_mutation_callback(move |event| {
    let sealer = sealer.clone();
    tokio::spawn(async move {
        match event {
            NodeChangeEvent::PropertySet { key, value, .. } => {
                sealer.record(SealEvent::PropertySet { ... }).await;
            }
            NodeChangeEvent::EdgeAdded { edge, .. } => {
                sealer.record(SealEvent::EdgeAdded { ... }).await;
            }
            _ => {}
        }
    });
});

// 3. 启动定时 seal 任务
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5分钟
    loop {
        interval.tick().await;
        let now = EventTime::now().as_micros();
        if let Ok(Some(frag_id)) = sealer.seal(now).await {
            tracing::info!("Sealed fragment: {:?}", frag_id);
        }
    }
});
```

### P2: 节点回滚功能 (预计 4-5 天)

```rust
// 1. 实现回滚逻辑
// crates/nexora-core/src/graph/mod.rs

impl GraphService {
    pub async fn restore_node_to(
        &self,
        node_id: NexoraId,
        as_of: u64,
    ) -> Result<(), GraphError> {
        // a. 从 Fragment 查询历史快照
        let query = TimeTravelQuery::at(as_of).for_node(node_id.clone());
        let result = execute_time_travel(&self.fragment_store, query).await?;
        let snapshot = result.find_node(&node_id)
            .ok_or(GraphError::NodeNotFound)?;
        
        // b. 构建回滚操作
        let ops = vec![
            // 清空当前属性
            MutationOp::ClearAllProperties,
            // 恢复历史属性
            ...snapshot.properties.iter().map(|(k, v)| {
                MutationOp::SetProperty { 
                    key: Symbol::from(k.as_str()), 
                    value: v.clone() 
                }
            }),
            // 恢复历史边
            ...snapshot.edges.iter().map(|e| {
                MutationOp::AddEdge { 
                    edge: HalfEdge::new(...) 
                }
            }),
        ];
        
        // c. 提交回滚事务
        self.write_batch(&node_id, ops, WriteBatchOptions::default()).await?;
        
        Ok(())
    }
    
    pub async fn undelete_node(&self, node_id: NexoraId) -> Result<(), GraphError> {
        // 发送 NodeRestored 事件，移除 Tombstone
        let op = MutationOp::RestoreNode;
        self.write_batch(&node_id, vec![op], WriteBatchOptions::default()).await
    }
}

// 2. 实现 API 端点
pub async fn restore_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(params): Json<RestoreParams>,
) -> Result<StatusCode, StatusCode> {
    let qid = NexoraId::from_hex(&node_id)?;
    let timestamp = parse_timestamp(&params.at)?;
    
    state.graph.restore_node_to(qid, timestamp).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    Ok(StatusCode::OK)
}
```

### P3: Cypher 时间旅行支持 (预计 5-7 天)

```rust
// 1. 扩展 Cypher 执行器
// crates/nexora-cypher/src/executor.rs

pub struct CypherExecutor {
    graph: Arc<GraphService>,
    fragment_store: Option<Arc<FragmentStore>>, // 新增
    as_of: Option<u64>, // 新增: 时间旅行时间点
}

impl CypherExecutor {
    pub fn with_time_travel(mut self, as_of: u64) -> Self {
        self.as_of = Some(as_of);
        self
    }
    
    async fn resolve_node(&self, pattern: &NodePattern) -> Result<Vec<NodeSnapshot>> {
        if let Some(ts) = self.as_of {
            // 时间旅行模式: 从 Fragment 查询
            let query = TimeTravelQuery::at(ts);
            execute_time_travel(self.fragment_store.as_ref().unwrap(), query).await
        } else {
            // 正常模式: 从活跃图查询
            self.graph.scan_nodes(pattern).await
        }
    }
}

// 2. API 支持 at 参数
POST /api/v2/query/cypher?at=2021-01-01T00:00:00Z
```

### P4: 分布式时间一致性 (预计 2-3 周)

```rust
// 选项 A: Hybrid Logical Clock (HLC)
// 每个节点维护 (physical_time, logical_counter)
// 优势: 无需中央授时服务

// 选项 B: 中央时间戳服务
// 类似 Google Spanner 的 TrueTime
// 每次事务从 TSO 获取全局递增时间戳

// 选项 C: 快照隔离 + 版本向量
// 每个 Fragment 携带因果依赖信息
// 查询时重建一致性快照
```

---

## 推荐优先级

### 立即修复 (P0)
1. **修正 README 文档**
   - 将 "Time Travel" 标记为 "⚠️ Experimental / API Not Available"
   - 或者删除该声称，直到实现完整

2. **实现基本 HTTP API**
   - GET /api/v2/graph/node/{id}/history?at={timestamp}
   - 至少让用户能查询单节点历史

### 短期目标 (1-2周)
3. **连接 Journal → Fragment 管道**
   - 让活跃图事件自动归档
   - 启用真正的时间旅行查询

4. **实现节点回滚功能**
   - POST /api/v2/graph/node/{id}/restore
   - POST /api/v2/graph/node/{id}/undelete

### 中期目标 (1-2月)
5. **Cypher 时间旅行支持**
   - 支持 `?at=<timestamp>` 查询参数
   - 在历史快照上执行复杂查询

6. **分布式时间一致性**
   - 实现 HLC 或 TSO
   - 保证跨分片查询的一致性语义

---

## 当前可用的替代方案

如果用户**现在**想要审计/回溯功能，可以：

### 方案 1: 手动导出 Journal
```rust
// 通过 NodeCommand::DrainJournal 获取事件
let (reply_tx, reply_rx) = oneshot::channel();
node_tx.send(NodeCommand::DrainJournal { reply: reply_tx }).await?;
let snapshot = reply_rx.await?;

// 将 journal 持久化到外部存储
for event in snapshot.journal {
    audit_log.write(event);
}
```

### 方案 2: 利用 WAL 回放
```rust
// WAL 包含所有变更，可以手动回放
let wal = WriteAheadLog::open(wal_dir)?;
for record in wal.scan_from(start_seq) {
    // 根据时间戳过滤
    if record.timestamp <= target_time {
        apply_to_graph(record);
    }
}
```

### 方案 3: 定期快照 + Delta
```rust
// 应用层实现定期快照
tokio::spawn(async {
    let mut interval = tokio::time::interval(Duration::from_hours(1));
    loop {
        interval.tick().await;
        graph.create_snapshot("/backups/snapshot_{timestamp}").await;
    }
});
```

---

## 结论与建议

### 总体评价

Nexora 的时间旅行功能是一个**半成品**:
- ✅ 底层基础设施设计优秀（Fragment、Event Sourcing）
- ⚠️ 但缺少用户可访问的接口
- ❌ 文档承诺与实际能力严重不符

### 具体建议

**给开发团队**:
1. **诚实更新文档** — 标记功能为"开发中"
2. **优先实现 P0 API** — 让功能真正可用
3. **添加端到端测试** — 验证用户场景
4. **考虑分布式语义** — 设计全局一致性方案

**给用户**:
- ⚠️ **不要依赖时间旅行功能**用于生产审计
- 如需审计日志，使用外部系统（如 ELK）
- 可以使用底层 API 进行离线分析，但需自行编码

**给技术决策者**:
- 这不是"简单集成"问题，需要**架构级别的补完**
- 预计需要 **1-2个月全职开发**才能达到生产级
- 分布式时间旅行是**学术级难题**，谨慎承诺

---

## 附录：时间旅行测试用例

### 当前通过的测试

```rust
// ✅ nexora-fragment/src/time_travel.rs
test time_travel::tests::test_time_travel_basic ... ok
test time_travel::tests::test_time_travel_for_specific_node ... ok
test time_travel::tests::test_consolidate ... ok
test tiered_store::tests::fragment_sinks_to_s3_warm_and_time_travels_back ... ok
```

### 缺失的测试

```rust
// ❌ 应该有但没有的测试

#[tokio::test]
async fn test_http_api_node_history() {
    // 通过 HTTP API 查询节点历史
}

#[tokio::test]
async fn test_cypher_time_travel() {
    // Cypher 查询在历史时间点
}

#[tokio::test]
async fn test_node_restore() {
    // 将节点回滚到历史版本
}

#[tokio::test]
async fn test_undelete_node() {
    // 恢复已删除节点
}

#[tokio::test]
async fn test_cross_shard_time_travel() {
    // 跨分片的一致性时间旅行查询
}

#[tokio::test]
async fn test_journal_to_fragment_pipeline() {
    // Journal 自动归档到 Fragment
}
```

---

**报告结束** | 审查员: Claude (Fable 5) | 2026-07-11
