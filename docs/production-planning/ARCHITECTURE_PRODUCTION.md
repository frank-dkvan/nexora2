# Nexora 生产级架构设计

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 设计中

---

## 1. 系统定位

Nexora 是一个**通用实时对象图引擎**（Universal Real-Time Object Graph Engine），专注于：

- ✅ 实时对象关系建模
- ✅ 动态图状态管理
- ✅ 事件驱动的图变更
- ✅ Standing Query（持续查询）
- ✅ 图模式匹配与影响传播
- ✅ AI Agent 图上下文管理

### 1.1 不是什么

- ❌ 不是核心交易主库（不替代 PostgreSQL/MySQL）
- ❌ 不是消息队列（不替代 Kafka/Zenoh）
- ❌ 不是时序数据库（不替代 ReductStore）
- ❌ 不是对象存储（不替代 MinIO/S3）
- ❌ 不是流处理引擎（不替代 Flink/RisingWave）
- ❌ 不是完整 Neo4j 替代品（专注实时性而非 ACID 复杂查询）

### 1.2 是什么

Nexora 位于业务系统和基础设施之间的**关系层**：

```
┌─────────────────────────────────────────────────────────────┐
│                    业务应用层                                │
│  (智慧货站 / 智能制造 / IT运维 / 能源电力 / 供应链 ...)      │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Nexora 实时对象图引擎                       │
│  • 对象关系建模           • Standing Query                   │
│  • 图状态管理             • 影响传播分析                      │
│  • 事件溯源               • AI Agent 上下文                   │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌──────────────┬───────────────┬──────────────┬──────────────┐
│ PostgreSQL   │  Kafka/Zenoh  │ ReductStore  │ OpenSearch   │
│ (核心数据)   │  (事件流)      │ (时序证据)   │ (日志/搜索)  │
└──────────────┴───────────────┴──────────────┴──────────────┘
```

---

## 2. 核心架构

### 2.1 Actor-per-Node 模型

每个节点是一个独立的 Tokio 异步任务（Actor），具有：

- **独立状态** — 属性、标签、边、事件日志
- **消息驱动** — 通过 Channel 接收操作请求
- **并发隔离** — 无全局锁，节点间互不阻塞
- **事件溯源** — 所有变更记录到 WAL

```rust
pub struct NodeTask {
    pub id: NexoraId,
    labels: HashSet<Symbol>,                        // 标签集合
    properties: BTreeMap<Symbol, PropertyValue>,    // 属性
    edges: HashSet<HalfEdge>,                       // 出边
    journal: Vec<TimedEvent<NodeChangeEvent>>,      // 事件日志
    wal: Option<SharedWal>,                         // WAL 引用
}
```

**优势：**
- 天然支持高并发（节点级别并行）
- 易于分片（节点 ID → Shard 映射）
- 故障隔离（单节点崩溃不影响其他）

**挑战：**
- 内存占用（大量 Actor）
- LRU 驱逐策略（冷节点持久化）

---

### 2.2 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                      API 层                                  │
│  HTTP REST API | PG Wire | gRPC | WebSocket                 │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    查询执行层                                │
│  Cypher Executor | Query Planner | Query Optimizer          │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   图服务层 (GraphService)                    │
│  • Node/Edge CRUD        • Standing Query Manager           │
│  • 索引管理              • Materialized View Manager         │
│  • 事件分发              • Fixpoint 传递闭包                 │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  持久化层 (Persistor)                        │
│  WAL (Write-Ahead Log) | RocksDB | 分层存储                 │
└─────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   集成层 (Ingest/Output)                     │
│  Kafka | Zenoh | MQTT | HTTP Webhook | CDC                  │
└─────────────────────────────────────────────────────────────┘
```

---

### 2.3 核心组件

#### GraphService
- 全局图管理器
- 节点 Actor 生命周期管理
- 索引维护（Label Index、Property Index、Edge Index）
- Standing Query 订阅

#### GraphShard
- 节点分片管理（按 Node ID 哈希）
- LRU 驱逐策略（冷节点持久化）
- 分布式路由（Zenoh）

#### WAL (Write-Ahead Log)
- 所有 mutation 先写 WAL，再更新内存
- 支持加密（AES-256-GCM）
- 崩溃恢复保证

#### RocksDB Persistor
- 8 个 Column Families：
  - `nodes` — 节点状态快照
  - `edges` — 边数据
  - `properties` — 属性索引
  - `labels` — 标签索引
  - `standing-queries` — Standing Query 规则
  - `standing-query-states` — SQ 匹配状态
  - `materialized-views` — MV 元数据
  - `evidence-refs` — 证据引用

#### Standing Query Manager
- 持续查询引擎（类似 DataLog/Differential Dataflow）
- 增量图模式匹配
- 触发器：PropertySet/EdgeAdded/LabelAdded/NodeDeleted
- 结果持久化与推送

#### Materialized View Manager
- 预计算视图管理
- 全量构建 + 增量刷新
- 查询重写（Query Rewriter）

#### Fixpoint Engine
- 增量传递闭包计算
- 基于 Semi-Naive Evaluation
- 与 Standing Query 集成

---

## 3. 数据模型

### 3.1 核心抽象

```rust
// 节点记录
pub struct NodeRecord {
    pub id: NexoraId,
    pub labels: HashSet<Symbol>,              // 一等公民标签
    pub properties: BTreeMap<Symbol, PropertyValue>,
    pub created_at: EventTime,
    pub updated_at: EventTime,
    pub version: u64,
    pub namespace: Option<Symbol>,            // 命名空间
    pub tenant_id: Option<Symbol>,            // 租户 ID
    pub tombstone: Option<TombstoneRecord>,   // 删除标记
}

// 边记录
pub struct EdgeRecord {
    pub src: NexoraId,
    pub edge_type: Symbol,
    pub dst: NexoraId,
    pub properties: BTreeMap<Symbol, PropertyValue>, // 边属性
    pub created_at: EventTime,
    pub version: u64,
}

// 图变更事件
pub enum GraphMutation {
    NodeCreated { id: NexoraId, labels: Vec<Symbol>, namespace: Option<Symbol> },
    PropertySet { node: NexoraId, key: Symbol, value: PropertyValue, prev: Option<PropertyValue> },
    LabelAdded { node: NexoraId, label: Symbol },
    LabelRemoved { node: NexoraId, label: Symbol },
    EdgeAdded { src: NexoraId, edge_type: Symbol, dst: NexoraId },
    EdgeRemoved { src: NexoraId, edge_type: Symbol, dst: NexoraId },
    EdgePropertySet { src: NexoraId, edge_type: Symbol, dst: NexoraId, key: Symbol, value: PropertyValue },
    NodeDeleted { id: NexoraId, tombstone: TombstoneRecord },
}
```

### 3.2 属性类型系统

```rust
pub enum PropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<PropertyValue>),              // 嵌套列表
    Map(BTreeMap<String, PropertyValue>),  // 嵌套映射
    Date(NaiveDate),
    DateTime(DateTime<Utc>),
    Duration(ChronoDuration),
}
```

### 3.3 证据引用（EvidenceRef）

Nexora **不保存大对象原文**（图片/视频/日志/报文），只保存引用：

```rust
pub struct EvidenceRef {
    pub evidence_id: String,
    pub store_type: EvidenceStoreType,     // ReductStore | S3 | Loki | OpenSearch
    pub bucket: String,
    pub entry: String,
    pub timestamp_start: Option<DateTime<Utc>>,
    pub timestamp_end: Option<DateTime<Utc>>,
    pub labels: HashMap<String, String>,
    pub uri: Option<String>,
    pub checksum: Option<String>,
    pub source_system: Option<String>,
    pub correlation_id: Option<String>,
    pub domain: Option<String>,
}
```

**用法示例：**
```cypher
MATCH (e:Exception {id: 'EVT001'})-[:HAS_EVIDENCE]->(ref:EvidenceRef)
RETURN ref.store_type, ref.uri
```

---

## 4. 一致性与持久化

### 4.1 WAL 写入流程

```
1. 客户端请求 → GraphService
2. GraphService → WAL.append(mutation)  [同步写入]
3. WAL 返回 CommitReceipt
4. GraphService → NodeTask.apply(mutation) [异步更新内存]
5. NodeTask 定期 flush → RocksDB [异步持久化]
```

**保证：**
- WAL 写入成功 = 数据已持久化
- 崩溃后可从 WAL 完整恢复

### 4.2 崩溃恢复

```
1. 启动时读取 WAL 文件
2. 按顺序 replay 所有 mutation
3. 重建内存图状态
4. 恢复索引
5. 恢复 Standing Query 订阅
```

**测试覆盖：**
- WAL 半写入（CRC 校验）
- 进程 SIGKILL
- 磁盘满
- 乱序恢复

---

## 5. 查询执行

### 5.1 Cypher 查询路径

```
1. 解析 Cypher → AST
2. 查询规划 → 逻辑计划
3. 查询优化 → 物理计划
   - 索引选择（Label Index / Property Index）
   - Join 顺序优化
   - Limit 下推
4. 执行引擎 → 结果集
   - Lazy Node Loading（避免全图快照）
   - 流式返回
```

**当前限制：**
- MAX_SNAPSHOT_NODES = 100,000（P0.2 任务将移除）

### 5.2 Standing Query 执行

```
1. 用户注册 SQ：
   REGISTER STANDING QUERY impact_analysis AS
   MATCH (e:Exception)-[:AFFECTS]->(obj:Object)-[:DEPENDS_ON*1..3]->(downstream)
   WHERE e.severity IN ['P1','P2']
   RETURN e, obj, downstream

2. GraphMutation 触发：
   - PropertySet → 重新评估受影响节点
   - EdgeAdded → 重新评估涉及边的 SQ
   - LabelAdded → 重新评估 label 匹配规则

3. 增量计算：
   - 只重算受影响的局部子图
   - Fixpoint 引擎计算传递闭包

4. 结果推送：
   - WebSocket 推送
   - Kafka 输出
   - HTTP Webhook
```

---

## 6. 扩展性

### 6.1 水平扩展（分片）

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Shard 0     │    │  Shard 1     │    │  Shard 2     │
│  Node 0-999  │    │  Node 1k-1999│    │  Node 2k-2999│
└──────────────┘    └──────────────┘    └──────────────┘
       ▲                   ▲                   ▲
       └───────────────────┴───────────────────┘
                   Zenoh 分布式路由
```

**分片策略：**
- 节点 ID 哈希 → Shard 映射
- 边跨 Shard：源节点 Shard 负责维护
- Standing Query 订阅广播到所有 Shard

### 6.2 垂直扩展（分层存储）

```
Hot Tier (RocksDB)  ← 活跃节点 (最近访问)
    ↓ 降级（30 天未访问）
Warm Tier (Parquet) ← 半活跃节点
    ↓ 降级（180 天未访问）
Cold Tier (S3)      ← 冷数据归档
```

**P2.1 任务实现**

---

## 7. 多租户与隔离

### 7.1 Namespace 隔离

```cypher
CREATE (:Device {id: 'AGV001', namespace: 'airport_cargo'})
CREATE (:Device {id: 'AGV001', namespace: 'manufacturing'})
// 两个节点可以有相同 ID，但不同 namespace
```

### 7.2 Tenant 权限

```rust
pub struct QueryContext {
    pub tenant_id: Option<Symbol>,
    pub allowed_namespaces: Vec<Symbol>,
    pub allowed_labels: Vec<Symbol>,
    pub masked_properties: Vec<Symbol>,  // 脱敏属性
}
```

**RBAC 集成（P4.2）：**
- 用户 → 角色 → 权限
- 权限粒度：Namespace / Label / Property
- 审计日志记录所有访问

---

## 8. 可观测性

### 8.1 Metrics (Prometheus)

```
# 图规模
nexora_nodes_total{namespace="airport_cargo"}
nexora_edges_total{edge_type="DEPENDS_ON"}

# 性能
nexora_query_duration_seconds{percentile="p95"}
nexora_mutation_throughput_ops

# Standing Query
nexora_standing_query_evaluations_total
nexora_standing_query_hits_total

# 资源
nexora_actor_count
nexora_memory_usage_bytes
```

### 8.2 Tracing (OpenTelemetry)

每个请求携带：
- `trace_id` — 分布式追踪 ID
- `correlation_id` — 业务关联 ID
- `mutation_id` — 写入幂等 ID

**示例：**
```
Span: POST /api/v1/graph/nodes
  ├─ Span: WAL.append
  ├─ Span: NodeTask.apply
  └─ Span: StandingQueryManager.evaluate
```

### 8.3 Health Checks

- `GET /health` — 健康检查
- `GET /ready` — 就绪检查（WAL replay 完成）
- `GET /live` — 存活检查（Actor 系统响应）
- `GET /metrics` — Prometheus metrics

---

## 9. 部署架构

### 9.1 单节点部署（开发/小规模）

```yaml
version: '3.8'
services:
  nexora:
    image: nexora:latest
    ports:
      - "8080:8080"  # HTTP API
      - "5432:5432"  # PG Wire
    volumes:
      - ./data:/data
    environment:
      - NEXORA_WAL_DIR=/data/wal
      - NEXORA_ROCKSDB_PATH=/data/rocksdb
```

### 9.2 集群部署（生产）

```
┌─────────────────────────────────────────────────────────────┐
│                     Load Balancer                            │
│                    (Nginx / HAProxy)                         │
└─────────────────────────────────────────────────────────────┘
           │                   │                   │
   ┌───────┴───────┐   ┌───────┴───────┐   ┌───────┴───────┐
   │  Nexora Pod 1 │   │  Nexora Pod 2 │   │  Nexora Pod 3 │
   │  Shard 0-99   │   │  Shard 100-199│   │  Shard 200-299│
   └───────────────┘   └───────────────┘   └───────────────┘
           │                   │                   │
           └───────────────────┴───────────────────┘
                        Zenoh Mesh Network
```

**Kubernetes 部署：**
- StatefulSet（持久化 WAL/RocksDB）
- Headless Service（Zenoh 对等发现）
- PVC（持久化卷）

---

## 10. 安全

### 10.1 认证（P4.1）

- OIDC / Keycloak 集成
- JWT Token 验证
- mTLS 客户端证书

### 10.2 授权（P4.2）

- RBAC（基于角色的访问控制）
- 细粒度权限：Namespace / Label / Property
- 敏感属性脱敏

### 10.3 审计（P4.3）

所有操作记录审计日志：
```json
{
  "timestamp": "2026-07-05T10:30:00Z",
  "user": "alice@example.com",
  "action": "QUERY",
  "resource": "namespace:airport_cargo",
  "query": "MATCH (n:Device) RETURN n LIMIT 10",
  "result": "SUCCESS",
  "latency_ms": 45
}
```

---

## 11. 未来演进

### Phase 1: 单节点生产化（当前）
- P0 任务：图模型、查询优化、MV 填充
- P1 任务：Standing Query 完善

### Phase 2: 分布式增强（3-6 个月）
- Raft 共识完整实现
- 跨 Shard 事务
- 分布式 Standing Query

### Phase 3: 高级功能（6-12 个月）
- 时间旅行查询（Historical Query）
- 图神经网络集成（GNN）
- 自动 Schema 推断
- 多模型融合（图 + 向量 + 全文）

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05  
**状态:** 设计文档，随实现更新
