# Nexora 图数据模型

**版本:** 2.0 (生产级)  
**日期:** 2026/07/05  
**状态:** 设计中（P0.1 实现中）

---

## 1. 核心概念

Nexora 图模型包含以下一等公民：

- **Node（节点）** — 对象实体
- **Edge（边）** — 对象关系
- **Label（标签）** — 节点类型/分类
- **Property（属性）** — 节点/边的键值对
- **Event（事件）** — 图变更的原子操作
- **Namespace（命名空间）** — 逻辑隔离
- **Tenant（租户）** — 多租户隔离

---

## 2. Node（节点）

### 2.1 数据结构

```rust
pub struct NodeRecord {
    /// 全局唯一 ID（128-bit UUID）
    pub id: NexoraId,
    
    /// 标签集合（一等公民，非 synthetic property）
    pub labels: HashSet<Symbol>,
    
    /// 属性映射
    pub properties: BTreeMap<Symbol, PropertyValue>,
    
    /// 创建时间（微秒精度）
    pub created_at: EventTime,
    
    /// 最后更新时间
    pub updated_at: EventTime,
    
    /// 版本号（乐观锁）
    pub version: u64,
    
    /// 命名空间（可选，用于逻辑隔离）
    pub namespace: Option<Symbol>,
    
    /// 租户 ID（可选，用于多租户）
    pub tenant_id: Option<Symbol>,
    
    /// 删除标记（软删除）
    pub tombstone: Option<TombstoneRecord>,
}
```

### 2.2 Label（标签）

**当前实现（待迁移）：**
- ❌ 使用 synthetic property `__labels: List<String>` 模拟
- ❌ Label 变化不触发独立事件

**目标实现（P0.1）：**
- ✅ `labels: HashSet<Symbol>` 一等公民字段
- ✅ 触发 `GraphMutation::LabelAdded` / `LabelRemoved` 事件
- ✅ 独立的 Label Index 索引

**Cypher 示例：**
```cypher
// 创建节点带标签
CREATE (n:Device:Asset {id: 'AGV001', status: 'RUNNING'})

// 添加标签
MATCH (n {id: 'AGV001'})
SET n:Emergency

// 删除标签
MATCH (n {id: 'AGV001'})
REMOVE n:Emergency

// 查询多标签
MATCH (n:Device:Emergency)
RETURN n
```

### 2.3 Property（属性）

支持丰富的类型系统：

```rust
pub enum PropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    
    // 复合类型
    List(Vec<PropertyValue>),              // 嵌套列表
    Map(BTreeMap<String, PropertyValue>),  // 嵌套映射
    
    // 时间类型
    Date(NaiveDate),                       // 日期
    DateTime(DateTime<Utc>),               // 时间戳
    Duration(ChronoDuration),              // 时长
}
```

**示例数据：**
```json
{
  "id": "AGV001",
  "status": "RUNNING",
  "battery_level": 85.5,
  "location": {
    "x": 120.5,
    "y": 340.2,
    "zone": "A1"
  },
  "sensors": [
    {"type": "temperature", "value": 45.2},
    {"type": "vibration", "value": 0.03}
  ],
  "last_maintenance": "2026-06-15T10:30:00Z"
}
```

### 2.4 Tombstone（删除标记）

```rust
pub struct TombstoneRecord {
    /// 删除时间
    pub deleted_at: EventTime,
    
    /// 删除操作者
    pub deleted_by: Option<String>,
    
    /// 删除原因
    pub reason: Option<String>,
}
```

**删除语义：**
- **软删除（默认）** — 设置 tombstone，查询自动过滤
- **硬删除（定期清理）** — `GraphService::cleanup_tombstones()` 物理删除

**Cypher 示例：**
```cypher
// 软删除
DELETE n

// 查询自动过滤已删除节点
MATCH (n:Device)
RETURN n  // 不返回已删除节点

// 查询包含已删除节点（管理员模式）
MATCH (n:Device)
WHERE n.__deleted = true
RETURN n
```

---

## 3. Edge（边）

### 3.1 数据结构

```rust
pub struct EdgeRecord {
    /// 源节点 ID
    pub src: NexoraId,
    
    /// 边类型（关系类型）
    pub edge_type: Symbol,
    
    /// 目标节点 ID
    pub dst: NexoraId,
    
    /// 边属性（一等公民，非 synthetic property）
    pub properties: BTreeMap<Symbol, PropertyValue>,
    
    /// 创建时间
    pub created_at: EventTime,
    
    /// 版本号
    pub version: u64,
}
```

### 3.2 Half-Edge 实现

Nexora 使用**半边（Half-Edge）**模型：

```rust
pub struct HalfEdge {
    pub edge_type: Symbol,
    pub target: NexoraId,
    pub properties: BTreeMap<Symbol, PropertyValue>,
}
```

**存储策略：**
- 源节点存储出边（`edges: HashSet<HalfEdge>`）
- 目标节点维护入边索引（反向查询）

**Cypher 示例：**
```cypher
// 创建边
MATCH (a:Device {id: 'AGV001'}), (b:Task {id: 'TASK001'})
CREATE (a)-[:EXECUTING {started_at: '2026-07-05T10:00:00Z', priority: 1}]->(b)

// 查询出边
MATCH (a:Device {id: 'AGV001'})-[r:EXECUTING]->(b)
RETURN b, r.started_at

// 查询入边
MATCH (a)-[r:EXECUTING]->(b:Task {id: 'TASK001'})
RETURN a, r.priority

// 更新边属性
MATCH (a)-[r:EXECUTING]->(b {id: 'TASK001'})
SET r.progress = 50
```

### 3.3 EdgeProperty（边属性）

**当前实现（待迁移）：**
- ❌ 使用 synthetic property `__rel_<type>_<target>_<prop>` 模拟
- ❌ 查询性能低（需要属性扫描）

**目标实现（P0.1）：**
- ✅ `properties: BTreeMap<Symbol, PropertyValue>` 直接存储
- ✅ 触发 `GraphMutation::EdgePropertySet` 事件
- ✅ Edge Index 支持属性查询

**迁移兼容层：**
```rust
// 读取时同时检查新旧存储
if let Some(edge) = node.edges.find(edge_type, target) {
    // 新存储：直接返回 edge.properties
    edge.properties.get(key)
} else {
    // 旧存储：回退到 synthetic property
    let synthetic_key = format!("__rel__{}_{}", edge_type, target, key);
    node.properties.get(&synthetic_key)
}
```

---

## 4. GraphMutation（图变更事件）

### 4.1 事件类型

```rust
pub enum GraphMutation {
    // 节点操作
    NodeCreated {
        id: NexoraId,
        labels: Vec<Symbol>,
        namespace: Option<Symbol>,
    },
    
    PropertySet {
        node: NexoraId,
        key: Symbol,
        value: PropertyValue,
        prev: Option<PropertyValue>,  // 用于回滚
    },
    
    PropertyRemoved {
        node: NexoraId,
        key: Symbol,
        prev: PropertyValue,
    },
    
    // 标签操作（NEW in P0.1）
    LabelAdded {
        node: NexoraId,
        label: Symbol,
    },
    
    LabelRemoved {
        node: NexoraId,
        label: Symbol,
    },
    
    // 边操作
    EdgeAdded {
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
    },
    
    EdgeRemoved {
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
    },
    
    // 边属性操作（NEW in P0.1）
    EdgePropertySet {
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
        key: Symbol,
        value: PropertyValue,
    },
    
    EdgePropertyRemoved {
        src: NexoraId,
        edge_type: Symbol,
        dst: NexoraId,
        key: Symbol,
    },
    
    // 节点删除（NEW in P0.1）
    NodeDeleted {
        id: NexoraId,
        tombstone: TombstoneRecord,
    },
    
    NodeRestored {
        id: NexoraId,
    },
    
    // 批量操作
    BatchMutationApplied {
        mutations: Vec<GraphMutation>,
        request_id: Option<String>,  // 幂等性
    },
}
```

### 4.2 事件溯源

所有 mutation 记录到 WAL 和 NodeTask journal：

```rust
pub struct TimedEvent<T> {
    pub event: T,
    pub timestamp: EventTime,  // 微秒精度
    pub correlation_id: Option<String>,
    pub source_system: Option<String>,
}
```

**用途：**
- 崩溃恢复（WAL replay）
- 时间旅行查询（Historical Query）
- 审计日志
- Standing Query 触发

---

## 5. 索引系统

### 5.1 Label Index

```rust
pub struct LabelIndex {
    /// label -> Set<NodeId>
    index: BTreeMap<Symbol, HashSet<NexoraId>>,
}
```

**支持查询：**
```cypher
MATCH (n:Device)  // O(1) 查找 Label Index
RETURN count(n)
```

### 5.2 Property Index

```rust
pub struct PropertyIndex {
    /// (key, value) -> Set<NodeId>
    index: BTreeMap<(Symbol, PropertyValue), HashSet<NexoraId>>,
}
```

**支持查询：**
```cypher
MATCH (n {status: 'RUNNING'})  // O(log n) 查找 Property Index
RETURN n
```

### 5.3 Edge Index

```rust
pub struct EdgeIndex {
    /// (edge_type, src) -> Set<dst>
    outgoing: BTreeMap<(Symbol, NexoraId), HashSet<NexoraId>>,
    
    /// (edge_type, dst) -> Set<src>
    incoming: BTreeMap<(Symbol, NexoraId), HashSet<NexoraId>>,
}
```

**支持查询：**
```cypher
MATCH (a)-[:EXECUTING]->(b)  // O(1) 查找 Edge Index
WHERE a.id = 'AGV001'
RETURN b
```

---

## 6. Namespace & Tenant 隔离

### 6.1 Namespace 使用场景

```cypher
// 多环境隔离
CREATE (:Device {id: 'AGV001', namespace: 'production'})
CREATE (:Device {id: 'AGV001', namespace: 'staging'})

// 多领域隔离
CREATE (:Asset {id: 'A001', namespace: 'airport_cargo'})
CREATE (:Asset {id: 'A001', namespace: 'manufacturing'})
```

### 6.2 Tenant 权限控制

```rust
pub struct QueryContext {
    pub tenant_id: Option<Symbol>,
    pub allowed_namespaces: Vec<Symbol>,
    pub allowed_labels: Vec<Symbol>,
    pub masked_properties: Vec<Symbol>,  // 脱敏属性
}
```

**示例：**
```rust
// 租户 A 只能访问 airport_cargo namespace
let ctx = QueryContext {
    tenant_id: Some("tenant_a".into()),
    allowed_namespaces: vec!["airport_cargo".into()],
    allowed_labels: vec!["Device".into(), "Task".into()],
    masked_properties: vec!["password".into(), "api_key".into()],
};
```

---

## 7. 图模式示例

### 7.1 通用对象依赖图

```cypher
// 节点
CREATE (:Object {id: 'OBJ_ROOT', type: 'GenericAsset', status: 'RUNNING'})
CREATE (:Object {id: 'OBJ_DOWNSTREAM_1', type: 'DependentAsset', status: 'RUNNING'})
CREATE (:Object {id: 'OBJ_DOWNSTREAM_2', type: 'DependentAsset', status: 'RUNNING'})
CREATE (:Exception {id: 'EVT001', severity: 'P1', type: 'FAILURE'})

// 关系
MATCH (root:Object {id: 'OBJ_ROOT'}), (d1:Object {id: 'OBJ_DOWNSTREAM_1'})
CREATE (root)-[:DEPENDS_ON]->(d1)

MATCH (d1:Object {id: 'OBJ_DOWNSTREAM_1'}), (d2:Object {id: 'OBJ_DOWNSTREAM_2'})
CREATE (d1)-[:DEPENDS_ON]->(d2)

MATCH (e:Exception {id: 'EVT001'}), (root:Object {id: 'OBJ_ROOT'})
CREATE (e)-[:AFFECTS]->(root)
```

### 7.2 航空货站场景（Domain Package）

```cypher
// 节点
CREATE (:Device {id: 'AGV001', type: 'AGV', status: 'RUNNING', namespace: 'airport_cargo'})
CREATE (:Task {id: 'TASK001', status: 'IN_PROGRESS', namespace: 'airport_cargo'})
CREATE (:Piece {id: 'PIECE001', awb: '123-45678901', namespace: 'airport_cargo'})
CREATE (:ULD {id: 'AKE12345', namespace: 'airport_cargo'})
CREATE (:Flight {id: 'LH729', cutoff_time: '2026-07-05T18:00:00Z', namespace: 'airport_cargo'})

// 关系
MATCH (agv:Device {id: 'AGV001'}), (task:Task {id: 'TASK001'})
CREATE (agv)-[:EXECUTING {started_at: '2026-07-05T10:00:00Z'}]->(task)

MATCH (task:Task {id: 'TASK001'}), (piece:Piece {id: 'PIECE001'})
CREATE (task)-[:MOVES]->(piece)

MATCH (piece:Piece {id: 'PIECE001'}), (uld:ULD {id: 'AKE12345'})
CREATE (piece)-[:LOADED_IN]->(uld)

MATCH (uld:ULD {id: 'AKE12345'}), (flight:Flight {id: 'LH729'})
CREATE (uld)-[:ASSIGNED_TO]->(flight)
```

### 7.3 智能制造场景（Domain Package）

```cypher
CREATE (:Machine {id: 'M001', type: 'CNC', status: 'RUNNING', namespace: 'manufacturing'})
CREATE (:ProductionLine {id: 'LINE_A', status: 'ACTIVE', namespace: 'manufacturing'})
CREATE (:WorkOrder {id: 'WO001', priority: 1, namespace: 'manufacturing'})
CREATE (:Exception {id: 'E001', severity: 'P1', type: 'MACHINE_FAILURE', namespace: 'manufacturing'})

MATCH (m:Machine {id: 'M001'}), (line:ProductionLine {id: 'LINE_A'})
CREATE (m)-[:PART_OF]->(line)

MATCH (line:ProductionLine {id: 'LINE_A'}), (order:WorkOrder {id: 'WO001'})
CREATE (order)-[:SCHEDULED_ON]->(line)

MATCH (e:Exception {id: 'E001'}), (m:Machine {id: 'M001'})
CREATE (e)-[:AFFECTS]->(m)
```

### 7.4 IT 运维场景（Domain Package）

```cypher
CREATE (:Service {id: 'api-gateway', status: 'HEALTHY', namespace: 'it_ops'})
CREATE (:Service {id: 'user-service', status: 'HEALTHY', namespace: 'it_ops'})
CREATE (:Service {id: 'database', status: 'DEGRADED', namespace: 'it_ops'})
CREATE (:Alert {id: 'A001', severity: 'critical', message: 'DB connection pool exhausted', namespace: 'it_ops'})

MATCH (gw:Service {id: 'api-gateway'}), (usr:Service {id: 'user-service'})
CREATE (gw)-[:DEPENDS_ON]->(usr)

MATCH (usr:Service {id: 'user-service'}), (db:Service {id: 'database'})
CREATE (usr)-[:DEPENDS_ON]->(db)

MATCH (alert:Alert {id: 'A001'}), (db:Service {id: 'database'})
CREATE (alert)-[:AFFECTS]->(db)
```

---

## 8. 数据约束

### 8.1 唯一性约束

```cypher
CREATE CONSTRAINT unique_device_id
ON (n:Device)
ASSERT n.id IS UNIQUE
```

### 8.2 存在性约束

```cypher
CREATE CONSTRAINT require_device_id
ON (n:Device)
ASSERT exists(n.id)
```

### 8.3 类型约束

```rust
pub struct PropertySchema {
    pub key: Symbol,
    pub value_type: PropertyType,
    pub required: bool,
    pub default: Option<PropertyValue>,
}

pub enum PropertyType {
    Integer,
    Float,
    String,
    DateTime,
    // ...
}
```

---

## 9. 迁移路径（P0.1）

### 9.1 Label 迁移

```rust
// 阶段 1：读取兼容层
fn get_labels(node: &NodeTask) -> HashSet<Symbol> {
    // 优先读取新字段
    if !node.labels.is_empty() {
        return node.labels.clone();
    }
    
    // 回退到 __labels 属性
    if let Some(PropertyValue::List(labels)) = node.properties.get("__labels") {
        labels.iter()
            .filter_map(|v| if let PropertyValue::String(s) = v { Some(s.into()) } else { None })
            .collect()
    } else {
        HashSet::new()
    }
}

// 阶段 2：写入双写
fn set_label(node: &mut NodeTask, label: Symbol) {
    // 新存储
    node.labels.insert(label.clone());
    
    // 旧存储（过渡期）
    let mut label_list = get_labels(node).into_iter().collect::<Vec<_>>();
    label_list.push(label.to_string());
    node.properties.insert("__labels".into(), PropertyValue::List(
        label_list.into_iter().map(PropertyValue::String).collect()
    ));
}

// 阶段 3：迁移工具
fn migrate_labels(graph: &GraphService) -> Result<()> {
    for node_id in graph.all_node_ids() {
        if let Some(PropertyValue::List(labels)) = graph.get_property(node_id, "__labels")? {
            for label in labels {
                if let PropertyValue::String(label_str) = label {
                    graph.add_label(node_id, label_str.into())?;
                }
            }
            // 可选：删除旧属性
            // graph.remove_property(node_id, "__labels")?;
        }
    }
    Ok(())
}
```

### 9.2 EdgeProperty 迁移

```rust
// 类似 Label 迁移策略
// 1. 读取兼容层
// 2. 写入双写
// 3. 迁移工具
```

---

## 10. 性能考虑

### 10.1 内存占用

- **每节点开销** — 约 200-500 字节（不含属性）
- **1M 节点** — 约 200-500 MB（裸节点）
- **LRU 驱逐** — 冷节点持久化到 RocksDB

### 10.2 查询性能

- **Label 查询** — O(1) 通过 Label Index
- **Property 查询** — O(log n) 通过 Property Index
- **边遍历** — O(1) 通过 Edge Index
- **多跳查询** — O(k * avg_degree^k) 需要 Fixpoint 优化

### 10.3 写入性能

- **WAL 写入** — O(1) 追加写
- **索引更新** — O(log n) BTreeMap 插入
- **Standing Query 触发** — O(affected_queries)

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05  
**实现进度:** P0.1 设计完成，待实现
