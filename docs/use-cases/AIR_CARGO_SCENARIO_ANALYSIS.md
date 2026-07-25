# 航空货运站场景分析：Nexora 适配度评估

**日期**: 2026-07-18  
**场景来源**: 真实应用需求  
**评估人**: Claude (基于代码 file:line 级核查)

---

## 一、场景理解（结构化拆解）

### 1.1 核心业务对象

| 对象类型 | 示例 | Nexora 映射 |
|---------|------|------------|
| **人员** | 运维工程师 | Node (Label: `:Engineer`) |
| **设备** | 叉车 | Node (Label: `:Forklift`) |
| **货物** | 货箱、托盘 | Node (Label: `:Cargo`) |
| **设施场所** | 仓库、停机坪、充电区 | Node (Label: `:Facility`) |

### 1.2 关系图谱

```
Engineer --[INSPECTS]--> Forklift
Engineer --[RECEIVES_REQUEST]--> MaintenanceRequest
MaintenanceRequest --[FOR_EQUIPMENT]--> Forklift
Forklift --[LOCATED_AT]--> ChargingStation
Cargo --[LOADED_ON]--> Forklift
Facility --[CONTAINS]--> Forklift
```

### 1.3 时序事件流（运维工程师的一天）

| 时间 | 事件 | 数据类型 | 属性示例 |
|------|------|---------|---------|
| 09:00 | 清点叉车清单 | **结构化** | `{event_type: "inventory_check", forklift_ids: [...], status: "completed"}` |
| 09:30 | 拍照留档 | **结构化 + 图片** | `{event_type: "photo_log", forklift_id: "F001", photos: [BlobRef...], location: "yard"}` |
| 10:30 | 登记保修请求 | **结构化 + 文本** | `{event_type: "maintenance_request", forklift_id: "F002", description: "液压泵异响", priority: "high"}` |
| 11:00 | 充电完成登记 | **结构化** | `{event_type: "charging_complete", forklift_id: "F003", battery_level: 100%, duration_mins: 120}` |

### 1.4 核心需求（四个维度）

1. **实时状态维护**：每个事件触发对应业务对象（Engineer / Forklift）的 **properties 更新** + **关系变更**
2. **异构数据支持**：结构化（JSON）+ 非结构化（文本、图片、视频）
3. **时序追溯**：未来查询"2026-07-18 10:30 时刻，叉车 F002 的状态是什么？"
4. **统计分析**：跨时间维度聚合（如"本月叉车维修次数 TOP10"）

---

## 二、Nexora 当前能力评估（逐项核查）

### ✅ 2.1 业务对象建模 —— **完全支持**

**证据**：
- Node = 业务对象（`nexora-core/src/graph/mod.rs`）
- Label 分类（`:Engineer` / `:Forklift` / `:Cargo`）
- Property 系统：`PropertyValue` enum（`nexora-id/src/property_value.rs:23`）支持：
  ```rust
  pub enum PropertyValue {
      String(String),        // 姓名、ID
      Integer(i64),          // 电量、维修次数
      Float(f64),            // 经纬度
      Boolean(bool),         // 是否在线
      List(Vec<PropertyValue>), // 叉车清单
      Map(BTreeMap<String, PropertyValue>), // 嵌套结构
      Date / LocalDateTime / ZonedDateTime, // 时间戳
      // ... 完整类型系统
  }
  ```

**实践**：
```cypher
CREATE (e:Engineer {
  id: "E001",
  name: "张三",
  department: "技术工程部",
  shift: "morning",
  last_activity_time: datetime("2026-07-18T11:00:00Z")
})

CREATE (f:Forklift {
  id: "F002",
  model: "Toyota 7FBE15",
  battery_level: 85.5,
  status: "charging",
  location: {x: 120.5, y: 31.2},
  last_maintenance: date("2026-07-10")
})

CREATE (e)-[:INSPECTS {inspected_at: datetime()}]->(f)
```

---

### ✅ 2.2 实时关系图谱 —— **完全支持**

**证据**：
- Edge = 关系（`nexora-value/src/half_edge.rs`）
- 实时更新：Actor-per-Node 模型，每个对象独立处理事件
- Standing Query（`nexora-standing-query`）：模式匹配，变更实时触发

**实践**：
```rust
// 事件：运维工程师接收保修请求
graph.add_edge(
    engineer_id,
    HalfEdge::out(Symbol::new("RECEIVES_REQUEST"), request_id)
).await?;

graph.add_edge(
    request_id,
    HalfEdge::out(Symbol::new("FOR_EQUIPMENT"), forklift_id)
).await?;

// Standing Query 自动触发：检测到"高优先级请求"→ 推送告警
```

---

### ⚠️ 2.3 异构数据支持 —— **部分支持，需补强**

#### ✅ 已支持：结构化 + 文本

**证据**：
- `PropertyValue::String`：文本信息（如"液压泵异响"）
- `PropertyValue::Map`：嵌套 JSON（如 `{description: "...", tags: [...]}`）

#### ⚠️ 已有基础设施，但未完整集成：图片 / 视频 (BlobRef)

**证据**：
```rust
// nexora-id/src/blob.rs:7
pub struct BlobRef {
    pub bucket: String,        // 存储桶（如 "inspection_photos"）
    pub entry: String,         // 对象 ID（如 "forklift_F001"）
    pub timestamp_us: u64,     // 微秒时间戳
    pub size: u64,             // 文件大小
    pub content_type: String,  // MIME 类型（如 "image/jpeg"）
    pub labels: HashMap<String, String>, // 元数据标签
}

// 存储路径：bucket/entry/timestamp
// 示例：inspection_photos/forklift_F001/1721305800000000
```

**当前状态**：
- ✅ `BlobRef` 结构完整，ReductStore 兼容
- ✅ 作为 `PropertyValue` 的一部分存在（`nexora-id/src/lib.rs:19` re-export）
- ❌ **缺失**：没有 `PropertyValue::BlobRef` variant（当前 PropertyValue enum 未包含 BlobRef）
- ❌ **缺失**：没有 HTTP API 上传/下载 blob（`handlers.rs` 无 `/api/blob` 端点）
- ❌ **缺失**：没有实际的 blob 存储后端集成（ReductStore / S3 / MinIO）

**实际影响**：
- **当前只能存 BlobRef 的元数据**（bucket/entry/size），真实图片/视频文件需外部存储
- 应用需自己管理 S3 上传 → 拿到 URL → 存为 `String` property

---

### ⚠️ 2.4 时序追溯 —— **骨架存在，需补全**

#### ✅ 已有基础：Fragment + Time Travel

**证据**：
```rust
// nexora-fragment/src/time_travel.rs:20
/// Time-travel query engine: snapshots, consolidation, historical views.

pub struct TimeTravelQuery {
    pub as_of: u64,                // 目标时间戳（微秒）
    pub node_id: Option<NexoraId>, // 查询特定节点
    pub namespace: Option<String>, // 命名空间过滤
    pub property_filter: Option<(String, PropertyCondition)>,
}

// nexora-fragment/src/time_travel.rs:85
pub struct NodeSnapshot {
    pub id: NexoraId,
    pub timestamp: u64,
    pub properties: HashMap<String, PropertyValue>,
    pub edges: Vec<EdgeSnapshot>,
}
```

**设计**：
- Fragment = 时间窗口内的状态切片
- Time Travel = 重放 fragments 到目标时间点
- `execute_time_travel(query)` → 返回 `NodeSnapshot`

**当前状态**：
- ✅ 接口完整（`time_travel.rs` 185 行）
- ✅ Fragment store 存在（`nexora-fragment/src/store.rs`）
- ⚠️ **未完整集成**：
  - `FragmentStore` 标记为 "planned for future integration"
  - 没有 HTTP API `/api/time-travel`（`handlers.rs` 无此端点）
  - Fragment 生成/归档未自动化（需手动触发）

#### ✅ 已有补充：Node Journal（事件日志）

**证据**：
```rust
// nexora-core/src/graph/node_task.rs:183
pub journal: Vec<TimedEvent<NodeChangeEvent>>,

// nexora-core/src/event.rs
pub struct TimedEvent<E> {
    pub event: E,
    pub event_time: EventTime, // 微秒时间戳
}

pub enum NodeChangeEvent {
    PropertySet { key, value },
    EdgeAdded { edge },
    LabelAdded { label },
    // ... 11 种变更事件
}
```

**用途**：
- 每个节点维护自己的事件流（journal）
- Sleep 时持久化，Wake 时 replay
- 可用于"查看叉车 F002 的最近 100 条事件"

**当前状态**：
- ✅ Journal 机制完整工作
- ❌ **缺失**：没有 HTTP API 查询某节点的 journal（`GET /api/node/{id}/history`）
- ❌ **缺失**：Journal 未与 Fragment time-travel 互通（两套系统）

---

### ⚠️ 2.5 统计分析 —— **部分支持，需增强**

#### ✅ 已支持：实时聚合（Materialized View）

**证据**：
```rust
// nexora-core/src/materialized_view.rs
// 增量物化视图：变更触发更新

// 示例：维修次数统计
CREATE MATERIALIZED VIEW maintenance_count AS
SELECT forklift_id, COUNT(*) as count
FROM MaintenanceRequest
GROUP BY forklift_id
```

**机制**：
- Standing Query 检测变更 → 触发 MV 更新
- `SQMVBridge` 连接两者（`materialized_view.rs`）

**当前状态**：
- ✅ 增量聚合能力存在
- ⚠️ **限制**：只能对"实时状态"聚合，不能跨历史时间维度（如"本月 vs 上月对比"）

#### ❌ 缺失：时序分析（Time Series）

**需求示例**：
- "过去 7 天，叉车 F002 的电量曲线"
- "本月每天的维修请求数量趋势"

**当前状态**：
- ❌ 没有专门的 time-series 数据结构
- ❌ 没有时间窗口聚合（如 `GROUP BY date_trunc('day', timestamp)`）
- ⚠️ **可绕过**：用 Fragment time-travel + Cypher 查询模拟，但性能差

---

## 三、适配度总结（能力矩阵）

| 需求维度 | 当前状态 | 完成度 | 阻断级别 |
|---------|---------|:---:|:---:|
| **业务对象建模** | ✅ 完全支持 | 100% | - |
| **实时关系图谱** | ✅ 完全支持 | 100% | - |
| **结构化数据** | ✅ 完全支持 | 100% | - |
| **文本数据** | ✅ 完全支持 | 100% | - |
| **图片/视频（BlobRef）** | ⚠️ 基础设施有，未集成 | 30% | **P1** |
| **时序追溯（Time Travel）** | ⚠️ 骨架存在，未完整 | 50% | **P1** |
| **事件日志（Journal）** | ✅ 工作中，缺 API | 70% | P2 |
| **实时聚合（MV）** | ✅ 增量聚合支持 | 80% | - |
| **时序分析（TS）** | ❌ 缺失 | 10% | P2 |

**总体判断**：
- **核心图能力（对象+关系+实时）**：✅ 生产可用
- **异构数据（图片/视频）**：⚠️ 需 2-3 周补齐 blob 存储集成
- **时序能力（追溯/分析）**：⚠️ 基础设施有，需 3-4 周完整接入

---

## 四、必须完善的增强项（优先级排序）

### P0：无此能力则场景不可用

**无** —— 核心图能力已满足最小需求。

### P1：短期必须补齐（2-4 周）

#### 增强 1：BlobRef 存储集成 ⭐

**任务**：
1. 在 `PropertyValue` enum 添加 `BlobRef(BlobRef)` variant
2. 实现 blob 存储后端（优先 S3/MinIO，备选 ReductStore）
3. HTTP API：
   ```
   POST /api/blob/upload   → 返回 BlobRef
   GET  /api/blob/download → 根据 BlobRef 下载
   ```
4. 集成到事件摄入：从 Kafka 消息中解析 BlobRef，关联到 node property

**工作量**：2 周

**代码位置**：
- `nexora-id/src/property_value.rs`：添加 variant
- 新建 `nexora-storage/src/blob_backend.rs`：S3 SDK 集成
- `nexora-app/src/handlers.rs`：添加 upload/download 端点
- `nexora-stream/src/lib.rs`：事件中的 blob 字段解析

**验证**：
```rust
// 运维工程师拍照事件
let event = json!({
    "event_type": "photo_log",
    "engineer_id": "E001",
    "forklift_id": "F002",
    "photos": [
        {
            "bucket": "inspection_photos",
            "entry": "forklift_F002",
            "timestamp_us": 1721305800000000,
            "content_type": "image/jpeg",
            "size": 2048576
        }
    ]
});

// 摄入后，叉车节点的 properties 包含：
forklift.get_property("last_inspection_photo") 
    → PropertyValue::BlobRef(...)
```

---

#### 增强 2：Time Travel API 完整接入 ⭐

**任务**：
1. 完成 `FragmentStore` 集成（当前 "planned"）
2. Fragment 生成自动化：定期（如每小时）或触发式（如大批量写入后）
3. HTTP API：
   ```
   POST /api/time-travel
   Body: {
       "as_of": "2026-07-18T10:30:00Z",
       "node_id": "forklift_F002",
       "properties": ["status", "battery_level", "location"]
   }
   Response: NodeSnapshot
   ```
4. 与 Journal 互通：Fragment + Journal = 完整时间线

**工作量**：2-3 周

**代码位置**：
- `nexora-fragment/src/store.rs`：完成 RocksDB 后端
- `nexora-fragment/src/lib.rs`：Fragment 生成调度器
- `nexora-app/src/handlers.rs`：添加 `/api/time-travel` 端点
- `nexora-core/src/graph/shard/mod.rs`：Fragment 触发点

**验证**：
```bash
curl -X POST http://localhost:8080/api/time-travel \
  -d '{"as_of": "2026-07-18T10:30:00Z", "node_id": "forklift_F002"}'

# 返回：
{
  "id": "forklift_F002",
  "timestamp": 1721305800000000,
  "properties": {
    "status": "maintenance_requested",
    "battery_level": 65.0,
    "location": {"x": 120.3, "y": 31.1}
  },
  "edges": [
    {"edge_type": "RECEIVES_REQUEST", "other": "req_001", ...}
  ]
}
```

---

### P2：中期增强（1-2 月）

#### 增强 3：Node History API（事件日志查询）

**任务**：
```
GET /api/node/{id}/history?limit=100&before=timestamp
```

返回该节点的 `journal` 事件流（`Vec<TimedEvent<NodeChangeEvent>>`）。

**工作量**：1 周

---

#### 增强 4：时序聚合能力（基于 Cypher 扩展）

**任务**：
1. Cypher 扩展函数：
   ```cypher
   // 叉车 F002 过去 7 天的电量曲线（每小时采样）
   MATCH (f:Forklift {id: "F002"})
   CALL time_series.sample(f, 'battery_level', 
       datetime('2026-07-11T00:00:00Z'),
       datetime('2026-07-18T00:00:00Z'),
       '1 hour'
   ) YIELD timestamp, value
   RETURN timestamp, value
   ```
2. 底层实现：遍历 Fragment + Journal，按时间窗口采样

**工作量**：2-3 周（需 Cypher UDF 机制）

---

#### 增强 5：Domain Package for Air Cargo（领域模板）

**任务**：
创建 `air_cargo_domain.yaml`，预定义：
- Labels：`:Engineer`, `:Forklift`, `:Cargo`, `:Facility`, `:MaintenanceRequest`
- EdgeTypes：`:INSPECTS`, `:RECEIVES_REQUEST`, `:LOCATED_AT`
- EventMapping：Kafka 事件 → 图变更规则
- Standing Queries：高优先级保修请求 → 告警

**工作量**：1 周（配置 + 测试）

**代码位置**：
- `examples/domains/air_cargo.yaml`（新建）
- `nexora-core/src/domain_package.rs`（已有加载器）

---

## 五、实施路线（针对此场景）

### 阶段 1：快速验证（1 周）

**目标**：证明核心图能力可用

**任务**：
1. 用现有 API 建模：Engineer / Forklift / Cargo 节点
2. 摄入模拟事件流（Kafka）：清点、充电完成（纯结构化）
3. Standing Query：检测"电量 < 20%" → 推送告警
4. Cypher 查询：`MATCH (e:Engineer)-[:INSPECTS]->(f:Forklift) RETURN f.status`

**验收**：实时图谱工作，无 blob/time-travel

---

### 阶段 2：补齐关键能力（3-4 周）

**目标**：支持图片 + 时序追溯

**任务**：
1. 增强 1：BlobRef 存储集成（2 周）
2. 增强 2：Time Travel API（2-3 周）
3. 端到端测试：拍照事件 → blob 上传 → 关联到节点 → 历史查询

**验收**：
- 能存储"09:30 拍照事件"（图片）
- 能查询"10:30 时刻叉车 F002 的状态"

---

### 阶段 3：生产优化（1-2 月）

**目标**：统计分析 + 运维友好

**任务**：
1. 增强 3：Node History API（1 周）
2. 增强 4：时序聚合（2-3 周）
3. 增强 5：Air Cargo Domain Package（1 周）
4. 性能调优：索引优化、MV 策略

**验收**：
- 能查询"本月维修次数 TOP10 叉车"
- 能绘制"过去 7 天电量曲线"

---

## 六、风险与限制

### 风险 1：BlobRef 存储开销

**问题**：大量图片/视频（如每天 1000 张照片）会产生可观存储成本。

**缓解**：
- 使用 S3 生命周期策略：>30 天 → Glacier
- BlobRef 只存元数据（bucket/entry/size），真实文件在 S3
- 可选：图片压缩/缩略图生成

### 风险 2：Time Travel 性能

**问题**：重放大量 Fragment 到历史时间点，查询可能慢（秒级）。

**缓解**：
- Fragment 粒度优化（如每小时一个，而非每分钟）
- 只保留热数据的 Fragment（如最近 90 天）
- 冷数据导出到 Parquet（离线分析）

### 风险 3：时序分析不如专业 TSDB

**问题**：Nexora 不是时序数据库，对"高频采样+聚合"（如每秒心跳）性能不如 InfluxDB/TimescaleDB。

**缓解**：
- **图能力 + TSDB 混合架构**：
  - Nexora：业务对象图 + 关系 + 事件流
  - InfluxDB：高频指标时序（电量、温度、心跳）
  - 两者通过 node property 关联（如 `forklift.timeseries_id → InfluxDB measurement`）

---

## 七、最终建议

### ✅ Nexora **适合**此场景，理由：

1. **核心诉求是图关系**：人员-设备-货物-设施的复杂关系网络，图数据库是天然选择
2. **事件驱动**：航空货运站是高频事件流（装卸、运输、维修），对标 Nexora 的流式定位
3. **实时性**：Standing Query 可实时检测"设备故障"→ 告警，比批处理快
4. **增量成本低**：核心能力已有，BlobRef + Time Travel 补齐只需 3-4 周

### ⚠️ 但需要补齐两项（否则体验不完整）：

1. **BlobRef 存储集成**（P1，2 周）：支持图片/视频
2. **Time Travel API**（P1，2-3 周）：支持历史追溯

### 🔄 长期演化方向：

- **混合架构**：Nexora（图+事件） + InfluxDB（高频指标） + S3（Blob）
- **Domain Package 生态**：构建"航空货运"/"制造业"/"物流"等领域模板
- **Low-Code 配置**：通过 YAML 配置事件映射，无需写代码

---

## 附：快速启动示例

```yaml
# air_cargo_domain.yaml
schema:
  labels:
    - name: Engineer
      properties: [id, name, department, shift]
    - name: Forklift
      properties: [id, model, battery_level, status, location]
    - name: MaintenanceRequest
      properties: [id, description, priority, created_at]
  
  edge_types:
    - name: INSPECTS
      properties: [inspected_at, notes]
    - name: RECEIVES_REQUEST
      properties: [received_at]

event_mappings:
  - source: kafka.air_cargo.events
    event_type: photo_log
    node:
      label: Engineer
      id: $.engineer_id
    properties:
      last_photo_time: $.timestamp
      last_inspection_photos: $.photos  # BlobRef[]
    edges:
      - edge_type: INSPECTS
        target:
          label: Forklift
          id: $.forklift_id

standing_queries:
  - name: high_priority_maintenance
    pattern: |
      MATCH (r:MaintenanceRequest {priority: 'high'})
      WHERE r.created_at > datetime() - duration('PT1H')
      RETURN r.id, r.description
    sink:
      type: webhook
      url: https://ops.example.com/alerts
```

```bash
# 启动 Nexora + 加载领域模板
nexora --config air_cargo.toml \
       --domain-package air_cargo_domain.yaml \
       --kafka-bootstrap localhost:9092
```
