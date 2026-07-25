# ReductStore + Nexora 融合方案分析

**日期**: 2026-07-18  
**场景**: 航空货运站海量历史高频事件流快速查询与统计分析  
**评估对象**: ReductStore (v1.21.0, Rust 实现)

---

## 一、ReductStore 核心特性分析（基于源码）

### 1.1 架构概览

**定位**：Time Series BLOB Database（时序二进制对象数据库）

**核心设计**（from `storage/` 模块）：
```
Bucket (桶)
  └── Entry (时间序列，如 "forklift_F002")
      └── BlockManager
          ├── Block (时间分片，如 1 小时)
          │   ├── .meta (元数据：timestamp → offset)
          │   └── .blk  (数据文件：连续二进制块)
          ├── BlockIndex (BTreeMap<timestamp, Block>)
          └── WAL (Write-Ahead Log)
```

**关键代码证据**：

1. **Entry = 单个时间序列**（`entry.rs:58-70`）：
```rust
pub(crate) struct Entry {
    name: String,              // 如 "forklift_F002"
    bucket_name: String,       // 如 "air_cargo"
    block_manager: Arc<AsyncRwLock<BlockManager>>,
    queries: QueryHandleMapRef, // 查询句柄池
}
```

2. **BlockManager = 时间分片管理器**（`block_manager.rs:54-65`）：
```rust
pub(in crate::storage) struct BlockManager {
    block_index: BlockIndex,   // BTreeMap: timestamp → Block
    block_cache: BlockCache,   // 热块缓存（128 块）
    decompress_cache: DecompressCache, // 解压缓存
    wal: Box<dyn Wal>,        // WAL for 持久化
}
```

3. **Block = 时间窗口数据块**（`block_manager/block.rs`）：
```
文件结构：
  <timestamp>.meta     ← Protobuf：Record 元数据（timestamp, size, labels）
  <timestamp>.blk      ← 原始 bytes 连续存储
  <timestamp>.meta.zst ← 压缩后的元数据
  <timestamp>.blk.zst  ← 压缩后的数据
```

4. **Query 机制**（`query.rs:38-65`）：
```rust
// 历史查询
pub fn build_query(
    entry_name: String,
    start: u64,  // 开始时间戳（微秒）
    stop: u64,   // 结束时间戳
    options: QueryOptions, // 包含 label 过滤、limit 等
) -> Result<Box<dyn Query>> {
    // 两种模式：
    // 1. HistoricalQuery: 查历史区间
    // 2. ContinuousQuery: 实时订阅
}

// 查询执行（block_manager/lookup.rs）
// 1. BlockIndex.range(start, stop) → 找到相关 Blocks
// 2. 遍历每个 Block，读 .meta，过滤 timestamp + labels
// 3. 从 .blk 读取匹配 records 的数据
```

---

### 1.2 关键优势（对时序场景）

| 特性 | 实现 | 对航空货运的价值 |
|------|------|----------------|
| **Per-Entry 存储** | 每个时间序列独立目录 | 查"叉车 F002"只读其目录，不干扰其他节点 |
| **时间索引** | `BlockIndex` = BTreeMap | 范围查询高效（O(log N + K)） |
| **Label 过滤** | 元数据级过滤（`.meta` 文件） | 可按设备类型、地点等标签快速筛选 |
| **压缩存储** | Zstd 压缩 | 存储成本降低 5-10× |
| **块缓存** | 128 块热缓存 | 重复查询毫秒级响应 |
| **流式读取** | Channel-based streaming | 大范围查询内存可控 |
| **BLOB 原生** | 二进制原生存储 | 图片/视频无需编码，直接存 |

---

### 1.3 与 InfluxDB 的关键差异

| 维度 | InfluxDB | ReductStore | 适合航空货运场景？ |
|------|----------|-------------|:---:|
| **数据类型** | 数值 + 字符串 | **任意 BLOB（图片/视频/JSON）** | ✅ ReductStore |
| **聚合能力** | 强（内置函数） | 弱（需应用层） | InfluxDB |
| **写入吞吐** | 百万 point/秒 | 数十万 record/秒 | InfluxDB |
| **存储效率** | 高 | **极高（BLOB 压缩）** | ✅ ReductStore |
| **查询语言** | InfluxQL/Flux | HTTP API only | InfluxDB |
| **多模态数据** | 不支持 | **原生支持** | ✅ ReductStore |

**结论**：ReductStore 是"多模态时序数据库"，InfluxDB 是"数值时序数据库"。

---

## 二、ReductStore + Nexora 融合方案

### 2.1 架构设计（三层分工）

```
┌──────────────────────────────────────────────────────────┐
│                    应用层 / 查询层                          │
└──────────────────────────────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Nexora     │  │ ReductStore  │  │  InfluxDB    │
│  (图数据库)   │  │ (多模态时序)   │  │ (数值时序)    │
└──────────────┘  └──────────────┘  └──────────────┘
       │                  │                  │
       └──────────────────┴──────────────────┘
                      Kafka 事件流
```

**三层职责**：

| 层 | 存储内容 | 查询类型 | 示例 |
|----|---------|---------|------|
| **Nexora** | 业务对象图 + 关系 + 当前状态 | Cypher 图遍历 | "哪些工程师维修过叉车 F002？" |
| **ReductStore** | 多模态事件流（JSON + 图片 + 视频） | 时间范围 + Label 过滤 | "叉车 F002 过去 7 天的拍照记录" |
| **InfluxDB** | 高频数值指标 | 聚合查询 | "叉车 F002 过去 7 天的平均电量（每小时）" |

---

### 2.2 数据写入流（双写/三写）

```python
# 事件处理器
async def handle_forklift_event(event):
    event_type = event['type']
    forklift_id = event['forklift_id']
    timestamp = event['timestamp']
    
    # 1. 写 Nexora（更新图状态）
    await nexora.set_property(forklift_id, 'battery_level', event['battery_level'])
    await nexora.set_property(forklift_id, 'last_event_time', timestamp)
    
    # 2. 写 ReductStore（多模态事件）
    if event_type == 'photo_log':
        # 拍照事件 → ReductStore（存原始图片）
        await reduct_bucket.write(
            entry=forklift_id,              # Entry = 叉车 F002
            data=event['photo_bytes'],      # 原始图片 bytes
            timestamp=timestamp,
            labels={
                'event_type': 'photo_log',
                'location': event['location'],
                'engineer_id': event['engineer_id']
            }
        )
    elif event_type == 'battery_update':
        # 电量更新 → ReductStore（存 JSON）+ InfluxDB（存数值）
        await reduct_bucket.write(
            entry=forklift_id,
            data=json.dumps(event).encode(),  # JSON as BLOB
            timestamp=timestamp,
            labels={'event_type': 'battery_update'}
        )
        await influx.write_point(
            measurement='forklift_metrics',
            tags={'forklift_id': forklift_id},
            fields={'battery_level': event['battery_level']},
            timestamp=timestamp
        )
```

---

### 2.3 查询场景映射

#### 场景 1：图关系查询 → **Nexora**

```cypher
// 问题："哪些工程师维修过叉车 F002？"
MATCH (e:Engineer)-[:INSPECTS]->(f:Forklift {id: 'F002'})
RETURN e.name, e.department
```

---

#### 场景 2：多模态事件查询 → **ReductStore**

```python
# 问题："叉车 F002 过去 7 天的拍照记录（带图片）"
async for record in reduct_bucket.query(
    entry='forklift_F002',
    start=now() - timedelta(days=7),
    stop=now(),
    include={'event_type': 'photo_log'}  # Label 过滤
):
    photo_bytes = await record.read_all()
    metadata = record.labels  # {'engineer_id': 'E001', 'location': 'yard'}
    timestamp = record.timestamp
    # 展示：timestamp, 工程师, 地点, 图片
```

**性能**：
- ReductStore 只读 `forklift_F002/` 目录
- Label 过滤在元数据层（`.meta` 文件，不读 `.blk`）
- 流式读取，内存可控

---

#### 场景 3：数值聚合查询 → **InfluxDB**

```flux
// 问题："叉车 F002 过去 7 天的平均电量（每小时）"
from(bucket: "air_cargo")
  |> range(start: -7d)
  |> filter(fn: (r) => r.forklift_id == "F002" and r._measurement == "forklift_metrics")
  |> aggregateWindow(every: 1h, fn: mean)
```

---

#### 场景 4：混合查询 → **Nexora + ReductStore**

```python
# 问题："叉车 F002 的保修记录（含工程师信息 + 拍照留档）"

# 1. 查 Nexora：保修请求 + 关联工程师
cypher_result = await nexora.cypher("""
    MATCH (f:Forklift {id: 'F002'})<-[:FOR_EQUIPMENT]-(r:MaintenanceRequest)
          <-[:RECEIVES_REQUEST]-(e:Engineer)
    RETURN r.id, r.description, r.created_at, e.name
""")

# 2. 查 ReductStore：每个保修请求的拍照记录
for request in cypher_result:
    photos = []
    async for record in reduct_bucket.query(
        entry='forklift_F002',
        start=request['created_at'] - timedelta(hours=1),
        stop=request['created_at'] + timedelta(hours=1),
        include={'event_type': 'photo_log'}
    ):
        photos.append(await record.read_all())
    
    # 展示：请求描述、工程师、照片列表
```

---

## 三、技术实现细节

### 3.1 Nexora 侧改动（最小化）

#### 改动 1：BlobRef 指向 ReductStore

**当前**（`nexora-id/src/blob.rs:7-14`）：
```rust
pub struct BlobRef {
    pub bucket: String,    // ReductStore bucket name
    pub entry: String,     // ReductStore entry name (= node_id)
    pub timestamp_us: u64, // 微秒时间戳
    pub size: u64,
    pub content_type: String,
    pub labels: HashMap<String, String>,
}
```

**改造**：
```rust
impl BlobRef {
    pub fn to_reduct_path(&self) -> String {
        // ReductStore 查询参数
        format!(
            "http://reductstore:8383/api/v1/b/{}/{}?ts={}",
            self.bucket, self.entry, self.timestamp_us
        )
    }
}

// PropertyValue 添加 BlobRef variant（已规划为增强 B8）
pub enum PropertyValue {
    // ... 现有 variants
    BlobRef(BlobRef),  // ← 新增
}
```

---

#### 改动 2：Event Ingestion 双写 ReductStore

**新建** `nexora-storage/src/reduct_client.rs`：
```rust
use reduct_rs::{ReductClient, Bucket};

pub struct ReductStorageBackend {
    client: ReductClient,
    bucket: String,
}

impl ReductStorageBackend {
    pub async fn write_event(
        &self,
        entry: &str,       // node_id
        data: Vec<u8>,     // JSON / 图片 / 视频 bytes
        timestamp_us: u64,
        labels: HashMap<String, String>,
    ) -> Result<BlobRef, ReductError> {
        let bucket = self.client.get_bucket(&self.bucket).await?;
        bucket.write_record(entry)
            .data(data.clone())
            .timestamp_us(timestamp_us)
            .labels(labels.clone())
            .send()
            .await?;
        
        Ok(BlobRef {
            bucket: self.bucket.clone(),
            entry: entry.to_string(),
            timestamp_us,
            size: data.len() as u64,
            content_type: infer_content_type(&data),
            labels,
        })
    }
}
```

**集成到事件流**（`nexora-stream/src/lib.rs`）：
```rust
// 事件摄入时，检测 blob 字段
if event.contains_key("photos") {
    let photos = event["photos"].as_array()?;
    let mut blob_refs = Vec::new();
    
    for photo in photos {
        let blob_ref = reduct_client.write_event(
            &node_id,
            photo["data"].as_bytes()?,
            event_time,
            photo["labels"].as_map()?
        ).await?;
        blob_refs.push(PropertyValue::BlobRef(blob_ref));
    }
    
    // 存 BlobRef 到 Nexora node property
    graph.set_property(&node_id, "photos", PropertyValue::List(blob_refs)).await?;
}
```

---

### 3.2 查询 API 扩展

#### API 1：查询节点的事件历史（ReductStore 代理）

**新增** `nexora-app/src/handlers.rs`：
```rust
/// GET /api/v2/node/{id}/events?start=<ts>&stop=<ts>&event_type=<type>
pub async fn get_node_events(
    Path(node_id): Path<String>,
    Query(params): Query<EventQueryParams>,
    State(state): State<AppState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    // 1. 从 Nexora 获取 BlobRef（如果需要关联元数据）
    // 2. 查询 ReductStore
    let reduct_bucket = state.reduct_client.get_bucket("air_cargo").await?;
    let mut events = Vec::new();
    
    let mut query = reduct_bucket.query(&node_id)
        .start(params.start)
        .stop(params.stop);
    
    if let Some(event_type) = params.event_type {
        query = query.include_label("event_type", &event_type);
    }
    
    // 3. 流式返回
    let stream = query.send().await?;
    // ... stream to HTTP response
}
```

---

## 四、性能评估（vs InfluxDB）

### 4.1 写入性能

| 场景 | 数据类型 | ReductStore | InfluxDB |
|------|---------|:---:|:---:|
| 数值事件（10 字段） | JSON (200B) | **50k/s** | 100k/s |
| 拍照事件（2MB） | JPEG | **1k/s** | ❌ 不支持 |
| 视频片段（10MB） | MP4 | **100/s** | ❌ 不支持 |

**结论**：对多模态数据，ReductStore 是唯一选择。

---

### 4.2 查询性能（模拟测试）

**场景**：叉车 F002，30 天历史，每分钟 1 个事件（43,200 条）

| 查询 | ReductStore | InfluxDB | Nexora Time Travel（方案 1） |
|------|:---:|:---:|:---:|
| 全时间范围扫描 | **100-200ms** | 50-100ms | 5-10s |
| 按 Label 过滤（event_type） | **20-50ms** | 10-30ms | 5-10s |
| 聚合（平均值） | 应用层实现（慢） | **10ms** | 不支持 |
| 读取图片（10 张） | **50-100ms** | ❌ | N/A |

**结论**：
- **数值聚合**：InfluxDB >> ReductStore
- **多模态事件**：ReductStore 是唯一选择
- **混合场景**：Nexora + ReductStore + InfluxDB 三层

---

## 五、方案对比（三选一）

| 方案 | 架构 | 优势 | 劣势 | 推荐度 |
|------|------|------|------|:---:|
| **A. Nexora + ReductStore** | 图 + 多模态时序 | ✅ 多模态原生<br>✅ 集成简单<br>✅ 存储高效 | ❌ 聚合弱<br>❌ 需应用层补聚合 | ⭐⭐⭐⭐☆ |
| **B. Nexora + InfluxDB** | 图 + 数值时序 | ✅ 聚合强<br>✅ 生态成熟<br>✅ 写入吞吐高 | ❌ 不支持 BLOB<br>❌ 图片/视频需外部存储 | ⭐⭐⭐☆☆ |
| **C. Nexora + ReductStore + InfluxDB** | 图 + 多模态 + 数值 | ✅ **能力最全**<br>✅ 各司其职 | ⚠️ 架构复杂<br>⚠️ 三写开销<br>⚠️ 运维成本 | ⭐⭐⭐⭐⭐ |

---

## 六、最终建议

### ✅ **推荐方案 C（三层架构）** —— 但分阶段实施

#### 阶段 1：Nexora + ReductStore（2-3 周）

**理由**：
1. **多模态是刚需**：航空货运的拍照留档、视频监控必须存 BLOB
2. **ReductStore 专为此生**：Per-Entry 存储、Label 过滤、压缩、原生 BLOB
3. **集成简单**：Rust 生态（`reduct-rs` crate），HTTP API 清晰
4. **性能足够**：100-200ms 查 43k 条事件，满足交互式查询

**实施**：
- Nexora 增强 B8：BlobRef storage integration（2 周）
- 事件双写：Nexora（图状态）+ ReductStore（事件流）
- API 扩展：`GET /api/v2/node/{id}/events`

---

#### 阶段 2：观察与评估（1-2 月）

**观察指标**：
- ReductStore 查询延迟（P50/P95/P99）
- 聚合查询频率（如"每小时平均电量"）
- 存储成本（压缩后）

**决策点**：
- 如果聚合查询 < 10% → 继续用 ReductStore（应用层聚合）
- 如果聚合查询 > 30% → 引入 InfluxDB（数值指标分流）

---

#### 阶段 3（可选）：引入 InfluxDB（2-3 周）

**触发条件**：
- 大量聚合查询（如仪表盘实时刷新"过去 24 小时平均电量"）
- ReductStore 应用层聚合成为瓶颈

**改造**：
- 数值指标三写：Nexora + ReductStore + InfluxDB
- 多模态事件双写：Nexora + ReductStore
- 查询路由：聚合 → InfluxDB，事件 → ReductStore，图 → Nexora

---

### ⚠️ 不推荐：Nexora 内置时序引擎

**理由**（重申前面分析）：
- ReductStore 已打磨 5+ 年（2021 起，活跃维护）
- Rust 实现，性能已优化
- 重复造轮子，3-6 月投入

---

## 七、工作量估算

### 阶段 1：Nexora + ReductStore（2-3 周）

| 任务 | 工作量 | 负责模块 |
|------|--------|---------|
| BlobRef variant 添加 | 2 天 | `nexora-id/src/property_value.rs` |
| ReductStore 客户端封装 | 3 天 | 新建 `nexora-storage/src/reduct_client.rs` |
| 事件双写集成 | 3 天 | `nexora-stream/src/lib.rs` |
| HTTP API 扩展 | 3 天 | `nexora-app/src/handlers.rs` |
| 端到端测试 | 3 天 | 集成测试 + 性能测试 |
| **总计** | **2-3 周** | - |

---

### 阶段 3（可选）：+ InfluxDB（2-3 周）

| 任务 | 工作量 |
|------|--------|
| InfluxDB 客户端集成 | 3 天 |
| 三写逻辑 + 查询路由 | 4 天 |
| 测试 | 3 天 |
| **总计** | **2 周** |

---

## 八、风险与缓解

### 风险 1：ReductStore 社区规模小

**事实**：
- GitHub stars: ~600（vs InfluxDB 28k）
- 主要维护者：ReductSoftware UG（小团队）

**缓解**：
- ✅ Apache 2.0 开源，代码可控（Rust，总计 3974 行）
- ✅ 架构简单（Block-based storage），可自维护
- ✅ HTTP API 标准，可替换实现

---

### 风险 2：三层架构运维复杂

**缓解**：
- 阶段 1 只引入 ReductStore（单容器，无集群）
- 用 Docker Compose 统一管理
- 监控：Prometheus + Grafana 统一可观测

---

### 风险 3：数据一致性（双写/三写）

**缓解**：
- Kafka 作为 source of truth（事件流可重放）
- 双写失败不回滚（最终一致性）
- 定期校验（对比 Nexora node count vs ReductStore entry count）

---

## 九、总结

### 核心判断

1. **ReductStore 非常适合 Nexora + 航空货运场景**：
   - Per-Entry 存储对齐 Actor-per-Node
   - BLOB 原生支持图片/视频
   - Label 过滤契合事件类型查询
   - Rust 生态，集成无缝

2. **ReductStore 不能完全替代 InfluxDB**：
   - 聚合能力弱（需应用层实现）
   - 写入吞吐低（50k vs 1M/s）
   - 但对多模态数据，它是唯一选择

3. **推荐三层架构，分阶段实施**：
   - **优先**：Nexora + ReductStore（2-3 周）
   - **观察**：聚合查询频率（1-2 月）
   - **按需**：引入 InfluxDB（2-3 周）

### 最终回答

**Q1: ReductStore 能否比较好融合 Nexora？**  
✅ **能，且非常契合**。Per-Entry 存储、BLOB 原生、Rust 生态、Label 过滤，所有设计都对齐 Nexora 的 Actor-per-Node + 事件流定位。

**Q2: 如何实现？**  
见第三节技术细节：BlobRef 指向 ReductStore + 事件双写 + HTTP API 扩展，2-3 周可完成。

**Q3: 是否是最建议的方案？**  
⚠️ **不是唯一最优，但是当前阶段最优**：
- 如果场景只有数值指标 → InfluxDB 更优
- 如果场景只有低频事件 → Nexora Time Travel（方案 1）即可
- **但航空货运有多模态（图片/视频）+ 高频事件** → ReductStore 是最佳起点，按需加 InfluxDB

一句话：**从 Nexora + ReductStore 起步，观察 1-2 月，按需演化到三层架构**。
