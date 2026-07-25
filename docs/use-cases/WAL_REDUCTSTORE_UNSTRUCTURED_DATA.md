# WAL + ReductStore 异步复制对非结构化数据支持的影响分析

**日期**: 2026-07-18  
**问题**: WAL + ReductStore 异步复制方案，是否能支持事件流中的非结构化属性（文本、图片、视频）？  
**核心洞察**: 异步复制作为"转换层"，可以改变数据流向

---

## 一、关键判断

### ✅ 是的！这个方案天然支持非结构化数据

**原因**：异步复制器充当"转换网关"，可以识别并路由不同类型的数据到不同后端。

---

## 二、当前架构的限制

### 2.1 WAL 当前只支持结构化数据

**代码证据**（`event.rs:64`）：
```rust
pub enum NodeChangeEvent {
    /// Set a property on this node.
    PropertySet { key: Symbol, value: PropertyValue },
    // ...
}
```

**PropertyValue 已支持的类型**（`property_value.rs:23`）：
```rust
pub enum PropertyValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),        // ← 文本支持
    Bytes(Vec<u8>),        // ← 可以存小二进制（但不适合大文件）
    List(Vec<PropertyValue>),
    Map(BTreeMap<String, PropertyValue>),
    Date, LocalDateTime, ZonedDateTime,
    // ... 但无 BlobRef variant
}
```

**问题**：
- `String` 可以存文本，但存 base64 编码的图片会导致 WAL 膨胀
- `Bytes` 可以存小二进制，但存 10MB 视频会导致：
  - WAL 文件巨大（影响 replay 速度）
  - 内存占用高（actor mailbox 存事件）
  - 序列化开销大（FlatBuffers 不适合大 blob）

---

### 2.2 Track B8 计划的改进（未完成）

**计划**（`ROADMAP.md` Track B8）：
```rust
// 添加 BlobRef variant
pub enum PropertyValue {
    // ... 现有类型
    BlobRef(BlobRef),  // ← 指向外部 blob 存储
}

pub struct BlobRef {
    bucket: String,
    entry: String,
    timestamp_us: u64,
    size: u64,
    content_type: String,
}
```

**但需要外部 blob 存储**（如 S3/MinIO），WAL 只存引用。

---

## 三、WAL + ReductStore 异步复制的天然优势

### 3.1 ReductStore 原生支持 BLOB

**关键特性**：
- ReductStore = **Time Series Blob Database**
- 设计目标：存储任意大小的二进制对象（图片/视频/传感器数据）
- 优势：压缩、分块、流式读取

**API**：
```python
# 写入图片（原始 JPEG bytes）
await bucket.write(
    entry='forklift_F002',
    data=photo_bytes,  # ← 10MB 图片
    timestamp=now(),
    labels={'event_type': 'photo_log', 'size': '10MB'}
)

# 流式读取（不占内存）
async for record in bucket.query('forklift_F002', ...):
    photo_bytes = await record.read_all()  # 或 read() 流式
```

---

### 3.2 异步复制器作为"智能网关"

**核心思路**：复制器检测事件类型，分流处理

```rust
async fn replicate_event(event: &WalRecord) -> Result<()> {
    match &event.operation {
        WalOperation::NodeEvent { qid, event } => {
            match &event.event {
                // 普通属性 → 正常复制
                NodeChangeEvent::PropertySet { key, value } => {
                    match value {
                        // 结构化数据 → JSON 序列化后存 ReductStore
                        PropertyValue::Integer(_) | PropertyValue::Float(_) 
                        | PropertyValue::String(_) => {
                            reduct.write(
                                &format!("node_{}", qid),
                                serde_json::to_vec(&event)?,
                                event.time.as_micros()
                            ).await?;
                        },
                        
                        // BLOB 引用 → 已在外部存储，跳过
                        PropertyValue::BlobRef(blob_ref) => {
                            // Option 1: 不复制（ReductStore 已是 blob 后端）
                            // Option 2: 复制元数据（不复制 blob 本身）
                            reduct.write(
                                &format!("node_{}", qid),
                                serde_json::to_vec(&BlobRefEvent {
                                    event_type: "blob_ref",
                                    key: key.to_string(),
                                    blob_ref: blob_ref.clone(),
                                })?,
                                event.time.as_micros()
                            ).await?;
                        },
                        
                        // Bytes（小二进制）→ 直接复制
                        PropertyValue::Bytes(bytes) if bytes.len() < 1024 * 1024 => {
                            reduct.write(...).await?;
                        },
                        
                        // Bytes（大二进制，如嵌入的图片）→ 提取到 ReductStore
                        PropertyValue::Bytes(bytes) => {
                            // 原始事件中的大 blob 提取出来
                            reduct.write(
                                &format!("node_{}_blob", qid),
                                bytes.clone(),
                                event.time.as_micros()
                            ).await?;
                            
                            // 事件本身只存引用
                            reduct.write(
                                &format!("node_{}", qid),
                                serde_json::to_vec(&BlobExtractedEvent {
                                    event_type: "blob_extracted",
                                    key: key.to_string(),
                                    blob_entry: format!("node_{}_blob", qid),
                                    size: bytes.len(),
                                })?,
                                event.time.as_micros()
                            ).await?;
                        },
                        
                        _ => { /* 其他类型正常处理 */ }
                    }
                }
                _ => { /* 其他事件类型 */ }
            }
        }
    }
    Ok(())
}
```

---

## 四、三种集成方案

### 方案 A：直写模式（推荐）⭐

**架构**：
```
┌─────────────────────────────────────────────────┐
│            事件摄入（Kafka）                      │
└─────────────────────────────────────────────────┘
                    │
           识别事件类型（应用层）
                    │
        ┌───────────┴────────────┐
        │                        │
   结构化事件              非结构化事件
  （属性更新）            （图片/视频）
        │                        │
        ▼                        ▼
┌──────────────┐        ┌──────────────┐
│ WAL + Actor  │        │ ReductStore  │
│（图状态更新）  │        │ （直接写入）   │
└──────────────┘        └──────────────┘
        │                        │
        │                        ▼
        │              生成 BlobRef
        ▼                        │
 PropertySet(BlobRef) ───────────┘
        │
        ▼
   WAL 记录（只含 BlobRef）
        │
        ▼
  ReductStore 异步复制（只复制元数据）
```

**关键**：
- 大 blob 直接写 ReductStore（绕过 WAL）
- WAL 只存 BlobRef（几十字节）
- 异步复制时，blob 已在 ReductStore，无需重复写入

---

**实现**：
```rust
// 事件摄入处理器
async fn handle_forklift_event(event: KafkaEvent) -> Result<()> {
    match event.event_type.as_str() {
        // 结构化事件 → 走 WAL
        "battery_update" => {
            graph.set_property(
                &event.forklift_id,
                "battery_level",
                PropertyValue::Float(event.battery_level)
            ).await?;
        },
        
        // 非结构化事件（拍照）→ 直写 ReductStore
        "photo_log" => {
            // 1. 直接写 ReductStore
            let blob_ref = reduct_client.write_blob(
                entry: &event.forklift_id,
                data: event.photo_bytes,  // 10MB JPEG
                timestamp: event.timestamp,
                labels: hashmap! {
                    "event_type" => "photo_log",
                    "engineer_id" => event.engineer_id,
                }
            ).await?;
            
            // 2. WAL 只存 BlobRef
            graph.set_property(
                &event.forklift_id,
                "last_photo",
                PropertyValue::BlobRef(blob_ref)  // ← Track B8 的 BlobRef variant
            ).await?;
            
            // 3. 追加到 photos 列表
            graph.append_to_list(
                &event.forklift_id,
                "photos",
                PropertyValue::BlobRef(blob_ref)
            ).await?;
        },
        
        _ => {}
    }
    Ok(())
}
```

**优势**：
- ✅ WAL 轻量（只存引用，不存 blob）
- ✅ Replay 快速（blob 不在 WAL 中）
- ✅ ReductStore 直接可查（无需等异步复制）
- ✅ 无重复写入

---

### 方案 B：WAL 内嵌小 blob，复制器提取

**架构**：
```
事件摄入 → WAL（含小 blob，如缩略图）
              │
              ▼
         Actor apply
              │
              ▼
    异步复制器（检测 blob）
              │
     ┌────────┴────────┐
     │                 │
  元数据事件      提取 blob
     │                 │
     ▼                 ▼
ReductStore       ReductStore
(JSON event)      (blob entry)
```

**适用场景**：
- WAL 需要包含完整事件（用于审计/合规）
- 事件中的 blob 不大（如 100KB 缩略图）

**缺点**：
- ⚠️ WAL 膨胀（blob 在 WAL 中）
- ⚠️ Replay 变慢（需反序列化 blob）
- ⚠️ 重复存储（WAL + ReductStore 都有 blob）

---

### 方案 C：混合模式（现实场景）

**策略**：按 blob 大小分流

```rust
async fn handle_event_with_blob(event: Event, blob: Vec<u8>) -> Result<()> {
    if blob.len() < 256 * 1024 {  // 小于 256KB
        // 方案 B：内嵌到 WAL
        graph.set_property(
            &event.node_id,
            &event.key,
            PropertyValue::Bytes(blob)  // ← 直接存 WAL
        ).await?;
    } else {
        // 方案 A：直写 ReductStore
        let blob_ref = reduct.write_blob(...).await?;
        graph.set_property(
            &event.node_id,
            &event.key,
            PropertyValue::BlobRef(blob_ref)  // ← 只存引用
        ).await?;
    }
    Ok(())
}
```

**优势**：
- ✅ 小 blob（缩略图、配置）→ WAL 内嵌（简化查询）
- ✅ 大 blob（原图、视频）→ ReductStore（避免 WAL 膨胀）

---

## 五、具体实现步骤

### 步骤 1：添加 BlobRef variant（1 周）

**Track B8**：
```rust
// nexora-id/src/property_value.rs
pub enum PropertyValue {
    // ... 现有类型
    BlobRef(BlobRef),  // ← 新增
}
```

---

### 步骤 2：直写 ReductStore 工具（1 周）

```rust
// nexora-storage/src/reduct_blob_writer.rs
pub struct ReductBlobWriter {
    client: ReductClient,
    bucket: String,
}

impl ReductBlobWriter {
    pub async fn write_blob(
        &self,
        entry: &str,
        data: Vec<u8>,
        timestamp_us: u64,
        labels: HashMap<String, String>,
    ) -> Result<BlobRef> {
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

---

### 步骤 3：事件摄入识别 blob（1 周）

```rust
// nexora-stream/src/event_handler.rs
async fn handle_kafka_event(event: KafkaEvent) -> Result<()> {
    // 检测事件中的 blob 字段
    if let Some(photo_base64) = event.get("photo") {
        let photo_bytes = base64::decode(photo_base64)?;
        
        // 直写 ReductStore
        let blob_ref = reduct_writer.write_blob(
            &event.node_id,
            photo_bytes,
            event.timestamp,
            hashmap! { "event_type" => "photo_log" }
        ).await?;
        
        // WAL 只存 BlobRef
        graph.set_property(&event.node_id, "photo", PropertyValue::BlobRef(blob_ref)).await?;
    }
    Ok(())
}
```

---

### 步骤 4：异步复制器跳过 blob（1 天）

```rust
// nexora-zenoh/src/wal_replicator.rs
async fn replicate_event(event: &WalRecord) -> Result<()> {
    match extract_property_value(&event.operation) {
        Some(PropertyValue::BlobRef(blob_ref)) => {
            // Blob 已在 ReductStore，只复制元数据
            reduct.write(
                &event_entry_name(&event),
                serde_json::to_vec(&BlobRefMetadata {
                    event_type: "blob_ref",
                    blob_ref: blob_ref.clone(),
                })?,
                event.seq_no
            ).await?;
        },
        Some(value) => {
            // 正常事件，全量复制
            reduct.write(...).await?;
        },
        None => {}
    }
    Ok(())
}
```

---

## 六、端到端示例

### 场景：叉车拍照事件

**Kafka 事件**：
```json
{
  "event_type": "photo_log",
  "forklift_id": "F002",
  "engineer_id": "E001",
  "timestamp": 1721305800000000,
  "photo": "<base64 encoded JPEG, 2MB>",
  "location": "yard"
}
```

---

**处理流程**：
```rust
// 1. 解码 base64
let photo_bytes = base64::decode(event.photo)?;

// 2. 直写 ReductStore
let blob_ref = reduct.write_blob(
    entry: "forklift_F002",
    data: photo_bytes,  // 2MB
    timestamp: event.timestamp,
    labels: {
        "event_type": "photo_log",
        "engineer_id": "E001",
        "location": "yard"
    }
).await?;
// blob_ref = BlobRef {
//   bucket: "air_cargo",
//   entry: "forklift_F002",
//   timestamp_us: 1721305800000000,
//   size: 2097152,
//   content_type: "image/jpeg"
// }

// 3. WAL 存 BlobRef（几十字节）
graph.set_property(
    "F002",
    "last_photo",
    PropertyValue::BlobRef(blob_ref.clone())
).await?;

// 4. WAL 写入的 WalRecord
WalRecord {
    seq_no: 123456,
    operation: WalOperation::NodeEvent {
        qid: "F002",
        event: TimedEvent {
            event: NodeChangeEvent::PropertySet {
                key: "last_photo",
                value: PropertyValue::BlobRef(blob_ref),  // ← 只有引用
            },
            time: 1721305800000000
        }
    }
}
// WAL 文件大小增加：~100 字节（不是 2MB）
```

---

**查询**：
```cypher
// Cypher 查询
MATCH (f:Forklift {id: 'F002'})
RETURN f.last_photo AS photo_ref

// 返回 BlobRef
{
  "bucket": "air_cargo",
  "entry": "forklift_F002",
  "timestamp_us": 1721305800000000,
  "size": 2097152,
  "content_type": "image/jpeg"
}

// 应用层下载图片
GET http://reductstore:8383/api/v1/b/air_cargo/forklift_F002?ts=1721305800000000
→ 返回 JPEG bytes（流式，不占内存）
```

---

**时序查询**：
```python
# 查询"叉车 F002 过去 7 天的所有拍照记录"
async for record in reduct.query(
    entry='forklift_F002',
    start=now() - timedelta(days=7),
    stop=now(),
    include={'event_type': 'photo_log'}
):
    photo_bytes = await record.read_all()
    engineer_id = record.labels['engineer_id']
    # 展示：timestamp, engineer_id, 图片
```

---

## 七、总结

### 核心回答

**✅ 是的，WAL + ReductStore 异步复制方案天然支持非结构化数据**

**三个关键点**：

1. **ReductStore 原生支持 BLOB**
   - Time Series Blob Database（设计目标）
   - 压缩、分块、流式读取

2. **直写模式避免 WAL 膨胀**
   - 大 blob 直接写 ReductStore（绕过 WAL）
   - WAL 只存 BlobRef（几十字节）
   - 异步复制时，blob 已在 ReductStore

3. **异步复制器作为智能网关**
   - 检测事件类型，分流处理
   - 结构化 → JSON 复制
   - BlobRef → 元数据复制
   - Bytes → 提取到独立 blob entry

---

### 实施路径

| 步骤 | 内容 | 工作量 |
|------|------|--------|
| **1** | 添加 PropertyValue::BlobRef | 1 周（Track B8） |
| **2** | ReductStore 直写工具 | 1 周 |
| **3** | 事件摄入识别 blob | 1 周 |
| **4** | 异步复制器跳过 blob | 1 天 |
| **总计** | | **3-4 周** |

---

### 最终架构

```
Kafka 事件流
    │
    ├─ 结构化事件（battery_level）
    │    └→ WAL → Actor → Snapshot
    │           └→ 异步复制 → ReductStore
    │
    └─ 非结构化事件（photo_log）
         └→ ReductStore（直写 blob）
              └→ 生成 BlobRef
                   └→ WAL（存引用）
                        └→ Actor（BlobRef property）
                             └→ 异步复制（元数据）
```

**优势**：
- ✅ WAL 轻量（blob 不在 WAL 中）
- ✅ 崩溃恢复快（replay 不涉及 blob）
- ✅ 时序查询强（ReductStore 原生优化）
- ✅ 无重复存储（blob 只在 ReductStore）
- ✅ 架构统一（单一事件流入口）

---

**一句话总结**：
> WAL + ReductStore 异步复制不仅能支持非结构化数据，而且通过"直写大 blob + WAL 存引用"的模式，比传统方案更优雅——既避免了 WAL 膨胀，又复用了 ReductStore 的原生 BLOB 能力，3-4 周即可实现。
