# 分布式 Event 写入架构设计

## 目标

支持两种模式：
1. **分布式写入到共享 S3**（AWS S3 / MinIO / SeaweedFS 等 S3 兼容存储）
2. **分布式写入到本地文件** + 跨节点同步（fallback 模式）

两种模式都确保数据一致性。

---

## 方案一：分布式写入到共享 S3 ✅ 推荐

### 架构

```
用户 → node-a → S3 Iceberg (共享)
                    ↓
                所有节点共享
                    ↑
用户 → node-b → S3 Iceberg (共享)
```

### 实现要点

#### 1. 所有节点配置相同的 S3 后端

```rust
// 每个节点启动时使用相同的 S3 配置
let event_config = StorageConfig::s3(
    "http://minio.cluster:9000",    // 共享 S3 endpoint
    "nexora-events",                 // 共享 bucket
    "us-east-1",
    "access_key",
    "secret_key",
    Some("prod".into()),
    true,  // path_style for MinIO
    "/data/catalog.db",  // 本地 SQLite catalog
);

let event_store = EventLogStore::new_with_config(event_config).await?;
```

#### 2. Iceberg 并发控制

**Iceberg 原生支持多 writer**：
- ✅ **Optimistic Concurrency**：基于 snapshot 版本
- ✅ **Atomic Commit**：metadata.json 原子更新
- ✅ **冲突检测**：commit 时检查版本冲突

```rust
// EventLogStore::append() 内部已实现
// iceberg-rust 的 Transaction::commit() 会:
// 1. 检查当前 metadata 版本
// 2. 如果版本不匹配 → 重试
// 3. 原子性更新 metadata.json
```

**并发写入流程**：
```
时刻 T0: metadata version = 10

node-a: 读取 v10 → 写数据文件 → commit (v10 → v11)
node-b: 读取 v10 → 写数据文件 → commit (v10 → v11) ← 冲突！

node-b 的 commit 检测到 metadata 已是 v11
→ 重新读取 v11 → 重试 commit (v11 → v12) ✅
```

#### 3. SQLite Catalog 的处理

**问题**：每个节点的 `catalog.db` 是本地的，如何同步？

**方案 A**：SQLite catalog 只是缓存
- Iceberg metadata 真相在 S3 (`metadata/*.json`)
- SQLite catalog 可以重建：`catalog.refresh()`

**方案 B**：共享 SQLite catalog
```rust
StorageConfig::s3(
    // ... S3 配置 ...
    "/shared/nfs/catalog.db",  // 共享文件系统上的 catalog
)
```

**推荐**：方案 A + 定期刷新
```rust
// 定期刷新 catalog（每次读取前）
if let Some(event_store) = &state.event_store {
    // Iceberg catalog 会从 S3 重新加载 metadata
    let table = event_store.load_table(table_name).await?;
}
```

#### 4. PG-wire 写入路径改造

**当前**：
```rust
// 只在 coordinator 写入
if let Some(event_store) = state.event_store.as_ref() {
    event_store.append(&events).await?;
}
```

**改造后**：保持不变！
```rust
// 每个节点都能写，因为后端是共享 S3
// 用户连接到哪个节点，就由哪个节点写入
if let Some(event_store) = state.event_store.as_ref() {
    event_store.append(&events).await?;  // 写到共享 S3
}
```

### 数据一致性保证

| 场景 | 保证机制 |
|-----|---------|
| **并发写入** | Iceberg optimistic concurrency + 重试 |
| **原子性** | Iceberg transaction (metadata.json 原子更新) |
| **跨节点读一致性** | 所有节点读同一份 S3 数据 |
| **故障恢复** | S3 持久化 + Iceberg snapshot 可回溯 |

### 配置示例

```toml
# nexora.toml
[event_store]
type = "s3"

# AWS S3
# endpoint = "https://s3.amazonaws.com"
# bucket = "nexora-events-prod"
# region = "us-west-2"

# MinIO
endpoint = "http://minio.cluster.local:9000"
bucket = "nexora-events"
region = "us-east-1"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true

# 本地 catalog 缓存
catalog_db_path = "/data/nexora/catalog.db"
```

---

## 方案二：分布式写入到本地文件 + 跨节点同步

### 架构

```
用户 → node-a → 本地 /data/events/*.parquet
         ↓
      同步到 node-b
         ↓
      node-b → 本地 /data/events/*.parquet
```

### 实现要点

#### 1. Event Replication Protocol

新增 `ReplicateEvent` 操作到 `GraphOperation`：

```rust
// crates/nexora-zenoh/src/lib.rs
pub enum GraphOperation {
    // ... 现有操作 ...
    
    /// 跨节点复制 event (本地文件模式)
    ReplicateEvent {
        topic: String,
        events: Vec<RawEvent>,
    },
}

pub enum GraphResult {
    // ... 现有结果 ...
    
    /// Event 复制确认
    EventReplicated { count: u64 },
}
```

#### 2. Event Replication Writer

类似 `ReplicaWriter`，实现 `EventReplicaWriter`：

```rust
// crates/nexora-eventlog/src/event_replica_writer.rs

/// 跨节点 event 复制 (仅本地文件模式)
pub struct EventReplicaWriter {
    client: Arc<TcpRemoteClient>,
    node_list: Vec<String>,
}

impl EventReplicaWriter {
    pub async fn replicate_events(
        &self,
        events: &[RawEvent],
    ) -> Result<()> {
        // 并行复制到所有其他节点
        let tasks: Vec<_> = self.node_list
            .iter()
            .map(|node_id| {
                let events = events.to_vec();
                let client = self.client.clone();
                let node_id = node_id.clone();
                async move {
                    let op = GraphOperation::ReplicateEvent {
                        topic: events[0].topic.clone(),
                        events,
                    };
                    client.execute(&node_id, op).await
                }
            })
            .collect();
        
        // 等待所有副本确认
        let results = futures::future::join_all(tasks).await;
        
        // 检查是否所有节点都成功
        for result in results {
            result.context("Event replication failed")?;
        }
        
        Ok(())
    }
}
```

#### 3. PG-wire 写入路径改造

```rust
// crates/nexora-pgwire/src/simple_query.rs

// INSERT 处理
if let Some(event_store) = state.event_store.as_ref() {
    // 1. 写入本地 event store
    event_store.append(&events).await?;
    
    // 2. 如果是本地文件模式 + 集群模式 → 复制到其他节点
    if state.event_replica_writer.is_some() {
        state.event_replica_writer
            .as_ref()
            .unwrap()
            .replicate_events(&events)
            .await?;
    }
}
```

#### 4. GraphServiceAdapter 处理 ReplicateEvent

```rust
// crates/nexora-zenoh/src/graph_service_adapter.rs

impl GraphServiceAdapter {
    pub async fn handle_operation(
        &self,
        op: GraphOperation,
    ) -> Result<GraphResult> {
        match op {
            // ... 现有操作 ...
            
            GraphOperation::ReplicateEvent { topic, events } => {
                // 写入本地 event store
                if let Some(event_store) = &self.event_store {
                    let count = event_store.append(&events).await?;
                    Ok(GraphResult::EventReplicated { count })
                } else {
                    Err(anyhow::anyhow!("Event store not configured"))
                }
            }
        }
    }
}
```

### 数据一致性保证

| 场景 | 保证机制 |
|-----|---------|
| **写入原子性** | 本地写入 + 同步复制（类似 2PC） |
| **跨节点一致性** | Quorum write（至少 N/2 + 1 节点确认） |
| **故障处理** | 复制失败 → 整个写入回滚 |
| **数据恢复** | 定期 reconciliation（比较各节点 snapshot） |

### 问题与限制

1. **写入延迟**：需要等待所有副本确认
2. **复杂度高**：需要实现复制协议 + 一致性检查
3. **网络依赖**：节点间网络问题影响写入
4. **数据冗余**：每个节点都存完整数据

---

## 推荐方案

### 优先级

1. **首选**：方案一（共享 S3）
   - ✅ 简单：无需跨节点同步
   - ✅ 可靠：S3 持久化 + Iceberg ACID
   - ✅ 可扩展：节点数增加不影响复杂度

2. **备选**：方案二（本地文件 + 同步）
   - ⚠️  仅在无法使用 S3 时考虑
   - ⚠️  实现复杂度高
   - ⚠️  维护成本高

### 自动降级策略

```rust
// 启动时检测 S3 可用性
let event_store = if s3_config.is_available().await {
    // 方案一：共享 S3
    EventLogStore::new_with_config(s3_config).await?
} else {
    // 方案二：本地文件 + 启用复制
    let local_store = EventLogStore::new(&local_path).await?;
    enable_event_replication = true;
    local_store
};
```

---

## 实现步骤

### Phase 1: 共享 S3 支持（已完成 90%）

1. ✅ `StorageConfig::S3` 已实现
2. ✅ `EventLogStore::new_with_config()` 已实现
3. ⚠️  需要验证：多节点并发写入 S3

**待办**：
```rust
// 测试：多节点并发写入同一个 S3 Iceberg 表
#[tokio::test]
async fn test_concurrent_s3_writes() {
    let s3_config = StorageConfig::s3(...);
    
    let store_a = EventLogStore::new_with_config(s3_config.clone()).await?;
    let store_b = EventLogStore::new_with_config(s3_config.clone()).await?;
    
    // 并发写入
    let (r1, r2) = tokio::join!(
        store_a.append(&events_a),
        store_b.append(&events_b),
    );
    
    assert!(r1.is_ok() && r2.is_ok());
    
    // 验证：两批数据都存在
    let batches = store_a.read_table_batches("test_topic").await?;
    assert_eq!(total_rows, events_a.len() + events_b.len());
}
```

### Phase 2: PG-wire 集成

1. 在 `PgAppState` 添加配置字段
2. 每个节点启动时配置相同的 S3
3. INSERT 时直接写入（无需改动）

### Phase 3: 本地文件降级（可选）

1. 实现 `EventReplicaWriter`
2. 添加 `ReplicateEvent` 操作
3. 实现 quorum write 逻辑

---

## 配置示例

```rust
// 启动参数
nexora --cluster \
  --event-store-type s3 \
  --event-store-s3-endpoint http://minio:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-region us-east-1 \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin

// 或者环境变量
export NEXORA_EVENT_STORE_TYPE=s3
export NEXORA_EVENT_STORE_S3_ENDPOINT=http://minio:9000
export NEXORA_EVENT_STORE_S3_BUCKET=nexora-events
```

---

## 总结

### 回答你的问题

1. **能否支持分布式写入到共享 S3？**
   - ✅ 可以，当前代码已支持 90%
   - ✅ Iceberg 原生支持多 writer
   - ✅ 只需配置所有节点使用相同的 S3

2. **支持 S3 兼容存储（MinIO/SeaweedFS）？**
   - ✅ 已支持，通过 `path_style` 参数

3. **S3 不存在时支持本地文件？**
   - ✅ 可以实现自动降级
   - ⚠️  需要额外的跨节点复制逻辑

4. **数据一致性保证？**
   - ✅ S3 模式：Iceberg ACID + optimistic concurrency
   - ⚠️  本地文件模式：需要实现 quorum write

**推荐**：优先使用共享 S3，简单可靠！
