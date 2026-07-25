# 分布式 Event 写入方案总结

## 你的问题

1. 能否支持分布式写入到共享 S3？
2. 支持 AWS S3 或兼容 S3（MinIO/SeaweedFS）？
3. S3 不存在时，能否分布式写入到本地文件？
4. 两种方案都能保证数据一致性吗？

## 答案

### ✅ 1. 分布式写入到共享 S3（强烈推荐）

**可以！而且当前代码已经支持 90%。**

#### 工作原理

```
所有节点共享同一个 S3 Iceberg 表：

用户 A → node-a → S3 (s3://bucket/events/)
                    ↓
                共享存储
                    ↑
用户 B → node-b → S3 (s3://bucket/events/)
```

#### 配置方法

```rust
// 每个节点使用相同的配置
let config = StorageConfig::s3(
    "http://minio.cluster:9000",  // 共享 endpoint
    "nexora-events",               // 共享 bucket
    "us-east-1",
    "access_key",
    "secret_key",
    Some("prod".into()),
    true,  // path_style (MinIO 必须 true)
    "/data/catalog.db",  // 本地 catalog 缓存
);

let event_store = EventLogStore::new_with_config(config).await?;
```

#### 数据一致性保证

| 问题 | 解决方案 |
|-----|---------|
| **并发写入冲突** | Iceberg optimistic concurrency（自动重试） |
| **原子性** | Iceberg transaction（metadata.json 原子更新） |
| **跨节点读一致性** | 所有节点读同一份 S3 数据 |
| **持久化** | S3 的 99.999999999% 持久性 |

#### 并发写入示例

```
时刻 T0: table snapshot = v10

node-a: 读 v10 → 写数据文件 a.parquet → commit (v10→v11) ✅
node-b: 读 v10 → 写数据文件 b.parquet → commit (v10→v11) ❌ 冲突
        ↓
        重新读 v11 → 重试 commit (v11→v12) ✅

最终: v12 包含 a.parquet + b.parquet ✅
```

**Iceberg 自动处理冲突，无需额外代码！**

---

### ✅ 2. 支持 S3 兼容存储

**已支持！通过 `path_style` 参数。**

#### AWS S3

```rust
StorageConfig::s3(
    "https://s3.amazonaws.com",
    "my-bucket",
    "us-west-2",
    "AKIA...",
    "secret...",
    None,
    false,  // AWS S3 用 virtual-hosted style
    "/data/catalog.db",
)
```

#### MinIO

```rust
StorageConfig::s3(
    "http://minio.local:9000",
    "nexora-events",
    "us-east-1",
    "minioadmin",
    "minioadmin",
    None,
    true,  // MinIO 必须用 path-style
    "/data/catalog.db",
)
```

#### SeaweedFS

```rust
StorageConfig::s3(
    "http://seaweedfs:8333",
    "nexora-events",
    "us-east-1",
    "admin",
    "admin",
    None,
    true,  // SeaweedFS 也用 path-style
    "/data/catalog.db",
)
```

---

### ⚠️ 3. 本地文件模式（需要额外开发）

**可以，但需要实现跨节点同步机制。**

#### 方案：Event Replication

```
用户 → node-a → 写本地 /data/events/
         ↓
      复制到 node-b
         ↓
      node-b → 写本地 /data/events/
```

#### 需要实现的组件

1. **EventReplicaWriter**（类似 ReplicaWriter）
```rust
pub struct EventReplicaWriter {
    client: Arc<TcpRemoteClient>,
    replicas: Vec<String>,  // 其他节点列表
}

impl EventReplicaWriter {
    async fn replicate(&self, events: &[RawEvent]) -> Result<()> {
        // 并行复制到所有节点
        let tasks = self.replicas.iter().map(|node| {
            self.client.execute(node, ReplicateEvent { events })
        });
        futures::join_all(tasks).await;
        Ok(())
    }
}
```

2. **写入流程**
```rust
// 1. 写本地
event_store.append(&events).await?;

// 2. 复制到其他节点（如果配置了）
if let Some(replicator) = &state.event_replicator {
    replicator.replicate(&events).await?;
}
```

3. **GraphOperation 新增**
```rust
pub enum GraphOperation {
    // ... 现有操作 ...
    ReplicateEvent { events: Vec<RawEvent> },
}
```

#### 数据一致性保证

需要实现 **Quorum Write**：
- 至少 N/2 + 1 个节点确认才算成功
- 类似 Raft/Paxos 的多数派写入
- 复杂度远高于 S3 方案

---

### ✅ 4. 数据一致性对比

| 方案 | 一致性保证 | 实现复杂度 | 推荐度 |
|-----|-----------|-----------|-------|
| **共享 S3** | ✅ Iceberg ACID<br>✅ 自动冲突解决<br>✅ 原子提交 | ⭐ 低（已实现 90%） | ⭐⭐⭐⭐⭐ |
| **本地文件 + 复制** | ⚠️ 需实现 Quorum<br>⚠️ 需实现冲突解决<br>⚠️ 需实现 reconciliation | ⭐⭐⭐⭐ 高 | ⭐⭐ |

---

## 推荐方案

### 🥇 首选：共享 S3

**理由**：
1. ✅ 简单：无需跨节点同步
2. ✅ 可靠：Iceberg ACID + S3 持久化
3. ✅ 可扩展：节点增加不影响复杂度
4. ✅ 已实现 90%：只需验证并发场景

**配置示例**：
```bash
# 启动参数
nexora --cluster \
  --event-store-type s3 \
  --event-store-endpoint http://minio:9000 \
  --event-store-bucket nexora-events \
  --event-store-access-key minioadmin \
  --event-store-secret-key minioadmin
```

### 🥈 备选：本地文件 + 复制

**仅在以下情况考虑**：
- ❌ 完全无法使用 S3（网络隔离环境）
- ❌ 无法使用 MinIO/SeaweedFS 等开源 S3

**代价**：
- 需要实现完整的复制协议
- 需要处理网络分区和脑裂
- 维护成本显著增加

---

## 当前状态

### 已实现（90%）

1. ✅ `StorageConfig::S3` - S3 配置
2. ✅ `EventLogStore::new_with_config()` - 支持 S3 后端
3. ✅ Iceberg 并发写入支持（iceberg-rust 库自带）

### 需要补充（10%）

1. **测试多节点并发写入 S3**
```rust
#[tokio::test]
async fn test_concurrent_s3_writes() {
    // 两个节点同时写同一个表
    let (r1, r2) = tokio::join!(
        node_a_store.append(&events_a),
        node_b_store.append(&events_b),
    );
    
    // 验证：无数据丢失
    assert_eq!(total_rows, events_a.len() + events_b.len());
}
```

2. **添加启动参数**
```rust
// 支持 --event-store-type=s3 命令行参数
// 支持环境变量配置
```

3. **文档和示例**
```markdown
# 如何配置 MinIO
# 如何配置 SeaweedFS
# 如何验证并发写入
```

---

## 总结

### 回答你的问题

1. **能否支持分布式写入到共享 S3？**
   - ✅ 可以！代码已支持 90%
   - ✅ 每个节点配置相同的 S3，直接写入即可
   - ✅ Iceberg 自动处理并发冲突

2. **支持 AWS S3 或兼容 S3？**
   - ✅ AWS S3：设置 `path_style = false`
   - ✅ MinIO：设置 `path_style = true`
   - ✅ SeaweedFS：设置 `path_style = true`

3. **S3 不存在时，能否分布式写入到本地文件？**
   - ⚠️ 可以，但需要额外开发跨节点复制
   - ⚠️ 复杂度高，不推荐
   - ✅ 建议：使用开源 MinIO 替代本地文件

4. **两种方案都能保证数据一致性？**
   - ✅ S3 方案：Iceberg ACID 保证一致性
   - ⚠️ 本地方案：需要实现 Quorum Write 保证一致性

### 推荐行动

1. **立即可用**：配置共享 S3（MinIO 最简单）
2. **短期补充**：测试并发写入场景
3. **长期可选**：实现本地文件复制（如果真的需要）

---

**详细设计文档**：`docs/architecture/DISTRIBUTED_EVENT_WRITE_DESIGN.md`
