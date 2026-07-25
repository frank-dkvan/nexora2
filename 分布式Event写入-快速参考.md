# 分布式 Event 写入 - 快速参考

## TL;DR

✅ **推荐方案**：所有节点共享 S3（MinIO/SeaweedFS/AWS S3）  
⚠️  **备选方案**：本地文件 + 跨节点复制（需要额外开发）

---

## 方案对比

| 维度 | 共享 S3 | 本地文件 + 复制 |
|-----|---------|----------------|
| **实现状态** | ✅ 90% 完成 | ❌ 需要开发 |
| **配置复杂度** | ⭐ 简单 | ⭐⭐⭐⭐ 复杂 |
| **数据一致性** | ✅ Iceberg ACID | ⚠️ 需实现 Quorum |
| **故障处理** | ✅ S3 自动冗余 | ⚠️ 需实现 failover |
| **扩展性** | ✅ 节点随意增减 | ⚠️ 复制复杂度增加 |
| **写入延迟** | ⭐⭐⭐ 中等（S3 网络） | ⭐⭐⭐⭐ 高（同步复制） |
| **成本** | 💰 S3 存储费用 | 💾 本地磁盘 |

---

## 快速配置：共享 S3

### 1. MinIO（最简单）

```bash
# 启动 MinIO
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# 启动 Nexora 节点（所有节点相同配置）
nexora --cluster \
  --node-id node-a \
  --event-store-type s3 \
  --event-store-endpoint http://minio:9000 \
  --event-store-bucket nexora-events \
  --event-store-access-key minioadmin \
  --event-store-secret-key minioadmin \
  --event-store-path-style true
```

### 2. SeaweedFS

```bash
# 启动 SeaweedFS S3
weed server -s3 -s3.port=8333

# Nexora 配置（与 MinIO 类似）
nexora --cluster \
  --event-store-endpoint http://seaweedfs:8333 \
  --event-store-path-style true
```

### 3. AWS S3

```bash
# Nexora 配置
nexora --cluster \
  --event-store-type s3 \
  --event-store-endpoint https://s3.amazonaws.com \
  --event-store-bucket my-nexora-events \
  --event-store-region us-west-2 \
  --event-store-access-key AKIA... \
  --event-store-secret-key ... \
  --event-store-path-style false  # AWS 用 virtual-hosted
```

---

## 工作原理

### 共享 S3 模式

```
┌─────────┐
│ 用户 A  │ → psql → node-a → S3 (s3://bucket/events/)
└─────────┘                      ↓
                            Iceberg 表
┌─────────┐                      ↑
│ 用户 B  │ → psql → node-b → S3 (s3://bucket/events/)
└─────────┘
```

**关键点**：
- ✅ 所有节点读写同一个 S3 bucket
- ✅ Iceberg 自动处理并发冲突
- ✅ 无需跨节点通信

### 并发写入处理

```
时刻 T0: table metadata version = 10

┌─ node-a ────────────────────────────┐
│ 1. 读取 v10                          │
│ 2. 写 data_a.parquet 到 S3          │
│ 3. commit: v10 → v11 ✅             │
└─────────────────────────────────────┘

┌─ node-b ────────────────────────────┐
│ 1. 读取 v10                          │
│ 2. 写 data_b.parquet 到 S3          │
│ 3. commit: v10 → v11 ❌ 冲突！       │
│ 4. 重新读取 v11                      │
│ 5. 重试 commit: v11 → v12 ✅        │
└─────────────────────────────────────┘

最终: v12 包含 data_a + data_b
```

**Iceberg 自动重试，无需应用代码处理！**

---

## 代码示例

### 配置 S3

```rust
use nexora_eventlog::{EventLogStore, StorageConfig};

// 所有节点使用相同配置
let config = StorageConfig::s3(
    "http://minio:9000",       // endpoint
    "nexora-events",           // bucket
    "us-east-1",               // region
    "minioadmin",              // access_key
    "minioadmin",              // secret_key
    Some("prod".into()),       // prefix (可选)
    true,                      // path_style (MinIO=true)
    "/data/catalog.db",        // 本地 catalog 缓存
);

let event_store = EventLogStore::new_with_config(config).await?;
```

### 写入（每个节点都能写）

```rust
// INSERT 处理（无需改动）
if let Some(event_store) = state.event_store.as_ref() {
    event_store.append(&events).await?;  // 直接写 S3
}
```

### 读取（跨节点）

```sql
-- 任何节点都能查询完整数据
SELECT COUNT(*) FROM user_events;
```

---

## 常见问题

### Q1: SQLite catalog 如何同步？

**A**: 不需要同步！
- SQLite catalog 只是本地缓存
- Iceberg metadata 真相在 S3 (`metadata/*.json`)
- 每次读取时从 S3 刷新即可

### Q2: 并发写入会冲突吗？

**A**: 会，但自动解决！
- Iceberg 检测到版本冲突 → 自动重试
- 应用无感知，透明处理

### Q3: 需要配置什么？

**A**: 只需所有节点使用相同的：
- ✅ S3 endpoint
- ✅ Bucket 名称
- ✅ 访问密钥

本地的 `catalog.db` 路径可以不同。

### Q4: 性能如何？

**A**: 
- **写入**：受 S3 网络延迟影响（通常 10-50ms）
- **读取**：与单节点相同（DataFusion 查询）
- **并发**：冲突时重试，轻微影响

### Q5: 需要开发什么？

**A**: 几乎不需要！
- ✅ StorageConfig 已实现
- ✅ EventLogStore 已支持 S3
- ⚠️ 需要添加启动参数解析
- ⚠️ 需要测试并发场景

---

## 验证清单

在生产环境使用前，测试以下场景：

- [ ] 单节点写入 → 其他节点能读取
- [ ] 两节点并发写入 → 无数据丢失
- [ ] 三节点并发写入 → 性能可接受
- [ ] 节点重启 → 数据持久化
- [ ] S3 临时不可用 → 写入失败但不丢数据
- [ ] 大批量写入（10K+ events）→ 性能可接受

---

## 下一步

1. **测试**：编写并发写入测试
2. **配置**：添加命令行参数支持
3. **文档**：补充部署文档
4. **监控**：添加 S3 操作指标

---

**详细文档**：
- 设计文档：`docs/architecture/DISTRIBUTED_EVENT_WRITE_DESIGN.md`
- 方案对比：`分布式Event写入方案.md`
