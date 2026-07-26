# Nexora 2.0 存储架构

**版本**: 2.0  
**更新时间**: 2026-07-25

---

## 概述

Nexora 2.0 实现了**双层存储架构**，两个存储系统可以独立配置：

1. **图数据存储** (`--storage-backend`) - 分层热温冷存储
2. **事件日志存储** (`--event-store-backend`) - Apache Iceberg 事件表

---

## 1. 图数据存储 (Graph Storage)

### 1.1 配置参数

```bash
--storage-backend <TYPE>
```

**支持的类型**:
- `memory` (默认) - 纯内存存储，无持久化
- `local` - 本地文件系统分层存储
- `s3` - S3/MinIO 分层存储

### 1.2 分层存储架构 (Tiered Storage)

当使用 `local` 或 `s3` 时，启用三层存储：

```
┌─────────────────────────────────────┐
│   Hot Tier (RocksDB)                │  ← 活跃数据，毫秒级访问
│   - 最近访问的节点/边               │
│   - 默认保留策略: 1 小时            │
└─────────────────────────────────────┘
           ↓ (age out)
┌─────────────────────────────────────┐
│   Warm Tier (Local FS / S3)         │  ← 次活跃数据，秒级访问
│   - Parquet 列式存储                │
│   - 默认保留策略: 7 天              │
└─────────────────────────────────────┘
           ↓ (age out)
┌─────────────────────────────────────┐
│   Cold Tier (S3 Glacier)            │  ← 归档数据，分钟级访问
│   - 压缩 Parquet + 索引             │
│   - 无限期保留                      │
└─────────────────────────────────────┘
```

### 1.3 使用场景

| Backend | 适用场景 | 持久化 | 分布式 |
|---------|----------|--------|--------|
| `memory` | 开发测试、临时分析 | ❌ | ❌ |
| `local` | 单机部署、边缘计算 | ✅ | ❌ |
| `s3` | 生产环境、多节点集群 | ✅ | ✅ |

### 1.4 示例配置

```bash
# 本地分层存储
./nexora \
  --storage-backend local \
  --storage-path /data/nexora/tiered

# S3 分层存储
./nexora \
  --storage-backend s3 \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket nexora-graph-data \
  --s3-access-key $AWS_ACCESS_KEY_ID \
  --s3-secret-key $AWS_SECRET_ACCESS_KEY
```

---

## 2. 事件日志存储 (Event Log Storage)

### 2.1 配置参数

```bash
--event-store-backend <TYPE>
```

**支持的类型**:
- `local` (默认) - 本地文件系统 + SQLite catalog
- `s3` - S3 数据文件 + 本地 SQLite catalog
- `rest` - REST catalog (Lakekeeper) + S3 数据文件

### 2.2 架构模式

#### 模式 1: Local (单节点开发)

```
┌───────────────────────────────┐
│  Nexora Node                  │
│  ┌─────────────────────────┐  │
│  │ Event Log Store         │  │
│  │ ├─ Iceberg Tables       │  │
│  │ ├─ Parquet Files        │  │
│  │ │  └─ /data/events/*.parquet
│  │ └─ SQLite Catalog       │  │
│  │    └─ catalog.db (local)│  │
│  └─────────────────────────┘  │
└───────────────────────────────┘
```

**特点**:
- ✅ 零依赖，开箱即用
- ✅ ACID 事务保证
- ❌ 不支持多节点写入

#### 模式 2: S3 + Local Catalog (实验性多节点)

```
┌─────────────────┐  ┌─────────────────┐
│  Node 1         │  │  Node 2         │
│  ├─ SQLite      │  │  ├─ SQLite      │
│  │  catalog.db │  │  │  catalog.db   │
└──┼──────────────┘  └──┼──────────────┘
   │                    │
   └────────┬───────────┘
            ↓
   ┌─────────────────────┐
   │  S3 / MinIO         │
   │  ├─ events/         │
   │  │  ├─ *.parquet    │
   │  │  └─ metadata/    │
   └─────────────────────┘
```

**特点**:
- ✅ 数据文件集中存储
- ⚠️ 每个节点有独立 catalog，需要手动同步
- ⚠️ 适合读多写少场景

#### 模式 3: REST Catalog (生产推荐)

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Node 1    │  │   Node 2    │  │   Node 3    │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        ↓
            ┌──────────────────────┐
            │  REST Catalog Server │
            │  (Lakekeeper/Tabular)│
            │  ├─ PostgreSQL       │
            │  └─ Metadata Cache   │
            └──────────┬───────────┘
                       ↓
            ┌──────────────────────┐
            │  S3 / MinIO          │
            │  └─ events/*.parquet │
            └──────────────────────┘
```

**特点**:
- ✅ 多节点共享元数据
- ✅ ACID 并发写入 (Iceberg 乐观锁)
- ✅ 自动冲突解决
- ✅ 生产级别可靠性

### 2.3 示例配置

#### Local 模式
```bash
./nexora \
  --event-store-backend local \
  --event-store-path /data/nexora/events
```

#### S3 模式
```bash
./nexora \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin
```

#### REST Catalog 模式
```bash
# 启动 Lakekeeper catalog 服务
docker run -d -p 8181:8181 \
  lakekeeper/lakekeeper \
  --postgres-url postgresql://user:pass@localhost/catalog

# 启动 Nexora 节点
./nexora \
  --event-store-backend rest \
  --event-store-rest-uri http://localhost:8181/catalog \
  --event-store-rest-warehouse nexora \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events
```

---

## 3. 组合配置示例

### 3.1 开发环境 (最小配置)

```bash
./nexora --host 127.0.0.1 --port 8080
```

**存储配置**:
- 图数据: `memory` (默认)
- 事件日志: `local` (默认)

### 3.2 单机生产 (本地持久化)

```bash
./nexora \
  --storage-backend local \
  --storage-path /data/nexora/graph \
  --event-store-backend local \
  --event-store-path /data/nexora/events
```

**存储配置**:
- 图数据: 本地分层存储
- 事件日志: 本地 Iceberg

### 3.3 多节点生产 (S3 + REST Catalog)

```bash
./nexora \
  --storage-backend s3 \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket nexora-graph \
  --event-store-backend rest \
  --event-store-rest-uri http://lakekeeper:8181/catalog \
  --event-store-rest-warehouse nexora-prod \
  --cluster \
  --node-id node-1 \
  --seed-nodes node-1:7001,node-2:7001,node-3:7001
```

**存储配置**:
- 图数据: S3 分层存储 (hot/warm/cold)
- 事件日志: REST catalog + S3 (多节点共享)

---

## 4. S3 参数 Fallback 机制

Nexora 实现了智能参数 fallback，避免重复配置：

```
优先级 1: event-store-s3-* (事件日志专用)
    ↓
优先级 2: s3-* (图存储和事件日志共享)
    ↓
优先级 3: AWS_* 环境变量 (标准 AWS 配置)
```

**示例**: 共享 S3 配置

```bash
# 只配置一次 S3，两个系统都使用
./nexora \
  --storage-backend s3 \
  --event-store-backend s3 \
  --s3-endpoint http://localhost:9000 \
  --s3-bucket nexora-data \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

**示例**: 分离配置不同 S3 存储

```bash
# 图数据存储在生产 S3
# 事件日志存储在专用 MinIO
./nexora \
  --storage-backend s3 \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket nexora-prod-graph \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://minio:9000 \
  --event-store-s3-bucket nexora-events
```

---

## 5. 对比总结

### 5.1 两种存储系统对比

| 维度 | 图数据存储 | 事件日志存储 |
|------|-----------|------------|
| **用途** | 图节点/边的当前状态 | 不可变事件历史 |
| **数据格式** | RocksDB KV / Parquet | Iceberg Tables (Parquet) |
| **查询语言** | Cypher / SQL | SQL (DataFusion) |
| **更新方式** | 原地更新 | 仅追加写入 |
| **时间旅行** | ❌ | ✅ |
| **分布式写入** | ❌ (单 leader) | ✅ (乐观并发) |
| **典型大小** | GB - TB | TB - PB |

### 5.2 部署模式推荐

| 场景 | 图数据存储 | 事件日志存储 | 说明 |
|------|-----------|------------|------|
| **本地开发** | `memory` | `local` | 快速启动，无持久化 |
| **单机部署** | `local` | `local` | 完整功能，本地持久化 |
| **边缘计算** | `local` | `s3` | 本地计算，云端归档 |
| **生产集群** | `s3` | `rest` | 完整分布式，多节点共享 |
| **只读副本** | `s3` | `rest` | 多个读节点共享数据 |

### 5.3 迁移路径

#### 从单机到集群

```bash
# 步骤 1: 单机模式导出数据
./nexora --storage-backend local
curl -X POST http://localhost:8080/api/admin/backup \
  -d '{"path": "/backup/nexora.tar.gz"}'

# 步骤 2: 部署 Lakekeeper catalog
docker-compose -f deploy/lakekeeper.yml up -d

# 步骤 3: 启动多节点集群
for node in node-{1..3}; do
  ./nexora \
    --storage-backend s3 \
    --event-store-backend rest \
    --node-id $node \
    --cluster
done

# 步骤 4: 恢复数据
curl -X POST http://node-1:8080/api/admin/restore \
  -d '{"path": "/backup/nexora.tar.gz"}'
```

---

## 6. 常见问题

### Q1: 是否必须同时使用两种存储？

**A**: 不是。两个系统可以独立配置：
- 只需图数据存储: 适合纯图查询场景
- 只需事件日志: 适合流式处理 + 批量查询
- 两者结合: 获得完整的图查询 + 时间旅行能力

### Q2: event-first feature 影响哪些功能？

**A**: 编译时需要 `--features event-first` 才能启用：
- `--event-store-backend` 参数
- Apache Iceberg 集成
- DataFusion SQL 查询引擎
- 时间旅行查询
- 分布式并发写入

默认编译**不包含**这些功能，只有基础的 RocksDB 图存储。

### Q3: 如何选择 S3 vs REST catalog？

**A**: 
- **S3 模式**: 单节点写入 + 多节点读取，catalog 在每个节点本地
- **REST 模式**: 多节点并发写入，catalog 集中管理，推荐生产使用

### Q4: 分层存储会自动触发吗？

**A**: 是的。当使用 `--storage-backend local/s3` 时：
- Hot → Warm: 根据 LRU 策略自动降级
- Warm → Cold: 根据时间策略 (默认 7 天) 自动归档
- 可通过 `--tier-hot-ttl` 和 `--tier-warm-ttl` 调整

### Q5: 如何监控存储使用情况？

**A**: 通过 Metrics 端点：

```bash
curl http://localhost:8080/api/metrics | jq '{
  hot_size: .storage_hot_bytes,
  warm_size: .storage_warm_bytes,
  cold_size: .storage_cold_bytes,
  event_log_size: .event_log_bytes,
  event_count: .total_events
}'
```

---

## 7. 性能调优

### 7.1 图数据存储优化

```bash
# 增大 RocksDB 缓存 (hot tier)
--rocksdb-cache-size 8GB

# 调整 hot tier 保留时间
--tier-hot-ttl 3600  # 1 小时

# 启用 Bloom filter
--rocksdb-bloom-bits 10
```

### 7.2 事件日志优化

```bash
# 增大 Parquet 行组大小
--event-log-row-group-size 100000

# 启用 Z-order 排序
--event-log-sort-order timestamp,node_id

# 配置压缩算法
--event-log-compression zstd
```

---

## 8. 相关文档

- [事件日志设计](./DISTRIBUTED_EVENT_WRITE_DESIGN.md)
- [分层存储实现](./TIERED_STORAGE_DESIGN.md)
- [Iceberg 集成指南](../EVENT_STORE_DEPLOYMENT_QUICK_START.md)
- [集群部署](../cluster-ops.md)
