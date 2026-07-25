# Event Store S3 配置指南

## 概述

Nexora 的 Event Store 现在支持两种存储后端：
1. **本地文件系统**（默认）：适用于单节点或共享 NFS
2. **S3 兼容存储**：适用于多节点分布式部署（推荐生产环境）

## 命令行参数

### 通用参数

```bash
--event-store-backend <local|s3>
  # 选择存储后端，默认: local

--event-store-catalog-path <PATH>
  # Iceberg catalog 数据库路径（本地 SQLite）
  # 默认: ./nexora-data/event_catalog.db
  # 注意：即使在 S3 模式下，catalog 也存储在本地
```

### 本地文件系统模式

```bash
--event-store-dir <PATH>
  # Event 数据存储目录
  # 默认: ./nexora-data/events
```

**示例：启动单节点**
```bash
nexora-app \
  --event-store-backend local \
  --event-store-dir ./data/events
```

### S3 模式

```bash
--event-store-s3-endpoint <URL>
  # S3 endpoint URL（必需）
  # AWS S3: https://s3.<region>.amazonaws.com
  # MinIO: http://localhost:9000

--event-store-s3-bucket <BUCKET>
  # S3 bucket 名称（必需）

--event-store-s3-region <REGION>
  # AWS region，默认: us-east-1

--event-store-s3-access-key <KEY>
  # S3 access key
  # 环境变量: AWS_ACCESS_KEY_ID

--event-store-s3-secret-key <KEY>
  # S3 secret key
  # 环境变量: AWS_SECRET_ACCESS_KEY

--event-store-s3-prefix <PREFIX>
  # S3 对象前缀（可选）
  # 用于多环境隔离，例如: prod, staging, dev

--event-store-s3-path-style
  # 使用 path-style S3 地址（MinIO 必需）
```

## 部署场景

### 场景 1：单节点开发环境（本地文件）

```bash
nexora-app \
  --event-store-backend local \
  --event-store-dir ./nexora-data/events
```

**优点**：
- 简单，无需额外服务
- 低延迟

**缺点**：
- 无法跨节点共享数据
- 单点故障

---

### 场景 2：多节点集群 + MinIO（推荐）

#### 步骤 1：启动 MinIO

```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  --name minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -v /data/minio:/data \
  minio/minio server /data --console-address ":9001"
```

访问 MinIO Console: http://localhost:9001

#### 步骤 2：创建 Bucket

```bash
# 使用 mc (MinIO Client)
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/nexora-events
```

或者通过 Web Console 创建。

#### 步骤 3：启动多个 Nexora 节点

**Node A:**
```bash
nexora-app \
  --node-id node-a \
  --cluster \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-region us-east-1 \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin \
  --event-store-s3-path-style \
  --event-store-catalog-path ./data/node-a/catalog.db
```

**Node B:**
```bash
nexora-app \
  --node-id node-b \
  --cluster \
  --peer node-a@localhost:7000@localhost:7001 \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-region us-east-1 \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin \
  --event-store-s3-path-style \
  --event-store-catalog-path ./data/node-b/catalog.db
```

**优点**：
- ✅ 所有节点共享同一个 event 数据源
- ✅ 任何节点都能读写
- ✅ Iceberg 自动处理并发冲突
- ✅ 数据持久化到对象存储
- ✅ MinIO 开源免费

---

### 场景 3：生产环境 + AWS S3

```bash
nexora-app \
  --node-id prod-node-1 \
  --cluster \
  --event-store-backend s3 \
  --event-store-s3-endpoint https://s3.us-west-2.amazonaws.com \
  --event-store-s3-bucket nexora-prod-events \
  --event-store-s3-region us-west-2 \
  --event-store-s3-prefix prod \
  --event-store-catalog-path /var/nexora/catalog.db
```

**环境变量方式（推荐）：**
```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

nexora-app \
  --node-id prod-node-1 \
  --cluster \
  --event-store-backend s3 \
  --event-store-s3-endpoint https://s3.us-west-2.amazonaws.com \
  --event-store-s3-bucket nexora-prod-events \
  --event-store-s3-region us-west-2 \
  --event-store-s3-prefix prod
```

---

### 场景 4：SeaweedFS（开源 S3 兼容存储）

```bash
# 启动 SeaweedFS
weed server -s3 -s3.port=8333

# 启动 Nexora
nexora-app \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:8333 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-region us-east-1 \
  --event-store-s3-access-key any \
  --event-store-s3-secret-key any \
  --event-store-s3-path-style
```

---

## 数据一致性保证

### S3 模式（推荐）

**并发写入处理**：
- ✅ Iceberg 使用 **Optimistic Concurrency Control**
- ✅ 每次写入生成新的 metadata 文件
- ✅ Commit 时检测冲突，自动重试
- ✅ 最终一致性保证

**示例：两个节点同时写入**
```
Time    Node A              Node B              S3 Iceberg
t0      append(10 rows)     append(10 rows)     
t1      commit v1          commit v2 (检测到 v1)
t2      ✅ success          🔄 retry (base on v1)
t3                          ✅ success (v2)
```

**结果**：两个节点的数据都被保留，总共 20 行。

### 本地文件模式

**限制**：
- ⚠️ 两个节点并发创建同一个表会失败（UNIQUE 约束冲突）
- ✅ 表创建后，并发写入可以工作（Iceberg 冲突解决）

**最佳实践**：
- 使用共享 NFS 挂载
- 通过协调机制确保只有一个节点创建表
- 或者，使用 S3 模式避免此问题

---

## 测试验证

### 本地文件系统并发写入测试

```bash
cargo test --package nexora-eventlog --features olap \
  --test concurrent_s3_writes_test \
  test_local_fs_concurrent_writes -- --nocapture
```

**输出**：
```
✅ Local FS concurrent write test passed
test concurrent_s3_writes::test_local_fs_concurrent_writes ... ok
```

### S3 并发写入测试（需要 MinIO）

```bash
# 启动 MinIO
docker run -p 9000:9000 minio/minio server /data

# 创建 bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/nexora-test-events

# 运行测试
cargo test --package nexora-eventlog --features olap \
  --test concurrent_s3_writes_test \
  test_two_nodes_concurrent_writes -- --nocapture --ignored
```

---

## 性能考虑

### 写入延迟

| 后端 | 典型延迟 | 吞吐量 |
|-----|---------|--------|
| 本地文件 | 5-20ms | 高 |
| MinIO (本地) | 10-30ms | 中 |
| AWS S3 | 50-200ms | 中 |

### 优化建议

1. **批量写入**：每次 append 尽量包含多行（100-1000 行）
2. **S3 Endpoint**：使用 VPC Endpoint 减少延迟
3. **Region**：选择地理上接近的 region
4. **Catalog 本地化**：catalog 始终存储在本地磁盘（快速查询）

---

## 故障处理

### 问题 1：S3 连接失败

**错误**：
```
Failed to create EventLogStore: Failed to connect to S3
```

**排查**：
```bash
# 检查 endpoint 可达性
curl http://localhost:9000

# 检查凭证
aws s3 ls s3://nexora-events --endpoint-url http://localhost:9000

# 检查 bucket 存在
mc ls local/nexora-events
```

### 问题 2：并发创建表冲突（本地模式）

**错误**：
```
UNIQUE constraint failed: iceberg_tables.catalog_name, iceberg_tables.table_namespace, iceberg_tables.table_name
```

**解决方案**：
1. 切换到 S3 模式（推荐）
2. 或者，通过协调确保表在第一个节点启动时创建

### 问题 3：Catalog 权限错误

**错误**：
```
Failed to open catalog database: Permission denied
```

**解决方案**：
```bash
# 确保目录存在且可写
mkdir -p ./nexora-data
chmod 755 ./nexora-data
```

---

## 迁移指南

### 从本地文件迁移到 S3

**步骤 1：导出数据**
```bash
# TODO: 等待 nexora-cli export 命令
```

**步骤 2：配置 S3 并启动**
```bash
nexora-app --event-store-backend s3 ...
```

**步骤 3：导入数据**
```bash
# TODO: 等待 nexora-cli import 命令
```

---

## 监控指标

推荐监控以下指标：

1. **Event 写入延迟**：`event_append_duration_ms`
2. **S3 API 调用次数**：CloudWatch (AWS) 或 MinIO metrics
3. **Iceberg commit 冲突率**：日志中的 "retry" 关键字
4. **Catalog 数据库大小**：磁盘占用

---

## 总结

| 特性 | 本地文件 | S3 (MinIO/AWS) |
|-----|---------|---------------|
| **多节点写入** | ⚠️ 有限支持 | ✅ 完全支持 |
| **数据一致性** | ✅ Iceberg ACID | ✅ Iceberg ACID |
| **并发冲突处理** | ✅ 自动重试 | ✅ 自动重试 |
| **部署复杂度** | 低 | 中 |
| **生产推荐度** | ⚠️ 单节点可用 | ✅ 多节点推荐 |

**推荐配置**：
- 开发环境：本地文件系统
- 生产环境：S3 (MinIO 或 AWS S3)
