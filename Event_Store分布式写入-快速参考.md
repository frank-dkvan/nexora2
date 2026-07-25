# Event Store 分布式写入 - 快速参考

## 🚀 快速开始

### 本地模式（开发）
```bash
nexora-app --event-store-backend local
```

### S3 模式 + MinIO（生产推荐）
```bash
# 1. 启动 MinIO
docker run -d -p 9000:9000 -p 9001:9001 \
  --name minio \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# 2. 创建 bucket
aws s3 mb s3://nexora-events \
  --endpoint-url http://localhost:9000

# 3. 启动 Nexora 节点
nexora-app \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin \
  --event-store-s3-path-style
```

### AWS S3 模式
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

nexora-app \
  --event-store-backend s3 \
  --event-store-s3-endpoint https://s3.us-west-2.amazonaws.com \
  --event-store-s3-bucket nexora-prod \
  --event-store-s3-region us-west-2
```

---

## 📋 命令行参数速查

| 参数 | 说明 | 默认值 |
|-----|------|--------|
| `--event-store-backend` | `local` 或 `s3` | `local` |
| `--event-store-dir` | 本地目录 | `./nexora-data/events` |
| `--event-store-s3-endpoint` | S3 URL | - |
| `--event-store-s3-bucket` | Bucket 名 | - |
| `--event-store-s3-region` | AWS Region | `us-east-1` |
| `--event-store-s3-access-key` | Access Key | 环境变量 |
| `--event-store-s3-secret-key` | Secret Key | 环境变量 |
| `--event-store-s3-prefix` | 对象前缀 | - |
| `--event-store-s3-path-style` | Path-style（MinIO） | false |
| `--event-store-catalog-path` | Catalog 路径 | `./nexora-data/event_catalog.db` |

---

## ✅ 测试

```bash
# 本地测试
cargo test --package nexora-eventlog --features olap \
  test_local_fs_concurrent_writes

# 完整测试（需要 MinIO）
./scripts/test_event_store_s3_concurrent.sh --with-minio

# 清理
./scripts/test_event_store_s3_concurrent.sh --cleanup
```

---

## 🎯 核心特性

### ✅ 多节点分布式写入
- 任何节点都能写到共享 S3
- 无 coordinator 单点限制
- 完全对等架构

### ✅ 数据一致性
- Iceberg ACID 事务
- 自动冲突检测和重试
- 无数据丢失保证

### ✅ 灵活部署
- 本地文件：开发环境
- MinIO：生产（开源）
- AWS S3：云服务

---

## 📊 对比

| 特性 | 本地文件 | S3 |
|-----|---------|-----|
| 多节点写入 | ⚠️ 限制 | ✅ 完全支持 |
| 并发冲突 | ✅ 自动解决 | ✅ 自动解决 |
| 延迟 | 5-20ms | 10-200ms |
| 复杂度 | 低 | 中 |
| 生产推荐 | ⚠️ 单节点 | ✅ 多节点 |

---

## 📖 文档

- **配置指南**：`docs/architecture/EVENT_STORE_S3_CONFIGURATION.md`
- **完成报告**：`docs/testing/EVENT_STORE_DISTRIBUTED_WRITE_COMPLETION_REPORT.md`
- **中文总结**：`Event_Store分布式写入完成总结.md`

---

## 🐛 常见问题

### Q: 两个节点并发创建表失败？
**A**: 本地模式限制。解决：
1. 先由一个节点创建表
2. 或切换到 S3 模式

### Q: MinIO 连接失败？
**A**: 检查：
```bash
curl http://localhost:9000
docker ps | grep minio
```

### Q: S3 写入慢？
**A**: 优化：
- 批量写入（100-1000 行）
- 使用就近的 endpoint
- 考虑使用 VPC Endpoint

---

**任务完成** ✅  
**日期**: 2026-07-21
