# Event Store 部署快速参考

**快速选择适合你的方案** 👇

---

## 我应该用哪个方案？

### 🚀 开发环境

**使用本地文件模式**（默认）

```bash
nexora-app --event-store-backend local
```

**优点**：零依赖，启动快，性能高  
**限制**：单节点

---

### 🏢 生产环境（多节点）

#### ✅ 推荐：MinIO

**为什么选择 MinIO？**
- ✅ 真正的分布式（无单点故障）
- ✅ 所有节点完全并发写入
- ✅ 开源免费
- ✅ vs AWS S3 节省 95% 成本
- ✅ 5 分钟快速开始

**快速开始（Docker）**：
```bash
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

nexora-app \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin \
  --event-store-s3-path-style
```

**生产部署**：📖 [`docs/deployment/MINIO_DEPLOYMENT.md`](deployment/MINIO_DEPLOYMENT.md)

---

#### ⚠️ 备选：NFS

**仅在以下情况使用**：
- ❌ 无法部署 MinIO（合规限制）
- ✅ 已有 NFS 基础设施
- ✅ 数据量小 + 写入量低

**快速开始**：
```bash
# NFS 服务器
sudo apt-get install nfs-kernel-server
sudo mkdir -p /srv/nexora/events
echo "/srv/nexora/events 192.168.1.0/24(rw,sync)" | sudo tee -a /etc/exports
sudo exportfs -a

# Nexora 客户端
sudo mount -t nfs nfs-server:/srv/nexora/events /mnt/nexora-events
nexora-app --event-store-backend local --event-store-dir /mnt/nexora-events
```

**详细指南**：📖 [`docs/deployment/EVENT_STORE_NFS_SETUP.md`](deployment/EVENT_STORE_NFS_SETUP.md)

---

## 方案对比

| 特性 | 本地文件 | NFS | MinIO |
|-----|---------|-----|-------|
| **多节点** | ❌ 单节点 | ✅ 支持 | ✅ 完全支持 |
| **性能** | ⚡ 最高 | ⚠️ 中等 | ⚡ 高 |
| **可靠性** | ⚠️ 单点 | ⚠️ 需 HA | ✅ 内置 HA |
| **运维** | ✅ 简单 | ⚠️ 复杂 | ✅ 简单 |
| **成本** | 免费 | 硬件 | 硬件（一次性）|
| **推荐场景** | 开发/测试 | 特殊场景 | **生产** |

---

## 详细文档

### 部署指南
- 📖 [MinIO 部署指南](deployment/MINIO_DEPLOYMENT.md)（**推荐阅读**）
- 📖 [NFS 部署指南](deployment/EVENT_STORE_NFS_SETUP.md)

### 设计文档
- 📖 [本地文件复制设计分析](architecture/EVENT_STORE_LOCAL_REPLICATION_DESIGN.md)
- 📖 [S3 配置说明](architecture/EVENT_STORE_S3_CONFIGURATION.md)
- 📖 [任务完成报告](architecture/EVENT_STORE_LOCAL_REPLICATION_COMPLETION.md)

---

## 决策树

```
需要多节点？
├─ 否 → 本地文件（开发/测试）
│       命令：nexora-app --event-store-backend local
│
└─ 是 → 能部署 MinIO？
        ├─ 是 → ✅ MinIO（推荐）
        │       文档：docs/deployment/MINIO_DEPLOYMENT.md
        │       时间：5 分钟（开发）/ 1 小时（生产）
        │
        └─ 否 → ⚠️ NFS（备选）
                文档：docs/deployment/EVENT_STORE_NFS_SETUP.md
                时间：30 分钟
```

---

## 常见问题

**Q: 为什么不实现文件级复制？**

A: 经过技术评估，发现有 4 个关键阻塞问题：
1. SQLite catalog 并发冲突
2. 数据传输开销大
3. 元数据一致性复杂
4. 故障恢复难实现

MinIO 提供了更好的解决方案，且零开发成本。详见：[设计分析文档](architecture/EVENT_STORE_LOCAL_REPLICATION_DESIGN.md)

---

**Q: MinIO 安全吗？**

A: MinIO 是生产级开源项目，被广泛使用：
- Slack、Adobe、Tesla 等公司在使用
- 支持完整的认证、授权、加密
- 定期安全审计和漏洞修复
- 详见：[MinIO 安全文档](https://min.io/docs/minio/linux/operations/security.html)

---

**Q: MinIO vs AWS S3 成本对比？**

A: MinIO 节省 95% 成本：
- AWS S3（1TB + 10TB 流量）：~$11,000/年
- MinIO（3 节点自建）：~$1,800 硬件 + $400 电费/年
- 投资回收期：2 个月

详见：[MinIO 部署指南 - 成本分析](deployment/MINIO_DEPLOYMENT.md#成本分析)

---

**Q: 能否从本地文件迁移到 MinIO？**

A: 可以，使用 `mc` 工具同步数据：
```bash
mc mirror /nexora-data/events/ myminio/nexora-events/
```

详见：[MinIO 部署指南 - 迁移指南](deployment/MINIO_DEPLOYMENT.md#迁移指南)

---

**更新日期**: 2026-07-21
