# Event Store 本地文件复制 - 设计分析与推荐方案

## 背景

当前 EventLogStore 支持两种后端：
- **S3 模式**：所有节点共享同一个对象存储（AWS S3/MinIO/SeaweedFS），天然支持多节点写入
- **本地文件模式**：每个节点独立的本地目录，目前是单节点限制

本文档分析在**本地文件模式**下实现多节点复制的可行性。

---

## 方案评估

### 方案 A：文件级复制（通过 Zenoh）

#### 设计思路
1. 主节点写入 Iceberg 数据文件
2. 捕获新生成的 Parquet 文件路径
3. 通过 Zenoh pub/sub 广播文件内容到所有副本节点
4. 副本节点接收后写入本地，并更新 SQLite catalog

#### 关键挑战

**🚫 Blocker 1：SQLite Catalog 并发冲突**
```
Iceberg 使用 SQLite 作为 catalog 存储元数据：
- nexora-data/event_catalog.db

问题：
- SQLite 不支持多节点并发写入（文件锁）
- 即使通过文件复制同步，也会有不一致窗口
- 两个节点同时创建表会 UNIQUE 约束冲突（已在测试中证实）
```

**🚫 Blocker 2：数据传输开销**
```
Parquet 文件大小：
- 小批次（100 行）：~10KB
- 中批次（10K 行）：~1MB
- 大批次（1M 行）：~100MB

通过 Zenoh 传输完整文件内容：
- 网络带宽占用高
- 延迟大（需等待传输完成）
- 内存占用高（序列化/反序列化）
```

**🚫 Blocker 3：Iceberg 元数据一致性**
```
Iceberg 的元数据结构：
- Catalog DB：表定义、schema 版本
- Metadata files：snapshot 链、manifest list
- Manifest files：数据文件清单

问题：
- 这些文件之间有严格的引用关系
- 复制时需要保证原子性和顺序性
- 任何不一致都会导致读取失败
```

**🚫 Blocker 4：故障恢复复杂**
```
场景：节点 B 复制到一半时崩溃

需要处理：
- 部分写入的 Parquet 文件
- 不完整的 manifest 更新
- SQLite catalog 的回滚
- 与节点 A 的状态差异

复杂度极高，容易引入 bug
```

#### 实现成本估算
- 开发时间：3-4 周
- 代码量：~2000 行
- 测试覆盖：需要大量的并发/故障场景测试
- 维护成本：高（Iceberg 升级可能破坏假设）

#### 结论
**❌ 不推荐**：实现复杂度高，维护成本大，且无法解决 SQLite 并发冲突的根本问题。

---

### 方案 B：NFS 共享存储（推荐）

#### 设计思路
使用 NFS（Network File System）作为共享存储层，所有节点挂载同一个 NFS 目录。

```
┌─────────┐    ┌─────────┐    ┌─────────┐
│ Node A  │    │ Node B  │    │ Node C  │
└────┬────┘    └────┬────┘    └────┬────┘
     │              │              │
     └──────────────┴──────────────┘
                    │
              ┌─────▼─────┐
              │ NFS Server│
              │           │
              │ /nexora   │
              │  /events  │
              │  /catalog │
              └───────────┘
```

#### 优势

**✅ 透明性**
- 对 Iceberg 完全透明，无需修改代码
- SQLite catalog 通过 NFS 文件锁自动协调
- Parquet 文件自然共享

**✅ 一致性保证**
- 文件系统级别的原子性
- NFS 锁机制防止并发冲突
- 所有节点看到相同的文件视图

**✅ 零开发成本**
- 无需编写复制逻辑
- 无需维护一致性代码
- 只需配置 NFS 挂载

**✅ 成熟稳定**
- NFS 是 40 年的成熟技术
- 生产环境广泛使用
- 有完善的监控和调优工具

#### 实施步骤

**1. 安装 NFS 服务器**
```bash
# Ubuntu/Debian
sudo apt-get install nfs-kernel-server

# CentOS/RHEL
sudo yum install nfs-utils
```

**2. 配置 NFS 导出**
```bash
# /etc/exports
/srv/nexora/events  192.168.1.0/24(rw,sync,no_subtree_check,no_root_squash)
```

**3. 客户端挂载**
```bash
# 每个 Nexora 节点
sudo mount -t nfs nfs-server:/srv/nexora/events /mnt/nexora-events

# 启动 Nexora
nexora-app \
  --event-store-backend local \
  --event-store-dir /mnt/nexora-events
```

**4. 持久化挂载（可选）**
```bash
# /etc/fstab
nfs-server:/srv/nexora/events  /mnt/nexora-events  nfs  defaults  0  0
```

#### 性能考虑

| 指标 | 本地磁盘 | NFS（千兆网）| NFS（万兆网）|
|-----|---------|------------|-------------|
| 读延迟 | 0.1-1ms | 1-5ms | 0.5-2ms |
| 写延迟 | 1-10ms | 10-50ms | 5-20ms |
| 吞吐量 | 500MB/s | 100MB/s | 800MB/s |

**优化建议**：
- 使用 SSD 作为 NFS 存储
- 启用 NFS 读缓存（async mount option）
- 对于写密集型，考虑 10GbE 网络
- 使用专用的存储网络（隔离流量）

#### 高可用方案

**NFS HA（使用 DRBD + Pacemaker）**
```
┌──────────┐         ┌──────────┐
│Primary NFS│◄──DRBD─►│Secondary│
│ + VIP    │         │  NFS    │
└──────────┘         └──────────┘
     │
     │ Pacemaker 自动故障切换
     │
┌────▼────────────────────┐
│  Nexora Cluster         │
│  (自动重连到新 VIP)      │
└─────────────────────────┘
```

---

### 方案 C：继续使用 S3（强烈推荐）

#### 为什么 S3 是最佳选择

**✅ 天然分布式**
- 无单点故障
- 无容量限制
- 无需管理文件系统

**✅ 成本优势**
```
MinIO（开源）：
- 免费
- 可部署在现有硬件
- 兼容 S3 API

对比 NFS HA：
- 无需 DRBD 复制开销
- 无需 Pacemaker 复杂配置
- 更简单的运维
```

**✅ 更好的性能**
```
并发写入：
- S3：多节点完全并发，无锁
- NFS：需要 SQLite 文件锁协调

水平扩展：
- S3：无限扩展（对象存储特性）
- NFS：受单台服务器限制
```

**✅ Iceberg 原生支持**
- Iceberg 为对象存储优化
- 元数据操作使用原子 PUT
- 无 SQLite 并发问题

---

## 推荐决策

### 生产环境 → S3（MinIO/SeaweedFS）

**理由**：
1. 真正的分布式架构
2. 无单点故障
3. 水平扩展能力
4. 运维简单

**成本**：
- MinIO：开源免费
- 3 台服务器即可搭建 HA 集群
- 比 NFS HA 更简单

### 开发/测试 → 本地文件（单节点）

**理由**：
1. 零依赖，启动快
2. 适合笔记本开发
3. 不需要多节点

### 边缘场景 → NFS

**仅当满足以下所有条件时考虑**：
1. 确实无法部署 MinIO（例如：合规限制）
2. 需要多节点本地文件共享
3. 已有 NFS 基础设施
4. 可以接受 NFS 的性能和 HA 复杂度

---

## 实施建议

### 现状：保持原样

**当前实现已经很好**：
- S3 模式：完全支持多节点，生产就绪
- 本地模式：开发友好，单节点足够

**文档化限制**：
```rust
// crates/nexora-eventlog/src/event_log_store.rs

/// EventLogStore manages Iceberg event tables.
///
/// ## Multi-node deployment
///
/// For **production multi-node clusters**, use S3-compatible storage:
/// - AWS S3, MinIO, SeaweedFS, etc.
/// - All nodes share the same object store
/// - Fully distributed, no single point of failure
///
/// For **local filesystem mode**:
/// - Single-node only (dev/test)
/// - OR use NFS for multi-node (not recommended vs S3)
///
/// See docs/architecture/EVENT_STORE_LOCAL_REPLICATION_DESIGN.md
```

### 提供 NFS 配置示例

创建 `docs/deployment/EVENT_STORE_NFS_SETUP.md`，详细说明 NFS 配置步骤。

### 提供 MinIO 快速部署

创建 `scripts/deploy_minio_ha.sh`，一键部署 3 节点 MinIO 集群。

---

## 总结

| 方案 | 开发成本 | 运维成本 | 性能 | 可靠性 | 推荐度 |
|-----|---------|---------|-----|--------|--------|
| **文件复制** | 高（3-4周）| 高 | 中 | 低 | ❌ |
| **NFS** | 零 | 中 | 中 | 中 | ⚠️ |
| **S3/MinIO** | 零 | 低 | 高 | 高 | ✅✅✅ |

**最终建议**：
1. **不实现文件级复制**（成本 >> 收益）
2. **文档化当前限制**（本地=单节点）
3. **提供 NFS 配置指南**（边缘场景）
4. **推荐 MinIO**（最佳平衡点）

---

**作者**: Claude Code  
**日期**: 2026-07-21  
**状态**: 设计决策文档
