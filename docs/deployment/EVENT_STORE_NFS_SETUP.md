# Event Store 基于 NFS 的多节点部署

> ⚠️ **注意**: 这是备选方案，仅在无法使用 S3/MinIO 时考虑。  
> **生产推荐**: 使用 MinIO（更简单、更可靠）。见 `MINIO_DEPLOYMENT.md`。

---

## 概述

本指南说明如何使用 NFS（Network File System）为 EventLogStore 提供共享存储，实现多节点部署。

### 架构

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Nexora A   │  │  Nexora B   │  │  Nexora C   │
│ (Writer)    │  │ (Writer)    │  │ (Reader)    │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┴────────────────┘
                        │
                  NFS Mount
                 /mnt/nexora
                        │
            ┌───────────▼───────────┐
            │    NFS Server         │
            │  /srv/nexora/events   │
            │  - Parquet files      │
            │  - catalog.db         │
            │  - metadata/*.json    │
            └───────────────────────┘
```

### 优势与限制

**优势**：
- ✅ 对 Iceberg 透明（无需代码修改）
- ✅ SQLite catalog 通过 NFS 文件锁自动协调
- ✅ 所有节点看到统一视图
- ✅ 成熟稳定的技术

**限制**：
- ⚠️ NFS 服务器是单点故障（除非配置 HA）
- ⚠️ 网络延迟影响性能（10-50ms 写延迟）
- ⚠️ 需要额外的 NFS 基础设施
- ⚠️ 运维复杂度高于 MinIO

---

## 部署步骤

### 1. 准备 NFS 服务器

#### Ubuntu/Debian

```bash
# 安装 NFS 服务器
sudo apt-get update
sudo apt-get install -y nfs-kernel-server

# 创建共享目录
sudo mkdir -p /srv/nexora/events
sudo chown nobody:nogroup /srv/nexora/events
sudo chmod 777 /srv/nexora/events

# 配置导出
sudo tee /etc/exports <<EOF
# Nexora event storage
/srv/nexora/events  192.168.1.0/24(rw,sync,no_subtree_check,no_root_squash)
EOF

# 应用配置
sudo exportfs -a
sudo systemctl restart nfs-kernel-server

# 检查导出状态
sudo exportfs -v
```

#### CentOS/RHEL

```bash
# 安装 NFS 服务器
sudo yum install -y nfs-utils

# 启动服务
sudo systemctl enable nfs-server
sudo systemctl start nfs-server

# 创建共享目录
sudo mkdir -p /srv/nexora/events
sudo chown nfsnobody:nfsnobody /srv/nexora/events
sudo chmod 777 /srv/nexora/events

# 配置导出
sudo tee /etc/exports <<EOF
/srv/nexora/events  192.168.1.0/24(rw,sync,no_subtree_check,no_root_squash)
EOF

# 应用配置
sudo exportfs -a
sudo systemctl restart nfs-server

# 防火墙配置
sudo firewall-cmd --permanent --add-service=nfs
sudo firewall-cmd --permanent --add-service=rpc-bind
sudo firewall-cmd --permanent --add-service=mountd
sudo firewall-cmd --reload
```

### 2. 配置 Nexora 客户端

#### 安装 NFS 客户端

```bash
# Ubuntu/Debian
sudo apt-get install -y nfs-common

# CentOS/RHEL
sudo yum install -y nfs-utils
```

#### 挂载 NFS

```bash
# 创建挂载点
sudo mkdir -p /mnt/nexora-events

# 测试挂载
sudo mount -t nfs nfs-server.example.com:/srv/nexora/events /mnt/nexora-events

# 验证挂载
df -h | grep nexora
ls -la /mnt/nexora-events

# 测试写入
sudo touch /mnt/nexora-events/test.txt
sudo rm /mnt/nexora-events/test.txt
```

#### 持久化挂载（推荐）

```bash
# 添加到 /etc/fstab
sudo tee -a /etc/fstab <<EOF
# Nexora event storage
nfs-server.example.com:/srv/nexora/events  /mnt/nexora-events  nfs  defaults,_netdev  0  0
EOF

# 测试 fstab 配置
sudo mount -a
```

### 3. 启动 Nexora 节点

```bash
# 在所有节点上使用相同的挂载路径
nexora-app \
  --node-id node-a \
  --event-store-backend local \
  --event-store-dir /mnt/nexora-events \
  --event-store-catalog-path /mnt/nexora-events/catalog.db
```

---

## 性能优化

### NFS 挂载选项

```bash
# 高性能配置（适合低延迟网络）
sudo mount -t nfs \
  -o rw,sync,hard,intr,timeo=600,retrans=2,rsize=1048576,wsize=1048576 \
  nfs-server:/srv/nexora/events /mnt/nexora-events
```

**选项说明**：
- `sync`: 同步写入（数据一致性）
- `hard`: 失败时无限重试（避免数据丢失）
- `intr`: 允许中断挂起的操作
- `timeo=600`: 超时 60 秒
- `retrans=2`: 重传 2 次
- `rsize/wsize=1MB`: 大块传输（提高吞吐量）

### 读缓存优化（开发环境）

```bash
# 使用异步挂载（牺牲一致性换取性能）
sudo mount -t nfs \
  -o rw,async,hard,intr,rsize=1048576,wsize=1048576 \
  nfs-server:/srv/nexora/events /mnt/nexora-events
```

⚠️ **警告**: `async` 模式在服务器崩溃时可能丢失数据，仅用于开发环境。

### NFS 服务器优化

```bash
# /etc/nfs.conf
[nfsd]
threads=16              # 增加线程数（默认 8）
udp=n                   # 禁用 UDP（仅使用 TCP）
vers2=n                 # 禁用 NFSv2
vers3=n                 # 禁用 NFSv3（仅使用 v4）
vers4=y
vers4.0=y
vers4.1=y
vers4.2=y
```

---

## 高可用配置（可选）

### 方案 A：DRBD + Pacemaker

```
┌──────────────────┐        ┌──────────────────┐
│  Primary NFS     │        │  Secondary NFS   │
│  - Active        │◄─DRBD─►│  - Standby       │
│  - VIP: 10.0.1.100│       │                  │
└──────────────────┘        └──────────────────┘
         │
    Pacemaker 监控
    故障自动切换 VIP
         │
┌────────▼─────────────────┐
│   Nexora Cluster         │
│   挂载: 10.0.1.100:/...  │
└──────────────────────────┘
```

**优势**：
- 自动故障切换（30-60 秒）
- 数据实时同步（DRBD）
- 无需修改客户端配置

**劣势**：
- 配置复杂
- 需要 2 台服务器 + 心跳网络
- 故障切换有短暂中断

### 方案 B：使用 MinIO 代替（强烈推荐）

```bash
# 3 节点 MinIO 集群（比 NFS HA 更简单）
# 见 docs/deployment/MINIO_DEPLOYMENT.md
```

---

## 监控与维护

### 健康检查

```bash
#!/bin/bash
# check_nfs_health.sh

# 1. 检查挂载状态
if ! mountpoint -q /mnt/nexora-events; then
    echo "ERROR: NFS not mounted"
    exit 1
fi

# 2. 检查 NFS 服务器可达性
if ! showmount -e nfs-server.example.com >/dev/null 2>&1; then
    echo "ERROR: NFS server unreachable"
    exit 1
fi

# 3. 测试读写
TEST_FILE="/mnt/nexora-events/.health_check_$(date +%s)"
if ! touch "$TEST_FILE" 2>/dev/null; then
    echo "ERROR: Cannot write to NFS"
    exit 1
fi
rm -f "$TEST_FILE"

echo "OK: NFS healthy"
```

### 性能监控

```bash
# 查看 NFS 统计
nfsstat -c  # 客户端统计
nfsstat -s  # 服务器统计

# 查看挂载统计
mountstats /mnt/nexora-events

# I/O 性能测试
dd if=/dev/zero of=/mnt/nexora-events/test bs=1M count=1000
rm /mnt/nexora-events/test
```

### 故障排查

**问题：挂载失败**
```bash
# 检查 NFS 服务器是否运行
systemctl status nfs-server

# 检查导出配置
sudo exportfs -v

# 检查客户端日志
sudo dmesg | grep -i nfs
sudo journalctl -u nfs-client.target
```

**问题：性能慢**
```bash
# 检查网络延迟
ping nfs-server.example.com

# 检查 NFS 队列深度
nfsiostat 1 10

# 检查服务器负载
ssh nfs-server 'top -bn1 | head -20'
```

**问题：文件锁定**
```bash
# 查看锁定的文件
cat /proc/locks | grep nfs

# 清除僵尸锁（服务器端）
sudo systemctl restart nfs-lock
```

---

## 备份策略

### 快照备份（服务器端）

```bash
# 使用 LVM 快照（假设 NFS 存储在 LVM 卷上）
sudo lvcreate -L 10G -s -n nexora_snap /dev/vg0/nexora

# 挂载快照
sudo mkdir /mnt/nexora_backup
sudo mount /dev/vg0/nexora_snap /mnt/nexora_backup

# 复制到备份位置
rsync -av /mnt/nexora_backup/ /backup/nexora/$(date +%Y%m%d)/

# 清理
sudo umount /mnt/nexora_backup
sudo lvremove -f /dev/vg0/nexora_snap
```

### Iceberg 表导出

```bash
# 使用 Iceberg 自带的导出工具
# 导出元数据（更轻量）
iceberg export \
  --catalog-uri sqlite:///mnt/nexora-events/catalog.db \
  --namespace events \
  --output /backup/nexora-metadata-$(date +%Y%m%d).tar.gz
```

---

## 迁移到 MinIO

如果决定从 NFS 迁移到 MinIO：

```bash
#!/bin/bash
# migrate_nfs_to_minio.sh

NFS_PATH="/mnt/nexora-events"
MINIO_ENDPOINT="http://minio.example.com:9000"
BUCKET="nexora-events"

# 1. 停止所有 Nexora 节点
# ...

# 2. 上传数据到 MinIO
aws s3 sync $NFS_PATH/ s3://$BUCKET/ \
  --endpoint-url $MINIO_ENDPOINT

# 3. 更新 Nexora 配置
# --event-store-backend s3
# --event-store-s3-endpoint $MINIO_ENDPOINT
# --event-store-s3-bucket $BUCKET

# 4. 启动 Nexora 节点
# ...
```

---

## 成本对比

| 项目 | NFS (HA) | MinIO (3节点) |
|-----|----------|---------------|
| 服务器 | 2 台 | 3 台 |
| 额外软件 | DRBD + Pacemaker | MinIO (开源) |
| 配置复杂度 | 高 | 低 |
| 故障切换 | 30-60秒 | 自动（秒级） |
| 性能 | 中 | 高 |
| 运维成本 | 高 | 低 |

---

## 总结

### 何时使用 NFS

✅ **适合**：
- 已有 NFS 基础设施
- 合规要求禁止对象存储
- 数据量小（< 100GB）
- 写入量低（< 100 TPS）

❌ **不适合**：
- 高并发写入（> 1000 TPS）
- 大规模集群（> 10 节点）
- 需要横向扩展
- 关键任务生产环境

### 推荐决策树

```
需要多节点 Event Store？
  ├─ 是 → 能部署 MinIO 吗？
  │      ├─ 是 → ✅ 使用 MinIO（推荐）
  │      └─ 否 → ⚠️ 使用 NFS（本指南）
  └─ 否 → ✅ 使用本地文件（--event-store-backend local）
```

---

**参考资料**：
- [NFS 官方文档](https://nfs.sourceforge.net/)
- [Linux NFS FAQ](https://nfs.sourceforge.net/nfs-faq/)
- [DRBD 用户指南](https://linbit.com/drbd-user-guide/)
- [MinIO 部署指南](./MINIO_DEPLOYMENT.md)

**相关文档**：
- [设计分析](../architecture/EVENT_STORE_LOCAL_REPLICATION_DESIGN.md)
- [S3 配置](../architecture/EVENT_STORE_S3_CONFIGURATION.md)

---

**更新日期**: 2026-07-21
