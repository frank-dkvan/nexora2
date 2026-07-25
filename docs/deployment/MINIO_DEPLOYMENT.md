# MinIO 高可用集群部署指南

> ✅ **推荐方案**: 生产环境 Event Store 的最佳选择。

---

## 概述

MinIO 是高性能、Kubernetes 原生的对象存储服务，100% 兼容 Amazon S3 API。

### 为什么选择 MinIO

**vs 本地文件**：
- ✅ 真正的分布式存储（无单点故障）
- ✅ 所有节点并发读写（无 SQLite 锁冲突）
- ✅ 水平扩展（添加节点即扩容）

**vs NFS**：
- ✅ 更高性能（对象存储优化）
- ✅ 更简单的 HA（无需 DRBD/Pacemaker）
- ✅ 更好的故障恢复（纠删码自动修复）

**vs AWS S3**：
- ✅ 完全开源免费
- ✅ 本地部署（数据主权）
- ✅ 更低延迟（局域网）
- ✅ 无流量费用

---

## 快速开始（单节点开发）

### Docker 方式

```bash
# 启动 MinIO
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v /data/minio:/data \
  minio/minio server /data --console-address ":9001"

# 创建 bucket
docker exec minio \
  mc alias set local http://localhost:9000 minioadmin minioadmin

docker exec minio \
  mc mb local/nexora-events

# 启动 Nexora
nexora-app \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://localhost:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-access-key minioadmin \
  --event-store-s3-secret-key minioadmin \
  --event-store-s3-path-style
```

### 原生二进制方式

```bash
# 下载 MinIO
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/

# 创建数据目录
sudo mkdir -p /data/minio
sudo chown $USER:$USER /data/minio

# 启动 MinIO
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
minio server /data/minio --console-address ":9001"

# 在浏览器访问 http://localhost:9001
# 登录后创建 bucket "nexora-events"
```

---

## 生产部署（3 节点 HA 集群）

### 架构

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│  MinIO 1   │  │  MinIO 2   │  │  MinIO 3   │
│ minio1.lan │  │ minio2.lan │  │ minio3.lan │
│ /data1     │  │ /data1     │  │ /data1     │
│ /data2     │  │ /data2     │  │ /data2     │
└──────┬─────┘  └──────┬─────┘  └──────┬─────┘
       │               │               │
       └───────────────┴───────────────┘
                       │
                  纠删码 (EC:1)
              (3 data + 1 parity)
                       │
       ┌───────────────┴───────────────┐
       │                               │
┌──────▼─────┐                  ┌──────▼─────┐
│  Nexora    │ ◄── Load ───────►│  Nexora    │
│  Cluster   │    Balancer      │  Cluster   │
└────────────┘                  └────────────┘
```

### 容错能力

- **3 节点集群**：可容忍 1 节点故障
- **4 节点集群**：可容忍 1 节点故障（推荐）
- **5+ 节点集群**：可容忍 2+ 节点故障

### 硬件要求

| 组件 | 最低配置 | 推荐配置 |
|-----|---------|---------|
| CPU | 4 核 | 8 核 |
| 内存 | 8 GB | 16 GB |
| 磁盘 | 100 GB SSD | 500 GB NVMe |
| 网络 | 1 Gbps | 10 Gbps |

### 部署步骤

#### 1. 准备服务器

```bash
# 在所有 3 个节点上执行

# 安装 MinIO
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/

# 创建数据目录（每个节点 2 个磁盘）
sudo mkdir -p /mnt/disk1/minio /mnt/disk2/minio
sudo chown minio:minio /mnt/disk1/minio /mnt/disk2/minio

# 创建 MinIO 用户
sudo useradd -r -s /bin/false minio
```

#### 2. 配置 MinIO（所有节点相同）

```bash
# /etc/default/minio
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=<strong-password-here>

# 集群端点（所有节点的所有磁盘）
MINIO_VOLUMES="http://minio{1...3}.example.com/mnt/disk{1...2}/minio"

# 控制台地址
MINIO_OPTS="--console-address :9001"

# 区域设置（可选）
MINIO_REGION_NAME=us-east-1
```

#### 3. 创建 systemd 服务

```bash
# /etc/systemd/system/minio.service
[Unit]
Description=MinIO
Documentation=https://docs.min.io
Wants=network-online.target
After=network-online.target
AssertFileIsExecutable=/usr/local/bin/minio

[Service]
Type=notify
User=minio
Group=minio
EnvironmentFile=/etc/default/minio
ExecStart=/usr/local/bin/minio server $MINIO_VOLUMES $MINIO_OPTS
Restart=always
LimitNOFILE=65536
TasksMax=infinity
TimeoutStopSec=infinity
SendSIGKILL=no

[Install]
WantedBy=multi-user.target
```

#### 4. 启动集群

```bash
# 在所有 3 个节点上执行
sudo systemctl daemon-reload
sudo systemctl enable minio
sudo systemctl start minio

# 检查状态
sudo systemctl status minio

# 查看日志
sudo journalctl -u minio -f
```

#### 5. 配置负载均衡（可选但推荐）

**使用 Nginx**：

```nginx
# /etc/nginx/conf.d/minio.conf
upstream minio_backend {
    server minio1.example.com:9000;
    server minio2.example.com:9000;
    server minio3.example.com:9000;
}

server {
    listen 80;
    server_name minio.example.com;

    # 允许大文件上传
    client_max_body_size 1000M;

    location / {
        proxy_pass http://minio_backend;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # WebSocket 支持
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

**使用 HAProxy**：

```
# /etc/haproxy/haproxy.cfg
frontend minio_frontend
    bind *:9000
    default_backend minio_backend

backend minio_backend
    balance roundrobin
    server minio1 minio1.example.com:9000 check
    server minio2 minio2.example.com:9000 check
    server minio3 minio3.example.com:9000 check
```

#### 6. 创建 Bucket 和用户

```bash
# 安装 MinIO Client
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# 配置 alias
mc alias set myminio http://minio1.example.com:9000 admin <password>

# 创建 bucket
mc mb myminio/nexora-events

# 创建专用用户（最小权限原则）
mc admin user add myminio nexora-user <strong-password>

# 创建策略
cat > /tmp/nexora-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::nexora-events",
        "arn:aws:s3:::nexora-events/*"
      ]
    }
  ]
}
EOF

mc admin policy create myminio nexora-policy /tmp/nexora-policy.json
mc admin policy attach myminio nexora-policy --user nexora-user

# 验证
mc ls myminio/nexora-events
```

#### 7. 启动 Nexora 集群

```bash
# 在所有 Nexora 节点上执行
nexora-app \
  --node-id node-a \
  --event-store-backend s3 \
  --event-store-s3-endpoint http://minio.example.com:9000 \
  --event-store-s3-bucket nexora-events \
  --event-store-s3-access-key nexora-user \
  --event-store-s3-secret-key <password> \
  --event-store-s3-region us-east-1
```

---

## 高级配置

### TLS/HTTPS 加密

```bash
# 生成自签名证书（开发）
openssl req -new -x509 -days 365 -nodes \
  -out /etc/minio/certs/public.crt \
  -keyout /etc/minio/certs/private.key \
  -subj "/CN=minio.example.com"

# 或使用 Let's Encrypt（生产）
certbot certonly --standalone -d minio.example.com

# 复制证书到 MinIO
sudo mkdir -p /etc/minio/certs
sudo cp /etc/letsencrypt/live/minio.example.com/fullchain.pem \
  /etc/minio/certs/public.crt
sudo cp /etc/letsencrypt/live/minio.example.com/privkey.pem \
  /etc/minio/certs/private.key

# 重启 MinIO
sudo systemctl restart minio

# 更新 Nexora 配置
--event-store-s3-endpoint https://minio.example.com:9000
```

### 数据压缩

```bash
# 启用自动压缩（减少存储成本）
mc admin config set myminio compression \
  extensions=".parquet,.json,.csv" \
  mime_types="application/json,text/csv"

mc admin service restart myminio
```

### 生命周期管理

```bash
# 自动删除 90 天前的数据
cat > /tmp/lifecycle.json <<EOF
{
  "Rules": [
    {
      "ID": "expire-old-events",
      "Status": "Enabled",
      "Expiration": {
        "Days": 90
      }
    }
  ]
}
EOF

mc ilm import myminio/nexora-events < /tmp/lifecycle.json
```

### 版本控制（可选）

```bash
# 启用版本控制（防止意外删除）
mc version enable myminio/nexora-events

# 查看对象版本
mc ls --versions myminio/nexora-events
```

---

## 监控与告警

### Prometheus 指标

```bash
# 启用 Prometheus 指标
mc admin prometheus generate myminio > /tmp/minio-metrics.yaml

# 添加到 Prometheus 配置
# prometheus.yml:
# - job_name: 'minio'
#   static_configs:
#   - targets: ['minio1.example.com:9000']
```

### 健康检查

```bash
#!/bin/bash
# /usr/local/bin/check_minio.sh

ENDPOINT="http://minio.example.com:9000"
BUCKET="nexora-events"

# 检查集群健康
mc admin info myminio >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "CRITICAL: MinIO cluster unhealthy"
    exit 2
fi

# 检查 bucket 可访问
mc ls myminio/$BUCKET >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "CRITICAL: Bucket $BUCKET not accessible"
    exit 2
fi

# 测试写入
TEST_FILE="/tmp/health_check_$(date +%s).txt"
echo "test" > $TEST_FILE
mc cp $TEST_FILE myminio/$BUCKET/.health_check >/dev/null 2>&1
WRITE_OK=$?
mc rm myminio/$BUCKET/.health_check >/dev/null 2>&1
rm $TEST_FILE

if [ $WRITE_OK -ne 0 ]; then
    echo "CRITICAL: Cannot write to $BUCKET"
    exit 2
fi

echo "OK: MinIO healthy"
exit 0
```

### Grafana 仪表盘

MinIO 官方提供了 Grafana 仪表盘模板：
- [MinIO Dashboard](https://grafana.com/grafana/dashboards/13502)

导入步骤：
1. Grafana → Dashboards → Import
2. 输入 Dashboard ID: `13502`
3. 选择 Prometheus 数据源
4. 点击 Import

---

## 备份与恢复

### 使用 mc mirror 备份

```bash
# 定期镜像到另一个 MinIO 集群
mc mirror --watch myminio/nexora-events backup-minio/nexora-events-backup

# 或备份到本地
mc mirror myminio/nexora-events /backup/nexora-events-$(date +%Y%m%d)
```

### 使用 Velero（Kubernetes）

```bash
# 安装 Velero
velero install \
  --provider aws \
  --plugins velero/velero-plugin-for-aws:v1.5.0 \
  --bucket nexora-backups \
  --backup-location-config \
    region=us-east-1,s3ForcePathStyle=true,s3Url=http://minio.example.com:9000 \
  --use-volume-snapshots=false

# 创建备份
velero backup create nexora-backup --include-namespaces nexora

# 恢复
velero restore create --from-backup nexora-backup
```

---

## 性能优化

### 网络优化

```bash
# 增加 TCP 缓冲区（所有节点）
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"

# 持久化
sudo tee -a /etc/sysctl.conf <<EOF
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
net.ipv4.tcp_rmem = 4096 87380 134217728
net.ipv4.tcp_wmem = 4096 65536 134217728
EOF
```

### 磁盘优化

```bash
# 使用 XFS 文件系统（推荐）
sudo mkfs.xfs -f /dev/sdb1
sudo mount -o noatime /dev/sdb1 /mnt/disk1

# 持久化挂载
echo "/dev/sdb1 /mnt/disk1 xfs noatime 0 0" | sudo tee -a /etc/fstab

# 禁用磁盘缓存（数据一致性）
echo none | sudo tee /sys/block/sdb/queue/scheduler
```

### 批量写入优化

```rust
// Nexora 端：批量写入
let events = collect_events_batch(1000); // 1000 events/batch
event_store.append(&events).await?;

// 避免：逐条写入
for event in events {
    event_store.append(&[event]).await?; // ❌ 慢
}
```

---

## 故障排查

### 常见问题

**1. 集群启动失败**

```bash
# 检查所有节点时间同步
sudo ntpdate -s time.nist.gov

# 检查节点间网络连通性
telnet minio2.example.com 9000
```

**2. 性能慢**

```bash
# 检查磁盘 I/O
iostat -x 1 10

# 检查网络带宽
iperf3 -s  # 在 minio1 上
iperf3 -c minio1.example.com  # 在 minio2 上

# 检查 MinIO 内部指标
mc admin trace myminio
```

**3. 数据丢失/损坏**

```bash
# 运行数据完整性检查
mc admin heal myminio

# 查看修复进度
mc admin heal myminio --verbose
```

---

## 迁移指南

### 从本地文件迁移到 MinIO

```bash
#!/bin/bash
# migrate_local_to_minio.sh

LOCAL_DIR="/nexora-data/events"
MINIO_ALIAS="myminio"
BUCKET="nexora-events"

# 1. 停止所有 Nexora 节点
echo "停止 Nexora 节点..."
# systemctl stop nexora-*

# 2. 上传数据
echo "上传数据到 MinIO..."
mc cp --recursive $LOCAL_DIR/ $MINIO_ALIAS/$BUCKET/

# 3. 验证数据
echo "验证数据..."
mc ls --recursive $MINIO_ALIAS/$BUCKET/ | wc -l

# 4. 更新 Nexora 配置
echo "请更新 Nexora 配置，使用以下参数："
echo "  --event-store-backend s3"
echo "  --event-store-s3-endpoint http://minio.example.com:9000"
echo "  --event-store-s3-bucket $BUCKET"

# 5. 启动 Nexora
echo "启动 Nexora 节点..."
# systemctl start nexora-*
```

---

## 成本分析

### 硬件成本（3 节点 HA 集群）

| 项目 | 数量 | 单价（USD）| 总价（USD）|
|-----|-----|----------|----------|
| 服务器（8核/16GB/500GB SSD）| 3 | $500 | $1,500 |
| 交换机（10GbE）| 1 | $300 | $300 |
| **总计** | | | **$1,800** |

### 运营成本（年）

| 项目 | 成本（USD/年）|
|-----|-------------|
| 电费（300W/台 × 3）| $400 |
| 网络带宽 | $0（内网）|
| 许可证 | $0（开源）|
| **总计** | **$400** |

### vs AWS S3 成本对比

假设：1TB 数据，10TB 月流量

| 项目 | MinIO（自建）| AWS S3 |
|-----|-------------|--------|
| 存储费 | $0（已计入硬件）| $23/月 |
| 流量费 | $0（内网）| $900/月 |
| **年成本** | **$400** | **$11,076** |

**投资回收期**：2 个月

---

## 总结

### MinIO 优势总结

| 特性 | MinIO | NFS | 本地文件 |
|-----|-------|-----|---------|
| 多节点写入 | ✅ 完全支持 | ⚠️ 需文件锁 | ❌ 单节点 |
| 高可用 | ✅ 自动故障切换 | ⚠️ 需 HA 配置 | ❌ 无 |
| 水平扩展 | ✅ 添加节点即扩容 | ❌ 受限 | ❌ 无 |
| 性能 | ✅ 高（对象存储优化）| ⚠️ 中 | ✅ 高 |
| 成本 | ✅ 开源免费 | ✅ 开源免费 | ✅ 免费 |
| 运维复杂度 | ✅ 低 | ⚠️ 高（HA）| ✅ 低 |

### 推荐配置

**开发环境**：
- 单节点 MinIO Docker 容器
- 或本地文件模式

**生产环境（小规模）**：
- 3 节点 MinIO 集群
- 纠删码 EC:1
- Nginx 负载均衡

**生产环境（大规模）**：
- 5+ 节点 MinIO 集群
- 纠删码 EC:2
- HAProxy 负载均衡 + Keepalived VIP
- Prometheus + Grafana 监控

---

**相关文档**：
- [设计分析](../architecture/EVENT_STORE_LOCAL_REPLICATION_DESIGN.md)
- [NFS 部署](./EVENT_STORE_NFS_SETUP.md)
- [S3 配置](../architecture/EVENT_STORE_S3_CONFIGURATION.md)

**官方资源**：
- [MinIO 文档](https://min.io/docs/minio/linux/index.html)
- [MinIO GitHub](https://github.com/minio/minio)
- [MinIO Slack](https://slack.min.io/)

---

**更新日期**: 2026-07-21
