# Nexora 2.0 运行操作指南

**版本**: 2.0  
**更新日期**: 2026-07-27  
**适用场景**: 生产部署、开发测试

---

## 快速导航

- [单节点模式](#单节点模式) - 生产可用 ✅
- [RisingWave 单节点模式](#risingwave-单节点模式) - 开发测试 ✅
- [RisingWave 集群模式](#risingwave-集群模式) - 生产可用 ✅
- [分布式集群模式](#分布式集群模式) - ⚠️ 不建议生产使用
- [验证与测试](#验证与测试)
- [监控与运维](#监控与运维)
- [故障排查](#故障排查)

---

## 生产就绪度说明

| 部署模式 | 生产可用 | 推荐场景 |
|---------|---------|---------|
| 单节点 Nexora | ✅ 是 | 开发、测试、低风险生产 |
| 单节点 Nexora + RisingWave 集群 | ✅ 是 | **生产推荐方案** |
| RisingWave 单节点 | ⚠️ 有限 | 开发、测试 |
| Nexora 分布式集群 | ❌ 否 | 等待 Track A 完成 |

---

## 前置准备

### 系统要求

**最低配置**:
- CPU: 4 核
- 内存: 8GB
- 磁盘: 50GB SSD
- OS: Linux / macOS
- Rust: 1.88+

**生产推荐配置**:
- CPU: 16 核
- 内存: 32GB
- 磁盘: 500GB SSD (NVMe)
- OS: Ubuntu 22.04 LTS
- 网络: 10Gbps

### 依赖安装

```bash
# 1. 安装 Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup update

# 2. 安装系统依赖
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev cmake

# macOS
brew install cmake openssl pkg-config

# 3. 克隆仓库
git clone https://github.com/frank-dkvan/nexora2.git
cd nexora2

# 4. 检查环境
rustc --version
cargo --version
```

---

## 单节点模式

### 基础启动

**适用场景**: 开发、测试、低风险生产

```bash
# 1. 编译（首次运行）
cargo build --release

# 2. 启动 Nexora（最简配置）
./target/release/nexora \
  --host 127.0.0.1 \
  --port 8080 \
  --allow-unauthenticated

# 输出示例:
# 🚀 Nexora 2.0 starting...
#    HTTP:   listening on 127.0.0.1:8080
#    Graph:  RocksDB at ./nexora-data/graph
# ✅ Nexora started successfully
```

### 完整配置启动

```bash
# 创建数据目录
mkdir -p /data/nexora/{graph,events,wal}

# 启动 Nexora（完整配置）
./target/release/nexora \
  --host 0.0.0.0 \
  --port 8080 \
  --data-dir /data/nexora/graph \
  --wal-dir /data/nexora/wal \
  --allow-unauthenticated \
  --pg-port 5432 \
  --pg-trust

# 输出示例:
# 🚀 Nexora 2.0 starting...
#    HTTP:   listening on 0.0.0.0:8080
#    PG:     listening on 0.0.0.0:5432
#    Graph:  RocksDB at /data/nexora/graph
#    WAL:    /data/nexora/wal
# ✅ Nexora started successfully
```

### 使用配置文件

```bash
# 1. 创建配置文件
cat > nexora.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/graph"
wal_dir = "/data/nexora/wal"

[pgwire]
enabled = true
port = 5432
trust_auth = false
users_file = "/etc/nexora/users.conf"

[event_store]
backend = "s3"
s3_endpoint = "http://localhost:9000"
s3_bucket = "nexora-events"
s3_access_key = "minioadmin"
s3_secret_key = "minioadmin"
EOF

# 2. 启动
./target/release/nexora --config nexora.toml

# 3. 后台运行（systemd）
sudo tee /etc/systemd/system/nexora.service << 'EOF'
[Unit]
Description=Nexora Graph Database
After=network.target

[Service]
Type=simple
User=nexora
WorkingDirectory=/opt/nexora
ExecStart=/opt/nexora/nexora --config /etc/nexora/nexora.toml
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable nexora
sudo systemctl start nexora
sudo systemctl status nexora
```

---

## RisingWave 单节点模式

### Phase 7: 嵌入式单节点

**适用场景**: 开发、测试（不推荐生产）

```bash
# 1. 编译（带 RisingWave 特性）
cargo build --release --features risingwave,embedded

# 2. 下载 RisingWave 二进制（如果没有）
# 选项 A: 使用预编译版本
wget https://github.com/risingwavelabs/risingwave/releases/download/v3.0.2/risingwave-v3.0.2-x86_64-unknown-linux.tar.gz
tar xzf risingwave-v3.0.2-x86_64-unknown-linux.tar.gz
sudo mv risingwave /usr/local/bin/
sudo chmod +x /usr/local/bin/risingwave

# 选项 B: 设置 binary_path
export RISINGWAVE_BIN=/path/to/risingwave

# 3. 启动 Nexora + RisingWave 单节点
./target/release/nexora \
  --host 0.0.0.0 \
  --port 8080 \
  --enable-event-streams \
  --embedded-event-streams

# 输出示例:
# 🚀 Nexora 2.0 starting...
#    HTTP:   listening on 0.0.0.0:8080
#    RisingWave: starting standalone mode...
#      Meta:     127.0.0.1:5690
#      Frontend: 127.0.0.1:4566 (PostgreSQL protocol)
#      Compute:  127.0.0.1:5688
# ✅ RisingWave started successfully
# ✅ Nexora started successfully
```

### 验证 RisingWave 单节点

```bash
# 1. 检查进程
ps aux | grep risingwave
# 应该看到 1 个 risingwave standalone 进程

# 2. 测试 PostgreSQL 连接
psql -h localhost -p 4566 -U root -d dev -c "SELECT 1"

# 3. 创建测试 SOURCE
psql -h localhost -p 4566 -U root -d dev << 'EOF'
CREATE SOURCE test_source (
    id INT,
    name VARCHAR
) WITH (
    connector = 'datagen',
    fields.id.kind = 'sequence',
    fields.id.start = '1',
    fields.name.kind = 'random'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW test_mv AS
SELECT COUNT(*) as total FROM test_source;

SELECT * FROM test_mv;
EOF

# 4. 检查 Nexora API
curl http://localhost:8080/api/risingwave/status | jq .
```

---

## RisingWave 集群模式

### Phase 8: 3 节点 HA 集群 ✅ 生产推荐

**适用场景**: 生产环境（流处理 HA）

#### 快速启动

```bash
# 1. 编译（带 embedded 特性）
cargo build --release --features risingwave,embedded

# 2. 确保 RisingWave 二进制可用
which risingwave || echo "需要安装 RisingWave 二进制"

# 3. 使用示例配置启动
./target/release/nexora \
  --enable-event-streams \
  --embedded-event-streams \
  --event-streams-cluster

# 或使用配置文件
cp nexora-cluster.toml.example nexora.toml
./target/release/nexora --config nexora.toml

# 输出示例:
# 🚀 Nexora 2.0 starting...
#    HTTP:   listening on 0.0.0.0:8080
#    RisingWave Cluster: starting 3-node HA mode...
#      Meta-1:   127.0.0.1:5690 (dashboard: 5691)
#      Meta-2:   127.0.0.1:5692 (dashboard: 5693)
#      Meta-3:   127.0.0.1:5694 (dashboard: 5695)
#      Starting Meta nodes sequentially...
#      ✅ Meta-1 started
#      ✅ Meta-2 started (joined Meta-1)
#      ✅ Meta-3 started (joined Meta-1)
#      Waiting for Raft Leader election...
#      ✅ Leader elected: Meta-1 (node_id=1)
#      Frontend: 127.0.0.1:4566
#      Compute:  127.0.0.1:5688 (parallelism=4)
# ✅ RisingWave cluster started successfully
# ✅ Nexora started successfully
```

#### 完整配置文件

```bash
# 创建生产配置
cat > /etc/nexora/nexora-cluster.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 8080

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/graph"

[event_streams]
enabled = true
embedded = true
cluster_mode = true
data_dir = "/data/risingwave/cluster"
startup_timeout_secs = 120
shutdown_timeout_secs = 30

# Meta 节点 1 (Leader 候选)
[[risingwave.meta_nodes]]
node_id = 1
listen_addr = "127.0.0.1:5690"
advertise_addr = "127.0.0.1:5690"
dashboard_addr = "127.0.0.1:5691"

# Meta 节点 2
[[risingwave.meta_nodes]]
node_id = 2
listen_addr = "127.0.0.1:5692"
advertise_addr = "127.0.0.1:5692"
dashboard_addr = "127.0.0.1:5693"

# Meta 节点 3
[[risingwave.meta_nodes]]
node_id = 3
listen_addr = "127.0.0.1:5694"
advertise_addr = "127.0.0.1:5694"
dashboard_addr = "127.0.0.1:5695"

# Frontend 节点
frontend_addr = "127.0.0.1:4566"

# Compute 节点 1
[[risingwave.compute_nodes]]
listen_addr = "127.0.0.1:5688"
parallelism = 8

# 可选：Compute 节点 2（扩展处理能力）
# [[risingwave.compute_nodes]]
# listen_addr = "127.0.0.1:5689"
# parallelism = 8
EOF

# 创建数据目录
sudo mkdir -p /data/risingwave/cluster
sudo chown nexora:nexora /data/risingwave/cluster

# 启动
./target/release/nexora --config /etc/nexora/nexora-cluster.toml
```

#### 验证集群健康

```bash
# 1. 检查所有进程
ps aux | grep risingwave
# 应该看到:
# - 3 个 meta-node 进程
# - 1 个 frontend-node 进程
# - 1 个 compute-node 进程（或更多）

# 2. 检查端口监听
sudo netstat -tlnp | grep risingwave
# 应该看到:
# 5690, 5691 (Meta-1)
# 5692, 5693 (Meta-2)
# 5694, 5695 (Meta-3)
# 4566 (Frontend)
# 5688 (Compute-1)

# 3. 查询集群状态（API）
curl http://localhost:8080/api/risingwave/cluster | jq .

# 预期输出:
{
  "cluster_mode": true,
  "leader_node_id": 1,
  "meta_nodes": [
    {
      "node_id": 1,
      "is_running": true,
      "address": "127.0.0.1:5690"
    },
    {
      "node_id": 2,
      "is_running": true,
      "address": "127.0.0.1:5692"
    },
    {
      "node_id": 3,
      "is_running": true,
      "address": "127.0.0.1:5694"
    }
  ],
  "frontend": {
    "node_id": 0,
    "is_running": true,
    "address": "127.0.0.1:4566"
  },
  "compute_nodes": [
    {
      "node_id": 0,
      "is_running": true,
      "address": "127.0.0.1:5688"
    }
  ]
}

# 4. 查询 Meta Leader（Dashboard API）
curl http://localhost:5691/cluster_info 2>/dev/null | jq .

# 5. 测试 PostgreSQL 连接
psql -h localhost -p 4566 -U root -d dev -c "SELECT 1"

# 6. 创建测试 MV
psql -h localhost -p 4566 -U root -d dev << 'EOF'
CREATE SOURCE kafka_test WITH (
    connector = 'datagen',
    fields.id.kind = 'sequence',
    fields.id.start = '1'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW count_mv AS
SELECT COUNT(*) FROM kafka_test;

SELECT * FROM count_mv;
EOF
```

#### 故障演练（验证 HA）

```bash
# 1. 找到 Leader Meta 节点
LEADER_PID=$(ps aux | grep "meta-node.*5690" | grep -v grep | awk '{print $2}')
echo "Leader Meta-1 PID: $LEADER_PID"

# 2. Kill Leader
sudo kill -9 $LEADER_PID

# 3. 观察日志（另一个终端）
tail -f /data/risingwave/cluster/meta-*/meta.log | grep -i "elected\|leader"

# 4. 等待重新选举（应 <10 秒）
sleep 10

# 5. 验证新 Leader
curl http://localhost:5693/cluster_info 2>/dev/null | jq .leader

# 6. 测试查询（应该继续工作）
psql -h localhost -p 4566 -U root -d dev -c "SELECT * FROM count_mv"
```

---

## 分布式集群模式

### ⚠️ 警告：不建议生产使用

**原因**: Track A 正确性地基缺失，详见 [`docs/PRODUCTION_READINESS_AUDIT_2026-07-27.md`](docs/PRODUCTION_READINESS_AUDIT_2026-07-27.md)

#### 仅供测试

```bash
# 1. 安装 etcd（需要外部 etcd 集群）
# Ubuntu
sudo apt-get install etcd

# macOS
brew install etcd

# 2. 启动 etcd
etcd --listen-client-urls http://0.0.0.0:2379 \
     --advertise-client-urls http://localhost:2379

# 3. 配置 Nexora 集群
cat > nexora-node1.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 8080

[cluster]
enabled = true
node_id = "node1"
etcd_endpoints = ["http://localhost:2379"]
shard_count = 16
replication_factor = 3

[storage]
data_dir = "/data/nexora/node1/graph"
EOF

# 4. 启动节点 1
./target/release/nexora --config nexora-node1.toml

# 5. 启动节点 2 和 3（类似配置，修改 node_id 和端口）
# ... (省略)

# ⚠️ 警告：此模式可能导致数据丢失，仅供测试
```

---

## 验证与测试

### 健康检查

```bash
# 1. HTTP API 健康检查
curl http://localhost:8080/api/health

# 预期输出:
{
  "status": "healthy",
  "version": "2.0.0",
  "uptime_secs": 3600
}

# 2. RisingWave 状态
curl http://localhost:8080/api/risingwave/status | jq .

# 3. 集群状态（如果启用）
curl http://localhost:8080/api/risingwave/cluster | jq .
```

### 功能测试

#### Cypher 查询

```bash
# 创建节点
curl -X POST http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{
    "query": "CREATE (p:Person {name: \"Alice\", age: 30}) RETURN p"
  }' | jq .

# 查询节点
curl -X POST http://localhost:8080/api/query/cypher \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (p:Person) WHERE p.name = \"Alice\" RETURN p"
  }' | jq .
```

#### SQL 查询

```bash
# 查询节点数
curl -X POST http://localhost:8080/api/query/sql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT COUNT(*) FROM nodes"
  }' | jq .
```

#### PG-Wire 协议

```bash
# 1. 连接 Nexora PG-Wire
psql -h localhost -p 5432 -U admin -d nexora

# 2. 执行 SQL
nexora=> SELECT COUNT(*) FROM nodes;
nexora=> \q

# 3. 连接 RisingWave Frontend
psql -h localhost -p 4566 -U root -d dev

# 4. 查询 MV
dev=> SELECT * FROM count_mv;
dev=> \q
```

### 性能测试

```bash
# 1. 写入性能测试
time for i in {1..1000}; do
  curl -s -X POST http://localhost:8080/api/query/cypher \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"CREATE (:Person {id: $i})\"}"
done

# 2. 查询性能测试
time for i in {1..100}; do
  curl -s -X POST http://localhost:8080/api/query/cypher \
    -H "Content-Type: application/json" \
    -d '{"query": "MATCH (p:Person) RETURN COUNT(p)"}'
done
```

---

## 监控与运维

### 日志查看

```bash
# 1. Nexora 日志（如果直接运行）
# 日志输出到 stdout/stderr

# 2. systemd 日志
sudo journalctl -u nexora -f

# 3. RisingWave 日志
tail -f /data/risingwave/cluster/meta-1/meta.log
tail -f /data/risingwave/cluster/frontend/frontend.log
tail -f /data/risingwave/cluster/compute-1/compute.log
```

### Metrics 收集

```bash
# 1. 暴露 Prometheus metrics（如果启用）
curl http://localhost:8080/metrics

# 2. 配置 Prometheus
cat > /etc/prometheus/prometheus.yml << 'EOF'
scrape_configs:
  - job_name: 'nexora'
    static_configs:
      - targets: ['localhost:8080']
EOF

# 3. 启动 Prometheus
prometheus --config.file=/etc/prometheus/prometheus.yml

# 4. 访问 Prometheus UI
open http://localhost:9090
```

### 备份与恢复

```bash
# 1. 停止 Nexora
sudo systemctl stop nexora

# 2. 备份 RocksDB
tar czf nexora-backup-$(date +%Y%m%d).tar.gz /data/nexora/graph

# 3. 备份 WAL
tar czf nexora-wal-$(date +%Y%m%d).tar.gz /data/nexora/wal

# 4. 备份配置
cp /etc/nexora/nexora.toml nexora-config-$(date +%Y%m%d).toml

# 5. 恢复（示例）
tar xzf nexora-backup-20260727.tar.gz -C /
sudo systemctl start nexora
```

---

## 故障排查

### 常见问题

#### 1. 端口冲突

```bash
# 检查端口占用
sudo lsof -i :8080
sudo lsof -i :5432
sudo lsof -i :4566

# 杀死占用进程
sudo kill -9 <PID>

# 或修改配置使用其他端口
./target/release/nexora --port 8081
```

#### 2. RisingWave 启动失败

```bash
# 检查 RisingWave 二进制
which risingwave
risingwave --version

# 检查日志
tail -100 /data/risingwave/cluster/meta-1/meta.log

# 清理数据重新启动
rm -rf /data/risingwave/cluster/*
./target/release/nexora --config nexora.toml
```

#### 3. RisingWave Leader 未选举

```bash
# 检查 Meta 节点日志
grep -i "raft\|leader\|election" /data/risingwave/cluster/meta-*/meta.log

# 增加启动超时
# 编辑 nexora.toml:
[event_streams]
startup_timeout_secs = 180  # 从 90 增加到 180

# 检查网络连通性
ping 127.0.0.1
nc -zv 127.0.0.1 5690
```

#### 4. PostgreSQL 连接失败

```bash
# 检查 Frontend 是否启动
ps aux | grep frontend-node
sudo lsof -i :4566

# 检查防火墙
sudo ufw status
sudo ufw allow 4566/tcp

# 测试连接
nc -zv localhost 4566
psql -h localhost -p 4566 -U root -d dev -c "SELECT 1"
```

#### 5. 内存不足

```bash
# 检查内存使用
free -h
ps aux --sort=-%mem | head -10

# RisingWave 内存优化
# 减少 Compute 并行度:
[[risingwave.compute_nodes]]
parallelism = 2  # 从 8 减少到 2

# 或增加系统内存
```

### 调试模式

```bash
# 1. 启用详细日志
RUST_LOG=debug ./target/release/nexora

# 2. 启用特定模块日志
RUST_LOG=nexora_risingwave=trace,nexora_core=debug ./target/release/nexora

# 3. 启用 backtrace
RUST_BACKTRACE=1 ./target/release/nexora
```

---

## 生产部署检查清单

### 启动前检查

- [ ] 系统资源充足（CPU、内存、磁盘）
- [ ] 防火墙规则配置正确
- [ ] 数据目录权限正确
- [ ] 配置文件语法正确
- [ ] 依赖服务已启动（etcd、MinIO 等）
- [ ] 备份策略已制定

### 启动后验证

- [ ] 健康检查通过
- [ ] 日志无错误
- [ ] 端口正常监听
- [ ] 基础查询测试通过
- [ ] 性能指标正常
- [ ] 监控告警配置完成

### 生产运行监控

- [ ] CPU 使用率 < 80%
- [ ] 内存使用率 < 80%
- [ ] 磁盘使用率 < 70%
- [ ] 网络延迟 < 10ms
- [ ] 查询 P99 延迟 < 100ms
- [ ] RisingWave Leader 稳定

---

## 快速参考

### 常用命令

```bash
# 启动 Nexora（单节点）
./target/release/nexora --host 0.0.0.0 --port 8080 --allow-unauthenticated

# 启动 Nexora + RisingWave 单节点
./target/release/nexora --enable-event-streams --embedded-event-streams

# 启动 Nexora + RisingWave 集群
./target/release/nexora --event-streams-cluster

# 使用配置文件
./target/release/nexora --config nexora.toml

# 检查健康
curl http://localhost:8080/api/health

# 查看集群状态
curl http://localhost:8080/api/risingwave/cluster | jq .

# 连接 Nexora PG-Wire
psql -h localhost -p 5432 -U admin -d nexora

# 连接 RisingWave
psql -h localhost -p 4566 -U root -d dev

# 查看日志
journalctl -u nexora -f

# 重启服务
sudo systemctl restart nexora
```

### 端口对照表

| 服务 | 默认端口 | 用途 |
|------|---------|------|
| Nexora HTTP API | 8080 | REST API |
| Nexora PG-Wire | 5432 | PostgreSQL 协议 |
| RisingWave Frontend | 4566 | PostgreSQL 协议 |
| RisingWave Meta-1 | 5690, 5691 | Raft RPC, Dashboard |
| RisingWave Meta-2 | 5692, 5693 | Raft RPC, Dashboard |
| RisingWave Meta-3 | 5694, 5695 | Raft RPC, Dashboard |
| RisingWave Compute | 5688 | 内部 RPC |
| Dashboard UI | 8080 | Web 界面 |

---

## 相关文档

- **生产就绪度审核**: [`docs/PRODUCTION_READINESS_AUDIT_2026-07-27.md`](docs/PRODUCTION_READINESS_AUDIT_2026-07-27.md)
- **RisingWave Phase 8 总结**: [`docs/RISINGWAVE_PHASE8_FINAL_SUMMARY.md`](docs/RISINGWAVE_PHASE8_FINAL_SUMMARY.md)
- **RisingWave 快速参考**: [`docs/RISINGWAVE_PHASE8_QUICKREF.md`](docs/RISINGWAVE_PHASE8_QUICKREF.md)
- **架构说明**: [`docs/RISINGWAVE_PGWIRE_ARCHITECTURE.md`](docs/RISINGWAVE_PGWIRE_ARCHITECTURE.md)
- **项目 README**: [`README.md`](../README.md)

---

## 技术支持

- **Issue Tracker**: https://github.com/frank-dkvan/nexora2/issues
- **Discussions**: https://github.com/frank-dkvan/nexora2/discussions
- **文档**: `docs/` 目录

---

**生成时间**: 2026-07-27  
**版本**: 1.0  
**作者**: Claude (Nexora 开发团队)
