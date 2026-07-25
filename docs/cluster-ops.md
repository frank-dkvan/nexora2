# Nexora-RS 集群运维手册

**版本**: 1.0 | **更新日期**: 2026-07-03

> ⚠️ **重要（2026-07-08 更新）**：当前 `--cluster` / `--raft-port` 为**实验性**，**不提供生产级高可用**。分布式写/复制路径尚未接入服务路径——各节点只服务本地图，写入不跨节点复制（复制因子实为 1），**节点故障会丢失该节点数据且无自动恢复**。所谓 "Raft" 无选举、日志为空 payload，非真实共识。本手册描述的部分能力属于尚未落地的目标状态。落地计划与真实现状见 [HA 落地路线图](production-planning/HA_ROADMAP.md)。**请勿依赖多节点部署做容错。**

本手册详细描述 Nexora-RS 图数据库的分布式集群架构、部署配置、监控告警、日常运维和故障排查。

---

## 目录

1. [架构概览](#1-架构概览)
2. [部署](#2-部署)
3. [集群配置](#3-集群配置)
4. [监控](#4-监控)
5. [运维操作](#5-运维操作)
6. [故障排查](#6-故障排查)
7. [Zenoh 传输设置](#7-zenoh-传输设置)
8. [备份与恢复](#8-备份与恢复)
9. [扩缩容](#9-扩缩容)

---

## 1. 架构概览

### 1.1 Actor-per-Node 模型

Nexora-RS 采用 Actor-per-Node 架构：每个图节点运行为独立的异步任务（`NodeTask`），通过消息传递处理读写请求。

```
┌─────────────────────────────────────────────────┐
│                  GraphService                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Shard 0  │  │ Shard 1  │  │ Shard N  │      │
│  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │      │
│  │ │Node A│ │  │ │Node C│ │  │ │Node E│ │      │
│  │ ├──────┤ │  │ ├──────┤ │  │ ├──────┤ │      │
│  │ │Node B│ │  │ │Node D│ │  │ │Node F│ │      │
│  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │      │
│  └──────────┘  └──────────┘  └──────────┘      │
│                      │                          │
│               ┌──────┴──────┐                   │
│               │ Persistor   │                   │
│               │ (RocksDB)   │                   │
│               └─────────────┘                   │
└─────────────────────────────────────────────────┘
```

**核心组件**:
- **NodeTask**: 每个节点的 Actor，处理属性读写、边操作，维护事件日志
- **GraphShard**: 管理一组节点的生命周期，包含 LRU 缓存和节点调度
- **GraphService**: 顶层服务，协调所有分片、持久化和 Standing Query
- **NodeEvent**: 事件溯源模型，所有节点变更以事件形式记录

**优势**:
- 节点级并发：不同节点的操作无锁并行
- 弹性扩展：通过增加分片数线性扩展并发能力
- 事件溯源：完整的变更历史，支持时间旅行查询

### 1.2 一致性哈希（256 分片）

Nexora-RS 使用固定数量的逻辑分片（默认 256），通过一致性哈希将图节点映射到分片:

```
NexoraId → hash(qid) % num_shards → ShardID → OwnerNode
```

**分片特性**:
- 分片数量在集群创建时固定，不支持动态调整
- 每个分片在同一时刻只属于一个节点（owner）
- 分片所有权通过 `ShardMap` 维护，支持版本化更新
- 节点加入/离开时触发分片重分配（rebalance）

**分片路由决策** (`HybridRouter`):
- 本地分片：直接调用本地 `GraphService`
- 远程分片：通过 `TcpRemoteClient` 或 Zenoh 转发到 owner 节点

### 1.3 事件溯源 + WAL

所有节点变更操作通过 Write-Ahead Log (WAL) 持久化:

```
写请求 → NodeEvent → WAL append → RocksDB flush → 响应客户端
```

**WAL 特性**:
- 基于 JSON 序列化的事件日志，支持 AES-256-GCM 加密
- 同步策略可配置: `Never` | `EveryN(n)` | `Always`
- 启动时自动重放 WAL 恢复崩溃前的状态
- WAL 重放是幂等的，支持多次安全重放

**崩溃恢复流程**:
1. 服务启动，打开 RocksDB 和 WAL 目录
2. 调用 `replay_all_wals()` 读取所有 WAL 文件
3. 按顺序重放每个事件，恢复节点状态
4. 重放完成后，服务进入就绪状态

### 1.4 TCP/Zenoh 传输

Nexora-RS 支持两种集群间通信传输:

| 传输方式 | 特点 | 使用场景 |
|----------|------|---------|
| TCP（默认） | 自定义长度前缀 JSON 协议，零外部依赖 | 开发、小规模集群、内网部署 |
| Zenoh（可选） | Eclipse Zenoh P2P 路由，自动发现，多传输支持 | 大规模集群、跨网络部署、动态拓扑 |

TCP 传输使用 4 字节大端长度前缀 + JSON 载荷的简单协议，分为:
- **Graph 操作通道**: 处理 `GraphOperation` 请求/响应
- **心跳通道**: 节点存活检测和 Gossip 传播

---

## 2. 部署

### 2.1 单节点（开发环境）

**Lite 模式（内存，无持久化）**:

```bash
cargo build --release
./target/release/nexora-app --no-rocksdb --port 8080
```

**Durable 模式（RocksDB + WAL）**:

```bash
./target/release/nexora-app \
  --rocksdb-path ./data \
  --wal-dir ./data/wal \
  --port 8080
```

### 2.2 多节点集群（生产环境）

**3 节点集群示例**:

节点 1:
```bash
./target/release/nexora-app \
  --cluster \
  --node-id node-1 \
  --port 8080 \
  --num-shards 256 \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal \
  --cluster-listen-addr 0.0.0.0:7000 \
  --cluster-heartbeat-addr 0.0.0.0:7001 \
  --peer node-2:10.0.0.2:7000:10.0.0.2:7001 \
  --peer node-3:10.0.0.3:7000:10.0.0.3:7001
```

节点 2:
```bash
./target/release/nexora-app \
  --cluster \
  --node-id node-2 \
  --port 8080 \
  --num-shards 256 \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal \
  --cluster-listen-addr 0.0.0.0:7000 \
  --cluster-heartbeat-addr 0.0.0.0:7001 \
  --peer node-1:10.0.0.1:7000:10.0.0.1:7001 \
  --peer node-3:10.0.0.3:7000:10.0.0.3:7001
```

节点 3:
```bash
./target/release/nexora-app \
  --cluster \
  --node-id node-3 \
  --port 8080 \
  --num-shards 256 \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal \
  --cluster-listen-addr 0.0.0.0:7000 \
  --cluster-heartbeat-addr 0.0.0.0:7001 \
  --peer node-1:10.0.0.1:7000:10.0.0.1:7001 \
  --peer node-2:10.0.0.2:7000:10.0.0.2:7001
```

**Peer 格式说明**: `node_id:graph_addr:heartbeat_addr`
- `node_id`: 节点唯一标识
- `graph_addr`: 图操作监听地址（对应 `--cluster-listen-addr`）
- `heartbeat_addr`: 心跳监听地址（对应 `--cluster-heartbeat-addr`）

**带 Raft 共识的集群**:

```bash
./target/release/nexora-app \
  --cluster \
  --node-id node-1 \
  --raft-port 8000 \
  --raft-peer 10.0.0.2:8000 \
  --raft-peer 10.0.0.3:8000 \
  --cluster-listen-addr 0.0.0.0:7000 \
  --cluster-heartbeat-addr 0.0.0.0:7001 \
  --peer node-2:10.0.0.2:7000:10.0.0.2:7001 \
  --peer node-3:10.0.0.3:7000:10.0.0.3:7001 \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal
```

Raft 提供强一致性日志复制，quorum 大小为 `floor(N/2) + 1`（3 节点集群 quorum = 2）。

### 2.3 Docker 部署

**Dockerfile**:

```dockerfile
FROM rust:1.88-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/nexora-app /usr/local/bin/
EXPOSE 8080 7000 7001
VOLUME ["/data"]
ENTRYPOINT ["nexora-app"]
CMD ["--host", "0.0.0.0", "--port", "8080", "--rocksdb-path", "/data", "--wal-dir", "/data/wal"]
```

**docker-compose.yml（3 节点集群）**:

```yaml
version: "3.8"
services:
  node-1:
    build: .
    hostname: node-1
    ports:
      - "8081:8080"
    volumes:
      - node1-data:/data
    command: >
      --cluster
      --node-id node-1
      --port 8080
      --num-shards 256
      --rocksdb-path /data
      --wal-dir /data/wal
      --cluster-listen-addr 0.0.0.0:7000
      --cluster-heartbeat-addr 0.0.0.0:7001
      --peer node-2:node-2:7000:node-2:7001
      --peer node-3:node-3:7000:node-3:7001

  node-2:
    build: .
    hostname: node-2
    ports:
      - "8082:8080"
    volumes:
      - node2-data:/data
    command: >
      --cluster
      --node-id node-2
      --port 8080
      --num-shards 256
      --rocksdb-path /data
      --wal-dir /data/wal
      --cluster-listen-addr 0.0.0.0:7000
      --cluster-heartbeat-addr 0.0.0.0:7001
      --peer node-1:node-1:7000:node-1:7001
      --peer node-3:node-3:7000:node-3:7001

  node-3:
    build: .
    hostname: node-3
    ports:
      - "8083:8080"
    volumes:
      - node3-data:/data
    command: >
      --cluster
      --node-id node-3
      --port 8080
      --num-shards 256
      --rocksdb-path /data
      --wal-dir /data/wal
      --cluster-listen-addr 0.0.0.0:7000
      --cluster-heartbeat-addr 0.0.0.0:7001
      --peer node-1:node-1:7000:node-1:7001
      --peer node-2:node-2:7000:node-2:7001

volumes:
  node1-data:
  node2-data:
  node3-data:
```

### 2.4 系统要求

| 部署规模 | CPU | 内存 | 磁盘 | 网络 |
|----------|-----|------|------|------|
| 开发（单节点） | 2 核 | 2 GB | 10 GB SSD | 任意 |
| 小型生产（3 节点） | 4 核 | 8 GB | 100 GB SSD | 1 Gbps |
| 中型生产（5+ 节点） | 8 核 | 16 GB | 500 GB NVMe | 10 Gbps |
| 大型生产（10+ 节点） | 16 核 | 32 GB | 1 TB NVMe | 10 Gbps+ |

**操作系统要求**:
- Linux: 推荐 Ubuntu 22.04+ / CentOS 8+
- macOS: 支持（开发环境）
- 文件系统: 推荐 ext4 或 xfs
- 文件描述符限制: `ulimit -n 65536`

---

## 3. 集群配置

### 3.1 CLI 参数

集群相关 CLI 参数（定义在 `crates/nexora-app/src/main.rs`）:

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--cluster` | 启用集群模式 | false |
| `--node-id <ID>` | 节点唯一标识 | `node-{port}` |
| `--cluster-listen-addr <ADDR>` | 图操作监听地址 | `0.0.0.0:{port+1000}` |
| `--cluster-heartbeat-addr <ADDR>` | 心跳监听地址 | `0.0.0.0:{port+1001}` |
| `--peer <PEER>` | 种子节点（可多次指定） | — |
| `--num-shards <N>` | 逻辑分片数量 | 256 |
| `--raft-port <PORT>` | Raft RPC 端口（可选） | — |
| `--raft-peer <ADDR>` | Raft 对端地址（可多次指定） | — |

**运行模式验证规则**（`config.rs`）:
- `lite-ephemeral` 模式不能使用 `--cluster`
- `clustered` 模式必须使用 `--cluster`，且不能使用 `--no-rocksdb`
- `single-durable` 模式不能使用 `--cluster`

### 3.2 分片映射配置

分片映射（ShardMap）由 `ControlPlane` 管理:

- **初始状态**: 所有分片分配给当前节点（`ShardMap::new_local`）
- **集群启动后**: `distribute_shards()` 将分片在存活节点间轮询分配
- **版本控制**: ShardMap 有单调递增的版本号，更新时通过 `update_shard_map` 广播

**分片分配算法**: 轮询（round-robin），将 256 个分片均匀分布到 N 个存活节点:
- 3 节点: 每节点约 85-86 个分片
- 5 节点: 每节点约 51-52 个分片

### 3.3 复制因子

当前版本的分片分配为单主模式（每个分片一个 owner）。Raft 共识模式提供日志复制:

- `--raft-port` 启用后，写操作通过 Raft 复制到 quorum 节点
- Quorum = `floor(total_nodes / 2) + 1`
- 3 节点集群可容忍 1 节点故障
- 5 节点集群可容忍 2 节点故障

### 3.4 心跳间隔

心跳协议参数（在 `ClusterConfig` 中定义）:

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `heartbeat_interval` | 心跳发送间隔 | 2 秒 |
| `failure_timeout` | 故障判定超时 | 10 秒 |

心跳协议工作流:
1. 每个节点以 `heartbeat_interval` 频率向所有已知 peer 发送心跳
2. 心跳消息包含: `node_id`, `graph_addr`, `heartbeat_addr`
3. 接收方更新注册表中该节点的最后心跳时间，并回复已知存活节点列表（Gossip）
4. 失败检测器以 `failure_timeout` 频率检查，超时节点标记为 failed 并触发 failover

### 3.5 TOML 配置文件

集群配置也可通过 `nexora.toml` 文件管理:

```toml
[server]
host = "0.0.0.0"
port = 8080

[graph]
num_shards = 256
max_nodes_per_shard = 10000

[cluster]
mode = "peer"
node_id = "node-1"
listen = ["0.0.0.0:7000"]
seeds = ["node-2:10.0.0.2:7000", "node-3:10.0.0.3:7000"]
raft_port = 8000

[storage.rocksdb]
path = "/data/nexora"
write_buffer_size = "64MB"
compression = "lz4"

[storage.wal]
dir = "/data/nexora/wal"
sync_policy = "every_n"
sync_interval = 1000

[logging]
level = "info"
format = "json"
```

---

## 4. 监控

### 4.1 集群统计端点

```
GET /api/v2/cluster/stats
```

仅在集群模式下可用。返回集群状态统计:

```bash
curl http://localhost:8080/api/v2/cluster/stats | jq .
```

响应:
```json
{
  "node_id": "node-1",
  "alive_nodes": 3,
  "total_known_nodes": 3,
  "uptime_secs": 3600,
  "shard_map_version": 5,
  "total_shards": 256,
  "local_shards": 85
}
```

| 字段 | 说明 |
|------|------|
| `node_id` | 当前节点 ID |
| `alive_nodes` | 存活节点数 |
| `total_known_nodes` | 已知节点总数 |
| `uptime_secs` | 当前节点运行时间（秒） |
| `shard_map_version` | 分片映射版本号 |
| `total_shards` | 总分片数 |
| `local_shards` | 当前节点负责的分片数 |

### 4.2 健康检查端点

```
GET /api/v2/health
GET /api/v2/health/ready
GET /api/v2/health/live
```

**完整健康检查**:

```bash
curl http://localhost:8080/api/v2/health | jq .
```

```json
{
  "status": "healthy",
  "mode": "single-node",
  "profile": "clustered",
  "active_nodes": 15234,
  "shards": 256,
  "standing_queries": 3,
  "readiness": "ready",
  "liveness": "alive",
  "durability": "durable",
  "version": "0.1.0"
}
```

**Kubernetes 探针配置**:

```yaml
livenessProbe:
  httpGet:
    path: /api/v2/health/live
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /api/v2/health/ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
```

### 4.3 指标端点

```
GET /api/v2/metrics        # JSON 格式
GET /metrics               # Prometheus 格式
```

**Prometheus 指标**:

```bash
curl http://localhost:8080/metrics
```

输出示例:
```
# HELP nexora_active_nodes Number of active nodes in the graph
# TYPE nexora_active_nodes gauge
nexora_active_nodes 15234
# HELP nexora_standing_queries Number of registered standing queries
# TYPE nexora_standing_queries gauge
nexora_standing_queries 3
# HELP nexora_events_total Total events processed
# TYPE nexora_events_total counter
nexora_events_total 456789
# HELP nexora_sq_matches_total Total standing query matches
# TYPE nexora_sq_matches_total counter
nexora_sq_matches_total 1234
# HELP nexora_errors_total Total errors
# TYPE nexora_errors_total counter
nexora_errors_total 5
```

### 4.4 Raft 状态端点

```
GET /api/v2/cluster/raft
```

仅在启用 Raft 时可用:

```bash
curl http://localhost:8080/api/v2/cluster/raft | jq .
```

```json
{
  "commit_index": 15234,
  "last_applied": 15230,
  "follower_count": 2
}
```

### 4.5 Grafana Dashboard 设置

**Prometheus scrape 配置** (`prometheus.yml`):

```yaml
scrape_configs:
  - job_name: "nexora"
    static_configs:
      - targets:
          - "node-1:8080"
          - "node-2:8080"
          - "node-3:8080"
    metrics_path: /metrics
    scrape_interval: 10s
```

**Grafana Dashboard 核心面板**:

| 面板 | 指标 | PromQL |
|------|------|--------|
| 活跃节点数 | `nexora_active_nodes` | `nexora_active_nodes` |
| 事件吞吐 | `nexora_events_total` | `rate(nexora_events_total[1m])` |
| SQ 匹配率 | `nexora_sq_matches_total` | `rate(nexora_sq_matches_total[1m])` |
| 错误率 | `nexora_errors_total` | `rate(nexora_errors_total[1m])` |
| 集群存活节点 | `nexora_cluster_alive_nodes` | `nexora_cluster_alive_nodes` |
| 分片版本 | `nexora_shard_map_version` | `max(nexora_shard_map_version)` |

**告警规则**:

```yaml
groups:
  - name: nexora
    rules:
      - alert: NexoraNodeDown
        expr: up{job="nexora"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Nexora-RS node {{ $labels.instance }} is down"

      - alert: NexoraHighErrorRate
        expr: rate(nexora_errors_total[5m]) > 10
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High error rate on {{ $labels.instance }}"

      - alert: NexoraShardMapVersionStale
        expr: changes(nexora_shard_map_version[10m]) == 0
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Shard map not updating on {{ $labels.instance }}"
```

---

## 5. 运维操作

### 5.1 添加节点

**步骤**:

1. 在新机器上部署 nexora-app 二进制
2. 准备 RocksDB 数据目录和 WAL 目录
3. 使用 `--peer` 指向现有集群节点启动:

```bash
./target/release/nexora-app \
  --cluster \
  --node-id node-4 \
  --port 8080 \
  --num-shards 256 \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal \
  --cluster-listen-addr 0.0.0.0:7000 \
  --cluster-heartbeat-addr 0.0.0.0:7001 \
  --peer node-1:10.0.0.1:7000:10.0.0.1:7001 \
  --peer node-2:10.0.0.2:7000:10.0.0.2:7001
```

4. 新节点通过心跳 Gossip 协议发现集群中其他节点
5. `ControlPlane.rebalance_shards()` 自动将部分分片迁移到新节点
6. 验证分片分布:

```bash
curl http://localhost:8080/api/v2/cluster/stats | jq .local_shards
```

### 5.2 移除节点

**优雅下线**:

1. 发送 SIGTERM 信号触发优雅关闭:

```bash
kill -TERM $(cat .backend.pid)
```

2. 服务执行优雅关闭流程:
   - 停止接受新请求
   - 等待在途请求完成
   - 刷新所有活跃节点到 RocksDB (`flush_all_nodes`)
   - 关闭集群管理器
   - 关闭 Raft handler（如启用）

3. 其他节点通过心跳超时检测到该节点离开
4. 失败检测器将该节点的分片 failover 到其他节点

**强制下线（节点崩溃）**:

节点崩溃后，其他节点在 `failure_timeout`（默认 10 秒）后自动触发 failover:
- 失败检测器标记节点为 failed
- 该节点拥有的所有分片通过 `failover_shard` 转移到其他存活节点
- ShardMap 更新并广播到全集群

### 5.3 分片重平衡

分片重平衡由 `ControlPlane.rebalance_shards()` 自动执行:

- **触发时机**: 节点加入/离开时自动触发
- **算法**: 轮询分配，将分片均匀分布到所有存活节点
- **版本控制**: 每次重平衡递增 ShardMap 版本号

手动触发检查:

```bash
# 查看当前分片分布
curl http://localhost:8080/api/v2/cluster/stats | jq '{
  total_shards: .total_shards,
  local_shards: .local_shards,
  alive_nodes: .alive_nodes,
  avg_per_node: (.total_shards / .alive_nodes)
}'
```

### 5.4 Failover 过程

当节点故障时的 failover 流程:

```
节点故障 → 心跳超时(10s) → 标记 failed → 收集 owned shards
  → 逐个 failover_shard → 更新 ShardMap → 广播到全集群
```

**代码路径** (`cluster.rs`):

1. `start_failure_detector()` 以 `failure_timeout` 频率运行
2. 检查每个节点的 `last_heartbeat_ms`
3. 超时节点调用 `control_plane.mark_node_failed()` 获取其拥有的分片列表
4. 对每个分片调用 `control_plane.failover_shard(shard_id, self_node_id)`
5. 更新 `HybridRouter` 的 ShardMap，使后续请求路由到新 owner

### 5.5 备份与恢复（WAL 重放）

**备份**:

WAL 文件本身就是增量备份。备份策略:

```bash
# 方式1: 定期拷贝 WAL 文件
cp -r /data/nexora/wal /backup/wal-$(date +%Y%m%d)

# 方式2: 使用 rsync 增量同步
rsync -av /data/nexora/wal/ /backup/wal/

# 方式3: 文件系统快照（如 ZFS/Btrfs）
zfs snapshot nexora/data@backup-$(date +%Y%m%d)
```

**恢复**:

1. 停止服务
2. 将备份的 WAL 文件恢复到 WAL 目录:

```bash
cp -r /backup/wal-20260701 /data/nexora/wal
```

3. 启动服务，WAL 自动重放:

```bash
./target/release/nexora-app \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal
```

4. 日志中会显示重放记录数:
```
WAL replay: 15234 records recovered
```

**完整恢复流程**:

```bash
# 1. 停止服务
kill -TERM $(cat .backend.pid)

# 2. 清理损坏的数据（可选，如果 RocksDB 数据损坏）
mv /data/nexora /data/nexora.corrupt
mkdir -p /data/nexora/wal

# 3. 恢复 WAL 备份
cp -r /backup/wal-20260701/* /data/nexora/wal/

# 4. 重启服务（会自动重放 WAL）
./target/release/nexora-app --rocksdb-path /data/nexora --wal-dir /data/nexora/wal

# 5. 验证数据完整性
curl http://localhost:8080/api/v2/health | jq .active_nodes
```

---

## 6. 故障排查

### 6.1 节点故障诊断

**症状**: 节点无响应或频繁掉线

**诊断步骤**:

1. 检查节点健康状态:
```bash
curl http://<node-ip>:8080/api/v2/health | jq .
```

2. 检查集群状态，确认节点是否被标记为 failed:
```bash
curl http://localhost:8080/api/v2/cluster/stats | jq .
```

3. 查看日志:
```bash
# 搜索故障相关日志
journalctl -u nexora-app | grep -E "(WARN|ERROR|failed|timeout)"

# 或查看日志文件
grep -E "(WARN|ERROR|failed|timeout)" backend.log | tail -50
```

4. 检查网络连通性:
```bash
# 检查图操作端口
telnet <node-ip> 7000

# 检查心跳端口
telnet <node-ip> 7001

# 检查 HTTP 端口
curl -m 5 http://<node-ip>:8080/api/v2/health/live
```

5. 检查系统资源:
```bash
# CPU/内存
top -p $(pgrep nexora-app)

# 磁盘空间
df -h /data/nexora

# 文件描述符
ls /proc/$(pgrep nexora-app)/fd | wc -l
```

### 6.2 WAL 损坏恢复

**症状**: 启动时 WAL 重放失败，日志中出现 `WAL replay error`

**恢复步骤**:

1. 停止服务
2. 检查损坏的 WAL 文件:
```bash
# 查看 WAL 目录
ls -la /data/nexora/wal/

# 尝试读取 WAL 文件（JSON 格式，每行一个事件）
head -5 /data/nexora/wal/*.wal
```

3. 如果 WAL 文件损坏，将其移走并从备份恢复:
```bash
mv /data/nexora/wal /data/nexora/wal.corrupt
mkdir /data/nexora/wal
cp /backup/wal-latest/* /data/nexora/wal/
```

4. 如果没有备份，可以清空 WAL 从 RocksDB 快照恢复（丢失未刷新的最近变更）:
```bash
mv /data/nexora/wal /data/nexora/wal.corrupt
mkdir /data/nexora/wal
```

5. 重启服务

### 6.3 网络分区处理

**症状**: 集群分裂为多个子集，各子集无法互相通信

**诊断**:
- 不同节点报告的 `alive_nodes` 数量不一致
- 心跳连接超时
- 远程图操作返回 `RouterError::Timeout`

**处理**:

1. 确认网络分区:
```bash
# 在每个节点上检查集群视图
for node in node-1 node-2 node-3; do
  echo "=== $node ==="
  curl -m 5 http://$node:8080/api/v2/cluster/stats | jq '{alive: .alive_nodes, total: .total_known_nodes}'
done
```

2. 恢复网络连接后，集群自动通过 Gossip 协议重新合并
3. 分片映射通过 ShardMap 版本号仲裁：版本最高的 ShardMap 胜出
4. 如果使用 Raft，分区恢复后自动通过日志追赶同步数据

**预防**:
- 确保 `failure_timeout` 大于预期网络分区持续时间
- 使用 Raft 共识模式确保强一致性
- 部署多个心跳路径（不同网络接口）

### 6.4 性能调试

**写入延迟高**:

```bash
# 1. 检查 WAL 同步策略
# Never: ~550K ops/s, Always: ~8K ops/s
# 建议生产环境使用 EveryN(100)

# 2. 检查 RocksDB 写缓冲
# 增大 write_buffer_size: 64MB → 128MB
# 增大 max_write_buffers: 3 → 4

# 3. 检查分片热点
curl http://localhost:8080/api/v2/cluster/stats | jq .local_shards
# 如果某节点 local_shards 远超平均值，可能存在热点
```

**查询延迟高**:

```bash
# 1. 检查活跃节点数
curl http://localhost:8080/api/v2/metrics | jq .active_nodes

# 2. 检查 LRU 驱逐
# 如果 active_nodes 接近 max_nodes_per_shard * num_shards，
# 考虑增大 --max-nodes-per-shard

# 3. 查看错误计数
curl http://localhost:8080/api/v2/metrics | jq .errors
```

### 6.5 常见错误场景与解决方案

| 错误 | 原因 | 解决方案 |
|------|------|---------|
| `Node temporarily unavailable` | 节点 Actor 被 LRU 驱逐 | 增大 `--max-nodes-per-shard` |
| `RouterError::NodeNotFound` | 目标节点不在集群中 | 检查 `--peer` 配置，确认节点已加入 |
| `RouterError::Timeout` | 远程操作超时 | 检查网络连通性和负载 |
| `WAL replay error` | WAL 文件损坏 | 从备份恢复或清空 WAL |
| `Persistence error` | RocksDB I/O 错误 | 检查磁盘空间和权限 |
| `Shard not found` | 分片映射不一致 | 等待 ShardMap 自动同步 |
| `quorum not reached` | Raft 多数节点不可用 | 恢复故障节点或调整集群大小 |
| 心跳超时 | 网络分区或节点过载 | 检查网络，增大 `failure_timeout` |

---

## 7. Zenoh 传输设置

### 7.1 何时使用 Zenoh vs TCP

| 场景 | 推荐传输 | 原因 |
|------|----------|------|
| 开发/测试 | TCP | 零依赖，简单 |
| 内网小集群（<5 节点） | TCP | 延迟低，无需额外组件 |
| 跨网段/跨可用区 | Zenoh | 自动路由，NAT 穿透 |
| 动态拓扑（节点频繁加入/离开） | Zenoh | 自动发现，无需静态配置 |
| 大规模集群（10+ 节点） | Zenoh | P2P 路由效率更高 |
| 混合云部署 | Zenoh | 多传输支持（TCP/UDP/Serial） |

### 7.2 Zenoh 会话配置

启用 Zenoh 传输需要编译时启用 `zenoh` feature:

```bash
cargo build --release -p nexora-app --features zenoh
```

Zenoh 模块结构 (`crates/nexora-zenoh/src/lib.rs`):

| 模块 | 说明 |
|------|------|
| `zenoh_cluster` | Zenoh 集群管理器 |
| `zenoh_discovery` | 基于 Zenoh liveliness 的自动发现 |
| `zenoh_transport` | Zenoh 图操作传输层 |

**Zenoh 集群配置**:

Zenoh 集群管理器替代 TCP 的 `ClusterManager`，提供:
- `ZenohClusterManager`: 集群成员管理
- `ZenohGraphServer`: 接收远程图操作
- `ZenohRemoteClient`: 发送远程图操作
- 自动发现: 无需 `--peer` 静态配置

### 7.3 发现机制

Zenoh 使用 Liveliness Token 进行自动节点发现:

1. 每个节点启动时创建 liveliness token，广播自身存在
2. 新节点加入时自动发现所有已存活节点
3. 节点离开时 liveliness token 过期，其他节点自动感知
4. 无需手动配置 `--peer` 列表

对比 TCP 传输:
- TCP: 需要手动配置所有 peer 节点地址
- Zenoh: 自动发现，支持动态拓扑

### 7.4 从 TCP 迁移到 Zenoh

**迁移步骤**:

1. 重新编译启用 Zenoh feature:
```bash
cargo build --release -p nexora-app --features zenoh
```

2. 逐节点滚动升级:
   - 逐个停止 TCP 模式节点
   - 使用 Zenoh 模式重新启动
   - 集群在混合模式下继续运行（TCP 和 Zenoh 节点共存期间通过心跳 Gossip 互通）

3. 验证所有节点已切换:
```bash
# 每个节点检查集群状态
curl http://localhost:8080/api/v2/cluster/stats | jq .
```

4. 移除不再需要的 `--peer` 配置（Zenoh 自动发现）

**注意事项**:
- 迁移过程中确保 `--num-shards` 保持一致
- WAL 数据在传输层切换时保持不变
- 建议在低峰期进行迁移

---

## 8. 备份与恢复

### 8.1 WAL 回放

Nexora-RS 使用 WAL（Write-Ahead Log）保证数据持久性。服务启动时自动回放 WAL：

```bash
# 启动时自动回放 WAL（正常启动即可）
./target/release/nexora-app --rocksdb-path ./data --wal-dir ./data/wal

# 日志输出示例：
# WAL replay: 15423 records recovered
```

WAL 回放流程：
1. 扫描 WAL 目录下的所有 `.wal` 文件
2. 按 sequence number 排序
3. 逐条重放事件到对应的 NodeTask
4. 回放完成后开始接受新请求

### 8.2 WAL 手动备份

```bash
# 停止服务（优雅关闭，确保 WAL 完整）
kill -SIGTERM $(pgrep nexora-app)

# 备份 WAL 目录
tar -czf wal-backup-$(date +%Y%m%d).tar.gz -C ./data wal/

# 备份 RocksDB 数据目录
tar -czf rocksdb-backup-$(date +%Y%m%d).tar.gz -C ./data nexora-data/

# 重启服务
./target/release/nexora-app --rocksdb-path ./data --wal-dir ./data/wal
```

### 8.3 定时备份脚本

```bash
#!/bin/bash
# backup-nexora.sh — 定时备份 Nexora-RS 数据
set -euo pipefail

BACKUP_DIR="/backups/nexora"
DATA_DIR="/var/nexora/data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR"

# 1. 触发 RocksDB checkpoint（在线备份，不停服务）
# 使用 RocksDB 的 Checkpoint API 创建一致性快照
curl -s -X POST http://localhost:8080/api/v2/storage/migrate > /dev/null || true

# 2. 备份 WAL 文件
tar -czf "$BACKUP_DIR/wal-$TIMESTAMP.tar.gz" -C "$DATA_DIR" wal/

# 3. 备份 RocksDB 数据
tar -czf "$BACKUP_DIR/rocksdb-$TIMESTAMP.tar.gz" -C "$DATA_DIR" nexora-data/

# 4. 清理过期备份
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup complete: $BACKUP_DIR/{wal,rocksdb}-$TIMESTAMP.tar.gz"
```

通过 crontab 设置定时执行：
```bash
# 每天凌晨 2 点备份
0 2 * * * /opt/nexora/backup-nexora.sh >> /var/log/nexora-backup.log 2>&1
```

### 8.4 灾难恢复

**场景：RocksDB 数据损坏但 WAL 完好**

```bash
# 1. 停止服务
kill -SIGTERM $(pgrep nexora-app)

# 2. 删除损坏的 RocksDB 数据
rm -rf ./data/nexora-data/

# 3. 从备份恢复 RocksDB
tar -xzf /backups/nexora/rocksdb-20260701_020000.tar.gz -C ./data/

# 4. 重启服务（自动回放 WAL）
./target/release/nexora-app --rocksdb-path ./data --wal-dir ./data/wal

# 5. 验证数据完整性
curl http://localhost:8080/api/v2/health | jq .
curl http://localhost:8080/api/v2/metrics | jq .active_nodes
```

**场景：WAL 损坏**

```bash
# 1. 停止服务
kill -SIGTERM $(pgrep nexora-app)

# 2. 从备份恢复 WAL
tar -xzf /backups/nexora/wal-20260701_020000.tar.gz -C ./data/

# 3. 删除 RocksDB 数据（从上次备份恢复后重新回放 WAL）
rm -rf ./data/nexora-data/
tar -xzf /backups/nexora/rocksdb-20260701_020000.tar.gz -C ./data/

# 4. 重启服务
./target/release/nexora-app --rocksdb-path ./data --wal-dir ./data/wal
```

### 8.5 集群备份策略

| 策略 | 频率 | 保留期 | 适用场景 |
|------|------|--------|---------|
| WAL 文件备份 | 每小时 | 24 小时 | 快速恢复（RPO < 1h） |
| RocksDB 全量备份 | 每天 | 7 天 | 灾难恢复 |
| RocksDB 全量备份（异地） | 每周 | 4 周 | 灾难恢复（异地容灾） |

**RPO/RTO 目标**:
- RPO（恢复点目标）：< 1 小时（通过 WAL 小时备份）
- RTO（恢复时间目标）：< 30 分钟（从备份恢复 + WAL 回放）

---

## 9. 扩缩容

### 9.1 添加新节点

**TCP 模式**：

```bash
# 现有集群: node-1 (port 8080), node-2 (port 8081)
# 添加 node-3

# 1. 启动新节点
./target/release/nexora-app \
  --cluster \
  --node-id node-3 \
  --port 8082 \
  --cluster-listen-addr 127.0.0.1:7004 \
  --cluster-heartbeat-addr 127.0.0.1:7005 \
  --peer node-1:127.0.0.1:7000:127.0.0.1:7001 \
  --peer node-2:127.0.0.1:7002:127.0.0.1:7003 \
  --rocksdb-path ./data/node-3 \
  --wal-dir ./data/node-3/wal

# 2. 验证节点加入
curl http://localhost:8082/api/v2/cluster/stats | jq .
# 确认 nodes 列表包含 node-3

# 3. 更新现有节点的 --peer 配置（需要重启现有节点）
#    或使用 Zenoh 模式自动发现，无需重启
```

**Zenoh 模式**（自动发现，无需配置 peer）：

```bash
./target/release/nexora-app \
  --cluster \
  --node-id node-3 \
  --port 8082 \
  --rocksdb-path ./data/node-3 \
  --wal-dir ./data/node-3/wal
# Zenoh 自动发现集群并加入
```

### 9.2 移除节点

```bash
# 1. 优雅停止目标节点
kill -SIGTERM $(pgrep -f "node-id node-3")

# 2. 确认节点已离开
curl http://localhost:8080/api/v2/cluster/stats | jq .
# 确认 nodes 列表不再包含 node-3

# 3. 在剩余节点上更新 --peer 配置（TCP 模式）
#    Zenoh 模式自动感知节点离开，无需操作

# 4. 可选：备份被移除节点的数据
tar -czf node-3-final-backup.tar.gz -C ./data node-3/
```

### 9.3 分片重平衡

Nexora-RS 使用一致性哈希进行分片分配。添加/移除节点时，分片自动重新分配：

```bash
# 查看分片分布
curl http://localhost:8080/api/v2/cluster/stats | jq .
# {
#   "nodes": ["node-1", "node-2", "node-3"],
#   "total_shards": 256,
#   "local_shards": [86, 85, 85],
#   "shard_map_version": 3
# }

# 分片迁移是自动的，通过以下机制：
# 1. ShardMap 版本号递增
# 2. 新节点通知集群更新分片映射
# 3. 数据按需迁移（lazy migration）
```

**注意事项**:
- 分片数量（`--num-shards`）在集群创建后不可更改
- 所有节点必须使用相同的 `--num-shards` 值
- 添加节点后数据不会自动迁移，新写入的数据会分配到新分片
- 如需立即重平衡，可通过批量查询 + 重新写入实现

### 9.4 滚动升级

```bash
# 1. 逐个节点升级（确保每次只有一个节点离线）
for node in node-1 node-2 node-3; do
  echo "Upgrading $node..."

  # 停止节点
  kill -SIGTERM $(pgrep -f "node-id $node")
  sleep 5

  # 更新二进制
  cp /path/to/new/nexora-app /usr/local/bin/nexora-app

  # 重启节点
  ./target/release/nexora-app --cluster --node-id $node ... &

  # 等待节点恢复
  until curl -s http://localhost:8080/api/v2/health/live | grep -q alive; do
    sleep 1
  done

  echo "$node upgraded."
done
```

### 9.5 容量规划

| 集群规模 | 建议分片数 | 建议内存 | 建议磁盘 | 适用场景 |
|---------|----------|---------|---------|---------|
| 1 节点 | 64 | 4 GB | 50 GB SSD | 开发/测试 |
| 3 节点 | 128 | 8 GB/节点 | 100 GB SSD/节点 | 小型生产 |
| 5 节点 | 256 | 16 GB/节点 | 500 GB SSD/节点 | 中型生产 |
| 10+ 节点 | 512 | 32 GB/节点 | 1 TB NVMe/节点 | 大型生产 |

**分片数选择原则**:
- 每个节点至少 20-30 个分片以实现均衡分配
- 分片数不宜过大（增加管理开销）
- 分片数创建后不可更改，建议预留增长空间

- [ ] 所有节点的 `--num-shards` 值一致
- [ ] 每个节点有唯一的 `--node-id`
- [ ] 图操作端口（7000）和心跳端口（7001）在防火墙中开放
- [ ] RocksDB 数据目录有足够磁盘空间
- [ ] WAL 目录与 RocksDB 在不同磁盘（推荐）
- [ ] 文件描述符限制 >= 65536 (`ulimit -n`)
- [ ] 所有节点的 `--peer` 配置互相指向
- [ ] 如使用 Raft，`--raft-port` 和 `--raft-peer` 配置正确
- [ ] NTP 时间同步已启用（心跳时间戳依赖系统时钟）
- [ ] 监控系统（Prometheus）已配置 scrape target

---

## 附录：端口规划

| 端口 | 用途 | 协议 |
|------|------|------|
| 8080 | HTTP API | TCP |
| 7000 | 集群图操作 | TCP (长度前缀 JSON) |
| 7001 | 集群心跳 | TCP (长度前缀 JSON) |
| 8000 | Raft RPC（可选） | TCP |

端口默认值可通过 CLI 参数调整:
- HTTP: `--port`
- 图操作: `--cluster-listen-addr`（默认 `port + 1000`）
- 心跳: `--cluster-heartbeat-addr`（默认 `port + 1001`）
- Raft: `--raft-port`

---

*本文档基于 Nexora-RS 源码（`crates/nexora-zenoh/`、`crates/nexora-app/src/main.rs`）编写。*
