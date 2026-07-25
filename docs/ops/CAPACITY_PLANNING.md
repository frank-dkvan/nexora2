# Nexora 集群容量规划

> 本文档基于 bench 数据、soak 结果和代码常量估算。生产部署前请用真实负载压测校准数字。

---

## 单节点规格（参考基线）

| 指标 | 估算值 | 依据 |
|------|--------|------|
| 写吞吐（WAL Group Commit） | ~80k–120k ops/s | `group_commit_throughput` bench，max\_ops=256, max\_delay=500μs |
| 写延迟 p99（Group Commit 模式） | < 2ms | flusher 在 max\_delay=500μs 触发；inline flush 在 ops 或 bytes 超限时触发 |
| 写延迟 p99（Always 模式） | < 5ms | 每条写一次 fsync |
| 单节点内存（shard 元数据） | ~500 MB @ 256 shards × 1000 nodes/shard | `max_nodes_per_shard=1000`, ~2 KB/node 估算 |
| 单节点 CPU | 2–4 core 可跑 4 shard worker + WAL flusher | tokio async，单 flusher task per WAL |
| 推荐磁盘 | NVMe SSD（WAL 顺序写敏感）；HDD 写吞吐下降约 10× | |

---

## 集群分片模型

```
total_shards = 256（可配，当前默认 4 用于测试；生产建议 256）
RF = 3（推荐生产 RF，每条数据 3 副本）

写路径：
  客户端 → Router（shard_key mod total_shards → owner）
       → owner 写本地 WAL + GraphService
       → Replication Log → RF-1 个副本

读路径（read-local 优化）：
  客户端 → Router → 就近副本（如本 pod）
```

**节点数与 shard 数的关系：**

- 每个节点平均持有 `total_shards / n_nodes` 个 shard（RF=1）
- RF=3 时每节点持有约 `total_shards × 3 / n_nodes` 个 shard slot
- 建议 `n_nodes` 为奇数（Raft 选举需过半数）

---

## 容量估算公式

### 节点数

```
n_nodes = ceil(target_qps / throughput_per_node)

建议余量：n_nodes_actual = n_nodes × 1.5（故障转移冗余）
奇数约束：若结果为偶数，+1
```

示例：目标 500k write qps，单节点 100k ops/s

```
n_nodes = ceil(500000 / 100000) = 5 → 取奇数 = 5（RF=3 满足过半数）
```

### 内存

```
内存/节点 ≈ max_nodes_per_shard × shards_per_node × ~2 KB

示例（256 shards, 5 nodes, RF=3, max_nodes_per_shard=1000）：
  shards_per_node = 256 × 3 / 5 ≈ 154 shards
  内存/节点 ≈ 1000 × 154 × 2 KB = ~300 MB（纯 shard 元数据）
```

### WAL 磁盘空间

```
磁盘/节点/天 ≈ write_qps × avg_record_size × 86400 × retention_days

示例（100k ops/s, ~200 B/record, 3 天保留）：
  100000 × 200 × 86400 × 3 = ~5 TB/天 WAL（压缩前）
  建议开启 WAL segment rotation + 定期归档
```

---

## 压测命令

```bash
# WAL Group Commit 吞吐基准
cargo bench -p nexora-core --bench group_commit_throughput

# 图写入端到端吞吐（含 WAL + sharding）
cargo bench -p nexora-core --bench add_edge_decomposition

# 完整应用层吞吐
cargo bench -p nexora-app

# 解读输出
# "throughput: X ops/s"：单线程串行写吞吐
# 并发场景：乘以 tokio worker 数（一般 = CPU core 数）
# fsync 不在 bench 热路径（Group Commit），实测需含 sync 回路才反映真实 p99
```

---

## 关键限制与调优旋钮

| 参数 | 位置 | 默认值 | 说明 |
|------|------|--------|------|
| `max_ops` | `WalSyncPolicy::Group` | 256 | ops 批上限，超限触发 inline flush |
| `max_delay` | `WalSyncPolicy::Group` | 500 μs | flusher 最大等待，控制尾延迟 |
| `max_bytes` | `WalSyncPolicy::Group` | `None` | 字节预算（C4 新增）；建议生产设为 `Some(64 * 1024)`（64 KB） |
| `total_shards` | `ClusterConfig` | 4（测试） | 生产建议 256；改变后需完整重分片 |
| `replication_factor` | `ClusterConfig` | 1–3 | RF=3 可容忍 1 节点故障；RF=1 无数据冗余 |
| `max_nodes_per_shard` | `GraphServiceConfig` | 1000 | 超限触发 shard split（未来功能） |
| `failure_timeout` | `ClusterConfig` | 2s（测试）/ 建议 10–30s（生产） | 故障检测灵敏度与误判率权衡 |

### 写放大因子

```
实际写放大 = 1（本地 WAL）+ RF - 1（副本 WAL）= RF
RF=3 → 每条客户端写产生 3 条 WAL 写
+ 周期性 ShardMap snapshot（低频，可忽略）
```

---

## Quorum 与反脑裂

- 控制面所有变更（failover、add\_node、remove\_node、propose\_shard\_map\_update）需要过半数 voter 在线
- 少数侧返回 `ControlError::NoQuorum`，不允许提交（参见 C2 测试 `chaos_split_brain.rs`）
- 最小生产集群：**3 节点**（可容忍 1 节点宕机并保持可写）
- 5 节点集群可容忍 2 节点同时宕机

---

## 常见容量瓶颈与处置

| 瓶颈 | 症状 | 处置 |
|------|------|------|
| WAL fsync 饱和 | 写延迟 p99 > 10ms，磁盘 IO util > 90% | 切换 NVMe；增大 `max_ops` 或 `max_bytes` 以增加批大小 |
| Shard 热点 | 单节点 CPU 100%，其余节点空闲 | 增大 `total_shards` 后重分片；检查 key 分布是否均匀 |
| 内存不足 | OOM，`max_nodes_per_shard` 频繁触达 | 增加内存或减小 `max_nodes_per_shard` + 增加节点数 |
| 网络复制延迟 | 副本 WAL 落后，读到 stale 数据 | 检查网络 BW；减小 RF 或使用 read-from-leader 策略 |
| 脑裂风险 | 偶现 `NoQuorum` 但集群整体健康 | 调大 `failure_timeout`；检查网络分区（VPC/防火墙规则） |
