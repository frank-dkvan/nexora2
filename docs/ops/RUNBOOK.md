# Nexora 运维 Runbook

> 版本：1.0 | 更新日期：2026-07-18

---

## 集群启动与关闭

### 正常启动（三节点示例）

最小配置：3 个投票者（奇数 quorum）。配置文件示例：

```yaml
# config/examples/cluster-3node.yaml
node_id: node-1
listen_addr: "0.0.0.0:7000"
heartbeat_addr: "0.0.0.0:7001"
total_shards: 256
replication_factor: 3
peers:
  - node_id: node-2
    graph_addr: "10.0.0.2:7000"
    heartbeat_addr: "10.0.0.2:7001"
  - node_id: node-3
    graph_addr: "10.0.0.3:7000"
    heartbeat_addr: "10.0.0.3:7001"
```

启动命令：

```bash
nexora-app \
  --cluster \
  --cluster-config config/examples/cluster-3node.yaml \
  --rocksdb-path /data/nexora \
  --wal-dir /data/nexora/wal \
  --port 8080
```

启动顺序无严格要求，但需在 `failure_timeout`（默认 10 s）内所有节点上线，否则 quorum 无法建立。

### 优雅关停（滚动升级）

**逐节点执行以下步骤，完成一个节点后再操作下一个。**

1. 标记节点"正在排空"——拒绝新写入，等待飞行中请求完成：

   ```bash
   curl -s -X POST http://NODE:8080/api/v2/admin/drain \
        -H "Authorization: Bearer $TOKEN"
   # 等待响应：{"status":"drained","node_id":"node-1"}
   ```

2. 发送 SIGTERM，触发 graceful shutdown（PG drain + graph flush）：

   ```bash
   kill -SIGTERM $(pgrep nexora-app)
   ```

   观察日志关键字：`Flushing active nodes...` 和 `shutdown complete`。

3. 部署新版本二进制，重启节点（保持相同配置）：

   ```bash
   systemctl restart nexora-app@node-1
   ```

4. 确认节点已追赶 follower lag（复制进度归零）后，进行下一个节点：

   ```bash
   curl -s http://NODE:8080/api/v2/cluster/stats | jq '.replication_lag_ms'
   ```

### 强制停止（SIGKILL）

仅在节点无响应时使用。

```bash
kill -9 $(pgrep nexora-app)
```

WAL 会在下次启动时自动 replay（日志关键字 `WAL replay: N records recovered`）。Torn-write repair 会自动截断最后一条未完成的 WAL 记录。数据丢失窗口取决于 `wal_sync_policy`：
- `group`（默认）：最多丢失一个 group-commit 窗口（通常 < 1 ms）。
- `always`：无丢失。
- `every_n` / `never`：丢失 N 条记录或最后一个 fsync 间隔内的写。

---

## 故障排查

### 场景 1：节点宕机

**症状**：`/api/v2/cluster/stats` 中某节点状态变为 `dead`；写到该节点 shard 的请求返回 503。

**处置流程**：

1. 确认节点确实宕机（不是网络分区）：

   ```bash
   curl -s http://DEAD_NODE:8080/api/v2/health/live
   # 应超时或拒绝连接
   ```

2. Nexora 的 heartbeat 检测器在 `failure_timeout`（默认 10 s）后触发 failover。
   观察存活节点的日志：

   ```
   PromotionSucceeded shard=42 new_owner=node-2
   ```

   若看到 `PromotionFailed`，说明 follower 落后过多，需手动触发 state transfer（见场景 1b）。

3. 检查告警指标：

   ```
   replication_quorum_failed_total > 0  →  quorum 写入失败，需立即介入
   ```

4. 重启宕机节点后，它作为 follower 自动追赶（`state_transfer` 或增量 replog replay）。

**场景 1b：Promotion 失败**：

```bash
# 手动触发 shard N 的 state transfer
curl -s -X POST http://NODE:8080/api/v2/admin/state-transfer \
     -H "Content-Type: application/json" \
     -d '{"shard": 42}'
```

---

### 场景 2：脑裂（Split-brain）

**症状**：集群分裂为两个子集，少数侧拒绝写（503 + `kind: "no_quorum"`）。

**诊断**：

```bash
# 检查控制平面 Raft leader 是否稳定
curl -s http://NODE:8080/api/v2/cluster/stats | jq '.raft_leader'
# 告警指标：
#   control_raft_leader_changes 短时间内 > 2  →  选举震荡
```

**处置**：

- 多数侧（≥ ⌈N/2⌉+1 节点）正常服务写入——无需操作。
- 少数侧拒绝写是正确行为，不要绕过。
- 修复网络分区后，少数侧节点会重新加入并追赶日志。
- 若两侧节点数相等（无法形成多数），需手动指定 leader：

  ```bash
  # 强制某节点成为 Raft leader（危险！确保另一侧已停服）
  curl -s -X POST http://NODE:8080/api/v2/cluster/raft/force-leader
  ```

---

### 场景 3：WAL 损坏

**症状**：节点启动时崩溃，日志出现：

```
ERROR: WAL read error: InvalidData("torn write at offset 8192")
```

**处置**：

Nexora 在启动时自动检测并截断最后一条不完整的 WAL 记录（torn-write repair）。日志关键字：

```
WAL truncated at offset 8192 (torn-write repair)
```

若自动修复失败（日志 `WAL repair failed: ...`）：

1. 停止节点。
2. 用备份或 PITR 恢复：

   ```bash
   # 从最近备份恢复
   curl -s -X POST http://NODE:8080/api/v2/admin/restore \
        -H "Content-Type: application/json" \
        -d '{"backup_path": "/data/backups/nexora-20260718-030000.json"}'
   ```

3. 或手动删除损坏的 WAL 文件后重启（**丢失最后一批写**）：

   ```bash
   rm /data/nexora/wal/*.wal
   systemctl restart nexora-app@node-1
   ```

---

### 场景 4：慢查询

**症状**：`x-query-duration-ms` 响应头 > 1000 ms；日志出现 `slow_query` target。

**诊断步骤**：

1. 检查 projection miss rate（无 shard 数据，回源 RocksDB）：

   ```bash
   curl -s http://NODE:8080/api/v2/metrics | jq '.tiered_miss_total'
   ```

2. 若 miss rate 高，触发手动 flush（将 WAL 数据写入 shard 投影）：

   ```bash
   curl -s -X POST http://NODE:8080/api/v2/admin/reindex
   ```

3. 检查 TopologyAwareEviction 是否过于激进地驱逐热节点：

   ```bash
   # 查看 idle sweep 日志（节点驱逐频率）
   grep "idle sweep" /var/log/nexora.log | tail -20
   # 调大 --idle-evict-secs（默认 3600 s）
   ```

4. 检查慢查询日志（Cypher）：

   ```bash
   curl -s http://NODE:8080/api/v2/admin/slow-queries | jq '.'
   ```

---

## 关键 Metrics

| 指标 | 类型 | 健康阈值 | 告警条件 |
|------|------|----------|---------|
| `deepstreaming_events_total` | counter | 单调递增 | 停止增长 > 60 s（无写入） |
| `deepstreaming_sq_matches_total` | counter | 与业务匹配 | 突增 10x（误报规则） |
| `deepstreaming_errors_total` | counter | < 0.1% of events | 错误率 > 1% |
| `deepstreaming_slow_queries_total` | counter | 0 | > 0（需检查查询） |
| `deepstreaming_query_duration_avg_ms` | gauge | < 100 ms | > 500 ms |
| `deepstreaming_wal_append_avg_us` | gauge | < 5000 μs | > 20000 μs（磁盘 I/O 问题） |
| `deepstreaming_active_nodes` | gauge | 与业务预期一致 | 骤降 50%（可能驱逐过激） |
| `replication_quorum_failed_total` | counter | 0 | > 0（立即告警） |
| `control_raft_leader_changes` | counter | < 2 / 10 min | > 5 / 10 min（选举震荡） |

Prometheus 抓取端点：`GET /metrics`（Prometheus text format）
JSON 格式：`GET /api/v2/metrics`

---

## 备份与恢复

### 触发备份

```bash
curl -s -X POST http://NODE:8080/api/v2/admin/backup \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"backup_path": "/data/backups/nexora-$(date +%Y%m%d-%H%M%S).json"}'
```

响应：`{"status":"backup_completed","nodes_exported":<N>,"backup_path":"..."}`

### 恢复

```bash
curl -s -X POST http://NODE:8080/api/v2/admin/restore \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"backup_path": "/data/backups/nexora-20260718-030000.json"}'
```

响应：`{"status":"restore_completed","nodes_restored":<N>,...}`

### PITR（Point-in-Time Recovery）

Nexora 的 WAL 记录每一次写操作并带有时间戳。恢复到某个时间点：

1. 停止节点。
2. 从最近备份恢复基准快照（见上）。
3. 重放 WAL，指定截止时间（单位：microseconds since epoch）：

   ```bash
   nexora-app --replay-wal-until 1753574400000000 \
              --rocksdb-path /data/nexora \
              --wal-dir /data/nexora/wal \
              --no-listen  # 仅 replay，不启动 HTTP 服务
   ```

4. 重新启动节点正常服务。

---

## 滚动升级步骤（完整流程）

适用于三节点集群逐节点升级，**零停机**（quorum 始终保持）。

```
┌────────────────────────────────────────────────────────┐
│  节点 A（升级）   节点 B（正常）   节点 C（正常）       │
└────────────────────────────────────────────────────────┘
```

**对每个节点重复以下步骤**：

```bash
NODE=http://10.0.0.1:8080
TOKEN=$NEXORA_ADMIN_TOKEN

# 步骤 1：排空节点（拒绝新写，等待飞行请求完成）
curl -sf -X POST $NODE/api/v2/admin/drain \
     -H "Authorization: Bearer $TOKEN"
# 等待返回：{"status":"drained","node_id":"node-1"}

# 步骤 2：停止旧版本进程
ssh 10.0.0.1 "kill -SIGTERM $(pgrep nexora-app)"
# 等待日志：nexora shutdown complete

# 步骤 3：部署新版本二进制
rsync -az nexora-app-v2.0.0 10.0.0.1:/usr/local/bin/nexora-app

# 步骤 4：启动新版本
ssh 10.0.0.1 "systemctl start nexora-app@node-1"
# 等待健康检查通过：
until curl -sf $NODE/api/v2/health/ready > /dev/null; do sleep 2; done

# 步骤 5：确认 follower 追赶完成（replication_lag_ms = 0）
curl -s $NODE/api/v2/cluster/stats | jq '.nodes[] | select(.id=="node-1") | .lag_ms'

# 步骤 6：进行下一个节点
```

**注意事项**：

- 每次升级前确保其余两节点健康（`/api/v2/health/ready` 全部返回 200）。
- RF=1 时不能滚动升级（每次只有一个节点，停机即无法写入）；至少需要 RF=3。
- 版本协商（wire protocol v1）：旧节点（< v1.0）接受新客户端；新节点会拒绝 major version != 1 的连接。混合部署时建议在 30 s 内完成连接切换。

## 已知限制与能力边界

> 部署前必读。详细技术债见 `docs/production-planning/ROADMAP_TO_PRODUCTION_LEADING_2026-07-18.md` 附录。

### 写一致性边界（TD-1）

集群写走 **best-effort quorum**（owner 先本地提交，再复制到 follower）：

- **RF=1**：无影响，等价单机。
- **RF>1 且复制 quorum 失败**：客户端收到 **503**，但该写已在 owner 落盘（"孤儿写"）。后台 anti-entropy + failover catch-up 会最终把它同步到 follower——**最终一致，不永久分歧**。
- **运维含义**：收到写 503 不代表数据一定没写入 owner；客户端应幂等重试（平台的 SetProperty/AddEdge 幂等，重试安全）。**不要**假设 503 = 完全未写入。
- **不适用场景**：需要"严格无部分写"的强原子多副本语义（金融级）。当前定位是最终一致 + 幂等重放（对标 Flink/RisingWave）。

### 幂等 op 边界（TD-2）

当前所有写 op（SetProperty / AddEdge）**幂等**，重试/重放收敛。

- **禁止**：在未接入 request_id 幂等键前引入**非幂等 op**（计数器自增、append-only 追加），否则 failover/客户端重试会重复应用。
- 引入非幂等 op 前，先联系开发接 request_id + 两阶段提交（TD-1/TD-2）。

### 监控建议

- `replication_quorum_failed_total` 持续 > 0：说明 RF>1 下有孤儿写在产生，检查 follower 健康 + anti-entropy 修复是否跟上。
- 写 503 速率突增：优先查 follower 可达性，而非 owner。
