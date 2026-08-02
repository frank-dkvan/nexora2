# Week 8: 生产验证计划

**执行时间**: 2026-08-02  
**执行人**: Claude (Fable 5)  
**任务来源**: 生产就绪路线图 Week 8

---

## 📊 验证目标

Week 8 的目标是通过 **72小时负载测试 + 混沌工程测试 + Staging部署验证**，确保 Nexora 2 在生产环境下的稳定性和性能。

### 验证维度

| 维度 | 测试方法 | 通过标准 |
|------|----------|----------|
| **持久性** | 72小时连续负载 | 无数据丢失、无崩溃 |
| **性能** | 基准测试套件 | 达到设计指标 |
| **容错** | 混沌工程测试 | 自动恢复、数据一致 |
| **可运维** | Staging部署 | 监控覆盖、可观测 |

---

## 🔥 Task 1: 72小时负载测试

### 测试环境

**集群配置**:
```yaml
# 3节点 Raft 集群
nodes:
  - node1: 127.0.0.1:8080 (Leader)
  - node2: 127.0.0.1:8081 (Follower)
  - node3: 127.0.0.1:8082 (Follower)

resources:
  cpu: 4 cores per node
  memory: 8GB per node
  disk: 100GB NVMe SSD per node

storage:
  graph: RocksDB (local)
  events: MinIO (S3-compatible)
  wal: local file
```

### 负载模式

#### 模式 1: 恒定写入负载（24小时）

**目标**: 验证系统在恒定负载下的稳定性

**负载参数**:
```yaml
duration: 24h
write_qps: 10,000 writes/sec
read_qps: 5,000 reads/sec
query_qps: 1,000 queries/sec

data_pattern:
  node_types: [User, Product, Order, Payment]
  edge_types: [PURCHASED, PAID, SHIPPED, REVIEWED]
  avg_node_size: 500 bytes
  avg_properties: 5 per node
```

**验证指标**:
- ✅ 写入延迟 p99 < 50ms
- ✅ 查询延迟 p99 < 100ms
- ✅ CPU 使用率 < 70%
- ✅ 内存使用率 < 80%
- ✅ 零数据丢失
- ✅ 零崩溃

#### 模式 2: 峰值冲击负载（12小时）

**目标**: 验证系统在流量峰值时的弹性

**负载参数**:
```yaml
duration: 12h
base_qps: 5,000 writes/sec
peak_qps: 50,000 writes/sec
spike_interval: 30min
spike_duration: 5min

pattern:
  - [00:00-00:25] base load
  - [00:25-00:30] ramp up to peak
  - [00:30-00:35] peak load
  - [00:35-00:40] ramp down
  - [00:40-01:00] base load
  - repeat
```

**验证指标**:
- ✅ 峰值期间 p99 延迟 < 200ms
- ✅ 基准期间 p99 延迟 < 50ms
- ✅ 队列无堆积（恢复时间 < 5min）
- ✅ 无 OOM
- ✅ Group Commit 自适应生效

#### 模式 3: 混合负载（24小时）

**目标**: 验证系统在真实业务场景下的表现

**负载参数**:
```yaml
duration: 24h

workload_mix:
  - 60% graph writes (CREATE/UPDATE/DELETE)
  - 20% graph queries (MATCH patterns)
  - 10% event stream ingestion (Kafka → Graph)
  - 5% analytical queries (SQL OLAP)
  - 5% time-travel queries

query_complexity:
  simple: 50% (1-hop traversal)
  medium: 30% (2-3 hop traversal)
  complex: 20% (4+ hop, joins)
```

**验证指标**:
- ✅ 写入吞吐 > 10K/秒
- ✅ 查询吞吐 > 5K/秒
- ✅ 事件摄入延迟 < 100ms
- ✅ OLAP 查询延迟 < 5秒
- ✅ 时间旅行查询可用

#### 模式 4: 长时间运行（12小时）

**目标**: 验证系统长时间运行的稳定性（内存泄漏、资源泄漏）

**负载参数**:
```yaml
duration: 12h
write_qps: 5,000 writes/sec
checkpoint_interval: 10min

monitoring:
  - 内存使用趋势
  - 文件描述符数量
  - RocksDB compaction
  - Iceberg file count
```

**验证指标**:
- ✅ 内存使用曲线平稳（无持续增长）
- ✅ 文件描述符数量稳定
- ✅ RocksDB compaction 正常
- ✅ Checkpoint 正常执行
- ✅ 旧 Checkpoint 清理正常

### 测试脚本

**启动脚本** (`scripts/week8/run_72h_loadtest.sh`):
```bash
#!/bin/bash
set -euo pipefail

echo "🔥 Starting 72-hour load test..."

# 1. 启动 3节点集群
./scripts/start_cluster.sh 3

# 2. 等待集群就绪
./scripts/wait_cluster_ready.sh

# 3. 启动 Prometheus + Grafana 监控
docker-compose -f docker/monitoring.yml up -d

# 4. 运行负载测试
# 模式 1: 恒定负载 (24h)
cargo run --release --bin loadtest -- \
  --duration 24h \
  --write-qps 10000 \
  --read-qps 5000 \
  --pattern constant \
  --report-interval 1m \
  --output results/mode1_constant.json

# 模式 2: 峰值冲击 (12h)
cargo run --release --bin loadtest -- \
  --duration 12h \
  --base-qps 5000 \
  --peak-qps 50000 \
  --spike-interval 30m \
  --spike-duration 5m \
  --pattern spike \
  --output results/mode2_spike.json

# 模式 3: 混合负载 (24h)
cargo run --release --bin loadtest -- \
  --duration 24h \
  --workload-config config/mixed_workload.yaml \
  --pattern mixed \
  --output results/mode3_mixed.json

# 模式 4: 长时间运行 (12h)
cargo run --release --bin loadtest -- \
  --duration 12h \
  --write-qps 5000 \
  --checkpoint-interval 10m \
  --pattern longrun \
  --output results/mode4_longrun.json

# 5. 生成测试报告
./scripts/generate_loadtest_report.sh results/

echo "✅ 72-hour load test completed!"
```

### 监控指标

**实时监控 Dashboard** (Grafana):

1. **系统健康**
   - CPU使用率 (per node)
   - 内存使用率 (per node)
   - 磁盘I/O (per node)
   - 网络带宽

2. **性能指标**
   - 写入吞吐量 (writes/sec)
   - 查询吞吐量 (queries/sec)
   - 写入延迟 (p50/p95/p99)
   - 查询延迟 (p50/p95/p99)

3. **Raft 指标**
   - Leader状态
   - Follower延迟
   - 日志复制速率
   - 选举次数

4. **Event Log 指标**
   - 事件摄入速率
   - Iceberg 文件数量
   - S3 PUT/GET 速率
   - 微批处理批量大小

5. **Checkpoint 指标**
   - Checkpoint 间隔
   - 刷新延迟
   - 分片并行度
   - 恢复时间

### 失败标准

**立即中止测试的条件**:
- 🚨 数据丢失（任何节点）
- 🚨 进程崩溃（无自动恢复）
- 🚨 内存泄漏（使用率持续增长）
- 🚨 写入吞吐下降超过50%
- 🚨 p99延迟超过设计值3倍

**警告但继续测试**:
- ⚠️ 单次延迟峰值（< 1%请求）
- ⚠️ 短暂的CPU峰值（< 5min）
- ⚠️ Raft Leader切换（< 3次/小时）

---

## 🌀 Task 2: 混沌工程测试

### 测试目标

验证系统在各种故障场景下的容错能力和自动恢复能力。

### 故障场景

#### 场景 1: 网络分区（Split Brain）

**测试步骤**:
```bash
# 1. 启动 3节点集群
./scripts/start_cluster.sh 3

# 2. 注入网络分区: node1 vs (node2, node3)
./scripts/chaos/network_partition.sh node1 node2,node3

# 3. 等待 Leader 重新选举
sleep 30s

# 4. 验证: node2 或 node3 成为新 Leader
./scripts/verify_leader.sh

# 5. 继续写入流量到新 Leader
./scripts/loadtest.sh --duration 5m --write-qps 1000

# 6. 恢复网络
./scripts/chaos/heal_partition.sh

# 7. 验证: node1 重新加入集群，数据一致
./scripts/verify_consistency.sh
```

**验证指标**:
- ✅ Leader 重新选举时间 < 30秒
- ✅ 分区期间写入无丢失
- ✅ 恢复后数据一致性
- ✅ 日志自动追赶

#### 场景 2: Follower 节点崩溃

**测试步骤**:
```bash
# 1. 启动 3节点集群
./scripts/start_cluster.sh 3

# 2. 杀死 Follower 节点
kill -9 $(pgrep -f "nexora-app.*8081")

# 3. 继续写入流量
./scripts/loadtest.sh --duration 10m --write-qps 5000

# 4. 重启 Follower
./scripts/start_node.sh node2 8081

# 5. 验证: Follower 自动恢复并追赶日志
./scripts/verify_recovery.sh node2
```

**验证指标**:
- ✅ 集群继续可用（2/3 quorum）
- ✅ 写入无丢失
- ✅ Follower 重启后自动追赶
- ✅ 追赶时间 < 2分钟

#### 场景 3: Leader 节点崩溃

**测试步骤**:
```bash
# 1. 启动 3节点集群
./scripts/start_cluster.sh 3

# 2. 识别当前 Leader
LEADER=$(./scripts/get_leader.sh)

# 3. 杀死 Leader
kill -9 $LEADER

# 4. 验证: 新 Leader 选举
sleep 10s
NEW_LEADER=$(./scripts/get_leader.sh)

# 5. 继续写入到新 Leader
./scripts/loadtest.sh --duration 10m --write-qps 5000

# 6. 重启旧 Leader
./scripts/start_node.sh $LEADER

# 7. 验证: 旧 Leader 以 Follower 身份重新加入
./scripts/verify_follower.sh $LEADER
```

**验证指标**:
- ✅ Leader 选举时间 < 10秒
- ✅ 选举期间无数据丢失
- ✅ 旧 Leader 自动降级为 Follower
- ✅ 数据一致性

#### 场景 4: 磁盘满

**测试步骤**:
```bash
# 1. 启动单节点
./scripts/start_node.sh node1 8080

# 2. 限制磁盘空间
./scripts/chaos/limit_disk.sh node1 1GB

# 3. 持续写入直到磁盘满
./scripts/loadtest.sh --duration 60m --write-qps 10000

# 4. 验证: 系统拒绝写入但不崩溃
./scripts/verify_graceful_degradation.sh

# 5. 清理磁盘空间
./scripts/chaos/cleanup_disk.sh node1

# 6. 验证: 系统自动恢复写入
./scripts/verify_recovery.sh node1
```

**验证指标**:
- ✅ 磁盘满时返回明确错误（非崩溃）
- ✅ 读操作继续可用
- ✅ 清理后自动恢复写入
- ✅ 无数据损坏

#### 场景 5: S3 故障（Iceberg 不可用）

**测试步骤**:
```bash
# 1. 启动集群 + MinIO
./scripts/start_cluster.sh 3

# 2. 停止 MinIO (模拟 S3 故障)
docker stop minio

# 3. 继续写入（应降级到仅 WAL 模式）
./scripts/loadtest.sh --duration 5m --write-qps 1000

# 4. 验证: 图写入继续工作，事件存储失败
./scripts/verify_degraded_mode.sh

# 5. 恢复 MinIO
docker start minio

# 6. 验证: 事件存储自动恢复
./scripts/verify_recovery.sh
```

**验证指标**:
- ✅ 图写入继续可用
- ✅ 事件存储返回明确错误
- ✅ 恢复后事件存储自动工作
- ✅ 无数据丢失

#### 场景 6: 时钟偏移

**测试步骤**:
```bash
# 1. 启动 3节点集群
./scripts/start_cluster.sh 3

# 2. 注入时钟偏移: node1 慢 30秒
./scripts/chaos/clock_skew.sh node1 -30s

# 3. 继续写入
./scripts/loadtest.sh --duration 10m --write-qps 5000

# 4. 验证: Raft 检测时钟偏移并警告
./scripts/verify_clock_skew_warning.sh

# 5. 恢复时钟
./scripts/chaos/fix_clock.sh node1

# 6. 验证: 系统继续正常工作
./scripts/verify_consistency.sh
```

**验证指标**:
- ✅ 检测到时钟偏移并记录警告
- ✅ 写入继续可用（Raft 使用逻辑时钟）
- ✅ 时间戳一致性
- ✅ 数据无损坏

### 混沌测试脚本

**主脚本** (`scripts/week8/run_chaos_tests.sh`):
```bash
#!/bin/bash
set -euo pipefail

echo "🌀 Starting chaos engineering tests..."

# 运行所有场景
./scripts/chaos/scenario1_network_partition.sh
./scripts/chaos/scenario2_follower_crash.sh
./scripts/chaos/scenario3_leader_crash.sh
./scripts/chaos/scenario4_disk_full.sh
./scripts/chaos/scenario5_s3_failure.sh
./scripts/chaos/scenario6_clock_skew.sh

# 生成测试报告
./scripts/generate_chaos_report.sh

echo "✅ Chaos engineering tests completed!"
```

---

## 🚀 Task 3: Staging 部署验证

### 部署架构

**Staging 环境** (Kubernetes):
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nexora
spec:
  replicas: 3
  serviceName: nexora
  template:
    spec:
      containers:
      - name: nexora
        image: nexora:2.0.0
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 9090
          name: metrics
        
        # 健康检查
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
        
        # 资源限制
        resources:
          requests:
            cpu: 2000m
            memory: 4Gi
          limits:
            cpu: 4000m
            memory: 8Gi
        
        # 配置挂载
        volumeMounts:
        - name: config
          mountPath: /etc/nexora
        - name: data
          mountPath: /data/nexora
      
      volumes:
      - name: config
        configMap:
          name: nexora-config
  
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

### 验证清单

#### 1. 部署验证

- [ ] 3个 Pod 成功启动
- [ ] 健康检查通过 (liveness + readiness)
- [ ] Raft 集群自动形成
- [ ] Leader 自动选举

#### 2. 监控验证

- [ ] Prometheus 自动抓取指标
- [ ] Grafana Dashboard 显示正常
- [ ] 告警规则配置正确
- [ ] 日志聚合到 Loki/ELK

#### 3. 运维验证

- [ ] 滚动更新正常
- [ ] Pod 重启自动恢复
- [ ] 数据持久化正常
- [ ] 备份/恢复流程可用

#### 4. 性能验证

- [ ] 写入吞吐 > 10K/秒
- [ ] 查询延迟 p99 < 100ms
- [ ] CPU使用率 < 70%
- [ ] 内存使用率 < 80%

---

## 📊 Week 8 交付物

### 必须完成

1. ✅ **72小时负载测试报告**
   - 测试环境配置
   - 负载模式和参数
   - 性能指标曲线
   - 故障和异常记录
   - 优化建议

2. ✅ **混沌工程测试报告**
   - 故障场景列表
   - 每个场景的测试结果
   - 恢复时间统计
   - 发现的问题和修复
   - 容错能力评估

3. ✅ **Staging 部署验证报告**
   - 部署架构图
   - 验证清单结果
   - 监控截图
   - 运维流程文档
   - 生产部署建议

4. ✅ **生产就绪评估报告**
   - 功能完整性 (95%)
   - 性能达标情况
   - 可靠性评估
   - 可观测性评估
   - 运维就绪度

5. ✅ **生产部署手册**
   - 硬件要求
   - 软件依赖
   - 部署步骤
   - 配置调优
   - 故障排查

6. ✅ **SRE 运维手册**
   - 日常运维检查
   - 告警响应流程
   - 常见故障处理
   - 性能调优指南
   - 备份恢复流程

### 可选（时间允许）

- [ ] 安全渗透测试报告
- [ ] 容量规划建议
- [ ] 成本优化建议
- [ ] 多区域部署方案

---

## ⏱️ 时间安排

| 任务 | 时长 | 开始时间 | 完成时间 |
|------|------|----------|----------|
| 72小时负载测试 | 72h | Day 1 00:00 | Day 4 00:00 |
| 混沌工程测试 | 8h | Day 4 09:00 | Day 4 17:00 |
| Staging 部署验证 | 4h | Day 5 09:00 | Day 5 13:00 |
| 报告编写 | 4h | Day 5 13:00 | Day 5 17:00 |
| **总计** | **88h** | **Day 1** | **Day 5** |

---

## ✅ 成功标准

### 必须达到

- ✅ 72小时测试零崩溃
- ✅ 零数据丢失
- ✅ 性能达到设计指标
- ✅ 所有混沌场景自动恢复
- ✅ Staging部署成功

### 允许的问题

- ⚠️ 非关键路径的警告日志
- ⚠️ 偶发的延迟峰值（< 1%请求）
- ⚠️ 资源使用波动（在限制内）

### 阻塞问题

- 🚨 数据丢失
- 🚨 进程崩溃
- 🚨 内存泄漏
- 🚨 性能衰退
- 🚨 无法自动恢复

---

**文档版本**: 1.0  
**最后更新**: 2026-08-02  
**状态**: 📋 计划中
