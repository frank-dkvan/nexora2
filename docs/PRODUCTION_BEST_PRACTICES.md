# Nexora 2.0/2.1 生产部署最佳实践

> **基于**: 代码库完整盘点分析  
> **日期**: 2026-07-31  
> **版本**: v2.1.0

---

## 🚀 推荐生产部署配置

### 单节点模式（推荐起点）

```bash
# 基础Nexora服务（无RisingWave）
nexora \
  --host 0.0.0.0 \
  --port 8080 \
  --pg-port 5432 \
  --data-dir /data/nexora \
  --event-store-type s3 \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket nexora-events-prod \
  --s3-region us-east-1 \
  --rocksdb-cache-size 4GB \
  --require-auth \
  --jwt-secret-file /etc/nexora/jwt.secret \
  --tls-cert /etc/nexora/cert.pem \
  --tls-key /etc/nexora/key.pem \
  --log-level info
```

**资源配置**:
- CPU: 4核
- 内存: 8GB
- 存储: 100GB SSD (本地) + S3 (事件日志)
- 适用: <10M节点，<50M边

---

### 集群模式（RisingWave集成 - 生产级）

```bash
# 节点1（Leader）
nexora --profile clustered \
  --enable-event-streaming \
  --library-event-streaming \
  --node-id 1 \
  --cluster-peers node2:5690,node3:5690 \
  --pg-port 5432 \
  --risingwave-meta-addr 0.0.0.0:5690 \
  --risingwave-frontend-addr 0.0.0.0:4566 \
  --kafka-brokers kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --replication-factor 3 \
  --require-auth \
  --tls-cert /etc/nexora/cert.pem \
  --tls-key /etc/nexora/key.pem

# 节点2和节点3类似配置，修改 --node-id
```

**资源配置（3节点）**:
- CPU: 36核（12核/节点）
- 内存: 54GB（18GB/节点）
- 存储: 300GB SSD/节点 + S3
- 网络: 10Gbps
- 适用: >10M节点，高可用需求

---

## 🔧 关键技术决策

### 1. RisingWave库模式 vs 独立进程

**推荐**: 库模式（`--library-event-streaming`）

**优势**:
- ✅ 避免外部进程依赖
- ✅ 简化部署和运维
- ✅ 统一日志和监控
- ✅ 降低网络开销

**劣势**:
- ⚠️ 资源隔离较弱（共享内存）
- ⚠️ 需要nightly Rust编译

**适用场景**: 单节点或小型集群（<3节点）

---

### 2. 事件存储选型

**推荐**: S3/MinIO + Apache Iceberg

**配置示例**:
```toml
[event_store]
type = "iceberg"
warehouse = "s3://nexora-events-prod/"
catalog_type = "rest"
catalog_uri = "http://localhost:8181"

[event_store.s3]
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[event_store.iceberg]
format = "parquet"
compression = "zstd"
partition_spec = "day(event_time)"
```

**优势**:
- ✅ 分布式并发写入
- ✅ 时间旅行查询
- ✅ ACID保证
- ✅ 成本效益（S3便宜）

---

### 3. 查询引擎选择

| 场景 | 推荐引擎 | 延迟 |
|------|----------|------|
| 简单图遍历 | Cypher (nexora-core) | <10ms |
| 复杂路径查询 | Cypher (nexora-core) | <100ms |
| 事件分析 | SQL (DataFusion) | 500ms-2s |
| 实时聚合 | RisingWave MV | ~100ms |
| 向量搜索 | HNSW (nexora-hnsw) | <10ms |

---

### 4. 序列化格式

**推荐**: FlatBuffers（零拷贝）

**配置**:
```toml
[serialization]
default_format = "flatbuffers"
compression = "lz4"

# 备选格式
fallback_formats = ["messagepack", "json"]
```

**性能对比**:
| 格式 | 序列化 | 反序列化 | 大小 |
|------|--------|----------|------|
| FlatBuffers | N/A | 0ms (零拷贝) | 100% |
| MessagePack | 5ms | 3ms | 80% |
| JSON | 10ms | 15ms | 150% |

---

### 5. 共识协议

**推荐**: openraft（已集成）

**配置**:
```toml
[consensus]
type = "raft"
election_timeout_ms = 3000
heartbeat_interval_ms = 1000
snapshot_interval = 1000
log_compaction_threshold = 10000

[consensus.storage]
type = "rocksdb"
path = "/data/nexora/raft"
```

**适用**: RisingWave Meta HA，未来Nexora Graph集群

---

## 🔒 安全配置

### 1. 认证和授权

```toml
[auth]
enabled = true
jwt_secret_file = "/etc/nexora/jwt.secret"
jwt_expiration_hours = 24

# RBAC（计划中）
rbac_enabled = false
```

### 2. TLS/SSL

```toml
[tls]
enabled = true
cert_file = "/etc/nexora/cert.pem"
key_file = "/etc/nexora/key.pem"
# 可选：客户端证书验证
client_ca_file = "/etc/nexora/ca.pem"
```

### 3. 网络隔离

```yaml
# Docker Compose示例
services:
  nexora:
    networks:
      - internal  # 仅内部服务可访问
      - external  # 对外暴露
    
  risingwave-meta:
    networks:
      - internal  # 不对外暴露
```

---

## 📊 监控和告警

### Prometheus指标

```bash
# 可用指标
curl http://localhost:8080/metrics

# 关键指标
nexora_graph_node_count
nexora_graph_edge_count
nexora_query_duration_seconds
nexora_event_ingest_rate
nexora_rocksdb_cache_hit_rate
nexora_risingwave_mv_lag_seconds
```

### Grafana面板

**推荐面板**:
1. 图规模（节点/边数量）
2. 查询性能（QPS、延迟分布）
3. 事件摄取速率
4. RocksDB性能（缓存命中率、写放大）
5. RisingWave延迟（端到端）

---

## 🛠️ 性能调优

### RocksDB优化

```toml
[rocksdb]
cache_size = "4GB"
max_background_jobs = 8
write_buffer_size = "128MB"
max_write_buffer_number = 4

# 压缩策略
compression = "lz4"
bottommost_compression = "zstd"

# LSM树调优
level0_file_num_compaction_trigger = 4
level0_slowdown_writes_trigger = 20
level0_stop_writes_trigger = 36
```

### DataFusion优化

```toml
[datafusion]
target_partitions = 8  # = CPU核心数
batch_size = 8192
max_memory = "4GB"
enable_vectorization = true
```

### RisingWave调优

```toml
[risingwave]
# 并行度
parallelism = 8

# 检查点间隔
checkpoint_interval_ms = 10000

# 内存限制
compute_memory_limit = "8GB"
frontend_memory_limit = "2GB"
```

---

## 🔄 备份和恢复

### 1. 事件日志备份（自动）

Iceberg表自动在S3上保存历史版本，无需额外备份。

**快照保留策略**:
```toml
[event_store.retention]
snapshot_retention_days = 30
min_snapshots_to_keep = 10
```

### 2. 图状态备份

```bash
# 创建快照
nexora admin snapshot create --name daily-backup-20260731

# 列出快照
nexora admin snapshot list

# 恢复
nexora admin restore --snapshot daily-backup-20260731
```

**自动化脚本**:
```bash
#!/bin/bash
# 每日备份
SNAPSHOT_NAME="daily-backup-$(date +%Y%m%d)"
nexora admin snapshot create --name $SNAPSHOT_NAME

# 清理30天前的快照
nexora admin snapshot prune --older-than 30d
```

### 3. 时间旅行恢复

```bash
# 查询历史状态（1天前）
nexora query --cypher "MATCH (n) RETURN n" \
  --as-of "2026-07-30T00:00:00Z"

# 恢复到历史状态
nexora admin restore --as-of "2026-07-30T00:00:00Z"
```

---

## 📈 容量规划

### 存储估算

| 图规模 | RocksDB | Iceberg事件日志 | 总计 |
|--------|---------|----------------|------|
| 1M节点 + 5M边 | 30GB | 50GB | 80GB |
| 10M节点 + 50M边 | 300GB | 500GB | 800GB |
| 100M节点 + 500M边 | 3TB | 5TB | 8TB |

**压缩比**: 约30%（RocksDB + Parquet压缩后）

### 内存估算

```
基础内存 = 2GB
RocksDB缓存 = 图大小 * 0.3  # 约30%缓存命中
工作内存 = 2GB
RisingWave（可选）= 2GB/节点

总计（单节点）= 2 + 图大小*0.3 + 2 + 2 ≈ 图大小*0.3 + 6GB
```

**示例**:
- 30GB图 → 约15GB内存
- 300GB图 → 约100GB内存

### CPU估算

- **基准**: 4核支持 ~50k 写入/s
- **扩展**: 线性扩展（8核 → ~100k 写入/s）
- **RisingWave**: 额外4-8核/节点

---

## 🚨 故障排查

### 常见问题

#### 1. 高写延迟

**症状**: 写入延迟 >100ms

**诊断**:
```bash
# 检查RocksDB统计
nexora admin stats rocksdb

# 关注指标
# - write_stall: 写停顿次数
# - pending_compaction_bytes: 待压缩字节数
```

**解决**:
- 增加 `max_background_jobs`
- 调大 `write_buffer_size`
- 检查磁盘I/O

#### 2. 查询慢

**症状**: 查询延迟 >1s

**诊断**:
```bash
# 查看查询计划
nexora query --cypher "MATCH (n) RETURN n" --explain

# 检查索引使用
nexora admin stats indexes
```

**解决**:
- 添加属性索引
- 优化查询模式
- 增加RocksDB缓存

#### 3. RisingWave延迟高

**症状**: 物化视图更新延迟 >500ms

**诊断**:
```bash
# 检查RisingWave状态
nexora risingwave status --verbose

# 关键指标
# - barrier_latency_ms: 检查点延迟
# - backpressure: 反压信号
```

**解决**:
- 增加并行度
- 调整检查点间隔
- 扩容计算节点

---

## 📋 运维检查清单

### 日常检查

- [ ] 监控面板正常（Grafana）
- [ ] 磁盘使用率 <80%
- [ ] 内存使用率 <85%
- [ ] CPU使用率 <70%（平均）
- [ ] 查询P95延迟 <100ms
- [ ] 事件摄取无积压
- [ ] 错误日志无异常

### 每周检查

- [ ] 备份策略执行正常
- [ ] 快照数量符合预期
- [ ] 压缩任务正常运行
- [ ] 索引统计更新
- [ ] 依赖服务健康（Kafka, S3等）

### 每月检查

- [ ] 容量趋势分析
- [ ] 性能基准测试
- [ ] 安全补丁更新
- [ ] 灾难恢复演练
- [ ] 配置审计

---

## 🎓 最佳实践总结

### DO ✅

1. **使用S3+Iceberg**作为事件存储
2. **启用TLS和JWT认证**
3. **监控RocksDB缓存命中率**（目标 >95%）
4. **定期备份**（每日快照）
5. **使用库模式RisingWave**（单节点场景）
6. **启用压缩**（LZ4 + Zstd）
7. **调优RocksDB**（根据负载）
8. **容量规划**（提前6个月）

### DON'T ❌

1. ❌ 不要在生产环境禁用认证
2. ❌ 不要使用本地文件系统存储事件（单节点除外）
3. ❌ 不要忽略磁盘I/O监控
4. ❌ 不要在高峰期执行大批量导入
5. ❌ 不要跳过灾难恢复演练
6. ❌ 不要使用JSON作为主序列化格式
7. ❌ 不要在单节点部署RisingWave集群模式

---

## 📞 支持资源

- **文档**: [docs/](.)
- **Issues**: https://github.com/frank-dkvan/nexora2/issues
- **Discussions**: https://github.com/frank-dkvan/nexora2/discussions
- **功能清单**: [NEXORA2_FEATURE_INVENTORY.md](NEXORA2_FEATURE_INVENTORY.md)

---

**最后更新**: 2026-07-31  
**版本**: v2.1.0
