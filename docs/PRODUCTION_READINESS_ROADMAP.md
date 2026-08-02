# Nexora 2 生产就绪完善路线图

**文档版本**: 1.0  
**创建日期**: 2026-08-02  
**基于**: 全面生产级分布式系统评估报告  
**评估发现**: 86个关键问题，20个重大性能优化机会  
**预估总工作量**: 6-8周（2名全职开发者）

---

## 执行摘要

本路线图提供了将 Nexora 2 从当前状态提升到生产就绪的详细计划。评估发现：

- ✅ **架构设计优秀**: Actor模型、事件溯源、测试覆盖完善
- 🔴 **17个严重问题**: 必须在生产部署前修复（数据丢失、死锁、安全漏洞风险）
- ⚡ **20个性能优化机会**: 可实现10-100倍性能提升
- ⚠️ **23个高危问题**: 强烈建议在生产前修复
- 📊 **可观测性缺失**: 需补齐监控、追踪、健康检查

**目标**: 8周后达到企业级生产就绪标准

---

## 路线图概览

```
Week 1-2: 严重问题修复（阻塞项）
  ├─ Raft 死锁/竞态修复
  ├─ 连接超时保护
  ├─ 认证安全加固
  └─ 资源泄漏修复

Week 3-4: 高危问题 + P0性能优化
  ├─ unwrap()清理（前100个）
  ├─ 边索引重构（100x）
  ├─ 标签索引优化（1000x）
  └─ Raft并行复制（10x）

Week 5-6: 可观测性 + P1性能优化
  ├─ 健康检查端点
  ├─ Prometheus指标
  ├─ Group Commit（50x）
  └─ Iceberg微批处理（10-100x）

Week 7-8: 生产验证
  ├─ 混沌工程测试
  ├─ 72小时负载测试
  ├─ Staging部署验证
  └─ 生产部署指南

Week 9: 金丝雀发布
```

---

## 第1-2周: 严重问题修复（阻塞项）

**目标**: 修复所有17个严重问题，消除数据丢失和系统挂起风险  
**负责人**: 分布式系统专家 + Rust工程师  
**完成标准**: 所有现有测试通过 + 新增故障场景测试覆盖

### 任务1.1: Raft共识层修复（优先级P0）

**预估时间**: 40小时

#### T1.1.1 修复死锁风险（锁顺序不一致）

**文件**: `crates/nexora-raft/src/lib.rs:522-540`  
**问题**: 锁获取顺序取决于响应顺序，可能导致死锁  
**工作量**: 8小时

**实现步骤**:
1. 建立全局锁顺序约定：`config` → `progress`
2. 重构 `replicate()` 方法，在循环外获取锁
3. 添加锁顺序验证的单元测试
4. 添加并发复制的集成测试（模拟死锁场景）

**验收标准**:
- [ ] 100次并发复制测试无死锁
- [ ] 添加 `test_concurrent_replication_no_deadlock()` 测试
- [ ] Code review通过

#### T1.1.2 修复Commit Index竞态条件

**文件**: `crates/nexora-raft/src/lib.rs:245-260`  
**问题**: 读-修改-写无原子性，可导致commit index回退  
**工作量**: 10小时

**实现步骤**:
1. 将 `commit_index` 改为 `AtomicU64`
2. 使用 `compare_exchange` 实现原子更新
3. 添加并发更新的单元测试
4. 验证持久化逻辑正确性

**验收标准**:
- [ ] Commit index单调递增（无回退）
- [ ] 添加 `test_concurrent_commit_index_update()` 测试
- [ ] 100次并发测试通过

#### T1.1.3 Raft并行复制（同时解决性能问题）

**文件**: `crates/nexora-raft/src/lib.rs:307-396`  
**问题**: 顺序复制导致延迟放大  
**工作量**: 12小时

**实现步骤**:
1. 使用 `futures::join_all` 并行发送 `append_entries`
2. 实现超时保护（每个RPC 5秒超时）
3. 添加重试逻辑（失败follower不阻塞其他）
4. 性能基准测试（before/after对比）

**验收标准**:
- [ ] 5-node集群延迟从5×RTT降至1×RTT
- [ ] 吞吐量提升5-10倍
- [ ] 添加 `bench_parallel_replication` 基准测试

#### T1.1.4 全局写锁优化

**文件**: `crates/nexora-consensus/src/raft_impl.rs:261-297`  
**问题**: I/O在锁内执行，限制吞吐量  
**工作量**: 10小时

**实现步骤**:
1. 使用 `AtomicU64` 管理 log_index
2. 将 `storage.append()` 移出锁外
3. 实现乐观并发控制
4. 压力测试验证正确性

**验收标准**:
- [ ] 吞吐量从100 commits/sec提升至10K commits/sec
- [ ] 无数据竞争（Miri测试通过）
- [ ] 添加 `bench_commit_throughput` 基准测试

---

### 任务1.2: 存储与持久化层修复（优先级P0）

**预估时间**: 24小时

#### T1.2.1 Iceberg Catalog连接超时

**文件**: `crates/nexora-eventlog/src/event_log_store.rs:169-175`  
**工作量**: 4小时

**实现步骤**:
1. 使用 `tokio::time::timeout` 包装catalog连接
2. 设置30秒超时（可配置）
3. 添加连接失败的重试逻辑（3次，指数退避）
4. 记录超时事件到日志

**验收标准**:
- [ ] Catalog宕机时30秒内失败（不挂起）
- [ ] 添加 `test_catalog_connection_timeout()` 测试
- [ ] 重试机制测试通过

#### T1.2.2 RisingWave Frontend连接超时

**文件**: `crates/nexora-risingwave/src/library_client.rs:35-48`  
**工作量**: 4小时

**实现步骤**:
1. 为 `tokio_postgres::connect` 添加10秒超时
2. 实现连接池预热机制
3. 添加健康检查探针（定期ping）
4. 连接失败时优雅降级

**验收标准**:
- [ ] Frontend宕机时10秒内返回错误
- [ ] 连接池健康检查正常工作
- [ ] 添加 `test_frontend_connection_timeout()` 测试

#### T1.2.3 Checkpoint fsync持久化

**文件**: `crates/nexora-stream/src/checkpoint.rs`  
**工作量**: 6小时

**实现步骤**:
1. 在 `save_manifest()` 中添加 `file.sync_all()?`
2. 使用临时文件+原子重命名模式
3. 添加checksum验证
4. 实现崩溃恢复测试

**验收标准**:
- [ ] 写入后立即崩溃，重启后checkpoint可恢复
- [ ] 添加 `test_checkpoint_crash_recovery()` 测试
- [ ] Checksum验证通过

#### T1.2.4 Kafka Consumer资源清理

**文件**: `crates/nexora-stream/src/lib.rs`（推断位置）  
**工作量**: 6小时

**实现步骤**:
1. 为 `KafkaSource` 实现 `Drop` trait
2. 在drop中显式调用 `consumer.unsubscribe()`
3. 添加panic时的资源清理测试
4. 验证consumer group快速rebalance

**验收标准**:
- [ ] Consumer panic后5秒内触发rebalance
- [ ] 添加 `test_consumer_cleanup_on_panic()` 测试
- [ ] 无文件描述符泄漏

#### T1.2.5 S3连接池实现

**文件**: `crates/nexora-eventlog/src/event_log_store.rs`  
**工作量**: 4小时

**实现步骤**:
1. 使用 `aws-sdk-s3` 的 `HyperClientBuilder`
2. 配置连接池大小（默认50）
3. 设置连接超时和空闲超时
4. 添加连接池监控指标

**验收标准**:
- [ ] 连接复用率>90%
- [ ] 并发写入无连接耗尽
- [ ] 添加 `test_s3_connection_pool()` 测试

---

### 任务1.3: 安全层加固（优先级P0）

**预估时间**: 20小时

#### T1.3.1 认证密码恒定时间比较

**文件**: `crates/nexora-app/src/auth.rs`（推断位置）  
**工作量**: 6小时

**实现步骤**:
1. 引入 `subtle` crate依赖
2. 使用 `ConstantTimeEq` 替换 `==` 比较
3. 添加时序攻击测试（统计方差）
4. 审查所有HMAC验证代码

**验收标准**:
- [ ] 密码比较时间方差<1μs
- [ ] 添加 `test_constant_time_comparison()` 测试
- [ ] 安全审计通过

#### T1.3.2 强制非空认证密钥

**文件**: `crates/nexora-app/src/main.rs`  
**工作量**: 4小时

**实现步骤**:
1. 启动时检查 `auth_secret` 是否为空
2. 空密钥时拒绝启动（返回错误码2）
3. 移除CLI参数支持，仅保留环境变量
4. 更新文档和示例配置

**验收标准**:
- [ ] 空密钥时启动失败（exit code 2）
- [ ] CLI `--auth-secret` 参数移除
- [ ] 文档更新为仅使用 `NEXORA_AUTH_SECRET`

#### T1.3.3 WAL加密密钥保护

**文件**: `crates/nexora-core/src/config.rs`  
**工作量**: 10小时

**实现步骤**:
1. 集成 `keyring` crate（系统密钥链）
2. 实现密钥轮换机制（支持多版本密钥）
3. 添加密钥导入/导出工具
4. 更新配置为引用密钥ID而非明文

**验收标准**:
- [ ] 密钥不以明文存储在配置文件
- [ ] 密钥轮换测试通过
- [ ] 添加 `nexora-cli key rotate` 命令

---

### 任务1.4: 资源管理修复（优先级P0）

**预估时间**: 16小时

#### T1.4.1 事件缓冲区背压控制

**文件**: `crates/nexora-eventlog/src/event_log_store.rs:887`  
**工作量**: 6小时

**实现步骤**:
1. 将无界channel改为有界channel（容量1000）
2. 实现背压信号传播到上游source
3. 添加缓冲区满时的指标和告警
4. 配置项：`event_buffer_size`

**验收标准**:
- [ ] 慢消费者不导致OOM
- [ ] 背压信号在100ms内传播
- [ ] 添加 `test_event_backpressure()` 测试

#### T1.4.2 Standing Query结果缓冲限制

**文件**: `crates/nexora-standing-query/src/lib.rs`（推断位置）  
**工作量**: 5小时

**实现步骤**:
1. 添加结果缓冲区大小限制（默认10K）
2. 实现LRU淘汰策略
3. 缓冲区满时发送告警
4. 添加监控指标

**验收标准**:
- [ ] 单个查询结果集不超过配置上限
- [ ] LRU淘汰策略正确
- [ ] 添加 `test_standing_query_buffer_limit()` 测试

#### T1.4.3 pgwire连接池限制

**文件**: `crates/nexora-pgwire/src/server.rs`（推断位置）  
**工作量**: 5小时

**实现步骤**:
1. 设置最大并发连接数（默认1000）
2. 连接数达到上限时返回429错误
3. 添加连接空闲超时（默认5分钟）
4. 实现连接监控仪表板

**验收标准**:
- [ ] 最大连接数限制生效
- [ ] 空闲连接自动清理
- [ ] 添加 `test_connection_pool_limit()` 测试

---

### Week 1-2 里程碑检查点

**完成标准**:
- [ ] 所有17个严重问题修复完成
- [ ] 新增40+故障场景测试
- [ ] 所有现有测试通过（1,590+测试）
- [ ] Code review完成（2轮）
- [ ] 更新CHANGELOG.md

**交付物**:
- [ ] 代码提交到 `feat/critical-fixes` 分支
- [ ] 测试覆盖率报告
- [ ] 性能对比报告（before/after）
- [ ] 安全审计报告

---

## 第3-4周: 高危问题 + P0性能优化

**目标**: 修复高危问题，实现关键性能优化（10-1000倍提升）  
**负责人**: 性能优化专家 + 数据结构工程师  
**完成标准**: 吞吐量从500写/秒提升至5K写/秒

### 任务2.1: unwrap()清理（优先级P1）

**预估时间**: 30小时

#### T2.1.1 识别关键路径unwrap()

**工作量**: 10小时

**实现步骤**:
1. 使用静态分析工具扫描所有unwrap()
2. 按调用频率和影响范围排序
3. 识别前100个关键路径unwrap()
4. 分类：数据验证、配置解析、网络I/O

**交付物**:
- [ ] `docs/unwrap_audit.csv`（CSV报告）
- [ ] 优先级排序列表

#### T2.1.2 重构关键路径（批次1: 前50个）

**工作量**: 12小时

**实现步骤**:
1. 图操作路径：返回 `Result<_, GraphError>`
2. WAL写入路径：返回 `Result<_, WalError>`
3. Raft复制路径：返回 `Result<_, ReplicationError>`
4. 添加详细错误上下文

**验收标准**:
- [ ] 前50个unwrap()替换完成
- [ ] 错误处理测试覆盖
- [ ] 错误消息可操作性强

#### T2.1.3 重构配置和启动路径（批次2: 后50个）

**工作量**: 8小时

**实现步骤**:
1. 配置解析：返回 `Result<_, ConfigError>`
2. 启动初始化：返回 `Result<_, StartupError>`
3. 依赖注入：使用 `?` 传播错误
4. 更新错误处理文档

**验收标准**:
- [ ] 所有关键路径unwrap()清理完成
- [ ] CI增加unwrap()检测（clippy规则）
- [ ] 代码审查通过

---

### 任务2.2: P0性能优化（10-1000倍提升）

**预估时间**: 50小时

#### T2.2.1 边索引重构（100-1000倍加速）

**文件**: `crates/nexora-core/src/edge_index.rs`  
**工作量**: 20小时

**当前问题**:
- 平铺结构 `edge_type → HashSet<EdgeTuple>`
- 遍历需O(该类型所有边)扫描

**新设计**:
```rust
// 嵌套索引：edge_type → src → Vec<dst>
struct EdgeIndex {
    forward: DashMap<String, DashMap<NexoraId, Vec<NexoraId>>>,
    reverse: DashMap<String, DashMap<NexoraId, Vec<NexoraId>>>,
}
```

**实现步骤**:
1. 实现新的嵌套索引结构
2. 保持API兼容性（facade模式）
3. 批量迁移现有数据
4. 性能基准测试（before/after）

**验收标准**:
- [ ] 1M边遍历从50ms降至0.05ms（1000倍）
- [ ] 内存减少32字节/边
- [ ] 添加 `bench_edge_traversal_nested_index` 基准
- [ ] 向后兼容性测试通过

#### T2.2.2 标签索引反向映射（1000倍加速）

**文件**: `crates/nexora-core/src/label_index.rs`  
**工作量**: 15小时

**当前问题**:
- `get_labels(node_id)` 需全扫描所有标签
- O(标签数 × 平均节点数)

**新设计**:
```rust
struct LabelIndex {
    forward: DashMap<String, HashSet<NexoraId>>,  // label → nodes
    reverse: DashMap<NexoraId, HashSet<String>>,  // node → labels
}
```

**实现步骤**:
1. 添加反向索引（node → labels）
2. 保持双向索引同步
3. 实现增量更新策略
4. 添加一致性验证测试

**验收标准**:
- [ ] `get_labels()` 从100ms降至0.1ms（1000倍）
- [ ] 双向索引一致性测试通过
- [ ] 内存增加20-40字节/节点（可接受）
- [ ] 添加 `bench_label_lookup_reverse_index` 基准

#### T2.2.3 Iceberg微批处理（10-100倍延迟降低）

**文件**: `crates/nexora-eventlog/src/event_log_store.rs:204-232`  
**工作量**: 15小时

**当前问题**:
- 每次append创建新Iceberg快照
- 50-200ms/append → 最大20-50 appends/sec

**新设计**:
- 批量聚合多个append到单个快照
- 100ms窗口或100事件触发提交

**实现步骤**:
1. 实现批处理缓冲区（bounded channel）
2. 添加定时器触发器（100ms）
3. 实现批量事务提交
4. 添加批处理监控指标

**验收标准**:
- [ ] 单次append延迟从200ms降至1-5ms（40-200倍）
- [ ] 吞吐量从50 appends/sec提升至1000+ appends/sec
- [ ] 添加 `bench_iceberg_micro_batching` 基准
- [ ] 批量失败时正确回滚

---

### 任务2.3: 熔断器和超时策略（优先级P1）

**预估时间**: 20小时

#### T2.3.1 外部服务熔断器

**工作量**: 12小时

**实现步骤**:
1. 引入 `tokio-retry` 和熔断器库
2. 为S3、Kafka、RisingWave添加熔断器
3. 配置：失败率阈值50%，半开状态10秒
4. 熔断器状态监控指标

**验收标准**:
- [ ] S3故障时自动熔断（5秒内）
- [ ] 半开状态自动恢复测试
- [ ] 添加 `test_circuit_breaker_s3()` 测试

#### T2.3.2 超时策略标准化

**工作量**: 8小时

**实现步骤**:
1. 定义超时配置结构体
2. 统一所有网络操作超时设置
3. 实现超时监控和告警
4. 文档化超时决策树

**验收标准**:
- [ ] 所有网络操作有明确超时
- [ ] 超时配置可调（配置文件）
- [ ] 超时事件记录到日志

---

### Week 3-4 里程碑检查点

**完成标准**:
- [ ] 前100个unwrap()清理完成
- [ ] 边索引重构完成（1000倍加速验证）
- [ ] 标签索引优化完成（1000倍加速验证）
- [ ] Iceberg微批处理完成（40倍加速验证）
- [ ] 熔断器机制部署到所有外部服务

**性能指标**:
- [ ] 单节点写吞吐: 1K → 5K 写/秒
- [ ] 边遍历延迟: 50ms → 0.05ms
- [ ] 标签查询延迟: 100ms → 0.1ms
- [ ] Event append延迟(p95): 200ms → 5ms

**交付物**:
- [ ] 性能基准报告（对比图表）
- [ ] 代码提交到 `feat/performance-optimization` 分支
- [ ] 更新架构文档

---

## 第5-6周: 可观测性 + P1性能优化

**目标**: 补齐生产监控能力，实现Group Commit  
**负责人**: DevOps工程师 + 性能专家  
**完成标准**: 完整的监控、追踪、告警体系

### 任务3.1: 可观测性基础设施（优先级P0）

**预估时间**: 40小时

#### T3.1.1 健康检查端点

**文件**: `crates/nexora-app/src/handlers/health.rs`（新建）  
**工作量**: 10小时

**端点设计**:
```rust
GET /health           → 200 OK (简单存活检查)
GET /health/ready     → 200/503 (就绪检查)
GET /health/live      → 200/503 (存活检查)
```

**就绪检查项**:
1. RocksDB可写入
2. Iceberg catalog可连接
3. Raft集群有leader
4. RisingWave frontend响应

**实现步骤**:
1. 实现轻量级健康检查处理器
2. 添加依赖检查逻辑
3. 配置Kubernetes探针
4. 添加健康检查监控

**验收标准**:
- [ ] 健康检查响应时间<10ms
- [ ] 依赖故障时正确返回503
- [ ] Kubernetes readiness/liveness配置文档

#### T3.1.2 Prometheus指标导出

**文件**: `crates/nexora-app/src/metrics/mod.rs`（新建）  
**工作量**: 15小时

**关键指标**:
```
# 图操作
nexora_graph_nodes_total
nexora_graph_edges_total
nexora_graph_query_duration_seconds{quantile="0.5|0.95|0.99"}

# Raft共识
nexora_raft_commit_index
nexora_raft_replication_lag_seconds{follower_id}
nexora_raft_leader_election_total

# Event处理
nexora_events_ingested_total{topic}
nexora_events_projection_duration_seconds

# 资源使用
nexora_rocksdb_size_bytes
nexora_memory_usage_bytes
```

**实现步骤**:
1. 引入 `prometheus` crate
2. 实现指标收集器
3. 添加 `/metrics` 端点
4. 创建Grafana仪表板模板

**验收标准**:
- [ ] 50+关键指标导出
- [ ] Grafana仪表板可导入
- [ ] 指标收集开销<1% CPU

#### T3.1.3 分布式追踪（OpenTelemetry）

**文件**: `crates/nexora-app/src/tracing/mod.rs`（新建）  
**工作量**: 15小时

**实现步骤**:
1. 引入 `opentelemetry` 和 `tracing-opentelemetry`
2. 实现trace context传播（跨服务）
3. 关键操作添加span标记
4. 配置Jaeger exporter

**关键trace span**:
- `cypher_query_execute`
- `raft_replicate`
- `event_project_to_graph`
- `iceberg_append`

**验收标准**:
- [ ] 端到端请求trace可视化
- [ ] Trace采样率可配置（默认1%）
- [ ] Jaeger UI显示完整调用链
- [ ] 添加trace ID到所有日志

---

### 任务3.2: P1性能优化（50倍提升）

**预估时间**: 40小时

#### T3.2.1 Group Commit实现（50倍吞吐提升）

**文件**: `crates/nexora-raft/src/group_commit.rs`（新建）  
**工作量**: 25小时

**当前问题**:
- 每次写入独立fsync → 1ms fsync = 1000写/秒上限

**新设计**:
- 批量聚合写入，单次fsync
- 10ms窗口或1000个写入触发

**实现步骤**:
1. 实现批量写入缓冲区
2. 定时器触发fsync（10ms）
3. 批量通知所有等待者
4. 添加监控指标（批量大小、延迟）

**验收标准**:
- [ ] 吞吐量从1K写/秒提升至50K写/秒（50倍）
- [ ] p99延迟<20ms（可接受的trade-off）
- [ ] 崩溃恢复测试通过（无数据丢失）
- [ ] 添加 `bench_group_commit_throughput` 基准

#### T3.2.2 并行Checkpoint刷新（10倍加速）

**文件**: `crates/nexora-stream/src/checkpoint.rs:497-536`  
**工作量**: 8小时

**当前问题**:
- 4个shard顺序刷新 = 500ms

**实现步骤**:
1. 使用 `tokio::spawn` 并行刷新shard
2. 使用 `futures::join_all` 等待完成
3. 限制并发数（最多4个）
4. 添加并行刷新监控

**验收标准**:
- [ ] Checkpoint延迟从500ms降至50ms（10倍）
- [ ] 添加 `test_parallel_checkpoint_flush()` 测试
- [ ] 并发刷新无数据竞争

#### T3.2.3 Arrow转换单次遍历（2-3倍加速）

**文件**: `crates/nexora-eventlog/src/record_batch_writer.rs:37-75`  
**工作量**: 7小时

**当前问题**:
- 4+次独立迭代同一批事件

**实现步骤**:
1. 重构为单次遍历构建所有列
2. 使用结构体绑定优化内存布局
3. Arrow builder复用
4. 性能对比测试

**验收标准**:
- [ ] 转换时间减少50-70%
- [ ] 内存分配减少50%
- [ ] 添加 `bench_arrow_conversion_single_pass` 基准

---

### 任务3.3: 结构化日志和告警（优先级P1）

**预估时间**: 20小时

#### T3.3.1 结构化日志标准化

**工作量**: 12小时

**实现步骤**:
1. 统一使用 `tracing` crate（已有）
2. 添加trace ID到所有日志
3. 配置JSON格式输出（生产环境）
4. 实现日志采样（高频日志）

**日志级别策略**:
- ERROR: 需要人工介入的错误
- WARN: 潜在问题（熔断器打开、超时）
- INFO: 关键状态变更（leader选举、checkpoint）
- DEBUG: 调试信息（仅开发环境）

**验收标准**:
- [ ] 所有日志包含trace ID
- [ ] JSON日志可被Elasticsearch索引
- [ ] 高频日志采样配置生效

#### T3.3.2 告警规则定义

**文件**: `deploy/prometheus/alerts.yml`（新建）  
**工作量**: 8小时

**关键告警**:
```yaml
- alert: RaftReplicationLagHigh
  expr: nexora_raft_replication_lag_seconds > 10
  severity: critical

- alert: EventIngestionStalled
  expr: rate(nexora_events_ingested_total[5m]) == 0
  severity: warning

- alert: MemoryUsageHigh
  expr: nexora_memory_usage_bytes > 16GB
  severity: warning

- alert: DiskSpaceLow
  expr: disk_free_bytes < 10GB
  severity: critical
```

**验收标准**:
- [ ] 20+告警规则定义
- [ ] AlertManager配置模板
- [ ] 告警测试（模拟触发）

---

### Week 5-6 里程碑检查点

**完成标准**:
- [ ] 健康检查端点部署
- [ ] Prometheus指标导出（50+指标）
- [ ] 分布式追踪集成
- [ ] Group Commit实现（50倍吞吐验证）
- [ ] 结构化日志和告警规则部署

**性能指标**:
- [ ] 单节点写吞吐: 5K → 50K 写/秒
- [ ] 3节点集群吞吐: 500 → 30K 写/秒
- [ ] Checkpoint延迟: 500ms → 50ms
- [ ] Event append延迟(p95): 5ms → 2ms

**交付物**:
- [ ] Grafana仪表板JSON
- [ ] Prometheus告警规则
- [ ] OpenTelemetry配置示例
- [ ] 运维手册（Runbook）

---

## 第7-8周: 生产验证

**目标**: 混沌测试、负载测试、Staging验证  
**负责人**: SRE + QA工程师  
**完成标准**: 通过72小时持续负载测试，无严重问题

### 任务4.1: 混沌工程测试套件（优先级P0）

**预估时间**: 40小时

#### T4.1.1 网络分区测试

**工作量**: 15小时

**测试场景**:
1. **对称分区**: 3节点集群分裂为[1,2]和[3]
2. **非对称分区**: 节点1与2,3失联，但2-3互通
3. **恢复测试**: 分区10秒后恢复

**实现步骤**:
1. 使用 `toxiproxy` 或 `tc` 模拟网络分区
2. 编写自动化测试脚本
3. 验证数据一致性（分区前后比对）
4. 测试leader选举行为

**验收标准**:
- [ ] 对称分区下正确选举新leader（<5秒）
- [ ] 非对称分区下数据无丢失
- [ ] 分区恢复后状态收敛（<30秒）
- [ ] 添加 `test_network_partition_symmetric()` 测试

#### T4.1.2 节点崩溃和恢复

**工作量**: 12小时

**测试场景**:
1. **Leader崩溃**: kill -9 leader进程
2. **Follower崩溃**: kill -9 follower进程
3. **批量崩溃**: 同时kill 2个节点（3节点集群）
4. **崩溃恢复**: 重启后加入集群

**实现步骤**:
1. 编写进程控制脚本
2. 验证WAL回放正确性
3. 测试快照恢复路径
4. 添加自动化测试

**验收标准**:
- [ ] Leader崩溃后5秒内选出新leader
- [ ] 节点重启后正确恢复状态
- [ ] 批量崩溃后数据无丢失
- [ ] 添加 `test_node_crash_recovery()` 测试

#### T4.1.3 磁盘故障模拟

**工作量**: 8小时

**测试场景**:
1. **磁盘满**: 模拟磁盘空间耗尽
2. **I/O错误**: 模拟fsync失败
3. **磁盘慢**: 模拟高延迟I/O

**实现步骤**:
1. 使用 `fio` 或 `dd` 填满磁盘
2. 使用 `failpoint` 注入fsync错误
3. 使用 `blktrace` 模拟慢磁盘
4. 验证优雅降级行为

**验收标准**:
- [ ] 磁盘满时拒绝写入（不崩溃）
- [ ] fsync失败时记录错误并重试
- [ ] 慢磁盘下触发告警
- [ ] 添加 `test_disk_failure_scenarios()` 测试

#### T4.1.4 时钟偏移测试

**工作量**: 5小时

**测试场景**:
1. **时钟快进**: 节点时间+5分钟
2. **时钟慢速**: 节点时间-5分钟
3. **时钟漂移**: 逐渐偏移

**实现步骤**:
1. 使用 `faketime` 控制系统时间
2. 测试Raft选举超时行为
3. 测试Event时间戳处理
4. 验证时间戳单调性

**验收标准**:
- [ ] 时钟偏移不导致数据丢失
- [ ] Event时间戳保持单调
- [ ] 添加 `test_clock_skew()` 测试

---

### 任务4.2: 负载测试（优先级P0）

**预估时间**: 40小时

#### T4.2.1 持续负载测试（72小时）

**工作量**: 20小时（准备10h + 监控10h）

**测试配置**:
```yaml
cluster: 3 nodes (8 cores, 16GB RAM each)
workload:
  - write_rate: 10K ops/sec
  - read_rate: 50K ops/sec
  - query_rate: 1K queries/sec
duration: 72 hours
```

**监控指标**:
- CPU使用率（目标: <70%）
- 内存使用率（目标: 稳定，无泄漏）
- 磁盘I/O（目标: 无积压）
- 网络带宽（目标: <80%）
- p99延迟（目标: <100ms）

**实现步骤**:
1. 编写负载生成器（基于nexora-bench）
2. 配置监控仪表板
3. 设置自动告警
4. 每8小时检查一次关键指标

**验收标准**:
- [ ] 72小时无崩溃、无OOM
- [ ] 内存使用稳定（无泄漏）
- [ ] p99延迟<100ms
- [ ] 无数据丢失（校验和验证）

#### T4.2.2 峰值负载测试

**工作量**: 10小时

**测试场景**:
1. **写入峰值**: 50K ops/sec持续10分钟
2. **查询峰值**: 10K queries/sec持续10分钟
3. **混合峰值**: 同时执行

**实现步骤**:
1. 配置峰值负载脚本
2. 监控系统行为
3. 验证背压机制
4. 测试自动恢复

**验收标准**:
- [ ] 峰值负载下无崩溃
- [ ] 背压机制正常工作
- [ ] 恢复到正常负载后性能恢复
- [ ] 添加 `test_peak_load()` 测试

#### T4.2.3 容量规划测试

**工作量**: 10小时

**测试目标**:
- 单节点最大节点数
- 单节点最大边数
- 单节点最大查询并发
- 集群最大写入吞吐

**实现步骤**:
1. 逐步增加数据规模
2. 记录性能拐点
3. 绘制容量曲线
4. 生成容量规划文档

**验收标准**:
- [ ] 单节点支持10M+节点
- [ ] 单节点支持100M+边
- [ ] 文档化容量限制和推荐配置
- [ ] 生成 `docs/CAPACITY_PLANNING.md`

---

### 任务4.3: Staging环境部署（优先级P0）

**预估时间**: 30小时

#### T4.3.1 Kubernetes部署

**工作量**: 15小时

**实现步骤**:
1. 创建Helm chart（`deploy/helm/nexora/`）
2. 配置StatefulSet（3副本）
3. 配置Service和Ingress
4. 配置ConfigMap和Secret

**Helm chart结构**:
```
deploy/helm/nexora/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── statefulset.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── servicemonitor.yaml
```

**验收标准**:
- [ ] Helm chart可一键部署
- [ ] 滚动升级测试通过
- [ ] PVC自动创建和挂载
- [ ] 添加 `deploy/helm/README.md`

#### T4.3.2 CI/CD管道

**工作量**: 10小时

**实现步骤**:
1. 配置GitHub Actions自动构建
2. 推送Docker镜像到registry
3. 自动部署到Staging环境
4. 运行冒烟测试

**Pipeline阶段**:
```
1. Build (cargo build --release)
2. Test (cargo test --all)
3. Docker Build (multi-stage build)
4. Push to Registry
5. Deploy to Staging
6. Smoke Test
7. Notify (Slack/Email)
```

**验收标准**:
- [ ] PR合并后自动部署到Staging
- [ ] 部署失败时自动回滚
- [ ] 冒烟测试覆盖关键功能
- [ ] 添加 `.github/workflows/deploy-staging.yml`

#### T4.3.3 生产部署指南

**文件**: `docs/PRODUCTION_DEPLOYMENT.md`  
**工作量**: 5小时

**文档内容**:
1. 系统要求（CPU、内存、磁盘）
2. 网络拓扑（端口、防火墙）
3. 部署步骤（逐步指南）
4. 配置调优（生产推荐值）
5. 备份和恢复流程
6. 故障排查清单

**验收标准**:
- [ ] 运维团队可独立部署
- [ ] 文档包含所有必要配置
- [ ] 添加故障决策树图

---

### 任务4.4: 灾难恢复测试（优先级P0）

**预估时间**: 20小时

#### T4.4.1 备份和恢复流程

**工作量**: 10小时

**备份范围**:
1. RocksDB数据目录
2. WAL日志
3. Iceberg catalog元数据
4. Raft快照

**实现步骤**:
1. 编写自动化备份脚本
2. 实现增量备份（基于快照）
3. 测试恢复流程（全量+增量）
4. 验证数据完整性

**验收标准**:
- [ ] 全量备份完成时间<30分钟（100GB数据）
- [ ] 增量备份完成时间<5分钟
- [ ] 恢复后数据一致性验证通过
- [ ] 添加 `scripts/backup-nexora.sh`

#### T4.4.2 灾难恢复演练

**工作量**: 10小时

**演练场景**:
1. **数据中心故障**: 整个可用区不可用
2. **数据损坏**: RocksDB文件损坏
3. **误删除**: 用户误删除关键数据

**实现步骤**:
1. 从备份恢复到新集群
2. 验证数据完整性
3. 测试服务切换（DNS切换）
4. 记录RTO和RPO

**验收标准**:
- [ ] RTO (Recovery Time Objective) <1小时
- [ ] RPO (Recovery Point Objective) <5分钟
- [ ] 灾难恢复文档完整
- [ ] 生成 `docs/DISASTER_RECOVERY.md`

---

### Week 7-8 里程碑检查点

**完成标准**:
- [ ] 混沌测试套件完成（4个场景）
- [ ] 72小时负载测试通过
- [ ] Staging环境部署成功
- [ ] 灾难恢复演练通过
- [ ] 生产部署指南完成

**质量指标**:
- [ ] 72小时测试无崩溃
- [ ] 内存使用稳定（无泄漏）
- [ ] p99延迟<100ms
- [ ] 数据一致性验证通过
- [ ] 灾难恢复RTO<1小时

**交付物**:
- [ ] 混沌测试报告
- [ ] 负载测试报告（性能曲线图）
- [ ] Helm chart
- [ ] 生产部署指南
- [ ] 灾难恢复手册

---

## 第9周: 金丝雀发布

**目标**: 生产环境平滑上线  
**负责人**: SRE团队  
**完成标准**: 金丝雀流量无异常，逐步扩展至100%

### 阶段9.1: 金丝雀部署（1%流量）

**时间**: 第9周周一-周三  
**流量**: 1%生产流量

**实施步骤**:
1. 部署1个金丝雀实例（独立namespace）
2. 配置Ingress路由1%流量
3. 24小时监控关键指标
4. 无异常后进入下一阶段

**监控指标**:
- 错误率（目标: <0.1%）
- p99延迟（目标: <100ms）
- 内存使用（目标: 稳定）
- 数据一致性（实时验证）

**回滚条件**:
- 错误率>0.5%
- p99延迟>200ms
- 内存泄漏
- 数据不一致

---

### 阶段9.2: 扩展至10%流量

**时间**: 第9周周三-周五  
**流量**: 10%生产流量

**实施步骤**:
1. 扩展金丝雀实例至3个
2. 配置Ingress路由10%流量
3. 48小时监控
4. 用户反馈收集

**验收标准**:
- [ ] 48小时无严重问题
- [ ] 用户反馈正面
- [ ] 关键业务指标正常

---

### 阶段9.3: 全量发布

**时间**: 第9周周末  
**流量**: 100%生产流量

**实施步骤**:
1. 逐步扩展至25% → 50% → 100%
2. 每个阶段间隔4小时
3. 持续监控所有指标
4. 保留旧版本1周（快速回滚）

**验收标准**:
- [ ] 全量发布成功
- [ ] 旧版本下线
- [ ] 运维团队培训完成
- [ ] 上线报告归档

---

## 资源规划

### 人员配置

| 角色 | 人数 | 参与阶段 | 总工时 |
|------|------|----------|--------|
| **分布式系统专家** | 1 | Week 1-2 | 80h |
| **Rust工程师** | 1 | Week 1-4 | 160h |
| **性能优化专家** | 1 | Week 3-6 | 160h |
| **DevOps工程师** | 1 | Week 5-9 | 200h |
| **SRE工程师** | 1 | Week 7-9 | 120h |
| **QA工程师** | 1 | Week 7-8 | 80h |
| **技术文档工程师** | 0.5 | Week 1-8 | 80h |
| **总计** | 6.5人 | 9周 | **880小时** |

### 成本估算

| 项目 | 成本估算（USD） |
|------|----------------|
| **人力成本** | $88,000 (880h × $100/h平均) |
| **云基础设施** | $3,000 (测试环境2个月) |
| **工具许可** | $1,000 (Grafana Cloud, Datadog等) |
| **外部咨询** | $5,000 (安全审计、性能调优) |
| **培训费用** | $2,000 (运维团队培训) |
| **应急预留** | $9,000 (10%缓冲) |
| **总计** | **$108,000** |

### 风险预留时间

| 风险场景 | 概率 | 影响 | 预留时间 |
|---------|------|------|---------|
| Raft重构复杂度超预期 | 中 | 高 | +1周 |
| 性能优化未达预期 | 低 | 中 | +0.5周 |
| 负载测试发现新问题 | 中 | 高 | +1周 |
| 灾难恢复流程问题 | 低 | 高 | +0.5周 |
| **总缓冲** | - | - | **+3周** |

**最坏情况时间线**: 9周 + 3周缓冲 = **12周**

---

## 验收标准总览

### 功能完整性

- [x] 所有现有测试通过（1,590+测试）
- [ ] 17个严重问题全部修复
- [ ] 100个关键unwrap()清理
- [ ] 健康检查端点部署
- [ ] Prometheus指标导出（50+指标）
- [ ] 分布式追踪集成

### 性能指标

| 指标 | 当前 | 目标 | 达成 |
|------|------|------|------|
| 单节点写吞吐 | 1K/秒 | 50K/秒 | [ ] |
| 3节点集群吞吐 | 500/秒 | 30K/秒 | [ ] |
| 边遍历延迟(1M边) | 50ms | 0.05ms | [ ] |
| 标签查询延迟(1K标签) | 100ms | 0.1ms | [ ] |
| Event append延迟(p95) | 200ms | 2ms | [ ] |
| Checkpoint延迟 | 500ms | 50ms | [ ] |

### 可靠性指标

- [ ] 72小时负载测试无崩溃
- [ ] 网络分区恢复<30秒
- [ ] 节点崩溃恢复<5秒
- [ ] 内存使用稳定（无泄漏）
- [ ] 数据一致性验证通过
- [ ] 灾难恢复RTO<1小时

### 可观测性

- [ ] 50+关键指标导出
- [ ] 20+告警规则部署
- [ ] Grafana仪表板可用
- [ ] 分布式追踪端到端可视化
- [ ] 所有日志包含trace ID

### 文档完整性

- [ ] 生产部署指南
- [ ] 灾难恢复手册
- [ ] 运维手册（Runbook）
- [ ] 容量规划文档
- [ ] API文档更新
- [ ] 故障排查指南

---

## 后续优化（生产后）

### P2优化（Week 10-14）

**目标**: 中优先级问题修复，P2性能优化

**主要任务**:
1. 修复31个中优先级问题
2. 实现查询结果缓存
3. 优化边属性存储
4. 实现动态shard rebalancing
5. 添加只读副本支持

**预估工作量**: 4周，2人

---

### P3优化（Week 15-20）

**目标**: 低优先级问题，高级特性

**主要任务**:
1. 修复15个低优先级问题
2. 实现机器学习模型集成
3. 图算法库（PageRank、社区检测）
4. 联邦查询支持
5. 多区域主-主复制

**预估工作量**: 6周，3人

---

### 持续改进

**长期目标**:
1. **Q1 2027**: 完成所有P2优化
2. **Q2 2027**: 完成所有P3优化
3. **Q3 2027**: 达到Neo4j性能对标
4. **Q4 2027**: 实现多区域部署

---

## 附录

### A. 关键文件清单

**需修复的关键文件**:

```
# 严重问题（Week 1-2）
crates/nexora-raft/src/lib.rs                      # 死锁、竞态、并行复制
crates/nexora-consensus/src/raft_impl.rs           # 全局写锁
crates/nexora-eventlog/src/event_log_store.rs      # 连接超时、连接池
crates/nexora-risingwave/src/library_client.rs     # 连接超时
crates/nexora-stream/src/checkpoint.rs             # fsync、并行刷新
crates/nexora-stream/src/lib.rs                    # Kafka清理、背压
crates/nexora-app/src/auth.rs                      # 恒定时间比较
crates/nexora-app/src/main.rs                      # 密钥验证

# 性能优化（Week 3-4）
crates/nexora-core/src/edge_index.rs               # 嵌套索引重构
crates/nexora-core/src/label_index.rs              # 反向映射
crates/nexora-eventlog/src/record_batch_writer.rs  # 单次遍历

# 可观测性（Week 5-6）
crates/nexora-app/src/handlers/health.rs           # 新建
crates/nexora-app/src/metrics/mod.rs               # 新建
crates/nexora-app/src/tracing/mod.rs               # 新建
crates/nexora-raft/src/group_commit.rs             # 新建
```

### B. 测试覆盖清单

**新增测试（预估100+）**:

```rust
// Week 1-2: 严重问题测试
test_concurrent_replication_no_deadlock()
test_concurrent_commit_index_update()
test_catalog_connection_timeout()
test_frontend_connection_timeout()
test_checkpoint_crash_recovery()
test_consumer_cleanup_on_panic()
test_s3_connection_pool()
test_constant_time_comparison()
test_event_backpressure()
test_standing_query_buffer_limit()
test_connection_pool_limit()

// Week 3-4: 性能测试
bench_edge_traversal_nested_index()
bench_label_lookup_reverse_index()
bench_iceberg_micro_batching()
bench_parallel_replication()
bench_commit_throughput()

// Week 7-8: 混沌测试
test_network_partition_symmetric()
test_network_partition_asymmetric()
test_node_crash_recovery()
test_disk_failure_scenarios()
test_clock_skew()
test_peak_load()
```

### C. 配置模板

**生产环境推荐配置** (`nexora.toml`):

```toml
[server]
host = "0.0.0.0"
port = 8080
max_connections = 1000
connection_idle_timeout = "5m"

[storage]
backend = "rocksdb"
data_dir = "/data/nexora/graph"

[storage.rocksdb]
write_buffer_size = "128MB"
max_write_buffer_number = 3
block_cache_size = "2GB"
bloom_filter_bits_per_key = 10

[storage.wal]
sync_policy = "group"
max_ops = 1000
max_delay_micros = 10000  # 10ms

[event_store]
backend = "s3"
s3_bucket = "nexora-events"
s3_region = "us-west-2"
catalog_uri = "postgresql://nexora:***@catalog.example.com/nexora"
connection_timeout = "30s"
connection_pool_size = 50

[event_streaming]
enabled = true
meta_addr = "nexora-risingwave-meta:5690"
frontend_addr = "nexora-risingwave-frontend:4566"
connection_timeout = "10s"

[raft]
heartbeat_interval = "100ms"
election_timeout_min = "300ms"
election_timeout_max = "500ms"
max_batch_size = 1000
replication_timeout = "5s"
snapshot_interval = 10000  # 每10K条目

[monitoring]
metrics_enabled = true
metrics_port = 9090
tracing_enabled = true
otlp_endpoint = "http://jaeger:4317"
trace_sample_rate = 0.01  # 1%

[logging]
level = "info"
format = "json"
trace_id_enabled = true

[security]
auth_required = true
# NEXORA_AUTH_SECRET environment variable required
tls_enabled = true
tls_cert_path = "/etc/nexora/tls/cert.pem"
tls_key_path = "/etc/nexora/tls/key.pem"
```

### D. Grafana仪表板清单

**仪表板1: 系统概览**
- 集群健康状态
- 总节点/边数
- 写入/查询QPS
- p50/p95/p99延迟
- 错误率

**仪表板2: Raft共识**
- Leader状态
- Commit index增长
- 复制延迟（per follower）
- 选举次数
- Snapshot大小

**仪表板3: 事件处理**
- Event摄入速率（per topic）
- 投影延迟
- Checkpoint频率
- Iceberg表大小
- 积压事件数

**仪表板4: 资源使用**
- CPU使用率（per node）
- 内存使用率
- 磁盘I/O
- 网络带宽
- RocksDB缓存命中率

**仪表板5: 告警面板**
- 活跃告警列表
- 告警历史趋势
- 静音告警管理

### E. 故障排查决策树

```
性能问题
├─ 查询慢
│  ├─ 检查边索引是否优化（期望<1ms）
│  ├─ 检查标签查询是否优化（期望<1ms）
│  ├─ 检查Cypher快照大小（>1M节点？）
│  └─ 查看Grafana "查询延迟分解" 面板
│
├─ 写入慢
│  ├─ 检查Raft复制延迟（期望<10ms）
│  ├─ 检查磁盘I/O利用率（>80%？）
│  ├─ 检查是否启用Group Commit
│  └─ 查看Grafana "写入吞吐" 面板
│
└─ Event处理慢
   ├─ 检查Iceberg append延迟（期望<5ms）
   ├─ 检查投影规则复杂度
   ├─ 检查是否有积压事件
   └─ 查看Grafana "Event Pipeline" 面板

数据不一致
├─ 检查Raft commit index是否单调
├─ 检查WAL回放日志
├─ 运行数据一致性验证脚本
└─ 查看 /health/ready 端点状态

服务不可用
├─ 检查所有节点状态（kubectl get pods）
├─ 检查网络连接（ping/telnet）
├─ 检查磁盘空间（df -h）
├─ 检查日志中的ERROR（journalctl -u nexora）
└─ 触发灾难恢复流程（RTO<1小时）

内存泄漏
├─ 检查Grafana内存趋势图（是否线性增长）
├─ 生成heap profile（cargo flamegraph）
├─ 检查Standing Query缓冲区大小
├─ 检查Event缓冲区积压
└─ 重启受影响节点（临时缓解）
```

### F. 联系信息

**项目负责人**: frank-dkvan  
**技术架构**: 参见 `docs/RISINGWAVE_INTEGRATION_PLAN.md`  
**Issue跟踪**: https://github.com/frank-dkvan/nexora2/issues  
**Slack频道**: #nexora-production (内部)

---

## 结语

本路线图提供了将Nexora 2从当前状态提升至企业级生产就绪的详细计划。关键要点：

✅ **优势巩固**:
- 保持优秀的架构设计和测试覆盖
- 发挥事件溯源和时间旅行查询的独特优势

🔧 **短期加固**（6-8周）:
- 修复17个严重问题（数据安全第一）
- 实现关键性能优化（10-100倍提升）
- 补齐可观测性（生产运维基础）

🚀 **长期演进**（6-12月）:
- 持续性能优化
- 高级功能开发（图算法、ML集成）
- 多区域部署能力

**成功的关键**:
1. **严格遵循时间线**: 不跳过严重问题修复
2. **数据安全优先**: 每个变更都要有数据一致性测试
3. **渐进式上线**: 金丝雀发布，快速回滚能力
4. **持续监控**: 告警驱动的运维响应

**预期成果**:
- 生产就绪的分布式图数据库
- 40K写/秒吞吐量（3节点集群）
- 0.05ms边遍历延迟（vs当前50ms）
- 企业级可观测性和可靠性

---

**文档版本**: 1.0  
**最后更新**: 2026-08-02  
**下次审查**: 每周一（站会）

**批准签字**:
- [ ] 技术负责人
- [ ] 产品负责人  
- [ ] SRE负责人
- [ ] 安全负责人

---

*本路线图基于2026-08-02的全面生产级评估报告，由三个并行分析agent（架构/安全/性能）共计审查345,450 tokens生成。*
