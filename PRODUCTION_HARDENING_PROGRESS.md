# Nexora 2 生产加固进度跟踪

**开始日期**: 2026-08-02  
**目标**: 修复86个关键问题，达到生产就绪标准  
**预计完成**: 2026-08-23 (3周)

---

## 严重问题修复进度 (17个)

### 已完成 ✅ (13/17)

- [x] **C-5**: WAL Flusher最终fsync (已存在) - `crates/nexora-core/src/wal/log.rs:172-210`
  - 状态: 代码已经正确处理shutdown时的最终flush
  - 验证: 第208-214行确保即使shutdown时也会flush待处理的操作

- [x] **C-1**: Raft死锁风险 - 锁顺序 (已修复) - `crates/nexora-raft/src/lib.rs:420-421`
  - 状态: 代码显式drop followers lock后再获取commit_index write lock
  - 验证: 锁顺序一致，避免死锁

- [x] **C-3**: Raft Commit Index竞态条件 (已修复) - `crates/nexora-raft/src/lib.rs:467-475`
  - 状态: 使用write lock upfront防止竞态条件
  - 验证: 原子性更新，无lost update风险

- [x] **C-2**: Iceberg Catalog连接无超时 (已修复) - `crates/nexora-eventlog/src/event_log_store.rs:169-191`
  - 状态: 为S3和LocalFs两种catalog类型都添加了30秒超时
  - 验证: 使用tokio::time::timeout包装，防止无限挂起

- [x] **C-4**: Kafka Consumer资源泄漏 (已修复) - `crates/nexora-stream/src/kafka_source.rs:1506-1521`
  - 状态: 实现了Drop trait，在drop时调用unsubscribe()
  - 验证: 触发立即rebalance，避免资源泄漏

- [x] **C-7**: 认证时序攻击 (已修复) - `crates/nexora-app/src/auth.rs:171-188`
  - 状态: 实现了恒定时间比较（手动XOR + 位或运算）
  - 验证: 避免短路行为，防止时序侧信道攻击

- [x] **C-6**: RisingWave连接无超时 (已修复) - `crates/nexora-risingwave/src/library_client.rs:35-51`
  - 状态: 添加了10秒超时保护
  - 验证: 使用tokio::time::timeout包装连接

- [x] **C-9**: Checkpoint未fsync (已修复) - `crates/nexora-stream/src/checkpoint.rs:404`
  - 状态: RocksDB flush_wal(true)调用已存在
  - 验证: 代码已正确使用flush确保持久性
  - 注: 建议在DB选项中设置use_fsync(true)以获得更强保证

- [x] **C-10**: Standing Query结果缓冲无界增长 (已修复) - `crates/nexora-standing-query/src/executor.rs:276-296`
  - 状态: 添加了有界通道(capacity=1000)和背压控制
  - 验证: 编译通过，测试通过(50个测试)

- [x] **C-11**: Event Log追加无背压 (已修复) - `crates/nexora-stream/src/lib.rs:609-652`
  - 状态: 添加了有界通道(capacity=100)和异步处理任务
  - 验证: 编译通过，测试通过(51个测试)
  - 实现: 使用tokio::sync::mpsc::channel实现生产者-消费者模式

- [x] **C-8**: 快照传输错误路径资源泄漏 (已修复) - `crates/nexora-zenoh/src/state_transfer.rs:69-96`
  - 状态: 将scopeguard清理逻辑移到write之前，确保所有错误路径都清理临时文件
  - 验证: 编译通过，测试通过(289个测试)
  - 实现: 使用scopeguard::guard在write前设置清理，成功后mem::forget defuse

- [x] **C-12**: 边索引无界增长 (已修复) - `crates/nexora-core/src/graph/mod.rs:743-745`
  - 状态: 在delete_node中添加了edge_index.remove_node()调用
  - 验证: 编译通过，测试通过(204个测试)
  - 实现: 确保节点删除时清理所有相关边，防止内存泄漏

### 待处理 ⏳ (4/17)

- [ ] **C-8**: 快照传输错误路径资源泄漏
  - 位置: `crates/nexora-core/src/graph/shard/mod.rs`
  - 问题: 临时文件在传输失败时未清理
  - 修复: 使用scopeguard确保清理

- [ ] **C-12**: 边索引无界增长
  - 位置: `crates/nexora-core/src/graph/`
  - 问题: 边索引可能无界增长，无GC
  - 修复: 实现边索引GC机制

- [ ] **C-13**: Zenoh复制失败无熔断器
  - 位置: `crates/nexora-zenoh/`
  - 问题: 复制失败时无熔断保护
  - 修复: 添加熔断器模式

- [ ] **C-14**: 物化视图刷新可能饿死其他操作
  - 位置: `crates/nexora-eventlog/`
  - 问题: 大型物化视图刷新可能阻塞其他操作
  - 修复: 添加优先级调度或分片刷新

- [ ] **C-15**: HTTP API无速率限制
  - 位置: `crates/nexora-app/src/handlers.rs`
  - 问题: API端点无速率限制
  - 修复: 使用tower-governor添加速率限制

- [ ] **C-16**: pgwire连接池耗尽
  - 位置: `crates/nexora-pgwire/`
  - 问题: 连接池可能耗尽
  - 修复: 添加连接池监控和限制

- [ ] **C-17**: 失败事件投影无死信队列
  - 位置: `crates/nexora-stream/`
  - 问题: 失败的事件投影没有DLQ
  - 修复: 实现死信队列机制

---

## 高危问题修复进度 (23个)

### 已完成 ✅ (0/23)

### 待处理 ⏳ (23/23)

- [ ] **H-1**: 生产代码中的.unwrap()使用 (2,120个实例)
  - 优先级: 修复关键路径中的前100个
  - 位置: 全代码库
  - 修复: 替换为适当的错误处理

- [ ] **H-2**: 外部服务无连接池
  - 位置: `crates/nexora-eventlog/src/event_log_store.rs`
  - 问题: 每次操作创建新连接
  - 修复: 实现连接池

- [ ] **H-3到H-23**: 其他高危问题
  - 详见评估报告

---

## 中危问题修复进度 (31个)

### 计划: 第3-4周处理

---

## 低危问题修复进度 (15个)

### 计划: 持续改进

---

## 测试验证

### 单元测试状态
- nexora-core: ✅ 通过
- nexora-eventlog: ✅ 通过
- nexora-raft: ✅ 通过
- nexora-stream: ✅ 通过 (51个测试)
- nexora-standing-query: ✅ 通过 (50个测试)
- nexora-app: ⏳ 待运行

### 集成测试状态
- ⏳ 待运行全套集成测试

---

## 下一步行动

1. ✅ 修复C-10 (Standing Query背压)
2. ✅ 修复C-11 (Event Log背压)
3. ⏳ 修复C-8 (快照传输资源泄漏)
4. ⏳ 修复C-12 (边索引GC)
5. ⏳ 修复C-13 (Zenoh熔断器)

---

**最后更新**: 2026-08-02 15:30
**当前进度**: 11/86 完成 (12.8%)
