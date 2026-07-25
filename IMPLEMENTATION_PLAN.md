# Nexora 生产就绪路线图实施计划

**当前日期**: 2026-07-18  
**当前分支**: feat/metadata-durability-a0-a1-pgwire-dbeaver  
**状态**: A0+A1+A2已完成，开始Track A核心任务

## 已完成 ✅

### A0 - 单节点持久化止血
- [x] SQ定义持久化 (persist/restore)
- [x] MV定义持久化 (with_rocksdb + load_from_db)
- [x] Shard map持久化 (shard_map_store.rs)
- [x] Label索引重建 (restart后重建)
- [x] 测试: sq_restart_survival, mv_restart_survival, shard_map_restart_survival

### A1 - 统一元数据存储
- [x] ControlPlaneStore trait (core/control_plane_store.rs)
- [x] RocksDbControlPlaneStore + InMemoryControlPlaneStore
- [x] 四命名空间: ShardMap/MvDef/MvData/SqState/RaftMeta
- [x] 三使用者迁移: shard_map_store, MV manager, SQ persist

### A2 - 控制面Raft共识
- [x] A2-1: ControlRaftTypeConfig (control_raft.rs)
- [x] A2-2: ControlStateMachine (control_raft_sm.rs)
- [x] A2-3: RaftLogStorage (control_raft_log.rs)
- [x] A2-4: RaftNetwork via TCP (control_raft_network.rs)
- [x] A2-5: 自适应投票者拓扑 (control_raft_topology.rs)
- [x] A2-6: ClusterManager集成 + bootstrap
- [x] A2-7: 控制面写路径走Raft + router更新
- [x] A2-8: 降级处理 (失quorum时拒绝写)
- [x] A2-9: 多投票者Raft E2E + split-brain验证
- [x] 测试: multi_voter_raft_e2e.rs (3投票者收敛+failover+脑裂守卫)

## 当前任务：Track A 核心（正确性地基）

### 阶段 1（2-3周） - 数据面两阶段提交 + 追赶

#### A1.1 两阶段提交（数据面）⭐ [进行中]
**目标**: 借鉴ArcadeDB的模式 `validate → WAL append → publish`

**当前状态**:
- `quorum_write_two_phase` 存在但未完整 (只检查quorum，未应用到owner)
- 缺少validate阶段
- 缺少WAL append作为不可回退点
- 缺少publish to actors阶段

**实施步骤**:
1. [ ] 在GraphService中添加validate钩子 (检查版本冲突、约束违反)
2. [ ] 修改quorum_write_two_phase:
   - Phase 0: Validate (owner + followers预检)
   - Phase 1: WAL append (owner + followers持久化，这是不可回退点)
   - Phase 2: Publish to actors (owner + followers应用到内存)
3. [ ] 添加回滚机制 (Phase 0/1失败时abort)
4. [ ] 测试: 并发写冲突、中途失败回滚、重启后WAL replay

**时间**: 5-7天

#### A1.2 严格 W+R>N（可配置）
**目标**: `WriteConcern::Majority` + `ReadConcern::Majority` 保证读最新已提交

**当前状态**:
- WriteConcern已实现 (Majority/All/One)
- ReadConcern缺失
- quorum_read.rs 使用字符串比较而非版本比较

**实施步骤**:
1. [ ] 在failover.rs添加ReadConcern enum (Local/Majority/Linearizable)
2. [ ] read_with_concern实现Majority读 (查询replica_caught_up + commit_index)
3. [ ] quorum_read.rs改用版本号比较 (HLC或单调版本)
4. [ ] 添加read-repair (读到陈旧副本时触发更新)
5. [ ] 配置项: cluster.yaml中write_concern + read_concern
6. [ ] 测试: write→immediate read验证一致性

**时间**: 3-5天

#### A1.3 Failover追赶协议
**目标**: 新owner上任前从存活副本catch_up_incremental追到最新

**当前状态**:
- state_transfer.rs有catch_up_shard逻辑
- 未接入failover流程
- ExportShard漏sleep节点 (B5任务)

**实施步骤**:
1. [ ] 修复ExportShard完整性 (先flush再遍历persistor) → B5
2. [ ] 在failover.rs添加promote_with_catchup
3. [ ] 新owner promotion流程:
   - 控制面Raft共识新owner
   - 新owner调用catch_up_incremental(from存活follower)
   - 追赶完成后设置CatchUpBarrier可写
4. [ ] 测试: kill owner → failover → 新owner追赶 → 数据完整

**时间**: 3-5天

#### A4 崩溃恢复验证 + torn-write repair
**目标**: WAL replay幂等 + torn-write检测修复

**当前状态**:
- WAL replay存在 (wal/log.rs)
- 无torn-write检测
- 无版本跳跃报错

**实施步骤**:
1. [ ] WAL record添加版本字段 + CRC32校验和
2. [ ] Replay时检测:
   - 版本重复 → 幂等跳过
   - 版本跳跃 → 报错需要state transfer
   - CRC失败 → torn write，丢弃并报警
3. [ ] Chaos测试: kill -9 during write → restart → replay → 验证一致性
4. [ ] 测试覆盖: 正常replay、torn write、版本跳跃

**时间**: 4-6天

### 阶段 2（1-2月） - 持久化 + 集群验证

#### B5 修复ExportShard完整性 [高优先级]
1. [ ] graph_service_adapter.rs ExportShard先flush再遍历persistor
2. [ ] 测试: export包含sleep节点

**时间**: 1-2天

#### B2 Offset-Aligned流式检查点 ⭐ [最关键]
1. [ ] 接通nexora-barrier到ingestion loop
2. [ ] Barrier注入Kafka消费批次
3. [ ] Shard处理Barrier: flush + 记录offset
4. [ ] CheckpointStore持久化 (epoch, shard_id, kafka_offset, snapshot_path)
5. [ ] 崩溃恢复从checkpoint重启
6. [ ] E2E测试: write → checkpoint → kill -9 → 重启 → exactly-once

**时间**: 2-3周

#### B1 统一快照原语
1. [ ] 定义SnapshotManifest (last_tx_id/timestamp/checksum)
2. [ ] 改造node snapshot为FlatBuffers + manifest
3. [ ] 五处复用: checkpoint/state_transfer/backup/time_travel/incremental_aggregation

**时间**: 1-2周

#### C1 State Transfer端到端接通
1. [ ] state_transfer.rs HTTP传输接入
2. [ ] 接入Raft (promote时触发)
3. [ ] 断点续传测试

**时间**: 1-2周

#### E1 Chaos测试体系 ⭐ [关键验证]
1. [ ] 基于Testcontainers的多进程集群harness
2. [ ] Docker network partition + tc netem延迟/丢包
3. [ ] 故障场景: kill leader, split-brain, 滚动重启, 网络分区
4. [ ] 验证: 数据一致性、无脑裂、failover时间、数据不丢
5. [ ] CI集成: 持续chaos测试

**时间**: 2-3周

## 验证清单

### A1.1 两阶段提交
- [ ] 并发写冲突正确abort
- [ ] WAL append失败全回滚
- [ ] publish失败时WAL已持久化，重启replay恢复
- [ ] kill owner during commit → 新owner replay → 无丢失/重复

### A1.2 W+R>N一致性
- [ ] 并发写(epoch N) + 读 → 读到epoch N值
- [ ] Read-repair修复落后副本
- [ ] 写后立即quorum读 → 必须读到新值

### A1.3 Failover追赶
- [ ] 新owner调用catch_up_incremental返回正确ops
- [ ] kill owner → Raft选新owner → 追赶 → 数据一致
- [ ] 网络分区期间写入 → 愈合 → 追赶 → 最终一致

### A4 崩溃恢复
- [ ] 写入中kill -9 → restart → replay → 验证一致
- [ ] torn-write注入 → replay → 等版本幂等修复或跳跃报错
- [ ] 72h持续写 + 每小时随机kill → 累计无数据损坏

## Metrics观测点（阶段2灰度必看）

- `replication_quorum_failed_total`: < 0.1% 写入量
- `replication_missing_acks_total`: < 1% 写入量
- `failover_triggered_total`: 预期内（计划维护）
- `wal_replay_duration_seconds`: < 10s
- `snapshot_load_duration_seconds`: < 1s
- `control_raft_leader_changes`: < 1次/小时

## 里程碑

- **M1** (Week 3): A1.1+A1.2+A1.3+A4完成，单元测试通过
- **M2** (Week 6): B5+B2完成，流式exactly-once验证
- **M3** (Week 10): E1 chaos测试体系建立，持续运行
- **M4** (Month 3): 非关键业务灰度，观察ReplicationMetrics
- **M5** (Month 6): 生产就绪，移除"EXPERIMENTAL"横幅

## 下一步行动

1. ✅ 修复编译错误，所有测试通过
2. ▶️ 开始A1.1两阶段提交实现 (validate → WAL → publish)
3. 并行B5修复ExportShard完整性（A1.3依赖）
4. 完成A1.1后立即开始A1.2 (W+R>N一致性)
5. A1.2完成后开始A1.3 (Failover追赶)
6. 并行启动E1 Chaos测试体系框架
