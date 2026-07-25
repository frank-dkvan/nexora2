# Nexora 迈向行业领先的生产级大规模高并发分布式平台 —— 体系化研发路线图

**制定日期**: 2026-07-18  
**决策更新**: 2026-07-18 — 共识路线决策：采用**路线 B（强化 Primary-Backup）**，详见 `CONSENSUS_DECISION_RAFT_VS_PRIMARY_BACKUP.md`  
**方法**: 基于逐层源码核查（file:line 级证据）+ 与 ArcadeDB 26.7.2 / Orleans / Flink / RisingWave 横向对标
**目标读者**: 研发团队、架构决策者
**性质**: 下一步研发重点工作内容的顶层规划（What & Why & 排序，非详细设计）

> 一句话总纲：**先补齐"正确性地基"（共识/恢复/备份），再做"性能领先"（遍历/摄入），最后做"运维成熟度"（可观测/弹性）。在地基未稳前，任何执行模型重构都是错误的资源投放。**

---

## 第一部分：Nexora 定位的体系化理解

### 1.1 本质定位

**Nexora 是一个"流式事件驱动的图状态平台"（Streaming Event-Driven Graph State Platform）。**

坐标：位于**流处理引擎**（Flink / RisingWave）与**图数据库**（Neo4j / TigerGraph）之间。核心职责是**从事件流中持续维护一份"活的、可查询的"图状态**，并在其上支持增量模式匹配与即席图查询。

判断依据（源自架构第一性原理，非官方描述）：
- **Event Sourcing 是地基**：所有状态变更是 `NodeChangeEvent`（`nexora-core/src/event.rs`）
- **流式接入是入口**：Kafka / MQTT / Kinesis / WebSocket / Zenoh（`nexora-stream`）
- **增量计算是输出**：Standing Query + 增量物化视图（`nexora-standing-query` / `materialized_view.rs`）
- **图查询是能力**：Cypher / SQL（`nexora-cypher` / `nexora-sql`）

它**不是**存储型图库（那以磁盘/事务为中心），**不是**纯流处理器（那不保留可查询图状态）。独特价值 = **live incremental graph state**。

### 1.2 定位内部的负载张力（关键）

定位内含三类诉求冲突的负载，是所有技术取舍的根源：

| 负载 | 占比 | 对引擎的诉求 | Actor-per-Node 契合度 |
|------|------|------------|:---:|
| **写入 / 事件驱动** | 最大（60-70%） | 高吞吐摄入、无全局写锁 | ✅ 主场 |
| **增量计算**（SQ/MV） | 中 | 变更局部触发 | ✅ 契合 |
| **图关系分析**（多跳遍历） | 次要但存在 | 邻接局部性、低跳转常数因子 | ❌ 结构性短板 |

### 1.3 执行模型判断（Actor-per-Node）

- **作为范式**：actor 语义（串行化、无共享、消息驱动）对主力负载（写入+增量）契合，是"区间最优"的合理选择。
- **作为粒度**：per-Node 偏细。数百万 tokio task 的内存/调度开销真实存在；per-shard actor + 分片内共享邻接可拿到几乎同等写并发，却改善遍历局部性。
- **已有演化信号**：`projection.rs` 的 lock-free `DashMap<NexoraId, Arc<NodeReadState>>` 让**读绕过 mailbox**——架构已自发走向"写走 actor、读走共享投影"的混合体。
- **结论**：**不重构，渐进演化**。核心执行模型重构是最高风险改动，在共识/恢复未夯实前投放是错误的。沿"读旁路做厚 + 零拷贝 wake + 拓扑感知常驻 + 多跳下推分片内"演化即可。

### 1.4 对"行业领先"的定义

对本平台，"行业领先"= 在**流式图 + actor 分页**这个细分赛道，同时做到：
1. **正确性**：强一致可选、崩溃不丢不重、分区不脑裂
2. **规模**：单集群承载 10⁹+ 节点、10⁵+ QPS 写入、水平线性扩展
3. **性能**：实时遍历 P99 可控、增量计算低延迟
4. **成熟度**：无人值守、可观测、可备份恢复、可滚动升级

当前差距：**正确性地基是架构级缺失（P0），其余是打磨级。**

---

## 第二部分：工作任务体系（按依赖与优先级分层）

工作分五个轨道（Track A-E），轨道内按 P0/P1/P2 排优先级。**依赖关系决定跨轨道顺序：A 是所有其他轨道的前提。**

```
Track A  正确性地基（共识/恢复/一致性）   ← 必须最先，架构级
   │
   ├─▶ Track B  数据持久化与快照统一        ← 依赖 A 的复制/版本基础设施
   │
   ├─▶ Track C  规模与分布式扩展            ← 依赖 A 的共识
   │
Track D  性能领先（遍历/摄入/查询）         ← 可与 A/B 部分并行（不触执行模型）
   │
Track E  运维成熟度（可观测/弹性/安全）     ← 贯穿，收尾集中
```

---

### Track A —— 正确性地基（架构级，最高优先级）

> 依据 `PRODUCTION_GAP_ASSESSMENT_2026-07-13.md`：这些是"不解决绝不能上生产"的 P0。  
> **共识路线决策（2026-07-18 拍板）**：采用**路线 B（混合架构：控制面 Raft + 数据面强化 Primary-Backup）**，总工程量 2-3 周起步 + 1-2 月灰度验证。详见 `CONSENSUS_DECISION_RAFT_VS_PRIMARY_BACKUP.md`。

**决策依据**：
- 控制面 Raft（openraft 0.9）已完整集成并运行（`control_raft*.rs` 2388 行），管理 shard map / schema 元数据
- 数据面不引入 Raft（避免 256 Raft groups 拖累高吞吐流式负载），保持 quorum write + epoch fencing
- 补强三处：两阶段提交、崩溃恢复验证、failover 追赶协议
- 契合"流式事件驱动"定位的最终一致性语义（对标 Flink/RisingWave，非事务型数据库）

| ID | 任务 | 现状证据 | 级别 | 预估 |
|----|------|---------|------|------|
| **A1** | **保持控制面 Raft + 补强数据面 Primary-Backup** | **控制面**：openraft 已集成（`cluster.rs:521`），管理 shard map/epoch。**数据面**：当前 `ReplicaWriter` quorum write 无共识，需补强而非重写 | P0 | 见下拆解 |
| **A1.1** | 两阶段提交（数据面） | 借鉴 ArcadeDB：`validateAndBumpVersions() → writeToWAL() → publishToActors()`，WAL append 是唯一不可回退点 | P0 | 2-3 周 |
| **A1.2** | 严格 W+R>N（可配置） | `WriteConcern::Majority` + `ReadConcern::Majority` 保证读最新已提交；配置项可选 | P0 | 1 周 |
| **A1.3** | Failover 追赶协议 | 新 owner 上任前（控制面 Raft 已共识），从存活副本 `catch_up_incremental(from_seq)` 追到最新 | P0 | 1 周 |
| **A2** | **强一致读闭环（数据面）** | `quorum_read.rs:171-189` 字符串比较改版本比较；引入单调版本/HLC；read-repair 修复落后副本 | P0 | 1-2 周 |
| **A3** | **写不丢保证验证** | 通过 A1.1（两阶段）+ A1.2（W+R>N）+ A1.3（追赶）组合保证；chaos 测试证明（kill owner → failover → 无丢失） | P0 | 含在 A1/E1 |
| **A4** | **崩溃恢复正确性验证** | WAL replay 端到端故障注入：kill -9 → restart → replay → 验证一致。**借鉴 ArcadeDB torn-write repair**（等版本幂等重放 + 版本跳跃报错） | P0 | 1-2 周 |
| **A5** | **（已合并到 A1.1）** | - | - | - |
| **A6** | **Per-DB/Namespace 分歧隔离** | 借鉴 ArcadeDB：单命名空间 apply 失败仅隔离该命名空间（quarantine），非全节点 fence；超阈值才 halt | P1 | 1 周 |

---

### Track B —— 数据持久化与快照统一

> 核心洞察：**不建四套快照系统，立一个统一快照原语，五个用途复用。** 当前 `checkpoint.rs`/两个 `state_transfer.rs`/`barrier`/`fragment/time_travel` 各写了半成品且未打通，根因是缺统一原语。

| ID | 任务 | 现状证据 | 级别 | 预估 |
|----|------|---------|------|------|
| **B1** | **统一快照原语** | 定义"node 状态捕获 + 一致性 cut + manifest（last_tx_id/timestamp/校验和）"。当前 node 快照用 JSON、无校验（`shard/mod.rs:704-730`） | P0 | 2 周 |
| **B2** | **⭐ Offset-Aligned 流式一致性检查点** | **对流式定位最关键**：把 `(kafka_offset, graph_state)` 绑成原子快照对，实现 exactly-once 恢复。`nexora-barrier` 是骨架、未 wire 到 ingestion+flush。**排在 Database backup 之前** | P0 | 2-3 周 |
| **B3** | **Database-level 备份/恢复/PITR** | **完全缺失**（`PRODUCTION_GAP_ASSESSMENT:27`）。借鉴 ArcadeDB：ZIP + manifest 作为最后 entry（截断检测）+ CRC32/Blake3 校验 + HTTP 流式 | P1 | 1-2 周 |
| **B4** | **零拷贝 wake（FlatBuffers）** | 当前 JSON 反序列化（代码自注 "FlatBuffers will be used in production"）；**直接降低遍历冷路径 miss 惩罚 3-4 数量级** | P1 | 1-2 周 |
| **B5** | **ExportShard 完整性修复** | `graph_service_adapter.rs:258` 只导出常驻节点，漏已 sleep 节点 → 新节点收到不完整快照。需先 flush 再遍历 persistor | P0 | 3-5 天 |
| **B6** | **shard flush 并发化** | `mod.rs:1255` 串行遍历 256 shards → 2.56M 节点串行 sleep 耗时 >5min。改 `buffer_unordered(CPU)` | P1 | 3 天 |
| **B7** | **控制面快照 + 算子状态快照** | schema/shard-map/成员元数据恢复；SQ 匹配态（已有 ControlPlaneStore 持久化）/ MV 增量聚合态待确认 | P1 | 1-2 周 |
| **B8** | **Fragment Consolidation（借鉴 TileDB）** | 后台自动合并小碎片。新建 `nexora-fragment/src/consolidator.rs`，实现策略：`min_frags=10`, `max_frags=100`, `size_ratio=0.3`, `min_size=10MB`。触发：显式 API + 可选后台线程（每 10 分钟检查）。详见 `TILEDB_INTEGRATION_ANALYSIS.md` | P2 | 2-3 周 |
| **B9** | **VFS 抽象（云存储，借鉴 TileDB）** | 统一文件系统接口。新建 `nexora-storage/src/vfs.rs`，trait `VFS { read/write/list/delete }`，实现 `LocalVFS`, `S3VFS`, `InMemoryVFS`（测试用）。为 S3 Iceberg 后端奠基 | P2 | 2-3 周 |
| **B10** | **Filter Pipeline（借鉴 TileDB）** | 可插拔压缩/加密链。新建 `nexora-storage/src/filter_pipeline.rs`，trait `Filter { encode/decode }`，struct `FilterPipeline { filters: Vec<Box<dyn Filter>> }`。内置：`ZstdFilter`, `Aes256GcmFilter`, `BitShuffleFilter`。用户可配置策略 | P2 | 1-2 周 |

---

### Track C —— 规模与分布式扩展

| ID | 任务 | 现状证据 | 级别 | 预估 |
|----|------|---------|------|------|
| **C1** | **State Transfer 端到端接通** | `zenoh/state_transfer.rs` 逻辑完整（含断点续传 `TransferCheckpoint`）但未接 HTTP 传输、未接 Raft、`raft_handler.rs` 返回 NOT_IMPLEMENTED | P0（集群） | 1-2 周 |
| **C2** | **自动 failover + 反脑裂验证** | failover 循环已接心跳（`cluster.rs:644`）+ quorum 守卫（`control.rs:173`），但无真实共识兜底、未注入分区验证少数侧 fail-closed | P1 | 1 周（依赖 A1） |
| **C3** | **动态成员变更（扩缩容）** | `remove_node` 仅契约级测试，无多进程端到端；`add_node` e2e 首跑暴露"未注册地址"bug | P1 | 1 周 |
| **C4** | **Group Commit 双维度背压** | 借鉴 ArcadeDB `RaftGroupCommitter`：entry 数（10k）+ 字节预算（256MB）双背压 + caller-runs；区分 DispatchedTimeout vs QuorumNotReached | P1 | 1-2 周 |
| **C5** | **分布式查询下推增强** | 多跳遍历下推到 shard 内，减少跨 shard round-trip；分布式 plan 已有基础（`distributed_query.rs`） | P2 | 2-3 周 |
| **C6** | **集群负载/容量基线** | 无集群级压测基线（单机 bench 有）。给出容量规划数字 | P1 | 1 周 |

---

### Track D —— 性能领先（不触执行模型）

> 遍历性能的最大杠杆在 Snapshot 之外。执行模型保持 actor-per-node，沿混合方向渐进优化。

| ID | 任务 | 现状证据 | 级别 | 预估 |
|----|------|---------|------|------|
| **D1** | **批量并发 BFS** | `handlers.rs:1274` `local_bfs` 逐节点串行 `.await`。改 frontier 用 `buffer_unordered` 并发取边 → 深度遍历 5-50× | P0（性能） | 1 周 |
| **D2** | **邻接表专用遍历路径** | `mod.rs:1037` `get_edges` 每跳 clone 全边集。走 `EdgeIndex` 避免逐节点唤醒/克隆 | P1 | 1-2 周 |
| **D3** | **读旁路投影做厚** | `projection.rs` 已有 lock-free 读旁路；扩展覆盖遍历/扫描/多跳，减少 mailbox round-trip | P1 | 1-2 周 |
| **D4** | **拓扑感知常驻策略** | 当前纯 LRU（`enforce_memory_limit`），不感知图结构。高入度/遍历热点节点优先钉常驻 → 命中率提升 | P1 | 1-2 周 |
| **D5** | **摄入吞吐优化** | ✅ **已核实达成（见附录 VER-1）**：per-shard WAL 文件池 + delta-only 记录均为现有设计，无需改动 | P2 | 已完成 |
| **D6** | **并行查询执行** | 借鉴 ArcadeDB 专用查询线程池（避免污染 common pool）+ 有界队列 + caller-runs | P2 | 1-2 周 |

---

### Track E —— 运维成熟度（贯穿，收尾集中）

| ID | 任务 | 现状证据 | 级别 | 预估 |
|----|------|---------|------|------|
| **E1** | **Chaos / 故障注入测试体系** | **P0-5 信心不足**：真实 e2e 首跑暴 2 bug。借鉴 ArcadeDB e2e-ha：Testcontainers 多进程集群 + Docker network 分区 + tc netem 丢包/延迟 + leader failover / split-brain / 滚动重启 | P0 | 2-3 周 |
| **E2** | **长稳 soak** | 无 72h+ 持续负载 + 周期故障注入，观察内存/句柄泄漏、性能衰减 | P1 | 1 周 + 观察 |
| **E3** | **failover 告警钩子** | Prometheus `/metrics` 已有（`main.rs:1571`）；failover/降级事件无 ops 推送（webhook/PagerDuty） | P1 | 2-3 天 |
| **E4** | **滚动升级** | 版本兼容协议 + graceful drain | P2 | 1-2 周 |
| **E5** | **运维 runbook** | 故障处置手册、监控面板、告警阈值 | P2 | 3-5 天 |
| **E6** | **移除 EXPERIMENTAL 横幅** | `main.rs:1033` 集群启动打印 "NO FAULT TOLERANCE"。能力达标后同步移除，让自我声明与实际一致 | P1 | 收尾 |
| **E7** | **安全加固** | HMAC-SHA256/RBAC/TLS/WAL 加密已有；补审计完整性、密钥轮转、多租户隔离 | P2 | 1-2 周 |

---

### Track F —— Stateful Streaming 对齐（架构演进，中长期）

> 对标 Apache Flink Stateful Functions + RisingWave，将 Nexora 从"Actor + Event Sourcing"演化为"完整 Stateful Streaming Graph Platform"。  
> **当前状态**：`nexora-barrier` 已有 Epoch Barrier 骨架（293 行），Nexora 已具备 60-70% Flink SF 核心能力。  
> **详细分析**：见 `ALIGN_FLINK_STATEFUL_FUNCTIONS.md`

| 任务 | 说明 | 现状证据 | 优先级 | 工作量 |
|------|------|---------|:---:|--------|
| **F1** | **Global Checkpoint（核心）** | `nexora-barrier` 有 Epoch Barrier 调度器，但未接入数据流。需：Barrier 注入到 Kafka 消费流 + Shard 处理 Barrier（flush + 报告）+ Checkpoint 元数据存储（绑定 Kafka offset）+ 崩溃恢复从 Checkpoint 重启 | P0 | **4-6 周** |
| **F1.0** | **Fragment Time Travel 实现（借鉴 TileDB）** | 补齐 `nexora-fragment/src/time_travel.rs`（185 行骨架）。**具体实现**：(1) Fragment 命名：`{start_time_us}_{end_time_us}/` 目录结构（参考 TileDB `__fragments/__<timestamp>_v1/`）；(2) `FragmentMetadata` 添加 `created_at: u64`, `node_count: usize`, `event_count: u64` 字段；(3) `TimeTravelQuery::new(target_time: u64)` 实现：列出所有 Fragments → 过滤 `start_time <= target_time` 的 → 按时间排序；(4) 后续 Fragment 覆盖前面数据（MVCC 语义）。测试：写入 3 个 Fragments（T1/T2/T3），查询 T2 时刻的数据。详见 `TILEDB_INTEGRATION_ANALYSIS.md` | P0 | 1 周 |
| **F1.1** | Barrier 注入到 Kafka | 无。需在 `nexora-stream` 定期插入 Barrier(epoch) 到消费批次 | P0 | 1 周 |
| **F1.2** | Shard 处理 Barrier | 无。需 `Shard::handle_barrier()` 实现：排空队列 → flush actors → flush WAL → 记录 offset → 报告 scheduler | P0 | 2 周 |
| **F1.3** | Checkpoint 元数据存储 | 无。需 `CheckpointStore` 持久化 `(epoch, shard_id, kafka_offset, snapshot_path)` 到 RocksDB | P0 | 1 周 |
| **F1.4** | 崩溃恢复从 Checkpoint | 无。需 `ClusterManager::recover_from_checkpoint()` 读元数据 → 恢复 snapshot + Kafka offset | P0 | 1-2 周 |
| **F2** | **Exactly-Once 端到端验证** | 无。需 Chaos 测试：写入 → checkpoint → kill -9 → 重启 → 验证无重复/无丢失 | P0 | 2 周 |
| **F3** | **WAL + ReductStore 异步复制** | 无。WAL 作为主路径（高性能），ReductStore 异步复制（时序查询 + BLOB）。需 `WalToReductReplicator` 后台任务定期将 WAL 增量复制到 ReductStore。详见 `REDUCTSTORE_REPLACE_WAL_ANALYSIS.md` | P1 | 2-3 周 |
| **F3.1** | PropertyValue::BlobRef variant | `property_value.rs` 无 BlobRef variant（Track B8 已规划）。需添加 `BlobRef(BlobRef)` | P1 | 3 天 |
| **F3.2** | ReductStore 直写工具 | 无。需 `ReductBlobWriter` 封装 ReductStore 客户端，提供 `write_blob() → BlobRef` | P1 | 1 周 |
| **F3.3** | 异步复制器 | 无。需 `WalToReductReplicator` 后台任务：定期读 WAL 增量 → 批量写 ReductStore + checkpoint 持久化 | P1 | 1 周 |
| **F4** | **Watermark + 事件时间窗口** | 无。需 `WatermarkGenerator` 计算全局 watermark + Cypher 扩展支持事件时间窗口聚合（`time_window.tumbling(..., 'event_time')`） | P2 | 3-4 周 |
| **F5** | **动态扩缩容** | Shard 数固定（256）。需 shard rebalance + 状态迁移机制 | P3 | 2-3 月 |

**关键里程碑**：
- **F1 + F2 完成**：Nexora 达到 **Flink SF 核心能力的 85%**，定位从"Actor 框架 + 图数据库"升级为"Stateful Streaming Graph Platform"
- **F3 完成**：统一事件流存储 + 支持非结构化数据（图片/视频），存储架构优雅化
- **F4 完成**：支持乱序事件 + 事件时间窗口聚合，达到 **Flink SF 核心能力的 95%**

**与其他 Track 的关系**：
- F1（Global Checkpoint）与 Track B（备份）协同：Checkpoint = 自动化快照
- F3（ReductStore 复制）与 F1 集成：ReductStore 复制进度也纳入 Checkpoint，崩溃恢复时同步恢复
- F1 可与 Track A/B 并行（不冲突），但建议在 Track A 完成后启动

---

## 第三部分：推进顺序与里程碑

**核心决策（2026-07-18 拍板）**：采用**路线 B（混合架构：控制面 Raft + 数据面强化 Primary-Backup）**，总时长 **4-6 个月**（vs 路线 A 的 6-9 个月）。

依赖关系决定顺序（非简单按优先级并行）：

```
阶段 1（正确性地基，~2-3 周）—— 路线 B 快速起步
  A1.1 两阶段提交（数据面）
  A1.2 严格 W+R>N（可配置）
  A1.3 Failover 追赶协议
  A4 崩溃恢复验证 + torn-write repair
  并行：B5 ExportShard 修复、E1 chaos 测试骨架

阶段 2（持久化 + 集群验证，~1-2 月）—— 灰度观察期
  B1 统一快照原语 → B2 ⭐流式检查点（offset-aligned） → B3 备份恢复
  C1 state transfer 接通 → C2 failover 验证（chaos） → A6/C4 分歧隔离/背压
  并行可选：F3 WAL + ReductStore 异步复制（2-3 周，与 B 协同）
  关键：**在非关键业务持续 chaos 测试，观察 ReplicationMetrics**

阶段 3（Stateful Streaming 对齐，~6-8 周）—— 架构升级（可选）
  F1.1 Barrier 注入（1 周）
  F1.2 Shard 处理 Barrier（2 周）
  F1.3 Checkpoint 元数据（1 周）
  F1.4 崩溃恢复（1-2 周）
  F2 Exactly-Once 验证（2 周）
  里程碑：达到 Flink SF 核心能力的 85%，定位升级为"Stateful Streaming Graph Platform"

阶段 4（性能领先，可与阶段 2-3 部分并行，~1-2 月）
  D1 并发 BFS → D2 邻接路径 → B4 零拷贝 wake → D3/D4 投影/常驻

阶段 5（成熟度收尾 + 完整性补齐，~1-2 月）
  C3 扩缩容 / C6 容量基线 → E2 长稳 soak（72h+） → E3/E6 告警/横幅移除 → E4/E5/E7
  按需：F4 Watermark + 事件时间窗口（3-4 周，如需处理乱序事件）
```

**关键决策点已定**：  
- ✅ **共识路线**：控制面 Raft（保持）+ 数据面 Primary-Backup（补强），**不引入数据面 Raft**
- ✅ **架构演进**：对齐 Flink Stateful Functions（Track F），从"Actor 框架 + 图数据库"升级为"Stateful Streaming Graph Platform"
- ✅ **时序能力**：WAL + ReductStore 异步复制（F3），统一事件流存储 + 支持非结构化数据（图片/视频）
- ✅ **不在共识就位前上关键业务**：先灰度"可容忍停机、有人值守、数据另有备份"场景
- ⚠️ **验证策略**：灰度 1-2 月 + chaos；若暴露不可接受风险 → 评估局部引入数据面 Raft（小 shard）

**总量估计**：
- **路线 B 达到"生产级"**：4-6 个月（vs 路线 A 的 6-9 月，快 2-3 月）
- **Track F（Stateful Streaming 对齐，可选）**：6-8 周核心工作（F1+F2），可与其他 Track 并行
- **达到"Flink SF 能力 85% + 生产级图平台"**：6-8 个月总计（含 Track F）

---

## 第四部分：不做什么（同等重要）

1. **不重构 actor-per-node 执行模型**：最高风险、最大爆炸半径；当前模型对主力负载契合，不是瓶颈根源。沿混合方向渐进演化即可。
2. **不在共识就位前上关键业务**：先在"可容忍停机、有人值守、数据另有备份"的非关键场景灰度。
3. **不把四类快照建成四套系统**：立统一原语，多用途复用。
4. **不对外宣称"Actor 分页行业领先"**：Orleans grains / thatDot Quine 是明确先例，实现层在零拷贝/分布式激活上仍落后；宣称前先补 B4 + D4。
5. **不在 Nexora Cypher/SQL 内部自研时序算子**：工程量 3-4 月，但 InfluxDB/ReductStore 已打磨多年，性能/功能难超越。推荐分层架构（Nexora + TSDB）+ SDK 封装实现统一体验，而非真正的"一体化"。
6. **不追求金融级强一致性**：Nexora 定位"流式事件驱动图状态平台"，对标 Flink/RisingWave 的最终一致 + 幂等重放，而非 Spanner 的强一致。路线 B（Primary-Backup）已够用，不在数据面引入 Raft。
7. **不用 ReductStore 直接替代 WAL**：ReductStore 缺 Group Commit，写入吞吐劣化 250×。推荐 WAL 保留（高性能主路径）+ ReductStore 异步复制（时序副本），既统一数据又保持性能。
5. **✅ 不在数据面引入 Raft**（关键决策）：控制面 Raft 已有且够用；数据面 256 Raft groups 会拖累高吞吐流式负载，与定位冲突。强化 Primary-Backup（两阶段 + 追赶 + W+R>N）即可保证"崩溃不丢、最终收敛"。
6. **不追求"金融级强一致"**：Nexora 定位是"流式事件驱动图状态平台"，语义是最终一致 + 幂等重放（对标 Flink/RisingWave），非事务型数据库。强一致应交给上游 Kafka（它已有 Raft/Paxos）。

---

## 附：横向对标结论速查

| 能力 | Nexora 现状 | ArcadeDB | 差距级别 | 备注 |
|------|-----------|----------|:---:|------|
| **控制面共识** | ✅ openraft 已集成运行 | Apache Ratis | 持平 | 管理 shard map/schema |
| **数据面共识** | ⚠️ Primary-Backup + quorum | Apache Ratis | 架构分歧 | **设计选择**：不引入数据面 Raft，契合流式定位 |
| 强一致读 | 未闭环 | 版本化 | 打磨级 | A2 补强 |
| 崩溃恢复验证 | 未验证 | torn-write repair | 打磨级 | A4 借鉴 |
| Database 备份 | 缺失 | ZIP+manifest+CRC | 功能级 | B3 实现 |
| Chaos 测试 | 弱 | e2e-ha 完整 | 体系级 | E1 补齐 |
| Actor 分页 | 单机称职 | （不同模型） | 对标 Orleans：中等 | B4 零拷贝可提升 |
| ⭐流式检查点 | barrier 骨架 | （非流式） | **Nexora 特有洞** | B2 补齐（最关键） |

**关键架构差异总结**：
- ArcadeDB = 事务型图数据库，全程 Raft（控制+数据面）
- Nexora = 流式图状态平台，**分层共识**（控制面 Raft + 数据面 quorum）
- 这不是"落后"，而是**负载特征驱动的设计取舍**：高吞吐事件流不适合 Raft consensus

---

## 第五部分：关键技术细节速查（防止误读）

### 5.1 现有代码资产清单

**已完成且可复用**：
- ✅ 控制面 Raft（`nexora-zenoh/src/control_raft*.rs` 2388 行）：
  - `cluster.rs:521` 启动入口：`openraft::Raft::new(node_id, raft_config, network, log_store, state_machine)`
  - 管理范围：shard map (`Namespace::ShardMap`)、schema (`Namespace::SchemaDef`)、MV/SQ 定义
  - 存储：`ControlPlaneStore`（RocksDB 后端）
  - 网络：`ControlRaftNetworkFactory`（基于 `RemoteGraphClient`）

- ✅ 数据面 quorum write（`nexora-zenoh/src/replica_writer.rs`）：
  - `ReplicaWriter::write_with_replication()` 主流程
  - Epoch fencing：`FencingToken` 验证
  - Quorum ack：`WriteConcern::Majority`（W > N/2）
  - **缺失**：无版本化、无两阶段提交、failover 未追赶

- ✅ State transfer 骨架（`nexora-zenoh/src/state_transfer.rs` 617 行）：
  - `catch_up_shard()` 完整逻辑（含断点续传 `TransferCheckpoint`）
  - `ExportShard`/`ExportDelta` 操作定义
  - **缺失**：HTTP 传输未接入、`ExportShard` 漏 sleep 节点（`graph_service_adapter.rs:258`）

- ✅ WAL 基础设施（`nexora-core/src/wal/`）：
  - Per-shard WAL files
  - Group commit（256 ops / 500µs）
  - **缺失**：torn-write repair、delta-only 格式

- ✅ Snapshot 机制（`nexora-core/src/graph/shard/mod.rs`）：
  - Node-level：`serialize_snapshot()`（`shard/mod.rs:704-730`）
  - LRU eviction + sleep/wake
  - **缺失**：零拷贝（当前 JSON）、校验和、manifest

**未完成但有骨架**：
- ⚠️ Data-plane Raft（`nexora-raft/src/lib.rs` 712 行）：
  - 接口完整但**未接入**复制路径
  - `main.rs:911` 横幅标记为 "EXPERIMENTAL SKELETON"
  - **决策**：不启用，走路线 B

- ⚠️ Checkpoint manager（`nexora-zenoh/src/checkpoint.rs` 401 行）：
  - `CheckpointManager` 结构完整
  - `create_full_checkpoint()` 有实现
  - **未集成**：不在 `GraphService` 启动流程

- ⚠️ Barrier coordinator（`nexora-barrier/src/lib.rs`）：
  - Chandy-Lamport / RisingWave 风格骨架
  - **未 wire**：不在 ingestion + flush 路径

### 5.2 关键文件路径速查

| 功能模块 | 核心文件 | 关键入口/结构 |
|---------|---------|--------------|
| **控制面 Raft** | `nexora-zenoh/src/control_raft.rs` | `ControlCommand` enum，`ControlRaftTypeConfig` |
| | `nexora-zenoh/src/control_raft_sm.rs` | `ControlStateMachine::apply()` |
| | `nexora-zenoh/src/cluster.rs:521` | Raft 启动：`openraft::Raft::new()` |
| **数据面复制** | `nexora-zenoh/src/replica_writer.rs` | `ReplicaWriter::write_with_replication()` |
| | `nexora-zenoh/src/replication.rs` | `FencingToken`, `WriteConcern` |
| **Quorum 读** | `nexora-zenoh/src/quorum_read.rs:171` | `select_consensus_value()`（**待修复**：字符串比较） |
| **State Transfer** | `nexora-zenoh/src/state_transfer.rs` | `StateTransfer::catch_up_shard()` |
| | `nexora-zenoh/src/graph_service_adapter.rs:258` | `ExportShard`（**待修复**：漏 sleep 节点） |
| **WAL** | `nexora-core/src/wal/log.rs` | `WALLog::append()`, `replay()` |
| | `nexora-core/src/wal/record.rs` | `WALRecord` 定义 |
| **Snapshot** | `nexora-core/src/graph/shard/mod.rs:704` | `serialize_snapshot()`, `deserialize_snapshot()` |
| | `nexora-core/src/graph/shard/mod.rs:441` | `wake_node()`（加载 snapshot + replay journal） |
| **Graph Service** | `nexora-core/src/graph/mod.rs` | `GraphService` 主结构 |
| | `nexora-core/src/graph/mod.rs:1255` | `flush_all_nodes()`（**待优化**：串行化） |
| **Projection** | `nexora-core/src/graph/projection.rs` | `ShardProjection`（lock-free `DashMap`） |
| **遍历** | `nexora-app/src/handlers.rs:1257` | `local_bfs()`（**待优化**：串行 `.await`） |

### 5.3 关键设计参数（默认值）

| 参数 | 当前值 | 位置 | Track A 建议 |
|------|--------|------|------------|
| Shard 数 | 256 | `GraphServiceConfig::num_shards` | 保持 |
| Replication Factor | 3 | `ClusterConfig::replication_factor` | 保持 |
| Write Concern | Majority | `WriteConcern::Majority` | 补强：严格 W+R>N |
| Read Concern | ❌ 无 | - | **新增**：`ReadConcern::Majority` |
| Group Commit | 256 ops / 500µs | `WALGroupCommit` | 保持 |
| LRU eviction | `max_nodes_per_shard` | `ShardConfig` | 补充：拓扑感知策略 |
| Snapshot 格式 | JSON | `serialize_snapshot()` | **改**：FlatBuffers |
| Epoch | 64-bit counter | `OwnerEpoch` | 保持 |

### 5.4 技术债务清单（防止重复造轮子）

**❌ 不要做**：
- 不要在 `replica_writer.rs` 之外另建一套 quorum write（已有，改强即可）
- 不要新建 Raft crate（`nexora-raft` 存在但不启用，走路线 B 不需要它）
- 不要为 snapshot 建新的序列化格式（复用 FlatBuffers，已在 `nexora-core` 依赖中）
- 不要在 `state_transfer.rs` 之外另建追赶协议（已有 `catch_up_incremental`，修 bug 即可）

**✅ 要改强的**：
- `quorum_read.rs:171`：字符串比较 → 版本号比较
- `graph_service_adapter.rs:258`：`ExportShard` 只导出常驻节点 → 先 flush 再遍历 persistor
- `replica_writer.rs`：加两阶段提交（validate → WAL → publish）
- `shard/mod.rs:704`：JSON → FlatBuffers
- `handlers.rs:1257`：`local_bfs` 串行 → `buffer_unordered` 并发

### 5.5 ArcadeDB 借鉴点的具体映射

| ArcadeDB 技术 | 对应文件/类 | Nexora 应用位置 | Track A 任务 |
|--------------|------------|----------------|------------|
| Two-phase commit | `TransactionContext.java:1164` | `replica_writer.rs` | A1.1 |
| Torn-write repair | `WALFile.java:replay()` | `wal/log.rs:replay()` | A4 |
| Group commit batching | `RaftGroupCommitter.java` | 已有 `WALGroupCommit` | C4（增强） |
| Snapshot manifest | `SnapshotManager.java` | **新建** `SnapshotManifest` | B1/B3 |
| Per-DB divergence isolation | `ArcadeStateMachine.java:applyTransaction()` | `control_raft_sm.rs` | A6 |
| Bootstrap fingerprint | `BootstrapElection.java` | **新建** 追赶协议 | A1.3 |

### 5.6 验证清单（防止遗漏）

每个 Track A 任务完成后必须通过：

**A1.1 两阶段提交**：
- [ ] Unit test：`validate` 失败 → 全回滚（无部分写）
- [ ] Unit test：`WAL append` 失败 → 全回滚
- [ ] Unit test：`publish` 失败 → 已 WAL 持久化，重启后 replay 恢复
- [ ] Integration test：并发写入 + kill owner → 新 owner replay → 无丢失/重复

**A1.3 Failover 追赶**：
- [ ] Unit test：新 owner 调用 `catch_up_incremental(from_seq)` 返回正确 ops
- [ ] Integration test：kill owner → 控制面 Raft 选新 owner → 追赶 → 开始服务 → 数据一致
- [ ] Chaos test：网络分区期间写入 → 愈合 → 追赶 → 最终一致

**A2 强一致读**：
- [ ] Unit test：并发写（epoch N）+ 读 → 读到 epoch N 的值，非 N-1
- [ ] Unit test：Read-repair 修复落后副本
- [ ] Integration test：写 → 立即 quorum 读 → 必须读到新值

**A4 崩溃恢复**：
- [ ] Chaos test：写入中 kill -9 → restart → replay → 验证一致（对比 pre-kill snapshot）
- [ ] Chaos test：torn-write 注入（截断 WAL 文件）→ replay → 等版本幂等修复或版本跳跃报错
- [ ] 长稳 test：72h 持续写 + 每小时随机 kill → 累计无数据损坏

### 5.7 Metrics 观测点（阶段 2 灰度必看）

| Metric | 含义 | 健康阈值 | 告警条件 |
|--------|------|---------|---------|
| `replication_quorum_failed_total` | Quorum 未达到次数 | < 0.1% 写入量 | > 1% 持续 5min |
| `replication_missing_acks_total` | 副本未 ack 次数 | < 1% 写入量 | > 5% 持续 5min |
| `failover_triggered_total` | Failover 触发次数 | 预期内（计划维护） | 非计划 failover |
| `wal_replay_duration_seconds` | WAL replay 耗时 | < 10s | > 60s |
| `snapshot_load_duration_seconds` | Snapshot 加载耗时 | < 1s | > 5s |
| `projection_miss_rate` | Projection 未命中率 | < 10% | > 30% |
| `control_raft_leader_changes` | 控制面 leader 切换 | < 1 次/小时 | > 3 次/小时 |

---

## 第六部分：决策记录（ADR）

### ADR-001：采用路线 B（混合架构）

**日期**：2026-07-18  
**状态**：✅ 已采纳  
**决策**：控制面 Raft（保持）+ 数据面强化 Primary-Backup（不引入数据面 Raft）

**上下文**：
- 控制面 Raft（openraft 0.9）已集成运行 2388 行代码
- 数据面当前是 quorum write + epoch fencing，无共识
- 路线 A（数据面也走 Raft）需 6-9 月，256 Raft groups 拖累高吞吐
- 路线 B（强化 Primary-Backup）需 4-6 月，契合流式定位

**决策依据**：
1. **负载特征**：数据面是高吞吐事件流（每秒万级），不是低频事务
2. **架构契合**：Actor-per-Node 已分散写，再套 Raft group 协调复杂度高
3. **定位对齐**：流式平台语义是最终一致 + 幂等重放（对标 Flink/RisingWave），非金融级强一致
4. **工程务实**：补强（两阶段 + 追赶 + W+R>N）比重写快 2-3 月

**后果**：
- ✅ 正向：快速达到生产级，架构轻量，性能无瓶颈
- ⚠️ 风险：极端时序窗口仍有风险，需严格 chaos 测试验证
- 🔄 可逆：如果混合架构不够，可局部引入数据面 Raft（小 shard 走 Raft，大 shard 保持 quorum）

**验证策略**：
- 阶段 1（2-3 周）：实现两阶段 + 追赶 + 强一致读
- 阶段 2（1-2 月）：非关键业务灰度 + 持续 chaos 测试，观察 `ReplicationMetrics`
- 决策点：如果灰度期暴露不可接受的风险，重新评估是否局部引入 Raft

---

## 附录：术语表（防止歧义）

| 术语 | 定义 | 代码位置 |
|------|------|---------|
| **控制面 Raft** | openraft 管理元数据（shard map/schema），低频强一致 | `control_raft*.rs` |
| **数据面** | 图节点数据的写入/复制/查询，高吞吐事件流 | `replica_writer.rs` |
| **Quorum write** | 写入需 W > N/2 副本 ack | `WriteConcern::Majority` |
| **Epoch fencing** | 用单调递增 epoch 拒绝旧 owner 写入 | `FencingToken` |
| **两阶段提交** | replicate → quorum → owner apply，quorum 失败不写 owner | **已实现未接线**（`quorum_write_two_phase`，见 TD-1） |
| **Torn-write repair** | WAL replay 时，等版本幂等重放 + 版本跳跃报错 | **待实现** A4 |
| **Failover 追赶** | 新 owner 从存活副本 `catch_up_incremental` 追最新 | **待实现** A1.3 |
| **Projection** | Lock-free `DashMap` 读旁路，绕过 actor mailbox | `projection.rs` |
| **Standing Query** | 增量模式匹配，变更触发推送 | `nexora-standing-query` |
| **Offset-aligned checkpoint** | `(kafka_offset, graph_state)` 原子快照对，exactly-once | **待实现** B2 |

---


## 附录：已知限制与技术债（2026-07-18 核实）

> 逐行源码核实结论。这些不是 bug，而是明确记录的设计取舍与能力边界，供运维与后续开发决策。

### TD-1：数据面写路径用 best-effort quorum，未接两阶段提交

**位置**：`crates/nexora-app/src/handlers.rs::AppState::replicate_write`

**现状**：
- 生产写路径（`set_property` / `add_edge`）走 **owner-first** 模式：先在本地 owner graph 提交，再调 `ReplicaWriter::quorum_write` best-effort 复制到 follower，quorum 未达成返回 503。
- 两阶段路径 `ReplicaWriter::quorum_write_two_phase`（replicate-first：先复制 follower，quorum 达成才经 `owner_apply` 回调写 owner）**已实现且有单测**（`test_two_phase_applies_to_owner_after_quorum` / `test_two_phase_aborts_if_quorum_not_reached`），但 `ReplicaWriter` 构造时未配 `owner_apply`，也未在写路径调用。

**影响面**：
| 场景 | 是否有差异 |
|------|:---:|
| RF=1（默认） | 无——两条路径都只写 owner |
| RF>1 + quorum 成功 | 无——结果一致 |
| RF>1 + quorum 失败 | **有**——owner-first 留下"孤儿写"（owner 有、多数 follower 没有），客户端收到 503 |

**为何不阻塞生产**：
- SetProperty（last-writer-wins）/ AddEdge（集合成员）幂等，重放收敛——符合平台"最终一致 + 幂等重放"定位（对标 Flink/RisingWave，非 Spanner 强一致）。
- `anti_entropy.rs` 后台 digest 比对会把孤儿写最终同步到 follower；owner failover 时被 catch-up 覆盖。孤儿写最终收敛，不永久分歧。

**何时必须修**：
- 部署需要"严格无部分写"的原子多副本语义（金融级）——届时反转写路径（不先本地提交、传 `owner_apply` 回调、owner 阻塞等 follower quorum）。代价：更高写延迟 + 较大重构。

### TD-2：写路径未携带幂等键（request_id）

**位置**：`handlers.rs::replicate_write` 的 `FencingToken::new(shard_id, epoch)` 未调 `.with_request_id()`

**现状**：`ReplicaWriter` 有 `idempotency` 去重缓存，但写路径不传 `request_id`，故该缓存在生产路径不生效。当前靠 op 本身幂等（SetProperty/AddEdge）兜底，安全。

**何时变成真 bug**：一旦引入**非幂等 op**（如计数器自增、append-only 列表），客户端/failover 重试会重复应用。**引入非幂等 op 前必须先接 request_id 幂等键（并配合 TD-1 的两阶段）。**

### 复核方式

```
# TD-1：确认写路径调用 best-effort 而非两阶段
grep -n "quorum_write\|quorum_write_two_phase" crates/nexora-app/src/handlers.rs

# TD-2：确认 FencingToken 未带 request_id
grep -n "FencingToken::new\|with_request_id" crates/nexora-app/src/handlers.rs
```

## 附录：已核实达成的任务（避免误判为未完成）

> 这些任务的 ROADMAP 目标在代码中**早已满足**（属设计之初的能力，非新增缺口）。此处记录核实结论 + 复核命令，防止后续因任务跟踪状态残留而误以为"未做"。

### VER-1：D5 摄入吞吐优化 —— 两个优化点均为现有设计，无需改动

**核实日期**：2026-07-18（代码实证，非推测）

**背景**：D5 的自动化核实曾因后台进程退出丢失，任务状态一度残留为 pending，易被误读为"未完成"。经逐行读码确认，D5 描述的两个优化点 Nexora 都已实现：

**① per-shard WAL 文件池（等价 ArcadeDB `activeWALFilePool`）—— 已实现**
- `GraphShard.wal: Option<SharedWal>`，`SharedWal = Arc<Mutex<WriteAheadLog>>`，每个 shard 一个独立实例（`shard/mod.rs:59`）。
- 构造循环给每个 shard 独立 WAL 目录：`wal_dir.join(format!("shard_{id}"))`（`graph/mod.rs:310`），注释亦写明 "Each shard gets its own WAL file under `wal_dir/shard_{id}/`"。
- 结论：256 shards = 256 个独立 WAL Mutex + 独立文件，不存在全局单 WAL 锁热点。

**② delta-only WAL 记录 —— 已实现**
- `NodeChangeEvent`（`event.rs:61`）是细粒度增量事件：`PropertySet{key,value}` / `PropertyRemoved{key}` / `EdgeAdded{edge}` / `LabelAdded{label}` / `EdgePropertySet{...}`。
- 每个 WAL 记录只记单个变更，不是全量节点快照（全量仅出现在 sleep 时的 snapshot，作为 journal truncation 点，属另一机制）。

**结论**：D5 的正确产出是"核实确认已优化"，而非写新代码。任务已标记 completed。

### 复核方式

```
# ① 每 shard 独立 WAL 目录
grep -n "shard_wal_dir\|format!(\"shard_{id}\")\|Each shard gets its own WAL" crates/nexora-core/src/graph/mod.rs

# ② delta-only 事件（细粒度变更，非全量快照）
grep -n "enum NodeChangeEvent\|PropertySet\|EdgeAdded\|LabelAdded" crates/nexora-core/src/event.rs
```
