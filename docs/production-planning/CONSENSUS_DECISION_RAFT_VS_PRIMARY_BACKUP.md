# 共识路线决策：成熟 Raft 库 vs 强化 Primary-Backup

**制定日期**: 2026-07-18  
**决策人**: [待填]  
**背景**: Track A 正确性地基的关键分叉点，决定整体时长和一致性强度  

---

## 一、现状核查（file:line 级证据）

### 1.1 代码里已经有两套 Raft 实现！

#### ✅ Control-Plane Raft（基于 openraft 0.9，**已集成**）

**依赖**：`crates/nexora-zenoh/Cargo.toml:openraft = { version = "0.9", features = ["serde", "storage-v2"] }`

**作用范围**（`control_raft.rs:1-10`）：
> This is the *control plane*: it replicates and agrees on nexora's own **metadata** (shard map, MV/SQ/schema definitions), which is non-replayable and must stay consistent across the cluster with a single elected owner per shard.

**完成度**：
- ✅ 6 个模块完整实现（2388 行）：
  - `control_raft.rs` (213 行)：type config 定义
  - `control_raft_sm.rs` (502 行)：state machine over `ControlPlaneStore`
  - `control_raft_log.rs` (348 行)：log storage
  - `control_raft_storage.rs` (140 行)：storage adapter
  - `control_raft_network.rs` (287 行)：RPC network layer
  - `control_raft_topology.rs` (186 行)：voter topology resolution
- ✅ **已 wire 到启动流程**：`cluster.rs:388-543` 显示在 ClusterManager 启动时初始化 openraft::Raft
- ✅ **测试覆盖**：roundtrip tests 存在（`control_raft.rs:181-209`）
- ⚠️ **作用域限定**：只管控制面元数据，**不管数据平面的 WAL 复制**

#### ⚠️ Data-Plane Raft（nexora-raft crate，**骨架未启用**）

**定义**：`crates/nexora-raft/src/lib.rs:1-30`
> A simplified Raft protocol focused on log replication and quorum commit... bridges the gap between local WAL writes and distributed replication.

**关键设计**（`lib.rs:20-29`）：
- **No leader election**：shard ownership 由 ShardMap + OwnerEpoch 管理，owner = de facto leader
- **Raft log = WAL**：复用现有 WAL 作为 Raft log
- **Quorum commit**：先 append 本地 WAL，再复制到 followers，quorum ack 后 commit

**完成度**：
- ✅ 接口定义完整（`ReplicationTarget` trait, `LogEntry`, `AppendEntriesResponse`）
- ❌ **未在 replica_writer.rs 中使用**：当前 `ReplicaWriter` 直接用 `RemoteGraphClient` 发 `GraphOperation`，不走 Raft log append
- ❌ **main.rs:911 横幅明示**：
  ```rust
  "⚠️  --raft-port ENABLES AN EXPERIMENTAL LOG-SHIPPING SKELETON, NOT \
   RAFT CONSENSUS — no leader election, no real log replication."
  ```

**结论**：nexora-raft 是**理论框架**，未实际接入复制路径。

---

### 1.2 当前数据平面复制的实际实现：Primary-Backup + Quorum（无共识）

**位置**：`crates/nexora-zenoh/src/replica_writer.rs:1-12`

**流程**：
```
Owner 收到写 → 验证 epoch → 并发发给所有 followers
  → 收集 acks (timeout) → 如果 quorum 达到 → commit
```

**关键特征**：
- ✅ Epoch fencing（`FencingToken`）
- ✅ Quorum write（`WriteConcern::Majority` = W > N/2）
- ✅ 并发复制 + timeout
- ❌ **无 Raft log consensus**：没有 term、没有 leader election、没有日志一致性检查

**风险**（`PRODUCTION_GAP_ASSESSMENT.md:41-45`）：
> Owner 在写已 ack 但未充分复制时崩溃，failover 到落后副本会丢已确认写。

---

## 二、两条路线的对比分析

### 路线 A：引入数据平面 Raft（基于 openraft，类比控制面）

**技术方案**：
1. 复用 openraft 0.9（控制面已验证）
2. 为数据平面定义 `DataRaftTypeConfig`：
   - `D = WAL entry`（当前的 `NodeChangeEvent` 序列化）
   - `R = CommitStatus`
   - State machine = apply to graph
3. 每个 shard 有自己的 Raft group（或按 shard 范围分组）
4. Owner = Raft leader，followers = Raft followers
5. Write path：`write_batch → propose to Raft → wait quorum commit → apply`

**优势**：
- ✅ **完整的共识保证**：日志一致性、leader election、term 防护
- ✅ **不会丢已 ack 的写**：Raft 的 quorum commit 语义天然保证
- ✅ **成熟实现**：openraft 0.9 已在控制面运行，不是新依赖
- ✅ **对标 ArcadeDB**：Apache Ratis 同等级能力

**代价**：
- ❌ **架构重**：每个 shard 一个 Raft group → 256 shards = 256 Raft 状态机
- ❌ **性能开销**：Raft log append + 网络 round-trip + fsync；写延迟增加
- ❌ **工程量大**：4-8 周（需要重写 `replica_writer.rs`，接入 Raft propose/apply，测试 leader election/failover）
- ⚠️ **WAL 双写？**：当前 WAL 是 per-shard 文件，Raft log 也是 per-group 文件，如何避免重复存储需设计

**对 Nexora 定位的契合度**：
- ⚠️ **流式事件驱动的负载特征**：Raft 为"事务型、低频、强一致"优化（如控制面元数据）；高吞吐事件流（Kafka 每秒万条写入）走 Raft propose 会成为瓶颈
- ⚠️ **Actor-per-Node 模型已分散写**：数百万节点各自写，再套一层 Raft group 协调复杂度高

---

### 路线 B：强化 Primary-Backup（无 Raft consensus，继续 quorum）

**技术方案**：
1. 保持当前 `ReplicaWriter` 的 epoch fencing + quorum write
2. 补强三处缺陷：
   - **A. 同步复制 + 严格 W+R > N**：write 必须等 quorum fsync，read 必须等 quorum 响应（借鉴 Cassandra quorum）
   - **B. Failover 时的 fencing + 追赶**：新 owner 上任前，用 `ExportDelta` 从存活副本追赶到最新 seq，确保不丢已 ack 的写
   - **C. 版本化冲突检测**：每个副本记录 `(epoch, seq)` 对，failover 时版本不连续拒绝服务（快速失败）

3. **借鉴 ArcadeDB 的轻量化保证**：
   - Two-phase commit：`validateAndBumpVersions → writeToWAL → publishPages`
   - Torn-write repair：replay 时对相等版本幂等重放
   - Abandoned TX tracking：timeout 后仍可能 commit 的写保留 10min 追踪

**优势**：
- ✅ **架构轻**：不引入 Raft group，当前 epoch fencing + quorum 基础上打补丁
- ✅ **性能保持**：写延迟 = 网络 RTT + quorum fsync，无 Raft log append 开销
- ✅ **工程量小**：2-3 周（主要是 A4 崩溃恢复验证 + A5 两阶段提交 + B 追赶逻辑）
- ✅ **契合流式负载**：event-driven 高吞吐不受 Raft consensus 瓶颈

**代价**：
- ❌ **一致性保证弱于 Raft**：极端时序窗口（同时 failover + 网络分区）仍有风险
- ❌ **无 leader election**：依赖控制面 Raft 选 owner，数据面自己不会自愈
- ⚠️ **需要严格证明**：W+R>N + epoch fencing 在各故障组合下不丢不重，需形式化验证或大量 chaos 测试

**风险**：
- ⚠️ **不适合"关键业务+强一致"目标**：`PRODUCTION_GAP_ASSESSMENT.md:45` 明确指出"关键业务+强一致，倾向 (a) Raft"

---

## 三、我的分析与建议

### 3.1 关键判断：Nexora 的定位决定了不该走纯 Raft

**理由 1：已经有 control-plane Raft，再加 data-plane Raft 是架构重复**

openraft 已经在控制面运行，管理 shard map / schema / MV/SQ 这些"低频、强一致"的元数据。这是 Raft 的主场。

但**数据平面的写入是完全不同的负载特征**：
- 控制面：低频（每秒几次 shard 迁移/schema 变更），小负载（KB 级 JSON）
- 数据平面：高频（每秒万级事件流写入），Actor-per-Node 已经分散到数百万节点

如果数据面也走 Raft，意味着：
- **256 shards × Raft group = 256 个独立共识域**，每个有自己的 leader election / log / state machine
- **每个 Actor 写入 → 提交到所属 shard 的 Raft group → propose → quorum append → commit → apply**
- 这把"无全局锁、细粒度并发"的 Actor 优势抵消了——Raft group 成为新的协调点

**理由 2：流式事件驱动 + Actor 模型天然契合最终一致性**

Event Sourcing 的语义是：**事件是真相，状态是派生**。Nexora 的 WAL 已经是事件流的持久化，Actor-per-Node 保证单节点串行处理。这和 Kafka 的 partition / Flink 的 keyed state 是同一模式——**最终一致 + 幂等重放**，不是强一致事务。

强一致（Raft）适合的是"事务型、低并发、不能丢不能重"的场景（如银行转账、控制面元数据）。

**理由 3：控制面 Raft 已经提供了"收敛到一致状态"的保证**

控制面 Raft 管理 shard map + epoch。这意味着：
- Owner 切换由 Raft 共识决定（不会有双主）
- Epoch fencing 确保旧 owner 的写被拒绝
- 新 owner 上任前从存活副本追赶（`ExportDelta`）

这套机制 + quorum write + W+R>N，已经能做到"**崩溃不丢、最终收敛**"——代价是 failover 窗口内可能短时不可用，但**对流式平台这是可接受的**（Flink / RisingWave 的 failover 也是秒级到分钟级）。

### 3.2 推荐路线：**混合架构（控制面 Raft + 数据面强化 Primary-Backup）**

具体方案：

#### 阶段 1：保持当前架构，补强三处（2-3 周）

1. **A5 两阶段提交**（借鉴 ArcadeDB）：
   ```rust
   validate_and_bump_versions() → write_to_wal() → publish_to_actors()
   ```
   WAL append 是唯一不可回退点。

2. **A4 崩溃恢复验证 + torn-write repair**：
   - 等版本重放（幂等修复）
   - 版本跳跃报错（不静默接受损坏）
   - Chaos 测试：kill -9 → restart → replay → 验证一致

3. **Failover 追赶协议**（依赖控制面 Raft 选主）：
   ```rust
   // 新 owner 上任前（控制面 Raft 已共识通过）
   new_owner.catch_up_incremental(old_replicas, from_seq);
   // 追赶完成后才开始服务
   ```

4. **严格 W+R > N（可选配）**：
   ```rust
   WriteConcern::Majority + ReadConcern::Majority
   // 保证读到最新已提交
   ```

#### 阶段 2：观察与验证（1-2 月）

- 在非关键业务灰度，持续 chaos 测试
- 监控 `ReplicationMetrics`：quorum_failed / missing_acks
- 长稳 soak（72h+ 持续写入 + 周期 failover）

#### 阶段 3（可选）：如果发现混合架构不够，再局部引入数据面 Raft

**不是全面替换，而是分层**：
- **热路径（高频写）**：保持 quorum，不走 Raft
- **冷路径（低频 DDL、大事务）**：可选走 Raft 保证

或者：
- **小 shard（<100 节点）**：走 Raft（consensus 开销可接受）
- **大 shard（>10k 节点）**：走 quorum（吞吐优先）

---

## 四、决策矩阵

| 维度 | 路线 A（数据面 Raft） | 路线 B（强化 Primary-Backup） | 推荐 |
|------|:---:|:---:|:---:|
| **一致性强度** | 强（Raft 共识） | 中（最终一致 + W+R>N） | 取决于目标 |
| **工程量** | 4-8 周 | 2-3 周 | B |
| **架构复杂度** | 高（256 Raft groups） | 中（控制面 Raft + 数据面 quorum） | B |
| **性能** | 写延迟增加（Raft log） | 写延迟低（直接 quorum） | B |
| **契合流式定位** | 不契合（Raft 为事务型优化） | 契合（事件流最终一致） | B |
| **对标 ArcadeDB** | 平级（都用 Raft） | 低一级（无数据面共识） | - |
| **风险** | 架构重，可能拖累吞吐 | 极端场景仍有风险 | B（可灰度验证） |

---

## 五、最终建议

### ✅ 推荐：**路线 B（强化 Primary-Backup），阶段性推进，保留 Raft 选项**

**理由**：
1. **架构契合**：控制面 Raft（已有）+ 数据面 quorum = 分层共识，各司其职
2. **工程务实**：2-3 周 vs 4-8 周，在共识未就位前不该投 4-8 周到可能拖累性能的方案
3. **定位匹配**：流式事件驱动 + Actor 模型的语义是最终一致，不是强一致事务
4. **可灰度验证**：补强后先灰度非关键业务，chaos 测试观察；如果不够再局部引入 Raft

### ⚠️ 如果必须选"强一致 + 关键业务"目标，则走路线 A

但要接受：
- 时间拉长到 6-9 月
- 写延迟增加
- 架构变重（256 Raft groups）
- **可能与流式高吞吐定位冲突**

### 🔑 关键是先问清楚：Nexora 要做什么级别的"一致性"？

- **如果是"事件流平台，容忍 failover 秒级恢复，数据最终一致"** → 路线 B
- **如果是"金融级强一致，已 ack 的写绝不能丢"** → 路线 A

**我的判断**：从"流式事件驱动的图状态平台"这个定位出发，前者更合理。后者应该交给上游 Kafka（它已经有 Raft 或 Paxos 保证）。

---

## 附：ArcadeDB 的实际做法（作为参照）

ArcadeDB 用 Apache Ratis（成熟 Raft），但它的定位是**事务型图数据库**，不是流式平台：
- 写入是低频事务（ACID，每秒几百到几千 TPS）
- 用户期望"强一致读、写不丢"
- 没有 Actor-per-Node，而是 page-based MVCC

所以 Ratis 对它是合理的。但**把 ArcadeDB 的选择直接复制到 Nexora 是刻舟求剑**——负载特征和架构模型都不同。

正确的借鉴是：学 ArcadeDB 的**两阶段提交、torn-write repair、abandoned TX tracking** 这些轻量化保证技巧，而非照搬 Raft。
