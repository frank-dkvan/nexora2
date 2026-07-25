# 高可用（HA）落地路线图

**状态**: 提案 | **生成日期**: 2026-07-08 | **依据**: 对分布式路径的源码逐项审计（file:line 见下）

> 本文档不是运维手册（那是 [cluster-ops.md](../cluster-ops.md)）。这是一份把当前**分布式脚手架**改造成**真实生产 HA**的分阶段实施计划，给排期与对外沟通用。每个阶段都标注了改动范围、依赖顺序、工作量粗估，以及"完成后能扛住什么故障"的验收标准。

---

## 0. 现状诊断（为什么现在不是 HA）

基于源码审计，`--cluster` 模式当前是 **pre-alpha 脚手架**：分布式组件作为进程存在，但**不在读写数据平面里**。核心事实：

| 断言（README） | 真实现状 | 证据 |
|---|---|---|
| 跨节点数据平面 | ❌ HTTP 写直打本地 `GraphService`，不走 router | `handlers.rs:552`；`AppState` 无 router/cluster 字段 |
| N 副本复制 | ❌ 复制因子恒为 1；`ReplicaWriter` 生产零调用；副本集从不填充 | `shard_map.rs:78`；`replica_writer.rs`（仅测试）；`control.rs:170` |
| Raft 共识 | ❌ 无选举（自认，`lib.rs:21`）；发空 payload 日志；term 恒为 1 | `raft/lib.rs:270-279`；`write_through.rs:130-141`；`raft_handler.rs:552` |
| 写入 quorum 确认 | ❌ 实时写直接落本地，无 quorum 门控 | `handlers.rs:552`；`append_local` 无调用者 |
| 故障后数据不丢 | ❌ failover 只换所有权切给自己；`state_transfer` 无生产实现，新 owner 拿到空 shard | `cluster.rs:261-286`；`state_transfer.rs:164-188` |
| 故障检测 | ✅ 心跳 + 超时（真实，但只驱动一个没人查的 ShardMap） | `cluster.rs:199-290` |
| 脑裂防护 | ⚠️ quorum 门控 + fencing（逻辑正确、有单测，但守的是写路径不查的控制结构） | `control.rs:73-129` |
| 动态成员 | ❌ 静态配置；收到的 gossip 被丢弃 | `cluster.rs:520`；`main.rs:747-765` |
| 多节点端到端故障测试 | ❌ 无一个测试真正杀主验证数据不丢 | `distributed_integration.rs`（全孤立组件/手搭路由） |

**一句话**：单机 + WAL 崩溃恢复扎实可靠；跨节点 HA 不成立——一个节点挂掉，它的数据丢失、shard 恢复为空，且各节点本就只服务本地图。

### 可复用的地基（不是从零）

审计确认以下部件设计正确、有单测，可作为后续阶段的基础，无需重写：
- follower 侧 `handle_raft_rpc`：按序应用、拒绝空洞、去重、fail-closed（`raft_handler.rs:447-583`）
- `OwnerEpoch` fencing token（`replication.rs:7-37`）
- quorum 门控的 `failover_shard` / `propose_shard_map_update`（`control.rs:73-129`）
- `ReplicaWriter::quorum_write` 的 fan-out + 超时 + 多数派计票逻辑（`replica_writer.rs:61-146`）
- 心跳 + 失败检测循环（`cluster.rs:199-290`）

问题不在这些部件本身，在于**它们没有被接进服务路径，也没有端到端验证**。

---

## 1. 设计目标与前置决策

在动手前必须先定下这几个语义，否则各阶段会互相打架：

1. **一致性模型**：目标是**单主每分片 + 同步 quorum 复制**（类似 Raft 的 leader-based），不是多主。每个 shard 有一个 owner（写入口）+ N 个 follower，写入需多数派确认才 ack。读默认走 owner（强一致）；后续可选副本读（最终一致）。
2. **复制因子**：默认 RF=3（1 owner + 2 follower，容忍 1 节点故障）。RF 可配置，但必须 ≥ 与 quorum 语义自洽（`quorum = RF/2 + 1`）。
3. **持久性边界**：写 ack = "owner WAL 已落盘 **且** quorum 个副本已确认"。这继承单机的 fsync 语义（见 [performance-tuning](../performance-tuning.md)），跨节点后延迟 = max(本地 fsync, 最慢的 quorum 副本 RTT)。
4. **失败语义**：quorum 达不到时**写失败并明确报错**（不静默落本地），客户端可重试。这优先于可用性——宁可拒写，不可丢数据后假装成功。
5. **不做的事（明确划出范围外）**：动态成员共识（joint consensus）、跨 shard 事务、多主/无主复制。这些是 v2 话题，本路线图不覆盖。

---

## 2. 阶段总览与依赖

```
阶段 0  ── 诚实化现状（文档 + 启动告警）        [S,  低风险]
   │
阶段 1  ── 写路径接入 router + 单主路由          [L,  中风险]   ← 数据平面地基
   │
阶段 2  ── 同步 quorum 复制（写复制到副本）      [XL, 高风险]   ← 依赖 1
   │
阶段 3  ── state transfer（failover 数据恢复）   [XL, 高风险]   ← 依赖 2
   │
阶段 4  ── failover 选主 + fencing 端到端接线    [L,  高风险]   ← 依赖 2,3
   │
阶段 5  ── 故障注入端到端测试 + 混沌验证         [L,  中风险]   ← 依赖 1-4
```

工作量记号：S≈1-2 天，L≈1-2 周，XL≈3-6 周（单人、含测试）。风险指"改坏现有单机路径 / 引入数据正确性 bug"的概率。

**关键依赖原则**：阶段必须严格顺序执行。跳过阶段 2 直接做 failover（当前代码的错误）就是"切换到没有数据的节点"。每个阶段结束时系统仍可发布（degrade gracefully），不留半接线状态。

---

## 阶段 0 — 诚实化现状（S，低风险）

**目标**：在真实 HA 落地前，不让运维误以为 `--cluster` 提供了容错。这是安全责任，先做。

**改动**：
- `--cluster` / `--raft-port` 启动时打印显式告警：当前分布式为实验性，**不提供跨节点容错，节点故障会丢该节点数据**。
- README 的 "Distributed / Raft consensus" 表述改为标注实验性/开发中，去掉 "consensus" 措辞（无选举，名不副实）。
- cluster-ops.md 顶部加醒目 banner 指向本路线图。

**改动范围**：`main.rs`（启动日志）、`README.md`、`cluster-ops.md`。纯文档 + 日志，不碰数据路径。

**验收标准**：
- 启动 `--cluster` 时日志含明确的"无容错"告警。
- 无任何文档再宣称生产级 HA / Raft 共识。

**完成后能扛住的故障**：无（现状不变），但**消除了误导**——运维不会误配多节点当 HA 用。

---

## 阶段 1 — 写路径接入 router + 单主路由（L，中风险）

**目标**：让数据平面真正跨节点。这是所有后续 HA 的地基——没有它，复制/failover 都是空中楼阁。

**当前问题**：`handlers.rs:552` 直接 `state.graph.set_property()` 打本地图；`AppState` 无 router。每个节点只服务本地图。

**改动**：
1. `AppState` 增加可选 `router: Option<Arc<HybridRouter>>` 字段（单机模式为 `None`，行为不变）。
2. 写/读 handler 改为：若 router 存在，按 `shard_of(qid)` 查 ShardMap → 本地 shard 直接走 `state.graph`，远程 shard 走 `router.route()` 转发到 owner 节点。
3. 集群启动时（`main.rs:729`）把已构造的 `ClusterManager` 的 router 接进 `AppState`。
4. `ShardMap::new_local` 的初始化改为按集群成员分配 owner（当前恒为本地）。

**改动范围**：`handlers.rs`（读写 handler 重构）、`main.rs`（AppState 装配）、`router.rs`（可能需补 API）、`shard_map.rs`（初始分配）。**这是 AppState 重构，触及所有读写 handler**——中风险主要在于别改坏单机路径。

**验收标准**：
- 单机模式（无 `--cluster`）：行为**逐字节不变**，所有现有测试通过。
- 两节点集群：向 node-A 写一个属于 node-B 的 shard 的 key，能在 node-B 读到（跨节点路由生效）。
- 新增端到端测试：真起两节点（真实 TCP + 真实 GraphService，不是 mock handler），验证跨节点读写一致。

**完成后能扛住的故障**：仍**不能**扛节点故障（RF 仍为 1），但数据平面已跨节点——为阶段 2 铺好路。**这一步本身不提升容错，是纯地基。**

---

## 阶段 2 — 同步 quorum 复制（XL，高风险）

**目标**：写入复制到多副本并等 quorum 确认。这是 HA 的核心——完成后单节点故障不再丢数据。

**当前问题**：RF 恒为 1（`shard_map.rs:78` 永远 `replicas: vec![]`）；`ReplicaWriter::quorum_write` 生产零调用；副本集从不填充。

**改动**：
1. **副本集分配**：集群启动/成员变化时，为每个 shard 按 RF 分配 owner + followers，调用 `assign_replicas`（当前生产零调用）。
2. **写路径接 quorum 复制**：owner 本地 WAL 落盘后，走 `ReplicaWriter::quorum_write` 并行复制到 followers，等多数派 ack 才向客户端 ack。复用现有 `quorum_write` 的 fan-out + 超时逻辑（`replica_writer.rs:61-146`）。
3. **补上真实 payload**：Raft 路径的空 payload（`lib.rs:270-279`）和 `WalLogReader::fill_entries` stub（`write_through.rs:130-141`）必须实现——从 WAL 读真实记录发给 follower。
4. **失败即报错**：quorum 达不到时返回明确错误，客户端可重试。**决策点**：owner 已本地写但 quorum 失败时，是回滚还是标记"未提交待协调"？建议后者（配合 fencing epoch），因为回滚跨节点很难做对。
5. **幂等**：复制带 seq_no，follower 去重（follower 侧 `handle_raft_rpc` 已具备此能力，`raft_handler.rs:447-583`）。

**改动范围**：`replica_writer.rs`（接线）、`cluster.rs`/`zenoh_cluster.rs`（副本分配）、`raft/lib.rs` + `write_through.rs`（真实 payload）、写 handler（接 quorum_write）。**最大、最高风险的一块**——涉及分布式写正确性。

**验收标准**：
- RF=3 集群：写入后，杀掉 owner，数据仍能从 follower 读到（**数据不丢**）。
- quorum 达不到时（杀掉 2/3 副本）写入明确失败报错，**不静默落本地**。
- 复制是同步的：客户端收到 ack ⟺ quorum 副本已持久化（用故障注入验证：ack 后立即杀节点，数据不丢）。
- owner 本地写但 quorum 失败的场景有明确定义的行为 + 测试。

**完成后能扛住的故障**：**owner 节点故障不丢数据**（数据在 follower 上），但此时该 shard 仍不可写（还没 failover）——需要阶段 3/4 才能自动恢复服务。

---

## 阶段 3 — state transfer（failover 数据恢复）（XL，高风险）

**目标**：让一个新加入或落后的节点能把某 shard 的完整数据补齐。这是 failover 能"切到有数据的节点"的前提。

**当前问题**：`StateTransferManager` 只在 HashMap 里记状态标记，`StateTransferClient::fetch_state` 只有 trait、**无任何生产实现**（`state_transfer.rs:164-188`）；failover 流程根本不调用它。

**改动**：
1. **实现 `StateTransferClient`**：新 owner 向存活副本请求 `(latest snapshot, WAL since snapshot)`，落盘后回放补齐。协议设计文档已存在（`state_transfer.rs:1-12` 注释），照它实现真实的网络拉取。
2. **接入 sleep/wake 与快照机制**：复用单机已有的 snapshot 序列化 + WAL 回放（阶段性成果，见 GAP-1 修复），跨节点传输的是同一套快照格式。
3. **增量追赶**：传输期间 owner 仍在写，需要传 snapshot 后再补传增量 WAL，直到追平才切读流量。
4. **背压与限流**：大 shard 传输不能打满网络/影响在线请求（`state_transfer.rs:74-76` 已有 batch/retry 配置字段）。

**改动范围**：`state_transfer.rs`（真实实现）、`raft/lib.rs`（接线）、需要一个跨节点的 snapshot/WAL 传输 RPC（可复用 TCP transport）。**高风险**：涉及数据完整性 + 追赶期一致性。

**验收标准**：
- 新节点加入 RF=3 集群，能从存活副本完整拉取一个 shard 的数据并追平。
- 传输中途 owner 继续写，传输完成后新节点数据与 owner **逐条一致**（用 checksum/Merkle 验证——`MerkleTree` 已有单测实现可复用）。
- 传输失败可重试、可断点续传，不损坏目标节点已有数据。

**完成后能扛住的故障**：节点可动态加入并恢复数据；为阶段 4 的自动 failover 提供"目标节点有完整数据"的保证。

---

## 阶段 4 — failover 选主 + fencing 端到端接线（L，高风险）

**目标**：owner 故障时，自动把 shard 切给一个**持有完整数据的 follower**，并用 fencing 防止旧 owner 复活后的脏写。

**当前问题**：failover 切给自己（`cluster.rs:271` 注释 "in production, pick best candidate"），切到空 shard；fencing epoch 从不在写路径被强制检查。

**改动**：
1. **选主改为选 follower**：`failover_shard` 的新 owner 从该 shard 的存活 follower 中选（它们有 quorum 数据），不是切给检测方自己。
2. **fencing 接进写路径**：owner 写入必须带当前 epoch 的 fencing token，follower 拒绝旧 epoch 的写（`replication.rs:7-37` 的 token 逻辑已有，需接进阶段 2 的写路径）。这防止被误判失败的旧 owner 复活后继续写。
3. **读流量切换**：failover 后 router 更新 ShardMap（`cluster.rs:284` 已做），确保后续读走新 owner。
4. **落后 follower 的处理**：若被选中的 follower 落后于 quorum 水位，先触发阶段 3 的 state transfer 追平再接管。

**改动范围**：`cluster.rs`/`zenoh_cluster.rs`（选主逻辑）、`control.rs`（failover）、写路径（fencing 强制）。风险高在于选主 + fencing 的边界条件。

**验收标准**：
- RF=3 集群杀掉 owner：**秒级内**自动 failover 到有数据的 follower，客户端写在短暂重试后恢复成功，**数据不丢**。
- 旧 owner 复活（网络分区恢复）：其携带旧 epoch 的写被 follower 拒绝（fencing 生效），不产生脏数据。
- 少数派分区侧：shard 保持只读（quorum 门控已有，`control.rs:113`），不 failover。

**完成后能扛住的故障**：**单节点故障自动恢复服务、数据不丢、无脑裂**——这是"生产 HA"的核心达成点。

---

## 阶段 5 — 故障注入端到端测试 + 混沌验证（L，中风险）

**目标**：用真实多节点 + 主动故障注入，证明前四个阶段真的成立。**没有这个，前面都只是"应该能工作"。**

**当前问题**：无一个测试真正杀主验证数据不丢（审计确认，`distributed_integration.rs` 全是孤立组件/手搭路由/mock handler）。

**改动**：
1. **真多节点测试框架**：起 3+ 个真实 `GraphService` + TCP，构成 RF=3 集群（不是 mock handler）。
2. **故障注入场景**（每个都是 pass/fail 断言，不是冒烟）：
   - 杀 owner → 数据不丢 + 自动 failover + 服务恢复。
   - 网络分区 → 少数派只读、多数派可写、无脑裂、恢复后数据一致。
   - ack 后立即杀节点 → 已 ack 的写不丢（验证同步复制语义）。
   - 慢副本 → quorum 仍达成，慢副本后续追平。
   - 滚动重启 → 服务不中断。
3. **数据一致性校验**：故障后用 Merkle/全量 checksum 比对各副本，逐条一致。
4. **纳入 CI**（可标 `#[ignore]` + 手动/夜间跑，因为耗时）。

**改动范围**：新增 `crates/nexora-zenoh/tests/ha_chaos.rs`（或独立测试 crate）。中风险在于测试本身的稳定性（分布式测试易 flaky）。

**验收标准**：
- 上述每个故障场景都有通过的自动化断言。
- 一致性校验：任意故障序列后，所有副本数据逐条一致。
- CI 可复现运行。

**完成后能扛住的故障**：以上全部经过验证——此时才可以**对外宣称生产级 HA**。

---

## 3. 里程碑与"每阶段能对外说什么"

对外沟通最容易出错的地方是**把中间阶段当成 HA**。下表钉死每个里程碑的诚实表述：

| 完成到 | 可以对外说 | 绝不能说 |
|---|---|---|
| 阶段 0 | "分布式为实验性，单机生产可用" | 任何"集群/HA/共识" |
| 阶段 1 | "数据平面已跨节点（水平分片）" | "容错"——RF 仍为 1，挂节点仍丢数据 |
| 阶段 2 | "写入同步复制到多副本，节点故障不丢数据" | "自动故障恢复"——挂 owner 后 shard 仍不可写 |
| 阶段 3 | "支持节点动态加入与数据恢复" | "自动 failover"——还没接选主 |
| 阶段 4 | "单节点故障自动恢复、数据不丢、防脑裂" | "已验证"——还没做混沌测试 |
| 阶段 5 | "生产级高可用（经故障注入验证）" | —— 此时才名副其实 |

**最小可用 HA = 阶段 1+2+3+4 全部完成。** 阶段 5 是"敢不敢对外宣称"的门槛。阶段 0 应立即做（安全）。

---

## 4. 工作量与风险总结

| 阶段 | 工作量 | 风险 | 关键风险点 |
|---|---|---|---|
| 0 诚实化 | S（1-2 天） | 低 | 无 |
| 1 写路径接 router | L（1-2 周） | 中 | 别改坏单机路径；AppState 重构面广 |
| 2 quorum 复制 | XL（3-6 周） | 高 | 分布式写正确性；quorum 失败语义；同步复制延迟 |
| 3 state transfer | XL（3-6 周） | 高 | 追赶期一致性；大 shard 传输背压 |
| 4 failover + fencing | L（1-2 周） | 高 | 选主边界条件；fencing 强制；分区恢复 |
| 5 混沌测试 | L（1-2 周） | 中 | 分布式测试 flaky |

**总计粗估：单人 3-4 个月**到"经验证的生产 HA"。阶段 2、3 是主体工作量与风险，各需一名熟悉分布式系统的工程师专注投入。

### 贯穿性注意事项

- **每阶段结束系统仍可发布**：单机路径始终不受影响（阶段 1 的 `Option<router>` 就是为此）；集群路径每阶段 degrade gracefully，不留半接线状态。
- **持久性延迟继承**：跨节点后写延迟 = max(本地 fsync ~4.3ms, 最慢 quorum 副本 RTT)。这是物理下限，见 [performance-tuning](../performance-tuning.md)。RF 越高、副本越远，尾延迟越高——这是 HA 的固有成本，需在阶段 2 的验收里量化。
- **不要跳阶段**：当前代码的根本错误就是有了 failover（阶段 4 的部分）却没有复制（阶段 2）和数据恢复（阶段 3），导致"切到空节点"。严格按依赖顺序。
- **复用已有正确部件**：见 §0 末尾清单——follower 应用逻辑、fencing token、quorum 门控、心跳检测都可直接用，不重写。

---

## 5. 立即可做 vs 需要立项

- **立即（本路线图外，低成本）**：阶段 0 全部——启动告警 + 文档诚实化。建议先做，消除误导风险。
- **需要立项**：阶段 1-5 是中大型工程（3-4 人月），涉及数据正确性，应作为独立里程碑排期，配专门的分布式系统评审，不宜夹在日常优化里推进。

---

*本路线图基于 2026-07-08 的源码审计。实施前应重新核对上述 file:line 是否仍有效（代码可能已变动）。*

