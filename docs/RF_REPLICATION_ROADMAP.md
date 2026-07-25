# Nexora 复制与高可用 - 生产就绪路线图（分层版）

**文档版本：** v2.1
**最后更新：** 2026-07-13
**维护者：** Nexora 分布式系统团队

---

## 0. 指导原则（v2 重构的核心）

本路线图 v2 相对 v1 的根本变化：**不再对所有数据一刀切做 RF=3 quorum 复制**。根据数据来源与耐久性分级，以及新鲜度的软性 SLA，工作被拆成**三条独立的线**，各自的一致性/持久性目标不同：

| 线 | 覆盖对象 | 持久性目标 | 一致性目标 | 手段 |
|---|---|---|---|---|
| **A. 控制平面** | 元数据：shard map、owner/epoch、MV 定义、SQ 定义、schema | **零丢失**（不可回灌，唯一真相源） | **强一致**（全集群单一视图、故障时唯一选主） | 持久化 + 进程内共识（openraft，编译进 nexora 二进制，无额外守护进程） |
| **B. 数据平面** | 用户图数据（第三方/IoT/Kafka/事件流） | 可接受较弱（**可回灌**） | best-effort 复制 | 异步复制 + **上游回灌恢复路径** |
| **C. 读路径** | 跨副本读一致性 | — | read-after-write，**有界 staleness** | majority 读 + session 追赶 |

**两条被反复引用的设计约束（来自需求方，2026-07-13）：**
1. 摄入的图数据主要来自可回灌的上游（Kafka/事件流/日志），丢了能重放；**但 nexora 自建的元数据/schema 不可回灌，必须零丢失。**
2. 新鲜度：原则上 P99 写完立刻在所有副本可见；实际高压下偶尔晚几秒可接受（软性 SLA）。

**据此得出的两条铁律：**
- **控制平面走共识，数据平面绝不碰共识。** 把用户数据塞进 Raft 日志会摧毁吞吐且毫无必要（它可回灌）。共识只用于小体量、低频写、必须互斥的元数据。
- **持久性 ≠ 一致性。** "元数据不丢"的 80% 靠单节点磁盘 fsync 就能解决，与共识无关；只有"多节点一致 + 故障选主"才需要共识。这两件事在下方分别排期。

---

## 现状核实（2026-07-13，读代码确认）

**关键矛盾：需求方要求"不可丢失"的元数据，恰是当前持久化最弱的部分。**

| 元数据 | 定义位置 | 是否持久化 | 是否跨集群一致 |
|---|---|---|---|
| Schema / catalog | 无统一子系统；`nexora-pgwire/src/catalog.rs` 仅 session 参数 shim | **否** | 否（per-session） |
| MV 定义 | `nexora-core/src/materialized_view.rs` | **是**——独立 node-local RocksDB（`rocksdb_path/materialized_views`） | **否**（`nexora-zenoh` 中无 MV 引用） |
| SQ 定义 | `nexora-standing-query/src/lib.rs`；codec 在 `persist.rs` | **实际否**——persist/restore 仅测试调用，`main.rs` 启动从不 restore → 重启即丢 | 否 |
| Shard map | `nexora-zenoh/src/shard_map.rs`；owner 副本 `control.rs` | **否**——重启时重建 + 重新 quorum 协商 | **是**——唯一走复制的元数据 |

**结论：** 约束 1 目前处于**被违反**状态（SQ 定义重启即丢、MV 定义单节点本地、shard map 不落盘）。这直接决定了下面阶段 0 必须置于一切之前。

**已有的共识雏形（v1 遗产）：** `ControlPlane::propose_shard_map_update`（`control.rs:128-158`）已实现 quorum_healthy 门控 + 单调 version + epoch fence + Zenoh gossip 传播——这已经是一个**手写的、不完整的共识协议**（约覆盖 Raft 的 60%）。未覆盖的部分（分区 + failover 交叉时的脑裂）正是手写共识最危险处。线 A 的目标之一就是用成熟共识实现替换掉它，消除这个正确性风险。

---

# 线 A：控制平面（元数据零丢失 + 强一致）

**最高优先级。** 这是唯一真相源，不可回灌。

## 阶段 A0：单节点持久化止血（跟共识无关，最低成本）✅ 已完成（2026-07-13）

**状态：** 三项全部交付并测试通过。(1) SQ persist/restore 接线，`main.rs` 启动 restore、定义变更写穿，顺带修了 codec 丢 `version`/`metadata` 的既有 bug；(2) MV manager 原来打开 RocksDB 却从不读回（盘上数据成孤儿）——加了启动加载 + 定义写 fsync；(3) shard map 新增 `ShardMapStore`（fsync + 原子 rename），`ControlPlane` 五个 commit 点写穿、`ClusterManager::new` 冷启动优先加载快照——修复了运行时 failover 的 owner+epoch 重启后丢失、reset epoch 无法 fence 旧 owner 的正确性缺口。诚实边界：持久化 opt-in，`--no-rocksdb`/无 `shard_map_dir` 时退回内存态、跨进程 restore 为 no-op。

**目标：** 让"元数据不丢（单节点）"立刻成立。不引入任何共识库。

**任务：**
1. **接线 SQ persist/restore**——代码已存在（`nexora-standing-query/src/persist.rs` 的 `persist_state`/`restore_state`），只是 `main.rs:478` 从不调用。启动时 restore、定义变更时 persist。
2. **确保 MV 定义持久化路径在生产开启**——`--no-rocksdb` 下 MV 定义纯内存，需明确文档化/告警；默认路径已落 RocksDB，核对 fsync 语义。
3. **shard map 落盘**——当前重启完全靠重建 + 重新 quorum。至少落一份本地快照，支持冷启动恢复与审计。

**验收标准：**
- 单节点重启后，SQ / MV 定义完整恢复。
- shard map 有磁盘快照，可离线检视。

**关键文件：** `crates/nexora-standing-query/src/persist.rs`、`crates/nexora-app/src/main.rs`、`crates/nexora-core/src/materialized_view.rs`、`crates/nexora-zenoh/src/shard_map.rs`

**预估：** 数天。

---

## 阶段 A1：统一元数据存储（收敛四处散落的定义）

**目标：** shard map + MV 定义 + SQ 定义 + schema 目前散在四个 crate、各管各的持久化。收敛到一个统一的控制平面存储抽象，为 A2 的共识接入做准备。

**状态：✅ 已完成（2026-07-13）。** 全部测试通过、全工作区构建通过、旁路检查干净。

**任务（已交付）：**
1. ✅ 定义 `ControlPlaneStore` trait，**放在 `nexora-core`**（不是原计划的 `nexora-zenoh`）——因为它是三个元数据 owner（core/standing-query/zenoh）的共同下游，放 zenoh 会成循环依赖。byte-oriented namespaced KV（`put/get/delete/list/snapshot/restore`），4 个 `Namespace`（ShardMap/MvDef/MvData/SqState），default 方法提供 snapshot/restore（为 A2 consensus 快照预留）。提供 `InMemoryControlPlaneStore` + `RocksDbControlPlaneStore`（写 fsync）。
2. ✅ 三类元数据全部迁移到该抽象后面：shard map（`ShardMapStore` 改为薄封装）、MV 定义+行数据（`MaterializedViewManager` 的 `db` 字段换成 `store`）、SQ 定义（`persist.rs` 改用 store，manager `set_store`）。schema 无独立子系统，不涉及。
3. ✅ main.rs 构建**单一** `control_plane_store` 共享给 MV（`with_store`）和 SQ（`set_store`）。

**验收标准（已达成）：**
- ✅ 所有元数据读写走单一 `ControlPlaneStore`，旁路检查确认三处不再直接碰 RocksDB/自管文件/图持久化层。
- ✅ 重启存活测试全过（shard map failover epoch、MV 定义+行、SQ 定义+version/metadata）。

**关键文件（实际）：** 新建 `crates/nexora-core/src/control_plane_store.rs`；改造 `nexora-zenoh/src/{shard_map_store,control}.rs`、`nexora-core/src/materialized_view.rs`、`nexora-standing-query/src/{persist,lib}.rs`、`nexora-app/src/main.rs`。

---

## 阶段 A2：控制平面共识（进程内 openraft，无额外守护进程）

**目标：** 用成熟共识替换手写的 `propose_shard_map_update`，让全部元数据获得强一致 + 故障唯一选主。

**这是本路线图唯一需要"引入共识"的地方，且仅覆盖控制平面。**

### 方案已定：进程内嵌入 openraft

**约束（需求方 2026-07-13）：** 不希望增加额外启动程序，希望共识在编译时融合进 nexora 二进制，简化运维架构复杂性。

**这条约束直接锁定方案。** 共识实现分两类：

| 类型 | 代表 | 是否满足"无额外进程" |
|---|---|---|
| **库（编译进 nexora）** | openraft、raft-rs | ✅ 是——`cargo add` 后共识作为 task 跑在 nexora 进程内 |
| **守护进程（独立部署）** | etcd、Consul | ❌ 否——必须单独启动/运维一个集群 |

**因此 etcd 被约束直接排除**（它是独立 Go 守护进程，违反"无额外启动程序"）。同类库中，**openraft 相对 raft-rs 是更贴合的选择**：raft-rs 只给算法内核，tick/ready/存储/传输全自建；openraft 是更完整的 async Rust 框架，且能直接复用 nexora 已有的传输与存储。**方案定为 openraft 进程内嵌入。**

### 进程内嵌入的三块适配（全部复用现有基础设施）

openraft 要求实现三个 trait，nexora 每一块都能复用已有代码，几乎不引入新的运维面：

1. **`RaftNetwork` — 复用现有 TCP 传输。**
   - nexora 已有 `TcpRemoteClient` / `TcpGraphServer`（`crates/nexora-zenoh/`）。Raft 消息（AppendEntries/Vote/InstallSnapshot）搭现有 TCP 通道走，**不新开端口类型、不引第二套协议栈**。
   - 实现要点：把 openraft 的 RPC 序列化后经现有 client 发到对端节点的一个新 message 变体。

2. **`RaftLogStorage` + `RaftStateMachine` — 复用现有 RocksDB。**
   - MV 已在用 rocksdb（`materialized_view.rs`）。Raft 日志、投票记录、状态机快照落到**同一个 rocksdb 依赖**，不引入新存储引擎。
   - **状态机 = A1 定义的统一元数据**（shard map + MV 定义 + SQ 定义 + schema）。每条 Raft log entry 是一次元数据 DDL（put/delete），apply 时写进 `ControlPlaneStore`。
   - 快照 = `ControlPlaneStore::snapshot()`（A1 已预留该接口）。

3. **共识组成员 = nexora 节点本身，co-located，零新增进程。**
   - Raft 组的投票者**不是新进程**，而是已经在跑的 nexora 节点。同一批进程**既是数据节点、又是控制平面投票者**。
   - 对运维而言，部署拓扑与无共识时**完全一样**：仍然只有"N 个 nexora 进程"一种东西要启动。

### 投票者拓扑（已定：自适应，2026-07-14 用户拍板）

**规则：**
- **节点数 ≤5**：全部节点当投票者。小集群简单，容错足够（3→容忍1，5→容忍2）。
- **节点数 >5**：固定 **5 个**投票者，其余节点作 openraft **learner**（只接收日志复制、不参与投票/选举），避免共识组过大拖慢选举。
- 投票者集合由启动时按稳定顺序（节点 id 排序）自动选前 N 个；可由 `cluster.yaml` 的 `cluster.voters` 显式覆盖（阶段 D）。
- 这是 etcd / CockroachDB system ranges 的主流做法——控制平面共识组保持小而固定，数据节点按需扩展。

**加载时必须校验（fail-fast）：**
- 投票者数为**奇数**（3 或 5）；偶数不提升容错还增加脑裂面。
- `cluster.voters` 显式集合必须是集群成员的子集（`{node.id} ∪ peers[].node_id`），否则拒绝启动——配错一个 typo 的投票者 id 会让 quorum 静默少一票、容错归零。

**实现映射（A2）：** 投票者 → openraft voter；非投票数据节点 → `add_learner`（收日志不投票）。冷启动 bootstrap：第一个节点起单节点组，其余先 `add_learner` 追上日志、再 `change_membership` 提升到目标投票者集。

### 进程内嵌入的诚实代价（设计时须考虑）

- **控制平面可用性与数据节点生命周期耦合。** 投票者就是 nexora 进程，需保证多数投票者存活，否则元数据平面（选主、shard map 变更、MV/SQ DDL）停摆。好在数据平面 best-effort + 可回灌，普通数据读写不必被拖住——但要在设计时明确"哪些操作依赖控制平面多数派"。
- **单二进制更重。** 共识 + 状态机 + 快照都在一个进程内，内存足迹与调试复杂度上升。这是换"运维简单"付的价，通常划算。
- **成员变更要自己编排。** openraft 提供 joint consensus，但"加节点/换节点/踢节点"的编排逻辑需自实现，不像 etcd 有现成 CLI。

### 明确排除的其它方案
- **etcd / Consul**：独立守护进程，违反"无额外启动程序"约束（见上表）。
- **raft-rs（tikv）**：同为进程内库，但只给算法内核，工作量远大于 openraft，无收益。
- **CRDT / gossip（LWW）**：合并语义给不了"互斥"，**无法保证全集群单一 owner**，对 shard map/选主是错的工具（它是线 B 用户数据异步复制的合适工具，不是这里）。
- **Multi-Paxos / VR**：同等保证但更难实现，无实际优势。

### Failover 流程（共识落地后）
1. 心跳/lease 检测 owner 故障（timeout 阈值，如 3s）。
2. openraft 组从 followers 选出复制最完整者为新 owner。
3. 递增 epoch，更新 shard map（作为一次 Raft log entry 提交），广播到所有数据节点。
4. 旧 owner 恢复后，其陈旧写因 epoch fence 被拒绝。

**验收标准：**
- 模拟 owner 进程 kill，30s 内自动切换。
- 旧 owner 重启后陈旧写被 epoch 拒绝。
- **分区注入下无脑裂**（同一 shard 不会出现两个 owner）——这是替换手写共识要买到的核心保证。
- 元数据变更在多数派确认后才可见，少数派分区侧拒绝写。
- **部署拓扑无新增进程**：三节点集群仅启动 3 个 nexora 进程，无独立共识守护进程。

**关键文件：** `crates/nexora-raft/`（openraft 适配：`RaftNetwork` 复用 `TcpRemoteClient`、`RaftLogStorage`/`RaftStateMachine` 复用 rocksdb + `ControlPlaneStore`）；`health_monitor.rs`、`lease.rs`、`failover.rs`

**预估：** 2-3 周。

---

# 线 B：数据平面（用户图数据：best-effort 复制 + 回灌恢复）

**用户数据可回灌，不走共识、不追求零丢失。核心是保证"故障后能恢复"，而非"故障时不丢"。**

## 阶段 B1：上游回灌恢复路径（v1 完全缺失的关键项）

**目标：** 明确的故障恢复语义——数据丢了能从上游重放灌回。

**任务：**
1. **记录消费位点**：Kafka offset / 事件流游标 持久化，与写入进度关联。
2. **回灌协议**：节点/shard 丢数据后，从记录的位点重放上游，恢复到一致点。
3. **幂等写入**：重放必须幂等（按 `__qid`/业务主键去重），避免回灌产生重复。
4. 全量快照 fallback（位点太旧、上游已过期时）。

**验收标准：**
- 杀掉一个 owner、丢弃其本地数据后，从上游回灌能恢复该 shard。
- 回灌不产生重复节点/边。

**关键文件：** 新建回灌协调器；`nexora-app` 摄入路径；`nexora-zenoh` 写路径的幂等键。

---

## 阶段 B2：best-effort 复制加固（保持现有语义，补齐可见性）

**目标：** 现有 RF>1 复制维持"尽力而为"（失败不阻塞写），但让降级**可见**、可监控。

**现状（v1 遗产，已核实）：** PG-wire 写路径 `execute_write`/`execute_create`/`router.route` 只写 owner、完全不碰 `.replicas`；`quorum_write` 只在 zenoh 单测/smoke 调用。`ReplicaWriter::quorum_write`（发 `FencedWrite` 给 follower 作为该 shard 副本应用）尚未接入 PG-wire 写路径。

**任务：**
1. 把 `ReplicaWriter::quorum_write` 接进 PG-wire 写路径（作为 best-effort，非阻塞）。
2. 复制失败**大声记录 + 暴露 metric**（复制滞后、失败率），而非静默。
3. 复制滞后进入监控，供线 C 的读路径判断 staleness。

**验收标准：**
- RF=3 写入后，followers 最终收到副本（异步）。
- 复制失败可观测，不假装成功。

**说明：** 这里**不做** quorum 写严格语义（不因 follower 不可达而拒绝用户数据写）——因为用户数据可回灌，宁可 best-effort + 回灌兜底，不为它付同步复制的延迟代价。严格 quorum 只留给线 A 的元数据。

**关键文件：** `crates/nexora-zenoh/src/replica_writer.rs`、`distributed_query_with_replication.rs`、PG-wire `simple_query.rs`

---

# 线 C：读路径一致性（P99 立即可见，压力下秒级容忍）

**目标：** 满足新鲜度约束——read-after-write 是硬需求，但可接受有界 staleness，不追求每次读的严格 linearizable 开销。

## 阶段 C1：session 级 read-after-write

**任务：**
1. 写入返回 `(epoch, seq)` 版本号。
2. 客户端 session 记录 `last_written_seq`。
3. 后续读等副本 `replica_seq >= last_written_seq` 才返回（session stickiness）。

**验收标准：** 客户端写后立即读，必定读到自己的写入。

## 阶段 C2：majority 读（有界 staleness）

**任务：**
1. `ReadConcern` 枚举：`Local`（任意副本，可能陈旧）/ `Majority`（多数派中取最新）/ `Linearizable`（owner，保证最新，最贵）。
2. `read_with_failover` 支持 read concern，默认 `Majority`。
3. 按写入版本比较，返回最新。

**验收标准：**
- Majority read 下，N/2 副本故障仍可读。
- 高压下允许读到秒级陈旧数据（符合软性 SLA），但不违反已建立的 session read-after-write。

**关键文件：** `crates/nexora-zenoh/src/failover.rs`、新建 `version_vector.rs`、PG-wire `session.rs`

---

# 阶段 D：黑盒集群验收（三条线交汇）

**目标：** 真实环境端到端验证。

**任务：**
1. 多机部署（3 台 VM），真实 CLI 启动（非测试代码）。
2. 配置文件驱动（`cluster.yaml`：拓扑 + RF + write_concern + 共识后端选择）。
3. 负载测试：1000 并发、10k QPS 混合读写。
4. 故障注入：kill owner、网络分区（iptables）、慢副本（tc qdisc）、**丢数据后回灌**。
5. 验收清单：
   - ✅ 元数据零丢失（线 A）：kill 任意节点，MV/SQ/shard map 定义不丢、全集群一致。
   - ✅ 分区下控制平面无脑裂（线 A）。
   - ✅ 用户数据丢失后可从上游回灌恢复（线 B）。
   - ✅ read-after-write + majority 读（线 C）。
   - ✅ Owner failover < 30s。
   - ✅ PG-wire CRUD、SQ 触发 webhook、MV 增量更新全链路。

**关键文件：** 新建 `tests/cluster_e2e/`、`deploy/cluster.yaml`、`deploy/chaos_test.sh`

---

## 时间线总结

| 线 | 阶段 | 时间 | 里程碑 |
|---|---|---|---|
| A | A0 单节点持久化止血 ✅ **已完成** | 数天 | 元数据不丢（单节点）立刻成立 |
| A | A1 统一元数据存储 ✅ **已完成** | 0.5-1 周 | 四处散落收敛为单一入口 |
| A | A2 控制平面共识（进程内 openraft） | 2-3 周 | 元数据强一致 + 唯一选主、消除脑裂、无额外进程 |
| B | B1 上游回灌恢复 | 1-1.5 周 | 用户数据故障可恢复 |
| B | B2 best-effort 复制加固 | 0.5-1 周 | 复制可见、可监控 |
| C | C1 read-after-write | 0.5 周 | 写后立即可见 |
| C | C2 majority 读 | 1 周 | 有界 staleness 读一致性 |
| D | 黑盒验收 | 1 周 | 生产就绪认证 |

**并行策略：**
- ~~**A0 必须最先做**（止血，解除约束 1 被违反的状态）。~~ ✅ 已完成。
- ~~A1→A2 串行~~；A1 ✅ 已完成，**A2 现在可以开始**。线 B、线 C 可与 A2 并行（不同人）。
- D 在三条线收敛后。

**关键路径：** ~~A0 → A1~~ → **A2（控制平面共识，进程内 openraft）** 是剩余最长链，约 2-3 周。A0+A1 已交付，剩余全部完成约 3-5 周。

---

## 当前可对外宣称的能力

✅ **已就绪：**
- PG-wire 分布式 SQL 全链路（INSERT/SELECT/UPDATE/DELETE/SQ/MV）。
- Shard map 走 quorum 门控 + epoch fence（手写，待 A2 用成熟共识替换）。
- **元数据单节点零丢失（A0 ✅ + A1 ✅ 已交付）**：SQ 定义、MV 定义+行、shard map 的 owner/epoch 均跨重启恢复；三者收敛到统一的 `ControlPlaneStore`（`nexora-core`），一个 durable 后端、一套 fsync 策略、一条 restore 路径，无旁路直写。

⚠️ **明确限制（v2 诚实边界）：**
- **元数据零丢失仅限单节点**（A0/A1 是持久性，非跨集群一致性）——多节点一致 + 故障唯一选主要等 A2 openraft。
- 用户数据复制为 best-effort，**回灌恢复路径尚未实现**（B1 前故障可能真丢数据且无法恢复）。
- 读路径不保证 read-after-write（C1 前）。
- 手写控制平面共识**未在分区注入下验证无脑裂**（A2 前为正确性风险）。

---

## 附录：v1 → v2 变更说明

v1（2026-07-12）把"用户数据 RF=3 quorum 写入"列为阶段 1 最高优先级。v2 依据需求方 2026-07-13 明确的数据分级与新鲜度约束**重排**：

- 用户数据可回灌 → 降级为 best-effort + 回灌兜底（线 B），**不再是第一优先级**。
- 不可回灌的元数据 → 提升为最高优先级（线 A），且拆分"持久化止血（A0，无关共识）"与"共识（A2）"。
- 读一致性从 v1 的阶段 4 提前为独立的线 C，按软性 SLA 设计（majority + session，非严格 linearizable）。
- A2 共识方案定为**进程内 openraft**：依据需求方"不希望额外启动程序、编译时融合"的运维约束，排除 etcd/Consul 等独立守护进程方案（详见 A2 的方案选型表）。
