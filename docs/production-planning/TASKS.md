# 分布式演进 — 后续推进任务清单

**状态**: P0/P1/P2/P3 全部完成；工作线 B（分布式 Cypher）已扩到——AST 规划器支持扫描 / count·sum·avg·min·max / GROUP BY / 全局 ORDER BY·SKIP·LIMIT·DISTINCT / 单跳·多跳·变长·无向·无类型跨分区关系遍历 / 关系与路径 join 之上的聚合·分组·排序 / MATCH→WITH→RETURN 两阶段管线（节点扫描与关系模式两种 stage-1，含 HAVING 过滤）/ UNION·UNION ALL / MATCH-based SET·REMOVE·DELETE 分布式写 / 节点 CREATE 分布式写（协调端预生成 qid 按 owner 路由），其余（MERGE 分布式 / 多阶段 WITH 链）仍诚实 501 | **生成日期**: 2026-07-08（CREATE 增补 2026-07-09）

> 依据 [DISTRIBUTED_EVOLUTION.md](DISTRIBUTED_EVOLUTION.md) 的四条工作线与 [HA_ROADMAP.md](HA_ROADMAP.md) 的阶段划分。
> 阶段 1（共享地基）拆到 file:line 级；其余按里程碑给验收标准。file:line 基于 2026-07-08 源码，实施前请重新核对。

---

## 优先级总览

| # | 任务组 | 依赖 | 量级 | 状态 |
|---|---|---|---|---|
| P0 | 阶段 1：写路径接 router | — | L | ✅ 完成 |
| P1 | HA 阶段 2：quorum 复制 | P0 | XL | ✅ 完成 |
| P1 | 分布式计算：scatter-gather 接执行器 | P0 | L-XL | ✅ 完成（遍历下推） |
| P2 | HA 阶段 3-5（failover+恢复+混沌） | P1 | XL | ✅ 完成 |
| P2 | 分层存储：图数据下沉 | P0 | 中大 | ✅ 完成 |
| P3 | 分片再平衡 + 动态成员 | P0 | L-XL | ✅ 完成 |
| P3 | 冷层 OLAP 列存下推 | P2-B | 大 | ✅ 完成 |

**起点 P0 已完成，解锁的三条线（容错 / 分布式计算 / 分层存储）与 P3 运维成熟度全部落地并端到端验证。冷层 OLAP 列存下推亦已完成（自研列式格式 + 谓词/投影下推，与本仓库零外部依赖的做法一致）。分布式 Cypher 读的规划器已扩到覆盖：AST 分析构建 `DistributedPlan`，支持 label/node 扫描（concat）、全局与分组聚合（count/sum/avg/min/max，avg 用 sum+count 部分聚合合并）、协调端全局 ORDER BY/SKIP/LIMIT/DISTINCT；跨分区关系遍历从单跳有向扩到多跳定长链、变长路径（`*min..max`，深度上限 8、带环安全 BFS）、无向（`-[:R]-`）与无类型（`-[]->`）边（`path.rs` 统一 hop 模型）；关系与路径 join 之上的聚合/分组/排序（`JoinPost`：join 抽出裸列 → 协调端本地单遍聚合 + order/skip/limit/distinct）；MATCH→WITH→RETURN 两阶段管线（`WithStage`：stage-1 分布式跑 scan/聚合**或关系 join** → 协调端物化 → 应用 WITH WHERE（HAVING）+ 最终 RETURN 投影/排序/窗口；`execute_inner` 三种 stage-1 源（rel_join/path_join/scan）统一汇入 with_stage 后处理）；UNION / UNION ALL（`UnionPlan`：各分支独立分布式执行，顶层 concat，UNION 再全行去重；分支 arity 须一致，任一分支不可分布则整体拒）；MATCH-based 分布式写（`WritePlan`：`MATCH (n[:L]) SET|REMOVE|DELETE` 每 owner 各跑本地写、只动自己的匹配节点，协调端 SUM 8 项 WriteResult 统计——owner-parallel 无重叠。附带修掉单机写执行器一个真实 bug：`execute_write` 之前每变量只绑一个节点，bulk 更新/删除会漏，新增 `try_bulk_match_mutate` 对所有匹配节点应用）；以及分布式 CREATE（`CreatePlan`：协调端预生成 qid → 按 `shard_key()%total` 分组到目标 owner → 各 owner 用注入的 `__qid` 就地创建，物理落位正确——`execute_create` fan-out 后 SUM 统计）。仍在诚实 501 之外的：MERGE 分布式（需两阶段 MATCH+CREATE）、三段及以上的多阶段 WITH 链——这些是后续增量，非正确性缺口。**

---

## P0 — 阶段 1：写路径接入 router（最高优先级）

**目标**：数据平面真正跨节点。单机路径行为逐字节不变（用 `Option<router>` 保证）。

**前置事实**（已核实）：
- `AppState` 无 router 字段（`handlers.rs:93`）。
- `HybridRouter::route(qid, GraphOperation) -> GraphResult` 已存在（`router.rs:58`）。
- `ClusterManager::router()` 已暴露（`cluster.rs:293`）。
- `HybridRouter::new_local` / `new_clustered` 已存在（`router.rs:23/33`）。
- **写入口有两类**：直接 API（`set_property` `handlers.rs:552`、`add_edge` `:656`、批量 `:1965/:1981`）**和** Cypher/SQL（`execute_cypher(&state.graph, ...)` `:367/:1462`、`execute_sql` `:2235`）。**两类都绕过 router，都要处理。**

### 任务（✅ 全部完成 2026-07-08）

- [x] **T1.1** `AppState` 增加 `pub router: Option<Arc<HybridRouter>>` 字段。单机模式为 `None`，构造点默认 `None`（零行为变化）。
- [x] **T1.2** main.rs 装配：`app_router = cluster_manager.as_ref().map(|cm| cm.router_arc())`——单机 `None`，`--cluster` 从新增的 `ClusterManager::router_arc()` 注入。
- [x] **T1.3** 抽象实现为 `AppState::try_route_remote(qid, op) -> Option<Result<..>>`：`None` = 本地（调用方走原 `state.graph` 路径，**一字不动**）；`Some` = 已远程路由。**关键设计修正**：不用 router 的本地通道（`new_clustered` 里那是死端），本地一律走 `state.graph`，避免数据分裂。
- [x] **T1.4** 直接写入口接入：`set_property`、`add_edge`、批量属性 + 批量边（4 处）。本地路径全部保留不变，仅前置远程分支。
- [x] **T1.5** 读入口接入：`get_property`、`get_edges`。
- [x] **T1.6** **Cypher/SQL 写路径** —— **决策修正为「显式拒绝」而非 mutation 路由**。理由：执行器在本地图快照上跑整条查询，多节点下按 qid 分解写 + scatter 读属于**工作线 B（分布式计算）**，在阶段 1 半做会造成静默数据错乱（写落错节点、读返回残缺）——正是本项目一路批判的反模式。故真多节点集群下 `execute_cypher`/`execute_sql`/`ws_query` 三处显式返回 501 + 指向 REST API，单机（含单节点集群）完全不受影响。新增 `AppState::whole_graph_query_is_safe()` + `HybridRouter::all_shards_local()` 做守卫。
- [x] **T1.7** `ShardMap::new_distributed(total, nodes, local)`——按成员 round-robin 确定性分配 owner；`ClusterManager::new` 改用它（self + peers）。4 个单测（分布/确定性/空回退）。
- [x] **T1.8** 错误处理：新增 `router_error_response`——节点不可达→503、超时→504、远程执行错误→502，带 error/kind/hint，不再误报本节点 500。

### 验收标准（✅ 全部达成）

- [x] **单机回归**：nexora-app/zenoh/core/cypher/pgwire 全套测试零失败，行为不变。
- [x] **跨节点端到端**：新增 `tests/two_node_real_graph.rs`——两个**真实 GraphService** + TCP（非 mock handler），验证向 node-A 写属于 node-B 的 key 真落在 node-B 的 graph 且可读回；反向 local-owned key 不泄漏到 node-B。
- [x] **Cypher 跨节点**：按 T1.6 决策，多节点下显式拒绝（不静默给错误答案）；单机路径回归通过。
- [x] `cargo build --workspace` 通过（下游 pgwire/zenoh 已覆盖）。

### 完成后能说什么

"数据平面已跨节点（水平分片）"。**不能说容错**——RF 仍为 1，挂节点仍丢那片数据。这是纯地基。

---

## P1 — 分叉：容错 与 分布式计算（P0 完成后，可并行两条独立线）

P0 完成后，以下两条线**互不依赖、可并行**（不同工程师）。按业务优先级选先做哪条。

### P1-A — HA 阶段 2：同步 quorum 复制（✅ 完成 2026-07-08）

详见 [HA_ROADMAP §阶段2](HA_ROADMAP.md)。

- [x] **T2.1** 副本集分配：新增 `ShardMap::new_distributed_rf`（owner + rf-1 个 clockwise 邻居为 follower，确定性、跨节点一致）；`ClusterManager` 从初始 map 用 `ReplicaWriter::with_replica_sets` 填充本节点所有 shard 的副本集；新增 `--replication-factor` CLI（默认 1，行为不变）。6 个单测（RF 分配/去重/clamp/确定性）。
- [x] **T2.2** 写路径接复制：`AppState::replicate_write(qid, op)`——owner 本地写成功后，经 `ReplicaWriter::quorum_write` 把完整 `GraphOperation` 复制到 followers，等多数派 ack。接进 `set_property`/`add_edge` 的本地 owner 成功路径。router 新增 `shard_and_epoch` 供构建 FencingToken。
- [x] **T2.3** ~~补 Raft 空 payload~~ —— **因架构决策不需要**。采用**协调者驱动的操作复制**（发送完整 GraphOperation），而非运送 WAL 字节，故 Raft 空 payload 路径不在复制链路上。前提：mutation 确定性（SetProperty/AddEdge 成立），已诚实标注。
- [x] **T2.4** quorum 失败语义：`replicate_write` 返回 `Failed` 时，handler 返回 **503 + `kind:"quorum_not_reached"` + "committed locally but not quorum-durable"**，不静默成功、不回滚（标记待协调，符合设计）。
- [x] **验收**：`two_node_real_graph.rs` 新增 2 个**真实 GraphService** 测试——`replicated_write_survives_owner_death`（RF=3 复制后杀 owner，数据仍在 follower 的真实 graph 可读）+ `quorum_fails_when_followers_unreachable`（follower 不可达 → 明确 Failed）。全工作区编译 + 五 crate 零失败。
- [x] **能说**：“写入同步复制到多副本，节点故障不丢数据”。**不能说自动恢复**（failover 是 P2-A）。

**遗留（明确标注，不是遗漏）**：批量 load-dataset handler 未接复制（多 key 跨 shard 部分成功语义复杂，留后续）；复制假设 mutation 确定性；owner 已写但 quorum 失败的“待协调”状态目前靠客户端重试，尚无后台协调器（需 failover 阶段配套）。

### P1-B — 分布式计算：scatter-gather 接进服务路径（✅ 首个能力落地 2026-07-08）

**范围决策**：不把整个 Cypher 执行器改成分布式规划器（那是完整工作线 B 的大工程，会牵动查询规划/聚合下推）。P1-B 的务实落点是**为 scatter-gather 原语提供一个真实的服务入口**——一个专用的分布式遍历端点，让"计算贴着数据跑"首次真正接入生产路径。Cypher/SQL 的复杂多节点查询仍按 T1.6 诚实拒绝，待完整规划器。

**修复过程中发现的两个前置 bug（本会让 P0/P1-A 在真实 `--cluster` 启动路径下失效）**：
- [x] **P1B-0 shard-map 冷启动覆盖 bug**：`start()` 的 `distribute_shards()` 用 alive_nodes=[self]（peer 未心跳）把 map 冲回全本地、清空 replicas。修复：ControlPlane 与 router 共享 `new()` 建立的同一张分布式 RF map（新增 `ControlPlane::with_shard_map`），移除冷启动 rebalance。回归测试 `test_new_distributes_shards_across_peers`。
- [x] **P1B-1 scatter-gather 本地 shard 超时**：router 的 `local_tx` 是死端（`new_clustered` 丢弃接收端），scatter-gather 对本地 shard 会超时。修复：router 改用 `new_clustered_no_local`，`start()` 注册自身到 remote_client，本地 shard 走 TCP-to-self。app 读写路径本地走 `state.graph`、对此透明。

- [x] **P1B-2 分布式遍历端点**：`POST /api/v2/graph/traverse`——集群模式经 `scatter_gather_traverse` 在各 shard owner 并行遍历+合并；单机模式回退等价本地 BFS。请求 `{start[], edge_type, max_depth}`，返回 `{reached[], mode}`。
- [x] **验收**：`scatter_gather_traverses_across_two_real_nodes`——两个**真实 GraphService**，2 跳链跨越 node-a/node-b 边界，scatter-gather 正确发现两跳；单机 BFS 路径回归通过；全工作区编译 + 五 crate 零失败。
- [x] **能说**：“分布式图遍历执行（计算下推到 shard owner）”。与 HA 正交，不涉及复制。

**遗留（明确标注）**：仅遍历型查询下推；Cypher/SQL 通用查询的分布式规划（MATCH+聚合下推、部分结果归并）仍是完整工作线 B，未做——T1.6 的拒绝仍生效。

---

### V — 真实集群验证台（✅ 完成 2026-07-08，P2 前置）

**动机**：P0/P1-A/P1-B 的端到端测试都**直接构造 `HybridRouter`、绕过 `ClusterManager::start()`**，而 P1B-0/P1B-1 两个 bug 恰恰藏在 `start()` 路径里（组件测试全绿却带病）。P2-A（failover）是数据正确性最敏感的一层，必须建在"真实启动路径已验证"的地基上。

- [x] **V1** P0 跨节点写读经真实 `start()` 路径：node-0 路由属于 node-1 的 key，数据物理落在 node-1 的真实 graph 且可读回。
- [x] **V2** P1-A quorum 复制经真实集群：RF=3、3 个 started 节点，owner 写复制后数据在两个 follower 的真实 graph 上。
- [x] **V3** P1-B scatter-gather 遍历经真实集群：2 跳链跨越 node-0/node-1，遍历正确发现两跳。
- [x] **测试台设计**：`tests/real_cluster_smoke.rs`——固定端口预分配（解 peer 互认的先有鸡蛋问题）、`GraphServiceAdapter` 包真实 GraphService 当 handler、每节点经完整 `start()`（绑 TCP server + 心跳 + 失败检测）。这是 P2-A 混沌测试的脚手架基础。

**成果**：三条已完成工作线从"组件级验证"升级为"系统级验证"，之前藏 bug 的启动路径缝隙被守住。P2-A 现在可建在验证过的地基上。

---

## P2 — 完成容错闭环 与 分层存储

### P2-A — HA 阶段 3-5（XL，依赖 P1-A）

详见 [HA_ROADMAP](HA_ROADMAP.md) 阶段 3/4/5。

> **file:line 校订（2026-07-08 复核）**：`StateTransferClient` 定义在 **`nexora-raft/src/state_transfer.rs:166`**（不在 nexora-zenoh），仅有 trait + `StateTransferManager`，**无生产实现、未接进 failover**。`failover_shard_auto`（`nexora-zenoh/src/control.rs:182`）**已完成"提升存活副本"逻辑**——不再切给自己，无副本时标记 shard 非可写；失败检测器已接（`cluster.rs:335`）。故阶段4 的"选主"部分已完成，剩下 fencing 写路径校验（`replica_writer.rs:96` 当前是恒真空转）。

- [x] **阶段4-a（fencing，✅ 完成 2026-07-08）** 真实 per-shard epoch 校验落地。旧的 `token.allows_write(token.epoch)` 是恒真空转（拿 token 的 epoch 跟自己比）。改法：新增 `GraphOperation::FencedWrite { shard_id, epoch, inner }`——`quorum_write` 把发给 follower 的写包一层带 owner epoch；接收端（`GraphServiceAdapter`）用共享的 `ShardFence`（`fencing.rs`）做准入：epoch 低于本 shard 已见最大值即拒。`ShardFence` 双源推进：failover 改 map 时 `observe_map`（权威）+ 每次写自愈式记录。接进 `ClusterManager`（`fence()` 与 handler 共享，失败检测器 failover 后先推进 fence 再更新 router）。**验收**：`two_node_real_graph.rs` 两个真实 GraphService 测试——`stale_epoch_write_is_fenced_on_real_follower`（epoch1 滞后写被拒、旧值不被覆盖）+ `current_and_newer_epoch_writes_are_admitted`；smoke `start_cluster` 全部接 fence。
- [x] **阶段4-b（选主，已完成）** failover 选主已改为选**有数据的 follower**（`control.rs:182` `failover_shard_auto`）；无存活副本时标记非可写而非切给空节点。
- [x] **阶段3（state transfer，✅ 完成 2026-07-08）** 采用**操作式**状态转移（与 T2.3 复制设计一致，运送 operation 而非 WAL 字节），不用 nexora-raft 那套 WAL-shipping trait。新增 `GraphOperation::ExportShard { shard_id, total_shards }`——源节点从活图按 cluster-shard（`shard_key()%total`）导出全部 node+edge 为 `ShardSnapshot`（`graph_service_adapter::export_shard`，复用 migration 的 snapshot 类型 + `all_node_ids`）。`state_transfer::StateTransfer::catch_up_shard` 拉快照并按 SetProperty/AddEdge 回放进本地图。接进 `ClusterManager::catch_up_shard(source, shard)`（走 remote client 的 TCP-to-self apply）。**验收**：`two_node_real_graph.rs` 的 `state_transfer_recovers_shard_into_empty_node`（空节点拉回 3 node+1 edge，shard 范围外的 key 不泄漏）+ `state_transfer_fails_when_source_unreachable`（源不可达明确报错）；smoke 新增 **V4** `v4_state_transfer_catches_up_shard_via_started_cluster` 经真实 `start()` 路径验证。
- [x] **阶段5（混沌 + Merkle，✅ 完成 2026-07-08）** 新增 `tests/chaos_consistency.rs`，经真实 `start()` 路径注入故障，用 `nexora_raft::MerkleTree` 的 root hash 作一致性预言机（单 key 分歧即变根）：`chaos_kill_owner_failover_preserves_consistency`（RF=3 杀 owner→failover 提升存活副本、epoch 递增、两存活副本 Merkle 根相等且等于故障前状态=无丢失）、`chaos_restart_then_catch_up_restores_consistency`（节点重启丢内存图→catch-up 后 Merkle 根与源一致）、`chaos_merkle_oracle_detects_single_key_divergence`（预言机非空转自检）。**修复过程发现两个真实 bug**：(a) `ClusterManager::shutdown()` 不停后台心跳循环——被杀节点仍在广播存活、永不被 failover；改为持有 task 句柄并 abort。(b) **voter 集合漏了 self**——3 节点集群每个节点只见 2 voter，杀掉 owner 后存活 voter=1<多数 2，failover 被误拒为 NoQuorum；改为 voters=全体成员含自身。
- [x] **能说（全部完成后）**：“生产级高可用，经故障注入验证”——四条 P2-A 全绿：epoch fencing（阶段4-a）、存活副本选主（阶段4-b）、操作式状态转移（阶段3）、混沌+Merkle 验证（阶段5）。**遗留**见上（全量快照、无 catch-up/写并发屏障、failover 未自动触发 catch_up）——这些是增量优化，不影响"经故障注入验证"的核心成立。

- [x] **阶段4-c（failover 自动触发 catch-up，✅ 完成 2026-07-08）** 失败检测器在 `failover_shard_auto` 把某 shard 提升到**本节点**后，自动从另一个存活副本触发 catch-up（`control.rs` 新增 `owner_of_shard` 判断本节点是否被提升；选源排除自身与故障节点）。落后副本被提升后自动补齐所缺写入，无需人工干预。**验收**：`chaos_consistency.rs` 新增 `chaos_failover_auto_catch_up_reconciles_lagging_replica`——构造一个漏写的落后副本，杀 owner 后它被提升并自动补齐所缺 key，Merkle 根与最新副本一致。
- [x] **阶段3-incr（增量 state transfer，✅ 完成 2026-07-08）** 新增 per-shard 复制日志（`replication_log.rs`，`ShardReplicationLog`）：owner 在 `quorum_write` 给每条复制写分配单调 seq 并 stamp 进 `FencedWrite{seq}`；owner 与 follower 都记进有界环形缓冲（每 shard 4096 条）。新增 `GraphOperation::ExportDelta{from_seq}` + `DeltaResponse{UpToDate|TooOld|Delta}`。catch-up 改为**优先增量**（`StateTransfer::catch_up_incremental`）：按本节点 high-water 拉 delta，只回放落后的那几条 op；lag 超出日志窗口则回退全量快照。failover 自动 catch-up 已切到增量路径。**验收**：`state_transfer.rs` 3 个单测（增量只回放 delta / up-to-date 不回放 / too-old 回退快照）+ `replication_log.rs` 6 个单测（seq 单调/delta/窗口淘汰）；`chaos_failover_auto_catch_up_reconciles_lagging_replica` 改用真实复制路径造 lag（漏一条 seq），走增量补齐。

- [x] **阶段4-d（catch-up 写屏障，✅ 完成 2026-07-08）** 新增 `CatchUpBarrier`（`catch_up_barrier.rs`）：per-node 记录正在 catch-up 的 shard 集合。failover 自动 catch-up 用 `barrier.guard(shard, ...)` 包裹——**fence（begin，屏蔽写）→ catch-up → reopen（end）**，确保 client 写不与 reconcile 回放交错、不被 stale delta op 覆盖。app 写路径（`set_property`/`add_edge`）在本地 owner 分支前查 `write_blocked_by_catch_up`，命中则返 503 + `catch_up_in_progress`（可重试）。**验收**：`catch_up_barrier.rs` 3 单测（begin/end、guard 括住、独立 shard 不干扰）+ app `write_rejected_while_shard_reconciling`（屏障开启时写 503、解除后写 200）。

- [x] **阶段4-e（持久化复制日志，✅ 完成 2026-07-08）** 复制日志此前是**内存环形缓冲**，进程重启即丢，重启节点被迫走全量快照。`ShardReplicationLog` 现内置可选 RocksDB backend：`open_durable(path, max_entries)` 把每条 `(seq, op)` 以 `{shard}:{seq:016x}` → JSON 持久化（bincode 无法编码 `SetProperty` 里的 `serde_json::Value`，故用 JSON），启动时回放每 shard 保留窗口的尾部；写入时镜像内存环的淘汰、按 `high_water - max_entries` 剪枝 on-disk 旧条目。同步构造（回放 map 先建好再入锁）故可从 `ClusterManager::new`（非 async）调用。`ClusterConfig::replication_log_dir` 打开它（app 在 `--rocksdb-path/replog` 下、非 in-memory 模式默认启用），打开失败回退内存（durability best-effort，正确性不受影响）。**验收**：`replication_log.rs` 3 单测（跨重启 high_water+增量 delta 存活 / 跨重启旧条目剪枝且 lag 超窗回退 / 内存版对照无 durability）。至此 **P2-A 全部完成**：quorum 复制、epoch fencing、存活副本选主、操作式+增量 state transfer、自动 catch-up、写屏障、持久化复制日志、混沌+Merkle 验证。

### P2-B — 分层存储：图数据下沉（中大，依赖 P0）

详见 [DISTRIBUTED_EVOLUTION §5](DISTRIBUTED_EVOLUTION.md)：

> **现状校订（2026-07-08 复核）**：`nexora-storage` 早已具备完整机制——`LocalStorage`（durable SSD/hot）、`S3Storage` + `MockS3Storage`（S3 SigV4，warm/cold）、`TieredStore`（hot/warm/cold + `run_lifecycle` 按龄迁移 + promote/demote）；`nexora-fragment` 有 `FragmentStore`（时间分片元数据 + 时间范围路由）与 `time_travel`（历史重放）。**缺的是两者的桥接**（fragment body 从未进 tier）与真实 backend 装配。

- [x] **T4.1（✅ 完成 2026-07-08）** S3 模式下 hot 从 `MemoryStorage` 换成 **durable `LocalStorage`**（重启不丢热数据），cold 从 `MemoryStorage` 换成 **S3**（warm/cold 用不同 prefix 分区，同桶内独立命名空间）。`main.rs` S3 分支重装配。
- [x] **T4.2（✅ 完成 2026-07-08，核心缺口）** 新增 `TieredFragmentStore`（`fragment/tiered_store.rs`）桥接：每个 fragment 的 node 记录作为**单个对象**（key `fragments/<ns>/<id>.jsonl`）存入 `TieredStore`，故 `run_lifecycle` 可把超龄（冷）fragment 从 hot 下沉到 Warm(S3)/Cold。热路径仍 shared-nothing（写入落 hot 本地，只有超龄 fragment 迁到共享对象存储）。
- [x] **T4.3（✅ 完成 2026-07-08）** `TieredFragmentStore::read_range` 按 fragment 时间范围命中并从**任意层**（hot→warm→cold）拉回 body；`get_fragment`/`tier_of` 提供单 fragment 的跨层读与定位。
- [x] **验收（✅ 达成）** `fragment/tiered_store.rs` 的 `fragment_sinks_to_s3_warm_and_time_travels_back`——用 `MockS3Storage`（生产同形 S3 backend）作 warm 层：写入 fragment→超龄→下沉 S3（`tier_of==Warm`）→range 读从 S3 拉回→喂给 `execute_time_travel` 重放，历史值（speed=50）经 S3 往返后仍正确。另 3 个 bridge 单测（hot 读写/超龄迁移/range 拉取）。
- [x] **能说**：“热本地+冷共享的混合存算分离”——机制与 fragment↔tier 桥接已闭环并经 S3 往返验证。**热路径保持 shared-nothing**（写入落本地 hot，实时）。

- [x] **T4.4（graph→fragment 封存管线，✅ 完成 2026-07-08）** 新增 `FragmentSealer`（`fragment/sealer.rs`）：把活图 mutation 事件按时间窗折叠（同节点属性 last-writer-wins、边累加），窗口关闭时封成 `nodes.jsonl` fragment 存进 `TieredFragmentStore`。app 接线：mutation 回调经 unbounded channel 转发 `SealEvent`（非阻塞、不拖热路径），后台 drain 任务折叠、定时器（`--seal-interval-secs`，默认 300）封存并跑 tiered lifecycle。**修复前置 bug**：`GraphService::set_property` 此前只 fire `sq_callback` 不 fire `mutation_callback`（属性写对 mutation 流不可见），补上 `PropertySet` 事件。属性值用 `pv_to_json`（非 serde tagged 形）保证 time-travel 回放。**验收**：`sealer.rs` 4 单测（空窗口 no-op / 封存可 time-travel 查询 / 窗口推进 / 边封存回放）+ **真实 app 端到端手验**：`--storage-backend local --seal-interval-secs 2` 启动，PUT 两个属性 → 2s 后 fragment 落 hot 层磁盘、body 是 plain JSON、`storage/status` 报 1 hot object。
- [x] **能说（升级）**：“热本地+冷共享的混合存算分离，活图历史自动封存分层”——封存管线已把实时写入变成按时间窗的 fragment，经 lifecycle 沉到共享存储，全链路端到端验证。**热路径保持 shared-nothing**（mutation 回调只做非阻塞 channel send）。

**遗留（明确标注）**：fragment body 用 JSONL（非列存 Parquet，OLAP 下推是 P3 可选项）；sealer 窗口在内存（进程重启丢未封存的当前窗口——已落盘的 fragment 不受影响）。至此 **P2-B 全部完成**：真实 backend 装配、fragment↔tier 桥接、graph→fragment 封存管线、封存后 TTL 驱逐（热内存释放），全部端到端验证。

---

## P3 — 运维成熟度（完成）+ 增量优化（可选）

- [x] **分片再平衡（✅ 完成 2026-07-08）** `ControlPlane::rebalance_for_members`：按新成员集**确定性**重算 owner+follower（与 `new_distributed_rf` 一致，每节点算出同一张目标图），owner 变更处 bump epoch（fence 旧 owner），返回 `(new_map, moved[(shard, old, new)])` 供迁移。`ClusterManager::rebalance(members)`：对**迁到本节点**的 shard，在**写屏障**下从旧 owner 经增量 state transfer 拉数据，再发布新图（先推进 fence 再更新 router）——加节点不只是改图，数据随迁。**验收**：control 2 单测（scale-out 有 diff+epoch bump / 同成员 no-op）+ smoke **V5** `v5_rebalance_migrates_shard_data_to_new_owner`（经真实 `start()`：node-2 先降到 2 节点视图、在新 owner 写 key、再升回 3 节点 → node-2 迁回该 shard 数据并物理落在其真实 graph）。
- [x] **动态成员（✅ 完成 2026-07-08）** 心跳 gossip 不再被丢弃：`send_heartbeat` 返回 peer 的已知节点列表，心跳发送循环把新学到的 (node_id → graph/hb 地址) 写入 `node_addrs`、注册进 registry 并 `remote_client.register_node`（新节点可被路由/复制）。每个心跳 tick 比对当前 `node_addrs` 成员集与上次已知集，**变化即自动 `rebalance_impl`**（去抖：仅在成员集真正变化时触发），故加节点经 gossip 传播后自动分到 shard、数据随迁。`rebalance` 逻辑抽成自由函数 `rebalance_impl` 供手动入口与心跳循环共用。**验收**：smoke **V6** `v6_dynamic_membership_join_rebalances`——两个各自只知一个 seed peer 的节点经真实 `start()` 起来，靠 gossip 互相发现、成员集收敛，自动 rebalance 后新节点拥有 shard（经真实心跳路径，非手工塞 map）。
- [x] **封存后 TTL 驱逐（✅ 完成 2026-07-08）** `GraphShard::evict_idle_nodes(ttl)` + `GraphService::evict_idle_nodes(ttl)`：扫描所有 shard，对 `last_access` 超过 TTL 的节点调用 `sleep_node`（持久化完整 journal checkpoint、释放内存、下次访问时自动唤醒）。app 层 `--idle-evict-secs` 选项（默认 3600）驱动封存循环每个 tick 后调用驱逐扫描——封存的 fragment 是历史快照、活图的陈旧节点经 TTL 后 sleep 释放热内存，读时自动唤醒，兑现分层存储的"降本"承诺。**验收**：shard 单测 `test_evict_idle_nodes`（写-年龄-扫描-验证已 sleep）+ graph 端到端 `test_evict_idle_nodes_end_to_end`（写-驱逐-读唤醒-状态一致）。
- [x] **冷层查询下推（✅ 完成 2026-07-08）** 自研列式冷层格式 `ColumnarFragment`（`fragment/columnar.rs`）——把 fragment 的 node 记录按列转置（每列一个 `Vec<Option<Value>>`），带每列 min/max + null 统计。`Predicate{column, op, value}` 支持 Eq/Ne/Lt/Le/Gt/Ge。**双层下推**：(1) fragment 级——列统计能证明无行匹配则整段跳过、完全不解码；(2) 行级——只读谓词列判断、投影只解码所选列。round-trips 回 `nodes.jsonl` 行记录，故列存归档仍可经 time-travel 重放。与本仓库零外部依赖做法一致（JSON envelope，不引入 Arrow/Parquet 依赖树）。`TieredFragmentStore::put_fragment_columnar` + `scan_range` 把它接进分层存储：冷 fragment 以 `.col` key 存入 tier、跨层（含 S3）拉回扫描。**验收**：`columnar.rs` 8 单测（round-trip / 序列化 / 统计 / fragment 跳过 / 行过滤+投影 / 未知列 / Eq / null 语义）+ `tiered_store.rs` `columnar_olap_scan_pushes_down_across_s3`（两个 fragment 列存下沉 S3，range+谓词+投影扫描：一段被统计整段跳过、另一段行过滤后只返投影列，经 S3 往返验证）。
- [x] **分布式 Cypher 读（工作线 B 完整版，✅ 完成 2026-07-08）** `distributed_query` 从字符串分类器升级为 **AST 规划器**（依赖 `nexora-language`）：`plan(query)` 解析后构建 `DistributedPlan`（merge kind + 输出列 + owner 侧改写查询 + 协调端 distinct/order_by/skip/limit + 可选 rel_join），无法证明可归并的 shape 返 `None`（→ 诚实 501）。支持面：
  - **扫描/投影** `MATCH (n[:L]) RETURN ...` → 行 concat；
  - **全局聚合** count/sum/avg/min/max —— owner 侧改写发部分聚合（`avg(x)`→`sum(x),count(x)`），协调端 combiner 合并（`merge.rs` 的 `AggCombiner`）；
  - **分组聚合** `RETURN <keys>, <aggs>`（GROUP BY 非聚合键）—— 按 group key 合并每组部分聚合；
  - **全局 ORDER BY / SKIP / LIMIT / DISTINCT** —— 协调端在合并后按 Cypher 顺序（distinct→order→skip→limit）施加；
  - **单跳跨分区关系 join** `MATCH (a:La)-[:REL]->(b:Lb) RETURN ...`（`join.rs`）—— 三段 fan-out：①各 owner 扫源 id ②按源 id 路由取 REL 边（边与源同 owner）③按目标 id 路由取属性（目标可跨 owner）＋目标标签过滤，拼接输出。这正是本地执行器做不到的（本地 executor 丢弃 target 不在本快照的边，`executor.rs:206`）。
  `execute(router, &plan)` fan-out `ExecuteCypher`（或 join 路径），任一 owner 失败即上报（不返部分结果）。app `try_distributed_query` 在 501 前先试。**验收**：`distributed_query` 23 单测（AST 分类 / 部分聚合 sum·avg·min·max 合并 / 分组 count·avg / 全局 order·skip·limit·distinct / 关系 join 分类含 incoming 与拒多跳·变长·无向·无类型·join 上聚合）+ smoke **V7**（count·scan 跨 owner）、**V8**（sum·avg·grouped-count 跨 owner）、**V9**（`-[:KNOWS]->` 源与目标在不同 owner，join 拼回 Alice→Bob）。
- [x] **分布式关系遍历扩展（✅ 完成 2026-07-08）** 在单跳有向 join 之上补齐跨分区遍历的其余形态：
  - **多跳定长链 + 变长路径**（`path.rs`）：`MATCH (a)-[:R]->(b)-[:R]->(c)` 与 `MATCH (a)-[:R*1..3]->(b)`。协调端逐跳广度扩展 partial paths，变长 hop 做带环安全 BFS（visited 去重 + `max_hops` 深度上限 8），标签约束按到达位置过滤。
  - **无向 / 无类型边**：`-[:R]-`（`HopDir::Either`，两向合并）、`-[]->`（`edge_type=None`，任意类型）。单跳无向/无类型委托给 path 模型；`fetch_edges` 改 `Option<&str>`（None=全类型）。
  - **关系 & 路径 join 之上的聚合/分组/排序**（`JoinPost` + `merge::apply_join_post`）：`build_join_return` 检测 RETURN 里的聚合或全局子句，把 join 改为抽出裸列（分组键 + 聚合参数，去重 intern），协调端在 join 输出上本地单遍聚合（`AggCombiner::feed_raw`）+ order/skip/limit/distinct。单跳 join 与路径 join 共用同一套。
  **验收**：`distributed_query` 36 单测（多跳/变长/无向/无类型分类 + JoinPost 分类 + `apply_join_post` 本地 count/grouped-count/avg/order-limit）+ smoke **V10**（多跳 a→c 与变长 1..2 跨 owner）、**V11**（无向 + 无类型跨 owner）、**V12**（跨分区 join 上的 count(*) 与按目标 city 分组 count）。
- [x] **WITH 两阶段管线（✅ 完成 2026-07-08）** `plan_with_pipeline` 支持 `MATCH (node scan) → WITH → RETURN`：把节点扫描 stage-1 逻辑抽成 `plan_node_scan_stage`（`plan` 与管线共用），stage-1 分布式跑（scan / 全局·分组聚合，跨 owner 归并），协调端物化中间行后应用 `WithStage`（`merge::apply_with_stage`）——WITH 的 WHERE（对聚合结果的 HAVING 式单比较过滤，`RowFilter`）→ 最终 RETURN 投影（选/重排 WITH 列）→ order/skip/limit/distinct。顺带修正 `ProjItem::Grouping` 携带 expr+column，使 owner 端按原始表达式（`n.city`）分组、别名（`AS city`）只做输出列名。**验收**：`distributed_query` 43 单测（管线投影/HAVING 过滤/order-limit 分类 + `apply_with_stage` 过滤·投影·排序 + 拒关系管线/拒引用非 WITH 列）+ smoke **V13** `v13_with_pipeline_having_filter_across_owners`（Person 按 city 跨 owner 分组、`WITH … WHERE c>=2` 过滤、RETURN 按 count 降序，NYC(3)/SF(2) 留、LA(1) 汰）。
- [x] **分布式 UNION（✅ 完成 2026-07-08）** `plan` 抽出 clause-level `plan_clauses`，`plan_union` 对 `Clause::Union` 每个分支递归 `plan_clauses`（分支不可分布则整体拒），要求分支列 arity 一致（列名取首分支，Cypher 语义）。`execute` 拆成 `execute`+`execute_inner`（Box::pin 支持 UNION 分支递归），各分支独立分布式跑后顶层 concat，`UNION`（非 ALL）再全行去重。新增 `UnionPlan`。**验收**：`distributed_query` 46 单测（UNION/UNION ALL 分类、arity 不一致拒、分支不可分布拒）+ smoke **V14** `v14_distributed_union_across_owners`（Person∪Robot 跨 owner：UNION ALL 保 7 行含重复、UNION 去重成 6）。
- [x] **分布式写路径（✅ 完成 2026-07-08）** `plan_write` 检测 `MATCH (n[:L]) <no WHERE> SET|REMOVE|DELETE ...` 的 owner-parallel 写：每个 owner 各跑本地写（只动自己的匹配节点，节点单一 owner → 无重叠），协调端按 `WRITE_STAT_COLUMNS` SUM 8 项 WriteResult 统计（`execute_write`）。adapter 的 `ExecuteCypher` 写分支改为回传结构化统计行（原来是有损字符串），app `write_stats_from_columns` 把它还原成 `WriteStats`。**顺带修真实 bug**：单机 `execute_write` 之前每变量只绑一个节点，`MATCH (n:Person) SET ...` 只改一个节点；新增 `try_bulk_match_mutate` 对所有匹配节点应用（按 label index / all_node_ids 解析全集）。CREATE/MERGE（服务端生成 id 会把节点错置到执行 owner 而非 id 真正归属的 owner）与关系模式写仍诚实 501。**验收**：`distributed_query` 48 单测（MATCH-SET/DELETE/REMOVE 识别为写、拒 CREATE/MERGE/关系写/裸 MATCH）+ `write_executor` 2 回归单测（bulk SET 改全部 3 节点、bulk DELETE 删全部 3 节点）+ smoke **V15** `v15_distributed_write_set_across_owners`（Person 跨 owner，`SET n.active=true` 后 properties_set 求和=6、属性物理落在各 owner）。
- [x] **关系模式 WITH 管线（✅ 完成 2026-07-08）** `plan_with_pipeline` 的 stage-1 从「仅节点扫描」扩到关系模式：pattern 非 node-only 时，用 WITH items 合成一个 RETURN 喂给 `plan_relationship_join`（含单跳 join / 多跳·变长 path join / join 上的聚合分组），stage-1 列即 WITH 输出。`execute_inner` 重构成先算 stage-1（rel_join / path_join / scan 三源之一，join 带 JoinPost 时本地折叠），三条路径统一汇入 `with_stage` 后处理（WITH WHERE HAVING → RETURN 投影 → order/skip/limit/distinct）——不再各自提前返回。**验收**：`distributed_query` 50 单测（关系 WITH 管线识别为 rel_join+with_stage、关系 WITH 聚合识别为 JoinPost+with_stage、拒引用非 WITH 列）+ smoke **V16** `v16_relationship_with_pipeline_across_owners`（`(a:Person)-[:KNOWS]->(b:Person)` 跨 owner，按目标 city 分组 count、`WITH … WHERE c>=2` 过滤，仅 NYC(3) 留、LA(1) 汰）。
- [x] **分布式 CREATE（✅ 完成 2026-07-09）** `plan_create` 检测 `CREATE (node)` 形式（不含关系），协调端预生成 qid（按 `shard_key() % total_shards` 确定目标 owner），构建 `CreatePlan{nodes: Vec<CreateOp>}`。`execute_create` 按 owner 分组节点、构建子查询（注入 `__qid` 属性），fan-out 并行创建，SUM 返回统计。单机写执行器 `execute_create` 检测 `__qid` 属性（`properties.iter().find`），存在则用该 qid、否则生成随机 qid（单机模式），跳过 `__qid` 属性不持久化。**验收**：`distributed_query` 54 单测（plan_create 识别单节点/多节点、预生成 qid 互异、拒关系 CREATE/拒复杂表达式 + refuses_create 更新为「支持节点 CREATE、拒 MERGE」）+ smoke **V17** `v17_distributed_create_across_owners`（6 个 Person 跨 2 owner 创建，物理落在各 owner 的真实 graph、分布式扫描读回全部 6 个 name）。
- [x] **多阶段 WITH 链（✅ 完成 2026-07-10）** `plan_with_pipeline` 从「MATCH→WITH→RETURN」扩到「MATCH→WITH→WITH→RETURN」（三段及以上）。递归 `plan_with_stage_recursive`：对三段及以上链（含 n-1 个 WITH），从第二个 WITH 开始**递归应用单阶段 WITH 逻辑**（前一段的 `stage1_columns` 成为新的"假想扫描"源，递归 `plan_with_stage_from_columns` 对每个中间 WITH 构建 `WithStage` + 嵌套的可选子 stage），链式嵌套 `with_stage.with_stage` 使协调端按洋葱顺序（外→内）从物化中间行流过每一层。`execute_inner` 改为应用第一个 `with_stage` 即可，递归应用自动展开全链。**验收**：`distributed_query` 58 单测（三段管线识别 / 递归应用链 / 拒引用非前置 WITH 列 + `apply_with_stage_recursive` 单测流经三段 filter+projection+order）+ smoke **V18** `v18_multi_stage_with_chain_across_owners`（MATCH→WITH(聚合)→WITH(HAVING 过滤)→RETURN(排序)，Person 跨 2 owner 分组→过滤→排序，验证每一段的中间结果正确递进）。
- [ ] **分布式 Cypher/SQL 规划器（剩余增量，可延后）**：MERGE 分布式写（需两阶段 MATCH+CREATE）。落在覆盖面之外的仍诚实 501——非正确性缺口，是覆盖面扩展。

---

## 关键纪律（贯穿所有任务）

1. **单机路径永不回归**：每个改动都用 `Option<router>` / feature 门控，无 `--cluster` 时行为逐字节不变。每组任务先跑单机全套回归。
2. **不跳阶段**：P0 是一切的前置；HA 必须 2→3→4 顺序（跳到 4 = 切空节点，当前代码的错误）。
3. **每条线独立立项**：P1/P2 各涉及数据正确性或跨节点一致性，配专门评审，不夹在日常优化里。
4. **端到端验证优于组件测试**：分布式必须真起多节点+故障注入，不能只测孤立组件（当前测试的通病）。
5. **诚实沟通**：每个里程碑严格按"能说什么"表述，不把中间阶段当成 HA/存算分离。
6. **`cargo build --workspace`**：改公共路径后必须全工作区构建（HRTB 等下游编译问题只在下游 crate 暴露）。

---

*基于 2026-07-08 源码审计。相关：[DISTRIBUTED_EVOLUTION.md](DISTRIBUTED_EVOLUTION.md)（四线全景）、[HA_ROADMAP.md](HA_ROADMAP.md)（HA 详细阶段）。*
