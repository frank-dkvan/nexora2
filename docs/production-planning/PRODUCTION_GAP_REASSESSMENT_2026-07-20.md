# 生产级差距再评估 — 集群 HA 与 event-first OLAP

**日期**: 2026-07-20
**方法**: 逐行源码核查(file:line 级证据),对前序笼统判断("离生产级还有 4-8 个月")的修正。
**范围**: 仅覆盖两块争议最大的能力 —— 集群 HA、event-first OLAP。单节点持久模式的生产就绪结论不变(见 [[PRODUCTION_READINESS_GAPS]])。

> **一句话结论**: 两块都被前序评估**低估了实现完成度、误判了真正的瓶颈**。集群 HA 卡在"从未在真实分布式故障下验证过"(不是缺功能);event-first OLAP 卡在"实时增量 MV 名不副实 + 缺表维护运维闭环"(不是不能用)。

---

## 修正:前序判断哪里不准

前序原话:*"集群 HA 是实验骨架(缺真共识);event-first OLAP 能用但受 iceberg 0.9.1 限制。还有 4-8 个月。"*

| 前序说法 | 代码真相(2026-07-20 核查) |
|---|---|
| "集群缺真共识" | ❌ 不准。`--cluster` 路径有**真在跑的 openraft 控制面共识**;failover detector 在 `crates/nexora-zenoh/src/cluster.rs:729` **无条件 spawn**,含真实 promotion 逻辑;state transfer `catch_up_shard` 已接 `RemoteGraphClient` 真传输(`state_transfer.rs:129/165/192`)。"缺真共识"只适用于**另一条独立的** `--raft-port` 日志复制骨架路径(`main.rs:1179` 明确标 EXPERIMENTAL),那是可选实验路径,非主集群路径。 |
| "实验骨架" | ⚠️ 一半对。数据面是**有意的路线 B**(quorum write + epoch fencing,非数据面共识)—— 设计取舍,非缺陷(见 [[CONSENSUS_DECISION_RAFT_VS_PRIMARY_BACKUP]])。控制面共识是真的。 |
| "4-8 个月" | ⚠️ 那是路线图**理想终态**(含 Track F Flink 对齐等增强),非"达到生产级"的最小集。 |

**同时发现文档自相矛盾(诚信项)**: `README.md:27` 称 "automatic failover ... not wired into the runtime",但 `cluster.rs:729` failover 明明已无条件运行;且 `README.md:92` 又宣称 "automatic failover"。两行口径互斥,27 行过期。本次一并修正。

---

## A. 集群 HA —— 差的是「验证」,不是「实现」

实现层面基本齐全。真正缺口是**信心/验证**:

### A-1 [P0] 真实多进程 + 网络分区的 chaos e2e
- **证据**: `crates/nexora-zenoh/tests/` 共 18 个测试文件,其中 7 个 `#[ignore]`;绝大多数是**进程内**(in-process)测试,未 spawn 多个独立 nexora 进程 + 注入网络分区/丢包/延迟。
- **缺什么**: 共识与 failover 的正确性**从未在真实分布式故障注入下验证过**。这是上生产前最硬的一关。
- **工作量**: 2-3 周(Testcontainers 或多进程 harness + `tc netem` 丢包/延迟 + kill-owner/split-brain/滚动重启场景)。对应路线图 E1。

### A-2 [P0] 跑一次真实 72h+ soak
- **证据**: 框架已就位(`crates/nexora-core/tests/soak.rs`,短跑 `soak_short_smoke` 每次 CI 跑,验证 harness 逻辑),但长跑变体 `soak_long_running` 是 `#[ignore]`(`soak.rs:188`),**从未实际跑过 72h**。
- **缺什么**: 内存/句柄泄漏、性能衰减是未知数。
- **工作量**: 搭建已完成,主要是 `NEXORA_SOAK_SECS=259200` 跑一轮 + 观察。对应路线图 E2。

### A-3 [P1] 数据面耐久性语义显式化
- **证据**: 热写路径用 unsynced `WriteOptions` 换吞吐,靠 group-commit `flush_durable` 做 fsync(`crates/nexora-persistor-rocksdb/src/persistor.rs:529-534`);WAL 有 `SyncPolicy`(`crates/nexora-core/src/wal/log.rs:80-98`)含 group-fsync 策略。控制面元数据是每写 fsync(`control_plane_store.rs:258/272` `set_sync(true)`)。
- **缺什么**: 配合 TD-1(写路径 best-effort quorum 未接两阶段)+ TD-2(无 request_id 幂等键),需向客户端**明确契约**:"最终一致 + 崩溃可能丢最近未 fsync 窗口"。当前该契约散落在代码注释,未成面向用户的耐久性文档。
- **工作量**: 1 周(文档 + 灰度指标验证);若要"严格无部分写"金融级语义,则接两阶段提交 2-3 周(对应路线图 A1.1)。

### A-4 [P1] 修文档诚信项
- `README.md:27` 过期声明("failover not wired")→ 已在本次修正。
- 统一 27/92 行口径。anti-entropy 应表述为"**已接线但默认关闭(opt-in `--anti-entropy-secs`)**"(证据 `cluster.rs:739` + `main.rs:1271`),而非"未接线"。
- **工作量**: 零工程量,纯措辞。

> **A 段结论**: 集群 HA 的代码路径**基本齐全且在跑**,但**没有一次在真实分布式故障注入下验证过**。生产级门槛 = A-1 chaos e2e + A-2 soak 各跑通一轮。乐观 **1-1.5 个月**补齐"敢灰度上生产"的信心,而非 4-8 个月。

---

## B. event-first OLAP —— 差的是「实时增量」与「运维闭环」

能编译、能用、e2e 打通(POST 本体 → 建 Iceberg 表 → topic 路由 → 定时刷新真触发)。真正缺口:

### B-1 [✅ 已完成 2026-07-20] 物化视图真增量
- **原缺口**: `view_refresher.rs` 的 `incremental_refresh` 退化为全量刷新,"实时增量 MV" 名不副实。
- **已实现**: 真增量聚合(`incremental_aggregate`)。目标表版本化(`_mv_version`/`_mv_src_snapshot_id`,读时取最新代,顺带修掉全量刷新重复 append 的既存 bug)+ 增量只读源表新增 Iceberg snapshot 文件(`EventLogStore::read_snapshot_delta`,append-only ⇒ 文件差集即增量)+ 部分聚合 `UNION ALL` 旧态再归约(COUNT/SUM/MAX/MIN 直接可合并,AVG 用 `sum`+`count` 伴生列)。Push/Hybrid 经 `RefreshScheduler::schedule_incremental` 短周期触发,空 delta no-op。
- **验证**: 集成测试 `test_incremental_equals_full_all_agg_types`(增量==全量,五种聚合逐分组比对)、`test_incremental_noop_when_no_new_events`、Phase 0 dedup 测试全绿;`cargo clippy --features olap -D warnings` 0 警告。
- **剩余边界**: 仅结构化 `Aggregate` 视图走真增量;`Sql`-transform 视图(含 DomainMV)仍全量刷新(需查询分析,超范围)。

### B-2 [P1] iceberg-rust 0.9.1 运维天花板(上游库限制,非本项目 bug)
- **证据**:
  - 快照过期/删除只能**识别不能执行**: `retention.rs:51-52` `TableCommit 构造私有 + Transaction 无 expire_snapshots action`;`retention.rs:129` warn 明示。
  - 无 compaction: 无 `RewriteDataFiles`(`nexora-eventlog-implementation-progress` 记录)。
  - DataFusion 直接注册 Iceberg 表被挡: `datafusion_store.rs:38` `IcebergTableProvider::try_new() 是 pub(crate)`;`register_table` `bail!("not yet implemented - waiting for iceberg-datafusion API")`(`datafusion_store.rs:39`)。现绕道 MemTable。
- **缺什么**: 生产前必须有**表维护运维方案** —— 数据不会自动 compaction/过期,小文件与快照无限增长。
- **工作量**: 等 iceberg-rust 0.10+,或接外部工具(每晚 PyIceberg/Spark/Trino 定时 compaction + expire),**1-2 周**接外部工具链。

### B-3 [P1] 进默认门禁与发版验证
- **证据**: `crates/nexora-app/Cargo.toml` `event-first` 特性非 default;CI(本次 PR #9 已补 `test-event-first` job + olap clippy)但**因 Actions 额度停摆从未在真实 runner 验证过**(见 [[nexora-v030-release-ci-debt]])。
- **缺什么**: 决定"默认开启(承担 iceberg 重依赖)"还是"保持可选但发版流程强制 `--features` 验证";并在 Actions 额度恢复后跑一次确认新 job 无误。
- **工作量**: 决策 + 1 次验证。

### B-4 [P2] topic 校验 TODO
- **证据**: `router.rs:134` `TODO(阶段 2): 实现 topic 校验` —— 阶段 2 标称完成但仍留 TODO。
- **缺什么**: 事件表名(= topic)注入防护(净化非法字符),防止 Iceberg 表名注入。
- **工作量**: 2-3 天。

> **B 段结论**: event-first 内核(事件真相源、Iceberg 写入、SQL 查询、本体驱动)**可用**,但 **"实时增量 MV" 名不副实(实为定时全量)+ 缺表维护运维闭环**。生产级 = 诚实降级卖点可立即上;补真增量(3-4 周)+ 外部 compaction/expire(1-2 周)。

---

## 汇总:达到生产级的最小工作集

| 块 | P0 阻断项 | P1 | 零工程量诚信项 | 乐观工期 |
|---|---|---|---|---|
| 集群 HA | A-1 chaos e2e、A-2 72h soak | A-3 耐久性契约文档 | A-4 README 27/92 行 | 1-1.5 月 |
| event-first OLAP | B-1 MV 增量(或诚实降级) | B-2 外部表维护、B-3 门禁 | B-1 措辞、B-4 topic 校验 | 1.5-2 月(补能力)/ 立即(降级) |

**零工程量、立即该做**(否则"生产级"站不住): README:27 过期 failover 声明、anti-entropy "opt-in" 措辞、MV "实时增量"→"近实时定时刷新"、耐久性契约文档化。本次已修 README:27 与 MV 措辞两项。

关联: [[ROADMAP_TO_PRODUCTION_LEADING_2026-07-18]]、[[PRODUCTION_READINESS_GAPS]]、[[CONSENSUS_DECISION_RAFT_VS_PRIMARY_BACKUP]]、[[HA_ROADMAP]]。
