# 生产就绪缺口清单与优化任务

> 生成于 2026-07-07。基于对源码的逐项验证(非报告转述)。每一项都标注了:代码位置、真实失败方式、影响面、是否已暴露给用户。
>
> 结论定性:**单机 + 增量物化视图 + 主体 Cypher/SQL + Kafka 摄取**这条主链路已达生产级。以下缺口是自带注释标注为 mock/未实现/no-op、且经验证确实接入了用户可达路径的部分。

## 严重程度分级

| 级别 | 含义 |
|------|------|
| P0 | 数据丢失/正确性错误,失败隐蔽(不报错),已接入生产路径 |
| P1 | 用户可达功能不可用,但会显式报错(不静默) |
| P2 | 功能降级/静默无效,影响有限或有明确文档说明 |
| P3 | 潜在隐患(latent),当前路径未触发,但属地雷 |

---

## P0 — 数据持久性 / 正确性(最高优先级)

### GAP-1:WAL 崩溃恢复丢失边属性与节点删除状态 ✅ 已修复(2026-07-07)
- **位置**:[crates/nexora-core/src/graph/shard/mod.rs](crates/nexora-core/src/graph/shard/mod.rs) 的 `wake_node` 回放循环 + snapshot 序列化。
- **原始验证结论**:生产写入走 `WalOperation::NodeEvents`(复数)→ JSON fallback,**事件已落盘**。但恢复回放循环对 `EdgePropertySet` / `EdgePropertyRemoved` / `NodeDeleted` / `NodeRestored` 四类事件是 `no-op`(仅 `LabelAdded/LabelRemoved` 有恢复)。
- **修复过程中发现的更深问题**:根因不止回放 no-op。`sleep_node` 走 snapshot 序列化路径,而 `serialize_snapshot` **只存了 properties+edges**,不含 labels/edge_properties/tombstone;而 `wake_node` 只回放 snapshot_time 之后的 journal 事件(sleep 场景下所有事件都 ≤ snapshot_time,被过滤)。因此:
  - sleep/wake 路径:全靠残缺 snapshot → labels(注释号称已恢复)、边属性、tombstone **全部丢失**。
  - 崩溃+replay_wal 路径:靠 journal 回放,但回放循环对这几类事件 no-op → 同样丢失。
- **修复内容**:
  1. 扩展 `SnapshotData`,新增 `labels` / `edge_properties` / `tombstone` 字段(均 `#[serde(default)]`,旧快照仍可反序列化)。边属性用 `(edge_type,target)` flatten 成记录列表以适配 JSON map key 限制。
  2. `serialize_snapshot` / `deserialize_snapshot` 改为承载全量状态;`wake_node` 以 snapshot 全量状态为回放初值,post-snapshot journal 事件叠加其上。
  3. 回放循环补全 `EdgePropertySet/EdgePropertyRemoved`(重建 edge_properties)与 `NodeDeleted/NodeRestored`(重建 tombstone)。
  4. `GraphService` 新增 `get_edge_properties` / `get_tombstone` getter(镜像 `get_labels` 的快照模式),供观测与查询软删除状态。
- **回归测试**:[crates/nexora-core/tests/crash_recovery_gap1.rs](crates/nexora-core/tests/crash_recovery_gap1.rs) —— 边属性/软删除/对照组共 3 项,全通过。全 nexora-core 套件(131 单测 + 各集成)无回归。
- **遗留**:`EdgePropertyRemoved` 恢复路径已实现,但目前无 `MutationOp::RemoveEdgeProperty` / `remove_edge_property` 公共 API 可产生该事件,故其端到端回归测试待删除 API 补齐后再加(测试文件已注明)。

### GAP-2:S3 冷存储是内存 mock ✅ 已修复(2026-07-07)
- **位置**:[crates/nexora-storage/src/s3.rs](crates/nexora-storage/src/s3.rs)。
- **原始问题**:`S3Storage` 是内存 HashMap mock,却已接入生产 CLI `--storage-backend=s3` 作为 `TieredStore` warm 层 → 用户以为落到对象存储,实际全在内存,重启即丢、内存无界增长,且启动日志误导性地打印 "Storage: s3"。
- **修复内容**:
  1. 用 `reqwest`(rustls TLS,与 app 栈一致)+ 手写 **AWS SigV4** 签名重写 `S3Storage`,实现真实 HTTP `put/get/head/list/delete/exists`。无 AWS SDK 依赖。兼容 AWS S3 / MinIO / R2 等(path-style 与 virtual-host 两种寻址)。
  2. `list` 用 ListObjectsV2 + 轻量 XML 解析(仅解析 `<Contents>` 的 Key/Size/LastModified,不引入 XML crate)。
  3. 原内存实现保留并改名 `MockS3Storage`(显式 test-only,生产代码无法再误用)。
  4. CLI 补齐 `--s3-endpoint` / `--s3-access-key` / `--s3-secret-key` / `--s3-path-style`,凭证回退到标准 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_ENDPOINT_URL` 环境变量;凭证为空时打印安全警告。
- **测试**:19 个单测(含 **AWS SigV4 官方测试向量**验证签名密钥正确、URI 编码、path/virtual-host URL 构建、XML 解析、Mock 行为)全通过。另加 [s3_minio_smoke.rs](crates/nexora-storage/tests/s3_minio_smoke.rs) —— 对活 MinIO/S3 的完整 put→head→get→list→delete 往返冒烟测试,默认 `#[ignore]`(需 `S3_SMOKE_*` 环境变量),不阻塞 CI。

---

## P1 — 用户可达功能不可用(显式报错)

### GAP-3:物化视图刷新 ✅ 已修复(2026-07-07)
- **验证中纠正的判断**:原文说 Manual/Scheduled 刷新"未实现"过于笼统。深入核实后:
  - **Manual 刷新其实已可用** —— 用户可达的 HTTP handler `refresh_materialized_view`([handlers/materialized_view.rs](crates/nexora-app/src/handlers/materialized_view.rs))早已执行 `source_query`(via Cypher)并调用 `replace_all`。`POST /api/v2/materialized-views/{id}/refresh` 是好的。
  - **真正的两个缺口**:(a) 核心 `refresh_view` 方法是个 stub 地雷(返回 "not implemented",但零调用者);(b) **Scheduled 模式完全没有调度器** —— cron 字符串存了却从不触发。
  - **事件触发的增量刷新链路只差最后一根接线**:SQ manager 早已把每个 `StandingQueryResult` 广播到 `result_tx`,`sq_mv_bridge.on_sq_result` 也能消费,但没有任何东西订阅通道连起来(`make_sq_callback` 的 `_bridge` 参数从未使用,注释还写着 "will be completed when StandingQueryManager exposes result events" —— 而它其实早就 expose 了)。
- **修复内容**(采纳"事件触发增量刷新"方向,优于轮询/定时):
  1. main.rs 启动时订阅 `sq_manager.subscribe()`,把每个 SQ 结果转发给 bridge → **SQ 匹配/取消匹配即时增量刷新对应物化视图**,无需轮询或 cron 调度。含 broadcast Lagged/Closed 处理。
  2. 清理 `make_sq_callback` 中未使用的 `_bridge` 参数与过时注释。
  3. 核心 `refresh_view` 的 Manual/Scheduled 分支从"静默返回 0 行"改为**显式报错**(说明 nexora-core 无查询引擎,应走 app 层 refresh 路径)—— 消除静默地雷。
  4. SQ manager 新增 `publish_result_for_test`(test-only)helper。
- **测试**:新增 `test_sq_broadcast_drives_mv_refresh`(端到端:广播 Matched → MV 插入行;广播 Unmatched → MV 删除行),复刻 main.rs 接线,通过。全 core/cypher/standing-query 套件无回归。
- **遗留**:基于 cron 表达式的真正定时刷新(`Scheduled(String)`)仍无调度器。鉴于事件触发已覆盖 SQ-backed 视图的实时刷新需求,cron 定时刷新降级为独立的后续增强项(需引入 cron 解析 + 时区处理)。

### GAP-4:Cypher `SET n.x = m.y`(变量引用赋值)未实现 ✅ 已修复(2026-07-07)
- **位置**:[crates/nexora-cypher/src/write_executor.rs](crates/nexora-cypher/src/write_executor.rs)。
- **修复中发现的细节**:`SET n.x = m.y` 的 RHS 实际解析为 `Expression::Property(Variable("m"), "y")`(不是原文以为的 `Expression::Variable`),且解析 `m.y` 需要**异步图读取**,而原 `expr_to_property_value` 是同步、无图句柄的。
- **修复内容**:新增异步 `resolve_set_value`,对 `Property(var, prop)` 从图中读取 `var` 绑定节点的 `prop`(缺失属性返回 null,符合 Cypher 语义);`BinOp` 递归解析两侧(用显式 `Box::pin` 递归,不引入 async-recursion 依赖);其余(字面量/函数/列表)仍走同步路径。`SetItem::Property` 与 `SetItem::MapProjection` 两条 SET 路径均改用它。
- **测试**:新增 `test_set_property_from_variable_reference`(`SET n.x = m.y` 复制值)与 `test_set_property_from_missing_reference_is_null`(缺失源属性 → null),全通过;cypher 全套件(21 单测 + 32 集成)无回归。

---

## P2 — 静默降级 / 有限影响

### GAP-5:Cypher `LOAD CSV` 是 no-op stub ✅ 已修复(2026-07-07,显式拒绝)
- **位置**:[write_executor.rs](crates/nexora-cypher/src/write_executor.rs) 与 [lib.rs](crates/nexora-cypher/src/lib.rs)。
- **原始问题**:`LOAD CSV FROM '...'` 仅 `tracing::warn!` 后静默无效(clause 被 strip),用户以为导入了数据实际没有。
- **决策**:选择"显式拒绝"而非实现完整 LOAD CSV。理由:完整实现是大特性且有真实安全面(任意文件读取、路径穿越),而用户实际踩的坑是**静默**——以为加载了其实没有。有现成替代路径(`/api/v2/ingest/file`)。
- **修复内容**:两处 LOAD CSV 分支从静默 no-op 改为返回 `CypherError::Unsupported`,错误信息明确指向 ingest API。
- **测试**:原三个断言"no-op 不报错"的测试(`test_load_csv_noop` 等)改写为断言显式拒绝(`test_load_csv_*_rejected`),验证错误信息含 "LOAD CSV" 与 "ingest";cypher 全套件无回归。

### GAP-6:Python UDF 运行时 stub 回退 ✅ 已修复(2026-07-07)
- **位置**:[crates/nexora-udf/src/python_runtime.rs](crates/nexora-udf/src/python_runtime.rs)。
- **原始问题**:PATH 无 `python3` 时回退 `stub()`,注册看似成功,execute 时才失败(失败时机太晚)。
- **修复内容**:新增 `is_available()`(stub 的 `python_path` 为空 → false);`register()` 在运行时不可用时**立即报错** `UdfRuntimeError::Python`,而非接受注册后延迟到 execute 才暴露。
- **测试**:新增 `test_stub_rejects_registration`(无需 python3 环境),全 udf 套件(33 单测 + 99 集成)无回归。

---

## P3 — 潜在隐患(当前未触发)

### GAP-7:flatbuf 单数编码路径丢弃 label/edge-prop/lifecycle 事件 ✅ 已修复(2026-07-07)
- **位置**:[crates/nexora-core/src/flatbuf_codec.rs](crates/nexora-core/src/flatbuf_codec.rs) —— `WalOperation::NodeEvent`(单数)对这几类事件曾 `return Vec::new()`(零长度/丢事件的 WAL 记录)。
- **决策**:不做完整 `.fbs` schema 重新生成(改动大、有风险,且对"当前未触发"的路径不成比例)。改为把这三类事件路由到复数 `NodeEvents` 路径**已在用的同一 JSON fallback** —— 复用经过验证的代码路径,消除损坏的空 Vec。
- **修复内容**:`LabelAdded/LabelRemoved/EdgePropertySet/EdgePropertyRemoved/NodeDeleted/NodeRestored` 六个单数分支从 `return Vec::new()` 改为 `serde_json::to_vec(&record)`。WAL 写入仍标 `WAL_MAGIC_FB`,读取侧 FB 分支在 flatbuf 解析失败时回退 JSON(与现有 `NodeEvents` fallback 同机制),故往返正确。
- **测试**:新增 `test_wal_record_landmine_events_roundtrip`,对全部六类事件断言 encode 非空 + `decode_wal_record_auto` 往返一致;全 core 套件(132 单测)无回归。

### GAP-8:pgwire COPY 未支持
- **位置**:[crates/nexora-pgwire/src/copy_handler.rs:1-5](crates/nexora-pgwire/src/copy_handler.rs) —— 明确不对外声明,SQL 层返回 feature-not-supported。
- **真实后果**:PG 客户端用 `COPY` 批量导入不可用。**这一项是诚实且有文档的**,不算隐患,列出仅为完整性。

---

## 优化任务清单执行状态(2026-07-07)

### 第一批(P0 — 阻塞生产部署)
1. ✅ **[GAP-1] WAL 恢复边属性重建** —— 完成(含快照全量状态修复)。
2. ✅ **[GAP-1] WAL 恢复节点 tombstone 重建** —— 完成。
3. ✅ **[GAP-2] 真实 S3 HTTP 后端** —— 完成(reqwest + SigV4,`MockS3Storage` test-only)。

### 第二批(P1 — 功能完整性)
4. ✅ **[GAP-3] MV 刷新** —— 完成(事件触发增量刷新接线 + 核心 stub 显式报错;cron 定时刷新降级为后续增强)。
5. ✅ **[GAP-4] Cypher SET 变量引用** —— 完成。

### 第三批(P2/P3 — 健壮性与地雷清除)
6. ✅ **[GAP-5] Cypher LOAD CSV** —— 完成(显式拒绝 + 指向 ingest API)。
7. ✅ **[GAP-6] Python UDF 注册期校验** —— 完成。
8. ✅ **[GAP-7] flatbuf 单数路径空 Vec 地雷** —— 完成(路由到 JSON fallback)。
9. ⬜ **[GAP-8] pgwire COPY**(可选,低优先) —— 未做。当前是诚实且有文档的"不支持",非隐患。

### 验收标准达成情况
- ✅ 崩溃恢复正确性集成测试套件 [crash_recovery_gap1.rs](crates/nexora-core/tests/crash_recovery_gap1.rs):覆盖 属性/边属性/标签/软删除/组合全状态 的写-崩溃-恢复往返(5 项,全通过)。
- ✅ S3 后端 CI 冒烟测试 [s3_minio_smoke.rs](crates/nexora-storage/tests/s3_minio_smoke.rs):完整 put→head→get→list→delete 往返,默认 `#[ignore]`(需 `S3_SMOKE_*` 环境变量 + MinIO 容器)。
- ✅ 每项修复均带能复现原缺陷的回归测试。

### 遗留后续增强项(非生产阻塞)
- **MV 基于 cron 表达式的定时刷新**:`Scheduled(String)` 仍无调度器。事件触发已覆盖 SQ-backed 视图的实时刷新;cron 定时刷新需引入 cron 解析 + 时区处理。
- **边属性删除 API**:`EdgePropertyRemoved` 的恢复路径与编码已就绪,但无 `MutationOp::RemoveEdgeProperty` / `remove_edge_property` 公共 API 可产生该事件;补齐后应加端到端回归测试。
- **GAP-8 pgwire COPY**。
- **完整 flatbuf schema**:GAP-7 用 JSON fallback 消除了地雷;若追求统一编码/性能,可后续扩展 `.fbs` 原生支持这些事件。
