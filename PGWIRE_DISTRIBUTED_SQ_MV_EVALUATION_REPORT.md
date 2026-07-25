# Nexora PG-wire 分布式链路复评报告

评估日期：2026-07-12

评估范围：PG-wire 分布式 CRUD/统计、Standing Query、sink、MV、RF=3 复制/failover、app-level 黑盒验收。

## 1. 总体结论

本轮复评结论没有上调：

> PG-wire 分布式主链路仍然通过，但本轮声称修复的 app-level 黑盒验收与 `nexora-zenoh` 包级回归仍存在编译阻断，不能认定高可用验收闭环完成。

核心 PG-wire 分布式 E2E 仍表现很好：`distributed_pgwire_e2e` 19/19 通过，覆盖分布式 CRUD、统计聚合、SQ、MV、聚合 MV、IN 批量、三节点和 RF=3 写复制/follower 读取基础能力。

但上一轮指出的两个硬阻断仍未修复：

1. `nexora-app` 的 `cluster_acceptance` 测试仍缺少 `assert_cmd`、`predicates` dev-dependencies，测试无法编译。
2. `nexora-zenoh` 的 `distributed_integration.rs` 仍未覆盖新增 `GraphOperation::Ping`，包级测试编译失败。

此外，`cluster_acceptance.rs` 中关键黑盒用例仍然是 `#[ignore]`，多条还是 “This test would...” 占位说明；`quorum_read.rs` 中版本化 consensus 选择仍是 TODO。

## 2. 本轮实测结果

| 测试命令 | 结果 | 说明 |
|---|---:|---|
| `cargo test -p nexora-pgwire --test distributed_pgwire_e2e -- --nocapture` | 19/19 通过 | PG-wire 分布式主链路和 RF3 基础能力通过 |
| `cargo test -p nexora-pgwire --test update_delete_auto_trigger_sq -- --nocapture` | 3/3 通过 | `WHERE id IN (...)` 批量 UPDATE 仍通过 |
| `cargo test -p nexora-app --test cluster_acceptance -- --nocapture` | 失败 | 缺少 `assert_cmd`、`predicates` |
| `cargo test -p nexora-app --test cluster_acceptance -- --ignored --nocapture` | 失败 | 同样无法编译 |
| `cargo test -p nexora-zenoh quorum_read -- --nocapture` | 失败 | `GraphOperation::Ping` 未被测试 handler match 覆盖 |

## 3. 已确认仍然正常的能力

1. PG-wire 分布式主链路未回归。

`distributed_pgwire_e2e` 19/19 通过，继续证明分布式 INSERT/SELECT/UPDATE/DELETE、全局 COUNT/SUM/AVG/MIN/MAX、GROUP BY、IN 批量、SQ Match/Unmatch、MV、聚合 MV、三节点和 RF3 基础路径仍可运行。

2. 单机 UPDATE/DELETE/SQ 自动触发未回归。

`update_delete_auto_trigger_sq` 3/3 通过。

3. `promote_follower_to_owner` 已不再是纯 TODO。

当前实现会校验 follower、递增 epoch、切换 owner、更新 replicas 和 shard map version。

## 4. 仍阻断验收的问题

### P0-1：app-level 黑盒测试仍无法编译

`crates/nexora-app/tests/cluster_acceptance.rs` 使用：

```rust
use assert_cmd::Command;
use predicates::prelude::*;
```

但 `crates/nexora-app/Cargo.toml` 没有对应 dev-dependencies。实际错误：

```text
failed to resolve: use of unresolved module or unlinked crate `predicates`
unresolved import `assert_cmd`
```

建议：补齐：

```toml
[dev-dependencies]
assert_cmd = "2"
predicates = "3"
```

如果 `predicates` 没实际使用，应删除 import。

### P0-2：`nexora-zenoh` 包级测试仍编译失败

`crates/nexora-zenoh/tests/distributed_integration.rs` 的 `TestGraphHandler::handle` match 未覆盖：

```rust
GraphOperation::Ping
```

实际错误：

```text
non-exhaustive patterns: GraphOperation::Ping not covered
```

建议增加显式分支：

```rust
GraphOperation::Ping => Ok(GraphResult::Status {
    ok: true,
    message: "pong".into(),
})
```

### P1：app-level 黑盒用例仍是 ignored/占位

当前 `cluster_acceptance.rs` 中关键测试仍为 `#[ignore]`：

- `test_three_node_cluster_distributed_insert_and_query`
- `test_distributed_standing_query_triggers_webhook`
- `test_distributed_materialized_view_updates_on_write`
- `test_rf3_cluster_survives_one_node_failure`

其中多条只有 “This test would...” 注释，没有真实执行逻辑。即使编译问题修好，也不能作为 app-level 黑盒验收通过证据。

### P1：quorum read 仍不能宣传为强一致完成

`quorum_read.rs` 中 `select_consensus_value` 仍是返回第一个响应，并保留：

```rust
// TODO: Implement proper version-based consensus
```

建议把口径限定为“quorum read 框架存在”，不要说“强一致 quorum read 已完成”。

## 5. 建议对团队的正式回复

本轮复评确认，PG-wire 分布式核心链路仍然稳定，`distributed_pgwire_e2e` 19/19 通过，`update_delete_auto_trigger_sq` 3/3 通过；RF=3 写复制和 follower 读取基础路径没有回归。

但上一轮指出的两个硬阻断仍未解决：`nexora-app` 的 app-level 黑盒测试缺少 dev-dependencies，导致无法编译；`nexora-zenoh` 包级测试仍因 `GraphOperation::Ping` 未覆盖而编译失败。此外 app-level 黑盒关键用例仍是 ignored/占位，quorum read 仍缺版本化 consensus。

建议对外口径保持谨慎：

> PG-wire 分布式核心链路继续通过；但本轮新增的 app-level 黑盒验收和 `nexora-zenoh` 包级回归仍未达标，需要先修复测试编译、取消占位用例并补齐真实黑盒执行，再认定高可用验收闭环完成。

整改优先级：

1. 补齐 `assert_cmd`/`predicates` 或删除未用 import。
2. 给 `GraphOperation::Ping` 增加测试 handler 分支。
3. 将至少一条 app-level 三节点 PG-wire/RF3 smoke 改为默认可跑。
4. 替换 “This test would...” 占位为真实 webhook/MV/failover 黑盒逻辑。
5. 补齐 quorum read 的版本化 consensus 选择和测试。
