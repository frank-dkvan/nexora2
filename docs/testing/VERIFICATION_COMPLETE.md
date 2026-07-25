# 多节点集群 PG-Wire 完整链路测试 - 执行总结

## 执行时间
2026-07-21

## 测试结果

### ✅ 全部通过（19/19）

```
running 19 tests
test distributed_insert_spreads_across_owners_and_count_is_global ... ok
test distributed_global_aggregates_match_single_node_oracle ... ok
test distributed_group_by_counts_match_oracle ... ok
test distributed_filtered_projection_read_applies_predicate_across_nodes ... ok
test distributed_filtered_update_applies_across_owners ... ok
test distributed_filtered_delete_applies_across_owners ... ok
test distributed_in_clause_batch_update_applies_across_owners ... ok
test distributed_insert_triggers_sq_for_remote_owned_node ... ok
test distributed_filtered_update_triggers_sq_across_owners ... ok
test distributed_filtered_delete_triggers_sq_unmatch_across_owners ... ok
test distributed_write_broadcasts_sq_result_for_mv_bridge ... ok
test distributed_mv_end_to_end_query_via_pgwire ... ok
test distributed_aggregate_mv_incremental_deltas_across_owners ... ok
test owner_down_errors_instead_of_returning_partial_data ... ok
test distributed_write_to_down_owner_errors_not_false_success ... ok
test unsupported_cluster_query_errors_instead_of_local_fallback ... ok
test three_node_insert_spreads_and_aggregates_are_global ... ok
test rf3_pgwire_write_replicates_to_followers ... ok
test rf3_owner_failure_read_from_follower ... ok

test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
Time: 0.30s
```

## 功能验证总结

### ✅ Graph 数据链路（完全验证）

| 功能类别 | 测试项 | 状态 |
|---------|--------|------|
| **写入分布** | INSERT 跨节点分布 | ✅ 通过 |
| **全局读取** | COUNT(*) 跨节点合并 | ✅ 通过 |
| **聚合查询** | SUM/MIN/MAX/AVG | ✅ 通过 |
| **分组聚合** | GROUP BY city | ✅ 通过 |
| **过滤查询** | WHERE age > N | ✅ 通过 |
| **分布式写** | UPDATE 跨节点 | ✅ 通过 |
| **分布式删除** | DELETE 跨节点 | ✅ 通过 |
| **批量更新** | IN 子句 | ✅ 通过 |
| **Standing Query** | 跨节点触发 | ✅ 通过 |
| **物化视图** | 端到端链路 | ✅ 通过 |
| **增量聚合 MV** | 跨节点增量 | ✅ 通过 |
| **故障处理** | Owner down 报错 | ✅ 通过 |
| **3节点集群** | 3节点分布 | ✅ 通过 |
| **副本复制** | RF=3 复制 | ✅ 通过 |

### Event 表链路

| 功能 | 状态 | 说明 |
|------|------|------|
| **跨节点读取** | ✅ 已实现 | event_table_handler.rs 实现了跨节点查询 |
| **写入** | ⚠️ Coordinator-only | 已知限制，已在代码中文档化 |

## 测试配置

- **测试类型**：集成测试（真实 TCP 传输）
- **节点数量**：2 节点 / 3 节点
- **分片数**：8 个 shard
- **副本因子**：RF=1 / RF=3
- **路由模式**：HybridRouter (no-local)
- **客户端**：tokio-postgres
- **测试文件**：`crates/nexora-pgwire/tests/distributed_pgwire_e2e.rs`

## 核心验证点

### 1. 数据分布正确性

```sql
INSERT INTO Product (id, name, price) VALUES ('prod-0', 'p0', 0);
-- ... 40 rows ...
```

**验证**：
- ✅ 数据按 shard map 分布到 node-a 和 node-b
- ✅ node-a 持有部分 ID，node-b 持有其余 ID
- ✅ 没有数据泄漏（node-a 不持有 node-b 的 ID）

### 2. 全局查询正确性

```sql
SELECT COUNT(*) FROM Product;  -- 返回 40（全局总数）
SELECT SUM(val) FROM Metric;   -- 跨节点合并
SELECT city, COUNT(*) FROM Person GROUP BY city;  -- 分组合并
```

**验证**：
- ✅ COUNT 返回全局总数（不是单节点局部数）
- ✅ 聚合函数跨节点正确合并
- ✅ GROUP BY 每组的结果是全局的

### 3. 谓词下推

```sql
SELECT id, name FROM Person WHERE age > 29;
```

**验证**：
- ✅ WHERE 条件在每个节点上执行
- ✅ 不匹配的行未被返回
- ✅ 结果来自多个节点

### 4. 分布式写入

```sql
UPDATE Person SET vip = true WHERE age > 40;
DELETE FROM Person WHERE age < 25;
```

**验证**：
- ✅ 写操作扇出到所有相关节点
- ✅ 每个节点只更新/删除其拥有的匹配行
- ✅ 物理验证数据变化

### 5. Standing Query 集成

```sql
INSERT INTO Product (id, name, price) VALUES ('remote-id', 'gpu', 1200);
-- SQ: price > 500 触发
```

**验证**：
- ✅ 本地节点写入触发 SQ
- ✅ 远程节点写入也触发 SQ
- ✅ SQ 通过 router 重新读取属性

### 6. 物化视图链路

```sql
UPDATE Person SET vip = true WHERE id IN (...);
SELECT * FROM vip_members;
```

**验证**：
- ✅ 分布式写入 → SQ 触发 → MV bridge 更新
- ✅ 通过 PG-wire 查询 MV 返回正确数据
- ✅ 增量聚合 MV 正确处理跨节点更新

### 7. 故障场景

```sql
-- node-B 不可达
SELECT COUNT(*) FROM Widget;  -- ERROR（不是局部数据）
```

**验证**：
- ✅ Owner 不可达时查询报错
- ✅ 不会静默返回不完整数据
- ✅ 错误消息清晰说明问题

### 8. 副本复制（RF=3）

```sql
INSERT INTO Emp (id, salary) VALUES ('rep-1', 500);
```

**验证**：
- ✅ 数据写到 owner + 2 个 follower
- ✅ 所有 3 个节点物理持有数据
- ✅ follower 可读（failover 准备）

## 相关文档

- **测试源码**：`crates/nexora-pgwire/tests/distributed_pgwire_e2e.rs` (2183 行)
- **详细报告**：`docs/testing/DISTRIBUTED_PGWIRE_TEST_REPORT.md`
- **功能总结**：`docs/testing/PGWIRE_CLUSTER_TEST_SUMMARY.md`
- **验证脚本**：`scripts/verify_pgwire_cluster.sh`

## 运行测试

```bash
# 运行完整测试套件
cargo test --package nexora-pgwire --test distributed_pgwire_e2e

# 运行单个测试
cargo test --package nexora-pgwire --test distributed_pgwire_e2e \
  distributed_insert_spreads_across_owners_and_count_is_global

# 运行验证脚本
./scripts/verify_pgwire_cluster.sh
```

## 结论

✅ **多节点集群环境下，从 pg-wire 写入数据后，graph 数据能正确分布到多个节点，并且能正确读取。**

**具体验证**：
1. ✅ 写入：INSERT 通过 shard map 正确路由到各个 owner
2. ✅ 分布：数据物理分布验证通过，无泄漏
3. ✅ 读取：全局查询（COUNT/聚合/GROUP BY/WHERE）跨节点正确合并
4. ✅ 写操作：UPDATE/DELETE 跨节点正确执行
5. ✅ 集成：Standing Query、物化视图端到端工作
6. ✅ 可靠性：故障场景正确处理，副本复制正常

**Event 表状态**：
- ✅ 读取路径已实现（跨节点查询和合并）
- ⚠️  写入路径限制为 coordinator-only（已知限制，已文档化）

---

**测试通过率**：100% (19/19)  
**执行时间**：0.30 秒  
**验证日期**：2026-07-21
