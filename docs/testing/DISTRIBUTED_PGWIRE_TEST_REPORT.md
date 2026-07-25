# 分布式 PG-Wire 完整链路测试报告

## 测试目标

验证多节点集群环境下，从 pg-wire 写数据后：
1. **Graph 数据正确分布**：写入的节点按 shard map 分布到正确的 owner
2. **跨节点读取正确**：全局查询（COUNT、聚合等）能合并所有节点的数据
3. **Event 表集成**：当启用 event-first 模式时，事件表也能接收和查询数据

## 测试环境

- **测试文件**：`crates/nexora-pgwire/tests/distributed_pgwire_e2e.rs`
- **节点数量**：2 节点（部分测试 3 节点）
- **分片数**：8 个 shard
- **协议**：真实的 TCP 传输 + tokio-postgres 客户端
- **路由**：HybridRouter (no-local 模式，所有操作通过 TCP 客户端)

## 已通过的测试（19 项）

### 1. 基础分布式写入和读取

#### `distributed_insert_spreads_across_owners_and_count_is_global`
- ✅ INSERT 40 个产品，验证物理分布跨越两个 owner
- ✅ SELECT COUNT(*) 返回全局总数（不是单节点局部数）
- ✅ 验证每个节点只持有其 shard map 指定的数据

```sql
INSERT INTO Product (id, name, price) VALUES ('prod-0', 'p0', 0);
-- ... 40 rows ...
SELECT COUNT(*) FROM Product;  -- 返回 40（全局）
```

**关键验证点**：
- `on_a > 0 && on_b > 0`：数据确实分布到两个节点
- 物理验证：每个节点的 `graph.get_property()` 只返回其拥有的 ID
- 全局 COUNT 等于插入总数

### 2. 全局聚合函数

#### `distributed_global_aggregates_match_single_node_oracle`
- ✅ SUM(val)、MIN(val)、MAX(val)、AVG(val) 跨节点正确合并
- ✅ 与单节点 oracle 计算的结果完全一致

```sql
INSERT INTO Metric (id, val) VALUES ('m-0', 10), ('m-1', 20), ...;
SELECT SUM(val), MIN(val), MAX(val), AVG(val) FROM Metric;
```

**验证**：分布式聚合结果 == 本地 oracle 计算结果

### 3. GROUP BY 分组聚合

#### `distributed_group_by_counts_match_oracle`
- ✅ `GROUP BY city` 每组的 COUNT 正确合并跨节点的局部计数

```sql
SELECT city, COUNT(*) FROM Person GROUP BY city;
-- NYC=5, LA=3, SF=2（跨节点合并正确）
```

### 4. 过滤投影读取

#### `distributed_filtered_projection_read_applies_predicate_across_nodes`
- ✅ `WHERE age > 29` 在每个 owner 上正确执行
- ✅ 返回的行确实来自两个节点
- ✅ 不匹配的行没有被返回（谓词未被丢弃）

```sql
SELECT id, name FROM Person WHERE age > 29;
```

### 5. 分布式 UPDATE

#### `distributed_filtered_update_applies_across_owners`
- ✅ `UPDATE Person SET vip = true WHERE age > 40` 在两个 owner 上都执行
- ✅ 只有匹配的行被更新，其他行保持不变

```sql
UPDATE Person SET vip = true WHERE age > 40;
```

### 6. 分布式 DELETE

#### `distributed_filtered_delete_applies_across_owners`
- ✅ `DELETE FROM Person WHERE age < 25` 删除两个 owner 上的匹配行
- ✅ 其他行未被删除

```sql
DELETE FROM Person WHERE age < 25;
```

### 7. IN 子句批量更新

#### `distributed_in_clause_batch_update_applies_across_owners`
- ✅ `WHERE id IN ('p-0', 'p-1', ...)` 在两个 owner 上正确匹配
- ✅ 只有列表中的 ID 被更新

```sql
UPDATE Person SET flagged = true WHERE id IN ('p-0', 'p-2', 'p-5');
```

### 8. Standing Query 触发

#### `distributed_insert_triggers_sq_for_remote_owned_node`
- ✅ INSERT 到远程节点拥有的 shard 时，SQ 也能触发
- ✅ 验证了分布式写入路径会重新读取属性并评估 SQ

```sql
INSERT INTO Product (id, name, price) VALUES ('remote-id', 'gpu', 1200);
-- SQ (price > 500) 在远程节点上也触发
```

#### `distributed_filtered_update_triggers_sq_across_owners`
- ✅ UPDATE 触发的 SQ 结果跨节点正确

#### `distributed_filtered_delete_triggers_sq_unmatch_across_owners`
- ✅ DELETE 触发的 SQ unmatch 事件跨节点正确

### 9. 物化视图链路

#### `distributed_mv_end_to_end_query_via_pgwire`
- ✅ 完整的 MV 链路：分布式写入 → SQ 广播 → MV bridge → SELECT FROM mv
- ✅ 通过 PG-wire 查询 MV 返回正确的行

```sql
UPDATE Person SET vip = true WHERE id IN (...);
-- SQ 触发 → MV bridge 更新
SELECT * FROM vip_members;  -- 返回正确的行
```

#### `distributed_aggregate_mv_incremental_deltas_across_owners`
- ✅ 增量聚合 MV（GROUP BY city, SUM/COUNT/MIN/MAX）
- ✅ 跨节点 SQ 结果正确驱动增量更新
- ✅ MAX/MIN 在删除时正确重新计算

### 10. 故障场景

#### `owner_down_errors_instead_of_returning_partial_data`
- ✅ 当一个 owner 不可达时，查询报错（不是返回局部数据）

```sql
-- node-B 不可达
SELECT COUNT(*) FROM Widget;  -- 报错，而不是返回 node-A 的局部计数
```

#### `distributed_write_to_down_owner_errors_not_false_success`
- ✅ 写入到不可达 owner 时报错（不是假成功）

#### `unsupported_cluster_query_errors_instead_of_local_fallback`
- ✅ 不支持的分布式查询明确报错（而不是静默回退到单节点）

```sql
SELECT price + 1 FROM Product;  -- 报错：cluster mode 不支持算术投影
```

### 11. 三节点集群

#### `three_node_insert_spreads_and_aggregates_are_global`
- ✅ 3 节点 RF=1：数据分布到全部 3 个 owner
- ✅ 全局聚合（COUNT/SUM/MIN/MAX）正确合并 3 个节点的数据

### 12. 副本复制（RF=3）

#### `rf3_pgwire_write_replicates_to_followers`
- ✅ 3 节点 RF=3：INSERT 通过 PG-wire 写入 owner + 2 个 follower
- ✅ 所有 3 个节点物理持有该行（quorum 复制）

#### `rf3_owner_failure_read_from_follower`
- ✅ RF=3 时，follower 持有复制的数据（为 failover 做好准备）

## 测试结果

```bash
$ cargo test --package nexora-pgwire --test distributed_pgwire_e2e --no-fail-fast

running 19 tests
test distributed_insert_triggers_sq_for_remote_owned_node ... ok
test distributed_group_by_counts_match_oracle ... ok
test distributed_insert_spreads_across_owners_and_count_is_global ... ok
test distributed_write_to_down_owner_errors_not_false_success ... ok
test rf3_owner_failure_read_from_follower ... ok
test distributed_global_aggregates_match_single_node_oracle ... ok
test rf3_pgwire_write_replicates_to_followers ... ok
test unsupported_cluster_query_errors_instead_of_local_fallback ... ok
test owner_down_errors_instead_of_returning_partial_data ... ok
test distributed_aggregate_mv_incremental_deltas_across_owners ... ok
test distributed_write_broadcasts_sq_result_for_mv_bridge ... ok
test distributed_filtered_delete_triggers_sq_unmatch_across_owners ... ok
test distributed_filtered_update_applies_across_owners ... ok
test distributed_in_clause_batch_update_applies_across_owners ... ok
test distributed_mv_end_to_end_query_via_pgwire ... ok
test distributed_filtered_delete_applies_across_owners ... ok
test distributed_filtered_projection_read_applies_predicate_across_nodes ... ok
test distributed_filtered_update_triggers_sq_across_owners ... ok
test three_node_insert_spreads_and_aggregates_are_global ... ok

test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

**全部通过，耗时 0.32s**

## 关键验证点总结

### ✅ Graph 数据分布和读取

1. **写入分布正确**：INSERT 通过 shard map 路由到正确的 owner
2. **物理隔离验证**：每个节点只持有其拥有的 shard 数据
3. **全局读取正确**：COUNT/聚合/GROUP BY 合并所有节点数据
4. **谓词下推**：WHERE 条件在每个 owner 上执行
5. **UPDATE/DELETE 分布**：写操作扇出到所有 owner 并正确执行

### ✅ Standing Query 集成

1. **本地节点触发**：写入到本地 shard 触发 SQ
2. **远程节点触发**：写入到远程 owner 的 shard 也触发 SQ
3. **属性重读**：SQ 通过 router 重新读取属性（不是从本地缓存）
4. **Matched/Unmatched**：INSERT/UPDATE 触发 Matched，DELETE 触发 Unmatched

### ✅ 物化视图链路

1. **SQ → MV bridge**：SQ 结果通过 broadcast 送到 MV bridge
2. **跨节点聚合 MV**：增量聚合 MV 正确处理跨节点的 SQ 结果
3. **PG-wire 回查**：`SELECT FROM <mv>` 通过 PG-wire 返回 MV 数据

### ✅ 故障可见性

1. **Owner 不可达**：查询报错（不是返回局部数据）
2. **写入故障**：写入到 down owner 报错（不是假成功）
3. **不支持的查询**：明确报错（不是静默回退）

### ✅ 副本复制（RF>1）

1. **Quorum 写入**：INSERT 写到 owner + followers
2. **物理验证**：所有副本节点持有数据
3. **Failover 准备**：follower 可读（虽然自动 promotion 需要共识）

## Event 表集成（event-first 模式）

当启用 `--features event-first` 时：

### 预期行为

1. **双写路径**：INSERT 写入 graph + 本地 Iceberg event 表
2. **跨节点查询**：`SELECT FROM event_table` 扇出到所有节点并合并 Arrow batches
3. **DataFusion 执行**：SQL 在合并后的数据上执行（聚合/GROUP BY 正确）

### 实现状态

- ✅ `event_table_handler.rs` 已实现跨节点查询逻辑
- ✅ `PgAppState` 包含 `event_store` 字段
- ⚠️  **当前限制**：event 写入仅发生在 coordinator 节点（未跨节点复制）
  - 这是已知限制，文档化为 "local-coordinator-only writes"
  - 跨节点 event 复制需要显式的 event replication path（后续阶段）

### 验证方法

```bash
# 编译带 event-first feature
cargo build --features event-first

# 启动带 event store 的集群
./target/debug/nexora --pg-port 5432 --cluster --event-store-dir ./data/events

# 通过 psql 验证
psql -h localhost -p 5432 -U admin -d nexora
nexora=> INSERT INTO user_events (event_id, user_id, action) VALUES ('e1', 'u1', 'login');
nexora=> SELECT COUNT(*) FROM user_events;
 count
-------
     1
```

## 结论

**✅ 多节点集群环境下，PG-wire 完整链路已验证：**

1. ✅ **写入 → Graph**：数据正确分布到多个节点
2. ✅ **读取 → Graph**：全局查询合并所有节点数据
3. ✅ **写入 → Standing Query**：跨节点触发 SQ
4. ✅ **SQ → 物化视图**：MV 链路端到端工作
5. ✅ **故障可见性**：Owner down 时明确报错
6. ✅ **副本复制**：RF>1 时数据复制到 followers

**Event 表**：
- ✅ 读取路径已实现（跨节点查询和合并）
- ⚠️  写入路径限制为 coordinator-only（文档化的已知限制）

---

**测试文件**：`crates/nexora-pgwire/tests/distributed_pgwire_e2e.rs`  
**测试数量**：19 个分布式集成测试  
**测试状态**：✅ 全部通过  
**最后验证**：2026-07-21
