# PG-Wire 多节点集群完整链路测试总结

## 测试结果

✅ **所有测试通过（19/19，耗时 0.30s）**

## 验证的功能

### ✅ Graph 数据完整链路

| 序号 | 功能 | 测试方法 | 结果 |
|-----|------|---------|------|
| 1 | **数据写入分布** | INSERT 40行 → 验证物理分布 | ✅ 正确分布到 node-a 和 node-b |
| 2 | **全局查询** | SELECT COUNT(*) | ✅ 返回全局总数（非局部） |
| 3 | **聚合函数** | SUM/MIN/MAX/AVG | ✅ 跨节点正确合并 |
| 4 | **分组聚合** | GROUP BY city | ✅ 每组合并全局结果 |
| 5 | **过滤查询** | WHERE age > 29 | ✅ 谓词在各节点执行 |
| 6 | **分布式更新** | UPDATE ... WHERE | ✅ 扇出到所有节点 |
| 7 | **分布式删除** | DELETE ... WHERE | ✅ 各节点正确删除 |
| 8 | **批量操作** | WHERE id IN (...) | ✅ IN子句跨节点工作 |
| 9 | **Standing Query** | INSERT触发SQ | ✅ 远程节点也触发 |
| 10 | **物化视图** | 完整MV链路 | ✅ 端到端工作 |
| 11 | **故障处理** | Owner down | ✅ 明确报错（非局部数据） |
| 12 | **3节点集群** | 3节点RF=1 | ✅ 数据分布到3个节点 |
| 13 | **副本复制** | 3节点RF=3 | ✅ 数据复制到所有副本 |

### Event 表链路

| 功能 | 状态 | 说明 |
|------|------|------|
| **跨节点读取** | ✅ 已实现 | 扇出查询 → 合并 Arrow batches → DataFusion 执行 |
| **写入** | ⚠️ Coordinator-only | 当前只写到协调节点，已知限制 |

## 典型测试案例

### 案例1：数据分布验证

```sql
-- 写入40个产品
INSERT INTO Product (id, name, price) VALUES ('prod-0', 'p0', 0);
-- ... 40 rows ...

-- 全局查询
SELECT COUNT(*) FROM Product;
-- 结果: 40（来自两个节点）
```

**验证点**：
- ✅ node-a 持有部分ID（例如：20个）
- ✅ node-b 持有其余ID（例如：20个）  
- ✅ 全局COUNT = 40（不是单节点的20）

### 案例2：跨节点聚合

```sql
-- 插入测试数据
INSERT INTO Metric (id, val) VALUES ('m-0', 10), ('m-1', 20), ...;

-- 全局聚合
SELECT SUM(val), MIN(val), MAX(val), AVG(val) FROM Metric;
```

**结果**：与单节点oracle计算结果完全一致

### 案例3：分布式UPDATE

```sql
-- 更新匹配的行
UPDATE Person SET vip = true WHERE age > 40;
```

**验证**：
- ✅ 更新操作扇出到node-a和node-b
- ✅ 每个节点只更新其拥有的匹配行
- ✅ 物理验证：vip字段正确设置

### 案例4：Standing Query集成

```sql
-- 插入触发SQ
INSERT INTO Product (id, name, price) VALUES ('remote-id', 'gpu', 1200);
-- SQ规则: price > 500
```

**验证**：
- ✅ 即使节点在remote owner，SQ也触发
- ✅ SQ通过router重新读取属性
- ✅ match_count正确增加

### 案例5：物化视图链路

```sql
-- 更新数据
UPDATE Person SET vip = true WHERE id IN ('mv-0', 'mv-1', ...);

-- 查询MV
SELECT * FROM vip_members;
```

**链路**：分布式写入 → SQ触发 → MV bridge → 通过PG-wire查询MV  
**结果**：✅ 返回正确的MV行

## 测试覆盖

- **节点配置**：2节点 / 3节点
- **分片数**：8个shard
- **副本因子**：RF=1 / RF=3
- **传输协议**：真实TCP传输（非mock）
- **客户端**：tokio-postgres
- **路由模式**：HybridRouter (no-local)

## 测试执行

```bash
# 方法1：运行完整测试套件
cargo test --package nexora-pgwire --test distributed_pgwire_e2e

# 方法2：使用验证脚本
./scripts/verify_pgwire_cluster.sh
```

## 文档

- 测试源码：`crates/nexora-pgwire/tests/distributed_pgwire_e2e.rs` (2183行)
- 详细报告：`docs/testing/DISTRIBUTED_PGWIRE_TEST_REPORT.md`
- 验证脚本：`scripts/verify_pgwire_cluster.sh`

## 结论

✅ **多节点集群环境下，从pg-wire写数据，graph能收到数据并正确分布到多个节点，跨节点读取正确合并结果。**

✅ **Event表读取路径已实现，写入路径为coordinator-only（已知限制）。**

**测试通过率**：100% (19/19)  
**验证日期**：2026-07-21
