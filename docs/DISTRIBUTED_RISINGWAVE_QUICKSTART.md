# 分布式 RisingWave 快速测试指南

## 一键启动测试

### 方式 1: 运行演示程序（推荐）

```bash
# 编译并运行完整演示
cargo run --release --features embedded \
    -p nexora-risingwave \
    --example distributed_risingwave_demo
```

演示程序会自动：
1. ✅ 启动 3 节点 Meta 集群
2. ✅ 启动 Frontend 和 Compute 节点
3. ✅ 创建测试数据源
4. ✅ 创建 Materialized View
5. ✅ 执行查询并展示结果
6. ✅ 监控集群健康状态
7. ✅ 优雅关闭集群

### 方式 2: 使用测试脚本

```bash
# 启动集群
./scripts/test-distributed-risingwave.sh start

# 运行测试
./scripts/test-distributed-risingwave.sh test

# 检查健康
./scripts/test-distributed-risingwave.sh health

# 停止集群
./scripts/test-distributed-risingwave.sh stop
```

### 方式 3: 手动测试

```bash
# 1. 启动集群（后台）
cargo run --release --features embedded \
    -p nexora-risingwave \
    --example distributed_risingwave_demo &

# 2. 等待启动（约 10 秒）
sleep 10

# 3. 连接并测试
psql -h 127.0.0.1 -p 4566 -U root -d dev << 'SQL'
-- 创建数据源
CREATE SOURCE events (
    id VARCHAR,
    type VARCHAR,
    timestamp BIGINT
) WITH (
    connector = 'datagen',
    datagen.rows.per.second = '10'
) FORMAT PLAIN ENCODE JSON;

-- 创建实时统计
CREATE MATERIALIZED VIEW event_stats AS
SELECT type, COUNT(*) AS count
FROM events
GROUP BY type;

-- 查询结果
SELECT * FROM event_stats;
SQL
```

## 预期输出

### 演示程序输出示例

```
========================================
  分布式 RisingWave 集群演示
========================================

配置:
  - Meta 节点: 3 个 (端口 5690, 5692, 5694)
  - Frontend: 1 个 (端口 4566)
  - Compute: 1 个 (端口 5688, 8 并发)

启动集群...
✓ 集群启动成功

连接 Frontend...
✓ 已连接到 Frontend

========================================
  测试 1: 创建 Source
========================================
✓ Source 创建成功

========================================
  测试 2: 创建 Materialized View
========================================
✓ Materialized View 创建成功

等待数据生成 (5 秒)...

========================================
  测试 3: 查询 Materialized View
========================================

Top 10 用户事件统计:
User ID    Event Type      Count       
----------------------------------------
1          abc123          3           
2          xyz456          5           
...

========================================
  测试 5: 集群健康状态
========================================

Meta 节点:
  - Node 1 (127.0.0.1:5690): 运行中 [Leader]
  - Node 2 (127.0.0.1:5692): 运行中
  - Node 3 (127.0.0.1:5694): 运行中

Frontend:
  - 127.0.0.1:4566: 运行中

Compute 节点:
  - Node 0 (127.0.0.1:5688): 运行中

当前 Meta Leader: Node 1

========================================
  演示完成！
========================================
```

## 系统要求

- ✅ Rust 1.75+
- ✅ 至少 2.2GB 可用内存
- ✅ RisingWave 二进制（自动查找或手动设置）

## 故障排查

**无法找到 RisingWave 二进制**:
```bash
export RISINGWAVE_BIN=/path/to/risingwave
```

**端口被占用**:
```bash
# 检查
lsof -i :5690

# 修改配置（编辑 distributed.rs 中的端口）
```

**启动超时**:
```bash
# 增加日志级别
RUST_LOG=debug cargo run --example distributed_risingwave_demo
```

## 下一步

- 📖 阅读详细文档: `docs/DISTRIBUTED_RISINGWAVE_TESTING_GUIDE.md`
- 🔧 自定义配置: 修改 `examples/distributed_risingwave_demo.rs`
- 🚀 集成到主程序: 参考 `docs/DISTRIBUTED_RISINGWAVE_COMPLETE.md`
