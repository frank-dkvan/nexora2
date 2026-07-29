# 分布式 RisingWave 测试使用指南

这是 Nexora 2.0 集成的 3 节点 HA RisingWave 集群，可以在单机上运行分布式模式进行功能测试。

## 🚀 一键启动

### 最简单方式 - 运行演示

```bash
./scripts/demo-distributed-risingwave.sh
```

这个脚本会：
- ✅ 自动编译项目（如果需要）
- ✅ 启动 3 节点 Meta 集群
- ✅ 启动 Frontend 和 Compute 节点
- ✅ 创建测试数据源和 Materialized View
- ✅ 执行查询并展示结果
- ✅ 监控集群健康状态
- ✅ 优雅关闭所有组件

**预计运行时间**: 约 30-60 秒

## 📋 三种测试方式

### 方式 1: 自动化演示（推荐）

```bash
# 运行完整演示
./scripts/demo-distributed-risingwave.sh
```

### 方式 2: 使用测试脚本

```bash
# 启动集群（后台运行）
./scripts/test-distributed-risingwave.sh start

# 检查健康状态
./scripts/test-distributed-risingwave.sh health

# 运行功能测试
./scripts/test-distributed-risingwave.sh test

# 停止集群
./scripts/test-distributed-risingwave.sh stop
```

### 方式 3: 手动命令

```bash
# 1. 编译
cargo build --release --features embedded -p nexora-risingwave

# 2. 运行演示程序
cargo run --release --features embedded \
    -p nexora-risingwave \
    --example distributed_risingwave_demo

# 3. 或手动连接已启动的集群
psql -h 127.0.0.1 -p 4566 -U root -d dev
```

## 🎯 核心功能测试

演示程序会自动测试以下功能：

### 1. 创建实时数据源

```sql
CREATE SOURCE user_events (
    user_id VARCHAR,
    event_type VARCHAR,
    timestamp BIGINT,
    properties JSONB
) WITH (
    connector = 'datagen',
    datagen.rows.per.second = '10'
) FORMAT PLAIN ENCODE JSON;
```

### 2. 创建 Materialized View

```sql
CREATE MATERIALIZED VIEW user_event_counts AS
SELECT
    user_id,
    event_type,
    COUNT(*) AS event_count,
    MAX(timestamp) AS latest_timestamp
FROM user_events
GROUP BY user_id, event_type;
```

### 3. 实时查询

```sql
SELECT * FROM user_event_counts LIMIT 10;
```

### 4. 复杂聚合

```sql
SELECT
    event_type,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events
FROM user_event_counts
GROUP BY event_type;
```

### 5. 集群健康监控

自动检测：
- Meta Leader 节点
- 所有节点运行状态
- Frontend 可用性
- Compute 节点状态

## 📊 集群架构

```
┌─────────────────────────────────────┐
│  Distributed RisingWave Cluster     │
│                                     │
│  ┌──────┐ ┌──────┐ ┌──────┐       │
│  │Meta 1│ │Meta 2│ │Meta 3│       │
│  │:5690 │ │:5692 │ │:5694 │ Raft  │
│  └──┬───┘ └──┬───┘ └──┬───┘       │
│     └────────┼────────┘             │
│              │                      │
│         ┌────▼────┐                │
│         │Frontend │                │
│         │  :4566  │ PostgreSQL     │
│         └────┬────┘                │
│              │                      │
│         ┌────▼────┐                │
│         │ Compute │                │
│         │  :5688  │                │
│         └─────────┘                │
└─────────────────────────────────────┘
```

## 🔍 预期输出

运行 `./scripts/demo-distributed-risingwave.sh` 后，你会看到：

```
╔═══════════════════════════════════════╗
║  Nexora 2.0 - 分布式 RisingWave 集成  ║
╚═══════════════════════════════════════╝

功能特性:
  ✅ 3 节点 Meta 集群 (Raft HA)
  ✅ 1 个 Frontend 节点 (PostgreSQL 协议)
  ✅ 动态 Compute 节点 (自动并行)
  ✅ 实时流式 SQL 处理
  ✅ 集群健康监控
  ✅ 自动生命周期管理

=========================================

配置:
  - Meta 节点: 3 个 (端口 5690, 5692, 5694)
  - Frontend: 1 个 (端口 4566)
  - Compute: 1 个 (端口 5688, 8 并发)

启动集群...
✓ 集群启动成功

连接 Frontend...
✓ 已连接到 Frontend

=========================================
  测试 1: 创建 Source
=========================================
✓ Source 创建成功

=========================================
  测试 2: 创建 Materialized View
=========================================
✓ Materialized View 创建成功

等待数据生成 (5 秒)...

=========================================
  测试 3: 查询 Materialized View
=========================================

Top 10 用户事件统计:
User ID    Event Type      Count       
----------------------------------------
1          abc123          3           
2          xyz456          5           
...

=========================================
  测试 5: 集群健康状态
=========================================

Meta 节点:
  - Node 1 (127.0.0.1:5690): 运行中 [Leader]
  - Node 2 (127.0.0.1:5692): 运行中
  - Node 3 (127.0.0.1:5694): 运行中

Frontend:
  - 127.0.0.1:4566: 运行中

Compute 节点:
  - Node 0 (127.0.0.1:5688): 运行中

当前 Meta Leader: Node 1

=========================================
  演示完成！
=========================================
```

## ⚙️ 系统要求

- **Rust**: 1.75+
- **内存**: 至少 2.2GB 可用
- **RisingWave 二进制**: 自动查找或手动设置

## 🔧 故障排查

### 问题 1: 找不到 RisingWave 二进制

```bash
# 设置环境变量
export RISINGWAVE_BIN=/path/to/risingwave

# 或从源码编译
cd vendor/risingwave
cargo build --release
cp target/release/risingwave ../../bin/risingwave-embedded
```

### 问题 2: 端口被占用

```bash
# 检查端口
lsof -i :5690
lsof -i :4566

# 杀死占用进程
kill -9 <PID>
```

### 问题 3: 启动超时

```bash
# 启用调试日志
RUST_LOG=debug ./scripts/demo-distributed-risingwave.sh
```

### 问题 4: 连接失败

```bash
# 验证 Frontend 运行
nc -zv 127.0.0.1 4566

# 查看日志
tail -f /tmp/nexora-risingwave.log
```

## 📚 更多资源

- **快速入门**: [docs/DISTRIBUTED_RISINGWAVE_QUICKSTART.md](./DISTRIBUTED_RISINGWAVE_QUICKSTART.md)
- **详细测试指南**: [docs/DISTRIBUTED_RISINGWAVE_TESTING_GUIDE.md](./DISTRIBUTED_RISINGWAVE_TESTING_GUIDE.md)
- **完整总结**: [docs/DISTRIBUTED_RISINGWAVE_COMPLETE.md](./DISTRIBUTED_RISINGWAVE_COMPLETE.md)
- **RisingWave 官方文档**: https://docs.risingwave.com

## 🎉 接下来做什么？

### 1. 尝试自定义查询

```bash
# 连接到集群
psql -h 127.0.0.1 -p 4566 -U root -d dev

-- 创建你自己的 Source
CREATE SOURCE my_source (...) WITH (...);

-- 创建你自己的 Materialized View
CREATE MATERIALIZED VIEW my_view AS SELECT ...;
```

### 2. 测试高可用性

```bash
# 运行 HA 测试（模拟节点故障）
./scripts/test-distributed-risingwave.sh ha
```

### 3. 性能测试

```bash
# 运行性能测试
./scripts/test-distributed-risingwave.sh perf
```

### 4. 集成到你的应用

参考 `examples/distributed_risingwave_demo.rs` 了解如何在代码中使用：

```rust
use nexora_risingwave::distributed::{
    DistributedEmbeddedRisingWave, DistributedConfig
};

let cluster = DistributedEmbeddedRisingWave::start(config).await?;
// ... 使用集群
cluster.shutdown().await?;
```

---

**祝测试愉快！** 🎉

如有问题，请查看详细文档或提交 Issue。
