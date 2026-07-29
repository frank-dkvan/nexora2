# 分布式 RisingWave 测试指南

本文档介绍如何启动和测试分布式 RisingWave 集群（3 节点 HA 配置）。

## 快速开始

### 1. 编译项目

```bash
# 编译带 embedded 特性的版本
cargo build --release --features embedded
```

### 2. 使用测试脚本

```bash
# 启动集群
./scripts/test-distributed-risingwave.sh start

# 检查健康状态
./scripts/test-distributed-risingwave.sh health

# 运行功能测试
./scripts/test-distributed-risingwave.sh test

# 停止集群
./scripts/test-distributed-risingwave.sh stop
```

### 3. 运行演示程序

```bash
cargo run --example distributed_risingwave_demo --features embedded
```

## 集群架构

### 节点配置

- **Meta 节点**: 3 个（Raft 共识，HA）
  - Node 1: `127.0.0.1:5690` (Dashboard: 5691)
  - Node 2: `127.0.0.1:5692` (Dashboard: 5693)
  - Node 3: `127.0.0.1:5694` (Dashboard: 5695)

- **Frontend 节点**: 1 个
  - PostgreSQL 协议: `127.0.0.1:4566`

- **Compute 节点**: 1 个
  - 地址: `127.0.0.1:5688`
  - 并行度: 自动检测 CPU 核心数

### 架构图

```
┌─────────────────────────────────────────────┐
│           Nexora Application                │
│                                             │
│  ┌────────────────────────────────────────┐ │
│  │   Distributed RisingWave Cluster       │ │
│  │                                        │ │
│  │  ┌──────┐  ┌──────┐  ┌──────┐        │ │
│  │  │Meta 1│  │Meta 2│  │Meta 3│ (Raft) │ │
│  │  │:5690 │  │:5692 │  │:5694 │        │ │
│  │  └──┬───┘  └──┬───┘  └──┬───┘        │ │
│  │     └─────────┼─────────┘             │ │
│  │               │                        │ │
│  │          ┌────▼─────┐                 │ │
│  │          │ Frontend │                 │ │
│  │          │  :4566   │                 │ │
│  │          └────┬─────┘                 │ │
│  │               │                        │ │
│  │          ┌────▼─────┐                 │ │
│  │          │ Compute  │                 │ │
│  │          │  :5688   │                 │ │
│  │          └──────────┘                 │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

## 功能测试

### 1. 基础功能测试

测试创建 Source 和 Materialized View：

```bash
./scripts/test-distributed-risingwave.sh test
```

该测试会：
- 创建 Kafka Source
- 创建聚合 Materialized View
- 创建时间窗口 Materialized View
- 查询数据并验证

### 2. 手动测试

连接到 Frontend 并执行 SQL：

```bash
psql -h 127.0.0.1 -p 4566 -U root -d dev
```

示例查询：

```sql
-- 创建数据生成 Source
CREATE SOURCE user_behaviors (
    user_id VARCHAR,
    action VARCHAR,
    timestamp BIGINT
) WITH (
    connector = 'datagen',
    fields.user_id.kind = 'sequence',
    fields.user_id.start = '1',
    fields.user_id.end = '100',
    datagen.rows.per.second = '10'
) FORMAT PLAIN ENCODE JSON;

-- 创建实时统计 MV
CREATE MATERIALIZED VIEW user_action_stats AS
SELECT
    user_id,
    action,
    COUNT(*) AS action_count,
    MAX(timestamp) AS last_action_time
FROM user_behaviors
GROUP BY user_id, action;

-- 查询实时结果
SELECT * FROM user_action_stats LIMIT 10;

-- 查看所有对象
SHOW SOURCES;
SHOW MATERIALIZED VIEWS;
```

### 3. 高可用测试

测试 Meta Leader 故障转移（未完整实现）：

```bash
./scripts/test-distributed-risingwave.sh ha
```

### 4. 性能测试

测试插入和查询性能：

```bash
./scripts/test-distributed-risingwave.sh perf
```

## 日志和调试

### 查看启动日志

```bash
tail -f /tmp/nexora-risingwave.log
```

### 启用调试日志

```bash
RUST_LOG=debug ./scripts/test-distributed-risingwave.sh start
```

或者在 Rust 代码中：

```bash
RUST_LOG=nexora_risingwave=trace,nexora_app=debug cargo run --features embedded
```

### 检查进程状态

```bash
# 查看所有 RisingWave 进程
ps aux | grep risingwave

# 查看端口占用
lsof -i :5690
lsof -i :4566
```

## 常见问题

### Q1: 集群启动超时

**问题**: 等待 60 秒后仍未启动

**解决方案**:
1. 检查端口是否被占用
2. 查看日志文件 `/tmp/nexora-risingwave.log`
3. 确认 RisingWave 二进制文件存在
4. 尝试手动启动单个节点测试

### Q2: 无法连接 Frontend

**问题**: `psql` 连接失败

**解决方案**:
```bash
# 检查 Frontend 是否运行
nc -zv 127.0.0.1 4566

# 查看 Frontend 日志
tail -f /tmp/nexora-risingwave.log | grep frontend

# 重启集群
./scripts/test-distributed-risingwave.sh restart
```

### Q3: Meta Leader 选举失败

**问题**: 日志显示 "Meta leader election timeout"

**解决方案**:
1. 确保所有 3 个 Meta 节点都启动
2. 检查节点间网络连通性
3. 增加启动超时时间（修改配置）
4. 检查 Raft 日志

### Q4: 找不到 RisingWave 二进制

**问题**: "RisingWave binary not found"

**解决方案**:
```bash
# 方案 1: 设置环境变量
export RISINGWAVE_BIN=/path/to/risingwave

# 方案 2: 从源码编译
cd vendor/risingwave
cargo build --release
cp target/release/risingwave ../../bin/risingwave-embedded

# 方案 3: 使用预编译版本
# 下载并放到 PATH 中
```

## 性能优化

### 调整并行度

```rust
ComputeNodeConfig {
    listen_addr: "127.0.0.1:5688".to_string(),
    parallelism: 8,  // 增加并行度
}
```

### 增加 Compute 节点

```rust
compute_nodes: vec![
    ComputeNodeConfig {
        listen_addr: "127.0.0.1:5688".to_string(),
        parallelism: 4,
    },
    ComputeNodeConfig {
        listen_addr: "127.0.0.1:5689".to_string(),
        parallelism: 4,
    },
],
```

### 使用持久化存储

当前配置使用内存后端 (`--backend mem`)，生产环境应使用 etcd：

```rust
// 修改 start_meta_node 函数
cmd.arg("--backend").arg("etcd")
    .arg("--etcd-endpoints").arg("http://localhost:2379");
```

## 下一步

### 集成到 Nexora 主程序

1. **CLI 参数支持**:
   ```bash
   nexora-app --risingwave-distributed \
       --risingwave-meta-nodes "1:127.0.0.1:5690,2:127.0.0.1:5692,3:127.0.0.1:5694"
   ```

2. **配置文件支持** (`nexora.toml`):
   ```toml
   [risingwave]
   enabled = true
   distributed = true

   [[risingwave.meta_nodes]]
   node_id = 1
   listen_addr = "127.0.0.1:5690"
   advertise_addr = "127.0.0.1:5690"
   dashboard_addr = "127.0.0.1:5691"

   [[risingwave.meta_nodes]]
   node_id = 2
   listen_addr = "127.0.0.1:5692"
   advertise_addr = "127.0.0.1:5692"
   dashboard_addr = "127.0.0.1:5693"

   [[risingwave.meta_nodes]]
   node_id = 3
   listen_addr = "127.0.0.1:5694"
   advertise_addr = "127.0.0.1:5694"
   dashboard_addr = "127.0.0.1:5695"
   ```

3. **API 集成**:
   - HTTP endpoints for cluster management
   - Health check API
   - Metrics export

### 生产部署

参考 `docs/RISINGWAVE_PHASE8_DISTRIBUTED_EMBEDDED.md` 了解完整的生产部署方案。

## 参考资料

- [RisingWave 官方文档](https://docs.risingwave.com)
- [Nexora RisingWave 集成计划](../docs/RISINGWAVE_INTEGRATION_PLAN.md)
- [Phase 8 实现报告](../docs/RISINGWAVE_PHASE8_REPORT.md)
- [分布式架构设计](../docs/RISINGWAVE_PHASE8_DISTRIBUTED_EMBEDDED.md)
