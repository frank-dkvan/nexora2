# Phase 8 快速参考 - 分布式嵌入式 RisingWave

## 一键启动

```bash
# 使用示例配置启动 3 节点集群
cargo run --release --features risingwave,embedded -- \
  --config nexora-cluster.toml.example
```

## 关键命令

### 编译

```bash
# 开发编译
cargo build --features risingwave,embedded

# 生产编译
cargo build --release --features risingwave,embedded

# 仅检查（快速）
cargo check --features risingwave,embedded
```

### 测试

```bash
# 单元测试
cargo test -p nexora-risingwave --features embedded --lib

# 集成测试（需 RisingWave 二进制）
cargo test -p nexora-risingwave --features embedded --test distributed_integration -- --ignored

# 所有测试
cargo test --workspace --features risingwave,embedded
```

### 运行

```bash
# 方式 1: CLI 参数
cargo run --release --features risingwave,embedded -- \
  --enable-risingwave \
  --enable-embedded-risingwave \
  --risingwave-cluster-mode

# 方式 2: 配置文件
cargo run --release --features risingwave,embedded -- \
  --config nexora-cluster.toml.example

# 方式 3: 环境变量 + 配置
RUST_LOG=info cargo run --release --features risingwave,embedded -- \
  --config /path/to/custom-config.toml
```

## 配置模板

### 最小配置

```toml
[risingwave]
enabled = true
embedded = true
cluster_mode = true

# 使用默认端口和 3 节点配置
```

### 完整配置

```toml
[risingwave]
enabled = true
embedded = true
cluster_mode = true
data_dir = "./nexora-data/risingwave-cluster"
startup_timeout_secs = 90
shutdown_timeout_secs = 30

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

frontend_addr = "127.0.0.1:4566"

[[risingwave.compute_nodes]]
listen_addr = "127.0.0.1:5688"
parallelism = 4
```

## 端口分配

| 组件 | 默认端口 | 用途 |
|------|---------|------|
| Nexora API | 8080 | HTTP API |
| Meta-1 RPC | 5690 | Raft 通信 |
| Meta-1 Dashboard | 5691 | Web UI |
| Meta-2 RPC | 5692 | Raft 通信 |
| Meta-2 Dashboard | 5693 | Web UI |
| Meta-3 RPC | 5694 | Raft 通信 |
| Meta-3 Dashboard | 5695 | Web UI |
| Frontend | 4566 | PostgreSQL 协议 |
| Compute | 5688 | 内部 RPC |

## 故障排查

### 检查进程

```bash
# 查看所有 RisingWave 进程
ps aux | grep risingwave

# 应该看到 5 个进程：
# - 3 个 meta-node
# - 1 个 frontend-node
# - 1 个 compute-node
```

### 检查 Meta Leader

```bash
# 方式 1: Dashboard API
curl http://127.0.0.1:5691/cluster_info

# 方式 2: 查看日志
tail -f nexora-data/risingwave-cluster/meta-*/meta.log | grep -i leader
```

### 连接测试

```bash
# PostgreSQL 客户端
psql -h 127.0.0.1 -p 4566 -d dev -U root

# 简单查询
psql -h 127.0.0.1 -p 4566 -d dev -U root -c "SELECT 1;"
```

### 常见错误

**错误**: `risingwave: command not found`
```bash
# 解决：设置 binary_path 或确保 risingwave 在 PATH
export PATH=$PATH:/path/to/risingwave/bin
```

**错误**: `Address already in use`
```bash
# 检查端口占用
lsof -i :5690
lsof -i :4566

# 杀死占用进程
kill -9 <pid>
```

**错误**: `Meta leader not elected within timeout`
```bash
# 增加启动超时
[risingwave]
startup_timeout_secs = 120  # 默认 90

# 或检查网络连接
ping 127.0.0.1
```

## 健康检查

### 手动检查

```bash
# 1. Meta 节点健康
curl -s http://127.0.0.1:5691/metrics | grep -i health

# 2. Frontend 健康
pg_isready -h 127.0.0.1 -p 4566

# 3. Nexora API 健康
curl http://localhost:8080/api/health
```

### 程序化检查

```rust
// 在代码中获取集群状态
let health = distributed_risingwave.monitor_health().await?;
println!("Leader: {:?}", health.leader_node_id);
println!("Meta nodes: {}", health.meta_nodes.len());
```

## 故障模拟

### Kill Meta Leader

```bash
# 1. 找到 Leader PID
ps aux | grep "meta-node" | grep "5690"  # 假设 Meta-1 是 Leader

# 2. Kill 进程
kill -9 <meta-1-pid>

# 3. 观察重新选举（应 <10 秒）
tail -f nexora-data/risingwave-cluster/meta-*/meta.log | grep -i "elected"
```

### Kill Frontend

```bash
# Frontend 故障会导致查询失败
kill -9 <frontend-pid>

# 当前需要重启 Nexora 恢复
# TODO: 未来版本支持自动重启
```

## 性能调优

### 增加 Compute 节点

```toml
# 添加第二个 Compute 节点
[[risingwave.compute_nodes]]
listen_addr = "127.0.0.1:5689"
parallelism = 4
```

### 调整并行度

```toml
[[risingwave.compute_nodes]]
listen_addr = "127.0.0.1:5688"
parallelism = 8  # 增加到 8 (默认 4)
```

### 减少启动时间

```toml
[risingwave]
startup_timeout_secs = 60  # 从 90 减少到 60
```

## 日志位置

```
nexora-data/
└── risingwave-cluster/
    ├── meta-1/
    │   └── meta.log
    ├── meta-2/
    │   └── meta.log
    ├── meta-3/
    │   └── meta.log
    ├── frontend/
    │   └── frontend.log
    └── compute-1/
        └── compute.log
```

## 数据目录结构

```
nexora-data/
└── risingwave-cluster/
    ├── meta-1/
    │   ├── db/           # Meta 状态
    │   └── meta.log
    ├── meta-2/
    │   ├── db/
    │   └── meta.log
    ├── meta-3/
    │   ├── db/
    │   └── meta.log
    ├── frontend/
    │   └── frontend.log
    └── compute-1/
        ├── state/        # Compute 状态
        └── compute.log
```

## 清理命令

```bash
# 停止 Nexora (Ctrl+C)

# 清理数据
rm -rf nexora-data/risingwave-cluster

# 清理所有残留进程
pkill -9 risingwave
```

## 开发技巧

### 快速迭代

```bash
# 1. 开发构建（快）
cargo build --features risingwave,embedded

# 2. 运行
./target/debug/nexora --config nexora-cluster.toml.example

# 3. 修改代码后快速重新编译
cargo build --features risingwave,embedded -p nexora-risingwave
cargo build --features risingwave,embedded -p nexora-app
```

### Debug 日志

```bash
# 全局 debug
RUST_LOG=debug cargo run --features risingwave,embedded

# 仅 RisingWave 模块
RUST_LOG=nexora_risingwave=trace cargo run --features risingwave,embedded

# 多模块
RUST_LOG=nexora_risingwave=debug,nexora_app=info cargo run --features risingwave,embedded
```

## API 端点（规划）

```bash
# 获取集群状态
GET /api/risingwave/cluster
{
  "cluster_mode": true,
  "leader_node_id": 1,
  "meta_nodes": [...],
  "frontend": {...},
  "compute_nodes": [...]
}

# 获取单节点健康（Phase 7）
GET /api/health/risingwave
{
  "status": "running",
  "meta_addr": "127.0.0.1:5690",
  "frontend_addr": "127.0.0.1:4566"
}
```

## 相关文件

- **实现**: `crates/nexora-risingwave/src/distributed.rs`
- **集成**: `crates/nexora-app/src/main.rs:1747-1988`
- **测试**: `crates/nexora-risingwave/tests/distributed_integration.rs`
- **配置**: `nexora-cluster.toml.example`
- **文档**: `docs/RISINGWAVE_PHASE8_DONE.md`

---

**版本**: Phase 8 MVP  
**更新**: 2026-07-27
