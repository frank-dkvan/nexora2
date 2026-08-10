# 🚀 Nexora 2.0 - 快速入门指南

欢迎使用 Nexora 2.0！本指南将帮助您在 5 分钟内启动并运行航空货运站演示。

---

## 📋 前提条件

- **Rust**: nightly-2026-06-11 或更高版本
- **操作系统**: macOS, Linux, 或 Windows (WSL)
- **内存**: 至少 4 GB 可用内存
- **磁盘**: 至少 2 GB 可用空间

---

## 🚀 三步启动

### 方式 1: 一键启动（推荐）

```bash
# 1. 设置 Rust 工具链
rustup default nightly-2026-06-11

# 2. 一键启动（自动完成：构建 → 启动 → 导入数据 → 演示查询）
./scripts/quick-start.sh
```

完成！服务器现在运行在 `http://127.0.0.1:8080`

### 方式 2: 分步执行

```bash
# 1. 构建项目
cargo build --release -p nexora-app

# 2. 启动服务器
./scripts/start-demo.sh

# 3. 在新终端窗口导入数据
./scripts/load-demo-data.sh

# 4. 运行演示查询
./scripts/demo-queries.sh
```

---

## 🎯 快速测试

### 健康检查

```bash
curl http://127.0.0.1:8080/health
```

预期输出：`{"status":"healthy"}`

### 简单查询

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (a:Airport) RETURN a.code, a.name LIMIT 3"}'
```

---

## 📊 演示场景

演示包含完整的航空货运业务数据：

- **8个国际机场**: 北京、上海、广州、香港、新加坡、洛杉矶、纽约、法兰克福
- **12条国际航线**: 覆盖亚太、北美、欧洲
- **5种货物类型**: 电子产品、医药品、生鲜食品、机械设备、纺织品
- **实时货运单**: 展示从仓储到在途的完整流程

---

## 🔍 常用查询示例

### 1. 查看所有机场

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (a:Airport) RETURN a.code, a.name ORDER BY a.code"}'
```

### 2. 查找航线（北京到纽约）

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH path = (o:Airport {code: \"PEK\"})-[:ROUTE*1..2]->(d:Airport {code: \"JFK\"}) RETURN [n in nodes(path) | n.code] AS route LIMIT 3"}'
```

### 3. 查询在途货物

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (s:Shipment) WHERE s.status = \"在途\" RETURN s.awb_number, s.weight_kg"}'
```

### 4. 高价值货物追踪

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (s:Shipment) WHERE s.declared_value_usd > 100000 RETURN s.awb_number, s.declared_value_usd ORDER BY s.declared_value_usd DESC"}'
```

---

## 📚 更多示例

运行预定义的 15 个演示查询：

```bash
./scripts/demo-queries.sh
```

---

## 🛑 停止服务器

```bash
./scripts/stop-demo.sh
```

---

## 📖 进阶学习

### 完整文档

- **航空货运演示**: `examples/AIR_CARGO_DEMO_README.md`
- **查询示例**: `examples/air-cargo-queries.cypher`
- **生产就绪报告**: `docs/PRODUCTION_READINESS_FINAL_REPORT.md`
- **灾难恢复手册**: `docs/DISASTER_RECOVERY_MANUAL.md`
- **负载测试报告**: `docs/LOAD_TEST_REPORT.md`

### API 端点

- **健康检查**: `http://127.0.0.1:8080/health`
- **查询 API**: `http://127.0.0.1:8080/api/query`
- **指标监控**: `http://127.0.0.1:8080/metrics`

---

## 🔧 故障排除

### 端口被占用

```bash
# 检查端口
lsof -i :8080

# 更改配置中的端口
vim data/demo/nexora-demo.toml
# [server]
# bind_address = "127.0.0.1:8081"
```

### 构建失败

```bash
# 确认工具链
rustc --version

# 切换到 nightly
rustup default nightly-2026-06-11

# 清理并重新构建
cargo clean
cargo build --release -p nexora-app
```

### 查询超时

调整配置文件 `data/demo/nexora-demo.toml`:

```toml
[query]
max_execution_time_secs = 60  # 增加到 60 秒
```

---

## 🌟 功能亮点

### 生产级保护

- ✅ **断路器**: 外部服务故障自动隔离
- ✅ **智能重试**: 指数退避 + 抖动
- ✅ **API 限流**: 全局 100K req/s，单客户端 1K req/s
- ✅ **资源限制**: 查询超时、内存保护、结果集限制

### 安全合规

- ✅ **CVE 修复**: 34 个漏洞全部解决
- ✅ **SOC 2 合规**: 无关键安全问题

### 高可用

- ✅ **Raft 共识**: 多节点强一致性
- ✅ **自动恢复**: RTO 30 分钟, RPO 1 分钟
- ✅ **负载测试**: 99.97% 可用性（72 小时）

---

## 🦀 技术栈

- **语言**: Rust (nightly-2026-06-11)
- **框架**: Axum + Tokio
- **存储**: RocksDB + Iceberg
- **共识**: Raft
- **查询**: Cypher (读 + 写)

---

**开始探索 Nexora 2.0 吧！** 🚀

如有问题，请查看 `examples/AIR_CARGO_DEMO_README.md` 获取详细文档。
