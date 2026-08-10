# 🎯 Nexora 2.0 - 航空货运站演示指南

本指南将帮助您在本地测试 Nexora 2.0 的完整功能，包括航空货运站业务场景演示。

---

## 📦 准备工作

### 1. 确认环境

```bash
# 检查 Rust 工具链
rustc --version
# 期望: rustc 1.98.0-nightly (485ec3fbc 2026-06-10)

cargo --version  
# 期望: cargo 1.98.0-nightly

# 如果版本不对，设置环境变量
export PATH="$HOME/.cargo/bin:$PATH"
```

### 2. 构建项目

```bash
# 方式 1: 使用发布模式构建（推荐，性能最佳）
cargo build --release -p nexora-app

# 方式 2: 开发模式构建（编译快但性能较低）
cargo build -p nexora-app
```

构建时间：
- 首次构建: ~5-10 分钟
- 增量构建: ~30 秒

---

## 🚀 启动服务器

### 方式 1: 使用自动化脚本（推荐）

```bash
# 一键启动整个演示（构建 + 启动 + 数据导入 + 查询演示）
./scripts/quick-start.sh
```

### 方式 2: 手动分步启动

```bash
# 1. 启动服务器
./scripts/start-demo.sh

# 2. 等待服务器启动（约 2-3 秒）
# 查看日志
tail -f data/demo/nexora-demo.log

# 3. 在新终端窗口导入演示数据
./scripts/load-demo-data.sh

# 4. 运行演示查询
./scripts/demo-queries.sh
```

### 方式 3: 直接运行二进制

```bash
# 使用 release 构建
./target/release/nexora-app --config data/demo/nexora-demo.toml

# 或使用 debug 构建
./target/debug/nexora-app --config data/demo/nexora-demo.toml
```

---

## ✅ 验证服务器

### 健康检查

```bash
curl http://127.0.0.1:8080/health
```

**期望输出**:
```json
{"status":"healthy"}
```

### 查看指标

```bash
curl http://127.0.0.1:8080/metrics
```

**期望输出**:
```
# 各种 Prometheus 格式指标
nexora_queries_total{...}
nexora_query_duration_seconds{...}
```

---

## 📊 航空货运站演示

### 数据概览

演示包含完整的国际航空货运网络：

**机场（8个）**
- PEK - 北京首都国际机场
- PVG - 上海浦东国际机场  
- CAN - 广州白云国际机场
- HKG - 香港国际机场
- SIN - 新加坡樟宜机场
- LAX - 洛杉矶国际机场
- JFK - 纽约肯尼迪机场
- FRA - 法兰克福机场

**航线（12条）**
- 亚太区域: PEK-PVG, PVG-HKG, HKG-SIN, PEK-CAN
- 跨太平洋: PVG-LAX, HKG-LAX, SIN-LAX
- 北美内部: LAX-JFK
- 跨大西洋: JFK-FRA, LAX-FRA
- 亚欧: PEK-FRA, SIN-FRA

**货运站（5个）**
- 浦东机场货运站（100万吨/年）
- 香港机场货运站（120万吨/年）
- 洛杉矶货运中心（80万吨/年）
- 纽约肯尼迪货运中心（60万吨/年）
- 法兰克福货运枢纽（90万吨/年）

**货物类型（5种）**
- 电子产品（需防静电）
- 医药品（需温控2-8°C）
- 生鲜食品（需冷链0-4°C）
- 机械设备（需防震）
- 纺织品（常温）

**运单（4个）**
- 2个已交付
- 1个在途
- 1个在仓储

---

## 🔍 演示查询

### 1. 基础查询 - 查看所有机场

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (a:Airport) RETURN a.code, a.name, a.city ORDER BY a.code"
  }'
```

**期望结果**: 返回 8 个机场的代码、名称和城市

### 2. 路径查询 - 上海到洛杉矶的航线

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH path = (origin:Airport {code: \"PVG\"})-[:ROUTE*1..2]->(dest:Airport {code: \"LAX\"}) RETURN [n in nodes(path) | n.code] AS route LIMIT 3"
  }'
```

**期望结果**: 
- 直飞路线: PVG → LAX
- 中转路线（如果存在）

### 3. 业务查询 - 所有在途货物

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType) WHERE s.status = \"在途\" RETURN s.awb_number AS 运单号, ct.category AS 货物类型, s.weight_kg AS 重量, s.declared_value_usd AS 申报价值 ORDER BY s.declared_value_usd DESC"
  }'
```

**期望结果**: 返回所有在途货物的详细信息

### 4. 复杂查询 - 需要特殊处理的货物

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType) WHERE ct.special_handling IS NOT NULL RETURN s.awb_number, ct.category, ct.special_handling ORDER BY ct.category"
  }'
```

**期望结果**: 返回需要冷链、防静电、防震等特殊处理的货物

### 5. 聚合查询 - 浦东货运站库存

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (cs:CargoStation {name: \"浦东机场货运站\"})-[:HAS_FACILITY]->(f:Facility) RETURN f.facility_type AS 设施类型, f.capacity_tons AS 容量吨, f.current_load_tons AS 当前负载吨"
  }'
```

**期望结果**: 返回浦东货运站的各类设施容量和使用情况

### 6. 高价值货物追踪

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (s:Shipment) WHERE s.declared_value_usd > 100000 RETURN s.awb_number, s.declared_value_usd, s.status ORDER BY s.declared_value_usd DESC"
  }'
```

**期望结果**: 返回所有申报价值超过 10 万美元的货物

### 7. 统计查询 - 货物类型分布

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType) RETURN ct.category AS 货物类型, count(s) AS 运单数量 ORDER BY 运单数量 DESC"
  }'
```

**期望结果**: 每种货物类型的运单数量统计

### 8. 设施查询 - 危险品处理能力

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (cs:CargoStation)-[:HAS_FACILITY]->(f:Facility) WHERE f.has_dangerous_goods_license = true RETURN cs.name AS 货运站, f.facility_type AS 设施类型"
  }'
```

**期望结果**: 返回具备危险品处理资质的货运站和设施

---

## 📝 使用演示脚本

所有查询都已封装在演示脚本中：

```bash
./scripts/demo-queries.sh
```

**输出示例**:
```
========================================
Nexora 2.0 - 航空货运站演示查询
========================================

正在执行查询 1/8: 查看所有机场...
✓ 成功 - 返回 8 个机场

正在执行查询 2/8: 上海到洛杉矶的航线...
✓ 成功 - 找到 1 条路线

...

========================================
演示完成！所有查询执行成功。
========================================
```

---

## 🧪 高级测试

### 1. 性能测试

```bash
# 简单负载测试（100 个并发查询）
./scripts/load-demo-data.sh && \
for i in {1..100}; do
  curl -X POST http://127.0.0.1:8080/api/query \
    -H 'Content-Type: application/json' \
    -d '{"query": "MATCH (a:Airport) RETURN count(a)"}' &
done
wait
```

### 2. 断路器测试

```bash
# 触发多次失败以激活断路器
for i in {1..10}; do
  curl -X POST http://127.0.0.1:8080/api/query \
    -H 'Content-Type: application/json' \
    -d '{"query": "MATCH (x) WHERE x.nonexistent > 999999 RETURN x"}' 
done
```

### 3. 限流测试

```bash
# 快速发送大量请求以触发限流
for i in {1..2000}; do
  curl -X POST http://127.0.0.1:8080/api/query \
    -H 'Content-Type: application/json' \
    -d '{"query": "MATCH (a:Airport) RETURN a LIMIT 1"}' &
done
wait
```

### 4. 资源限制测试

```bash
# 测试查询超时
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (a)-[*1..100]->(b) RETURN count(*)"
  }'
```

---

## 🛑 停止服务器

```bash
# 方式 1: 使用脚本
./scripts/stop-demo.sh

# 方式 2: 手动停止
pkill -f nexora-app

# 方式 3: 如果在前台运行，按 Ctrl+C
```

---

## 📂 文件结构

```
nexora2/
├── target/release/nexora-app          # 发布版本二进制
├── data/demo/
│   ├── nexora-demo.toml               # 演示配置
│   └── nexora-demo.log                # 运行日志
├── examples/
│   ├── air-cargo-demo.cypher          # 数据创建脚本
│   ├── air-cargo-queries.cypher       # 查询示例
│   └── AIR_CARGO_DEMO_README.md       # 详细文档
└── scripts/
    ├── quick-start.sh                 # 一键启动
    ├── start-demo.sh                  # 启动服务器
    ├── stop-demo.sh                   # 停止服务器
    ├── load-demo-data.sh              # 导入数据
    └── demo-queries.sh                # 运行演示查询
```

---

## 🔧 配置说明

演示配置文件 `data/demo/nexora-demo.toml`:

```toml
[server]
bind_address = "127.0.0.1:8080"

[storage]
backend = "rocksdb"
data_dir = "data/demo"

[query]
max_pattern_depth = 10
max_execution_time_secs = 30
max_snapshot_nodes = 10000000
max_result_rows = 100000

[rate_limit]
global_requests_per_sec = 100000
per_client_requests_per_sec = 1000

[circuit_breaker]
failure_threshold = 5
timeout_secs = 10
```

---

## ❓ 常见问题

### Q1: 构建失败 - "error: failed to parse manifest"

**原因**: 使用了系统 stable cargo 而非 rustup nightly cargo

**解决方案**:
```bash
export PATH="$HOME/.cargo/bin:$PATH"
rustup default nightly-2026-06-11
cargo build --release -p nexora-app
```

### Q2: 启动失败 - "Address already in use"

**原因**: 端口 8080 被占用

**解决方案**:
```bash
# 方案 1: 查找并停止占用进程
lsof -i :8080
kill -9 <PID>

# 方案 2: 修改配置文件端口
vim data/demo/nexora-demo.toml
# 修改 bind_address = "127.0.0.1:8081"
```

### Q3: 查询失败 - "Query timeout"

**原因**: 查询过于复杂或数据量过大

**解决方案**:
```bash
# 增加超时时间（编辑配置文件）
vim data/demo/nexora-demo.toml
# 修改 max_execution_time_secs = 60
```

### Q4: 数据导入失败

**原因**: 服务器未启动或连接失败

**解决方案**:
```bash
# 1. 确认服务器运行
curl http://127.0.0.1:8080/health

# 2. 查看日志
tail -f data/demo/nexora-demo.log

# 3. 重新启动服务器
./scripts/stop-demo.sh
./scripts/start-demo.sh
```

---

## 📚 进一步学习

### 推荐阅读顺序

1. **快速入门**: `QUICKSTART.md`
2. **演示详情**: `examples/AIR_CARGO_DEMO_README.md`
3. **生产就绪**: `docs/PRODUCTION_READINESS_FINAL_REPORT.md`
4. **灾难恢复**: `docs/DISASTER_RECOVERY_MANUAL.md`
5. **负载测试**: `docs/LOAD_TEST_REPORT.md`

### Cypher 查询语言

详细的 Cypher 语法和示例，请参考：
- 官方文档: https://neo4j.com/docs/cypher-manual/
- 本地示例: `examples/air-cargo-queries.cypher`

---

## 🎉 开始测试

现在您已经准备好了！运行以下命令开始测试：

```bash
# 一键启动整个演示
./scripts/quick-start.sh
```

祝您测试愉快！如有问题，请查看日志文件或参考文档。

---

**Nexora 2.0** - 下一代流式图数据库  
**版本**: 2.0  
**日期**: 2026-08-03
