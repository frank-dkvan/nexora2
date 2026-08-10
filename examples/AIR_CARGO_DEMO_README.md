# Nexora 航空货运站演示

本演示展示了如何使用 Nexora 图数据库管理国际航空货运网络。

## 场景说明

演示包含完整的航空货运业务场景：

- **8个国际机场**：北京、上海、广州、香港、新加坡、洛杉矶、纽约、法兰克福
- **5个货运站**：具备冷链、危险品、清关等能力
- **5家航空公司**：国航、东航、南航、国泰、新航
- **12条航线**：覆盖亚洲、北美、欧洲主要航空枢纽
- **5种货物类型**：电子产品、医药品、生鲜食品、机械设备、纺织品
- **4个运单**：包含不同状态的真实货运案例

## 快速开始

### 1. 构建项目

```bash
# 使用 nightly 工具链构建发布版本
cargo build --release -p nexora-app
```

### 2. 启动服务器

```bash
# 启动 Nexora 服务器（单节点模式）
./scripts/start-demo.sh
```

服务器将监听 `http://127.0.0.1:8080`

### 3. 导入演示数据

打开新的终端窗口：

```bash
# 导入航空货运站数据
./scripts/load-demo-data.sh
```

导入完成后会显示数据统计：
- Airport: 8个节点
- CargoTerminal: 5个节点
- Airline: 5个节点
- CargoType: 5个节点
- Shipment: 4个节点
- 各类关系约30+条

### 4. 运行演示查询

```bash
# 执行预定义的演示查询
./scripts/demo-queries.sh
```

这将执行以下查询：
1. 查看所有机场
2. 上海到洛杉矶的航线
3. 所有在途货物
4. 需要冷链运输的货物
5. 浦东机场货运站库存
6. 高价值货物（>10万美元）
7. 货物类型分布统计
8. 具备危险品处理资质的货运站

## 手动查询示例

### 使用 curl

```bash
# 查询所有机场
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (a:Airport) RETURN a.code, a.name, a.city ORDER BY a.code"
  }'

# 查找从北京到纽约的航线（含中转）
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH path = (origin:Airport {code: \"PEK\"})-[:ROUTE*1..2]->(dest:Airport {code: \"JFK\"}) RETURN [n in nodes(path) | n.code] AS route, length(path) AS segments ORDER BY segments LIMIT 3"
  }'

# 查询高价值货物
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType) WHERE s.declared_value_usd > 100000 RETURN s.awb_number, ct.category, s.declared_value_usd ORDER BY s.declared_value_usd DESC"
  }'
```

### 使用 HTTP 客户端

使用 Postman、Insomnia 或其他 HTTP 客户端：

- **URL**: `http://127.0.0.1:8080/api/query`
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Body**:
  ```json
  {
    "query": "MATCH (a:Airport) RETURN a LIMIT 5"
  }
  ```

## 高级查询示例

### 1. 路径分析：找到最优货运路径

```cypher
// 直飞路径
MATCH path = (origin:Airport {code: "PEK"})-[r:ROUTE]->(dest:Airport {code: "JFK"})
RETURN "直飞" AS 路径类型,
       [n in nodes(path) | n.code] AS 途经机场,
       r.airline AS 航空公司,
       r.flight_time_hours AS 总飞行时间小时;

// 中转路径（最多2段）
MATCH path = (origin:Airport {code: "PEK"})-[:ROUTE*1..2]->(dest:Airport {code: "JFK"})
WITH path,
     [r in relationships(path) | r.flight_time_hours] AS segments,
     [n in nodes(path) | n.code] AS airports
RETURN "经停" AS 路径类型,
       airports AS 途经机场,
       reduce(total = 0, time in segments | total + time) AS 总飞行时间小时,
       length(path) AS 航段数
ORDER BY 总飞行时间小时 ASC
LIMIT 3;
```

### 2. 枢纽机场分析

```cypher
MATCH (a:Airport)-[r:ROUTE]->(:Airport)
WITH a, count(r) AS outbound_routes
MATCH (a)<-[r2:ROUTE]-(:Airport)
WITH a, outbound_routes, count(r2) AS inbound_routes
RETURN a.code AS 机场代码,
       a.name AS 机场名称,
       outbound_routes AS 出港航线数,
       inbound_routes AS 进港航线数,
       outbound_routes + inbound_routes AS 总航线数
ORDER BY 总航线数 DESC;
```

### 3. 货运站使用率监控

```cypher
MATCH (t:CargoTerminal)
OPTIONAL MATCH (s:Shipment)-[:STORED_AT]->(t)
WITH t, sum(s.weight_kg) AS current_weight_kg
RETURN t.terminal_id AS 货运站ID,
       t.airport_code AS 机场代码,
       t.warehouse_capacity_tons AS 容量吨,
       current_weight_kg / 1000.0 AS 当前存储吨,
       round((current_weight_kg / (t.warehouse_capacity_tons * 1000.0)) * 100, 2) AS 使用率百分比,
       CASE
         WHEN current_weight_kg / (t.warehouse_capacity_tons * 1000.0) > 0.8 THEN "告警-接近满载"
         WHEN current_weight_kg / (t.warehouse_capacity_tons * 1000.0) > 0.5 THEN "正常-过半"
         ELSE "空闲"
       END AS 状态评估
ORDER BY 使用率百分比 DESC;
```

### 4. 待处理货物优先级排序

```cypher
MATCH (s:Shipment)-[:IS_TYPE]->(ct:CargoType)
WHERE s.status IN ["待装机", "仓储中"]
WITH s, ct,
     CASE
       WHEN ct.requires_temperature_control = true THEN 1
       WHEN s.declared_value_usd > 100000 THEN 2
       ELSE 3
     END AS priority
RETURN s.awb_number AS 运单号,
       s.origin AS 起运地,
       s.destination AS 目的地,
       ct.category AS 货物类型,
       s.status AS 状态,
       s.scheduled_departure AS 计划起飞时间,
       CASE priority
         WHEN 1 THEN "高优先级-冷链"
         WHEN 2 THEN "中优先级-高价值"
         ELSE "普通优先级"
       END AS 优先级说明
ORDER BY priority ASC, s.scheduled_departure ASC;
```

## 数据文件说明

- **examples/air-cargo-demo.cypher**: 完整的数据创建脚本
  - 包含所有节点和关系的 CREATE 语句
  - 可以直接在 Cypher 环境中执行

- **examples/air-cargo-queries.cypher**: 15个预定义查询
  - 覆盖常见业务场景
  - 展示复杂图遍历和路径分析
  - 包含聚合、过滤、排序等操作

## API 端点

### 健康检查

```bash
curl http://127.0.0.1:8080/health
```

### 执行 Cypher 查询

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(n)"}'
```

### 查看指标（如果启用）

```bash
curl http://127.0.0.1:8080/metrics
```

## 配置说明

演示使用的配置文件位于 `data/demo/nexora-demo.toml`：

```toml
[server]
bind_address = "127.0.0.1:8080"
num_workers = 4

[storage]
backend = "RocksDB"
data_dir = "./data/demo/rocksdb"

[query]
max_pattern_depth = 10
max_execution_time_secs = 30
max_snapshot_nodes = 10_000_000
max_result_rows = 100_000

[rate_limit]
global_rate = 100_000        # 全局 100K req/s
per_client_rate = 1_000      # 单客户端 1K req/s

[circuit_breaker]
failure_threshold = 5
base_delay_ms = 100
max_delay_ms = 5000

[retry]
max_attempts = 3
base_delay_ms = 100
max_delay_ms = 5000
jitter_percent = 25
```

## 数据模型

### 节点类型

1. **Airport（机场）**
   - code: 机场代码（PEK, PVG, etc.）
   - name: 机场名称
   - city: 所在城市
   - country: 国家
   - capacity_tons_per_day: 日处理能力（吨）

2. **CargoTerminal（货运站）**
   - terminal_id: 货运站ID
   - airport_code: 所属机场
   - area_sqm: 面积（平方米）
   - cold_storage: 是否有冷链仓储
   - dangerous_goods_certified: 危险品资质
   - warehouse_capacity_tons: 仓储容量（吨）

3. **Airline（航空公司）**
   - iata_code: IATA代码
   - name: 公司名称
   - cargo_fleet_size: 货机数量

4. **CargoType（货物类型）**
   - type_id: 类型ID
   - category: 类别
   - requires_temperature_control: 是否需要温控
   - fragile: 是否易碎
   - customs_hs_code: 海关HS编码

5. **Shipment（运单）**
   - awb_number: 运单号
   - shipper_name: 发货人
   - consignee_name: 收货人
   - origin/destination: 起运地/目的地
   - weight_kg: 重量（千克）
   - declared_value_usd: 申报价值（美元）
   - status: 状态

### 关系类型

1. **HAS_TERMINAL**: 机场拥有货运站
2. **ROUTE**: 航线连接
3. **STORED_AT**: 货物存储在货运站
4. **IS_TYPE**: 货物属于某类型

## 性能特性

本演示展示了 Nexora 的以下特性：

- ✅ **查询资源限制**：最大30秒执行时间，100K行结果
- ✅ **API限流**：全局100K req/s，单客户端1K req/s
- ✅ **断路器保护**：外部服务故障自动隔离
- ✅ **智能重试**：指数退避+抖动，最多3次
- ✅ **图遍历优化**：最大10层模式深度
- ✅ **内存保护**：1000万节点快照限制

## 故障排除

### 服务器无法启动

```bash
# 检查端口是否被占用
lsof -i :8080

# 查看数据目录权限
ls -la data/demo/

# 检查配置文件
cat data/demo/nexora-demo.toml
```

### 数据导入失败

```bash
# 检查服务器健康状态
curl http://127.0.0.1:8080/health

# 手动执行单个查询测试
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(n)"}'
```

### 查询超时

如果查询超时，可以调整配置：

```toml
[query]
max_execution_time_secs = 60  # 增加到60秒
```

## 清理

停止服务器：`Ctrl+C`

清理数据：

```bash
rm -rf data/demo/rocksdb
rm -rf data/demo/raft
rm -f data/demo/nexora-demo.toml
```

## 进一步学习

- 📖 查看完整文档：`docs/`
- 🔍 探索更多查询：`examples/air-cargo-queries.cypher`
- 🎯 生产部署指南：`docs/PRODUCTION_READINESS_FINAL_REPORT.md`
- 🚨 灾难恢复手册：`docs/DISASTER_RECOVERY_MANUAL.md`

## 支持

如有问题或建议，请：
- 提交 Issue: https://github.com/frank-dkvan/nexora2/issues
- 查看文档: `docs/`
- 联系开发团队

---

**Nexora 2.0** - 下一代流式图数据库  
© 2026 Nexora Project
