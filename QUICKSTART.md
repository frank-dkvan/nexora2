# 🚀 Nexora-RS 快速启动指南

## 启动服务

启动后端服务后，访问以下地址：

- **后端 API**: http://localhost:8080
- **前端界面**: http://localhost:3000 (需构建 UI)

---

## 📊 样例数据说明

已导入的 10 个节点:

1. **Forklifts (3个)**
   - `forklift-001`: Forklift Alpha (active, speed=45)
   - `forklift-002`: Forklift Beta (active, speed=120) ⚡
   - `forklift-003`: Forklift Gamma (inactive, speed=0)

2. **Operators (2个)**
   - `operator-001`: Alice Johnson (morning shift)
   - `operator-002`: Bob Smith (afternoon shift)

3. **Zones (2个)**
   - `zone-001`: Loading Dock A (capacity=50)
   - `zone-002`: Storage Area B (capacity=100)

4. **Sensors (2个)**
   - `sensor-001`: Temperature Sensor (22.5°C)
   - `sensor-002`: Vibration Sensor (0.8 g-force)

5. **Package (1个)**
   - `package-001`: Package A (25kg, in-transit)

---

## 🎯 测试场景

### 1. 打开前端界面

访问 http://localhost:3000，你会看到 6 个页面:

- **Dashboard**: 系统概览（健康状态、活跃节点数）
- **Graph Browser**: 可视化图浏览器（双击节点展开邻居）
- **Cypher**: Cypher 查询编辑器
- **Standing Queries**: 实时模式匹配（已注册 high-speed-alert）
- **Ingest**: 数据摄入管理
- **Metrics**: 系统指标监控

### 2. 在 Graph Browser 中浏览数据

1. 在输入框中输入 `forklift-001` 点击 Load
2. 双击节点查看属性
3. 右键菜单可以设置属性、添加边

### 3. 在 Cypher 页面运行查询

尝试这些查询:

```cypher
// 查找所有 active 状态的节点
MATCH (n {status: "active"}) RETURN n

// 查找速度超过 100 的叉车
MATCH (f:Forklift) WHERE f.speed > 100 RETURN f

// 创建叉车和操作员之间的关系
MATCH (f:Forklift {id: "forklift-001"}), (o:Person {id: "operator-001"})
CREATE (o)-[:OPERATES]->(f)
```

### 4. Standing Query 实时监控

已注册的 Standing Query `high-speed-alert` 会实时监控:
- 当任何节点的 `speed` 属性 > 100 时触发匹配
- `forklift-002` (speed=120) 应该已经匹配

在 Standing Queries 页面可以看到匹配计数。

### 5. 通过 Ingest 页面导入更多数据

创建一个测试文件:

```bash
cat > more-data.jsonl << 'EOF'
{"id":"forklift-004","name":"Forklift Delta","type":"Forklift","status":"active","speed":150}
{"id":"zone-003","name":"Shipping Area C","type":"Zone","capacity":200}
EOF
```

然后在 Ingest 页面输入路径并导入。

---

## 🛠️ 管理命令

### 停止所有服务
```bash
./STOP.sh
```

### 重新启动
```bash
./START.sh
```

### 查看日志
```bash
tail -f backend.log   # 后端日志
tail -f frontend.log  # 前端日志
```

### 运行 API 测试
```bash
./test-queries.sh
```

---

## 📁 设计文档位置

相关设计文档在 `docs/` 目录:

1. **design-implementation-mapping.md** - 设计与实现映射
2. **research-tiledb-risingwave.md** - TileDB & RisingWave 研究
3. **top10-enhancement-impact-analysis.md** - Top 10 增强功能影响分析

---

## 🔍 快速检查

检查后端健康:
```bash
curl http://localhost:8080/api/v2/health | jq .
```

检查已导入节点数:
```bash
curl http://localhost:8080/api/v2/metrics | jq .active_nodes
```

查询一个节点:
```bash
curl "http://localhost:8080/api/v2/graph/node/$(echo -n 'forklift-001' | xxd -p)/property/name" | jq .
```

---

## 🎨 前端技术栈

- **框架**: React 18 + TypeScript + Vite
- **UI**: Bootstrap 5 + CoreUI
- **图可视化**: vis-network (力导向图)
- **路由**: React Router v6

## 🦀 后端技术栈

- **语言**: Rust 2021 (MSRV 1.88)
- **框架**: Axum 0.8 + Tokio
- **存储**: RocksDB (可选) + WAL
- **查询**: Cypher (read + write)
- **图引擎**: 自研事件溯源架构

---

**享受探索 Nexora-RS！** 🎉
