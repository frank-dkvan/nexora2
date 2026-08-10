# Nexora 2.0 演示版 - 5分钟快速开始

**版本**: 2.0 Production-Ready  
**构建日期**: 2026-08-03  
**包大小**: 15MB  
**平台**: macOS (Intel/Apple Silicon 通用)

---

## 🎯 使用场景

本演示包适合：
- ✅ 快速体验 Nexora 2.0 图数据库功能
- ✅ 验证 Cypher 查询语言能力
- ✅ 测试航空货运场景的图查询
- ✅ 评估生产就绪特性（熔断器、重试、限流）

**不适合**:
- ❌ 生产环境部署（请使用集群版本）
- ❌ 高并发性能测试（单机版本）
- ❌ 大规模数据导入（演示数据仅 25 节点 + 20 边）

---

## 📦 快速开始（3 步）

### 步骤 1: 解压部署包

```bash
tar -xzf nexora-2.0-demo-macos-20260803.tar.gz
cd nexora-2.0-demo
```

### 步骤 2: 一键启动

```bash
./start-demo-simple.sh
```

**预期输出**:
```
🚀 启动 Nexora 演示服务器...
✅ Nexora 服务器已启动 (PID: 12345)
📊 导入演示数据...
✅ 演示数据导入完成 (25 节点 + 20 边)
🔍 服务器健康检查...
✅ 服务器运行正常

================================================
🎉 Nexora 2.0 演示环境已就绪！
================================================

📡 API 端点: http://127.0.0.1:8080
📝 日志文件: /tmp/nexora-demo.log

💡 测试查询示例:
...
```

### 步骤 3: 运行测试查询

**查询所有机场**:
```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (a:Airport) RETURN a.code, a.name LIMIT 5"}'
```

**预期结果**:
```json
{
  "columns": ["a.code", "a.name"],
  "rows": [
    ["PVG", "上海浦东国际机场"],
    ["PEK", "北京首都国际机场"],
    ["LAX", "洛杉矶国际机场"],
    ["JFK", "约翰·肯尼迪国际机场"],
    ["LHR", "伦敦希思罗机场"]
  ],
  "row_count": 5,
  "execution_time_ms": 2
}
```

---

## 🔍 演示数据说明

**场景**: 国际航空货运网络

| 实体类型 | 数量 | 说明 |
|---------|------|------|
| 机场 (Airport) | 5 | 上海PVG、北京PEK、洛杉矶LAX、纽约JFK、伦敦LHR |
| 货物 (Cargo) | 6 | 电子产品、服装、药品、机械零件、食品、化工原料 |
| 航线 (ROUTE_TO) | 8 | 含距离、飞行时间、班次频率 |
| 运输关系 (SHIPS_FROM/TO) | 12 | 货物起点终点关系 |

**总计**: 11 节点 + 20 边

---

## 🧪 常用测试查询

### 1. 查询从上海出发的航线

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (a:Airport {code: \"PVG\"})-[r:ROUTE_TO]->(b:Airport) RETURN b.code, b.name, r.distance, r.flight_time"}'
```

### 2. 查询在途货物

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (c:Cargo) WHERE c.status = \"in_transit\" RETURN c.id, c.type, c.weight, c.priority"}'
```

### 3. 查询紧急货物的运输路线

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (c:Cargo {priority: \"urgent\"})-[:SHIPS_FROM]->(from:Airport)-[r:ROUTE_TO]->(to:Airport)<-[:SHIPS_TO]-(c) RETURN c.id, from.code, to.code, r.distance"}'
```

### 4. 统计查询

```bash
# 统计所有节点数
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN COUNT(n) as total_nodes"}'

# 统计所有关系数
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH ()-[r]->() RETURN COUNT(r) as total_relationships"}'
```

---

## 🛠️ 故障排查

### Q1: 端口 8080 被占用

**症状**: `start-demo-simple.sh` 报错 "Address already in use"

**解决方案**:
```bash
# 方案 1: 查找并停止占用进程
lsof -i :8080
kill <PID>

# 方案 2: 修改配置文件端口
vim nexora.toml
# 修改: listen_addr = "127.0.0.1:8081"
```

### Q2: 服务器无法启动

**症状**: `start-demo-simple.sh` 超时

**解决方案**:
```bash
# 查看详细日志
tail -100 /tmp/nexora-demo.log

# 清理旧数据
rm -rf /tmp/nexora-demo-*
./start-demo-simple.sh
```

### Q3: 查询返回空结果

**症状**: 查询成功但 `rows: []`

**解决方案**:
```bash
# 验证数据已导入
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN COUNT(n)"}'

# 如果返回 0，重新导入数据
./stop-demo-simple.sh
rm -rf /tmp/nexora-demo-*
./start-demo-simple.sh
```

### Q4: 健康检查失败

**症状**: `curl http://127.0.0.1:8080/health` 返回错误

**解决方案**:
```bash
# 检查进程是否运行
ps aux | grep nexora

# 如果没有进程，查看日志
tail -50 /tmp/nexora-demo.log

# 检查端口监听
lsof -i :8080
```

---

## 🔧 配置说明

### nexora.toml 关键配置

```toml
[server]
listen_addr = "127.0.0.1:8080"  # API 监听地址

[storage]
backend = "rocksdb"             # 存储引擎
data_dir = "/tmp/nexora-demo-graph"  # 数据目录

[query]
max_pattern_depth = 10          # 最大模式深度
max_execution_time_secs = 30    # 最大执行时间
max_memory_mb = 512             # 最大内存
max_result_rows = 10000         # 最大结果行数
```

**生产环境建议**:
- `data_dir`: 使用持久化路径（非 /tmp）
- `max_execution_time_secs`: 根据业务调整（5-60秒）
- `max_memory_mb`: 根据可用内存调整（建议 ≥1024MB）

---

## 📊 性能基准

**测试环境**: MacBook Pro (M1, 16GB RAM)

| 操作 | 延迟 | 吞吐量 |
|------|------|--------|
| 简单节点查询 | ~2ms | 500 QPS |
| 2-hop 路径查询 | ~5ms | 200 QPS |
| 聚合查询 | ~10ms | 100 QPS |
| 写入操作 | ~3ms | 300 TPS |

**注意**: 这是单机演示版本的性能，生产集群版本可达到 10K+ QPS。

---

## ✨ 生产就绪特性（已启用）

本演示版本包含以下生产级特性：

### 1. 熔断器 (Circuit Breaker)
- **触发条件**: 5 次连续失败
- **恢复策略**: 指数退避 (100ms → 5s)
- **保护对象**: S3/Iceberg 操作、流数据源

### 2. 重试逻辑 (Retry)
- **最大重试**: 3 次
- **退避策略**: 指数退避 + ±25% 抖动
- **基础延迟**: 100ms
- **最大延迟**: 5s

### 3. 速率限制 (Rate Limiting)
- **全局限制**: 100,000 req/s
- **单客户端**: 1,000 req/s (按 IP)
- **算法**: 令牌桶
- **自动清理**: 5 分钟 TTL

### 4. 查询资源限制
- **模式深度**: 最大 10 层
- **执行时间**: 最大 30 秒
- **内存使用**: 最大 512MB
- **结果行数**: 最大 10,000 行

### 5. CVE 修复
- ✅ 18 个关键漏洞已修复（wasmtime）
- ✅ 8 个高危漏洞已修复（quick-xml）
- ✅ 1 个中危漏洞风险已评估（rsa）

### 6. 灾难恢复能力
- **RTO**: 30 分钟
- **RPO**: 1 分钟
- **备份**: Apache Iceberg 不可变事件日志
- **恢复**: 从事件日志完整重建图状态

---

## 🚀 下一步

### 进一步探索

1. **查看完整演示指南**:
   ```bash
   cat DEMO_GUIDE.md
   ```

2. **运行测试清单**:
   ```bash
   cat TESTING_CHECKLIST.md
   ```

3. **尝试高级查询**（见 DEMO_GUIDE.md）:
   - 多跳路径查询
   - 最短路径算法
   - 子图匹配
   - 聚合统计

### 生产部署

如需生产环境部署，请参考：
- **集群部署**: `deploy/k8s/README.md`
- **Docker 部署**: `deploy/docker-compose.yml`
- **完整文档**: `docs/DEPLOYMENT.md`

### 获取支持

- **GitHub**: https://github.com/frank-dkvan/nexora2
- **Issues**: https://github.com/frank-dkvan/nexora2/issues
- **邮件**: frank-dkvan@example.com

---

## 🧹 清理

### 停止服务

```bash
./stop-demo-simple.sh
```

### 删除所有数据

```bash
rm -rf /tmp/nexora-demo-*
```

### 完全卸载

```bash
cd ..
rm -rf nexora-2.0-demo
rm -f nexora-2.0-demo-macos-20260803.tar.gz
```

---

## 📄 附录

### 文件清单

| 文件 | 大小 | 说明 |
|------|------|------|
| nexora | 45MB | 主程序二进制文件 |
| nexora.toml | 575B | 配置文件 |
| demo-data.cypher | 2.6KB | 演示数据脚本 |
| start-demo-simple.sh | 3.4KB | 启动脚本 |
| stop-demo-simple.sh | 1.3KB | 停止脚本 |
| README.md | 3.3KB | 快速开始 |
| DEMO_GUIDE.md | 7.7KB | 完整演示指南 |
| TESTING_CHECKLIST.md | 7.9KB | 测试验证清单 |

### 系统要求

**最低配置**:
- macOS 10.15+
- 2GB 可用内存
- 500MB 磁盘空间
- 8080 端口可用

**推荐配置**:
- macOS 12+ (Monterey)
- 8GB+ 内存
- SSD 存储
- 4+ CPU 核心

### 许可证

本软件按 MIT 许可证分发。详见项目根目录 LICENSE 文件。

---

**最后更新**: 2026-08-03  
**文档版本**: 1.0  
**软件版本**: Nexora 2.0 Production-Ready

---

**祝您使用愉快！** 🎉

如有任何问题，请随时联系我们。
