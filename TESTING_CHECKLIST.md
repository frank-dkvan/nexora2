# Nexora 2.0 测试验证清单

## 构建验证

- [ ] 二进制编译成功：`cargo build --release --bin nexora`
- [ ] 二进制文件存在：`ls -lh target/release/nexora`
- [ ] 大小合理（预期：50-150MB）

## 启动验证

```bash
# 1. 启动服务器（后台）
./target/release/nexora --config nexora.toml > /tmp/nexora.log 2>&1 &
export NEXORA_PID=$!

# 2. 等待启动（5秒）
sleep 5

# 3. 检查进程
ps aux | grep nexora | grep -v grep

# 4. 检查端口
lsof -i :8080 || netstat -an | grep 8080
```

## 健康检查

```bash
# 健康检查端点
curl http://127.0.0.1:8080/health

# 预期输出：
# {"status":"ok"}
```

## 数据导入验证

```bash
# 导入航空货运站数据
./scripts/load-demo-data.sh

# 预期输出：
# ✅ Imported 5 airports
# ✅ Imported 8 routes
# ✅ Imported 6 cargo shipments
```

## 查询验证

### 1. 简单节点查询
```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (a:Airport) RETURN a.code, a.name LIMIT 5"}'
```

**预期**：返回 5 个机场节点（PVG, PEK, LAX, JFK, LHR）

### 2. 路径查询
```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (a:Airport {code: \"PVG\"})-[r:ROUTE_TO]->(b:Airport) RETURN b.code, r.distance"}'
```

**预期**：返回从上海出发的 3 条航线（PEK, LAX, LHR）

### 3. 货物追踪查询
```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (c:Cargo {id: \"CARGO001\"})-[:SHIPS_VIA]->(r:Route)-[:ROUTE_TO]->(dest:Airport) RETURN dest.name"}'
```

**预期**：返回货物的目的地机场

### 4. 聚合查询
```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (c:Cargo) RETURN COUNT(c) AS total_cargo"}'
```

**预期**：`{"total_cargo": 6}`

### 5. 复杂路径查询
```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH p = (origin:Airport {code: \"PVG\"})-[:ROUTE_TO*1..2]->(dest:Airport {code: \"LHR\"}) RETURN LENGTH(p) AS hops"}'
```

**预期**：返回上海到伦敦的跳数（1 或 2）

## 性能验证

```bash
# 使用 Apache Bench 进行压力测试
ab -n 1000 -c 10 -p query.json -T 'application/json' \
   http://127.0.0.1:8080/api/query

# 预期：
# - Requests per second: >1000
# - Mean time per request: <10ms
# - Failed requests: 0
```

## 资源限制验证

### 1. 查询超时测试
```bash
# 模拟深度递归查询（应该被限制）
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n)-[*20..30]->(m) RETURN n LIMIT 1"}'
```

**预期**：返回错误 "Pattern depth exceeds limit"

### 2. 结果集限制测试
```bash
# 查询大量结果（应该被限制）
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN n LIMIT 100000"}'
```

**预期**：返回最多配置的行数（默认 10000）

## 速率限制验证

```bash
# 发送大量快速请求
for i in {1..1100}; do
  curl -s -X POST http://127.0.0.1:8080/api/query \
    -H 'Content-Type: application/json' \
    -d '{"query": "MATCH (n) RETURN COUNT(n)"}' &
done
wait
```

**预期**：部分请求返回 429 Too Many Requests（超过每客户端 1000 req/s 限制）

## 清理

```bash
# 停止服务器
kill $NEXORA_PID

# 或使用脚本
./scripts/stop-server.sh

# 清理数据（可选）
rm -rf /tmp/nexora-demo-*
```

## 故障排查

### 服务器无法启动

1. 检查日志：`cat /tmp/nexora.log`
2. 检查端口占用：`lsof -i :8080`
3. 检查配置文件：`cat nexora.toml`

### 查询返回错误

1. 检查语法：确保 Cypher 查询语法正确
2. 检查数据：确保数据已导入
3. 检查日志：查看服务器日志中的错误信息

### 性能问题

1. 检查系统资源：`top` 或 `htop`
2. 检查数据量：过大的图可能影响性能
3. 优化查询：使用索引、限制结果集

## 生产就绪检查清单

- [x] P1-1: Panic 实例审计与修复
- [x] P1-2: 外部服务熔断器
- [x] P1-3: 网络调用重试逻辑
- [x] P1-4: API 速率限制
- [x] P1-5: CVE 评估与缓解
- [x] P1-6: Cypher 查询资源限制
- [x] P1-7: 灾难恢复手册
- [x] P1-8: 负载测试与报告

**状态**：✅ 所有 P1 任务已完成

---

**版本**: 2.0  
**日期**: 2026-08-03  
**构建**: Rust nightly-2026-06-11
