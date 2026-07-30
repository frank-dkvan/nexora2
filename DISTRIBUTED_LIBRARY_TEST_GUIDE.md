# RisingWave 分布式库模式测试指南

## 🎯 测试目标

验证 RisingWave 分布式库模式的完整功能：
- Meta/Frontend/Compute 三层架构
- PostgreSQL 协议连接
- Iceberg REST catalog 集成
- 完整的数据流（Source → MV → Sink）

---

## 📋 前置条件

### 1. 编译
```bash
cargo build --features event-first,event-streaming,library
```

### 2. 工具准备（可选）
```bash
# PostgreSQL 客户端（用于连接 RisingWave）
brew install postgresql  # macOS
# 或
apt-get install postgresql-client  # Linux

# JSON 处理工具
brew install jq
```

---

## 🚀 启动步骤

### 方式 1：简化启动（推荐）

```bash
# 1. 启动分布式库模式
./scripts/start-distributed-simple.sh
```

这将启动单节点分布式集群：
- **Meta Node**: 127.0.0.1:5690 (Raft 协调)
- **Frontend**: 127.0.0.1:4566 (PostgreSQL 协议)
- **Compute Node**: 127.0.0.1:5688 (计算节点)
- **HTTP API**: 127.0.0.1:8080 (Nexora API + Iceberg catalog)

### 方式 2：带配置文件启动

```bash
./scripts/start-distributed-library-test.sh
```

---

## 🧪 测试流程

### 第一步：验证服务启动

**在新终端窗口运行测试脚本：**
```bash
./scripts/test-distributed-library.sh
```

这将测试：
- ✅ HTTP API 健康检查
- ✅ Iceberg REST catalog 端点
- ✅ PostgreSQL 连接
- ✅ 基本表操作

### 第二步：手动测试 PostgreSQL 连接

```bash
# 连接到 RisingWave Frontend
psql -h localhost -p 4566 -U root -d dev

# 在 psql 提示符下：
dev=> SELECT version();
dev=> SHOW TABLES;
dev=> SELECT * FROM rw_catalog.rw_tables;
```

### 第三步：测试 Iceberg REST Catalog

```bash
# 获取 catalog 配置
curl http://localhost:8080/api/iceberg/catalog/v1/config | jq

# 列出 namespaces
curl http://localhost:8080/api/iceberg/catalog/v1/namespaces | jq

# 列出 default namespace 中的表
curl http://localhost:8080/api/iceberg/catalog/v1/namespaces/default/tables | jq
```

---

## 📊 完整数据流测试

### 1. 创建测试表

```sql
-- 连接到 RisingWave
psql -h localhost -p 4566 -U root -d dev

-- 创建测试表
CREATE TABLE events (
    id BIGINT,
    event_type VARCHAR,
    user_id BIGINT,
    timestamp TIMESTAMPTZ,
    data JSONB
);

-- 插入测试数据
INSERT INTO events VALUES 
    (1, 'click', 100, NOW(), '{"page": "home"}'),
    (2, 'view', 101, NOW(), '{"page": "product"}'),
    (3, 'click', 100, NOW(), '{"page": "checkout"}');

-- 查询
SELECT * FROM events;
```

### 2. 创建 Materialized View

```sql
-- 创建 MV：按用户聚合事件
CREATE MATERIALIZED VIEW user_event_stats AS
SELECT 
    user_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_type) as event_types
FROM events
GROUP BY user_id;

-- 查询 MV
SELECT * FROM user_event_stats;
```

### 3. 检查系统 Catalog

```sql
-- 查看所有表
SELECT name, type FROM rw_catalog.rw_tables;

-- 查看 Materialized Views
SELECT name FROM rw_catalog.rw_materialized_views;

-- 查看 Iceberg 表（如果有）
SELECT * FROM rw_catalog.iceberg_tables;
```

---

## 🔍 验证分布式功能

### 检查 Meta 节点状态

```bash
# Meta 节点健康检查（如果暴露了 metrics）
curl http://localhost:5690/metrics 2>/dev/null || echo "Metrics not exposed"
```

### 检查 Frontend 连接

```bash
# 多个并发连接测试
for i in {1..5}; do
  psql -h localhost -p 4566 -U root -d dev -c "SELECT $i as connection_id;" &
done
wait
```

### 检查 Compute 节点

```sql
-- 查看 compute 节点信息
SELECT * FROM rw_catalog.rw_worker_nodes;

-- 查看并行度设置
SHOW PARALLELISM;
```

---

## 📈 性能测试（可选）

### 1. 批量插入测试

```sql
-- 生成大量测试数据
INSERT INTO events 
SELECT 
    generate_series(1, 10000) as id,
    CASE (random() * 2)::int WHEN 0 THEN 'click' WHEN 1 THEN 'view' ELSE 'purchase' END as event_type,
    (random() * 1000)::int as user_id,
    NOW() - (random() * interval '30 days') as timestamp,
    '{"test": true}'::jsonb as data;

-- 检查 MV 自动更新
SELECT * FROM user_event_stats ORDER BY event_count DESC LIMIT 10;
```

### 2. 查询性能测试

```bash
# 使用 time 命令测试查询延迟
time psql -h localhost -p 4566 -U root -d dev -c "SELECT COUNT(*) FROM events;"
time psql -h localhost -p 4566 -U root -d dev -c "SELECT * FROM user_event_stats LIMIT 100;"
```

---

## 🛑 停止和清理

### 停止服务

```bash
# 在运行服务的终端按 Ctrl+C
```

### 清理数据

```bash
# 删除所有测试数据
rm -rf ./nexora-data/event-streaming-library
rm -rf ./nexora-data/meta-*.db
```

---

## ❌ 常见问题

### 问题 1: 启动失败 "Address already in use"

**原因**: 端口被占用

**解决**:
```bash
# 查找占用端口的进程
lsof -i :4566
lsof -i :5690
lsof -i :8080

# 杀死进程
kill -9 <PID>
```

### 问题 2: psql 连接失败

**原因**: Frontend 未启动或地址错误

**检查**:
```bash
# 检查端口是否监听
nc -zv localhost 4566

# 查看日志
tail -f logs/nexora.log
```

### 问题 3: 编译错误

**原因**: 缺少 feature flags

**解决**:
```bash
cargo clean
cargo build --features event-first,event-streaming,library
```

### 问题 4: 内存不足

**原因**: RisingWave 需要较多内存

**解决**:
- 单节点测试建议至少 2GB RAM
- 减少并行度：修改 compute.parallelism 参数
- 关闭其他占用内存的应用

---

## 📚 参考文档

- [Phase 4 Complete](../docs/PHASE4_COMPLETE.md) - Iceberg 集成文档
- [RisingWave 官方文档](https://docs.risingwave.com/)
- [Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

---

## 🎉 成功标志

如果看到以下输出，说明测试成功：

```
✅ HTTP API 响应正常
✅ Iceberg catalog 端点可访问
✅ PostgreSQL 连接成功
✅ 能够创建表和查询
✅ Materialized View 自动更新
✅ 系统 catalog 可查询
```

---

**测试完成后，请记录：**
1. 启动耗时
2. 内存占用
3. 查询响应时间
4. 遇到的任何错误

这些信息将帮助我们进一步优化！
