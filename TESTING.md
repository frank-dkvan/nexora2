# Nexora Event Streaming 测试指南

## 当前状态

正在编译 Nexora（library 模式，RisingWave 编译到进程内）。

编译完成后，你可以：

## 1. 启动 Nexora

```bash
./start-nexora-library.sh
```

这会启动 Nexora，并自动启动内嵌的 Event Streaming 引擎。

## 2. 验证启动成功

在另一个终端运行：

```bash
# 检查基本健康状态
curl http://127.0.0.1:8080/api/health

# 检查 Event Streaming 引擎状态
curl http://127.0.0.1:8080/api/event-streaming/status
```

期望输出：
```json
{
  "enabled": true,
  "meta_leader": true,
  "version": "v3.0.2"
}
```

## 3. 运行完整测试

```bash
./test-event-streaming.sh
```

这个脚本会测试：
- 健康检查
- Event Streaming 状态
- DDL 执行（创建物化视图）
- 查询物化视图
- 列出 sources 和 materialized views

## 4. 手动测试 SQL 流处理

### 创建一个简单的物化视图

```bash
curl -X POST http://127.0.0.1:8080/api/event-streaming/ddl \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "CREATE MATERIALIZED VIEW test_mv AS SELECT 1 as id, '\''test'\'' as name"
  }'
```

### 查询物化视图

```bash
curl -X POST http://127.0.0.1:8080/api/event-streaming/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM test_mv"
  }'
```

### 列出所有物化视图

```bash
curl http://127.0.0.1:8080/api/event-streaming/materialized_views
```

## 5. 架构说明

当前运行的是 **library 模式**：

- Event Streaming 引擎（RisingWave）直接编译到 Nexora 进程内
- 不需要外部二进制或子进程
- 单节点模式（meta + frontend + compute 都在同一进程）
- 数据目录：`./nexora-data/event-streaming/`

## 6. 停止服务

在 Nexora 运行的终端按 `Ctrl+C`，引擎会优雅关闭。

## 下一步测试（需要 Kafka）

如果你有 Kafka 运行，可以测试真实的流处理：

```sql
-- 创建 Kafka source
CREATE SOURCE events (
  event_id VARCHAR,
  event_type VARCHAR,
  payload JSONB
) WITH (
  connector = 'kafka',
  topic = 'nexora-events',
  properties.bootstrap.server = 'localhost:9092'
) FORMAT PLAIN ENCODE JSON;

-- 创建实时聚合物化视图
CREATE MATERIALIZED VIEW event_counts AS
SELECT 
  event_type,
  COUNT(*) as count,
  window_start
FROM TUMBLE(events, event_time, INTERVAL '1' MINUTE)
GROUP BY event_type, window_start;

-- 查询实时结果
SELECT * FROM event_counts ORDER BY window_start DESC LIMIT 10;
```

## 编译状态

当前编译命令：
```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release --features event-streaming,library
```

编译完成后，二进制位置：`./target/release/nexora`
