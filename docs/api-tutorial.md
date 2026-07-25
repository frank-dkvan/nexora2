# Nexora-RS API 使用教程

**版本**: 1.0 | **更新日期**: 2026-07-03

本教程全面介绍 Nexora-RS 图数据库的 REST API 使用方法，涵盖从基础 CRUD 到流式查询、向量搜索和认证安全的完整工作流。

---

## 目录

1. [入门](#1-入门)
2. [核心操作](#2-核心操作)
3. [Cypher 查询](#3-cypher-查询)
4. [Standing Queries（流式查询）](#4-standing-queries流式查询)
5. [数据摄入](#5-数据摄入)
6. [向量搜索](#6-向量搜索)
7. [认证与安全](#7-认证与安全)
8. [错误处理](#8-错误处理)
9. [UDF — 用户自定义函数](#9-udf--用户自定义函数)
10. [集群模式启动](#10-集群模式启动)
11. [Python SDK 使用示例](#11-python-sdk-使用示例)

---

## 1. 入门

### 1.1 安装与启动

**前置依赖**:
- Rust 1.88+ (`cargo`)
- Node.js 18+ (前端 UI，可选)

**编译后端**:

```bash
cd nexora
cargo build --release
```

**启动服务（开发模式 — 内存存储）**:

```bash
./target/release/nexora-app --host 0.0.0.0 --port 8080 --no-rocksdb
```

**启动服务（持久化模式 — RocksDB + WAL）**:

```bash
./target/release/nexora-app --host 0.0.0.0 --port 8080 \
  --rocksdb-path ./nexora-data \
  --wal-dir ./nexora-data/wal
```

**使用启动脚本（后端 + 前端 + 样例数据）**:

```bash
./START.sh
```

启动后可访问:
- 后端 API: `http://localhost:8080`
- 前端 UI: `http://localhost:3000`
- Swagger 文档: `http://localhost:8080/api/v2/docs`
- OpenAPI JSON: `http://localhost:8080/api/v2/openapi.json`

**验证服务健康**:

```bash
curl http://localhost:8080/api/v2/health | jq .
```

响应示例:
```json
{
  "status": "healthy",
  "mode": "single-node",
  "profile": "single-durable",
  "active_nodes": 0,
  "shards": 256,
  "standing_queries": 0,
  "readiness": "ready",
  "liveness": "alive",
  "durability": "durable",
  "version": "0.1.0"
}
```

### 1.2 第一次查询：创建节点和边

**使用 curl**:

```bash
# 创建一个带属性的节点
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE (n:Person {name: \"Alice\", age: 30})"}'

# 创建另一个节点
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE (n:Person {name: \"Bob\", age: 25})"}'

# 查询所有 Person 节点
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n:Person) RETURN n.name, n.age"}'

# 创建关系
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:KNOWS]->(b)"}'
```

**使用 Python**:

```python
import requests

BASE_URL = "http://localhost:8080"

def cypher(query: str):
    resp = requests.post(
        f"{BASE_URL}/api/v2/query/cypher",
        json={"query": query}
    )
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data

# 创建节点
cypher('CREATE (n:Person {name: "Alice", age: 30})')

# 查询
result = cypher('MATCH (n:Person) RETURN n.name, n.age')
print(f"Columns: {result['columns']}")
for row in result["rows"]:
    print(f"  {row}")
```

**使用 JavaScript (Node.js)**:

```javascript
const BASE_URL = "http://localhost:8080";

async function cypher(query) {
  const resp = await fetch(`${BASE_URL}/api/v2/query/cypher`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  const data = await resp.json();
  if (data.error) throw new Error(data.error);
  return data;
}

// 创建节点
await cypher('CREATE (n:Person {name: "Alice", age: 30})');

// 查询
const result = await cypher('MATCH (n:Person) RETURN n.name, n.age');
console.log("Columns:", result.columns);
for (const row of result.rows) {
  console.log("  ", row);
}
```

---

## 2. 核心操作

### 2.1 节点 CRUD

Nexora-RS 中节点通过 `NexoraId`（十六进制字符串）标识。节点操作可通过 Cypher 查询或 REST API 完成。

**创建节点（Cypher）**:

```bash
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query": "CREATE (n:Person {name: \"Charlie\", age: 35, city: \"Beijing\"})"}'
```

**读取节点属性（REST API）**:

```bash
# 将节点 ID 转为十六进制（例如 "charlie" -> 636861726c6965）
QID=$(echo -n "charlie" | xxd -p)
curl "http://localhost:8080/api/v2/graph/node/$QID/property/name" | jq .
```

响应:
```json
{
  "node_id": "636861726c6965",
  "key": "name",
  "value": "Charlie"
}
```

**更新节点属性（REST API）**:

```bash
curl -X PUT "http://localhost:8080/api/v2/graph/node/$QID/property/age" \
  -H "Content-Type: application/json" \
  -d '{"value": 36}' | jq .
```

**更新属性（Cypher SET）**:

```cypher
MATCH (n:Person {name: "Charlie"}) SET n.age = 36, n.city = "Shanghai"
```

**删除节点（Cypher）**:

```cypher
-- 删除节点（必须先删除关联的边）
MATCH (n:Person {name: "Charlie"}) DELETE n

-- 删除节点及其所有关系
MATCH (n:Person {name: "Charlie"}) DETACH DELETE n
```

### 2.2 边 CRUD

**添加边（REST API）**:

```bash
# 源节点和目标节点的 NexoraId（十六进制）
SOURCE=$(echo -n "alice" | xxd -p)  # 616c696365
TARGET=$(echo -n "bob" | xxd -p)    # 626f62

curl -X POST "http://localhost:8080/api/v2/graph/node/$SOURCE/edges" \
  -H "Content-Type: application/json" \
  -d "{
    \"edge_type\": \"KNOWS\",
    \"target\": \"$TARGET\",
    \"direction\": \"out\"
  }" | jq .
```

**添加边（Cypher）**:

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CREATE (a)-[:KNOWS {since: 2020}]->(b)
```

**查询边（REST API）**:

```bash
curl "http://localhost:8080/api/v2/graph/node/$SOURCE/edges" | jq .
```

响应:
```json
{
  "edges": [
    {
      "edge_type": "KNOWS",
      "direction": "out",
      "other": "626f62"
    }
  ]
}
```

**查询边（Cypher）**:

```cypher
MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b:Person)
RETURN b.name, r.since
```

### 2.3 属性管理

属性通过 `PUT /api/v2/graph/node/{qid}/property/{key}` 设置，支持 JSON 值类型（字符串、数字、布尔、数组、对象）。

**设置复杂属性**:

```bash
curl -X PUT "http://localhost:8080/api/v2/graph/node/$QID/property/metadata" \
  -H "Content-Type: application/json" \
  -d '{
    "value": {
      "tags": ["python", "rust"],
      "score": 95.5,
      "active": true
    }
  }' | jq .
```

**删除属性（Cypher REMOVE）**:

```cypher
MATCH (n:Person {name: "Charlie"}) REMOVE n.city
```

### 2.4 标签与索引

**设置标签（通过属性）**:

标签在 Nexora-Rs 中以 `labels` 属性存储，类型为字符串数组。

```bash
curl -X PUT "http://localhost:8080/api/v2/graph/node/$QID/property/labels" \
  -H "Content-Type: application/json" \
  -d '{"value": ["Person", "Engineer"]}' | jq .
```

**按标签查询（Cypher）**:

```cypher
MATCH (n:Person) RETURN n.name
MATCH (n:Engineer) WHERE n.score > 80 RETURN n.name
```

**Standing Query 按标签过滤**:

```bash
curl -X POST http://localhost:8080/api/v2/standing-query \
  -H "Content-Type: application/json" \
  -d '{
    "name": "all-engineers",
    "pattern": {
      "type": "LabelFilter",
      "labels": ["Engineer"]
    }
  }' | jq .
```

---

## 3. Cypher 查询

Nexora-RS 支持 OpenCypher 标准，覆盖率达 97%。所有查询通过 `POST /api/v2/query/cypher` 执行。

### 3.1 基本 MATCH 模式

```cypher
-- 查询所有节点
MATCH (n) RETURN n LIMIT 10

-- 按标签查询
MATCH (n:Person) RETURN n.name, n.age

-- 带关系的模式匹配
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name

-- 多跳查询
MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)
RETURN a.name, c.name
```

### 3.2 WHERE 过滤

```cypher
-- 数值比较
MATCH (n:Person) WHERE n.age > 18 RETURN n.name

-- 字符串匹配
MATCH (n:Person) WHERE n.name = "Alice" RETURN n

-- 多条件组合
MATCH (n:Person)
WHERE n.age > 18 AND n.city = "Beijing"
RETURN n.name, n.age

-- 属性存在性检查
MATCH (n) WHERE n.email IS NOT NULL RETURN n
```

### 3.3 聚合（COUNT, SUM, AVG）

```cypher
-- 计数
MATCH (n:Person) RETURN COUNT(n)

-- 求和与平均
MATCH (n:Person) RETURN SUM(n.age), AVG(n.age)

-- 多聚合组合
MATCH (n:Person)
RETURN COUNT(n), AVG(n.age), MIN(n.age), MAX(n.age)

-- 分组聚合（WITH + 聚合）
MATCH (n:Person)
WITH n.city AS city, COUNT(n) AS count
RETURN city, count
```

### 3.4 ORDER BY, LIMIT, SKIP

> **重要**: 子句顺序必须为 `RETURN → ORDER BY → SKIP → LIMIT`。

```cypher
-- 排序
MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC

-- 分页
MATCH (n:Person) RETURN n.name ORDER BY n.name SKIP 10 LIMIT 5

-- 组合使用
MATCH (n:Person)
RETURN n.name, n.age
ORDER BY n.age DESC
SKIP 0
LIMIT 10
```

### 3.5 路径查询

```cypher
-- 查询两节点间的路径
MATCH path = (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person {name: "Charlie"})
RETURN path

-- 可变长度路径
MATCH (a:Person {name: "Alice"})-[:KNOWS*2]->(b)
RETURN b.name

-- 最短路径风格查询
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..5]->(b:Person {name: "Bob"})
RETURN b.name LIMIT 1
```

### 3.6 MERGE（Upsert）

```cypher
-- 如果不存在则创建，存在则匹配
MERGE (n:Person {id: 123})
  ON CREATE SET n.created = timestamp(), n.name = "New User"
  ON MATCH SET n.updated = timestamp()
RETURN n
```

### 3.7 SET / REMOVE 标签和属性

```cypher
-- 设置属性
MATCH (n:Person {name: "Alice"}) SET n.age = 31, n.city = "Shanghai"

-- 使用 SET 添加标签
MATCH (n:Person {name: "Alice"}) SET n:Engineer

-- 删除属性
MATCH (n:Person {name: "Alice"}) REMOVE n.city

-- 删除标签
MATCH (n:Person {name: "Alice"}) REMOVE n:Engineer
```

### 3.8 DELETE 节点和边

```cypher
-- 删除节点（需先删除关联边）
MATCH (n:Person {name: "Charlie"}) DELETE n

-- 删除节点及所有关联边
MATCH (n:Person {name: "Charlie"}) DETACH DELETE n

-- 仅删除边
MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b:Person {name: "Bob"})
DELETE r
```

### 3.9 CASE 表达式

> **注意**: Nexora-RS 当前版本不支持 `CASE WHEN` 表达式。请在应用层处理条件逻辑，或使用多个查询替代。

### 3.10 EXISTS 子查询

> **注意**: Nexora-RS 当前版本不支持 `EXISTS` 子查询。建议使用 `WITH` 管道和 `WHERE` 过滤组合实现类似效果。

### 3.11 UNION / UNION ALL

> **注意**: Nexora-RS 当前版本不支持 `UNION` / `UNION ALL`。请分别执行查询后在应用层合并结果。

### 3.12 CALL 子查询

> **注意**: Nexora-RS 当前版本不支持 `CALL {}` 子查询。请使用 `WITH` 管道拆分查询步骤。

### 3.13 LOAD CSV

> **注意**: Nexora-RS 当前版本不支持 `LOAD CSV` 子句。请使用数据摄入 API 导入 CSV/JSONL 文件（参见[第 5 节](#5-数据摄入)）。

### 3.14 管道查询（WITH）

```cypher
-- 多步管道
MATCH (n:Person)
WITH n WHERE n.age > 18
WITH n ORDER BY n.age DESC
RETURN n.name, n.age LIMIT 10

-- UNWIND 展开
UNWIND [1, 2, 3, 4, 5] AS x
RETURN x, x * x AS square
```

### 3.15 时间旅行查询

```cypher
-- 查询历史时间点的数据
MATCH (n:Person) AS OF '20260101' RETURN n.name, n.age

-- 使用微秒时间戳
MATCH (n:Person) AS OF 1735689600000000 RETURN n
```

---

## 4. Standing Queries（流式查询）

Standing Queries 是 Nexora-RS 的核心特性，允许注册持续运行的模式匹配规则，在数据变更时实时触发。

### 4.1 创建 Standing Query

**PropertyFilter — 属性值过滤**:

```bash
curl -X POST http://localhost:8080/api/v2/standing-query \
  -H "Content-Type: application/json" \
  -d '{
    "name": "high-speed-alert",
    "pattern": {
      "type": "PropertyFilter",
      "key": "speed",
      "condition": {
        "type": "GreaterThan",
        "value": 100
      }
    }
  }' | jq .
```

支持的过滤条件类型:
| 类型 | 说明 | 示例值 |
|------|------|--------|
| `GreaterThan` | 大于 | `100` |
| `LessThan` | 小于 | `50` |
| `Equals` | 等于 | `"active"` |
| `Contains` | 字符串包含 | `"error"` |
| `Exists` | 属性存在 | — |
| `IsNull` | 值为 null | — |
| `IsNotNull` | 值非 null | — |

**LabelFilter — 标签过滤**:

```bash
curl -X POST http://localhost:8080/api/v2/standing-query \
  -H "Content-Type: application/json" \
  -d '{
    "name": "all-persons",
    "pattern": {
      "type": "LabelFilter",
      "labels": ["Person"]
    }
  }' | jq .
```

### 4.2 管理 Standing Queries

```bash
# 列出所有 Standing Queries
curl http://localhost:8080/api/v2/standing-query | jq .

# 获取单个 Standing Query
SQ_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
curl "http://localhost:8080/api/v2/standing-query/$SQ_ID" | jq .

# 删除 Standing Query
curl -X DELETE "http://localhost:8080/api/v2/standing-query/$SQ_ID" | jq .
```

### 4.3 WebSocket 订阅

通过 WebSocket 实时接收 Standing Query 匹配结果:

```javascript
const ws = new WebSocket("ws://localhost:8080/api/v2/ws/sq/" + sqId);

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log("SQ Match:", data);
  // { type: "SqMatch", sq_id: "...", match_count: 5 }
};

ws.onclose = () => console.log("Disconnected");
```

**Python WebSocket 示例**:

```python
import asyncio
import websockets

async def subscribe_sq(sq_id: str):
    uri = f"ws://localhost:8080/api/v2/ws/sq/{sq_id}"
    async with websockets.connect(uri) as ws:
        while True:
            msg = await ws.recv()
            print(f"Match: {msg}")

asyncio.run(subscribe_sq(sq_id))
```

### 4.4 WebSocket 查询

通过 WebSocket 执行 Cypher 查询:

```javascript
const ws = new WebSocket("ws://localhost:8080/api/v2/ws/query");

ws.onopen = () => {
  ws.send(JSON.stringify({
    queryId: "q1",
    query: "MATCH (n:Person) RETURN n.name LIMIT 10"
  }));
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.type === "TabularResults") {
    console.log("Columns:", data.columns);
    console.log("Rows:", data.results);
  } else if (data.type === "QueryFinished") {
    console.log("Query complete");
  }
};
```

### 4.5 Recipe 管道

Recipe 允许将多个查询步骤组合为声明式计算管道:

```bash
# 创建 Recipe
curl -X POST http://localhost:8080/api/v2/recipes \
  -H "Content-Type: application/json" \
  -d '{
    "name": "user-analytics",
    "description": "用户分析管道",
    "steps": [
      {"query": "MATCH (n:Person) RETURN COUNT(n) AS total"},
      {"query": "MATCH (n:Person) WHERE n.age > 18 RETURN AVG(n.age) AS avg_age"}
    ]
  }' | jq .

# 执行 Recipe
curl -X POST http://localhost:8080/api/v2/recipes/user-analytics/execute | jq .

# 查看执行历史
curl http://localhost:8080/api/v2/recipes/user-analytics/runs | jq .

# 删除 Recipe
curl -X DELETE http://localhost:8080/api/v2/recipes/user-analytics | jq .
```

---

## 5. 数据摄入

### 5.1 JSONL 文件摄入

Nexora-RS 支持通过 REST API 摄入 JSONL 格式文件，每行一个 JSON 对象。

**准备数据文件**:

```bash
cat > users.jsonl << 'EOF'
{"id":"user-001","name":"Alice","age":30,"type":"Person"}
{"id":"user-002","name":"Bob","age":25,"type":"Person"}
{"id":"user-003","name":"Charlie","age":35,"type":"Person"}
EOF
```

**启动摄入**:

```bash
curl -X POST http://localhost:8080/api/v2/ingest/file \
  -H "Content-Type: application/json" \
  -d '{
    "path": "users.jsonl",
    "id_field": "id"
  }' | jq .
```

响应:
```json
{
  "status": "started",
  "path": "/abs/path/users.jsonl",
  "name": "ingest-1719500000"
}
```

> **安全说明**: 文件路径必须是相对路径，且不允许包含 `..`。可通过 `--allow-ingest-dir` 限制允许的目录范围。

**查看活跃摄入任务**:

```bash
curl http://localhost:8080/api/v2/ingest | jq .
```

**停止摄入任务**:

```bash
curl -X DELETE http://localhost:8080/api/v2/ingest/ingest-1719500000 | jq .
```

### 5.2 通过 Cypher 批量创建

```cypher
UNWIND [
  {name: "Dave", age: 40},
  {name: "Eve", age: 28},
  {name: "Frank", age: 50}
] AS person
CREATE (n:Person {name: person.name, age: person.age})
```

### 5.3 Kafka 流摄入

Nexora-RS 支持从 Kafka 实时消费数据并写入图数据库。需要启用 `kafka` feature 编译。

**编译 Kafka 支持**:

```bash
cargo build --release -p nexora-app --features kafka
```

**通过 CLI 启动 Kafka 摄入**:

```bash
./target/release/nexora-app \
  --kafka-brokers localhost:9092 \
  --kafka-topic graph-events \
  --kafka-group-id nexora-consumer
```

**通过 REST API 启动 Kafka 流**:

```bash
curl -X POST http://localhost:8080/api/v2/streams/kafka \
  -H "Content-Type: application/json" \
  -d '{
    "brokers": "localhost:9092",
    "topic": "graph-events",
    "group_id": "nexora-consumer"
  }' | jq .
```

**查看活跃流**:

```bash
curl http://localhost:8080/api/v2/streams | jq .
```

**停止流**:

```bash
curl -X DELETE http://localhost:8080/api/v2/streams/kafka-1719500000 | jq .
```

### 5.4 Kinesis / Pulsar 连接器

Kinesis 和 Pulsar 连接器通过 `nexora-stream` crate 实现，使用方式与 Kafka 类似。消息格式为 JSON 对象，`id` 字段用作 NexoraId，其余字段作为节点属性。

---

## 6. 向量搜索

Nexora-RS 内置 HNSW（Hierarchical Navigable Small World）向量索引，支持 k-NN 相似性搜索。

### 6.1 创建 HNSW 索引

向量索引在服务启动时自动初始化。通过 API 插入向量数据来构建索引:

```bash
# 为节点插入向量
QID=$(echo -n "doc-001" | xxd -p)

curl -X POST http://localhost:8080/api/v2/vector/index \
  -H "Content-Type: application/json" \
  -d "{
    \"qid\": \"$QID\",
    \"vector\": [0.1, 0.2, 0.3, 0.4, 0.5]
  }" | jq .
```

响应:
```json
{
  "status": "indexed",
  "qid": "646f632d303031",
  "index_size": 1
}
```

### 6.2 k-NN 搜索

```bash
curl -X POST http://localhost:8080/api/v2/vector/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
    "k": 5
  }' | jq .
```

响应:
```json
{
  "query": [0.1, 0.2, 0.3, 0.4, 0.5],
  "k": 5,
  "neighbors": [
    {"qid": "646f632d303031", "distance": 0.0},
    {"qid": "646f632d303032", "distance": 0.015},
    {"qid": "646f632d303033", "distance": 0.089}
  ]
}
```

### 6.3 向量管理

```bash
# 获取节点的向量
curl "http://localhost:8080/api/v2/vector/node/$QID" | jq .

# 删除节点的向量
curl -X DELETE "http://localhost:8080/api/v2/vector/node/$QID" | jq .
```

### 6.4 相似性查询结合图查询

典型工作流: 先通过向量搜索找到相似节点，再用 Cypher 查询图关系:

```bash
# 1. 向量搜索获取相似节点
NEIGHBORS=$(curl -s -X POST http://localhost:8080/api/v2/vector/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.1, 0.2, 0.3], "k": 3}' | jq -r '.neighbors[0].qid')

# 2. 查询该节点的属性和关系
curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"MATCH (n)-[r]->(m) WHERE id(n) = '$NEIGHBORS' RETURN n, type(r), m\"}"
```

---

## 7. 认证与安全

### 7.1 API Key 设置

启用认证需要使用 `--require-auth` 标志并设置密钥:

```bash
./target/release/nexora-app \
  --host 0.0.0.0 \
  --port 8080 \
  --require-auth \
  --auth-secret "your-secure-secret-key-here"
```

也可通过环境变量设置密钥:

```bash
export NEXORA_AUTH_SECRET="your-secure-secret-key-here"
./target/release/nexora-app --require-auth
```

### 7.2 生成认证 Token

```bash
curl -X POST http://localhost:8080/api/v2/auth/token \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "role": "operator"
  }' | jq .
```

响应:
```json
{
  "token": "eyJ1c2VyX2lkIjoiYWxpY2UiLCJyb2xlIjoib3BlcmF0b3IiLCJleHAiOjE3MTk1MDAwMDB9.signature",
  "user_id": "alice",
  "role": "operator"
}
```

### 7.3 使用 Token 访问 API

```bash
TOKEN="eyJ1c2VyX2lkIjoiYWxpY2Ui..."

curl -X POST http://localhost:8080/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query": "MATCH (n) RETURN n LIMIT 10"}'
```

### 7.4 RBAC 角色

Nexora-RS 实现三级角色访问控制:

| 角色 | 权限 |
|------|------|
| `admin` | 完全访问：所有端点（包括 Standing Query 管理） |
| `operator` | 读写：属性、边、摄入、Cypher 查询 |
| `readonly` | 只读：查询、健康检查、指标 |

**角色层级**: `admin` > `operator` > `readonly`

**按角色访问的端点**:
- 公开端点（无需认证）: `/api/v2/health`, `/api/v2/metrics`, `/api/v2/auth/token`, `/metrics`, `/`
- 查询端点（任意角色）: `POST /api/v2/query/cypher`, `GET /api/v2/graph/...`
- 写端点（operator+）: `PUT /api/v2/graph/...`, `POST /api/v2/ingest/...`, `POST /api/v2/streams/...`
- 管理端点（admin）: `POST/DELETE /api/v2/standing-query/...`

### 7.5 TLS 配置

**生成自签名证书（开发用）**:

```bash
./target/release/nexora-app --gen-tls-cert ./tls
# 生成 tls/cert.pem 和 tls/key.pem
```

**启用 HTTPS**:

```bash
./target/release/nexora-app \
  --tls-cert ./tls/cert.pem \
  --tls-key ./tls/key.pem
```

启用 TLS 后，WebSocket 连接自动升级为 WSS。

### 7.6 速率限制

默认启用速率限制（100 req/s，突发 200），可通过 CLI 调整:

```bash
./target/release/nexora-app \
  --rate-limit-rate 200.0 \
  --rate-limit-burst 500
```

禁用速率限制:

```bash
./target/release/nexora-app --rate-limit false
```

### 7.7 WAL 加密

支持 AES-256-GCM 加密 WAL 数据:

```bash
# 生成 32 字节密钥（64 个十六进制字符）
KEY=$(openssl rand -hex 32)

./target/release/nexora-app \
  --encrypt-wal \
  --encryption-key "$KEY"
```

也可通过文件或环境变量提供密钥:

```bash
echo "$KEY" > /etc/nexora/encryption.key
./target/release/nexora-app --encrypt-wal --encryption-key-file /etc/nexora/encryption.key

# 或
export NEXORA_ENCRYPTION_KEY="$KEY"
./target/release/nexora-app --encrypt-wal
```

### 7.8 CORS 配置

```bash
# 允许所有来源（开发默认）
./target/release/nexora-app --cors-origin "*"

# 限制特定域名（生产推荐）
./target/release/nexora-app --cors-origin "https://your-domain.com"
```

---

## 8. 错误处理

### 8.1 常见错误代码

| HTTP 状态码 | 含义 | 原因与解决方案 |
|-------------|------|---------------|
| 200 | OK | 请求成功 |
| 201 | Created | 资源创建成功（如 Standing Query） |
| 400 | Bad Request | 请求参数错误（无效的 NexoraId、Cypher 语法错误等） |
| 401 | Unauthorized | 未认证或 Token 无效 |
| 403 | Forbidden | 角色权限不足 |
| 404 | Not Found | 资源不存在（节点、Standing Query 等） |
| 409 | Conflict | 资源冲突（如 Recipe 名称重复） |
| 500 | Internal Server Error | 服务器内部错误（持久化失败等） |
| 501 | Not Implemented | 功能未编译（如未启用 Kafka feature） |

### 8.2 Cypher 查询错误

Cypher 语法错误返回 400 状态码，错误信息在 `error` 字段:

```json
{
  "columns": [],
  "rows": [],
  "error": "Parse error: unexpected token 'LIMIT' at position 42",
  "as_of": null
}
```

**常见 Cypher 错误及解决**:

| 错误 | 原因 | 解决方案 |
|------|------|---------|
| `unexpected token` | 子句顺序错误 | 确保 `MATCH → WHERE → WITH → RETURN → ORDER BY → SKIP → LIMIT` |
| `Node not found` | 引用了不存在的节点 | 检查 NexoraId 是否正确 |
| `Node temporarily unavailable` | 节点 Actor 被驱逐（LRU） | 增大 `--max-nodes-per-shard` |

### 8.3 错误响应格式

所有错误响应使用统一 JSON 格式:

```json
{
  "error": "描述性错误信息"
}
```

### 8.4 重试策略

对于临时性错误（如节点不可用、超时），建议实现指数退避重试:

```python
import time
import requests

def cypher_with_retry(query: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                "http://localhost:8080/api/v2/query/cypher",
                json={"query": query},
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code >= 500:
                # 服务端错误，可重试
                wait = 2 ** attempt
                print(f"Server error {resp.status_code}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            # 客户端错误，不重试
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            print(f"Timeout, retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {max_retries} retries")
```

### 8.5 健康检查端点

```bash
# 存活检查
curl http://localhost:8080/api/v2/health/live
# {"alive": true}

# 就绪检查
curl http://localhost:8080/api/v2/health/ready
# {"ready": true, "shards": 256}

# 完整健康状态
curl http://localhost:8080/api/v2/health | jq .
```

### 8.6 系统信息与指标

```bash
# 系统信息
curl http://localhost:8080/api/v2/system/info | jq .

# Prometheus 格式指标
curl http://localhost:8080/metrics

# JSON 格式指标
curl http://localhost:8080/api/v2/metrics | jq .
```

### 8.7 存储管理

```bash
# 查看分层存储状态
curl http://localhost:8080/api/v2/storage/status | jq .

# 手动触发冷数据迁移
curl -X POST http://localhost:8080/api/v2/storage/migrate | jq .
```

---

## 9. UDF — 用户自定义函数

Nexora-RS 支持三种 UDF：原生表达式、WebAssembly 和 Python 脚本。

### 9.1 注册原生表达式 UDF

原生 UDF 使用 JSON 格式的表达式定义：

```bash
curl -X POST http://localhost:8080/api/v2/udf/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "greet",
    "language": "native",
    "code": "{\"expression\": \"concat(\\\"Hello, \\\", name)\", \"parameters\": [\"name\"]}"
  }'
```

### 9.2 注册 Python UDF

```bash
curl -X POST http://localhost:8080/api/v2/udf/python/greet \
  -H "Content-Type: application/json" \
  -d '{
    "code": "def greet(name): return f\"Hello, {name}!\""
  }'
```

### 9.3 注册 Wasm UDF

Wasm UDF 通过原始二进制 body 上传：

```bash
curl -X POST http://localhost:8080/api/v2/udf/wasm/my_wasm_fn \
  -H "Content-Type: application/octet-stream" \
  --data-binary @function.wasm
```

### 9.4 执行 UDF

通过通用执行端点：

```bash
curl -X POST http://localhost:8080/api/v2/udf/execute \
  -H "Content-Type: application/json" \
  -d '{
    "name": "greet",
    "inputs": {"name": "World"}
  }' | jq .
# {"result": "Hello, World!"}
```

或通过名称端点（适用于 Wasm/Python UDF）：

```bash
curl -X POST http://localhost:8080/api/v2/udf/greet/execute \
  -H "Content-Type: application/json" \
  -d '{"name": "World"}' | jq .
```

### 9.5 列出和删除 UDF

```bash
# 列出所有 UDF
curl http://localhost:8080/api/v2/udf | jq .

# 删除 UDF
curl -X DELETE http://localhost:8080/api/v2/udf/greet | jq .
```

---

## 10. 集群模式启动

### 10.1 单节点快速启动

详见第 1 节。开发模式：
```bash
./target/release/nexora-app --no-rocksdb --port 8080
```

### 10.2 双节点集群

```bash
# 节点 1
./target/release/nexora-app \
  --cluster \
  --node-id node-1 \
  --port 8080 \
  --cluster-listen-addr 127.0.0.1:7000 \
  --cluster-heartbeat-addr 127.0.0.1:7001 \
  --peer node-2:127.0.0.1:7002:127.0.0.1:7003 \
  --rocksdb-path ./data/node-1 \
  --wal-dir ./data/node-1/wal

# 节点 2（另一个终端）
./target/release/nexora-app \
  --cluster \
  --node-id node-2 \
  --port 8081 \
  --cluster-listen-addr 127.0.0.1:7002 \
  --cluster-heartbeat-addr 127.0.0.1:7003 \
  --peer node-1:127.0.0.1:7000:127.0.0.1:7001 \
  --rocksdb-path ./data/node-2 \
  --wal-dir ./data/node-2/wal
```

### 10.3 三节点集群 + Raft 共识

```bash
# 节点 1
./target/release/nexora-app \
  --cluster --node-id node-1 --port 8080 \
  --cluster-listen-addr 127.0.0.1:7000 \
  --cluster-heartbeat-addr 127.0.0.1:7001 \
  --raft-port 8000 \
  --raft-peer 127.0.0.1:8001 \
  --raft-peer 127.0.0.1:8002 \
  --peer node-2:127.0.0.1:7002:127.0.0.1:7003 \
  --peer node-3:127.0.0.1:7004:127.0.0.1:7005 \
  --rocksdb-path ./data/node-1 --wal-dir ./data/node-1/wal

# 节点 2
./target/release/nexora-app \
  --cluster --node-id node-2 --port 8081 \
  --cluster-listen-addr 127.0.0.1:7002 \
  --cluster-heartbeat-addr 127.0.0.1:7003 \
  --raft-port 8001 \
  --raft-peer 127.0.0.1:8000 \
  --raft-peer 127.0.0.1:8002 \
  --peer node-1:127.0.0.1:7000:127.0.0.1:7001 \
  --peer node-3:127.0.0.1:7004:127.0.0.1:7005 \
  --rocksdb-path ./data/node-2 --wal-dir ./data/node-2/wal

# 节点 3
./target/release/nexora-app \
  --cluster --node-id node-3 --port 8082 \
  --cluster-listen-addr 127.0.0.1:7004 \
  --cluster-heartbeat-addr 127.0.0.1:7005 \
  --raft-port 8002 \
  --raft-peer 127.0.0.1:8000 \
  --raft-peer 127.0.0.1:8001 \
  --peer node-1:127.0.0.1:7000:127.0.0.1:7001 \
  --peer node-2:127.0.0.1:7002:127.0.0.1:7003 \
  --rocksdb-path ./data/node-3 --wal-dir ./data/node-3/wal
```

### 10.4 查看集群状态

```bash
# 集群统计
curl http://localhost:8080/api/v2/cluster/stats | jq .

# Raft 状态（如启用）
curl http://localhost:8080/api/v2/cluster/raft | jq .
```

---

## 11. Python SDK 使用示例

Nexora-RS 提供 Python SDK，支持同步和异步两种模式。

### 11.1 安装

```bash
cd sdk/python
pip install -e .
```

### 11.2 同步客户端

```python
from nexora_rs import NexoraClient

with NexoraClient("http://localhost:8080") as client:
    # 创建节点
    client.set_property("alice", "name", "Alice")
    client.set_property("alice", "age", 30)
    client.set_property("alice", "labels", ["Person"])

    # 读取属性
    name = client.get_property("alice", "name")
    print(f"Name: {name}")

    # Cypher 查询
    results = client.query("MATCH (n:Person) RETURN n.name, n.age")
    for row in results:
        print(row)

    # 创建边
    client.set_property("bob", "name", "Bob")
    client.add_edge("alice", "bob", "KNOWS")

    # 健康检查
    health = client.health()
    print(f"Status: {health['status']}, Nodes: {health['active_nodes']}")
```

### 11.3 异步客户端

```python
import asyncio
from nexora_rs import AsyncNexoraClient

async def main():
    async with AsyncNexoraClient("http://localhost:8080") as client:
        # 批量创建节点
        ids = await client.bulk_create([
            {"labels": ["Person"], "properties": {"id": "alice", "name": "Alice"}},
            {"labels": ["Person"], "properties": {"id": "bob", "name": "Bob"}},
            {"labels": ["Person"], "properties": {"id": "carol", "name": "Carol"}},
        ])
        print(f"Created: {ids}")

        # 批量创建边
        await client.bulk_create_edges([
            {"from_id": "alice", "to_id": "bob", "edge_type": "KNOWS"},
            {"from_id": "bob", "to_id": "carol", "edge_type": "KNOWS"},
        ])

        # 查询朋友的朋友
        results = await client.query("""
            MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)
            WHERE a.name = 'Alice'
            RETURN c.name AS friend_of_friend
        """)
        for row in results:
            print(f"Friend of friend: {row['friend_of_friend']}")

asyncio.run(main())
```

### 11.4 Standing Query + WebSocket 订阅

```python
import asyncio
from nexora_rs import AsyncNexoraClient
from nexora_rs.streaming import StandingQuerySubscription

async def main():
    async with AsyncNexoraClient("http://localhost:8080") as client:
        # 注册 Standing Query
        sq_id = await client.create_standing_query(
            {
                "type": "PropertyFilter",
                "key": "speed",
                "condition": {"type": "GreaterThan", "value": 100},
            },
            name="high_speed_alert",
        )
        print(f"Standing Query ID: {sq_id}")

    # 通过 WebSocket 订阅匹配结果
    sub = StandingQuerySubscription("ws://localhost:8080", sq_id)
    await sub.start(lambda data: print(f"Alert! count={data.get('match_count')}"))

asyncio.run(main())
```

### 11.5 Cypher 流式查询

```python
import asyncio
from nexora_rs.streaming import CypherStreamSubscription

async def main():
    sub = CypherStreamSubscription("ws://localhost:8080")
    async for data in sub.stream("MATCH (n:Person) RETURN n.name, n.age"):
        if data.get("type") == "TabularResults":
            columns = data.get("columns", [])
            for row in data.get("results", []):
                print(dict(zip(columns, row)))
        elif data.get("type") == "QueryFinished":
            print("Stream complete.")

asyncio.run(main)
```

---

## 附录：运行模式

Nexora-RS 支持三种运行模式，通过 `--profile` 或 CLI 参数组合自动推断:

| 模式 | 说明 | 启动方式 |
|------|------|---------|
| `lite-ephemeral` | 内存存储，无持久化，零依赖 | `--no-rocksdb` |
| `single-durable` | RocksDB + WAL，单节点持久化 | 默认（不带 `--no-rocksdb` 和 `--cluster`） |
| `clustered` | 多节点分布式集群 | `--cluster` |

模式也可通过 `--profile` 显式指定:

```bash
./target/release/nexora-app --profile lite-ephemeral --no-rocksdb
./target/release/nexora-app --profile single-durable
./target/release/nexora-app --profile clustered --cluster --node-id node-1
```

模式验证规则:
- `lite-ephemeral` 必须搭配 `--no-rocksdb`，不能搭配 `--cluster`
- `single-durable` 不能搭配 `--cluster`
- `clustered` 必须搭配 `--cluster`，不能搭配 `--no-rocksdb`

---

## 附录：TOML 配置文件

除了 CLI 参数，Nexora-RS 还支持通过 `nexora.toml` 文件配置:

```toml
[server]
host = "0.0.0.0"
port = 8080

[graph]
num_shards = 256
max_nodes_per_shard = 10000

[storage.rocksdb]
path = "/var/nexora/data"
write_buffer_size = "64MB"
compression = "lz4"

[storage.wal]
dir = "/var/nexora/wal"
sync_policy = "every_n"
sync_interval = 1000

[logging]
level = "info"
format = "json"

[metrics]
enabled = true
```

配置优先级: CLI 参数 > 环境变量（`NEXORA_` 前缀）> `nexora.toml` > 默认值。

---

*本文档基于 Nexora-RS 源码编写，确保 API 示例的准确性。*
