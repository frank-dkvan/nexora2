# Nexora 2.0 API Endpoint Reference

**版本**: 2.0  
**基础路径**: `/api`  
**注意**: Nexora 2.0 已移除 `/api/v2` 前缀，所有端点直接使用 `/api`

---

## 1. 核心查询端点

### Cypher 查询
```http
POST /api/query/cypher
Content-Type: application/json

{
  "query": "MATCH (n:Person) RETURN n LIMIT 10"
}
```

### SQL 查询
```http
POST /api/query/sql
Content-Type: application/json

{
  "query": "SELECT * FROM nodes WHERE label = 'Person'"
}
```

### 时间旅行查询
```http
GET /api/graph/history?at=2026-01-01T00:00:00Z
```

### 分布式遍历
```http
POST /api/graph/traverse
Content-Type: application/json

{
  "start": ["node-id-1", "node-id-2"],
  "edge_type": "KNOWS",
  "max_depth": 3
}
```

---

## 2. Standing Query 端点

**注意**: Nexora 同时支持单数和复数两种形式的端点，功能完全相同。

### 创建 Standing Query
```http
# 两种形式均可
POST /api/standing-query
POST /api/standing-queries

Content-Type: application/json

{
  "name": "high-value-users",
  "query": "MATCH (u:User) WHERE u.value > 1000 RETURN u"
}
```

### 列出所有 Standing Queries
```http
GET /api/standing-query
GET /api/standing-queries
```

### 获取单个 Standing Query
```http
GET /api/standing-query/{id}
GET /api/standing-queries/{id}
```

### 删除 Standing Query
```http
DELETE /api/standing-query/{id}
DELETE /api/standing-queries/{id}
```

---

## 3. Materialized View 端点

### 创建物化视图
```http
POST /api/materialized-views
Content-Type: application/json

{
  "name": "user-summary",
  "query": "MATCH (u:User)-[:PURCHASED]->(p:Product) RETURN u.id, COUNT(p) AS purchases"
}
```

### 列出所有物化视图
```http
GET /api/materialized-views
```

### 获取单个物化视图
```http
GET /api/materialized-views/{view_id}
```

### 刷新物化视图
```http
POST /api/materialized-views/{view_id}/refresh
```

### 查询物化视图数据
```http
GET /api/materialized-views/{view_id}/data
```

### 关联 Standing Query
```http
POST /api/materialized-views/{view_id}/link-sq
Content-Type: application/json

{
  "standing_query_id": "sq-123"
}
```

### 删除物化视图
```http
DELETE /api/materialized-views/{view_id}
```

---

## 4. Ontology (Domain Schema) 端点

### 列出所有 Ontologies
```http
GET /api/ontologies
```

### 创建 Ontology (JSON)
```http
POST /api/ontologies
Content-Type: application/json

{
  "domain": "ecommerce",
  "entities": [...],
  "relations": [...]
}
```

### 创建 Ontology (YAML)
```http
POST /api/ontologies/yaml
Content-Type: application/x-yaml

domain: ecommerce
entities:
  - name: User
    properties: [...]
```

### 验证 Ontology
```http
POST /api/ontologies/validate
Content-Type: application/json

{
  "domain": "ecommerce",
  "entities": [...]
}
```

### 获取单个 Ontology
```http
GET /api/ontologies/{domain}
```

### 删除 Ontology
```http
DELETE /api/ontologies/{domain}
```

---

## 5. 节点/边操作端点

**注意**: 同时支持两种 RESTful 风格。

### 获取节点属性
```http
# 两种形式均可
GET /api/graph/node/{qid}/property/{key}
GET /api/nodes/{qid}/properties/{key}
```

### 设置节点属性
```http
PUT /api/graph/node/{qid}/property/{key}
PUT /api/nodes/{qid}/properties/{key}
Content-Type: application/json

{
  "value": "new value"
}
```

### 获取节点的边
```http
GET /api/graph/node/{qid}/edges
GET /api/nodes/{qid}/edges
```

### 添加边
```http
POST /api/graph/node/{qid}/edges
POST /api/nodes/{qid}/edges
Content-Type: application/json

{
  "target": "target-node-id",
  "type": "KNOWS",
  "properties": {"since": "2020"}
}
```

---

## 6. 向量搜索端点

### 索引向量
```http
POST /api/vector/index
Content-Type: application/json

{
  "node_id": "node-123",
  "vector": [0.1, 0.2, 0.3, ...],
  "dimension": 768
}
```

### k-NN 搜索
```http
POST /api/vector/search
Content-Type: application/json

{
  "vector": [0.1, 0.2, 0.3, ...],
  "k": 10,
  "filter": {"label": "Person"}
}
```

### 获取节点向量
```http
GET /api/vector/node/{qid}
GET /api/vectors/{qid}
```

### 删除节点向量
```http
DELETE /api/vector/node/{qid}
DELETE /api/vectors/{qid}
```

---

## 7. 数据摄入端点

### 文件摄入
```http
POST /api/ingest/file
Content-Type: multipart/form-data

file: <csv/json file>
format: csv
mapping: {"node_id": "id", "name": "name"}
```

### 批量摄入
```http
POST /api/ingest/bulk
Content-Type: application/json

{
  "nodes": [...],
  "edges": [...]
}
```

### 列出摄入任务
```http
GET /api/ingest
```

### 删除摄入任务
```http
DELETE /api/ingest/{name}
```

---

## 8. 流处理端点

### 列出所有流
```http
GET /api/streams
```

### 启动 Kafka 流
```http
POST /api/streams/kafka
Content-Type: application/json

{
  "name": "kafka-stream-1",
  "brokers": ["localhost:9092"],
  "topic": "graph-events",
  "group_id": "nexora-consumer"
}
```

### 删除流
```http
DELETE /api/streams/{name}
```

---

## 9. UDF (User Defined Function) 端点

### 注册 UDF
```http
POST /api/udf/register
Content-Type: application/json

{
  "name": "my_function",
  "language": "python",
  "code": "def my_function(x): return x * 2"
}
```

### 注册 Python UDF
```http
POST /api/udf/python/{name}
Content-Type: application/json

{
  "code": "def process(data): return data.upper()"
}
```

### 注册 Wasm UDF
```http
POST /api/udf/wasm/{name}
Content-Type: application/octet-stream

<wasm binary>
```

### 执行 UDF
```http
POST /api/udf/execute
Content-Type: application/json

{
  "name": "my_function",
  "args": [42]
}
```

### 按名称执行 UDF
```http
POST /api/udf/{name}/execute
Content-Type: application/json

{
  "args": [42]
}
```

### 列出所有 UDF
```http
GET /api/udf
```

### 删除 UDF
```http
DELETE /api/udf/{name}
```

---

## 10. Recipe 端点

### 列出所有 Recipes
```http
GET /api/recipes
```

### 创建 Recipe
```http
POST /api/recipes
Content-Type: application/json

{
  "name": "user-analytics",
  "steps": [...]
}
```

### 获取 Recipe
```http
GET /api/recipes/{name}
```

### 执行 Recipe
```http
POST /api/recipes/{name}/execute
```

### 获取执行历史
```http
GET /api/recipes/{name}/runs
```

### 删除 Recipe
```http
DELETE /api/recipes/{name}
```

---

## 11. SQL DDL 端点

### 执行 SQL DDL
```http
POST /api/sql/ddl
Content-Type: application/json

{
  "ddl": "CREATE MATERIALIZED VIEW user_stats AS SELECT u.id, COUNT(p.id) FROM users u JOIN purchases p ON u.id = p.user_id GROUP BY u.id"
}
```

---

## 12. 系统管理端点

### 系统信息
```http
GET /api/system/info
```

### 系统配置
```http
GET /api/system/config
```

### 管理状态
```http
GET /api/admin/status
```

### 备份
```http
POST /api/admin/backup
Content-Type: application/json

{
  "path": "/backup/nexora-2026-07-25.tar.gz"
}
```

### 恢复
```http
POST /api/admin/restore
Content-Type: application/json

{
  "path": "/backup/nexora-2026-07-25.tar.gz"
}
```

### 重建索引
```http
POST /api/admin/reindex
```

### 密钥轮换
```http
POST /api/admin/rotate-key
```

### 优雅下线
```http
POST /api/admin/drain
```

---

## 13. 集群管理端点

*需要启用 cluster 模式 (`--cluster` 参数)*

### 集群统计
```http
GET /api/cluster/stats
```

### 添加节点
```http
POST /api/cluster/add-node
Content-Type: application/json

{
  "node_id": "node-4",
  "address": "192.168.1.14:7001"
}
```

### 删除节点
```http
POST /api/cluster/remove-node
Content-Type: application/json

{
  "node_id": "node-3"
}
```

### Raft 状态
```http
GET /api/cluster/raft
```

---

## 14. 健康检查端点

### 健康检查
```http
GET /api/health
```

**响应示例**:
```json
{
  "status": "healthy",
  "active_nodes": 3,
  "standing_queries": 5,
  "uptime_seconds": 86400
}
```

### 就绪检查
```http
GET /api/health/ready
```

### 存活检查
```http
GET /api/health/live
```

---

## 15. Metrics 端点

### JSON Metrics
```http
GET /api/metrics
```

### Prometheus Metrics
```http
GET /metrics
```

---

## 16. WebSocket 端点

### WebSocket 查询
```
ws://localhost:8080/api/ws/query
```

**用法**:
```javascript
const ws = new WebSocket('ws://localhost:8080/api/ws/query');
ws.send(JSON.stringify({query: "MATCH (n) RETURN n LIMIT 10"}));
```

### 订阅所有 Standing Queries
```
ws://localhost:8080/api/ws/sq
```

### 订阅特定 Standing Query
```
ws://localhost:8080/api/ws/sq/{id}
```

### 订阅 Metrics 流
```
ws://localhost:8080/api/ws/metrics
```

---

## 17. 认证端点

### 获取 Token
```http
POST /api/auth/token
Content-Type: application/json

{
  "username": "admin",
  "password": "secret"
}
```

**响应**:
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_in": 3600
}
```

### 使用 Token
```http
GET /api/some-endpoint
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

---

## 18. 端点访问控制

| 端点类别 | 所需角色 | 示例 |
|---------|---------|------|
| 健康检查/Metrics | 无需认证 | `/api/health`, `/api/metrics` |
| 查询端点 | 任意角色 | `POST /api/query/cypher` |
| 写入端点 | operator+ | `PUT /api/graph/node/{id}/property/{key}` |
| 管理端点 | admin | `POST /api/admin/backup` |
| 集群管理 | admin | `POST /api/cluster/add-node` |

---

## 19. 错误响应格式

所有错误响应遵循统一格式：

```json
{
  "error": {
    "code": "INVALID_QUERY",
    "message": "Syntax error at line 1, column 10",
    "details": {
      "line": 1,
      "column": 10
    }
  }
}
```

---

## 20. 分页支持

支持分页的端点使用统一参数：

```http
GET /api/standing-queries?page=2&page_size=50
```

**响应包含分页元数据**:
```json
{
  "data": [...],
  "pagination": {
    "page": 2,
    "page_size": 50,
    "total": 150,
    "total_pages": 3
  }
}
```

---

## 相关文档

- [API Tutorial](../api-tutorial.md) - 完整的 API 使用教程
- [认证指南](../ops/SECURITY.md) - 认证和授权配置
- [集群操作](../cluster-ops.md) - 集群模式 API 使用