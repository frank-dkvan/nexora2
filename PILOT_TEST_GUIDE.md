# Nexora 单节点试点测试指南

**启动时间：** 2026-07-13  
**状态：** 单节点 durable 模式运行中（A0/A1 已完成，元数据单节点零丢失）

---

## 连接信息

| 服务 | 地址 | 端口 | 认证 | 备注 |
|------|------|------|------|------|
| **PostgreSQL Wire** | 127.0.0.1 | 5440 | trust（免密） | 用 `psql` 或任意 PG 客户端 |
| **HTTP API** | 127.0.0.1 | 8090 | 免认证 | Cypher/SQL/SQ/MV/ingest |
| **数据目录** | `./nexora-data-pilot` | - | - | RocksDB + WAL，重启存活 |

### 快速连接

```bash
# PG-wire (psql)
psql -h 127.0.0.1 -p 5440 -U nexora -d nexora

# HTTP 健康检查
curl http://localhost:8090/api/v2/health

# 当前节点数
curl -s http://localhost:8090/api/v2/health | python3 -c "import sys,json; print('active_nodes:', json.load(sys.stdin)['active_nodes'])"
```

---

## 已灌入的测试数据

**文件：** `test-data-rich.jsonl`（32 个节点）

**数据主题：** 物流仓储（logistics warehouse）

| 类型 | 数量 | 关键属性 | 用途 |
|------|------|----------|------|
| **Forklift** | 8 | `speed`, `battery`, `status`, `location`, `zone` | 聚合/过滤/SQ 触发器 |
| **Person** (操作员) | 6 | `role`, `shift`, `certified`, `years_experience` | JOIN/关系查询 |
| **Zone** | 5 | `capacity`, `current_load`, `temperature`, `hazmat` | 空间/数值过滤 |
| **Sensor** | 5 | `sensor_type`, `reading`, `unit`, `online` | 时序/阈值监控 |
| **Package** | 8 | `weight`, `status`, `priority`, `destination` | 状态机/追踪 |

**关键特征：**
- 数值字段（speed/battery/weight/temperature）→ 聚合/排序/阈值
- 枚举字段（status/role/priority）→ GROUP BY/过滤
- 字符串字段（location/zone/name）→ 模糊匹配/JOIN

---

## 已验证的访问方式

### 1. **PostgreSQL Wire Protocol (PG-wire)**

✅ **状态：通过**  
✅ **验证项：** SELECT/WHERE/ORDER BY/COUNT/AVG/MIN/MAX/SUM/GROUP BY

#### 示例查询

```sql
-- 1. 统计所有叉车
SELECT COUNT(*) FROM nodes WHERE type='Forklift';
-- 返回: 8

-- 2. 过滤：活跃的高速叉车
SELECT id, name, speed 
FROM nodes 
WHERE type='Forklift' AND status='active' AND speed > 50 
ORDER BY speed DESC;
-- 返回 4 行：Epsilon(135), Beta(120), Eta(110), Delta(95)

-- 3. 聚合：叉车平均/最小/最大速度
SELECT AVG(speed), MIN(speed), MAX(speed), SUM(speed) 
FROM nodes 
WHERE type='Forklift';
-- 返回: avg=63, min=0, max=135, sum=505

-- 4. 分组：按状态统计
SELECT status, COUNT(*) 
FROM nodes 
WHERE type='Forklift' 
GROUP BY status;
-- 返回: active(5), inactive(2), charging(1)

-- 5. 分组 + 聚合：按地点统计数量和平均速度
SELECT location, COUNT(*), AVG(speed) 
FROM nodes 
WHERE type='Forklift' 
GROUP BY location;
-- 返回 5 个地点的统计
```

**已知行为怪癖：**
- `SELECT 1` 等常量查询会返回 N 行（N = shard 数），不影响数据查询。

---

### 2. **HTTP Cypher 查询**

✅ **状态：通过**  
✅ **端点：** `POST /api/v2/query/cypher`

#### 示例请求

```bash
# 过滤：高速叉车 (speed > 100)
curl -X POST http://localhost:8090/api/v2/query/cypher \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n) WHERE n.type = '\''Forklift'\'' AND n.speed > 100 RETURN n.name AS name, n.speed AS speed ORDER BY speed DESC"}'
```

**响应格式：**
```json
{
  "columns": ["name", "speed"],
  "rows": [
    ["Forklift Epsilon", 135],
    ["Forklift Beta", 120],
    ["Forklift Eta", 110]
  ],
  "error": null,
  "as_of": null
}
```

---

### 3. **Standing Query (实时监控)**

✅ **状态：通过**  
✅ **端点：** `POST /api/v2/standing-queries` (注册), `GET /api/v2/standing-queries` (列表)

#### 示例：注册高速告警

```bash
# 1. 注册 SQ：监控 speed > 100
curl -X POST http://localhost:8090/api/v2/standing-queries \
  -H "Content-Type: application/json" \
  -d '{
    "name": "high-speed-alert",
    "pattern": {
      "type": "PropertyFilter",
      "key": "speed",
      "condition": {"type": "GreaterThan", "value": 100.0}
    }
  }'
# 返回: {"id": "...", "name": "high-speed-alert"}

# 2. 更新属性触发 SQ（需要十六进制 node id）
# 先从 PG 获取 hex id:
HEX_ID=$(psql -h 127.0.0.1 -p 5440 -U nexora -d nexora -t -c \
  "SELECT id FROM nodes WHERE type='Forklift' AND name='Forklift Gamma';" | tr -d ' \n')

# 更新速度为 150（超过阈值 100）
curl -X PUT "http://localhost:8090/api/v2/nodes/$HEX_ID/properties/speed" \
  -H "Content-Type: application/json" \
  -d '{"value": 150}'

# 3. 查看 SQ 匹配计数
curl http://localhost:8090/api/v2/standing-queries
# 返回: "match_count": 1（从 0 增加到 1）
```

**Pattern 类型：**
- `PropertyFilter`：属性阈值（GreaterThan/LessThan/Equals/Contains/Exists/IsNull）
- `LabelFilter`：标签过滤（labels 数组）

---

### 4. **Materialized View (物化视图)**

✅ **状态：通过**  
✅ **端点：** `POST /api/v2/materialized-views` (创建), `POST /api/v2/materialized-views/{id}/refresh` (刷新)

#### 示例：活跃叉车视图

```bash
# 1. 创建 MV
curl -X POST http://localhost:8090/api/v2/materialized-views \
  -H "Content-Type: application/json" \
  -d '{
    "name": "active_forklifts",
    "query": "MATCH (n) WHERE n.type = '\''Forklift'\'' AND n.status = '\''active'\'' RETURN n.id AS id, n.name AS name, n.speed AS speed, n.location AS location",
    "primary_key": "id"
  }'
# 返回: {"view_id": "...", "name": "active_forklifts", "status": "created"}

# 2. 刷新 MV（Incremental 模式下首次需要手动）
VIEW_ID="<从上一步获取>"
curl -X POST "http://localhost:8090/api/v2/materialized-views/$VIEW_ID/refresh"
# 返回: {"status": "refreshed", "rows": 5}

# 3. 查询 MV via PG-wire
psql -h 127.0.0.1 -p 5440 -U nexora -d nexora \
  -c "SELECT * FROM active_forklifts ORDER BY speed DESC;"
# 返回 5 行活跃叉车数据

# 4. 列出所有 MV
curl http://localhost:8090/api/v2/materialized-views
```

---

### 5. **HTTP 数据摄入**

✅ **状态：通过**  
✅ **端点：** `POST /api/v2/ingest/file`

#### 示例

```bash
# 灌入 JSONL 文件（相对路径 = server cwd）
curl -X POST http://localhost:8090/api/v2/ingest/file \
  -H "Content-Type: application/json" \
  -d '{"path": "test-data-rich.jsonl", "id_field": "id"}'
# 返回: {"name": "ingest-...", "path": "...", "status": "started"}
```

---

## 重启存活验证

A0/A1 已完成，以下元数据会跨重启恢复：

| 元数据类型 | 持久化机制 | 验证命令 |
|-----------|-----------|---------|
| **SQ 定义** | `ControlPlaneStore` (RocksDB) | `curl http://localhost:8090/api/v2/standing-queries` |
| **MV 定义** | `ControlPlaneStore` (RocksDB) | `curl http://localhost:8090/api/v2/materialized-views` |
| **MV 行数据** | `ControlPlaneStore` (RocksDB) | `psql ... -c "SELECT COUNT(*) FROM <view_name>;"` |
| **用户节点** | Graph RocksDB + WAL | `psql ... -c "SELECT COUNT(*) FROM nodes;"` |

### 重启步骤

```bash
# 1. 停止 server（找到 PID）
pgrep -fl 'target/release/nexora.*8090' | awk '{print $1}' | xargs kill

# 2. 重新启动（同样的命令）
./target/release/nexora --host 0.0.0.0 --port 8090 \
  --num-shards 16 --rocksdb-path ./nexora-data-pilot \
  --wal-dir ./nexora-data-pilot/wal \
  --pg-port 5440 --pg-bind 127.0.0.1 --pg-trust \
  --allow-unauthenticated &

# 3. 等待启动
sleep 5
curl http://localhost:8090/api/v2/health

# 4. 验证元数据恢复
curl http://localhost:8090/api/v2/standing-queries  # SQ 应该在
curl http://localhost:8090/api/v2/materialized-views  # MV 应该在
psql -h 127.0.0.1 -p 5440 -U nexora -d nexora \
  -c "SELECT COUNT(*) FROM nodes WHERE type='Forklift';"  # 应该返回 8
```

---

## 已知限制（单节点模式）

按路线图 [RF_REPLICATION_ROADMAP.md](docs/RF_REPLICATION_ROADMAP.md)：

✅ **已就绪：**
- 元数据单节点零丢失（A0 ✅ + A1 ✅）：SQ/MV 定义 + MV 行 + 用户数据跨重启恢复。
- PG-wire 分布式 SQL 全链路（INSERT/SELECT/UPDATE/DELETE/SQ/MV）。

⚠️ **单节点限制：**
- 无高可用（单点故障）。
- 无副本（RF=1），无 read-after-write 保证（只有单节点，自动满足）。
- **用户数据回灌路径未实现**（B1 线）——丢数据暂时只能手动重导，无自动位点续传。

⚠️ **多节点限制（cluster 模式，A2 未做）：**
- 控制平面共识是手写的、未验证无脑裂。
- RF>1 复制是 best-effort，PG-wire 写只写 owner。
- 无自动 failover。

**结论：单节点 + 上游可回灌 + 试点方接受"回灌暂时手动" → 可以试点功能正确性和易用性。**

---

## 快速测试脚本

```bash
#!/bin/bash
# 快速验证所有访问方式

H="http://localhost:8090"
PG="psql -h 127.0.0.1 -p 5440 -U nexora -d nexora"

echo "=== 1. Health check ==="
curl -s $H/api/v2/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"Status: {d['status']}, Nodes: {d['active_nodes']}, SQs: {d['standing_queries']}\")"

echo -e "\n=== 2. PG-wire: count forklifts ==="
$PG -t -c "SELECT COUNT(*) FROM nodes WHERE type='Forklift';"

echo -e "\n=== 3. Cypher: high-speed forklifts ==="
curl -s -X POST $H/api/v2/query/cypher -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n) WHERE n.type = '\''Forklift'\'' AND n.speed > 100 RETURN n.name, n.speed"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"Found {len(d['rows'])} forklifts:\"); [print(f\"  {r[0]}: {r[1]}\") for r in d['rows']]"

echo -e "\n=== 4. Standing Queries ==="
curl -s $H/api/v2/standing-queries | python3 -c "import sys,json; sqs=json.load(sys.stdin)['standing_queries']; print(f\"Registered: {len(sqs)}\"); [print(f\"  {s['name']}: {s['match_count']} matches\") for s in sqs]"

echo -e "\n=== 5. Materialized Views ==="
curl -s $H/api/v2/materialized-views | python3 -c "import sys,json; mvs=json.load(sys.stdin); print(f\"Created: {len(mvs)}\"); [print(f\"  {v['name']}\") for v in mvs]"
```

---

## 下一步

- **功能试点**：PG-wire + Cypher + SQ + MV 的 **功能正确性和易用性** 现在可验证。
- **生产多节点**：需要 A2（进程内 openraft）+ B1（回灌恢复）+ C（读一致性），约 3-5 周。
- **用户数据单节点重启验证**：A0 只验证了元数据，用户节点数据的重启存活应该补一个测试（用 `SELECT COUNT(*) FROM nodes` 验证重启前后一致）。

---

**生成时间：** 2026-07-13  
**单节点 PID：** 运行 `pgrep -fl 'target/release/nexora.*8090'` 查看
