# 航空货运站运控系统 - Nexora试点实施指南

> 首个生产试点应用  
> 目标: 替代传统关系数据库,实现实时图计算  
> 预期上线: 2026年第四季度

---

## 一、业务需求分析

### 1.1 现有系统痛点

**传统关系数据库方案**:
- ❌ 多表JOIN性能差 (货物-仓位-航班-人员 4-5层关联)
- ❌ 实时统计查询慢 (每次需要全表扫描)
- ❌ 关系变更代价高 (需要更新多个表)
- ❌ 难以支持图算法 (最短路径、资源调度)

**业务需求**:
- ✅ 货物全生命周期追踪 (入库→配载→出库)
- ✅ 实时仓位状态查询 (< 100ms响应)
- ✅ 智能配载优化 (基于图算法)
- ✅ 多维度统计分析 (按目的地、时间、优先级)

### 1.2 Nexora的优势

| 需求 | 关系数据库 | Nexora |
|------|-----------|--------|
| 关联查询 | 多表JOIN,慢 | 图遍历,快 |
| 实时统计 | 全表扫描 | 物化视图 |
| 关系变更 | 更新多表 | 修改边 |
| 复杂图算法 | 不支持 | 原生支持 |
| 可视化 | 需要额外工具 | 内置Dashboard |

---

## 二、数据模型设计

### 2.1 核心实体

```cypher
// 货物
CREATE (c:Cargo {
  id: "CG20260710001",           // 货物编号
  weight: 450.5,                  // 重量(kg)
  volume: 2.5,                    // 体积(m³)
  destination: "PVG",             // 目的地机场代码
  priority: "HIGH",               // 优先级: HIGH/NORMAL/LOW
  category: "GENERAL",            // 类别: GENERAL/DANGEROUS/PERISHABLE
  shipper: "顺丰速运",             // 发货方
  consignee: "京东物流",           // 收货方
  awb_no: "999-12345678",         // 空运提单号
  pieces: 5,                      // 件数
  declared_value: 50000,          // 申报价值
  status: "IN_STORAGE",           // 状态: IN_STORAGE/ASSIGNED/LOADED/DEPARTED
  inbound_time: 1720598400000,    // 入库时间(时间戳)
  remarks: "易碎品,轻拿轻放"        // 备注
})

// 航班
CREATE (f:Flight {
  flight_no: "CA1234",            // 航班号
  aircraft_type: "B777-300ER",    // 机型
  departure_time: 1720620000000,  // 起飞时间
  destination: "PVG",             // 目的地
  cargo_capacity: 18000,          // 货舱容量(kg)
  volume_capacity: 150,           // 体积容量(m³)
  status: "SCHEDULED",            // SCHEDULED/LOADING/DEPARTED/ARRIVED
  current_load: 12500,            // 当前载重
  current_volume: 95,             // 当前体积
  gate: "G12",                    // 登机口
  remarks: ""
})

// 仓位
CREATE (s:Storage {
  id: "A-01-05",                  // 仓位编号
  zone: "A",                      // 区域
  row: 1,                         // 排
  column: 5,                      // 列
  type: "STANDARD",               // STANDARD/COLD/DANGEROUS
  capacity: 50,                   // 容量(件)
  occupied: 15,                   // 已占用
  temperature: 25,                // 温度(°C,冷库专用)
  status: "AVAILABLE"             // AVAILABLE/FULL/MAINTENANCE
})

// 操作人员
CREATE (p:Staff {
  id: "EMP001",
  name: "张三",
  role: "LOADER",                 // LOADER/INSPECTOR/SUPERVISOR
  shift: "DAY",                   // DAY/NIGHT
  status: "ON_DUTY"
})
```

### 2.2 关系定义

```cypher
// 货物存储关系
(c:Cargo)-[:STORED_IN {
  stored_at: 1720598500000,       // 存储时间
  location_detail: "货架3层",      // 具体位置
  operator: "张三"                 // 操作员
}]->(s:Storage)

// 货物配载关系
(c:Cargo)-[:ASSIGNED_TO {
  assigned_at: 1720610000000,     // 配载时间
  position: "前舱左侧",            // 机舱位置
  operator: "李四"
}]->(f:Flight)

// 货物处理关系
(c:Cargo)-[:HANDLED_BY {
  action: "INSPECTION",           // INSPECTION/LOADING/UNLOADING
  timestamp: 1720598600000,
  duration: 300,                  // 耗时(秒)
  result: "PASSED"                // PASSED/FAILED
}]->(p:Staff)

// 航班使用仓位
(f:Flight)-[:USES {
  reserved_at: 1720590000000,
  pieces: 20                      // 预留件数
}]->(s:Storage)

// 货物依赖关系(需要同机运输)
(c1:Cargo)-[:MUST_WITH]->(c2:Cargo)
```

---

## 三、核心业务查询

### 3.1 入库操作

```cypher
// 1. 创建货物节点
CREATE (c:Cargo {
  id: $cargo_id,
  weight: $weight,
  volume: $volume,
  destination: $destination,
  priority: $priority,
  inbound_time: timestamp(),
  status: "IN_STORAGE"
})

// 2. 分配仓位(选择可用且距离装机点近的)
MATCH (s:Storage {zone: $preferred_zone, status: "AVAILABLE"})
WHERE s.occupied < s.capacity
WITH s, s.occupied * 1.0 / s.capacity AS utilization
ORDER BY utilization ASC
LIMIT 1
CREATE (c)-[:STORED_IN {
  stored_at: timestamp(),
  operator: $operator
}]->(s)
SET s.occupied = s.occupied + 1
RETURN c, s
```

### 3.2 智能配载

```cypher
// 为航班自动配载货物(按优先级和目的地)
MATCH (f:Flight {flight_no: $flight_no})
MATCH (c:Cargo {destination: f.destination, status: "IN_STORAGE"})
WHERE NOT (c)-[:ASSIGNED_TO]->()
  AND f.current_load + c.weight <= f.cargo_capacity
  AND f.current_volume + c.volume <= f.volume_capacity
WITH f, c
ORDER BY 
  CASE c.priority 
    WHEN "HIGH" THEN 1 
    WHEN "NORMAL" THEN 2 
    ELSE 3 
  END,
  c.inbound_time ASC
LIMIT 50
CREATE (c)-[:ASSIGNED_TO {
  assigned_at: timestamp(),
  operator: $operator
}]->(f)
SET c.status = "ASSIGNED",
    f.current_load = f.current_load + c.weight,
    f.current_volume = f.current_volume + c.volume
RETURN count(c) AS assigned_count,
       sum(c.weight) AS total_weight,
       sum(c.volume) AS total_volume
```

### 3.3 货物追踪

```cypher
// 查询货物完整历史轨迹
MATCH (c:Cargo {id: $cargo_id})
OPTIONAL MATCH (c)-[r:STORED_IN]->(s:Storage)
OPTIONAL MATCH (c)-[a:ASSIGNED_TO]->(f:Flight)
OPTIONAL MATCH (c)-[h:HANDLED_BY]->(p:Staff)
RETURN c,
       collect(DISTINCT {
         type: "STORAGE",
         storage: s.id,
         time: r.stored_at
       }) AS storage_history,
       collect(DISTINCT {
         type: "ASSIGNMENT",
         flight: f.flight_no,
         time: a.assigned_at
       }) AS flight_history,
       collect(DISTINCT {
         type: "HANDLING",
         staff: p.name,
         action: h.action,
         time: h.timestamp
       }) AS handling_history
ORDER BY time ASC
```

### 3.4 实时统计

```cypher
// 当前仓储概况
MATCH (s:Storage)
OPTIONAL MATCH (s)<-[:STORED_IN]-(c:Cargo)
WITH s.zone AS zone,
     count(DISTINCT s) AS total_slots,
     sum(s.occupied) AS occupied_count,
     count(DISTINCT c) AS cargo_count
RETURN zone,
       total_slots,
       occupied_count,
       cargo_count,
       round(occupied_count * 100.0 / total_slots, 2) AS utilization_pct
ORDER BY zone
```

---

## 四、Standing Queries(实时监控)

### 4.1 超时预警

```cypher
// SQ-01: 货物在仓超过4小时未配载
CREATE STANDING QUERY cargo_timeout AS
MATCH (c:Cargo)-[:STORED_IN]->(s:Storage)
WHERE NOT (c)-[:ASSIGNED_TO]->(:Flight)
  AND c.inbound_time < timestamp() - 4 * 3600 * 1000
  AND c.status = "IN_STORAGE"
RETURN c.id, c.destination, c.priority, 
       (timestamp() - c.inbound_time) / 3600000 AS hours_in_storage
OUTPUT TO webhook("https://alert.cargo-station.com/timeout")
```

### 4.2 容量预警

```cypher
// SQ-02: 仓位利用率超过80%
CREATE STANDING QUERY storage_high_usage AS
MATCH (s:Storage)
WHERE s.occupied * 1.0 / s.capacity > 0.8
  AND s.status = "AVAILABLE"
RETURN s.zone, s.id, s.occupied, s.capacity,
       round(s.occupied * 100.0 / s.capacity, 2) AS utilization
OUTPUT TO webhook("https://alert.cargo-station.com/capacity")
```

### 4.3 航班超载

```cypher
// SQ-03: 航班配载接近上限
CREATE STANDING QUERY flight_near_capacity AS
MATCH (f:Flight)
WHERE f.current_load > f.cargo_capacity * 0.95
   OR f.current_volume > f.volume_capacity * 0.95
  AND f.status = "SCHEDULED"
RETURN f.flight_no, f.current_load, f.cargo_capacity,
       f.current_volume, f.volume_capacity
OUTPUT TO webhook("https://alert.cargo-station.com/overload")
```

---

## 五、物化视图(加速查询)

```cypher
// MV-01: 每日货运量统计
CREATE MATERIALIZED VIEW daily_cargo_stats AS
MATCH (c:Cargo)
WHERE c.inbound_time >= timestamp() - 86400000
WITH date(datetime({epochMillis: c.inbound_time})) AS date,
     c.destination AS dest,
     count(c) AS count,
     sum(c.weight) AS total_weight,
     sum(c.volume) AS total_volume
RETURN date, dest, count, total_weight, total_volume
ORDER BY total_weight DESC
REFRESH INCREMENTAL

// MV-02: 航班配载状态
CREATE MATERIALIZED VIEW flight_load_status AS
MATCH (f:Flight)
WHERE f.status IN ["SCHEDULED", "LOADING"]
OPTIONAL MATCH (f)<-[:ASSIGNED_TO]-(c:Cargo)
WITH f.flight_no AS flight,
     f.departure_time AS departure,
     f.cargo_capacity AS capacity,
     count(c) AS cargo_count,
     sum(c.weight) AS current_weight,
     sum(c.volume) AS current_volume
RETURN flight, departure, capacity,
       cargo_count, current_weight, current_volume,
       round(current_weight * 100.0 / capacity, 2) AS load_pct
ORDER BY departure
REFRESH INCREMENTAL
```

---

## 六、部署步骤

### 6.1 环境准备(Week 1)

**服务器配置**:
```bash
# 阿里云 ECS
- 规格: ecs.c7.2xlarge (8vCPU 16GB)
- 系统盘: 100GB SSD
- 数据盘: 500GB SSD (挂载到 /data)
- 操作系统: Ubuntu 22.04 LTS
- 网络: VPC专有网络
```

**软件安装**:
```bash
# 1. 安装依赖
sudo apt update
sudo apt install -y build-essential pkg-config libssl-dev

# 2. 安装Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# 3. 构建Nexora
git clone https://github.com/frank-dkvan/nexora.git
cd nexora
cargo build --release -p nexora-app

# 4. 创建目录
sudo mkdir -p /data/nexora/{rocksdb,wal,backup}
sudo mkdir -p /var/log/nexora
sudo mkdir -p /etc/nexora
```

### 6.2 配置文件(Week 1)

```bash
# /etc/nexora/nexora.toml
cat > /etc/nexora/nexora.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 8080

[storage]
rocksdb_path = "/data/nexora/rocksdb"
wal_dir = "/data/nexora/wal"
wal_sync_policy = "group"

[graph]
num_shards = 256
max_nodes_per_shard = 50000

[security]
require_auth = true
auth_secret = "${NEXORA_AUTH_SECRET}"
rate_limit = true
audit_log_file = "/var/log/nexora/audit.jsonl"
EOF

# 环境变量
cat > /etc/nexora/env << 'EOF'
NEXORA_AUTH_SECRET=$(openssl rand -hex 32)
NEXORA_STRICT_SECURITY=true
RUST_LOG=info,nexora_core=debug
EOF
```

### 6.3 Systemd服务(Week 1)

```bash
# /etc/systemd/system/nexora.service
sudo cat > /etc/systemd/system/nexora.service << 'EOF'
[Unit]
Description=Nexora Graph Database
After=network.target

[Service]
Type=simple
User=nexora
Group=nexora
WorkingDirectory=/opt/nexora
EnvironmentFile=/etc/nexora/env
ExecStart=/opt/nexora/nexora-app --config /etc/nexora/nexora.toml
Restart=always
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

# 启动服务
sudo systemctl daemon-reload
sudo systemctl enable nexora
sudo systemctl start nexora
sudo systemctl status nexora
```

### 6.4 数据迁移(Week 2-3)

**阶段1: 双写模式**
```python
# migrate_cargo.py
from nexora_rs import NexoraClient
import mysql.connector

# 连接
nexora = NexoraClient("http://localhost:8080", 
                      auth_token="your-token")
mysql_db = mysql.connector.connect(
    host="old-db.internal",
    user="app",
    password="***",
    database="cargo_system"
)

# 迁移货物数据
cursor = mysql_db.cursor(dictionary=True)
cursor.execute("SELECT * FROM cargo")

for row in cursor:
    nexora.cypher(f"""
        CREATE (c:Cargo {{
            id: '{row['id']}',
            weight: {row['weight']},
            volume: {row['volume']},
            destination: '{row['destination']}',
            priority: '{row['priority']}',
            inbound_time: {row['inbound_time'].timestamp() * 1000}
        }})
    """)
    print(f"Migrated cargo: {row['id']}")

# 迁移关系
cursor.execute("""
    SELECT c.id AS cargo_id, s.id AS storage_id, cs.stored_at
    FROM cargo c
    JOIN cargo_storage cs ON c.id = cs.cargo_id
    JOIN storage s ON cs.storage_id = s.id
""")

for row in cursor:
    nexora.cypher(f"""
        MATCH (c:Cargo {{id: '{row['cargo_id']}'}}),
              (s:Storage {{id: '{row['storage_id']}'}})
        CREATE (c)-[:STORED_IN {{
            stored_at: {row['stored_at'].timestamp() * 1000}
        }}]->(s)
    """)
```


**阶段2: 数据验证**
```bash
# 对比工具
python3 scripts/validate_migration.py \
  --mysql-host old-db.internal \
  --nexora-url http://localhost:8080 \
  --sample-size 1000

# 验证输出
✓ Cargo count match: 12,543
✓ Storage relationship match: 12,543
✓ Flight assignment match: 8,234
✓ Data integrity: 100%
```

**阶段3: 切换流量**
```bash
# 前端配置
API_ENDPOINT=http://nexora.cargo-station.com
ENABLE_DUAL_WRITE=false
```

---

## 七、前端集成

### 7.1 实时监控大屏

```vue
<!-- CargoMonitor.vue -->
<template>
  <div class="cargo-dashboard">
    <div class="metrics">
      <MetricCard
        title="今日货运量"
        :value="stats.daily_cargo_count"
        unit="件"
        :trend="stats.cargo_trend"
      />
      <MetricCard
        title="仓位利用率"
        :value="stats.storage_utilization"
        unit="%"
        :alert="stats.storage_utilization > 80"
      />
      <MetricCard
        title="待配载货物"
        :value="stats.pending_assignment"
        unit="件"
        :alert="stats.pending_assignment > 100"
      />
    </div>
    
    <div class="charts">
      <GraphVisualization
        :nodes="cargoGraph.nodes"
        :edges="cargoGraph.edges"
        @node-click="showCargoDetail"
      />
    </div>
    
    <div class="alerts">
      <AlertList :items="realtimeAlerts" />
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { NexoraClient } from 'nexora-client'

const client = new NexoraClient('https://nexora.cargo-station.com')
const stats = ref({})
const cargoGraph = ref({ nodes: [], edges: [] })
const realtimeAlerts = ref([])

// WebSocket 实时更新
const ws = new WebSocket('wss://nexora.cargo-station.com/ws')
ws.onmessage = (event) => {
  const data = JSON.parse(event.data)
  if (data.type === 'standing_query_result') {
    realtimeAlerts.value.unshift(data.result)
  }
}

// 定期刷新统计
setInterval(async () => {
  const result = await client.queryMaterializedView('daily_cargo_stats')
  stats.value = result.data[0]
}, 5000)
</script>
```

### 7.2 货物追踪页面

```typescript
// CargoTracker.ts
import { NexoraClient } from 'nexora-client';

export class CargoTracker {
  private client: NexoraClient;

  constructor(endpoint: string) {
    this.client = new NexoraClient(endpoint);
  }

  async trackCargo(cargoId: string) {
    const query = `
      MATCH (c:Cargo {id: $cargoId})
      OPTIONAL MATCH (c)-[r:STORED_IN]->(s:Storage)
      OPTIONAL MATCH (c)-[a:ASSIGNED_TO]->(f:Flight)
      OPTIONAL MATCH (c)-[h:HANDLED_BY]->(p:Staff)
      RETURN c, 
             collect(DISTINCT {storage: s.id, time: r.stored_at}) AS storage,
             collect(DISTINCT {flight: f.flight_no, time: a.assigned_at}) AS flights,
             collect(DISTINCT {staff: p.name, action: h.action, time: h.timestamp}) AS handlers
    `;

    const result = await this.client.cypher(query, { cargoId });
    return this.formatTrackingData(result);
  }

  private formatTrackingData(result: any) {
    const timeline = [];
    
    // 合并所有事件并排序
    const events = [
      ...result.storage.map(s => ({ type: 'storage', ...s })),
      ...result.flights.map(f => ({ type: 'flight', ...f })),
      ...result.handlers.map(h => ({ type: 'handling', ...h }))
    ].sort((a, b) => a.time - b.time);

    return {
      cargo: result.c,
      timeline: events
    };
  }
}
```

---

## 八、性能测试

### 8.1 压力测试脚本

```python
# stress_test.py
import asyncio
import aiohttp
import time
import random

async def create_cargo(session, id):
    """模拟货物入库"""
    query = f"""
    CREATE (c:Cargo {{
        id: 'TEST{id:06d}',
        weight: {random.uniform(100, 1000)},
        volume: {random.uniform(1, 5)},
        destination: '{random.choice(["PVG", "PEK", "CAN", "SHA"])}',
        priority: '{random.choice(["HIGH", "NORMAL", "LOW"])}',
        inbound_time: timestamp()
    }})
    RETURN c.id
    """
    
    async with session.post(
        'http://localhost:8080/api/v2/query/cypher',
        json={'query': query}
    ) as resp:
        return await resp.json()

async def query_cargo(session, id):
    """模拟货物查询"""
    query = f"MATCH (c:Cargo {{id: 'TEST{id:06d}'}}) RETURN c"
    
    async with session.post(
        'http://localhost:8080/api/v2/query/cypher',
        json={'query': query}
    ) as resp:
        return await resp.json()

async def stress_test(concurrency=100, duration=60):
    """压力测试: 100并发, 持续60秒"""
    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        request_count = 0
        error_count = 0
        
        while time.time() - start_time < duration:
            tasks = []
            for i in range(concurrency):
                if random.random() > 0.5:
                    tasks.append(create_cargo(session, request_count + i))
                else:
                    tasks.append(query_cargo(session, random.randint(0, request_count)))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            request_count += len(results)
            error_count += sum(1 for r in results if isinstance(r, Exception))
        
        elapsed = time.time() - start_time
        qps = request_count / elapsed
        error_rate = error_count / request_count * 100
        
        print(f"Total Requests: {request_count}")
        print(f"QPS: {qps:.2f}")
        print(f"Error Rate: {error_rate:.2f}%")
        print(f"Elapsed: {elapsed:.2f}s")

if __name__ == '__main__':
    asyncio.run(stress_test(concurrency=100, duration=60))
```

### 8.2 预期性能指标

| 指标 | 目标 | 实测 | 状态 |
|------|------|------|------|
| 写入QPS | > 200 | - | 待测 |
| 查询P99延迟 | < 100ms | - | 待测 |
| 图遍历延迟 | < 50ms | - | 待测 |
| 并发用户 | > 100 | - | 待测 |
| 内存占用 | < 20GB | - | 待测 |

---

## 九、运维手册

### 9.1 日常巡检

**每日检查**:
```bash
# 服务状态
sudo systemctl status nexora

# 日志检查
tail -n 100 /var/log/nexora/nexora.log

# 磁盘空间
df -h /data/nexora

# 内存占用
free -h

# 查询性能
curl http://localhost:8080/metrics | grep nexora_query_duration
```

### 9.2 备份策略

**自动备份脚本**:
```bash
#!/bin/bash
# /opt/nexora/scripts/backup.sh

BACKUP_DIR="/data/nexora/backup"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="nexora_backup_${DATE}.tar.gz"

# 停止写入 (可选,热备份可跳过)
# curl -X POST http://localhost:8080/api/v2/admin/readonly

# 备份数据
tar -czf ${BACKUP_DIR}/${BACKUP_FILE} \
  /data/nexora/rocksdb \
  /data/nexora/wal \
  /etc/nexora/nexora.toml

# 上传到OSS
aliyun oss cp ${BACKUP_DIR}/${BACKUP_FILE} \
  oss://nexora-backup/cargo-station/${BACKUP_FILE}

# 恢复写入
# curl -X POST http://localhost:8080/api/v2/admin/writable

# 清理本地旧备份 (保留7天)
find ${BACKUP_DIR} -name "nexora_backup_*.tar.gz" -mtime +7 -delete

echo "Backup completed: ${BACKUP_FILE}"
```

**Crontab配置**:
```bash
# 每天凌晨2点备份
0 2 * * * /opt/nexora/scripts/backup.sh >> /var/log/nexora/backup.log 2>&1
```

### 9.3 故障处理

**场景1: 服务无响应**
```bash
# 1. 查看进程
ps aux | grep nexora-app

# 2. 检查端口
netstat -tlnp | grep 8080

# 3. 查看日志
journalctl -u nexora -n 100

# 4. 重启服务
sudo systemctl restart nexora
```

**场景2: 查询变慢**
```bash
# 1. 查看慢查询
curl http://localhost:8080/api/v2/admin/slow-queries

# 2. 检查内存
free -h

# 3. 查看索引统计
curl http://localhost:8080/api/v2/admin/index-stats

# 4. 重建索引 (如需要)
curl -X POST http://localhost:8080/api/v2/admin/rebuild-index
```

**场景3: 数据恢复**
```bash
# 1. 停止服务
sudo systemctl stop nexora

# 2. 清空现有数据
rm -rf /data/nexora/rocksdb/*
rm -rf /data/nexora/wal/*

# 3. 下载备份
aliyun oss cp oss://nexora-backup/cargo-station/nexora_backup_YYYYMMDD.tar.gz /tmp/

# 4. 解压恢复
tar -xzf /tmp/nexora_backup_YYYYMMDD.tar.gz -C /

# 5. 重启服务
sudo systemctl start nexora
```

---

## 十、培训计划

### 10.1 运维人员培训 (2天)

**Day 1: 基础操作**
- Nexora 架构介绍
- 服务启动/停止
- 日志查看和分析
- 备份和恢复操作
- 监控指标解读

**Day 2: 故障处理**
- 常见故障场景
- 性能调优
- 数据迁移
- 应急预案演练

### 10.2 开发人员培训 (3天)

**Day 1: Cypher基础**
- 图数据库概念
- Cypher语法
- 创建/查询/更新/删除
- 索引和性能优化

**Day 2: 业务建模**
- 货运站数据模型
- 复杂查询编写
- Standing Queries使用
- 物化视图配置

**Day 3: API集成**
- REST API使用
- WebSocket实时更新
- SDK集成 (Python/TypeScript)
- 前端可视化

---

## 十一、验收标准

### 11.1 功能验收

- ✅ 货物全生命周期追踪完整
- ✅ 仓位管理功能正常
- ✅ 航班配载算法准确
- ✅ 实时监控大屏展示正常
- ✅ 告警推送及时

### 11.2 性能验收

- ✅ 查询P99延迟 < 100ms
- ✅ 写入QPS > 200
- ✅ 并发用户 > 100
- ✅ 系统可用性 > 99.5%

### 11.3 安全验收

- ✅ 认证机制启用
- ✅ TLS加密通信
- ✅ 审计日志完整
- ✅ 敏感数据加密

### 11.4 运维验收

- ✅ 监控告警配置完成
- ✅ 备份恢复流程验证
- ✅ 故障演练通过
- ✅ 运维文档完善

---

## 十二、成功案例参考

### 12.1 预期业务价值

**效率提升**:
- 货物查询速度提升 10倍 (1000ms → 100ms)
- 配载操作时间减少 80% (30分钟 → 6分钟)
- 仓位利用率提升 15%

**成本节约**:
- 减少人工操作时间 50%
- 降低货物滞留成本 30%
- 节省IT维护成本 40%

**体验改善**:
- 客户查询响应时间 < 3秒
- 实时状态更新准确率 100%
- 异常告警响应时间 < 1分钟

---

## 附录

### A. 常用Cypher查询模板

```cypher
-- 1. 查询滞留货物
MATCH (c:Cargo)-[:STORED_IN]->(s:Storage)
WHERE NOT (c)-[:ASSIGNED_TO]->(:Flight)
  AND c.inbound_time < timestamp() - $hours * 3600000
RETURN c.id, c.destination, s.id, 
       (timestamp() - c.inbound_time) / 3600000 AS hours_delayed
ORDER BY hours_delayed DESC
LIMIT 20

-- 2. 航班负载详情
MATCH (f:Flight {flight_no: $flight_no})
OPTIONAL MATCH (f)<-[:ASSIGNED_TO]-(c:Cargo)
RETURN f,
       count(c) AS cargo_count,
       sum(c.weight) AS total_weight,
       sum(c.volume) AS total_volume,
       collect({id: c.id, weight: c.weight, dest: c.destination}) AS cargo_list

-- 3. 仓区热力图
MATCH (s:Storage)
OPTIONAL MATCH (s)<-[:STORED_IN]-(c:Cargo)
WITH s.zone AS zone,
     s.row AS row,
     s.column AS col,
     s.occupied * 1.0 / s.capacity AS utilization
RETURN zone, row, col, utilization
ORDER BY zone, row, col

-- 4. 人员工作量统计
MATCH (p:Staff)-[h:HANDLED_BY]-(c:Cargo)
WHERE h.timestamp > timestamp() - 86400000
WITH p.name AS staff,
     p.role AS role,
     count(DISTINCT c) AS handled_count,
     sum(h.duration) / 3600 AS work_hours
RETURN staff, role, handled_count, work_hours
ORDER BY handled_count DESC
```

### B. 监控告警配置

```yaml
# alertmanager.yml
groups:
- name: nexora_cargo_alerts
  interval: 30s
  rules:
  - alert: CargoTimeout
    expr: nexora_cargo_timeout_count > 10
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "货物滞留超时 ({{ $value }}件)"
      
  - alert: StorageHighUsage
    expr: nexora_storage_utilization > 0.9
    for: 10m
    labels:
      severity: critical
    annotations:
      summary: "仓位使用率过高 ({{ $value }}%)"
      
  - alert: FlightOverload
    expr: nexora_flight_load_ratio > 0.98
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "航班负载接近上限"
```

---

**文档版本**: v1.0  
**编写日期**: 2026-07-10  
**责任人**: [待指定]  
**审批人**: [待指定]
